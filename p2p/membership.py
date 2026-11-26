"""
membership.py - Serverless peer discovery, heartbeats and failure detection.

Join (no central server):
  1. new peer sends HELLO {name, port} by LAN multicast (239.255.42.99:50000)
     and/or by unicast to any --bootstrap address (only needed once).
  2. every peer that receives a HELLO from an unknown peer adds it and answers
     with WELCOME {name, port, peers:[[id, ip, port, name], ...]} (reliable).
  3. the new peer sends a unicast HELLO to every listed peer it doesn't know
     yet -> after one round trip it knows the whole group.
Liveness:
  * HEARTBEAT {name, port, peers} every second (unreliable) to every peer.
    The peer list is gossip: unknown peers in it are contacted with HELLO,
    so a peer that missed the multicast still finds everyone.
  * no heartbeat for 3 s -> SUSPECT, 5 s -> DEAD (crash detected).
  * LEAVE (reliable) on a graceful exit removes the peer immediately.
Events emitted: peer_joined, peer_suspect, peer_left.
"""

import logging
import socket
import struct
import threading

from . import config, protocol as P
from .peertable import PeerTable
from .util import now, pid_str

log = logging.getLogger("membership")


class Membership:
    def __init__(self, self_id, name, transport, events, table=None,
                 bootstrap=(), multicast=True,
                 heartbeat=config.HEARTBEAT_INTERVAL):
        self.self_id = self_id
        self.name = name
        self.t = transport
        self.events = events
        self.table = table or PeerTable()
        self.bootstrap = list(bootstrap)
        self.use_multicast = multicast
        self.heartbeat_interval = heartbeat
        self._left = set()               # peers that sent LEAVE (never revive)
        self._hello_sent = {}            # peer addr -> time (rate limit)
        self._running = False
        self._threads = []
        self._mcast = None

        transport.on(P.HELLO, self._on_hello)
        transport.on(P.WELCOME, self._on_welcome)
        transport.on(P.HEARTBEAT, self._on_heartbeat)
        transport.on(P.LEAVE, self._on_leave)
        transport.on_unreachable(self._on_unreachable)

    # ================= lifecycle =================
    def start(self):
        self._running = True
        if self.use_multicast:
            self._open_multicast()
        for fn in (self._hello_loop, self._heartbeat_loop, self._detector_loop):
            th = threading.Thread(target=fn, daemon=True,
                                  name=fn.__name__ + "-" + self.name)
            th.start()
            self._threads.append(th)

    def stop(self, graceful=True):
        if graceful:
            for pid in self.table.ids():
                self.t.send(pid, config.STREAM_CONTROL, P.LEAVE,
                            P.pack_json({"peer": self.self_id}))
            self.t.flush(timeout=1.0)
        self._running = False
        if self._mcast:
            try:
                self._mcast.close()
            except OSError:
                pass

    # ================= queries =================
    def peers(self):
        """Known live peers (ALIVE or SUSPECT), not including ourselves."""
        return self.table.ids()

    def members(self):
        """Everybody in the group including ourselves, sorted (for placement)."""
        return sorted(set(self.table.ids()) | {self.self_id})

    def is_alive(self, pid) -> bool:
        return pid == self.self_id or pid in self.table

    def name_of(self, pid) -> str:
        if pid == self.self_id:
            return self.name
        p = self.table.get(pid)
        return p.name if p and p.name else pid_str(pid)

    # ================= sending =================
    def _hello_payload(self):
        return P.pack_json({"name": self.name, "port": self.t.port})

    def _send_hello(self, addr):
        key = tuple(addr)
        t = now()
        if t - self._hello_sent.get(key, 0) < 1.0:
            return
        self._hello_sent[key] = t
        self.t.send_unreliable(key, P.HELLO, self._hello_payload())

    def _hello_loop(self):
        count = 0
        while self._running:
            if self.use_multicast:
                self.t.send_unreliable((config.MULTICAST_GROUP, config.MULTICAST_PORT),
                                       P.HELLO, self._hello_payload())
            for addr in self.bootstrap:
                self._hello_sent.pop(tuple(addr), None)
                self._send_hello(addr)
            count += 1
            wait = 0.5 if count < config.HELLO_REPEAT else config.HELLO_INTERVAL
            threading.Event().wait(wait)

    def _peer_list(self):
        return [[p.peer_id, p.addr[0], p.addr[1], p.name]
                for p in self.table.snapshot()
                if p.state == "alive"]

    def _heartbeat_loop(self):
        while self._running:
            payload = P.pack_json({"name": self.name, "port": self.t.port,
                                   "peers": self._peer_list()})
            for p in self.table.snapshot():
                self.t.send_unreliable(p.addr, P.HEARTBEAT, payload)
            threading.Event().wait(self.heartbeat_interval)

    def _detector_loop(self):
        while self._running:
            threading.Event().wait(0.25)
            suspects, dead = self.table.check()
            for pid in suspects:
                log.info("peer %s suspected", pid_str(pid))
                self.events.emit("peer_suspect", pid)
            for pid in dead:
                self._peer_gone(pid, "crashed (no heartbeat)")

    # ================= receiving =================
    def _add(self, pid, addr, name) -> bool:
        if pid == self.self_id or pid in self._left:
            return False
        new = self.table.upsert(pid, addr, name)
        self.t.add_peer(pid, addr)
        if new:
            log.info("peer %s (%s) joined from %s:%d", pid_str(pid), name, *addr)
            self.events.emit("peer_joined", pid)
        return new

    def _addr_from(self, addr, data):
        return (addr[0], int(data.get("port", addr[1])))

    def _on_hello(self, pid, addr, payload):
        data = P.unpack_json(payload)
        a = self._addr_from(addr, data)
        if self._add(pid, a, data.get("name", "")):
            self.t.send(pid, config.STREAM_CONTROL, P.WELCOME, P.pack_json(
                {"name": self.name, "port": self.t.port, "peers": self._peer_list()}))

    def _on_welcome(self, pid, addr, payload):
        data = P.unpack_json(payload)
        self._add(pid, self._addr_from(addr, data), data.get("name", ""))
        self._learn(data.get("peers", []))

    def _on_heartbeat(self, pid, addr, payload):
        data = P.unpack_json(payload)
        if not self.table.touch(pid):
            self._add(pid, self._addr_from(addr, data), data.get("name", ""))
        self._learn(data.get("peers", []))

    def _learn(self, peers):
        """Gossip: say HELLO to listed peers we don't know (direct evidence
        only - we never add a peer we have not heard from ourselves)."""
        for pid, ip, port, *_ in peers:
            pid = int(pid)
            if pid != self.self_id and pid not in self.table and pid not in self._left:
                self._send_hello((ip, int(port)))

    def _on_leave(self, pid, addr, payload):
        self._left.add(pid)
        self._peer_gone(pid, "left")

    def _on_unreachable(self, pid):
        if pid in self.table:
            self._peer_gone(pid, "unreachable")

    def _peer_gone(self, pid, why):
        if self.table.remove(pid) is None and why != "crashed (no heartbeat)":
            return
        log.info("peer %s %s", pid_str(pid), why)
        if why == "left":
            self.t.forget_peer(pid)
        else:
            self.t.reset_peer(pid)
        self.events.emit("peer_left", pid)

    # ================= multicast socket =================
    def _open_multicast(self):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            if hasattr(socket, "SO_REUSEPORT"):
                try:
                    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
                except OSError:
                    pass
            s.bind(("", config.MULTICAST_PORT))
            mreq = struct.pack("4s4s", socket.inet_aton(config.MULTICAST_GROUP),
                               socket.inet_aton("0.0.0.0"))
            s.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
            s.settimeout(0.5)
            self._mcast = s
        except OSError as exc:
            log.warning("multicast unavailable (%s) - use --bootstrap", exc)
            self._mcast = None
            return
        th = threading.Thread(target=self._mcast_loop, daemon=True,
                              name="mcast-" + self.name)
        th.start()
        self._threads.append(th)

    def _mcast_loop(self):
        while self._running and self._mcast:
            try:
                data, addr = self._mcast.recvfrom(2048)
            except socket.timeout:
                continue
            except OSError:
                return
            try:
                pkt = P.decode(data)
            except P.ProtocolError:
                continue
            if pkt.type == P.HELLO and pkt.src_id != self.self_id:
                try:
                    self._on_hello(pkt.src_id, addr, pkt.payload)
                except Exception:
                    log.exception("bad HELLO")
