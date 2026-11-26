"""
transport.py - Reliable, multi-stream message transport on top of ONE UDP socket.

    t = Transport(my_id, port=50001)
    t.on(protocol.CHAT, handler)          # handler(peer_id, addr, payload)
    t.start()
    t.add_peer(peer_id, ("192.168.1.7", 50002))
    t.send(peer_id, config.STREAM_CHAT, protocol.CHAT, b"...")   # reliable
    t.send_unreliable(addr, protocol.HEARTBEAT, b"...")          # fire & forget

Threads
  recv thread   : reads datagrams, processes ACKs, buffers/reassembles data,
                  sends one (coalesced) ACK per stream after draining the socket
  timer thread  : retransmissions (RTO / back-off), token-bucket refill, pump
  dispatch thread: runs application handlers in arrival order, so a slow
                  handler never stops ACK processing

Streams: every (peer, stream_id) pair has its own sequence space, so a lost
file segment on stream 2 never delays chat on stream 1 (no head-of-line
blocking). The sender serves lower stream ids first (control > chat > data).

Optional 'rate' (bytes/s) emulates a peer's upload capacity with a token
bucket, used by the benchmarks to make multi-source downloads measurable.
"""

import logging
import queue
import select
import socket
import sys
import threading

from . import config, protocol as P
from .connection import SendStream, RecvStream, epoch_newer
from .lossy import LossySocket
from .rtt import RttEstimator
from .util import now, pid_str

log = logging.getLogger("transport")


class Transport:
    def __init__(self, peer_id: int, port: int = config.DEFAULT_PORT,
                 bind_ip: str = "0.0.0.0", loss: float = 0.0, dup: float = 0.0,
                 window: int = config.SEND_WINDOW, rate: float = 0.0,
                 seed=None):
        self.peer_id = peer_id
        self.window = window
        raw = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        raw.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 * 1024)
        raw.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4 * 1024 * 1024)
        raw.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)
        raw.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
        if sys.platform == "win32":
            # Windows reports ICMP 'port unreachable' as an error on the next
            # recvfrom() of a UDP socket. Disable that (happens when a peer dies).
            try:
                raw.ioctl(socket.SIO_UDP_CONNRESET, False)
            except (AttributeError, OSError):
                pass
        raw.bind((bind_ip, port))
        raw.setblocking(False)
        self.sock = LossySocket(raw, loss=loss, dup=dup, seed=seed)
        self.port = raw.getsockname()[1]

        self._lock = threading.RLock()
        self._addrs = {}           # peer_id -> (ip, port)
        self._send = {}            # (peer_id, stream) -> SendStream
        self._recv = {}            # (peer_id, stream) -> RecvStream
        self._rtt = {}             # peer_id -> RttEstimator
        self._handlers = {}        # type -> handler
        self._unreachable_cbs = []
        self._dispatch_q = queue.Queue()
        self._running = False
        self._threads = []

        self.rate = rate
        self._tokens = rate * 0.05 if rate else 0.0
        self._last_refill = now()

        self.stats = {"sent": 0, "sent_bytes": 0, "retransmits": 0,
                      "fast_retransmits": 0, "timeouts_failed": 0,
                      "received": 0, "bad": 0, "duplicates": 0,
                      "acks_sent": 0, "messages": 0}

    # ================= public API =================
    def on(self, mtype: int, handler):
        self._handlers[mtype] = handler

    def on_unreachable(self, callback):
        """callback(peer_id) when a stream exceeded MAX_RETRIES."""
        self._unreachable_cbs.append(callback)

    def start(self):
        self._running = True
        for name, fn in (("recv", self._recv_loop), ("timer", self._timer_loop),
                         ("dispatch", self._dispatch_loop)):
            th = threading.Thread(target=fn, name="%s-%s" % (name, pid_str(self.peer_id)),
                                  daemon=True)
            th.start()
            self._threads.append(th)

    def stop(self):
        self._running = False
        self._dispatch_q.put(None)
        for th in self._threads:
            th.join(timeout=1.0)
        try:
            self.sock.close()
        except OSError:
            pass

    def add_peer(self, peer_id: int, addr):
        with self._lock:
            self._addrs[peer_id] = tuple(addr)

    def addr_of(self, peer_id: int):
        with self._lock:
            return self._addrs.get(peer_id)

    def reset_peer(self, peer_id: int):
        """Peer is dead: drop everything queued for it, new epochs next time."""
        with self._lock:
            for (pid, _), s in self._send.items():
                if pid == peer_id:
                    s.reset()

    def forget_peer(self, peer_id: int):
        """Peer left for good (LEAVE): free all state."""
        with self._lock:
            self.reset_peer(peer_id)
            for key in [k for k in self._send if k[0] == peer_id]:
                del self._send[key]
            for key in [k for k in self._recv if k[0] == peer_id]:
                del self._recv[key]
            self._addrs.pop(peer_id, None)
            self._rtt.pop(peer_id, None)

    def send(self, peer_id: int, stream: int, mtype: int, payload: bytes = b""):
        """Reliable, in-order (per stream) delivery of one message of any size."""
        with self._lock:
            if peer_id not in self._addrs:
                log.debug("send to unknown peer %s dropped", pid_str(peer_id))
                return False
            key = (peer_id, stream)
            s = self._send.get(key)
            if s is None:
                s = self._send[key] = SendStream(self.peer_id, stream, self.window)
            s.enqueue(mtype, payload)
            self._pump()
        return True

    def send_unreliable(self, addr, mtype: int, payload: bytes = b""):
        pkt = P.Packet(mtype, self.peer_id, payload=payload)
        self._raw_send(P.encode(pkt), addr, count_rate=False)

    def pending(self, peer_id: int = None) -> int:
        """Segments queued or in flight (optionally only towards one peer)."""
        with self._lock:
            return sum(len(s.queue) + len(s.inflight)
                       for (pid, _), s in self._send.items()
                       if peer_id is None or pid == peer_id)

    def flush(self, timeout: float = 2.0) -> bool:
        """Wait until everything sent has been ACKed (or timeout)."""
        end = now() + timeout
        while now() < end:
            if self.pending() == 0:
                return True
            threading.Event().wait(0.02)
        return False

    def rtt_of(self, peer_id: int):
        with self._lock:
            r = self._rtt.get(peer_id)
            return None if r is None else (r.srtt, r.rto)

    # ================= sending internals =================
    def _raw_send(self, data: bytes, addr, count_rate=True):
        try:
            self.sock.sendto(data, addr)
        except OSError as exc:            # e.g. network unreachable
            log.debug("sendto %s failed: %s", addr, exc)
            return
        self.stats["sent"] += 1
        self.stats["sent_bytes"] += len(data)
        if count_rate and self.rate:
            self._tokens -= len(data)

    def _send_pkt(self, pkt: P.Packet, peer_id: int):
        addr = self._addrs.get(peer_id)
        if addr is not None:
            self._raw_send(P.encode(pkt), addr)

    def _pump(self):
        """Send queued segments, highest-priority stream (lowest id) first."""
        for (pid, _), s in sorted(self._send.items(), key=lambda kv: kv[0][1]):
            while s.can_send():
                if self.rate and self._tokens <= 0:
                    return
                self._send_pkt(s.next_packet(now()), pid)

    def _rtt_for(self, peer_id):
        r = self._rtt.get(peer_id)
        if r is None:
            r = self._rtt[peer_id] = RttEstimator()
        return r

    # ================= threads =================
    def _timer_loop(self):
        while self._running:
            threading.Event().wait(config.TIMER_TICK)
            failed = set()
            with self._lock:
                t = now()
                if self.rate:
                    burst = max(self.rate * 0.05, 4 * config.MAX_DATAGRAM)
                    self._tokens = min(burst, self._tokens +
                                       self.rate * (t - self._last_refill))
                    self._last_refill = t
                for (pid, _), s in self._send.items():
                    if not s.inflight:
                        continue
                    pkts, gave_up = s.expired(t, self._rtt_for(pid))
                    if gave_up:
                        self.stats["timeouts_failed"] += 1
                        s.reset()
                        failed.add(pid)
                        continue
                    for pkt in pkts:
                        self.stats["retransmits"] += 1
                        self._send_pkt(pkt, pid)
                self._pump()
            for pid in failed:
                log.info("peer %s unreachable (max retries)", pid_str(pid))
                for cb in self._unreachable_cbs:
                    cb(pid)

    def _recv_loop(self):
        while self._running:
            try:
                ready, _, _ = select.select([self.sock.fileno()], [], [], 0.1)
            except (OSError, ValueError):
                break
            if not ready:
                continue
            touched = set()
            for _ in range(512):                 # drain the socket
                try:
                    data, addr = self.sock.recvfrom(65535)
                except (BlockingIOError, InterruptedError):
                    break
                except ConnectionResetError:     # Windows ICMP noise
                    continue
                except OSError:
                    return
                self._handle_datagram(data, addr, touched)
            if touched:
                with self._lock:
                    for key in touched:
                        self._send_ack(key)
                    self._pump()

    def _handle_datagram(self, data, addr, touched):
        try:
            pkt = P.decode(data)
        except P.ProtocolError as exc:
            self.stats["bad"] += 1
            log.debug("bad datagram from %s: %s", addr, exc)
            return
        if pkt.src_id == self.peer_id:
            return                               # our own multicast echo
        self.stats["received"] += 1
        pid = pkt.src_id

        if pkt.flags & P.FLAG_ACK:                         # ---- ACK
            with self._lock:
                s = self._send.get((pid, pkt.stream))
                if s is None:
                    return
                bits = P.sack_bitmap_unpack(pkt.payload) if pkt.flags & P.FLAG_SACK else 0
                sample, fast = s.on_ack(pkt.ack, bits, pkt.window, pkt.epoch, now())
                if sample is not None:
                    self._rtt_for(pid).sample(sample)
                for f in fast:
                    self.stats["fast_retransmits"] += 1
                    self._send_pkt(f, pid)
            return

        if not pkt.flags & P.FLAG_RELIABLE:                # ---- unreliable
            self._dispatch_q.put((pkt.type, pid, addr, pkt.payload))
            return

        with self._lock:                                   # ---- reliable data
            if pid not in self._addrs:
                self._addrs[pid] = addr          # learn the return address
            key = (pid, pkt.stream)
            r = self._recv.get(key)
            if r is None or (pkt.epoch != r.epoch and epoch_newer(pkt.epoch, r.epoch)):
                r = self._recv[key] = RecvStream(pkt.epoch)
            elif pkt.epoch != r.epoch:
                return                           # stale packet of an old epoch
            before = r.duplicates
            msgs = r.on_packet(pkt)
            self.stats["duplicates"] += r.duplicates - before
            touched.add(key)
        for mtype, payload in msgs:
            self.stats["messages"] += 1
            self._dispatch_q.put((mtype, pid, addr, payload))

    def _send_ack(self, key):
        r = self._recv.get(key)
        if r is None:
            return
        ack, bits, win = r.ack_info()
        flags = P.FLAG_ACK | (P.FLAG_SACK if bits else 0)
        pkt = P.Packet(P.ACK, self.peer_id, key[1], 0, ack, win, flags, r.epoch,
                       P.sack_bitmap_pack(bits) if bits else b"")
        addr = self._addrs.get(key[0])
        if addr:
            self.stats["acks_sent"] += 1
            self._raw_send(P.encode(pkt), addr, count_rate=False)

    def _dispatch_loop(self):
        while True:
            item = self._dispatch_q.get()
            if item is None or not self._running:
                return
            mtype, pid, addr, payload = item
            handler = self._handlers.get(mtype)
            if handler is None:
                log.debug("no handler for type 0x%02x", mtype)
                continue
            try:
                handler(pid, addr, payload)
            except Exception:
                log.exception("handler for %s failed", P.TYPE_NAMES.get(mtype, mtype))
