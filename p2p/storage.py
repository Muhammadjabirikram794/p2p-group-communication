"""
storage.py - Distributed chunk storage: sharing (upload), serving chunks,
and keeping the file catalog in sync.

Share a file (upload):
    1. build manifest, compute placement = HRW top-2 per chunk over members
    2. STORE_CHUNK (binary) to every holder (stored locally if holder == us)
    3. wait for STORE_ACK from every holder
    4. FILE_ANNOUNCE {files:[{manifest, locations}]} to every peer
The sharer keeps only the chunks HRW gave it - nobody stores the whole file.

Serve:
    CHUNK_REQUEST {file_id, index, offset, stream}
      -> CHUNK_DATA pieces (32 KB each) from 'offset' to the end of the chunk,
         on the stream the requester chose
      -> CHUNK_NACK {file_id, index} if we don't have it
Catalog sync:
    joining peer: FILE_LIST_REQ -> FILE_LIST (all manifests + locations)
    any change:   CHUNK_LOCATION {file_id, index, peer, present} to everyone
"""

import logging
import threading

from . import config, placement, protocol as P
from .manifest import build_manifest, iter_chunks, verify_chunk
from .util import now, pid_str

log = logging.getLogger("storage")


class StorageService:
    def __init__(self, self_id, transport, membership, catalog, store, events,
                 metrics=None, replication=config.REPLICATION,
                 chunk_size=config.CHUNK_SIZE):
        self.self_id = self_id
        self.t = transport
        self.members = membership
        self.catalog = catalog
        self.store = store
        self.events = events
        self.metrics = metrics
        self.replication = replication
        self.chunk_size = chunk_size
        self._acks = {}                     # (fid, idx, pid) -> threading.Event
        self._lock = threading.Lock()
        self._list_requests = 0
        self._started = now()

        for mtype, fn in ((P.STORE_CHUNK, self._on_store_chunk),
                          (P.STORE_ACK, self._on_store_ack),
                          (P.CHUNK_REQUEST, self._on_chunk_request),
                          (P.FILE_ANNOUNCE, self._on_file_announce),
                          (P.FILE_LIST_REQ, self._on_file_list_req),
                          (P.FILE_LIST, self._on_file_list),
                          (P.CHUNK_LOCATION, self._on_chunk_location)):
            transport.on(mtype, fn)
        events.on("peer_joined", self._on_peer_joined)
        events.on("peer_left", self._on_peer_left)

    # ================= sharing a file =================
    def share(self, path: str, timeout: float = 60.0):
        """Split 'path' into chunks and distribute them. Blocking.
        Returns the Manifest, raises RuntimeError on failure."""
        members = self.members.members()
        if not placement.can_place(len(members), self.replication):
            raise RuntimeError("need at least %d peers to share (have %d)"
                               % (self.replication + 1, len(members)))
        m = build_manifest(path, owner=self.self_id, chunk_size=self.chunk_size)
        if self.catalog.get(m.file_id):
            raise RuntimeError("file already shared: %s" % m.file_id)
        plan = placement.placement_for_file(m.file_id, m.num_chunks, members,
                                            self.replication)
        t0 = now()
        waits = []
        for idx, data in iter_chunks(path, self.chunk_size):
            for holder in plan[idx]:
                if holder == self.self_id:
                    self.store.put(m.file_id, idx, data)
                    self.catalog.set_location(m.file_id, idx, self.self_id)
                    continue
                ev = threading.Event()
                with self._lock:
                    self._acks[(m.file_id, idx, holder)] = ev
                waits.append((idx, holder, ev))
                self.t.send(holder, config.STREAM_DATA_BASE + idx % config.DATA_STREAMS,
                            P.STORE_CHUNK,
                            P.pack_chunk(m.file_id, idx, 0, len(data), data))
        missing = []
        deadline = now() + timeout
        for idx, holder, ev in waits:
            if not ev.wait(max(0.0, deadline - now())):
                missing.append((idx, holder))
        with self._lock:
            for idx, holder, _ in waits:
                self._acks.pop((m.file_id, idx, holder), None)
        for idx, holder, _ in waits:
            if (idx, holder) not in missing:
                self.catalog.set_location(m.file_id, idx, holder)
        if missing:
            log.warning("share %s: no STORE_ACK for %s (rebalancing will fix it)",
                        m.name, [(i, pid_str(h)) for i, h in missing])
        self.catalog.add_manifest(m)
        announce = P.pack_json(self.catalog.to_wire([m.file_id]))
        for pid in self.members.peers():
            self.t.send(pid, config.STREAM_CONTROL, P.FILE_ANNOUNCE, announce)
        secs = now() - t0
        if self.metrics:
            self.metrics.log("share_done", file=m.name, size=m.size,
                             chunks=m.num_chunks, seconds=round(secs, 3),
                             missing_acks=len(missing))
        log.info("shared %s (%d chunks) in %.2fs", m.name, m.num_chunks, secs)
        self.events.emit("file_added", m)
        return m

    # ================= chunk storage =================
    def keep_chunk(self, file_id, idx, data):
        """Store a chunk we are responsible for and tell everybody."""
        self.store.put(file_id, idx, data)
        self.catalog.set_location(file_id, idx, self.self_id)
        self.broadcast_location(file_id, idx, True)

    def drop_chunk(self, file_id, idx):
        self.store.delete(file_id, idx)
        self.catalog.set_location(file_id, idx, self.self_id, present=False)
        self.broadcast_location(file_id, idx, False)

    def broadcast_location(self, file_id, idx, present):
        payload = P.pack_json({"file_id": file_id, "index": idx,
                               "peer": self.self_id, "present": present})
        for pid in self.members.peers():
            self.t.send(pid, config.STREAM_CONTROL, P.CHUNK_LOCATION, payload)

    def _on_store_chunk(self, pid, addr, payload):
        fid, idx, _off, clen, data = P.unpack_chunk(payload)
        m = self.catalog.get(fid)
        ok = len(data) == clen and (m is None or verify_chunk(m, idx, data))
        if ok:
            self.keep_chunk(fid, idx, data)
        else:
            log.warning("rejected corrupt STORE_CHUNK %s/%d from %s", fid, idx,
                        pid_str(pid))
        self.t.send(pid, config.STREAM_CONTROL, P.STORE_ACK,
                    P.pack_json({"file_id": fid, "index": idx, "ok": ok}))

    def _on_store_ack(self, pid, addr, payload):
        d = P.unpack_json(payload)
        if d.get("ok"):
            with self._lock:
                ev = self._acks.get((d["file_id"], int(d["index"]), pid))
            if ev:
                ev.set()

    # ================= serving downloads =================
    def _on_chunk_request(self, pid, addr, payload):
        d = P.unpack_json(payload)
        fid, idx = d["file_id"], int(d["index"])
        offset = int(d.get("offset", 0))
        stream = int(d.get("stream", config.STREAM_DATA_BASE))
        data = self.store.get(fid, idx)
        if data is None:
            self.t.send(pid, config.STREAM_CONTROL, P.CHUNK_NACK,
                        P.pack_json({"file_id": fid, "index": idx}))
            return
        if self.metrics:
            self.metrics.incr("chunks_served")
        for off in range(offset - offset % config.PIECE_SIZE, len(data),
                         config.PIECE_SIZE):
            piece = data[off:off + config.PIECE_SIZE]
            self.t.send(pid, stream, P.CHUNK_DATA,
                        P.pack_chunk(fid, idx, off, len(data), piece))

    # ================= catalog sync =================
    def _on_file_announce(self, pid, addr, payload):
        for m in self.catalog.merge_wire(P.unpack_json(payload)):
            log.info("new file %s (%s, %d chunks) from %s", m.name, m.file_id,
                     m.num_chunks, pid_str(pid))
            self._verify_existing(m)
            self.events.emit("file_added", m)

    def _verify_existing(self, m):
        """Chunks may arrive before their manifest; check them now."""
        for idx in self.store.chunks_of(m.file_id):
            data = self.store.get(m.file_id, idx)
            if data is None or not verify_chunk(m, idx, data):
                log.warning("dropping corrupt chunk %s/%d", m.file_id, idx)
                self.drop_chunk(m.file_id, idx)

    def _on_file_list_req(self, pid, addr, payload):
        self.t.send(pid, config.STREAM_CONTROL, P.FILE_LIST,
                    P.pack_json(self.catalog.to_wire()))

    def _on_file_list(self, pid, addr, payload):
        for m in self.catalog.merge_wire(P.unpack_json(payload)):
            self._verify_existing(m)
            self.events.emit("file_added", m)

    def _on_chunk_location(self, pid, addr, payload):
        d = P.unpack_json(payload)
        self.catalog.set_location(d["file_id"], int(d["index"]), int(d["peer"]),
                                  bool(d.get("present", True)))

    def _on_peer_joined(self, pid):
        # A peer that just started asks the first peers it meets for the catalog.
        if now() - self._started < 15.0 and self._list_requests < 2:
            self._list_requests += 1
            self.t.send(pid, config.STREAM_CONTROL, P.FILE_LIST_REQ, b"{}")

    def _on_peer_left(self, pid):
        self.catalog.remove_peer(pid)
