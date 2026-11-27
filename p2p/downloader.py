"""
downloader.py - Multi-source parallel download with replica failover.

For every chunk we keep a small state machine:
    PENDING --request--> ACTIVE --all pieces + hash OK--> DONE
                           |  NACK / source dead / stalled / bad hash
                           +--------> PENDING (that source is excluded,
                                      resume from the first missing byte)

Scheduling: each pending chunk goes to the candidate holder with the fewest
outstanding requests (max MAX_OUTSTANDING_PER_PEER each) -> different chunks
come from different peers in parallel. Candidates = peers the catalog says
hold the chunk, then (fallback) other live peers in HRW order.

Fault tolerance: a source is abandoned when membership declares it dead,
when it sends CHUNK_NACK, or when no data arrived for REQUEST_STALL seconds.
Already received 32 KB pieces are kept, the next replica continues from
'offset' = first missing byte.

Integrity: SHA-256 per chunk (from the manifest) and for the whole file.
"""

import logging
import os
import threading

from . import config, placement, protocol as P
from .manifest import sha256_hex
from .util import now, pid_str

log = logging.getLogger("downloader")


class _Chunk:
    def __init__(self, idx, length):
        self.idx = idx
        self.length = length
        self.buf = bytearray(length)
        self.got = set()          # piece offsets received
        self.received = 0
        self.source = None
        self.last_progress = 0.0
        self.failed = set()       # peers that failed for this chunk
        self.done = False
        self.nacked = False
        self.attempts = 0

    def first_missing(self):
        off = 0
        while off in self.got:
            off += config.PIECE_SIZE
        return off

    def reset_data(self):
        self.buf = bytearray(self.length)
        self.got.clear()
        self.received = 0


class _Job:
    def __init__(self, manifest, indices):
        self.m = manifest
        self.chunks = {i: _Chunk(i, manifest.chunk_length(i)) for i in indices}
        self.bytes_from = {}      # peer -> bytes received


class DownloadError(Exception):
    pass


class Downloader:
    def __init__(self, self_id, transport, membership, catalog, store, events,
                 metrics=None, out_dir="downloads"):
        self.self_id = self_id
        self.t = transport
        self.members = membership
        self.catalog = catalog
        self.store = store
        self.events = events
        self.metrics = metrics
        self.out_dir = out_dir
        self._cond = threading.Condition()
        self._jobs = []
        transport.on(P.CHUNK_DATA, self._on_chunk_data)
        transport.on(P.CHUNK_NACK, self._on_chunk_nack)
        events.on("peer_left", lambda pid: self._wake())

    # ================= network callbacks =================
    def _on_chunk_data(self, pid, addr, payload):
        fid, idx, off, clen, data = P.unpack_chunk(payload)
        with self._cond:
            for job in self._jobs:
                c = job.chunks.get(idx)
                if job.m.file_id != fid or c is None or c.done or off in c.got:
                    continue
                if off + len(data) > c.length:
                    continue
                c.buf[off:off + len(data)] = data
                c.got.add(off)
                c.received += len(data)
                c.last_progress = now()
                job.bytes_from[pid] = job.bytes_from.get(pid, 0) + len(data)
            self._cond.notify_all()

    def _on_chunk_nack(self, pid, addr, payload):
        d = P.unpack_json(payload)
        with self._cond:
            for job in self._jobs:
                c = job.chunks.get(int(d["index"]))
                if job.m.file_id == d["file_id"] and c and c.source == pid:
                    c.nacked = True
            self._cond.notify_all()

    def _wake(self):
        with self._cond:
            self._cond.notify_all()

    # ================= engine =================
    def _candidates(self, fid, idx, exclude):
        alive = [p for p in self.members.peers() if p not in exclude]
        known = [p for p in alive if p in self.catalog.holders(fid, idx)]
        others = [p for p in placement.rank_peers(fid, idx, alive) if p not in known]
        return known + others

    def fetch(self, manifest, indices=None, use_local=True, max_sources=None,
              timeout=300.0, label="download"):
        """Fetch chunks; returns {idx: bytes}. Raises DownloadError."""
        return self._fetch(manifest, indices, use_local, max_sources, timeout,
                           label)[0]

    def _fetch(self, manifest, indices, use_local, max_sources, timeout, label):
        fid = manifest.file_id
        indices = list(range(manifest.num_chunks)) if indices is None else list(indices)
        job = _Job(manifest, indices)
        result = {}
        if use_local:
            for i in list(job.chunks):
                data = self.store.get(fid, i)
                if data is not None and sha256_hex(data) == manifest.chunk_hashes[i]:
                    result[i] = data
                    del job.chunks[i]
        with self._cond:
            self._jobs.append(job)
        deadline = now() + timeout
        load = {}                 # peer -> active requests
        used = []                 # distinct sources in order of first use
        try:
            while True:
                requests = []
                with self._cond:
                    t = now()
                    alive = set(self.members.peers())
                    active = [c for c in job.chunks.values() if not c.done and c.source]
                    # ---- 1. check active chunks
                    for c in active:
                        if c.received >= c.length:
                            data = bytes(c.buf)
                            if sha256_hex(data) == manifest.chunk_hashes[c.idx]:
                                c.done = True
                                result[c.idx] = data
                                self._release(load, c)
                                self.events.emit("download_progress", {
                                    "file": manifest.name, "chunk": c.idx,
                                    "from": c.source, "done": len(result),
                                    "total": len(indices), "label": label})
                                continue
                            reason = "bad hash"
                            c.reset_data()
                        elif c.nacked:
                            reason = "NACK"
                        elif c.source not in alive:
                            reason = "source dead"
                        elif t - c.last_progress > config.REQUEST_STALL:
                            reason = "stalled"
                        else:
                            continue
                        self._failover(job, c, reason, load, label)
                    if len(result) == len(indices):
                        break
                    if t > deadline:
                        raise DownloadError("timeout: %d/%d chunks"
                                            % (len(result), len(indices)))
                    # ---- 2. assign pending chunks
                    for c in sorted(job.chunks.values(), key=lambda c: c.idx):
                        if c.done or c.source:
                            continue
                        cands = self._candidates(fid, c.idx, c.failed | {self.self_id})
                        if not cands:
                            local = self.store.get(fid, c.idx)
                            if local is not None and \
                                    sha256_hex(local) == manifest.chunk_hashes[c.idx]:
                                c.done = True             # only copy left is ours
                                result[c.idx] = local
                                continue
                            if c.failed and t - c.last_progress > 1.0:
                                c.failed.clear()          # every replica failed:
                                c.attempts += 1           # wait 1 s, try them again
                                c.last_progress = t
                            continue
                        if max_sources:
                            preferred = [p for p in cands if p in used]
                            if len(used) >= max_sources and preferred:
                                cands = preferred
                        free = [p for p in cands
                                if load.get(p, 0) < config.MAX_OUTSTANDING_PER_PEER]
                        if not free:
                            continue
                        # least loaded first, ties broken by candidate order
                        src = min(free, key=lambda p: (load.get(p, 0), cands.index(p)))
                        c.source = src
                        c.nacked = False
                        c.last_progress = t
                        load[src] = load.get(src, 0) + 1
                        if src not in used:
                            used.append(src)
                        requests.append((src, c.idx, c.first_missing()))
                    if not requests:
                        self._cond.wait(0.05)
                for src, idx, off in requests:
                    self.t.send(src, config.STREAM_CONTROL, P.CHUNK_REQUEST, P.pack_json({
                        "file_id": fid, "index": idx, "offset": off,
                        "stream": config.STREAM_DATA_BASE + idx % config.DATA_STREAMS}))
        finally:
            with self._cond:
                self._jobs.remove(job)
        job.sources_used = used
        return result, job

    def _release(self, load, c):
        if c.source is not None:
            load[c.source] = max(0, load.get(c.source, 0) - 1)

    def _failover(self, job, c, reason, load, label):
        old = c.source
        self._release(load, c)
        c.failed.add(old)
        c.source = None
        log.info("%s %s chunk %d: %s from %s, switching replica (have %d/%d bytes)",
                 label, job.m.name, c.idx, reason, pid_str(old), c.received, c.length)
        if self.metrics:
            # a NACK just means "wrong guess, try the next holder"; only real
            # source failures (dead / stalled / corrupt) count as failovers
            self.metrics.log("nack" if reason == "NACK" else "failover",
                             file=job.m.name, chunk=c.idx,
                             reason=reason, old_source=pid_str(old),
                             resume_offset=c.first_missing())
        self.events.emit("download_progress", {"file": job.m.name, "chunk": c.idx,
                                               "failover": reason, "from": old,
                                               "label": label})

    # ================= public API =================
    def download(self, manifest, out_dir=None, use_local=True, max_sources=None,
                 timeout=300.0):
        """Download a whole file, verify it, write it to out_dir. Returns info."""
        t0 = now()
        chunks, job = self._fetch(manifest, None, use_local, max_sources, timeout,
                                  "download")
        out_dir = out_dir or self.out_dir
        os.makedirs(out_dir, exist_ok=True)
        path = os.path.join(out_dir, manifest.name)
        data = b"".join(chunks[i] for i in range(manifest.num_chunks))
        if sha256_hex(data) != manifest.file_hash:
            raise DownloadError("whole-file hash mismatch")
        with open(path, "wb") as f:
            f.write(data)
        secs = max(now() - t0, 1e-6)
        info = {"path": path, "seconds": secs, "size": manifest.size,
                "mbps": manifest.size / secs / 1e6,
                "sources": {pid_str(p): b for p, b in job.bytes_from.items()},
                "num_sources": len(job.bytes_from)}
        if self.metrics:
            self.metrics.log("download_done", file=manifest.name, size=manifest.size,
                             seconds=round(secs, 3), mbps=round(info["mbps"], 3),
                             sources=len(job.bytes_from))
        log.info("downloaded %s in %.2fs (%.2f MB/s) from %d peers", manifest.name,
                 secs, info["mbps"], len(job.bytes_from))
        return info

    def fetch_chunk(self, manifest, idx, timeout=60.0):
        """Fetch one chunk from the network (used by rebalancing)."""
        try:
            return self.fetch(manifest, [idx], use_local=False, timeout=timeout,
                              label="rebalance").get(idx)
        except DownloadError as exc:
            log.warning("rebalance fetch %s/%d failed: %s", manifest.file_id, idx, exc)
            return None
