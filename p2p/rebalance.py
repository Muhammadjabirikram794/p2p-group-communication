"""
rebalance.py - Keep every chunk on its R HRW holders as peers come and go.

Runs on every peer in its own thread, woken by peer_joined / peer_left /
file_added events and every REBALANCE_INTERVAL seconds (anti-entropy).

For every chunk of every known file:
    desired = HRW holders over the CURRENT member list (incl. ourselves)
    * we are desired but don't have it  -> fetch it from a replica
                                           (join takeover / re-replication)
    * we have it but are not desired    -> delete it, but ONLY when all the
                                           desired holders are alive and
                                           confirmed to have it (never lose data)

Because HRW is deterministic every peer reaches the same decisions without
coordination. A chunk is only lost if all R replicas die before re-replication.
"""

import logging
import threading

from . import config, placement
from .util import pid_str

log = logging.getLogger("rebalance")


class Rebalancer:
    def __init__(self, self_id, membership, catalog, store, storage, downloader,
                 events, metrics=None, replication=config.REPLICATION,
                 interval=config.REBALANCE_INTERVAL, settle=1.0):
        self.self_id = self_id
        self.members = membership
        self.catalog = catalog
        self.store = store
        self.storage = storage
        self.downloader = downloader
        self.metrics = metrics
        self.replication = replication
        self.interval = interval
        self.settle = settle            # wait for the membership to settle
        self._wake = threading.Event()
        self._running = False
        self._thread = None
        self.fetched = 0
        self.dropped = 0
        for ev in ("peer_joined", "peer_left", "file_added"):
            events.on(ev, lambda *_: self._wake.set())

    def start(self):
        self._running = True
        self._thread = threading.Thread(target=self._loop, daemon=True,
                                        name="rebalance-%s" % pid_str(self.self_id))
        self._thread.start()

    def stop(self):
        self._running = False
        self._wake.set()

    def _loop(self):
        while self._running:
            self._wake.wait(self.interval)
            self._wake.clear()
            if not self._running:
                return
            threading.Event().wait(self.settle)    # batch bursts of events
            try:
                self.run_once()
            except Exception:
                log.exception("rebalance failed")

    def run_once(self):
        members = self.members.members()
        if len(members) < 2:
            return
        alive = set(members)
        for m in self.catalog.files():
            if self.catalog.age(m.file_id) < config.MANIFEST_GRACE:
                self._wake.set()                   # look again later
                continue
            plan = placement.placement_for_file(m.file_id, m.num_chunks, members,
                                                self.replication)
            for idx in range(m.num_chunks):
                if not self._running:
                    return
                desired = plan[idx]
                have = self.store.has(m.file_id, idx)
                if self.self_id in desired and not have:
                    data = self.downloader.fetch_chunk(m, idx)
                    if data is not None:
                        self.storage.keep_chunk(m.file_id, idx, data)
                        self.fetched += 1
                        log.info("took over chunk %s/%d", m.name, idx)
                        if self.metrics:
                            self.metrics.log("rebalance_fetch", file=m.name, chunk=idx)
                elif have and self.self_id not in desired:
                    holders = self.catalog.holders(m.file_id, idx) & alive
                    others = [p for p in desired if p != self.self_id]
                    if all(p in holders for p in others) and \
                            len(holders - {self.self_id}) >= self.replication:
                        self.storage.drop_chunk(m.file_id, idx)
                        self.dropped += 1
                        log.info("handed over chunk %s/%d", m.name, idx)
                        if self.metrics:
                            self.metrics.log("rebalance_drop", file=m.name, chunk=idx)
