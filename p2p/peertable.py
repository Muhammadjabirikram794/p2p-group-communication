"""
peertable.py - Thread-safe table of known peers and their liveness state.

States:   ALIVE  --(no heartbeat SUSPECT_TIMEOUT)-->  SUSPECT
          SUSPECT --(heartbeat)--> ALIVE
          SUSPECT --(no heartbeat DEAD_TIMEOUT)--> removed (reported as dead)
"""

import threading
from dataclasses import dataclass

from . import config
from .util import now

ALIVE = "alive"
SUSPECT = "suspect"


@dataclass
class PeerInfo:
    peer_id: int
    addr: tuple          # (ip, port) of the peer's unicast socket
    name: str
    last_seen: float
    state: str = ALIVE
    joined_at: float = 0.0


class PeerTable:
    def __init__(self, suspect_timeout=config.SUSPECT_TIMEOUT,
                 dead_timeout=config.DEAD_TIMEOUT):
        self._peers = {}
        self._lock = threading.Lock()
        self.suspect_timeout = suspect_timeout
        self.dead_timeout = dead_timeout

    def upsert(self, peer_id: int, addr, name: str = "") -> bool:
        """Add or refresh a peer. Returns True if the peer is NEW."""
        t = now()
        with self._lock:
            p = self._peers.get(peer_id)
            if p is None:
                self._peers[peer_id] = PeerInfo(peer_id, tuple(addr), name, t,
                                                ALIVE, t)
                return True
            p.addr = tuple(addr)
            if name:
                p.name = name
            p.last_seen = t
            p.state = ALIVE
            return False

    def touch(self, peer_id: int) -> bool:
        """Heartbeat from a known peer. Returns False if the peer is unknown."""
        with self._lock:
            p = self._peers.get(peer_id)
            if p is None:
                return False
            p.last_seen = now()
            p.state = ALIVE
            return True

    def remove(self, peer_id: int):
        with self._lock:
            return self._peers.pop(peer_id, None)

    def check(self):
        """Run the failure detector. Returns (newly_suspect_ids, dead_ids).
        Dead peers are removed from the table."""
        t = now()
        suspects, dead = [], []
        with self._lock:
            for pid, p in list(self._peers.items()):
                age = t - p.last_seen
                if age > self.dead_timeout:
                    dead.append(pid)
                    del self._peers[pid]
                elif age > self.suspect_timeout and p.state == ALIVE:
                    p.state = SUSPECT
                    suspects.append(pid)
        return suspects, dead

    def get(self, peer_id: int):
        with self._lock:
            p = self._peers.get(peer_id)
            return None if p is None else PeerInfo(**vars(p))

    def __contains__(self, peer_id):
        with self._lock:
            return peer_id in self._peers

    def ids(self, include_suspect=True):
        with self._lock:
            return sorted(pid for pid, p in self._peers.items()
                          if include_suspect or p.state == ALIVE)

    def snapshot(self):
        with self._lock:
            return [PeerInfo(**vars(p)) for p in self._peers.values()]

    def find(self, text: str):
        """Look a peer up by name (case-insensitive) or hex id prefix."""
        text = text.lower()
        with self._lock:
            for p in self._peers.values():
                if p.name.lower() == text or ("%08x" % p.peer_id).startswith(text):
                    return p.peer_id
        return None
