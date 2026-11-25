"""
catalog.py - What files exist in the group and where their chunks are.

    manifests:  file_id -> Manifest
    locations:  (file_id, chunk_index) -> set(peer_id)   (peers that HAVE it)

Locations are learned from FILE_ANNOUNCE, FILE_LIST and CHUNK_LOCATION
messages. A new peer receives the whole catalog in FILE_LIST when it joins.
"""

import threading

from .manifest import Manifest
from .util import now


class Catalog:
    def __init__(self):
        self._lock = threading.Lock()
        self._manifests = {}
        self._added_at = {}
        self._locations = {}

    # ---------- manifests ----------
    def add_manifest(self, m: Manifest) -> bool:
        with self._lock:
            if m.file_id in self._manifests:
                return False
            self._manifests[m.file_id] = m
            self._added_at[m.file_id] = now()
            return True

    def get(self, file_id: str):
        with self._lock:
            return self._manifests.get(file_id)

    def age(self, file_id: str) -> float:
        with self._lock:
            return now() - self._added_at.get(file_id, now())

    def files(self):
        with self._lock:
            return list(self._manifests.values())

    def find(self, text: str):
        """Find a file by exact name, id prefix or unique name prefix."""
        text = text.strip()
        with self._lock:
            ms = list(self._manifests.values())
        for m in ms:
            if m.name == text or m.file_id.startswith(text.lower()):
                return m
        matches = [m for m in ms if m.name.lower().startswith(text.lower())]
        return matches[0] if len(matches) == 1 else None

    # ---------- locations ----------
    def set_location(self, file_id: str, idx: int, peer_id: int, present=True):
        with self._lock:
            s = self._locations.setdefault((file_id, idx), set())
            if present:
                s.add(peer_id)
            else:
                s.discard(peer_id)

    def holders(self, file_id: str, idx: int):
        with self._lock:
            return set(self._locations.get((file_id, idx), set()))

    def remove_peer(self, peer_id: int):
        with self._lock:
            for s in self._locations.values():
                s.discard(peer_id)

    # ---------- wire format (FILE_LIST / FILE_ANNOUNCE) ----------
    def to_wire(self, file_ids=None):
        with self._lock:
            ids = list(self._manifests) if file_ids is None else list(file_ids)
            out = []
            for fid in ids:
                m = self._manifests.get(fid)
                if m is None:
                    continue
                locs = {str(i): sorted(self._locations.get((fid, i), set()))
                        for i in range(m.num_chunks)}
                out.append({"manifest": m.to_dict(), "locations": locs})
            return {"files": out}

    def merge_wire(self, data) -> list:
        """Merge a FILE_LIST / FILE_ANNOUNCE payload. Returns new manifests."""
        new = []
        for entry in data.get("files", []):
            m = Manifest.from_dict(entry["manifest"])
            if self.add_manifest(m):
                new.append(m)
            for idx, pids in entry.get("locations", {}).items():
                for pid in pids:
                    self.set_location(m.file_id, int(idx), int(pid))
        return new
