"""
chunkstore.py - Local on-disk storage of the chunks this peer is responsible for.

Layout:   <root>/<file_id>_<index>.chunk
The folder is wiped at start-up: a peer that restarts gets a new peer id and
re-receives its share through rebalancing.
"""

import os
import shutil
import threading


class ChunkStore:
    def __init__(self, root: str, wipe: bool = True):
        self.root = root
        if wipe and os.path.isdir(root):
            shutil.rmtree(root)
        os.makedirs(root, exist_ok=True)
        self._lock = threading.Lock()
        self._index = set()          # {(file_id, idx)}
        self.bytes_stored = 0

    def _path(self, file_id: str, idx: int) -> str:
        return os.path.join(self.root, "%s_%d.chunk" % (file_id, idx))

    def put(self, file_id: str, idx: int, data: bytes):
        path = self._path(file_id, idx)
        tmp = path + ".tmp"
        with open(tmp, "wb") as f:
            f.write(data)
        os.replace(tmp, path)            # atomic: never a half-written chunk
        with self._lock:
            if (file_id, idx) not in self._index:
                self._index.add((file_id, idx))
                self.bytes_stored += len(data)

    def get(self, file_id: str, idx: int):
        with self._lock:
            if (file_id, idx) not in self._index:
                return None
        try:
            with open(self._path(file_id, idx), "rb") as f:
                return f.read()
        except FileNotFoundError:
            return None

    def has(self, file_id: str, idx: int) -> bool:
        with self._lock:
            return (file_id, idx) in self._index

    def delete(self, file_id: str, idx: int):
        with self._lock:
            if (file_id, idx) not in self._index:
                return
            self._index.discard((file_id, idx))
        path = self._path(file_id, idx)
        try:
            self.bytes_stored -= os.path.getsize(path)
            os.remove(path)
        except FileNotFoundError:
            pass

    def list(self):
        with self._lock:
            return sorted(self._index)

    def chunks_of(self, file_id: str):
        with self._lock:
            return sorted(i for f, i in self._index if f == file_id)
