"""
manifest.py - Splitting a file into fixed-size chunks and describing it.

A manifest is the ".torrent" of our system: it tells every peer what a shared
file looks like so chunks can be located, downloaded and verified.

    file_id      first 8 bytes of SHA-256(file) as 16 hex chars
    name         original file name
    size         bytes
    chunk_size   bytes per chunk (last chunk may be shorter)
    num_chunks   ceil(size / chunk_size)
    chunk_hashes SHA-256 of every chunk (hex) -> integrity check per chunk
    file_hash    SHA-256 of the whole file (hex) -> final integrity check
    owner        peer id of the peer that shared it
"""

import hashlib
import os
from dataclasses import dataclass, field, asdict
from typing import List

from . import config


@dataclass
class Manifest:
    file_id: str
    name: str
    size: int
    chunk_size: int
    num_chunks: int
    chunk_hashes: List[str] = field(default_factory=list)
    file_hash: str = ""
    owner: int = 0

    def chunk_length(self, index: int) -> int:
        """Length of chunk 'index' (the last one is usually shorter)."""
        if index < 0 or index >= self.num_chunks:
            raise IndexError(index)
        if index == self.num_chunks - 1:
            return self.size - index * self.chunk_size
        return self.chunk_size

    def to_dict(self):
        return asdict(self)

    @staticmethod
    def from_dict(d) -> "Manifest":
        return Manifest(d["file_id"], d["name"], int(d["size"]),
                        int(d["chunk_size"]), int(d["num_chunks"]),
                        list(d.get("chunk_hashes", [])), d.get("file_hash", ""),
                        int(d.get("owner", 0)))


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def iter_chunks(path: str, chunk_size: int = config.CHUNK_SIZE):
    """Yield (index, bytes) for every chunk of the file, reading lazily."""
    with open(path, "rb") as f:
        index = 0
        while True:
            data = f.read(chunk_size)
            if not data:
                break
            yield index, data
            index += 1


def build_manifest(path: str, owner: int = 0,
                   chunk_size: int = config.CHUNK_SIZE) -> Manifest:
    size = os.path.getsize(path)
    if size == 0:
        raise ValueError("cannot share an empty file")
    whole = hashlib.sha256()
    hashes = []
    for _, data in iter_chunks(path, chunk_size):
        whole.update(data)
        hashes.append(sha256_hex(data))
    file_hash = whole.hexdigest()
    return Manifest(file_id=file_hash[:16], name=os.path.basename(path),
                    size=size, chunk_size=chunk_size, num_chunks=len(hashes),
                    chunk_hashes=hashes, file_hash=file_hash, owner=owner)


def read_chunk(path: str, index: int, chunk_size: int = config.CHUNK_SIZE) -> bytes:
    with open(path, "rb") as f:
        f.seek(index * chunk_size)
        return f.read(chunk_size)


def verify_chunk(manifest: Manifest, index: int, data: bytes) -> bool:
    return (len(data) == manifest.chunk_length(index)
            and sha256_hex(data) == manifest.chunk_hashes[index])


def verify_file(manifest: Manifest, path: str) -> bool:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest() == manifest.file_hash
