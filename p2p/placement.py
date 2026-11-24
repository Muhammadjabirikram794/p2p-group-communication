

import hashlib

from . import config


def score(file_id: str, index: int, peer_id: int) -> bytes:
    return hashlib.sha256(("%s:%d:%08x" % (file_id, index, peer_id)).encode()).digest()


def rank_peers(file_id: str, index: int, peer_ids):
    """All peers ordered from most to least preferred holder of the chunk."""
    return sorted(set(peer_ids), key=lambda p: score(file_id, index, p),
                  reverse=True)


def holders(file_id: str, index: int, peer_ids, r: int = config.REPLICATION):
    return rank_peers(file_id, index, peer_ids)[:r]


def placement_for_file(file_id: str, num_chunks: int, peer_ids,
                       r: int = config.REPLICATION):
    """-> {chunk_index: [holder_ids]} with the 'no complete copy' rule applied."""
    peers = sorted(set(peer_ids))
    plan = {i: holders(file_id, i, peers, r) for i in range(num_chunks)}
    # Feasible only if every peer can be left out of at least one chunk.
    if num_chunks < 2 or len(peers) <= r or num_chunks * (len(peers) - r) < len(peers):
        return plan
    fix = num_chunks - 1                       # fix from the last chunk backwards
    for _ in range(num_chunks * len(peers)):   # bounded, always terminates
        full = [p for p in peers if all(p in plan[i] for i in plan)]
        if not full:
            break
        pid = full[0]
        counts = {p: sum(p in plan[i] for i in plan) for p in peers}
        idx = fix % num_chunks
        fix -= 1
        ranked = rank_peers(file_id, idx, peers)
        # replacement: a peer not yet holding this chunk, least loaded first
        candidates = [p for p in ranked if p not in plan[idx]]
        replacement = min(candidates, key=lambda p: (counts[p], ranked.index(p)))
        plan[idx] = [replacement if p == pid else p for p in plan[idx]]
    return plan


def can_place(num_peers: int, r: int = config.REPLICATION) -> bool:
    """Need at least r+1 peers so that nobody has to hold the whole file."""
    return num_peers >= r + 1
