"""
lossy.py - Packet-loss / duplication simulator.

Wraps a UDP socket so that sendto() randomly drops or duplicates datagrams.
This lets us test reliability on a perfect LAN/localhost and produce the
"throughput vs. loss rate" graphs.

    sock = LossySocket(real_sock, loss=0.10, dup=0.02)   # 10 % loss, 2 % dup
"""

import random
import threading


class LossySocket:
    def __init__(self, sock, loss: float = 0.0, dup: float = 0.0, seed=None):
        self._sock = sock
        self.loss = loss
        self.dup = dup
        self._rng = random.Random(seed)
        self._lock = threading.Lock()
        self.stats = {"sent": 0, "dropped": 0, "duplicated": 0}

    def sendto(self, data, addr):
        with self._lock:
            drop = self.loss > 0 and self._rng.random() < self.loss
            dup = (not drop) and self.dup > 0 and self._rng.random() < self.dup
            self.stats["sent"] += 1
            if drop:
                self.stats["dropped"] += 1
            if dup:
                self.stats["duplicated"] += 1
        if drop:
            return len(data)          # pretend it was sent
        n = self._sock.sendto(data, addr)
        if dup:
            self._sock.sendto(data, addr)
        return n

    def __getattr__(self, name):
        # recvfrom, bind, close, setsockopt, fileno ... go to the real socket
        return getattr(self._sock, name)
