"""Two real Transport objects on 127.0.0.1 talking through lossy sockets."""
import os
import threading
import time
import unittest

from p2p import config, protocol as P
from p2p.transport import Transport


class Pair:
    def __init__(self, loss=0.0, dup=0.0, rate=0.0):
        self.a = Transport(0xA, port=0, bind_ip="127.0.0.1", loss=loss, dup=dup,
                           rate=rate, seed=1)
        self.b = Transport(0xB, port=0, bind_ip="127.0.0.1", loss=loss, dup=dup,
                           seed=2)
        self.a.add_peer(0xB, ("127.0.0.1", self.b.port))
        self.b.add_peer(0xA, ("127.0.0.1", self.a.port))
        self.got = []
        self.done = threading.Event()

    def start(self, expect):
        def handler(pid, addr, payload):
            self.got.append(payload)
            if len(self.got) >= expect:
                self.done.set()
        self.b.on(P.CHAT, handler)
        self.b.on(P.CHUNK_DATA, handler)
        self.a.start(); self.b.start()

    def stop(self):
        self.a.stop(); self.b.stop()


class TestTransport(unittest.TestCase):
    def _transfer(self, loss, dup=0.0, n=300, size=900):
        pair = Pair(loss, dup)
        msgs = [os.urandom(size) + i.to_bytes(4, "big") for i in range(n)]
        pair.start(len(msgs))
        try:
            for m in msgs:
                pair.a.send(0xB, config.STREAM_CHAT, P.CHAT, m)
            self.assertTrue(pair.done.wait(60), "timeout, got %d" % len(pair.got))
            self.assertEqual(pair.got, msgs)       # in order, no dups, no loss
            return pair
        finally:
            pair.stop()

    def test_no_loss(self):
        self._transfer(0.0)

    def test_20_percent_loss(self):
        pair = self._transfer(0.20)
        self.assertGreater(pair.a.stats["retransmits"] + pair.a.stats["fast_retransmits"], 0)

    def test_duplicates_suppressed(self):
        pair = self._transfer(0.05, dup=0.2)
        self.assertGreater(pair.b.stats["duplicates"], 0)

    def test_large_message_fragmentation(self):
        pair = Pair(loss=0.1)
        blob = os.urandom(1024 * 1024)            # 1 MB -> ~900 segments
        pair.start(1)
        try:
            pair.a.send(0xB, 2, P.CHUNK_DATA, blob)
            self.assertTrue(pair.done.wait(60))
            self.assertEqual(pair.got[0], blob)
        finally:
            pair.stop()

    def test_streams_independent(self):
        """Chat sent after a 2 MB bulk message must arrive before the bulk."""
        pair = Pair(rate=1_000_000)               # 1 MB/s uplink
        order = []
        done = threading.Event()
        pair.b.on(P.CHUNK_DATA, lambda *a: (order.append("bulk"), done.set()))
        pair.b.on(P.CHAT, lambda *a: order.append("chat"))
        pair.a.start(); pair.b.start()
        try:
            pair.a.send(0xB, 2, P.CHUNK_DATA, os.urandom(2 * 1024 * 1024))
            time.sleep(0.1)
            pair.a.send(0xB, config.STREAM_CHAT, P.CHAT, b"hi")
            self.assertTrue(done.wait(30))
            self.assertEqual(order, ["chat", "bulk"])
        finally:
            pair.stop()

    def test_unreachable_peer(self):
        a = Transport(0xA, port=0, bind_ip="127.0.0.1")
        a.add_peer(0xDEAD, ("127.0.0.1", 9))      # nobody listens there
        gone = threading.Event()
        a.on_unreachable(lambda pid: gone.set())
        a.start()
        try:
            a.send(0xDEAD, 0, P.CHAT, b"x")
            self.assertTrue(gone.wait(30))
            self.assertEqual(a.pending(0xDEAD), 0)
        finally:
            a.stop()


if __name__ == "__main__":
    unittest.main()
