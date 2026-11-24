import socket
import unittest

from p2p.rtt import RttEstimator
from p2p.lossy import LossySocket


class TestRtt(unittest.TestCase):
    def test_first_sample(self):
        r = RttEstimator(min_rto=0.0, max_rto=10)
        r.sample(0.1)
        self.assertAlmostEqual(r.srtt, 0.1)
        self.assertAlmostEqual(r.rttvar, 0.05)
        self.assertAlmostEqual(r.rto, 0.1 + 4 * 0.05)

    def test_converges_and_clamps(self):
        r = RttEstimator(min_rto=0.1, max_rto=2.0)
        for _ in range(50):
            r.sample(0.001)
        self.assertEqual(r.rto, 0.1)          # clamped to MIN_RTO
        self.assertEqual(r.timeout_for(10), 2.0)   # back-off capped

    def test_backoff(self):
        r = RttEstimator(initial_rto=0.2, max_rto=5)
        self.assertAlmostEqual(r.timeout_for(0), 0.2)
        self.assertAlmostEqual(r.timeout_for(2), 0.8)


class TestLossy(unittest.TestCase):
    def test_loss_rate(self):
        rx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        rx.bind(("127.0.0.1", 0))
        tx = LossySocket(socket.socket(socket.AF_INET, socket.SOCK_DGRAM),
                         loss=0.3, seed=1)
        for _ in range(2000):
            tx.sendto(b"x", rx.getsockname())
        ratio = tx.stats["dropped"] / tx.stats["sent"]
        self.assertTrue(0.25 < ratio < 0.35, ratio)
        tx.close(); rx.close()


if __name__ == "__main__":
    unittest.main()
