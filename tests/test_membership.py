"""Membership over real sockets on 127.0.0.1 using bootstrap (no multicast)."""
import threading
import time
import unittest

from p2p.events import EventBus
from p2p.membership import Membership
from p2p.peertable import PeerTable
from p2p.transport import Transport


def make(pid, name, bootstrap=()):
    t = Transport(pid, port=0, bind_ip="127.0.0.1")
    bus = EventBus()
    m = Membership(pid, name, t, bus, PeerTable(0.6, 1.0), bootstrap=bootstrap,
                   multicast=False, heartbeat=0.2)
    return t, m, bus


def wait_for(cond, timeout=10.0):
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return True
        time.sleep(0.05)
    return False


class TestMembership(unittest.TestCase):
    def setUp(self):
        self.nodes = []

    def tearDown(self):
        for t, m, _ in self.nodes:
            m.stop(graceful=False)
            t.stop()

    def start_group(self, n):
        t0, m0, b0 = make(1, "n1")
        t0.start(); m0.start()
        self.nodes.append((t0, m0, b0))
        boot = [("127.0.0.1", t0.port)]
        for i in range(2, n + 1):
            t, m, b = make(i, "n%d" % i, boot)
            t.start(); m.start()
            self.nodes.append((t, m, b))
        return self.nodes

    def test_everyone_finds_everyone(self):
        nodes = self.start_group(4)
        ok = wait_for(lambda: all(len(m.peers()) == 3 for _, m, _ in nodes))
        self.assertTrue(ok, [m.peers() for _, m, _ in nodes])
        self.assertEqual(nodes[3][1].name_of(1), "n1")

    def test_bootstrap_not_needed_after_join(self):
        nodes = self.start_group(4)
        wait_for(lambda: all(len(m.peers()) == 3 for _, m, _ in nodes))
        t0, m0, _ = nodes[0]
        m0.stop(graceful=True); t0.stop()          # bootstrap peer leaves
        others = nodes[1:]
        self.assertTrue(wait_for(lambda: all(m.peers() == sorted(
            [x[1].self_id for x in others if x[1] is not m]) for _, m, _ in others), 5))
        t5, m5, _ = make(5, "n5", [("127.0.0.1", nodes[1][0].port)])  # join via n2
        t5.start(); m5.start()
        self.nodes.append((t5, m5, None))
        self.assertTrue(wait_for(lambda: len(m5.peers()) == 3))

    def test_crash_detected(self):
        nodes = self.start_group(3)
        wait_for(lambda: all(len(m.peers()) == 2 for _, m, _ in nodes))
        left = []
        nodes[0][2].on("peer_left", left.append)
        t3, m3, _ = nodes[2]
        m3.stop(graceful=False); t3.stop()         # crash, no LEAVE
        self.assertTrue(wait_for(lambda: left == [3], 5))

    def test_graceful_leave_is_fast(self):
        nodes = self.start_group(3)
        wait_for(lambda: all(len(m.peers()) == 2 for _, m, _ in nodes))
        left = threading.Event()
        nodes[0][2].on("peer_left", lambda pid: left.set())
        t0 = time.time()
        nodes[2][1].stop(graceful=True)
        self.assertTrue(left.wait(2))
        self.assertLess(time.time() - t0, 1.5)     # much faster than DEAD_TIMEOUT


if __name__ == "__main__":
    unittest.main()
