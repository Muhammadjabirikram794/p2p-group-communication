import time
import unittest

from p2p.peertable import PeerTable, SUSPECT


class TestPeerTable(unittest.TestCase):
    def test_upsert_and_find(self):
        t = PeerTable()
        self.assertTrue(t.upsert(0xAB, ("1.2.3.4", 5), "alice"))
        self.assertFalse(t.upsert(0xAB, ("1.2.3.4", 5)))
        self.assertEqual(t.find("ALICE"), 0xAB)
        self.assertEqual(t.find("000000a"), 0xAB)
        self.assertEqual(t.get(0xAB).name, "alice")

    def test_failure_detector(self):
        t = PeerTable(suspect_timeout=0.05, dead_timeout=0.15)
        t.upsert(1, ("h", 1)); t.upsert(2, ("h", 2))
        time.sleep(0.08)
        t.touch(2)
        suspects, dead = t.check()
        self.assertEqual((suspects, dead), ([1], []))
        self.assertEqual(t.get(1).state, SUSPECT)
        time.sleep(0.1)
        t.touch(2)
        suspects, dead = t.check()
        self.assertEqual(dead, [1])
        self.assertEqual(t.ids(), [2])


if __name__ == "__main__":
    unittest.main()
