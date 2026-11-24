import unittest

from p2p import placement as PL


class TestPlacement(unittest.TestCase):
    peers = [0x11111111, 0x22222222, 0x33333333, 0x44444444]

    def test_two_distinct_holders(self):
        plan = PL.placement_for_file("00ff00ff00ff00ff", 8, self.peers)
        for idx, hs in plan.items():
            self.assertEqual(len(hs), 2)
            self.assertEqual(len(set(hs)), 2)

    def test_deterministic(self):
        a = PL.placement_for_file("abcdabcdabcdabcd", 8, self.peers)
        b = PL.placement_for_file("abcdabcdabcdabcd", 8, list(reversed(self.peers)))
        self.assertEqual(a, b)

    def test_no_peer_holds_whole_file(self):
        for n in range(200):
            fid = "%016x" % (n * 7919)
            plan = PL.placement_for_file(fid, 3, self.peers[:3])
            for p in self.peers[:3]:
                self.assertFalse(all(p in plan[i] for i in plan), fid)

    def test_minimal_movement_on_join(self):
        before = PL.placement_for_file("1234123412341234", 64, self.peers)
        new = self.peers + [0x55555555]
        after = PL.placement_for_file("1234123412341234", 64, new)
        moved = sum(1 for i in before if set(before[i]) != set(after[i])
                    and 0x55555555 not in after[i])
        self.assertEqual(moved, 0)   # chunks only move TO the new peer

    def test_can_place(self):
        self.assertFalse(PL.can_place(2))
        self.assertTrue(PL.can_place(3))


if __name__ == "__main__":
    unittest.main()
