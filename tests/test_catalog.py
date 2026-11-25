import unittest

from p2p.catalog import Catalog
from p2p.manifest import Manifest


def man(fid="0011223344556677", name="a.bin"):
    return Manifest(fid, name, 600, 256, 3, ["h0", "h1", "h2"], "fh", 7)


class TestCatalog(unittest.TestCase):
    def test_locations(self):
        c = Catalog()
        c.add_manifest(man())
        c.set_location("0011223344556677", 0, 5)
        c.set_location("0011223344556677", 0, 6)
        self.assertEqual(c.holders("0011223344556677", 0), {5, 6})
        c.remove_peer(5)
        self.assertEqual(c.holders("0011223344556677", 0), {6})

    def test_wire_roundtrip(self):
        a = Catalog()
        a.add_manifest(man())
        a.set_location("0011223344556677", 2, 9)
        b = Catalog()
        new = b.merge_wire(a.to_wire())
        self.assertEqual(new[0], man())
        self.assertEqual(b.holders("0011223344556677", 2), {9})
        self.assertEqual(b.merge_wire(a.to_wire()), [])   # idempotent

    def test_find(self):
        c = Catalog()
        c.add_manifest(man())
        self.assertIsNotNone(c.find("a.bin"))
        self.assertIsNotNone(c.find("0011"))
        self.assertIsNone(c.find("zzz"))


if __name__ == "__main__":
    unittest.main()
