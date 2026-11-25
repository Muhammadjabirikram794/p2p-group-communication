import os
import tempfile
import unittest

from p2p.chunkstore import ChunkStore


class TestChunkStore(unittest.TestCase):
    def test_put_get_delete(self):
        root = os.path.join(tempfile.mkdtemp(), "store")
        s = ChunkStore(root)
        s.put("aa" * 8, 0, b"hello")
        s.put("aa" * 8, 3, b"world!")
        self.assertTrue(s.has("aa" * 8, 0))
        self.assertEqual(s.get("aa" * 8, 3), b"world!")
        self.assertEqual(s.chunks_of("aa" * 8), [0, 3])
        self.assertEqual(s.bytes_stored, 11)
        s.delete("aa" * 8, 0)
        self.assertIsNone(s.get("aa" * 8, 0))
        self.assertEqual(s.list(), [("aa" * 8, 3)])

    def test_wipe_on_start(self):
        root = os.path.join(tempfile.mkdtemp(), "store")
        ChunkStore(root).put("bb" * 8, 1, b"x")
        self.assertEqual(ChunkStore(root).list(), [])


if __name__ == "__main__":
    unittest.main()
