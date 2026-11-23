import os
import tempfile
import unittest

from p2p import manifest as M


class TestManifest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.path = os.path.join(self.tmp, "f.bin")
        self.data = os.urandom(2 * 1024 * 1024 + 1234)   # 2 MB + a bit
        with open(self.path, "wb") as f:
            f.write(self.data)

    def test_build(self):
        m = M.build_manifest(self.path, owner=42, chunk_size=256 * 1024)
        self.assertEqual(m.size, len(self.data))
        self.assertEqual(m.num_chunks, 9)                # 8 full + 1 partial
        self.assertEqual(m.chunk_length(8), 1234)
        self.assertEqual(len(m.file_id), 16)
        self.assertEqual(m.owner, 42)

    def test_verify(self):
        m = M.build_manifest(self.path, chunk_size=256 * 1024)
        for idx, chunk in M.iter_chunks(self.path, 256 * 1024):
            self.assertTrue(M.verify_chunk(m, idx, chunk))
        self.assertFalse(M.verify_chunk(m, 0, b"x" * (256 * 1024)))
        self.assertTrue(M.verify_file(m, self.path))

    def test_dict_roundtrip(self):
        m = M.build_manifest(self.path)
        self.assertEqual(M.Manifest.from_dict(m.to_dict()), m)


if __name__ == "__main__":
    unittest.main()
