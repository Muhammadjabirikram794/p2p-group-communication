import unittest

from p2p import config, protocol as P


class TestProtocol(unittest.TestCase):
    def test_header_size(self):
        self.assertEqual(P._HEADER.size, 28)
        self.assertEqual(config.HEADER_SIZE, 28)

    def test_roundtrip(self):
        pkt = P.Packet(P.CHAT, 0xDEADBEEF, stream=1, seq=7, ack=3, window=100,
                       flags=P.FLAG_RELIABLE | P.FLAG_EOM, epoch=9,
                       payload=b"hello")
        out = P.decode(P.encode(pkt))
        self.assertEqual(out, pkt)

    def test_big_endian_layout(self):
        raw = P.encode(P.Packet(P.HELLO, 1, seq=0x01020304))
        self.assertEqual(raw[0:2], b"\x50\x32")        # magic
        self.assertEqual(raw[2], 1)                     # version
        self.assertEqual(raw[3], P.HELLO)
        self.assertEqual(raw[12:16], b"\x01\x02\x03\x04")  # seq, big-endian

    def test_corruption_detected(self):
        raw = bytearray(P.encode(P.Packet(P.CHAT, 5, payload=b"abc")))
        raw[-1] ^= 0xFF
        with self.assertRaises(P.ProtocolError):
            P.decode(bytes(raw))

    def test_bad_magic_and_version(self):
        raw = bytearray(P.encode(P.Packet(P.CHAT, 5)))
        bad = bytearray(raw); bad[0] = 0
        with self.assertRaises(P.ProtocolError):
            P.decode(bytes(bad))
        bad = bytearray(raw); bad[2] = 99
        with self.assertRaises(P.ProtocolError):
            P.decode(bytes(bad))

    def test_payload_limit(self):
        with self.assertRaises(P.ProtocolError):
            P.encode(P.Packet(P.CHAT, 1, payload=b"x" * (config.MAX_PAYLOAD + 1)))
        P.encode(P.Packet(P.CHAT, 1, payload=b"x" * config.MAX_PAYLOAD))

    def test_chunk_payload(self):
        fid = "0123456789abcdef"
        blob = P.pack_chunk(fid, 3, 32768, 262144, b"data")
        self.assertEqual(P.unpack_chunk(blob), (fid, 3, 32768, 262144, b"data"))

    def test_json(self):
        self.assertEqual(P.unpack_json(P.pack_json({"a": [1, 2]})), {"a": [1, 2]})
        with self.assertRaises(P.ProtocolError):
            P.unpack_json(b"\xff{")


if __name__ == "__main__":
    unittest.main()
