"""
protocol.py - Wire format of the P2P-UDP protocol (version 1).

Every datagram = 28-byte header + payload (<= 1172 bytes).
All multi-byte fields are BIG-ENDIAN (network byte order).

 0               1               2               3
 0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7
+-------------------------------+---------------+---------------+
|         magic (0x5032)        |   version     |     type      |
+---------------+---------------+-------------------------------+
|     flags     |    epoch      |          stream_id            |
+---------------+---------------+-------------------------------+
|                         src_peer_id                           |
+---------------------------------------------------------------+
|                         seq                                   |
+---------------------------------------------------------------+
|                         ack                                   |
+-------------------------------+-------------------------------+
|           window              |         payload_len           |
+-------------------------------+-------------------------------+
|                         checksum (CRC32)                      |
+---------------------------------------------------------------+
|                         payload ...                           |
"""

import json
import struct
import zlib
from dataclasses import dataclass

from . import config

# ---------------- Header ----------------
#  H magic | B version | B type | B flags | B epoch | H stream_id |
#  I src_peer_id | I seq | I ack | H window | H payload_len | I checksum
HEADER_FORMAT = "!HBBBBHIIIHHI"
_HEADER = struct.Struct(HEADER_FORMAT)
assert _HEADER.size == config.HEADER_SIZE

# ---------------- Flags ----------------
FLAG_ACK = 0x01        # packet carries a valid 'ack' field (pure ACK)
FLAG_RELIABLE = 0x02   # packet must be ACKed and delivered in order
FLAG_EOM = 0x04        # last segment of an application message
FLAG_SACK = 0x08       # payload holds a 32-bit SACK bitmap

# ---------------- Message types ----------------
HELLO = 0x01           # discovery (multicast or unicast to bootstrap)  [unreliable]
WELCOME = 0x02         # reply to HELLO with membership list            [reliable]
HEARTBEAT = 0x03       # liveness + gossip of known peers               [unreliable]
LEAVE = 0x04           # graceful departure                             [reliable]
CHAT = 0x10            # group or private chat message                  [reliable]
HISTORY_REQ = 0x11     # ask for recent chat history                    [reliable]
HISTORY_RESP = 0x12    # recent chat history                            [reliable]
FILE_ANNOUNCE = 0x20   # new shared file manifest + chunk locations     [reliable]
FILE_LIST_REQ = 0x21   # joining peer asks for all files                [reliable]
FILE_LIST = 0x22       # all manifests + chunk locations                [reliable]
STORE_CHUNK = 0x23     # push a chunk to a holder (binary)              [reliable]
STORE_ACK = 0x24       # holder confirms it stored the chunk            [reliable]
CHUNK_REQUEST = 0x25   # ask a holder for (part of) a chunk             [reliable]
CHUNK_DATA = 0x26      # one 32 KB piece of a chunk (binary)            [reliable]
CHUNK_NACK = 0x27      # "I do not have that chunk"                     [reliable]
CHUNK_LOCATION = 0x28  # "I now hold / dropped chunk X"                 [reliable]
ACK = 0x30             # transport acknowledgement (+SACK)              [-]

TYPE_NAMES = {
    HELLO: "HELLO", WELCOME: "WELCOME", HEARTBEAT: "HEARTBEAT", LEAVE: "LEAVE",
    CHAT: "CHAT", HISTORY_REQ: "HISTORY_REQ", HISTORY_RESP: "HISTORY_RESP",
    FILE_ANNOUNCE: "FILE_ANNOUNCE", FILE_LIST_REQ: "FILE_LIST_REQ",
    FILE_LIST: "FILE_LIST", STORE_CHUNK: "STORE_CHUNK", STORE_ACK: "STORE_ACK",
    CHUNK_REQUEST: "CHUNK_REQUEST", CHUNK_DATA: "CHUNK_DATA",
    CHUNK_NACK: "CHUNK_NACK", CHUNK_LOCATION: "CHUNK_LOCATION", ACK: "ACK",
}


class ProtocolError(Exception):
    """Raised for malformed, corrupted or foreign datagrams."""


@dataclass
class Packet:
    type: int
    src_id: int
    stream: int = 0
    seq: int = 0
    ack: int = 0
    window: int = 0
    flags: int = 0
    epoch: int = 0
    payload: bytes = b""

    def name(self):
        return TYPE_NAMES.get(self.type, hex(self.type))


def encode(pkt: Packet) -> bytes:
    """Packet -> bytes. CRC32 is computed over header(with checksum=0)+payload."""
    if len(pkt.payload) > config.MAX_PAYLOAD:
        raise ProtocolError("payload too large: %d" % len(pkt.payload))
    fields = [config.MAGIC, config.PROTOCOL_VERSION, pkt.type, pkt.flags,
              pkt.epoch & 0xFF, pkt.stream, pkt.src_id, pkt.seq & 0xFFFFFFFF,
              pkt.ack & 0xFFFFFFFF, max(0, min(pkt.window, 0xFFFF)),
              len(pkt.payload)]
    raw = _HEADER.pack(*fields, 0) + pkt.payload
    crc = zlib.crc32(raw) & 0xFFFFFFFF
    return _HEADER.pack(*fields, crc) + pkt.payload


def decode(data: bytes) -> Packet:
    """bytes -> Packet. Raises ProtocolError if anything is wrong."""
    if len(data) < config.HEADER_SIZE:
        raise ProtocolError("short datagram")
    (magic, version, mtype, flags, epoch, stream, src, seq, ack, window,
     plen, crc) = _HEADER.unpack_from(data)
    if magic != config.MAGIC:
        raise ProtocolError("bad magic")
    if version != config.PROTOCOL_VERSION:
        raise ProtocolError("unsupported version %d" % version)
    if plen != len(data) - config.HEADER_SIZE:
        raise ProtocolError("length mismatch")
    zeroed = _HEADER.pack(magic, version, mtype, flags, epoch, stream, src,
                          seq, ack, window, plen, 0) + data[config.HEADER_SIZE:]
    if zlib.crc32(zeroed) & 0xFFFFFFFF != crc:
        raise ProtocolError("checksum mismatch")
    return Packet(mtype, src, stream, seq, ack, window, flags, epoch,
                  bytes(data[config.HEADER_SIZE:]))


# ---------------- Payload helpers ----------------
# Control messages carry UTF-8 JSON objects. Chunk data uses a binary layout.

def pack_json(obj) -> bytes:
    return json.dumps(obj, separators=(",", ":")).encode("utf-8")


def unpack_json(payload: bytes):
    try:
        return json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise ProtocolError("bad JSON payload: %s" % exc)


# STORE_CHUNK / CHUNK_DATA binary payload:
#   file_id (8 bytes) | chunk_index (uint32) | offset (uint32) |
#   chunk_length (uint32) | data ...
_CHUNK_HDR = struct.Struct("!8sIII")
CHUNK_HDR_SIZE = _CHUNK_HDR.size   # 20


def pack_chunk(file_id_hex: str, index: int, offset: int, chunk_len: int,
               data: bytes) -> bytes:
    return _CHUNK_HDR.pack(bytes.fromhex(file_id_hex), index, offset,
                           chunk_len) + data


def unpack_chunk(payload: bytes):
    """-> (file_id_hex, index, offset, chunk_len, data)"""
    if len(payload) < CHUNK_HDR_SIZE:
        raise ProtocolError("short chunk payload")
    fid, idx, off, clen = _CHUNK_HDR.unpack_from(payload)
    return fid.hex(), idx, off, clen, payload[CHUNK_HDR_SIZE:]


def sack_bitmap_pack(bits: int) -> bytes:
    return struct.pack("!I", bits & 0xFFFFFFFF)


def sack_bitmap_unpack(payload: bytes) -> int:
    return struct.unpack("!I", payload[:4])[0] if len(payload) >= 4 else 0
