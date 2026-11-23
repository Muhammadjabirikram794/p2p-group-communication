"""
config.py - All protocol constants and tunable parameters in one place.

Every peer MUST use the same values for the "wire" constants (magic, version,
header size, message types live in protocol.py) or they cannot interoperate.
The timing values can be tuned per run (see run_peer.py arguments).
"""

# ---------------- Wire-level constants ----------------
MAGIC = 0x5032              # "P2" - first 2 bytes of every datagram
PROTOCOL_VERSION = 1        # bump when the header/message format changes
HEADER_SIZE = 28            # bytes, see protocol.HEADER_FORMAT
MAX_DATAGRAM = 1200         # never send more than this (avoids IP fragmentation)
MAX_PAYLOAD = MAX_DATAGRAM - HEADER_SIZE   # 1172 bytes of payload per segment

# ---------------- Discovery ----------------
MULTICAST_GROUP = "239.255.42.99"
MULTICAST_PORT = 50000
DEFAULT_PORT = 50001
HELLO_REPEAT = 3            # HELLO is sent this many times at start-up
HELLO_INTERVAL = 5.0        # then re-sent every N seconds (lets partitions merge)

# ---------------- Membership / failure detection ----------------
HEARTBEAT_INTERVAL = 1.0    # seconds between heartbeats
SUSPECT_TIMEOUT = 3.0       # no heartbeat for this long -> SUSPECT
DEAD_TIMEOUT = 5.0          # no heartbeat for this long -> DEAD (crashed)

# ---------------- Reliable transport ----------------
STREAM_CONTROL = 0          # membership / storage control messages
STREAM_CHAT = 1             # chat (own stream => never blocked by file data)
STREAM_DATA_BASE = 2        # 2..(2+DATA_STREAMS-1) used for chunk transfers
DATA_STREAMS = 6

SEND_WINDOW = 64            # max un-ACKed segments per stream (sender side)
RECV_WINDOW = 256           # receiver buffer (segments) advertised in 'window'
SACK_BITS = 32              # SACK bitmap covers ack+1 .. ack+32
INITIAL_RTO = 0.5           # seconds, before any RTT sample (RFC 6298 uses 1s)
MIN_RTO = 0.1
MAX_RTO = 2.0
MAX_RETRIES = 10            # after this many timeouts the stream is reset
FAST_RETX_THRESHOLD = 3     # SACKed segments above a hole before fast retransmit
TIMER_TICK = 0.01           # retransmission timer granularity (s)

# ---------------- Chat ----------------
CHAT_HISTORY = 100          # messages kept for HISTORY_RESP

# ---------------- Storage / download ----------------
CHUNK_SIZE = 256 * 1024     # 256 KB chunks  (a 2 MB file -> 8 chunks)
PIECE_SIZE = 32 * 1024      # chunks are sent as 32 KB CHUNK_DATA pieces
REPLICATION = 2             # every chunk lives on at least 2 peers
MAX_OUTSTANDING_PER_PEER = 2   # parallel chunk requests to one source
REQUEST_STALL = 3.0         # no data for this long -> switch replica
REBALANCE_INTERVAL = 10.0   # periodic anti-entropy check
MANIFEST_GRACE = 5.0        # don't rebalance a file announced < N s ago
