# P2P-UDP Protocol Specification — Version 1

This document is the contract between peers. Any implementation that follows it
exactly can join a group of our peers. Constants live in `p2p/config.py`, and the
wire format lives in `p2p/protocol.py`.

## 1. Transport basics

* UDP over IPv4. Each peer uses **one unicast UDP socket** (default port 50001)
  for everything, plus a multicast listener for discovery.
* Discovery multicast group: **239.255.42.99 : 50000** (TTL 1, LAN only).
* A datagram is **at most 1200 bytes**: a 28-byte header plus at most 1172 bytes of
  payload. This avoids IP fragmentation.
* All multi-byte integers are **big-endian (network byte order)**.
* Peers are identified by a random non-zero **32-bit peer id**, chosen at
  start-up and never reused. Addresses are learned from packets and from `HELLO`.

## 2. Header (28 bytes)

| Offset | Size | Field        | Description |
|-------:|-----:|--------------|-------------|
| 0      | 2    | magic        | `0x5032` ("P2"). Datagrams with any other value are dropped |
| 2      | 1    | version      | `1`. Datagrams with any other version are dropped |
| 3      | 1    | type         | message type (section 4) |
| 4      | 1    | flags        | bit0 `ACK`, bit1 `RELIABLE`, bit2 `EOM`, bit3 `SACK` |
| 5      | 1    | epoch        | incarnation of the stream (section 3.6) |
| 6      | 2    | stream_id    | 0 control, 1 chat, 2–7 data |
| 8      | 4    | src_peer_id  | sender's peer id |
| 12     | 4    | seq          | segment sequence number in this stream (reliable packets) |
| 16     | 4    | ack          | next expected seq (valid only when `ACK` is set) |
| 20     | 2    | window       | receiver's free buffer, in segments (ACK packets) |
| 22     | 2    | payload_len  | number of payload bytes after the header |
| 24     | 4    | checksum     | CRC-32 (IEEE) of header and payload, computed with this field = 0 |

Python `struct` format: `!HBBBBHIIIHHI`.
The receiver drops a datagram silently if it is shorter than 28 bytes, has the
wrong magic or version, has a length mismatch, or fails the CRC check.

## 3. Reliable delivery (TCP-like, no congestion control)

### 3.1 Streams
Reliability state is kept separately for each **(peer, stream_id)** pair, in each
direction. Each stream has its own sequence space starting at 0, so a loss on
one stream never delays another (no head-of-line blocking between streams).

| stream | use |
|-------:|-----|
| 0 | control: WELCOME, LEAVE, HISTORY_*, FILE_*, STORE_ACK, CHUNK_REQUEST/NACK/LOCATION |
| 1 | CHAT |
| 2–7 | STORE_CHUNK and CHUNK_DATA (stream = 2 + chunk_index mod 6) |

When several streams have data waiting, the sender serves **lower stream ids first**.

### 3.2 Segmentation
An application message of any length is split into segments of at most 1172 bytes.
Every segment has `RELIABLE` set, and the last one also has `EOM`. All segments of
a message carry the message's `type`. The receiver concatenates in-order segments
up to and including the `EOM` segment, then delivers the message. An empty message
is a single segment with `EOM` and no payload.

### 3.3 Acknowledgements
* For every stream that received reliable packets, the receiver answers with an
  `ACK` packet: type `0x30`, flags `ACK` (and `SACK` when needed), same stream_id
  and epoch, `ack` = next expected seq (cumulative), and `window` = free buffer
  slots (256 − buffered segments).
* With `SACK`, the payload is a 4-byte bitmap: **bit i = 1 means segment
  ack + 1 + i was received**. This covers ack+1 to ack+32.
* ACKs are coalesced: one ACK per stream after the receiver has drained the
  socket.
* Segments below `ack`, or already buffered, are duplicates. They are dropped
  but still acknowledged.
* Segments at or beyond `ack + 256` are dropped (outside the window).

### 3.4 Retransmission
* RTT is estimated per peer (RFC 6298):
  `SRTT ← 7/8·SRTT + 1/8·R`, `RTTVAR ← 3/4·RTTVAR + 1/4·|SRTT − R|`,
  `RTO = SRTT + 4·RTTVAR`, clamped to [100 ms, 2 s]. The initial RTO is 500 ms.
* Karn's rule: RTT samples are taken only from segments that were sent once.
* A segment that is not acknowledged within `RTO · 2^retries` (capped at 2 s) is
  retransmitted.
* **Fast retransmit:** a hole that has at least 3 SACKed segments above it is
  retransmitted once, without waiting for the timer.
* After **10 retries** the stream is reset (section 3.6) and the peer is treated
  as unreachable.

### 3.5 Flow control
The sender keeps `next_seq − lowest_unacked < min(64, window)`. When the window
is 0, one segment may still be sent as a probe.

### 3.6 Epochs (stream reset)
When a stream is reset (the peer is declared dead or exceeded the retry limit),
the sender increments the 1-byte epoch and restarts seq at 0. When a receiver
sees a newer epoch, it discards its old state for that stream. Epoch e1 is
*newer* than e0 when `0 < (e1 − e0) mod 256 < 128`. Packets with an older epoch
are ignored.

### 3.7 Unreliable packets
`HELLO` and `HEARTBEAT` are sent with flags = 0, seq = 0, and stream 0. They are
never acknowledged or retransmitted.

## 4. Message types

Control payloads are **UTF-8 JSON objects**. Chunk payloads are **binary**.
Peer ids appear in JSON as integers. `file_id` appears in JSON as 16 hex
characters and in binary as 8 bytes.

| Code | Name | Rel. | Payload |
|-----:|------|:----:|---------|
| 0x01 | HELLO | no | `{"name": str, "port": int}` |
| 0x02 | WELCOME | yes | `{"name", "port", "peers": [[id, ip, port, name], ...]}` |
| 0x03 | HEARTBEAT | no | `{"name", "port", "peers": [[id, ip, port, name], ...]}` |
| 0x04 | LEAVE | yes | `{"peer": id}` |
| 0x10 | CHAT | yes | `{"sender": id, "name": str, "seq": int, "ts": float, "text": str, "to": id or 0}` |
| 0x11 | HISTORY_REQ | yes | `{}` |
| 0x12 | HISTORY_RESP | yes | `{"messages": [CHAT objects]}` (up to 100) |
| 0x20 | FILE_ANNOUNCE | yes | `{"files": [{"manifest": M, "locations": {"<idx>": [ids]}}]}` |
| 0x21 | FILE_LIST_REQ | yes | `{}` |
| 0x22 | FILE_LIST | yes | same layout as FILE_ANNOUNCE, with all known files |
| 0x23 | STORE_CHUNK | yes | binary chunk layout (below), whole chunk, offset 0 |
| 0x24 | STORE_ACK | yes | `{"file_id": hex, "index": int, "ok": bool}` |
| 0x25 | CHUNK_REQUEST | yes | `{"file_id": hex, "index": int, "offset": int, "stream": int}` |
| 0x26 | CHUNK_DATA | yes | binary chunk layout, one piece of at most 32 KB |
| 0x27 | CHUNK_NACK | yes | `{"file_id": hex, "index": int}` |
| 0x28 | CHUNK_LOCATION | yes | `{"file_id": hex, "index": int, "peer": id, "present": bool}` |
| 0x30 | ACK | – | empty, or a 4-byte SACK bitmap |

**Manifest object M:**
`{"file_id": hex16, "name": str, "size": int, "chunk_size": int, "num_chunks": int,
"chunk_hashes": [sha256 hex, ...], "file_hash": sha256 hex, "owner": id}`.
`file_id` is the first 16 hex characters of SHA-256(file). The default chunk size is
262 144 bytes (256 KB), and the last chunk may be shorter.

**Binary chunk layout** (STORE_CHUNK, CHUNK_DATA):

| Size | Field |
|-----:|-------|
| 8 | file_id (raw bytes) |
| 4 | chunk_index |
| 4 | offset of this data within the chunk |
| 4 | total chunk length |
| n | data |

## 5. Operations (message sequences)

### 5.1 Join
```
New                         Existing peers
 |-- HELLO (multicast, or unicast to --bootstrap) -->|   repeated 3x at 0.5 s, then every 5 s
 |<-------------- WELCOME {peers} -------------------|   reliable, from each peer that did not know New
 |-- HELLO (unicast) to each listed unknown peer --->|
 |<-------------- WELCOME ---------------------------|
 |-- FILE_LIST_REQ (to the first 2 peers met) ------>|
 |<-------------- FILE_LIST -------------------------|
 |-- HISTORY_REQ (to the first peer met) ----------->|
 |<-------------- HISTORY_RESP ----------------------|
```
A peer is added only after hearing from it directly (HELLO, WELCOME or
HEARTBEAT). A peer list received by gossip only triggers a HELLO. After joining,
the bootstrap peer is no longer needed.

### 5.2 Liveness
Every 1 s, each peer sends HEARTBEAT to every known peer. A peer with no
heartbeat for 3 s becomes **SUSPECT**, and after 5 s it is **DEAD**: it is removed,
its streams are reset, and `peer_left` is raised. LEAVE removes a peer
immediately. A peer that sent LEAVE is never re-added.

### 5.3 Group chat
The sender increments its own group `seq` (starting at 0) and sends CHAT on
stream 1 to every member. The receiver keeps `next_expected[sender]`. It delivers
in order, buffers messages that arrive after a gap, and drops duplicates. A late
joiner starts at the first seq it sees from each sender. Private messages
(`to` ≠ 0) use a separate counter and go only to that peer.

### 5.4 Share (upload) a file
1. Require at least R + 1 = 3 members. Build the manifest.
2. Placement: for chunk i, score every member p with
   `SHA-256("<file_id>:<i>:<p as 8 hex digits>")`. The R = 2 highest scores are
   the holders. If one peer would then hold every chunk, it is replaced in some
   chunks by the least-loaded non-holder (`p2p/placement.py`).
3. Send STORE_CHUNK to each holder. Each holder verifies the chunk (if it knows
   the manifest), stores it, replies STORE_ACK, and broadcasts CHUNK_LOCATION.
4. After all STORE_ACKs arrive (60 s timeout), send FILE_ANNOUNCE to every peer.

### 5.5 Download (multi-source)
For each chunk, the downloader chooses a source from the known holders (then
falls back to other live peers in HRW order). It picks the candidate with the
fewest outstanding requests, with at most 2 per source.
```
Downloader -- CHUNK_REQUEST {file, idx, offset, stream} --> Holder
Downloader <-- CHUNK_DATA pieces (32 KB) on 'stream' ------ Holder
           <-- or CHUNK_NACK if it does not hold the chunk
```
When all bytes of a chunk have arrived, its SHA-256 is checked against the
manifest. When every chunk is done, the whole-file SHA-256 is checked.

### 5.6 Failover
The downloader abandons a source for a chunk if: it sends CHUNK_NACK, membership
declares it dead, no data arrives for 3 s, or the chunk hash is wrong. It then
sends CHUNK_REQUEST to the next replica, with `offset` = the first missing byte,
so pieces already received are kept.

### 5.7 Rebalancing and re-replication
On every join, leave, or new file (and every 10 s), each peer recomputes the HRW
holders over the current members:
* if it is a holder but lacks the chunk, it fetches the chunk (join takeover or
  re-replication), stores it, and broadcasts CHUNK_LOCATION present = true;
* if it holds a chunk it is no longer responsible for, it deletes the chunk only
  when every new holder is alive and has announced it, then broadcasts
  CHUNK_LOCATION present = false.
