"""
connection.py - Per-stream reliability state machines (no sockets in here).

The transport keeps, for every (peer, stream) pair:
  * one SendStream  - what WE send to that peer on that stream
  * one RecvStream  - what WE receive from that peer on that stream

Reliability mechanisms (TCP-like, without congestion control):
  * sequence numbers per stream, cumulative ACK = next expected seq
  * Selective Repeat: receiver buffers out-of-order segments, ACK carries a
    32-bit SACK bitmap (bit i => segment ack+1+i received)
  * retransmission on timeout with exponential back-off (RTO from rtt.py)
  * fast retransmit: a hole with >= 3 SACKed segments above it is resent
  * flow control: never more than min(SEND_WINDOW, advertised window)
    segments beyond the lowest un-ACKed one
  * message framing: big messages are split into segments, FLAG_EOM marks
    the last segment, the receiver re-assembles
  * duplicate suppression: segments below 'expected' or already buffered
    are dropped (but still ACKed)
  * epochs: when a stream is reset (peer declared dead / too many retries)
    the epoch byte changes and seq restarts at 0; the receiver resets its
    state when it sees a newer epoch, and ignores packets of older epochs.
"""

import random
from collections import deque

from . import config
from .protocol import Packet, FLAG_RELIABLE, FLAG_EOM


def epoch_newer(a: int, b: int) -> bool:
    """True if epoch a is newer than epoch b (mod 256 arithmetic)."""
    return 0 < ((a - b) & 0xFF) < 128


class _InFlight:
    __slots__ = ("pkt", "sent_at", "first_sent", "retries", "fast_done")

    def __init__(self, pkt, t):
        self.pkt = pkt
        self.sent_at = t
        self.first_sent = t
        self.retries = 0
        self.fast_done = False


class SendStream:
    def __init__(self, src_id: int, stream: int, window: int = config.SEND_WINDOW):
        self.src_id = src_id
        self.stream = stream
        self.max_window = window
        self.epoch = random.randint(0, 255)
        self.next_seq = 0
        self.queue = deque()           # [(type, flags, payload)] not yet sent
        self.inflight = {}             # seq -> _InFlight
        self.peer_window = config.RECV_WINDOW
        self.queued_bytes = 0

    # ---------- application side ----------
    def enqueue(self, mtype: int, payload: bytes):
        """Split one application message into segments."""
        size = config.MAX_PAYLOAD
        if not payload:
            self.queue.append((mtype, FLAG_RELIABLE | FLAG_EOM, b""))
            return
        for off in range(0, len(payload), size):
            part = payload[off:off + size]
            last = off + size >= len(payload)
            flags = FLAG_RELIABLE | (FLAG_EOM if last else 0)
            self.queue.append((mtype, flags, part))
            self.queued_bytes += len(part)

    # ---------- sending ----------
    def base(self) -> int:
        return min(self.inflight) if self.inflight else self.next_seq

    def window(self) -> int:
        return max(1, min(self.max_window, self.peer_window))

    def can_send(self) -> bool:
        return bool(self.queue) and (self.next_seq - self.base()) < self.window()

    def next_packet(self, t: float) -> Packet:
        mtype, flags, payload = self.queue.popleft()
        self.queued_bytes -= len(payload)
        pkt = Packet(mtype, self.src_id, self.stream, self.next_seq, 0, 0,
                     flags, self.epoch, payload)
        self.inflight[self.next_seq] = _InFlight(pkt, t)
        self.next_seq += 1
        return pkt

    # ---------- ACK processing ----------
    def on_ack(self, ack: int, sack_bits: int, window: int, epoch: int, t: float):
        """Returns (rtt_sample_or_None, [packets_to_fast_retransmit])."""
        if epoch != self.epoch:
            return None, []                    # ACK for an old incarnation
        self.peer_window = window
        sample = None
        newest_clean = None
        acked = [s for s in self.inflight if s < ack]
        for bit in range(config.SACK_BITS):
            if sack_bits & (1 << bit):
                acked.append(ack + 1 + bit)
        for s in acked:
            inf = self.inflight.pop(s, None)
            if inf is not None and inf.retries == 0:      # Karn's rule
                if newest_clean is None or inf.sent_at > newest_clean:
                    newest_clean = inf.sent_at
        if newest_clean is not None:
            sample = t - newest_clean
        # Fast retransmit: holes below the highest SACKed segment
        fast = []
        if sack_bits:
            highest = ack + sack_bits.bit_length()      # highest SACKed seq
            for s in sorted(self.inflight):
                if s >= highest:
                    break
                inf = self.inflight[s]
                if not inf.fast_done and highest - s >= config.FAST_RETX_THRESHOLD:
                    inf.fast_done = True
                    inf.retries += 1
                    inf.sent_at = t
                    fast.append(inf.pkt)
        return sample, fast

    # ---------- timers ----------
    def expired(self, t: float, rtt):
        """Returns (packets_to_retransmit, failed). failed=True means the
        peer did not answer MAX_RETRIES times: caller should reset()."""
        out = []
        for inf in self.inflight.values():
            if t - inf.sent_at >= rtt.timeout_for(inf.retries):
                if inf.retries >= config.MAX_RETRIES:
                    return [], True
                inf.retries += 1
                inf.sent_at = t
                out.append(inf.pkt)
        return out, False

    def reset(self):
        """Forget everything and start a new epoch (seq restarts at 0)."""
        self.epoch = (self.epoch + 1) & 0xFF
        self.next_seq = 0
        self.queue.clear()
        self.inflight.clear()
        self.queued_bytes = 0
        self.peer_window = config.RECV_WINDOW

    def idle(self) -> bool:
        return not self.queue and not self.inflight


class RecvStream:
    def __init__(self, epoch: int, window: int = config.RECV_WINDOW):
        self.epoch = epoch
        self.expected = 0
        self.buffer = {}               # seq -> Packet (out of order)
        self.parts = []                # segments of the message being built
        self.part_type = None
        self.max_window = window
        self.duplicates = 0

    def on_packet(self, pkt: Packet):
        """Returns a list of complete (type, payload) messages, in order."""
        if pkt.seq < self.expected or pkt.seq in self.buffer:
            self.duplicates += 1
            return []
        if pkt.seq >= self.expected + self.max_window:
            return []                  # outside our window: drop, sender resends
        self.buffer[pkt.seq] = pkt
        out = []
        while self.expected in self.buffer:
            p = self.buffer.pop(self.expected)
            self.expected += 1
            self.parts.append(p.payload)
            self.part_type = p.type
            if p.flags & FLAG_EOM:
                out.append((self.part_type, b"".join(self.parts)))
                self.parts = []
        return out

    def ack_info(self):
        """-> (cumulative_ack, sack_bitmap, advertised_window)"""
        bits = 0
        for seq in self.buffer:
            off = seq - self.expected - 1
            if 0 <= off < config.SACK_BITS:
                bits |= 1 << off
        return self.expected, bits, max(0, self.max_window - len(self.buffer))
