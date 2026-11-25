"""
chatlog.py - Per-sender FIFO ordering + duplicate suppression for group chat.

Each sender numbers its group messages 0,1,2,...  (msg_seq).
For every sender we keep 'next_expected'. A message is
  * delivered       if msg_seq == next_expected (then buffered ones follow),
  * buffered        if msg_seq >  next_expected (a gap: wait for the missing one),
  * dropped (dup)   if msg_seq <  next_expected or already buffered.

The transport already gives in-order, exactly-once delivery per connection,
so this layer is defence in depth: it also covers retransmissions after a
stream reset and messages that arrive twice through history sync.

A peer that joins late starts each sender at the first msg_seq it sees
(it can't wait for messages sent before it existed) and gets older messages
from HISTORY_RESP instead.
"""

import threading
from collections import deque

from . import config


class ChatLog:
    def __init__(self, history_size: int = config.CHAT_HISTORY):
        self._lock = threading.Lock()
        self._next = {}             # sender -> next expected msg_seq
        self._pending = {}          # sender -> {msg_seq: msg}
        self._seen = set()          # (sender, msg_seq) for history merging
        self.history = deque(maxlen=history_size)
        self.duplicates = 0

    def receive(self, msg: dict):
        """msg needs 'sender' and 'seq'. Returns the list of messages that can
        now be shown, in order."""
        sender, seq = int(msg["sender"]), int(msg["seq"])
        with self._lock:
            if sender not in self._next:
                self._next[sender] = seq           # first message from sender
                self._pending[sender] = {}
            expected = self._next[sender]
            pend = self._pending[sender]
            if seq < expected or seq in pend:
                self.duplicates += 1
                return []
            pend[seq] = msg
            out = []
            while self._next[sender] in pend:
                m = pend.pop(self._next[sender])
                self._next[sender] += 1
                self._remember(m)
                out.append(m)
            return out

    def add_own(self, msg: dict):
        with self._lock:
            self._next[int(msg["sender"])] = int(msg["seq"]) + 1
            self._pending.setdefault(int(msg["sender"]), {})
            self._remember(msg)

    def merge_history(self, messages):
        """Add messages from HISTORY_RESP that we have not seen. Returns the
        new ones (sorted by timestamp)."""
        new = []
        with self._lock:
            for m in messages:
                key = (int(m["sender"]), int(m["seq"]))
                if key in self._seen:
                    continue
                self._remember(m)
                new.append(m)
            ordered = sorted(self.history, key=lambda m: m.get("ts", 0))
            self.history.clear()
            self.history.extend(ordered)
        return sorted(new, key=lambda m: m.get("ts", 0))

    def recent(self, n: int = config.CHAT_HISTORY):
        with self._lock:
            return list(self.history)[-n:]

    def pending_count(self) -> int:
        with self._lock:
            return sum(len(p) for p in self._pending.values())

    def _remember(self, m):
        self._seen.add((int(m["sender"]), int(m["seq"])))
        self.history.append(m)
