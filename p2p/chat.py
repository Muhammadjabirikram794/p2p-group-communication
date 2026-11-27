"""
chat.py - Group chat and private messages on top of the reliable transport.

CHAT payload (JSON):
    {"sender": id, "name": str, "seq": n, "ts": unix_time, "text": str,
     "to": id-or-0}            # to != 0  -> private one-to-one message
Group messages use a per-sender seq (0,1,2,...) checked by ChatLog for
FIFO order and duplicates. Private messages have their own counter so they
never leave a gap in the group sequence.

Chat always travels on STREAM_CHAT (1), separate from file data streams,
and the transport serves it before data -> low chat latency during transfers.

History: when we meet our first peer we send HISTORY_REQ; it answers with
HISTORY_RESP {"messages": [...last 100...]}.
"""

import itertools
import logging
import threading
import time

from . import config, protocol as P
from .chatlog import ChatLog

log = logging.getLogger("chat")


class ChatService:
    def __init__(self, self_id, name, transport, membership, events,
                 metrics=None, stream=config.STREAM_CHAT):
        self.self_id = self_id
        self.name = name
        self.t = transport
        self.members = membership
        self.events = events
        self.metrics = metrics
        self.stream = stream
        self.log = ChatLog()
        self._seq = itertools.count()
        self._pseq = itertools.count()
        self._lock = threading.Lock()
        self._history_asked = False
        transport.on(P.CHAT, self._on_chat)
        transport.on(P.HISTORY_REQ, self._on_history_req)
        transport.on(P.HISTORY_RESP, self._on_history_resp)
        events.on("peer_joined", self._on_peer_joined)

    # ---------- sending ----------
    def send(self, text: str) -> dict:
        with self._lock:
            msg = {"sender": self.self_id, "name": self.name,
                   "seq": next(self._seq), "ts": time.time(), "text": text, "to": 0}
            self.log.add_own(msg)
        payload = P.pack_json(msg)
        for pid in self.members.peers():
            self.t.send(pid, self.stream, P.CHAT, payload)
        return msg

    def send_private(self, peer_id: int, text: str) -> dict:
        msg = {"sender": self.self_id, "name": self.name, "seq": next(self._pseq),
               "ts": time.time(), "text": text, "to": peer_id}
        self.t.send(peer_id, self.stream, P.CHAT, P.pack_json(msg))
        return msg

    # ---------- receiving ----------
    def _on_chat(self, pid, addr, payload):
        msg = P.unpack_json(payload)
        if int(msg.get("to", 0)):
            self._deliver(msg)                  # private: transport ordering
            return
        for m in self.log.receive(msg):
            self._deliver(m)

    def _deliver(self, msg):
        if self.metrics:
            self.metrics.log("chat_latency", sender=msg.get("name"),
                             seq=msg.get("seq"),
                             latency_ms=round((time.time() - msg.get("ts", time.time())) * 1000, 3))
        self.events.emit("chat", msg)

    # ---------- history ----------
    def _on_peer_joined(self, pid):
        if not self._history_asked:
            self._history_asked = True
            self.t.send(pid, config.STREAM_CONTROL, P.HISTORY_REQ, b"{}")

    def _on_history_req(self, pid, addr, payload):
        group = [m for m in self.log.recent() if not int(m.get("to", 0))]
        self.t.send(pid, config.STREAM_CONTROL, P.HISTORY_RESP,
                    P.pack_json({"messages": group}))

    def _on_history_resp(self, pid, addr, payload):
        new = self.log.merge_history(P.unpack_json(payload).get("messages", []))
        if new:
            self.events.emit("chat_history", new)
