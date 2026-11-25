"""
events.py - Tiny publish/subscribe bus that decouples the subsystems.

    bus.on("peer_joined", storage.on_peer_joined)
    bus.emit("peer_joined", peer_id)

Events used in the project:
    peer_joined(peer_id)    membership saw a new peer
    peer_suspect(peer_id)   no heartbeat for SUSPECT_TIMEOUT
    peer_left(peer_id)      LEAVE received or no heartbeat for DEAD_TIMEOUT
    chat(msg_dict)          a chat message is ready to be shown
    file_added(manifest)    a new shared file is known
    download_progress(dict) downloader progress / failover notices
"""

import logging
import threading

log = logging.getLogger("events")


class EventBus:
    def __init__(self):
        self._subs = {}
        self._lock = threading.Lock()

    def on(self, name: str, callback):
        with self._lock:
            self._subs.setdefault(name, []).append(callback)

    def emit(self, name: str, *args, **kwargs):
        with self._lock:
            subs = list(self._subs.get(name, []))
        for cb in subs:
            try:
                cb(*args, **kwargs)
            except Exception:                       # one bad handler must not
                log.exception("handler for %s failed", name)   # break others
