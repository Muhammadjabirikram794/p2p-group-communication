"""
metrics.py - Measurement logging for the report graphs.

Every measurement is one CSV row:  time, peer, event, key=value...
Rows are written to <path> (if given) and kept in memory for tests/benchmarks.
"""

import csv
import os
import threading
import time


class Metrics:
    def __init__(self, peer_name: str = "", path: str = None):
        self.peer = peer_name
        self.path = path
        self.rows = []
        self.counters = {}
        self._lock = threading.Lock()
        self._file = None
        if path:
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
            self._file = open(path, "w", newline="")
            self._writer = csv.writer(self._file)
            self._writer.writerow(["time", "peer", "event", "data"])

    def log(self, event: str, **data):
        row = {"time": time.time(), "peer": self.peer, "event": event, **data}
        with self._lock:
            self.rows.append(row)
            if self._file:
                self._writer.writerow([
                    "%.6f" % row["time"], self.peer, event,
                    ";".join("%s=%s" % kv for kv in data.items())])
                self._file.flush()

    def incr(self, name: str, amount=1):
        with self._lock:
            self.counters[name] = self.counters.get(name, 0) + amount

    def events(self, name: str):
        with self._lock:
            return [r for r in self.rows if r["event"] == name]

    def close(self):
        with self._lock:
            if self._file:
                self._file.close()
                self._file = None
