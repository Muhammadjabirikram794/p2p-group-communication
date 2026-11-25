import os
import tempfile
import unittest

from p2p.events import EventBus
from p2p.metrics import Metrics


class TestEvents(unittest.TestCase):
    def test_emit_and_error_isolation(self):
        bus, got = EventBus(), []
        bus.on("x", lambda v: 1 / 0)             # broken handler
        bus.on("x", lambda v: got.append(v))
        bus.emit("x", 42)
        self.assertEqual(got, [42])


class TestMetrics(unittest.TestCase):
    def test_csv(self):
        path = os.path.join(tempfile.mkdtemp(), "m.csv")
        m = Metrics("A", path)
        m.log("download_done", seconds=1.5, bytes=100)
        m.incr("retx"); m.incr("retx")
        m.close()
        self.assertEqual(m.counters["retx"], 2)
        self.assertEqual(len(m.events("download_done")), 1)
        with open(path) as f:
            self.assertIn("seconds=1.5", f.read())


if __name__ == "__main__":
    unittest.main()
