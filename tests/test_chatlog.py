import unittest

from p2p.chatlog import ChatLog


def m(sender, seq, text=""):
    return {"sender": sender, "seq": seq, "text": text or "%d-%d" % (sender, seq),
            "ts": seq}


class TestChatLog(unittest.TestCase):
    def test_in_order(self):
        log = ChatLog()
        self.assertEqual(log.receive(m(1, 0)), [m(1, 0)])
        self.assertEqual(log.receive(m(1, 1)), [m(1, 1)])

    def test_gap_is_buffered(self):
        log = ChatLog()
        log.receive(m(1, 0))
        self.assertEqual(log.receive(m(1, 2)), [])
        self.assertEqual(log.receive(m(1, 3)), [])
        self.assertEqual(log.receive(m(1, 1)), [m(1, 1), m(1, 2), m(1, 3)])

    def test_duplicates(self):
        log = ChatLog()
        log.receive(m(1, 0)); log.receive(m(1, 0)); log.receive(m(1, 2))
        log.receive(m(1, 2))
        self.assertEqual(log.duplicates, 2)

    def test_senders_independent(self):
        log = ChatLog()
        log.receive(m(1, 5))
        self.assertEqual(log.receive(m(2, 9)), [m(2, 9)])   # late joiner

    def test_history_merge(self):
        log = ChatLog()
        log.receive(m(1, 3))
        new = log.merge_history([m(1, 1), m(1, 2), m(1, 3)])
        self.assertEqual(new, [m(1, 1), m(1, 2)])
        self.assertEqual([x["seq"] for x in log.recent()], [1, 2, 3])


if __name__ == "__main__":
    unittest.main()
