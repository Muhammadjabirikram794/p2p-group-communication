"""Pure state-machine tests: a SendStream talking to a RecvStream through a
simulated lossy, reordering 'network' (a Python list)."""
import random
import unittest

from p2p import config
from p2p.connection import SendStream, RecvStream, epoch_newer
from p2p.rtt import RttEstimator


def run_link(messages, loss=0.0, seed=0, reorder=False, max_rounds=20000):
    rng = random.Random(seed)
    tx = SendStream(src_id=1, stream=2)
    rx = RecvStream(tx.epoch)
    rtt = RttEstimator(initial_rto=0.0, min_rto=0.0)   # "time" = rounds
    for mtype, payload in messages:
        tx.enqueue(mtype, payload)
    delivered, t = [], 0.0
    while not tx.idle() and t < max_rounds:
        t += 1.0
        wire = []
        while tx.can_send():
            wire.append(tx.next_packet(t))
        resend, failed = tx.expired(t, rtt)
        assert not failed
        wire += resend
        if reorder:
            rng.shuffle(wire)
        for pkt in wire:
            if rng.random() < loss:
                continue
            delivered += rx.on_packet(pkt)
        ack, bits, win = rx.ack_info()
        if rng.random() >= loss:                # ACKs can be lost too
            _, fast = tx.on_ack(ack, bits, win, tx.epoch, t)
            for pkt in fast:
                if rng.random() >= loss:
                    delivered += rx.on_packet(pkt)
    return delivered, tx, rx


class TestConnection(unittest.TestCase):
    def test_segmentation_and_reassembly(self):
        big = bytes(range(256)) * 40          # 10 KB -> 9 segments
        out, _, _ = run_link([(0x26, big), (0x10, b"hi"), (0x10, b"")])
        self.assertEqual(out, [(0x26, big), (0x10, b"hi"), (0x10, b"")])

    def test_reliable_under_loss_and_reordering(self):
        msgs = [(0x10, ("msg %d" % i).encode() * 50) for i in range(200)]
        for loss in (0.05, 0.2, 0.4):
            out, _, rx = run_link(msgs, loss=loss, seed=int(loss * 100),
                                  reorder=True)
            self.assertEqual(out, msgs, "loss=%s" % loss)

    def test_window_respected(self):
        tx = SendStream(1, 2, window=8)
        for i in range(50):
            tx.enqueue(0x10, b"x")
        sent = 0
        while tx.can_send():
            tx.next_packet(0.0); sent += 1
        self.assertEqual(sent, 8)
        tx.on_ack(0, 0, 3, tx.epoch, 0.1)     # receiver advertises window 3
        self.assertFalse(tx.can_send())       # 8 in flight > 3

    def test_duplicates_dropped(self):
        tx = SendStream(1, 1)
        rx = RecvStream(tx.epoch)
        tx.enqueue(0x10, b"a")
        pkt = tx.next_packet(0)
        self.assertEqual(rx.on_packet(pkt), [(0x10, b"a")])
        self.assertEqual(rx.on_packet(pkt), [])
        self.assertEqual(rx.duplicates, 1)

    def test_sack_bitmap(self):
        rx = RecvStream(0)
        from p2p.protocol import Packet, FLAG_EOM
        for s in (1, 3):
            rx.on_packet(Packet(0x10, 1, 0, s, flags=FLAG_EOM, payload=b"x"))
        self.assertEqual(rx.ack_info()[:2], (0, 0b101))

    def test_reset_and_epochs(self):
        tx = SendStream(1, 1)
        e = tx.epoch
        tx.enqueue(0x10, b"a"); tx.next_packet(0)
        tx.reset()
        self.assertEqual(tx.next_seq, 0)
        self.assertTrue(epoch_newer(tx.epoch, e))
        self.assertFalse(epoch_newer(e, tx.epoch))
        self.assertTrue(epoch_newer(0, 255))


if __name__ == "__main__":
    unittest.main()
