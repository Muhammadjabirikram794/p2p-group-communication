"""
rtt.py - Round-trip-time estimation and retransmission timeout (RFC 6298).

    first sample R:   SRTT = R,  RTTVAR = R / 2
    later samples:    RTTVAR = (1 - beta) * RTTVAR + beta * |SRTT - R|
                      SRTT   = (1 - alpha) * SRTT + alpha * R
    RTO = SRTT + max(G, 4 * RTTVAR), clamped to [MIN_RTO, MAX_RTO]

Karn's rule: never take a sample from a retransmitted segment (the ACK is
ambiguous). The transport enforces that by only calling sample() for segments
that were sent exactly once.
"""

from . import config

ALPHA = 0.125
BETA = 0.25
GRANULARITY = 0.001


class RttEstimator:
    def __init__(self, initial_rto=config.INITIAL_RTO,
                 min_rto=config.MIN_RTO, max_rto=config.MAX_RTO):
        self.srtt = None
        self.rttvar = None
        self.rto = initial_rto
        self.min_rto = min_rto
        self.max_rto = max_rto
        self.samples = 0

    def sample(self, rtt: float):
        if rtt < 0:
            return
        if self.srtt is None:
            self.srtt = rtt
            self.rttvar = rtt / 2.0
        else:
            self.rttvar = (1 - BETA) * self.rttvar + BETA * abs(self.srtt - rtt)
            self.srtt = (1 - ALPHA) * self.srtt + ALPHA * rtt
        self.samples += 1
        rto = self.srtt + max(GRANULARITY, 4 * self.rttvar)
        self.rto = min(self.max_rto, max(self.min_rto, rto))

    def timeout_for(self, retries: int) -> float:
        """Exponential back-off: RTO * 2^retries, capped at MAX_RTO."""
        return min(self.max_rto, self.rto * (2 ** retries))
