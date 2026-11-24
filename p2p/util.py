"""
util.py - Small helpers shared by every module (logging, ids, addresses).
"""

import logging
import os
import random
import socket
import sys
import time


def new_peer_id() -> int:
    """Random non-zero 32-bit peer id, chosen fresh every time a peer starts."""
    return random.SystemRandom().randint(1, 0xFFFFFFFF)


def pid_str(pid: int) -> str:
    """Peer id as 8 hex digits, e.g. 0x1a2b3c4d -> '1a2b3c4d'."""
    return "%08x" % pid


def now() -> float:
    """Monotonic clock for all timers (never jumps when the wall clock changes)."""
    return time.monotonic()


def local_ip() -> str:
    """Best guess of this machine's LAN IP (no packets are actually sent)."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("10.255.255.255", 1))
        return s.getsockname()[0]
    except OSError:
        return "127.0.0.1"
    finally:
        s.close()


def parse_hostport(text: str, default_port: int):
    """'192.168.1.5:50001' -> ('192.168.1.5', 50001)."""
    if ":" in text:
        host, port = text.rsplit(":", 1)
        return host, int(port)
    return text, default_port


def human_bytes(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return "%.1f %s" % (n, unit)
        n /= 1024.0
    return "%.1f TB" % n


def setup_logging(name: str, level: str = "INFO", logfile: str = None):
    fmt = "%(asctime)s.%(msecs)03d [" + name + "] %(levelname)s %(name)s: %(message)s"
    handlers = []
    if logfile:
        os.makedirs(os.path.dirname(logfile) or ".", exist_ok=True)
        handlers.append(logging.FileHandler(logfile, mode="w"))
    else:
        handlers.append(logging.StreamHandler(sys.stderr))
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO),
                        format=fmt, datefmt="%H:%M:%S", handlers=handlers,
                        force=True)
