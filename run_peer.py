"""
run_peer.py - Start one peer.

Same LAN (multicast discovery):
    python run_peer.py --name Ali
Explicit bootstrap (if the network blocks multicast, e.g. university Wi-Fi):
    python run_peer.py --name Ali                       # first peer, port 50001
    python run_peer.py --name Sara --bootstrap 192.168.1.10:50001
Four peers on ONE computer:
    python run_peer.py --name A --port 50001 --no-multicast
    python run_peer.py --name B --port 50002 --bootstrap 127.0.0.1:50001 --no-multicast
    ...
Background seeder that shares files once 3 peers are present:
    python run_peer.py --name B --headless --share shared/B/*.bin ...
"""

import argparse
import glob
import logging
import signal
import sys
import threading
import time

from p2p import config
from p2p.cli import CLI
from p2p.peer import Peer
from p2p.util import parse_hostport, setup_logging, local_ip


def main():
    ap = argparse.ArgumentParser(description="Serverless P2P chat & file sharing over UDP")
    ap.add_argument("--name", required=True)
    ap.add_argument("--port", type=int, default=config.DEFAULT_PORT)
    ap.add_argument("--bind", default="0.0.0.0", help="local IP to bind")
    ap.add_argument("--bootstrap", action="append", default=[],
                    help="host:port of any existing peer (repeatable)")
    ap.add_argument("--no-multicast", action="store_true")
    ap.add_argument("--share", nargs="*", default=[],
                    help="files to share automatically once 3 peers are present")
    ap.add_argument("--data-dir", default="data")
    ap.add_argument("--loss", type=float, default=0.0, help="simulated loss 0..1")
    ap.add_argument("--dup", type=float, default=0.0, help="simulated duplication")
    ap.add_argument("--rate", type=float, default=0.0,
                    help="upload limit in bytes/s (0 = unlimited)")
    ap.add_argument("--headless", action="store_true", help="no interactive CLI")
    ap.add_argument("--metrics", default=None, help="CSV file for measurements")
    ap.add_argument("--log", default="WARNING", help="DEBUG/INFO/WARNING")
    ap.add_argument("--logfile", default=None)
    args = ap.parse_args()

    setup_logging(args.name, "INFO" if args.headless and args.log == "WARNING"
                  else args.log, args.logfile)
    boot = [parse_hostport(b, config.DEFAULT_PORT) for b in args.bootstrap]
    peer = Peer(args.name, port=args.port, bind_ip=args.bind, bootstrap=boot,
                multicast=not args.no_multicast, data_dir=args.data_dir,
                loss=args.loss, dup=args.dup, rate=args.rate,
                metrics_path=args.metrics).start()
    print("This machine: %s   peer UDP port: %d" % (local_ip(), peer.port))

    files = []
    for pattern in args.share:
        files += sorted(glob.glob(pattern)) or [pattern]
    if files:
        threading.Thread(target=auto_share, args=(peer, files), daemon=True).start()

    if args.headless:
        stop = threading.Event()
        signal.signal(signal.SIGINT, lambda *a: stop.set())
        if hasattr(signal, "SIGTERM"):
            signal.signal(signal.SIGTERM, lambda *a: stop.set())
        while not stop.is_set() and peer.running:
            stop.wait(0.5)
        peer.stop()
    else:
        CLI(peer).run()
    sys.exit(0)


def auto_share(peer, files):
    log = logging.getLogger("autoshare")
    while len(peer.membership.members()) < config.REPLICATION + 1:
        time.sleep(0.5)
    time.sleep(2.0)                    # let the others finish joining
    for path in files:
        try:
            m = peer.share(path)
            log.info("auto-shared %s", m.name)
        except Exception as exc:
            log.warning("could not share %s: %s", path, exc)


if __name__ == "__main__":
    main()
