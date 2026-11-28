"""
local_cluster.py - Start N background (headless) peers on this computer.

Each one shares its own two test files (from tools/make_test_files.py).
Then open a terminal and join as the interactive peer:

    python tools/make_test_files.py
    python tools/local_cluster.py --n 3            # peers A, B, C (ports 50001-3)
    python run_peer.py --name D --port 50004 --bootstrap 127.0.0.1:50001 --no-multicast

Logs: logs/<name>.log   Stop with Ctrl+C (sends LEAVE from every peer).
Kill one of them during a download to demo fault tolerance:
    python tools/local_cluster.py --kill B          (or close its process)
"""

import argparse
import glob
import os
import signal
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PIDFILE = os.path.join(ROOT, "logs", "cluster.pids")


def start(args):
    os.makedirs(os.path.join(ROOT, "logs"), exist_ok=True)
    procs = {}
    for i in range(args.n):
        name = chr(ord("A") + i)
        port = args.base_port + i
        cmd = [sys.executable, os.path.join(ROOT, "run_peer.py"), "--name", name,
               "--port", str(port), "--headless", "--no-multicast",
               "--logfile", os.path.join(ROOT, "logs", name + ".log"),
               "--metrics", os.path.join(ROOT, "results", "metrics_%s.csv" % name)]
        if i > 0:
            cmd += ["--bootstrap", "127.0.0.1:%d" % args.base_port]
        if args.rate:
            cmd += ["--rate", str(args.rate)]
        if args.loss:
            cmd += ["--loss", str(args.loss)]
        files = sorted(glob.glob(os.path.join(ROOT, "shared", name, "*")))
        if files and not args.no_share:
            cmd += ["--share"] + files
        procs[name] = subprocess.Popen(cmd, cwd=ROOT)
        print("started %s on port %d (pid %d) sharing %d files"
              % (name, port, procs[name].pid, len(files)))
        time.sleep(0.3)
    with open(PIDFILE, "w") as f:
        for name, p in procs.items():
            f.write("%s %d\n" % (name, p.pid))
    print("\nJoin with:\n  python run_peer.py --name Me --port %d --bootstrap "
          "127.0.0.1:%d --no-multicast\nCtrl+C here stops all peers."
          % (args.base_port + args.n, args.base_port))
    try:
        while any(p.poll() is None for p in procs.values()):
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("stopping...")
        for p in procs.values():
            if p.poll() is None:
                if os.name == "nt":
                    p.terminate()          # Windows: no SIGINT for child processes
                else:
                    p.send_signal(signal.SIGINT)   # graceful: peers send LEAVE
        for p in procs.values():
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()


def kill(name):
    """Hard-kill one peer (crash, no LEAVE)."""
    with open(PIDFILE) as f:
        for line in f:
            n, pid = line.split()
            if n == name:
                os.kill(int(pid), signal.SIGKILL if hasattr(signal, "SIGKILL")
                        else signal.SIGTERM)
                print("killed", name, pid)
                return
    print("no such peer", name)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=3)
    ap.add_argument("--base-port", type=int, default=50001)
    ap.add_argument("--rate", type=float, default=0, help="upload limit bytes/s")
    ap.add_argument("--loss", type=float, default=0)
    ap.add_argument("--no-share", action="store_true")
    ap.add_argument("--kill", metavar="NAME", help="crash one running peer")
    args = ap.parse_args()
    if args.kill:
        kill(args.kill)
    else:
        start(args)


if __name__ == "__main__":
    main()
