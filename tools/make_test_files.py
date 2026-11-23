"""
make_test_files.py - Create random test files to share (>= 2 MB each).

    python tools/make_test_files.py                 # 2 files per peer A..D
    python tools/make_test_files.py --peers A --sizes 2.5 3
"""

import argparse
import sys
import os


def make_file(path: str, size_bytes: int):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "wb") as f:
        remaining = size_bytes
        while remaining > 0:
            block = os.urandom(min(remaining, 1 << 20))
            f.write(block)
            remaining -= len(block)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="shared", help="output folder")
    ap.add_argument("--peers", nargs="+", default=["A", "B", "C", "D"])
    ap.add_argument("--sizes", nargs="+", type=float, default=[2.0, 3.0],
                    help="file sizes in MB (one file per size, per peer)")
    args = ap.parse_args()
    for peer in args.peers:
        for i, mb in enumerate(args.sizes, 1):
            path = os.path.join(args.out, peer, "%s_file%d_%gMB.bin" % (peer, i, mb))
            make_file(path, int(mb * 1024 * 1024))
            print("created", path)


if __name__ == "__main__":
    main()
