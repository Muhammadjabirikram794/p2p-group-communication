"""
cli.py - Interactive terminal interface for one peer.

Type a line to chat with the whole group, or a command:
    /peers                 list members and their state
    /files                 list shared files and where their chunks are
    /share <path>          share a file (needs >= 3 peers)
    /get <name|id>         download a file from several peers in parallel
    /msg <peer> <text>     private message (peer = name or id prefix)
    /history               last 20 chat messages
    /chunks                chunks stored on this peer
    /stats                 transport statistics (retransmissions, RTT, ...)
    /quit                  leave the group gracefully (sends LEAVE)
    /crash                 exit immediately WITHOUT LEAVE (demo crash)
"""

import os
import threading
import time

from .util import human_bytes, pid_str

HELP = __doc__.split("\n", 2)[2]


class CLI:
    def __init__(self, peer):
        self.p = peer
        peer.events.on("chat", self._show_chat)
        peer.events.on("chat_history", self._show_history)
        peer.events.on("peer_joined", lambda pid: self._info(
            "+ %s joined" % peer.membership.name_of(pid)))
        peer.events.on("peer_left", lambda pid: self._info(
            "- peer %s left / crashed" % pid_str(pid)))
        peer.events.on("file_added", lambda m: self._info(
            "* file available: %s (%s, %d chunks)" % (m.name, human_bytes(m.size),
                                                       m.num_chunks)))
        peer.events.on("download_progress", self._show_progress)

    # ---------- output ----------
    def _info(self, text):
        print("\r%s\n> " % text, end="", flush=True)

    def _show_chat(self, m):
        when = time.strftime("%H:%M:%S", time.localtime(m.get("ts", time.time())))
        tag = " (private)" if int(m.get("to", 0)) else ""
        self._info("[%s] %s%s: %s" % (when, m.get("name", "?"), tag, m.get("text", "")))

    def _show_history(self, msgs):
        self._info("--- %d earlier messages ---" % len(msgs))
        for m in msgs[-20:]:
            self._show_chat(m)

    def _show_progress(self, ev):
        if ev.get("label") != "download":
            return                     # background rebalancing: stay quiet
        name = self.p.membership.name_of(ev.get("from", 0))
        if "failover" in ev:
            self._info("! chunk %d: %s from %s -> switching to another replica"
                       % (ev["chunk"], ev["failover"], name))
        else:
            self._info("  chunk %d from %s  [%d/%d]" % (ev["chunk"], name,
                                                       ev["done"], ev["total"]))

    # ---------- commands ----------
    def run(self):
        p = self.p
        print("Peer %s (%s) on UDP port %d. Type /help for commands."
              % (p.name, pid_str(p.id), p.port))
        while p.running:
            try:
                line = input("> ").strip()
            except (EOFError, KeyboardInterrupt):
                line = "/quit"
            if not line:
                continue
            if not line.startswith("/"):
                p.say(line)
                continue
            cmd, _, arg = line.partition(" ")
            handler = getattr(self, "cmd_" + cmd[1:], None)
            if handler is None:
                print("unknown command, try /help")
                continue
            try:
                handler(arg.strip())
            except Exception as exc:
                print("error:", exc)

    def cmd_help(self, arg):
        print(HELP)

    def cmd_peers(self, arg):
        print("me: %s %s port %d" % (pid_str(self.p.id), self.p.name, self.p.port))
        for info in self.p.table.snapshot():
            print("  %s %-10s %s:%d  %s" % (pid_str(info.peer_id), info.name,
                                            info.addr[0], info.addr[1], info.state))

    def cmd_files(self, arg):
        files = self.p.catalog.files()
        if not files:
            print("no shared files yet")
        for m in files:
            print("%s  %-28s %9s  %d chunks" % (m.file_id, m.name, human_bytes(m.size),
                                               m.num_chunks))
            for i in range(m.num_chunks):
                names = sorted(self.p.membership.name_of(h)
                               for h in self.p.catalog.holders(m.file_id, i))
                print("     chunk %2d -> %s" % (i, ", ".join(names) or "?"))

    def cmd_share(self, arg):
        if not os.path.isfile(arg):
            print("no such file:", arg)
            return
        m = self.p.share(arg)
        print("shared %s as %s" % (m.name, m.file_id))

    def cmd_get(self, arg):
        def work():
            try:
                info = self.p.download(arg)
                srcs = ", ".join("%s=%s" % (k, human_bytes(v))
                                 for k, v in info["sources"].items())
                self._info("download OK: %s in %.2fs (%.2f MB/s), hash verified\n"
                           "  sources: %s" % (info["path"], info["seconds"],
                                              info["mbps"], srcs))
            except Exception as exc:
                self._info("download failed: %s" % exc)
        threading.Thread(target=work, daemon=True).start()

    def cmd_msg(self, arg):
        who, _, text = arg.partition(" ")
        pid = self.p.table.find(who)
        if pid is None:
            print("unknown peer:", who)
            return
        self.p.chat.send_private(pid, text)

    def cmd_history(self, arg):
        for m in self.p.chat.log.recent(20):
            self._show_chat(m)

    def cmd_chunks(self, arg):
        for fid, idx in self.p.store.list():
            m = self.p.catalog.get(fid)
            print("  %s chunk %d" % (m.name if m else fid, idx))
        print("total", human_bytes(self.p.store.bytes_stored))

    def cmd_stats(self, arg):
        for k, v in self.p.transport.stats.items():
            print("  %-18s %s" % (k, v))
        for pid in self.p.membership.peers():
            r = self.p.transport.rtt_of(pid)
            if r and r[0] is not None:
                print("  rtt %-12s srtt=%.2f ms rto=%.0f ms"
                      % (self.p.membership.name_of(pid), r[0] * 1000, r[1] * 1000))

    def cmd_quit(self, arg):
        print("leaving...")
        self.p.stop()

    def cmd_crash(self, arg):
        print("crashing (no LEAVE)!")
        os._exit(1)
