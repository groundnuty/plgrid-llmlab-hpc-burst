#!/usr/bin/env python3
"""A test double for ssh, used by the job-template tests. It is NOT the kit and runs nowhere else.

The plan file $FAKE_SSH_DIR/plan has one action per call (the last one repeats); $FAKE_SSH_DIR/calls
counts the calls. The -L argument names the local port the real ssh would open.

    hold                       open the local port, accept connections and keep them silent (as frps does)
    hold_for:SECS:RC:TEXT      like hold, then print TEXT to stderr and exit RC after SECS seconds
    token                      open the local port, send the probe token line to each client, close
    closed                     open the local port, close each client at once (a refused far end)
    data                       open the local port, send a line that is not the token
    fail:RC:TEXT               print TEXT to stderr and exit RC at once, opening nothing

FAKE_SSH_LEAK, if set, is printed to stderr first (a stand-in for ssh messages that name hosts, users, paths).
"""
import os
import re
import select
import signal
import socket
import sys
import time

TOKEN = b"S1-PROBE-TOKEN-OK\n"


def main(argv):
    if "-V" in argv:
        sys.stderr.write("OpenSSH_fake 0.0\n")
        return 0
    d = os.environ["FAKE_SSH_DIR"]
    counter = os.path.join(d, "calls")
    n = (int(open(counter).read()) if os.path.exists(counter) else 0) + 1
    with open(counter, "w") as fh:
        fh.write(str(n))
    plan = [ln.strip() for ln in open(os.path.join(d, "plan")) if ln.strip() and not ln.startswith("#")]
    action = plan[min(n, len(plan)) - 1]
    lport = None
    for i, a in enumerate(argv):
        if a == "-L" and i + 1 < len(argv):
            m = re.match(r"127\.0\.0\.1:(\d+):", argv[i + 1])
            if m:
                lport = int(m.group(1))
    leak = os.environ.get("FAKE_SSH_LEAK", "")
    if leak:
        sys.stderr.write(leak + "\n")
        sys.stderr.flush()
    kind, _, rest = action.partition(":")
    if kind == "fail":
        rc, _, text = rest.partition(":")
        sys.stderr.write(text + "\n")
        return int(rc)
    srv = socket.socket()
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", lport))
    srv.listen(8)

    def term(signum, frame):
        sys.stderr.write("Killed by signal %d.\n" % signum)
        os._exit(255)
    signal.signal(signal.SIGTERM, term)
    signal.signal(signal.SIGINT, term)
    end, rc, text = None, 255, ""
    if kind == "hold_for":
        secs, rc, text = rest.split(":", 2)
        end, rc = time.time() + float(secs), int(rc)
    held = []
    while end is None or time.time() < end:
        ready, _, _ = select.select([srv], [], [], 0.2)
        if not ready:
            continue
        conn, _ = srv.accept()
        if kind == "token":
            conn.sendall(TOKEN)
            conn.close()
        elif kind == "closed":
            conn.close()
        elif kind == "data":
            conn.sendall(b"HELLO\n")
            conn.close()
        else:
            held.append(conn)
    sys.stderr.write(text + "\n")
    return rc


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
