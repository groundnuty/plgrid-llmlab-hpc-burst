#!/usr/bin/env python3
"""A test double for ssh, used by the job-template tests. It is NOT the kit and runs nowhere else.

The job opens two forwards, each its own ssh: "-L 127.0.0.1:LPORT:127.0.0.1:RPORT", RPORT being the VM port
(AGENT_PORT for control, DIALHOME_PORT for the tunnel). Each forward has its own plan and its own call count,
keyed by RPORT, so the two doubles never race: $FAKE_SSH_DIR/plan-<RPORT> if it exists, else $FAKE_SSH_DIR/plan;
one action per call (the last one repeats); $FAKE_SSH_DIR/calls-<RPORT> counts the calls.

    fwd                        a working forward: open LPORT and relay each connection to 127.0.0.1:RPORT
    fwd_for:SECS:RC:TEXT       like fwd, then print TEXT to stderr and exit RC after SECS seconds
    hold                       open LPORT, accept connections and keep them silent
    closed                     open LPORT, close each client at once (a refused far end)
    fail:RC:TEXT               print TEXT to stderr and exit RC at once, opening nothing

FAKE_SSH_LEAK, if set, is printed to stderr first (a stand-in for ssh messages that name hosts, users, paths).
"""
import os
import re
import select
import signal
import socket
import sys
import threading
import time


def pump(a, b):
    """Copy bytes both ways between a and b until either side closes."""
    socks = [a, b]
    try:
        while True:
            ready, _, _ = select.select(socks, [], [], 1.0)
            for s in ready:
                data = s.recv(65536)
                if not data:
                    return
                (b if s is a else a).sendall(data)
    except OSError:
        return
    finally:
        for s in socks:
            try:
                s.close()
            except OSError:
                pass


def relay(conn, rport):
    try:
        far = socket.create_connection(("127.0.0.1", rport), 3)
    except OSError:
        conn.close()                 # nothing listens behind the forward: ssh closes the local connection
        return
    t = threading.Thread(target=pump, args=(conn, far))
    t.daemon = True
    t.start()


def next_action(d, rport):
    counter = os.path.join(d, "calls-%d" % rport)
    n = (int(open(counter).read()) if os.path.exists(counter) else 0) + 1
    with open(counter, "w") as fh:
        fh.write(str(n))
    plan_file = os.path.join(d, "plan-%d" % rport)
    if not os.path.exists(plan_file):
        plan_file = os.path.join(d, "plan")
    plan = [ln.strip() for ln in open(plan_file) if ln.strip() and not ln.startswith("#")]
    return plan[min(n, len(plan)) - 1]


def main(argv):
    if "-V" in argv:
        sys.stderr.write("OpenSSH_fake 0.0\n")
        return 0
    lport = rport = None
    for i, a in enumerate(argv):
        if a == "-L" and i + 1 < len(argv):
            m = re.match(r"127\.0\.0\.1:(\d+):127\.0\.0\.1:(\d+)$", argv[i + 1])
            if m:
                lport, rport = int(m.group(1)), int(m.group(2))
    if lport is None:
        sys.stderr.write("fake_ssh: no -L 127.0.0.1:L:127.0.0.1:R argument\n")
        return 2
    action = next_action(os.environ["FAKE_SSH_DIR"], rport)
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
    if kind in ("fwd_for", "hold_for"):
        secs, rc, text = rest.split(":", 2)
        end, rc = time.time() + float(secs), int(rc)
    held = []
    while end is None or time.time() < end:
        ready, _, _ = select.select([srv], [], [], 0.2)
        if not ready:
            continue
        conn, _ = srv.accept()
        if kind in ("fwd", "fwd_for"):
            relay(conn, rport)
        elif kind == "closed":
            conn.close()
        else:
            held.append(conn)
    sys.stderr.write(text + "\n")
    os._exit(rc)                      # relays die with the process, as an ssh session's channels do


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
