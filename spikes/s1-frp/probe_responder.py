#!/usr/bin/env python3
"""Loopback responder for the step-0 probe of spike S1. Stdlib only, Python 3.6 compatible.

In a PROBE_ONLY=1 run, a job's "ssh -N -L" listener opens as soon as the key authenticates. That
proves the key and the host key, not that the far end can be reached. This responder is the far end:
the operator runs it on the agents VM, on 127.0.0.1 at the port the key may forward to (the one frps
uses later), and it sends one fixed line to every client and closes. The job reads that line through
the forward. Only the exact token counts, so a listener that merely accepts connections does not pass.

    probe_responder.py --port 7000

It binds 127.0.0.1 only, never another address, and stops by itself after --max-seconds (default
2 h) so it cannot linger. Stop it before frps starts: frps needs the same port. It logs one line
per connection (UTC time and count, no peer address) to stderr.
"""
import argparse
import datetime
import socket
import sys
import time

BIND = "127.0.0.1"
TOKEN = b"S1-PROBE-TOKEN-OK\n"          # job.sbatch.tmpl holds the same string in PROBE_TOKEN


def utc_stamp():
    now = datetime.datetime.fromtimestamp(time.time(), datetime.timezone.utc)
    return now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def serve(port, max_seconds, ready_file=None):
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((BIND, port))
    srv.listen(16)
    port = srv.getsockname()[1]
    if ready_file:
        with open(ready_file, "w") as fh:
            fh.write("%d\n" % port)
    sys.stderr.write("%s probe_responder listening on %s:%d for up to %d s\n" % (utc_stamp(), BIND, port, max_seconds))
    sys.stderr.flush()
    deadline = time.monotonic() + max_seconds
    count = 0
    while True:
        left = deadline - time.monotonic()
        if left <= 0:
            break
        srv.settimeout(min(left, 1.0))
        try:
            conn, _ = srv.accept()
        except socket.timeout:
            continue
        count += 1
        try:
            conn.sendall(TOKEN)
        except OSError:
            pass
        finally:
            conn.close()
        sys.stderr.write("%s connection %d answered\n" % (utc_stamp(), count))
        sys.stderr.flush()
    sys.stderr.write("%s probe_responder stopping after %d connection(s)\n" % (utc_stamp(), count))
    return 0


def main(argv):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--port", type=int, default=7000, help="port on 127.0.0.1 (default 7000, the port frps will use)")
    ap.add_argument("--max-seconds", type=int, default=7200, help="stop by itself after this long (default 7200)")
    ap.add_argument("--ready-file", help="write the bound port here once listening (for tests; --port 0 picks one)")
    args = ap.parse_args(argv)
    try:
        return serve(args.port, args.max_seconds, args.ready_file)
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
