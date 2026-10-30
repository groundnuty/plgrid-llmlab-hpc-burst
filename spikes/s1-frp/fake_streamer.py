#!/usr/bin/env python3
"""Fake streaming engine for spike S1. Stdlib only. Runs on Python 3.6 (the Helios host python).

    GET /stream?seconds=N   chunked reply, one line per second: "token <i> <utc>\\n", for N seconds
    GET /health             the fixed body {"status": "ok"} (stream_watch.py checks it exactly)

It binds 127.0.0.1 only and never takes another address. --port 0 picks a free port; give
--port-file to learn it. The reply to /stream is written chunk by chunk with the framing done
by hand, flushed after every token, on a fixed one-second schedule (no drift).

--request-log FILE appends one line per request ("<epoch> GET <path>"). The job uses it to see that
the watcher has attached: a request that arrives through frpc after the job's own loopback baseline.
"""
import argparse
import datetime
import json
import os
import socket
import socketserver
import sys
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

MAX_SECONDS = 86400
BIND = "127.0.0.1"
HEALTH_BODY = b'{"status": "ok"}'
REQUEST_LOG = None


def utc_stamp():
    now = datetime.datetime.fromtimestamp(time.time(), datetime.timezone.utc)
    return now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def log(msg):
    sys.stderr.write("%s %s\n" % (utc_stamp(), msg))
    sys.stderr.flush()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        pass

    def setup(self):
        BaseHTTPRequestHandler.setup(self)
        self.request.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    def _json(self, code, obj):
        body = json.dumps(obj).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _note_request(self):
        if REQUEST_LOG:
            try:
                with open(REQUEST_LOG, "a") as fh:
                    fh.write("%.3f GET %s\n" % (time.time(), self.path.split("?")[0]))
            except OSError as exc:
                log("cannot write --request-log: %s" % exc)

    def do_GET(self):
        self._note_request()
        url = urlparse(self.path)
        if url.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(HEALTH_BODY)))
            self.end_headers()
            self.wfile.write(HEALTH_BODY)
        elif url.path == "/stream":
            self._stream(parse_qs(url.query))
        else:
            self._json(404, {"error": "not found"})

    def _stream(self, query):
        try:
            seconds = int((query.get("seconds") or ["10"])[0])
        except ValueError:
            seconds = -1
        if seconds < 1 or seconds > MAX_SECONDS:
            self._json(400, {"error": "seconds must be 1..%d" % MAX_SECONDS})
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Transfer-Encoding", "chunked")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        log("stream start seconds=%d" % seconds)
        t0 = time.monotonic()
        sent = 0
        try:
            for i in range(1, seconds + 1):
                wait = t0 + (i - 1) - time.monotonic()
                if wait > 0:
                    time.sleep(wait)
                line = ("token %d %s\n" % (i, utc_stamp())).encode("ascii")
                self.wfile.write(b"%x\r\n" % len(line) + line + b"\r\n")
                self.wfile.flush()
                sent = i
            wait = t0 + seconds - time.monotonic()  # make the whole reply last N seconds
            if wait > 0:
                time.sleep(wait)
            self.wfile.write(b"0\r\n\r\n")
            self.wfile.flush()
            log("stream end tokens=%d" % sent)
        except (BrokenPipeError, ConnectionResetError, socket.timeout) as exc:
            log("stream cut by peer after tokens=%d (%s)" % (sent, exc.__class__.__name__))
            self.close_connection = True


class Server(socketserver.ThreadingMixIn, HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--port", type=int, default=0, help="TCP port on 127.0.0.1 (0 = pick one)")
    ap.add_argument("--port-file", help="write the chosen port here once listening")
    ap.add_argument("--request-log", help="append one line per request: epoch, method, path")
    args = ap.parse_args()
    global REQUEST_LOG
    REQUEST_LOG = args.request_log
    srv = Server((BIND, args.port), Handler)
    port = srv.server_address[1]
    if args.port_file:
        tmp = args.port_file + ".tmp"
        with open(tmp, "w") as fh:
            fh.write("%d\n" % port)
        os.rename(tmp, args.port_file)
    log("fake_streamer listening on %s:%d" % (BIND, port))
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
