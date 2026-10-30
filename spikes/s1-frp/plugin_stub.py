#!/usr/bin/env python3
"""Stub for frps server plugin calls (spike S1). Stdlib only, Python 3.6 compatible.

It allows every call and writes one JSON line per call, to stdout and to --log:

    {"ts": "2026-10-30T12:00:00.123Z", "epoch": 1788091200.123, "op": "CloseProxy",
     "user": "<instance>", "proxy": "<instance>.engine", "run_id": "<frps run id>", ...}

When CloseProxy is enabled in frps this log is the "path down" signal: frps calls it when a
client's session ends. A Ping line is the instance's liveness heartbeat. Login and NewProxy
lines show that registration reached the controller. Secrets (privilege_key) are never logged.

    plugin_stub.py --listen 127.0.0.1:9000 --log stub.jsonl
"""
import argparse
import datetime
import json
import socketserver
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

ALLOW = json.dumps({"reject": False, "unchange": True}).encode("utf-8")

_lock = threading.Lock()
_log_fh = None


def utc_now():
    now = time.time()
    stamp = datetime.datetime.fromtimestamp(now, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    return now, stamp


def emit(record):
    line = json.dumps(record, sort_keys=True)
    with _lock:
        sys.stdout.write(line + "\n")
        sys.stdout.flush()
        if _log_fh is not None:
            _log_fh.write(line + "\n")
            _log_fh.flush()


def summarize(op, content):
    """Pick the fields worth logging. Login carries user and run_id at the top level of
    content; every other op carries them under content.user."""
    if not isinstance(content, dict):
        content = {}
    if op == "Login":
        user, run_id = content.get("user", ""), content.get("run_id", "")
    else:
        u = content.get("user")
        u = u if isinstance(u, dict) else {}
        user, run_id = u.get("user", ""), u.get("run_id", "")
    rec = {"user": user, "run_id": run_id, "proxy": content.get("proxy_name", "")}
    if op == "Login":
        rec["client_version"] = content.get("version", "")
        rec["arch"] = content.get("arch", "")
    if op == "NewProxy":
        rec["proxy_type"] = content.get("proxy_type", "")
        rec["domains"] = content.get("custom_domains") or []
        rec["multiplexer"] = content.get("multiplexer", "")
    return rec


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):  # the JSON lines are the log
        pass

    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b""
        query = parse_qs(urlparse(self.path).query)
        try:
            body = json.loads(raw.decode("utf-8")) if raw else {}
        except ValueError:
            body = None
        if not isinstance(body, dict):
            self.send_error(400, "body is not a JSON object")
            return
        op = (query.get("op") or [body.get("op", "")])[0]
        epoch, stamp = utc_now()
        rec = {"ts": stamp, "epoch": round(epoch, 3), "op": op,
               "reqid": self.headers.get("X-Frp-Reqid", "")}
        rec.update(summarize(op, body.get("content")))
        emit(rec)
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(ALLOW)))
        self.end_headers()
        self.wfile.write(ALLOW)


class Server(socketserver.ThreadingMixIn, HTTPServer):
    daemon_threads = True


def main():
    global _log_fh
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--listen", default="127.0.0.1:9000", help="HOST:PORT (default 127.0.0.1:9000)")
    ap.add_argument("--log", help="also append the JSON lines to this file")
    args = ap.parse_args()
    host, _, port = args.listen.rpartition(":")
    if not host or not port.isdigit():
        ap.error("--listen wants HOST:PORT")
    if args.log:
        _log_fh = open(args.log, "a")
    srv = Server((host, int(port)), Handler)
    sys.stderr.write("plugin_stub listening on %s:%s\n" % (host, port))
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
