#!/usr/bin/env python3
"""Path-rate server and meter for spike S1. Stdlib only. Runs on Python 3.6 (the Helios host python).

    path_rate.py serve   --listen 127.0.0.1:PORT [--tls-cert FILE --tls-key FILE] [--max-seconds N] [--log FILE]
    path_rate.py measure --host H --port P --forward NAME --direction up|down --seconds S [--max-bps R]
                         [--tls --ca FILE --server-name NAME] [--timeout T]
    path_rate.py probe   --host H --port P [--tls --ca FILE --server-name NAME] [--timeout T]

serve is the far end on the VM, on loopback only: GET /s1/probe (the token), POST /s1/sink (counts a
body, Content-Length or chunked, and times it at the server), GET /s1/source?seconds=S[&max_bps=R]
(a chunked body of random bytes). measure runs on the job and prints ONE JSON line (exit 0 when ok, 1
otherwise). The rate is always timed by the receiver: up by the server's own clock on the first and
last body byte, down by the client's. A sender's clock stops when bytes enter ssh and TCP buffers,
which overstates a slow path. probe sends one GET /s1/probe and prints one JSON line (exit 0 only for
far_end "token"). With --tls the certificate is verified against --ca for --server-name, which is not
the address connected to (in ssh mode the address is 127.0.0.1). Verification is never turned off.
"""
import argparse
import collections
import datetime
import ipaddress
import json
import os
import socket
import socketserver
import ssl
import sys
import threading
import time
from urllib.parse import parse_qs, urlparse

TOKEN = b"S1-PROBE-TOKEN-OK\n"          # job.sbatch.tmpl and probe_responder.py hold the same string
CHUNK = 65536
MAX_SOURCE_S = 600
FIELDS = ("forward", "direction", "ok", "requested_s", "max_bps", "bytes", "duration_s", "bps",
          "timed_by", "sender_s", "detail")


class ProtocolError(Exception):
    """The peer sent something that is not the HTTP framing this tool speaks."""


def utc_stamp():
    now = datetime.datetime.fromtimestamp(time.time(), datetime.timezone.utc)
    return now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def chunk_frame(payload):
    return b"%x\r\n" % len(payload) + payload + b"\r\n"


class Pacer(object):
    """Token bucket: after each chunk, sleep until the bytes sent so far fit under bps."""

    def __init__(self, bps):
        self.bps, self.start, self.sent = bps, time.monotonic(), 0

    def after(self, nbytes):
        self.sent += nbytes
        if self.bps:
            ahead = self.sent / self.bps - (time.monotonic() - self.start)
            if ahead > 0:
                time.sleep(ahead)


class BodyTimer(object):
    """Counts body bytes and keeps the wall-clock time of the first and the last read."""

    def __init__(self):
        self.total, self.first, self.last = 0, None, None

    def add(self, nbytes):
        self.last = time.time()
        if self.first is None:
            self.first = self.last
        self.total += nbytes

    def summary(self):
        duration = (self.last - self.first) if self.first is not None else 0.0
        return {"bytes": self.total, "first_byte_epoch": self.first, "last_byte_epoch": self.last,
                "duration_s": duration, "bps": (self.total / duration) if duration > 0 else None}


def read_message_head(rfile):
    """Start line (split) and lower-cased headers; (None, {}) when the peer closed before sending anything."""
    start = rfile.readline(8192)
    if not start:
        return None, {}
    headers = {}
    while True:
        hdr = rfile.readline(8192)
        if hdr in (b"\r\n", b"\n"):
            return start.decode("latin-1").split(), headers
        if not hdr:
            raise ProtocolError("message ended inside the headers")
        name, _, value = hdr.decode("latin-1").partition(":")
        headers[name.strip().lower()] = value.strip()


def read_fixed(rfile, length, timer):
    left = length
    while left > 0:
        data = rfile.read1(min(left, CHUNK))
        if not data:
            raise ProtocolError("body ended after %d of %d bytes" % (length - left, length))
        timer.add(len(data))
        left -= len(data)


def read_chunked(rfile, timer):
    """Decode a chunked body of any chunk size (extensions after ';' ignored) into timer."""
    while True:
        line = rfile.readline(1024)
        if not line.endswith(b"\n"):
            raise ProtocolError("missing or oversized chunk-size line")
        try:
            size = int(line.split(b";")[0].strip(), 16)
        except ValueError:
            raise ProtocolError("bad chunk size %r" % line[:40])
        if size == 0:
            while rfile.readline(1024) not in (b"\r\n", b"\n", b""):   # trailers, then the blank line
                pass
            return
        read_fixed(rfile, size, timer)
        if rfile.readline(8) != b"\r\n":
            raise ProtocolError("chunk not followed by CRLF")


# ---------------------------------------------------------------- serve

class Handler(socketserver.StreamRequestHandler):
    rbufsize = CHUNK
    timeout = 120

    def handle(self):
        self.status, self.nbytes, self.error = None, 0, None
        self.method, self.path, self.headers = "?", "?", {}
        try:
            try:
                if not self.read_request():
                    return                           # connected and closed: a port check, not a request
                route = {("GET", "/s1/probe"): self.probe, ("POST", "/s1/sink"): self.sink,
                         ("GET", "/s1/source"): self.source}.get((self.method, urlparse(self.path).path))
                if route is None:
                    self.reply(404, b"no such endpoint\n", "text/plain")
                else:
                    route()
            except ProtocolError as exc:
                self.error = str(exc)
                self.reply(400, (str(exc) + "\n").encode(), "text/plain")
        except OSError as exc:                       # the client went away mid-request
            self.error = "%s: %s" % (type(exc).__name__, exc)
        self.server.log_request(self.method, self.path, self.status, self.nbytes, self.error)

    def read_request(self):
        """Parse the request line and headers into self; False when the client sent nothing."""
        parts, self.headers = read_message_head(self.rfile)
        if parts is None:
            return False
        if len(parts) != 3 or not parts[2].startswith("HTTP/"):
            raise ProtocolError("bad request line")
        self.method, self.path = parts[0], parts[1]
        return True

    def reply(self, status, body, ctype):
        self.status = status
        head = "HTTP/1.1 %d %s\r\nContent-Type: %s\r\nContent-Length: %d\r\nConnection: close\r\n\r\n" % (
            status, {200: "OK", 400: "Bad Request", 404: "Not Found", 411: "Length Required"}[status],
            ctype, len(body))
        self.wfile.write(head.encode() + body)

    def probe(self):
        self.nbytes = len(TOKEN)
        self.reply(200, TOKEN, "text/plain")

    def sink(self):
        timer = BodyTimer()
        if "chunked" in self.headers.get("transfer-encoding", "").lower():
            read_chunked(self.rfile, timer)
        elif "content-length" in self.headers:
            try:
                length = int(self.headers["content-length"])
            except ValueError:
                length = -1
            if length < 0:
                raise ProtocolError("bad Content-Length")
            read_fixed(self.rfile, length, timer)
        else:
            self.reply(411, b"Content-Length or chunked body required\n", "text/plain")
            return
        self.nbytes = timer.total
        self.reply(200, json.dumps(timer.summary()).encode(), "application/json")

    def source(self):
        query = parse_qs(urlparse(self.path).query)
        try:
            seconds = float(query["seconds"][0])
            max_bps = float(query["max_bps"][0]) if "max_bps" in query else None
        except (KeyError, ValueError):
            raise ProtocolError("want seconds=S (number) and optionally max_bps=R (number)")
        if not 1 <= seconds <= MAX_SOURCE_S:
            raise ProtocolError("seconds must be within 1..%d" % MAX_SOURCE_S)
        if max_bps is not None and not 0 < max_bps < float("inf"):
            raise ProtocolError("max_bps must be a positive finite number")
        self.status = 200
        self.wfile.write(b"HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\n"
                         b"Transfer-Encoding: chunked\r\nConnection: close\r\n\r\n")
        frame, pacer, start = chunk_frame(os.urandom(CHUNK)), Pacer(max_bps), time.monotonic()
        while time.monotonic() - start < seconds:
            self.wfile.write(frame)
            self.nbytes += CHUNK
            pacer.after(CHUNK)
        self.wfile.write(b"0\r\n\r\n")


class Server(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True
    request_queue_size = 64

    def __init__(self, address, tls_context, log_fp):
        socketserver.TCPServer.__init__(self, address, Handler)
        self.tls_context, self.log_fp, self.log_lock = tls_context, log_fp, threading.Lock()

    def process_request_thread(self, request, client_address):
        if self.tls_context is not None:             # handshake in the connection's own thread
            try:
                request.settimeout(10)
                request = self.tls_context.wrap_socket(request, server_side=True)
            except (ssl.SSLError, OSError) as exc:
                self.log_request("-", "-", None, 0, "tls handshake failed: %s" % exc)
                self.shutdown_request(request)
                return
        socketserver.ThreadingMixIn.process_request_thread(self, request, client_address)

    def log_request(self, method, path, status, nbytes, error):
        entry = {"time": utc_stamp(), "method": method, "path": path, "status": status, "bytes": nbytes}
        if error:
            entry["error"] = error
        line = json.dumps(entry) + "\n"
        with self.log_lock:
            if self.log_fp is not None:
                self.log_fp.write(line)
                self.log_fp.flush()
            if error:
                sys.stderr.write(line)
                sys.stderr.flush()


def parse_listen(value):
    host, _, port = value.rpartition(":")
    try:
        addr, number = ipaddress.ip_address(host), int(port)
    except ValueError:
        raise ValueError("--listen wants 127.0.0.1:PORT, got %r" % value)
    if addr.version != 4 or not addr.is_loopback:
        raise ValueError("refusing to listen on %s: only IPv4 loopback (127.0.0.0/8) is allowed" % host)
    if not 1 <= number <= 65535:
        raise ValueError("bad port in --listen %r (port 0 would hide the port from every client)" % value)
    return host, number


def cmd_serve(args, address):
    context = None
    if args.tls_cert:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(args.tls_cert, args.tls_key)
    try:
        log_fp = open(args.log, "a") if args.log else None
        server = Server(address, context, log_fp)
    except OSError as exc:
        sys.stderr.write("path_rate: cannot start: %s\n" % exc)
        return 1
    stopper = threading.Timer(args.max_seconds, server.shutdown)
    stopper.daemon = True
    stopper.start()
    sys.stderr.write("%s path_rate serving %s:%d (%s) for up to %d s\n" % (
        utc_stamp(), address[0], address[1], "tls" if context else "plain", args.max_seconds))
    sys.stderr.flush()
    try:
        server.serve_forever(poll_interval=0.25)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


# ---------------------------------------------------------------- client

def tls_context(args):
    """Client context that trusts only --ca and checks the name; a CA that cannot be loaded is a TLS failure."""
    try:
        return ssl.create_default_context(cafile=args.ca)    # hostname check and verification stay ON
    except OSError as exc:
        raise ssl.SSLError("cannot load CA file %s: %s" % (args.ca, exc))


def connect(args):
    context = tls_context(args) if args.tls else None
    sock = socket.create_connection((args.host, args.port), args.timeout)
    if context is None:
        return sock
    try:
        return context.wrap_socket(sock, server_hostname=args.server_name)
    except BaseException:
        sock.close()
        raise


def host_header(args):
    return args.server_name if args.tls else "%s:%d" % (args.host, args.port)


def read_head(rfile):
    """Status code and lower-cased headers of an HTTP/1.x reply."""
    parts, headers = read_message_head(rfile)
    if parts is None:
        raise ProtocolError("connection closed before any reply")
    if len(parts) < 2 or not parts[0].startswith("HTTP/") or not parts[1].isdigit():
        raise ProtocolError("not an HTTP reply: %r" % " ".join(parts)[:60])
    return int(parts[1]), headers


def number(text):
    value = float(text)
    return int(value) if value == int(value) else value


def result(args, **fields):
    """The measure line. An OrderedDict because the job parses the line with sed, key order included."""
    out = collections.OrderedDict(zip(FIELDS, (
        args.forward, args.direction, False, number(args.seconds), number(args.max_bps) if args.max_bps else None,
        0, 0.0, None, "receiver-vm" if args.direction == "up" else "receiver-job", None, "")))
    for key in fields:
        assert key in out, key
    out.update(fields)
    return out


def early_reply(rfile):
    try:
        return "far end had answered HTTP %d" % read_head(rfile)[0]
    except (ProtocolError, OSError) as exc:
        return "no reply to read (%s)" % exc


def measure_up(args, sock):
    request = ("POST /s1/sink HTTP/1.1\r\nHost: %s\r\nTransfer-Encoding: chunked\r\n"
               "Content-Type: application/octet-stream\r\nConnection: close\r\n\r\n" % host_header(args))
    frame, pacer, sent = chunk_frame(os.urandom(CHUNK)), Pacer(args.max_bps), 0
    rfile = sock.makefile("rb")
    start = time.monotonic()
    try:
        sock.sendall(request.encode())
        while time.monotonic() - start < args.seconds:
            sock.sendall(frame)
            sent += CHUNK
            pacer.after(CHUNK)
        sock.sendall(b"0\r\n\r\n")
    except (BrokenPipeError, ConnectionResetError) as exc:    # the far end answered early, e.g. 404
        return result(args, detail="send broke after %d bytes (%s); %s" % (sent, exc, early_reply(rfile)))
    status, headers = read_head(rfile)
    length = int(headers.get("content-length", "0"))
    reply = rfile.read(length)
    if len(reply) != length:
        raise ProtocolError("sink reply ended after %d of %d bytes" % (len(reply), length))
    sender_s = time.monotonic() - start
    if status != 200:
        return result(args, sender_s=sender_s, detail="sink answered HTTP %d" % status)
    sink = json.loads(reply.decode())
    got = {"bytes": int(sink["bytes"]), "duration_s": float(sink["duration_s"]),
           "bps": None if sink["bps"] is None else float(sink["bps"])}
    ok = got["bytes"] == sent and got["bps"] is not None
    detail = "" if ok else "sink counted %d bytes, client sent %d, bps %s" % (got["bytes"], sent, got["bps"])
    return result(args, ok=ok, sender_s=sender_s, detail=detail, **got)


def measure_down(args, sock):
    query = "seconds=%s" % number(args.seconds) + ("&max_bps=%s" % number(args.max_bps) if args.max_bps else "")
    sock.sendall(("GET /s1/source?%s HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n" % (
        query, host_header(args))).encode())
    rfile = sock.makefile("rb")
    status, headers = read_head(rfile)
    if status != 200:
        return result(args, detail="source answered HTTP %d" % status)
    timer, broke = BodyTimer(), ""
    try:
        read_chunked(rfile, timer)
    except (ProtocolError, OSError) as exc:
        broke = "stream broke after %d bytes: %s" % (timer.total, exc)
    done = timer.summary()
    ok = not broke and done["bps"] is not None
    return result(args, ok=ok, bytes=done["bytes"], duration_s=done["duration_s"], bps=done["bps"] if ok else None,
                  detail=broke or ("" if ok else "no body bytes received"))


def classify_failure(exc, timeout):
    """(far_end, detail) for a connect, handshake or transfer failure, or None when exc is not one.

    closed: the far end hung up or reset, also before or during the TLS handshake (behind an ssh
    forward, nothing answering shows up as exactly that). tls_failed: the certificate did not verify,
    or the far end answered with bytes that are not TLS. refused / timeout as named.
    """
    text = "%s: %s" % (type(exc).__name__, exc)
    if isinstance(exc, socket.timeout) or (isinstance(exc, ssl.SSLError) and "timed out" in str(exc)):
        return "timeout", "no answer within %s s (%s)" % (timeout, text)
    if isinstance(exc, ConnectionRefusedError):
        return "refused", "connection refused"
    if isinstance(exc, ssl.CertificateError):            # also what Python 3.6 raises for a name mismatch
        return "tls_failed", text
    if isinstance(exc, ssl.SSLError):
        hung_up = isinstance(exc, (ssl.SSLEOFError, ssl.SSLZeroReturnError)) or \
            "EOF" in str(getattr(exc, "reason", "")).upper()       # OpenSSL 3 names it as a reason
        return ("closed", "connection closed during the TLS handshake (%s)" % text) if hung_up else ("tls_failed", text)
    if isinstance(exc, OSError):                                   # reset, broken pipe, unreachable ...
        return "closed", text
    return None


def cmd_measure(args):
    try:
        sock = connect(args)
        try:
            out = (measure_up if args.direction == "up" else measure_down)(args, sock)
        finally:
            sock.close()
    except Exception as exc:        # the contract is one JSON line even on failure, so nothing escapes
        verdict = classify_failure(exc, args.timeout)
        out = result(args, detail="%s: %s" % verdict if verdict else "%s: %s" % (type(exc).__name__, exc))
    print(json.dumps(out))
    return 0 if out["ok"] else 1


def classify_reply(buf, eof):
    """far_end and detail for the bytes read so far, or None when more are needed."""
    if not buf:
        return ("closed", "connection closed before any reply") if eof else None
    if not (buf.startswith(b"HTTP/") or (len(buf) < 5 and b"HTTP/".startswith(buf))):
        return "data", "reply is not HTTP: %r" % buf[:40]
    end = buf.find(b"\r\n\r\n")
    if end < 0:
        return ("data", "reply ended inside the headers") if eof else None
    head = buf[:end].decode("latin-1").split("\r\n")
    status = head[0].split()[1] if len(head[0].split()) > 1 else ""
    if status != "200":
        return "http_error", "HTTP status %r" % status
    body = buf[end + 4:]
    lengths = [h.split(":", 1)[1].strip() for h in head[1:] if h.lower().startswith("content-length:")]
    complete = eof or (lengths and lengths[0].isdigit() and len(body) >= int(lengths[0]))
    if not complete:
        return None                              # without a Content-Length the body ends at EOF
    return ("token", "") if body == TOKEN else ("data", "HTTP 200 with body %r" % body[:40])


def probe_once(args):
    sock = connect(args)
    try:
        sock.sendall(("GET /s1/probe HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n" % host_header(args)).encode())
        buf, eof = b"", False
        while True:
            verdict = classify_reply(buf, eof)
            if verdict:
                return verdict
            try:
                data = sock.recv(4096)
            except socket.timeout:
                return ("data", "partial reply, then silence: %r" % buf[:40]) if buf else ("timeout", "no reply")
            except ConnectionError:
                data = b""
            buf, eof = buf + data, not data
    finally:
        sock.close()


def cmd_probe(args):
    start = time.monotonic()
    try:
        far_end, detail = probe_once(args)
    except (OSError, ssl.CertificateError) as exc:    # CertificateError is a ValueError on Python 3.6
        far_end, detail = classify_failure(exc, args.timeout)
    line = collections.OrderedDict([("far_end", far_end), ("ms", int((time.monotonic() - start) * 1000)),
                                    ("detail", detail)])
    print(json.dumps(line))
    return 0 if far_end == "token" else 1


# ---------------------------------------------------------------- command line

def float_arg(low, high):
    """argparse type: a number within [low, high] (so never nan or inf)."""
    def parse(text):
        try:
            value = float(text)
        except ValueError:
            raise argparse.ArgumentTypeError("not a number: %r" % text)
        if not low <= value <= high:
            raise argparse.ArgumentTypeError("must be within %g..%g" % (low, high))
        return value
    return parse


def add_client_args(parser):
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--tls", action="store_true", help="HTTPS; needs --ca and --server-name")
    parser.add_argument("--ca", help="CA file the server certificate must chain to")
    parser.add_argument("--server-name", help="name the certificate must carry (not the address connected to)")
    parser.add_argument("--timeout", type=float_arg(0.1, 3600), default=30.0, help="connect and per-read timeout (default 30 s)")


def build_parser():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd")
    sub.required = True
    serve = sub.add_parser("serve", help="loopback HTTP(S) server")
    serve.add_argument("--listen", required=True, help="127.0.0.1:PORT (loopback only)")
    serve.add_argument("--tls-cert")
    serve.add_argument("--tls-key")
    serve.add_argument("--max-seconds", type=int, default=10800, help="exit by itself after this long (default 10800)")
    serve.add_argument("--log", help="append one JSON line per request (never the body)")
    meas = sub.add_parser("measure", help="timed bulk transfer, one JSON line")
    add_client_args(meas)
    meas.add_argument("--forward", required=True, help="label copied into the result")
    meas.add_argument("--direction", required=True, choices=("up", "down"))
    meas.add_argument("--seconds", required=True, type=float_arg(1, MAX_SOURCE_S))
    meas.add_argument("--max-bps", type=float_arg(1, 1e12), help="pace the sender to this many bytes/s")
    probe = sub.add_parser("probe", help="one GET /s1/probe, one JSON line")
    add_client_args(probe)
    return ap


def main(argv):
    ap = build_parser()
    args = ap.parse_args(argv)
    if args.cmd == "serve":
        if bool(args.tls_cert) != bool(args.tls_key):
            ap.error("--tls-cert and --tls-key go together")
        if args.max_seconds < 1:
            ap.error("--max-seconds must be at least 1")
        try:
            address = parse_listen(args.listen)
        except ValueError as exc:
            ap.error(str(exc))
        return cmd_serve(args, address)
    if args.tls and not (args.ca and args.server_name):
        ap.error("--tls needs --ca and --server-name")
    return (cmd_measure if args.cmd == "measure" else cmd_probe)(args)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
