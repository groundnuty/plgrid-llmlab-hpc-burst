#!/usr/bin/env python3
"""Watch an instance through frps tcpmux (spike S1). Stdlib only, Python 3.6 compatible.

Runs on the frps host. It reaches the instance the way the front would: a TCP connection to
frps' tcpmux port, "CONNECT <instance>:80", then plain HTTP over the piped connection. It

  * keeps a long GET /stream?seconds=N open and records every inter-token gap, every cut and
    every reconnect. With --streams N it keeps N of them open at once, over the same frps tcpmux
    route (so through the same frpc), and records gaps and cuts per stream; a stream that is cut
    is reopened on its own lane, as the single stream always was;
  * probes GET /health on a fresh tunnel connection every few seconds, classifies failures and
    declares the path down after --fail-threshold failures in a row;
  * takes a mark (SIGUSR1) at the moment you inject a fault, and reports time-to-detect from it;
  * with --stub-log, also reads plugin_stub.py's log: CloseProxy, Ping and Login after each mark;
  * with --direct-url, measures the same /health calls directly and reports the added latency;
  * writes a JSON summary (--out) and prints a short digest.

Failure classes of one probe:
  ok               HTTP 200 from /health through the tunnel
  path_unreachable the TCP connection to frps' tcpmux port failed or timed out
  path_down        frps answered the CONNECT with a non-200 (no proxy is registered under the name)
  no_response      CONNECT was accepted, then EOF, reset or silence before any HTTP response:
                   the agent or the engine behind it is dead or hung. Pings still arriving in the
                   stub log mean the agent is alive and the engine is down.
  http_error       an HTTP status other than 200

    stream_watch.py --tcpmux 127.0.0.1:PORT --domain NAME --duration 3900 [--streams 4] --out summary.json
    kill -USR1 <pid>      # mark: run it right before you inject a fault
"""
import argparse
import calendar
import datetime
import json
import os
import signal
import socket
import sys
import threading
import time
from urllib.parse import urlparse


class Fail(Exception):
    def __init__(self, cls, detail):
        Exception.__init__(self, "%s: %s" % (cls, detail))
        self.cls = cls
        self.detail = detail


# --- small helpers ------------------------------------------------------------------------------

def utc(epoch):
    return datetime.datetime.fromtimestamp(epoch, datetime.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def parse_utc(text):
    """'2026-10-30T12:00:00.123Z' -> epoch seconds."""
    base = calendar.timegm(time.strptime(text[:19], "%Y-%m-%dT%H:%M:%S"))
    frac = text[19:].rstrip("Z")
    return base + (float("0" + frac) if frac.startswith(".") else 0.0)


def pct(values, q):
    if not values:
        return None
    ordered = sorted(values)
    idx = min(len(ordered) - 1, int(round(q * (len(ordered) - 1))))
    return ordered[idx]


def stats(values, digits=3):
    if not values:
        return {"n": 0}
    return {"n": len(values), "min": round(min(values), digits), "mean": round(sum(values) / len(values), digits),
            "p50": round(pct(values, 0.50), digits), "p95": round(pct(values, 0.95), digits),
            "p99": round(pct(values, 0.99), digits), "max": round(max(values), digits)}


def remaining(deadline):
    left = deadline - time.monotonic()
    if left <= 0:
        raise socket.timeout("deadline passed")
    return left


# --- connections ----------------------------------------------------------------------------------

class Target(object):
    """Opens a connection to the engine's HTTP server, through frps tcpmux or directly."""

    def __init__(self, kind, host, port, domain=None):
        self.kind, self.host, self.port, self.domain = kind, host, port, domain

    def open(self, deadline):
        """Return (sock, reader). Raises Fail with a class."""
        try:
            sock = socket.create_connection((self.host, self.port), timeout=remaining(deadline))
        except (OSError, socket.timeout) as exc:
            raise Fail("path_unreachable", "%s: %s" % (exc.__class__.__name__, exc))
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        rd = sock.makefile("rb")
        if self.kind == "direct":
            return sock, rd
        try:
            sock.settimeout(remaining(deadline))
            req = "CONNECT %s:80 HTTP/1.1\r\nHost: %s:80\r\n\r\n" % (self.domain, self.domain)
            sock.sendall(req.encode("ascii"))
            line = rd.readline(65536)
            if not line:
                raise Fail("path_unreachable", "EOF after CONNECT")
            parts = line.split(None, 2)
            code = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else -1
            if code != 200:
                raise Fail("path_down", "CONNECT -> %s" % line.strip().decode("latin-1"))
            while True:  # skip the response headers of the CONNECT
                hl = rd.readline(65536)
                if not hl:
                    raise Fail("path_unreachable", "EOF in CONNECT response headers")
                if hl in (b"\r\n", b"\n"):
                    break
        except Fail:
            close_quiet(sock, rd)
            raise
        except (OSError, socket.timeout, ValueError) as exc:
            close_quiet(sock, rd)
            raise Fail("path_unreachable", "CONNECT: %s: %s" % (exc.__class__.__name__, exc))
        return sock, rd


def close_quiet(sock, rd=None):
    for obj in (rd, sock):
        try:
            if obj is not None:
                obj.close()
        except OSError:
            pass


def send_get(sock, host_header, path):
    req = "GET %s HTTP/1.1\r\nHost: %s\r\nAccept: */*\r\nConnection: keep-alive\r\n\r\n" % (path, host_header)
    sock.sendall(req.encode("ascii"))


def read_head(rd):
    """Read an HTTP response head. Returns (status, headers). Raises Fail('no_response')."""
    try:
        line = rd.readline(65536)
        if not line:
            raise Fail("no_response", "EOF before the HTTP status line")
        parts = line.split(None, 2)
        status = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else -1
        headers = {}
        while True:
            hl = rd.readline(65536)
            if not hl:
                raise Fail("no_response", "EOF inside the HTTP headers")
            if hl in (b"\r\n", b"\n"):
                break
            key, _, val = hl.decode("latin-1").partition(":")
            headers[key.strip().lower()] = val.strip()
        return status, headers
    except (OSError, socket.timeout) as exc:
        raise Fail("no_response", "%s: %s" % (exc.__class__.__name__, exc))


def read_crlf_line(rd, what):
    """One line that must end in a newline. EOF before the newline is a truncated reply."""
    line = rd.readline(1024)
    if not line.endswith(b"\n"):
        raise Fail("no_response", "EOF in %s" % what)
    return line


def read_trailers(rd):
    """After the zero chunk: trailer lines up to the blank line. EOF before the blank line is a cut."""
    while True:
        line = read_crlf_line(rd, "the trailer after the final chunk")
        if line in (b"\r\n", b"\n"):
            return


def read_body(rd, headers):
    """A complete HTTP/1.1 response body, Content-Length or chunked. Any premature EOF, short body
    or framing error raises Fail('no_response' for a cut, 'http_error' for a malformed reply)."""
    if "chunked" in headers.get("transfer-encoding", "").lower():
        parts = []
        while True:
            size_line = read_crlf_line(rd, "a chunk size line")
            try:
                size = int(size_line.split(b";")[0].strip(), 16)
            except ValueError:
                raise Fail("http_error", "bad chunk size %r" % size_line[:20])
            if size == 0:
                read_trailers(rd)
                return b"".join(parts)
            data = rd.read(size)
            if len(data) != size:
                raise Fail("no_response", "EOF after %d of %d bytes of a chunk" % (len(data), size))
            if rd.read(2) != b"\r\n":
                raise Fail("no_response", "chunk not followed by CRLF (EOF or bad framing)")
            parts.append(data)
    length = headers.get("content-length")
    if length is None or not length.isdigit():
        raise Fail("http_error", "reply has neither a valid Content-Length nor chunked framing")
    n = int(length)
    body = rd.read(n) if n else b""
    if len(body) != n:
        raise Fail("no_response", "EOF after %d of %d body bytes" % (len(body), n))
    return body


def http_get_health(target, deadline, path, host_header, sock_rd=None, expect=None):
    """One GET on a fresh connection (or on sock_rd). The reply must be complete: status 200, the whole
    body as framed, and, when expect (bytes) is given, exactly that body. Returns (total_s, request_s,
    sock, rd). Raises Fail."""
    t0 = time.monotonic()
    if sock_rd is None:
        sock, rd = target.open(deadline)
    else:
        sock, rd = sock_rd
    t1 = time.monotonic()
    try:
        sock.settimeout(remaining(deadline))
        send_get(sock, host_header, path)
        status, headers = read_head(rd)
        if status != 200:
            raise Fail("http_error", "status %s" % status)
        sock.settimeout(remaining(deadline))
        body = read_body(rd, headers)
        if expect is not None and body != expect:
            raise Fail("http_error", "unexpected body %r" % body[:60])
    except (OSError, socket.timeout) as exc:
        close_quiet(sock, rd)
        raise Fail("no_response", "%s: %s" % (exc.__class__.__name__, exc))
    except Fail:
        close_quiet(sock, rd)
        raise
    t2 = time.monotonic()
    return t2 - t0, t2 - t1, sock, rd


def measure(target, n, path, host_header, timeout=5.0, expect=None):
    """n GETs on fresh connections, then n GETs on one kept-alive connection. Times in ms."""
    fresh, warm, errors = [], [], 0
    for _ in range(n):
        try:
            total, _, sock, rd = http_get_health(target, time.monotonic() + timeout, path, host_header,
                                                 expect=expect)
            close_quiet(sock, rd)
            fresh.append(total * 1000.0)
        except Fail:
            errors += 1
            if errors >= 3:          # clearly down: do not spend minutes timing out
                return {"new_connection_ms": stats(fresh, 2), "kept_alive_request_ms": stats(warm, 2),
                        "errors": errors, "aborted": True}
    try:
        sock_rd = target.open(time.monotonic() + timeout)
        for _ in range(n):
            _, req_s, sock, rd = http_get_health(target, time.monotonic() + timeout, path, host_header, sock_rd,
                                                 expect=expect)
            warm.append(req_s * 1000.0)
        close_quiet(*sock_rd)
    except Fail:
        errors += 1
    return {"new_connection_ms": stats(fresh, 2), "kept_alive_request_ms": stats(warm, 2), "errors": errors}


# --- shared state ---------------------------------------------------------------------------------

class State(object):
    def __init__(self, args):
        self.args = args
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.marks = []                 # epoch seconds, from SIGUSR1
        self.mark_pre = {}              # mark epoch -> what the probe said at that moment (see precondition)
        self.streams = []               # one dict per stream attempt that opened
        self.gaps = []                  # (epoch, gap_s, seq, stream_id, lane)
        self.offsets = []               # receive time minus the token's own UTC stamp
        self.connect_fail = {}          # class -> {count, first_utc, last_utc}: attempts that never delivered a token
        self.probes = {"total": 0, "ok": 0}
        self.probe_fail_classes = {}
        self.probe_ms = []
        self.probe_req_ms = []
        self.probe_failures = []        # capped list of failing probes
        self.consec_fail = 0
        self.first_fail = None
        self.last_ok_epoch = None
        self.down = False
        self.transitions = []
        self.current = {}               # lane -> (sock, reader) of the stream that lane is reading

    def precondition(self, epoch):
        """What the /health probe says at 'epoch': 'up' only when the last probe was good and recent.
        'down' (declared down), 'failing' (a probe failed, not yet declared), 'stale' (no good probe
        lately) or 'never_up'. A fault injected from anything but 'up' is not a measurement."""
        a = self.args
        if self.last_ok_epoch is None:
            return "never_up"
        if self.down:
            return "down"
        if self.consec_fail > 0:
            return "failing"
        if epoch - self.last_ok_epoch > max(2 * a.health_interval + a.health_timeout, 5.0):
            return "stale"
        return "up"

    def add_mark(self, epoch):
        """Called from the SIGUSR1 handler, so it takes no lock: the state is written before the mark
        becomes visible in self.marks."""
        self.mark_pre[epoch] = self.precondition(epoch)
        self.marks.append(epoch)

    def note_open_failure(self, cls):
        """A stream attempt that never delivered a token. Caller holds self.lock."""
        now = time.time()
        entry = self.connect_fail.setdefault(cls, {"count": 0, "first_utc": utc(now)})
        entry["count"] += 1
        entry["last_utc"] = utc(now)

    def record_probe(self, epoch, ok, cls, total_s, req_s, detail):
        with self.lock:
            self.probes["total"] += 1
            if ok:
                self.probes["ok"] += 1
                self.last_ok_epoch = epoch
                self.probe_ms.append(total_s * 1000.0)
                self.probe_req_ms.append(req_s * 1000.0)
                if self.down:
                    self.transitions.append({"state": "up", "utc": utc(epoch), "epoch": round(epoch, 3)})
                    self.down = False
                    self.announce("probe: path UP again at %s" % utc(epoch))
                self.consec_fail = 0
                self.first_fail = None
                return
            self.probe_fail_classes[cls] = self.probe_fail_classes.get(cls, 0) + 1
            if len(self.probe_failures) < 500:
                self.probe_failures.append({"utc": utc(epoch), "epoch": round(epoch, 3), "class": cls,
                                            "detail": detail[:160]})
            self.consec_fail += 1
            if self.consec_fail == 1:
                self.first_fail = epoch
            if self.consec_fail >= self.args.fail_threshold and not self.down:
                self.down = True
                self.transitions.append({"state": "down", "class": cls, "first_fail_utc": utc(self.first_fail),
                                         "first_fail_epoch": round(self.first_fail, 3),
                                         "declared_utc": utc(epoch), "declared_epoch": round(epoch, 3)})
                self.announce("probe: path DOWN declared at %s (class %s)" % (utc(epoch), cls))

    def interrupt_streams(self):
        """Shut down the socket of every stream being read: the blocked reads return EOF and each lane
        marks its stream interrupted. It takes no lock, because the SIGTERM handler calls it in the main
        thread, which may hold self.lock; list() copies the dict in one step."""
        for cur in list(self.current.values()):
            try:
                cur[0].shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    def announce(self, text):
        """One line on stdout, for the operator who waits for 'path UP' before the next fault."""
        print("stream_watch: " + text, flush=True)


# --- stream lanes ---------------------------------------------------------------------------------
# Lane 1 runs on the main thread (so one stream behaves exactly as it always did), lanes 2..N on daemon threads.

def run_stream(st, target, sid, seconds, lane=1):
    a = st.args
    rec = {"id": sid, "lane": lane, "requested_s": seconds, "start_utc": utc(time.time()), "tokens": 0, "seq_breaks": 0,
           "ttfb_s": None, "completed": False, "interrupted": False, "reason": None}
    t0 = time.monotonic()
    sock = rd = None
    try:
        sock, rd = target.open(time.monotonic() + a.connect_timeout)
    except Fail as exc:
        with st.lock:
            st.note_open_failure(exc.cls)
        return None
    rec["start_utc"] = utc(time.time())
    st.current[lane] = (sock, rd)
    try:
        sock.settimeout(a.stall_timeout)
        send_get(sock, target.domain or "engine", "/stream?seconds=%d" % seconds)
        status, headers = read_head(rd)
        if status != 200:
            raise Fail("http_error", "stream status %s" % status)
        if "chunked" not in headers.get("transfer-encoding", "").lower():
            raise Fail("http_error", "reply is not chunked")
        expect, last = 1, None
        while True:
            size_line = rd.readline(1024)
            if not size_line.endswith(b"\n"):
                raise Fail("cut", "EOF before the final chunk" if not size_line else "EOF inside a chunk size line")
            try:
                size = int(size_line.split(b";")[0].strip(), 16)
            except ValueError:
                raise Fail("cut", "bad chunk size %r" % size_line[:20])
            if size == 0:
                while True:                      # trailers, up to the blank line; EOF here is a cut
                    tl = rd.readline(1024)
                    if not tl.endswith(b"\n"):
                        raise Fail("cut", "EOF before the blank line that ends the stream")
                    if tl in (b"\r\n", b"\n"):
                        break
                break
            data = rd.read(size)
            if len(data) != size:
                raise Fail("cut", "short chunk: %d of %d bytes" % (len(data), size))
            if rd.read(2) != b"\r\n":
                raise Fail("cut", "chunk not followed by CRLF (EOF or bad framing)")
            now_m, now_e = time.monotonic(), time.time()
            for line in data.decode("ascii", "replace").splitlines():
                parts = line.split()
                if len(parts) != 3 or parts[0] != "token":
                    continue
                seq = int(parts[1])
                rec["tokens"] += 1
                if seq != expect:
                    rec["seq_breaks"] += 1
                expect = seq + 1
                try:
                    off = now_e - parse_utc(parts[2])
                except ValueError:
                    off = None
                with st.lock:
                    if off is not None:
                        st.offsets.append(off)
                    if last is None:
                        rec["ttfb_s"] = round(now_m - t0, 3)
                    else:
                        st.gaps.append((now_e, now_m - last, seq, sid, lane))
                last = now_m
        rec["completed"] = rec["tokens"] == seconds and rec["seq_breaks"] == 0
        rec["reason"] = None if rec["completed"] else "token count %d of %d, seq breaks %d" % (
            rec["tokens"], seconds, rec["seq_breaks"])
    except socket.timeout:
        rec["reason"] = "stall: no data for %.0f s" % a.stall_timeout
    except Fail as exc:
        rec["reason"] = "%s: %s" % (exc.cls, exc.detail)
    except (OSError, ValueError) as exc:
        rec["reason"] = "%s: %s" % (exc.__class__.__name__, exc)
    finally:
        st.current.pop(lane, None)
        close_quiet(sock, rd)
    if st.stop.is_set() and not rec["completed"]:
        rec["interrupted"] = True                    # the watcher was told to stop: not a cut
    rec["end_utc"] = utc(time.time())
    rec["end_epoch"] = round(time.time(), 3)
    rec["duration_s"] = round(time.monotonic() - t0, 3)
    with st.lock:
        if rec["tokens"] == 0 and not rec["completed"]:
            # Never delivered a token: an open failure (the path or the engine is down), not a cut stream.
            st.note_open_failure((rec["reason"] or "unknown").split(":")[0])
            return None
        st.streams.append(rec)
    return rec


def stream_loop(st, target, t_end_mono, lane=1):
    a = st.args
    sid = 0
    while not st.stop.is_set():
        left = int(t_end_mono - time.monotonic())
        if left < 4:
            break
        seconds = max(1, min(a.stream_seconds, left - 2))
        sid += 1
        rec = run_stream(st, target, sid, seconds, lane)
        if rec is None:
            sid -= 1                                # nothing opened: not a stream
        st.stop.wait(1.0)                            # fixed 1 s retry spacing


# --- health thread --------------------------------------------------------------------------------

def health_loop(st, target, t_end_mono):
    a = st.args
    host_header = target.domain or "engine"
    nxt = time.monotonic()
    while not st.stop.is_set() and time.monotonic() < t_end_mono:
        t0 = time.monotonic()
        try:
            total, req, sock, rd = http_get_health(target, t0 + a.health_timeout, a.health_path, host_header,
                                                   expect=a.health_expect_bytes)
            close_quiet(sock, rd)
            st.record_probe(time.time(), True, "ok", total, req, "")
        except Fail as exc:
            st.record_probe(time.time(), False, exc.cls, time.monotonic() - t0, 0.0, exc.detail)
        nxt += a.health_interval
        pause = nxt - time.monotonic()
        if pause < 0:
            nxt = time.monotonic()
            pause = 0
        st.stop.wait(pause)


# --- analysis -------------------------------------------------------------------------------------

def read_stub_log(path):
    out = []
    try:
        with open(path) as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    out.append(json.loads(line))
                except ValueError:
                    pass
    except OSError as exc:
        sys.stderr.write("stream_watch: cannot read --stub-log: %s\n" % exc)
    return out


def lane_report(lane, streams, gaps, threshold, near):
    """One lane (one of the --streams concurrent streams): its streams, cuts, completion and gaps.
    'first_stream_completed' is the lane's first stream (lowest id) having run to its full length uncut."""
    mine = [s for s in streams if s.get("lane", 1) == lane]
    lane_gaps = [(e, g) for (e, g, _, _, ln) in gaps if ln == lane]
    first = min(mine, key=lambda s: s["id"]) if mine else None
    unfaulted = [g for (e, g) in lane_gaps if not near(e)]
    done = [s for s in mine if s["completed"]]
    return {"lane": lane, "streams": len(mine), "completed": len(done),
            "cuts": sum(1 for s in mine if not s["completed"] and not s["interrupted"]),
            "first_stream_completed": bool(first and first["completed"]),
            "longest_completed_stream_s": max([s["duration_s"] for s in done] or [0]),
            "tokens": sum(s["tokens"] for s in mine),
            "max_gap_s": round(max([g for (_, g) in lane_gaps]), 3) if lane_gaps else None,
            "max_gap_unfaulted_s": round(max(unfaulted), 3) if unfaulted else None,
            "gaps_over_threshold_unfaulted": sum(1 for g in unfaulted if g > threshold)}


def analyse(st, started_epoch, ended_epoch, args):
    gaps = [g for g in st.gaps]
    near = lambda e: any(m <= e <= m + args.fault_window for m in st.marks)   # noqa: E731
    over = [{"utc": utc(e), "gap_s": round(g, 3), "seq": s, "stream": sid, "lane": lane, "near_mark": near(e)}
            for (e, g, s, sid, lane) in gaps if g > args.gap_threshold]
    unfaulted = [g for (e, g, s, sid, lane) in gaps if not near(e)]
    buckets = [("lt_1.5", 0, 1.5), ("1.5_to_2", 1.5, 2), ("2_to_5", 2, 5), ("5_to_10", 5, 10), ("ge_10", 10, 1e9)]
    hist = dict((name, sum(1 for (_, g, _, _, _) in gaps if lo <= g < hi)) for name, lo, hi in buckets)
    cuts = []
    for s in st.streams:
        if not s["completed"] and not s["interrupted"]:
            cuts.append({"stream": s["id"], "lane": s.get("lane", 1), "end_utc": s["end_utc"],
                         "after_s": s["duration_s"], "reason": s["reason"], "near_mark": near(s["end_epoch"])})
    reconnects = 0                    # a cut stream that was followed by another one on its lane
    for lane in range(1, args.streams + 1):
        mine = [x for x in st.streams if x.get("lane", 1) == lane]
        reconnects += sum(1 for s in mine[:-1] if not s["completed"] and not s["interrupted"])
    per_stream = [lane_report(lane, st.streams, gaps, args.gap_threshold, near) for lane in range(1, args.streams + 1)]
    completed = [s for s in st.streams if s["completed"]]
    stub = read_stub_log(args.stub_log) if args.stub_log else None
    domain_events = [e for e in (stub or []) if e.get("user") == args.domain]

    marks = []
    for i, m in enumerate(st.marks):
        hi = st.marks[i + 1] if i + 1 < len(st.marks) else float("inf")   # a mark's window ends at the next mark
        pre = st.mark_pre.get(m, "unknown")
        entry = {"mark_utc": utc(m), "precondition": pre, "valid": pre == "up"}
        if pre != "up":
            # A fault injected while the path was already down or failing is not a detection: the
            # watcher would have nothing to see go down. Record it as invalid and report no times.
            entry["invalid_reason"] = ("the /health probe said '%s' when the mark was taken, so any later "
                                       "failure is not a new down transition; repeat the fault from 'up'" % pre)
            marks.append(entry)
            continue
        down = next((t for t in st.transitions if t["state"] == "down" and m <= t["declared_epoch"] < hi), None)
        fail = next((p for p in st.probe_failures if m <= p["epoch"] < hi), None)
        if fail:
            entry["probe_first_failure_s"] = round(fail["epoch"] - m, 2)
        if down:
            entry["probe_declared_down_s"] = round(down["declared_epoch"] - m, 2)
            entry["probe_class"] = down["class"]
            up = next((t for t in st.transitions if t["state"] == "up" and t["epoch"] > down["declared_epoch"]), None)
            if up:
                entry["probe_recovered_s"] = round(up["epoch"] - m, 2)
        cut = next((s for s in st.streams if not s["completed"] and not s["interrupted"]
                    and m <= s["end_epoch"] < hi), None)
        if cut:
            entry["stream_cut_s"] = round(cut["end_epoch"] - m, 2)
        if stub is not None:
            close = next((e for e in domain_events if e["op"] == "CloseProxy" and m <= e["epoch"] < hi), None)
            login = next((e for e in domain_events if e["op"] == "Login" and m <= e["epoch"] < hi), None)
            entry["stub_close_proxy_s"] = round(close["epoch"] - m, 2) if close else None
            entry["stub_pings_in_30s_after"] = sum(1 for e in domain_events
                                                   if e["op"] == "Ping" and m <= e["epoch"] <= min(m + 30, hi))
            entry["stub_login_after_s"] = round(login["epoch"] - m, 2) if login else None
        marks.append(entry)

    summary = {
        "tool": "stream_watch", "python": sys.version.split()[0],
        "started_utc": utc(started_epoch), "ended_utc": utc(ended_epoch),
        "run_s": round(ended_epoch - started_epoch, 1),
        "target": {"tcpmux": "%s:%d" % (args.tcpmux_host, args.tcpmux_port), "domain": args.domain},
        "streams": st.streams,
        "streams_requested": args.streams,
        "per_stream": per_stream,
        "overall": {"streams": args.streams,
                    "streams_whose_first_stream_completed": sum(1 for r in per_stream if r["first_stream_completed"]),
                    "cuts": len(cuts),
                    "max_gap_s": round(max([g for (_, g, _, _, _) in gaps]), 3) if gaps else None,
                    "max_gap_unfaulted_s": round(max(unfaulted), 3) if unfaulted else None},
        "streams_completed": len(completed),
        "longest_completed_stream_s": max([s["duration_s"] for s in completed] or [0]),
        "cuts": cuts,
        "reconnects_after_cut": reconnects,
        "stream_open_failures": st.connect_fail,
        "gaps": {"threshold_s": args.gap_threshold, "stats_s": stats([g for (_, g, _, _, _) in gaps]),
                 "stats_unfaulted_s": stats(unfaulted), "over_threshold": over[:200],
                 "over_threshold_unfaulted": sum(1 for o in over if not o["near_mark"]), "histogram": hist},
        "clock_offset_plus_one_way_s": stats(st.offsets, 4),
        "health": {"interval_s": args.health_interval, "timeout_s": args.health_timeout,
                   "fail_threshold": args.fail_threshold, "probes": st.probes["total"], "ok": st.probes["ok"],
                   "fail_classes": st.probe_fail_classes,
                   "new_connection_ms": stats(st.probe_ms, 2), "kept_request_ms": stats(st.probe_req_ms, 2),
                   "transitions": st.transitions, "failures_sample": st.probe_failures[:60]},
        "marks": marks,
    }
    if stub is not None:
        logins = sum(1 for e in domain_events if e["op"] == "Login")
        summary["stub"] = {"events": len(domain_events), "logins": logins, "reconnects": max(0, logins - 1),
                           "ops": dict((op, sum(1 for e in domain_events if e["op"] == op))
                                       for op in sorted(set(e["op"] for e in domain_events)))}
    return summary


# --- main -----------------------------------------------------------------------------------------

def parse_args(argv):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--tcpmux", help="frps tcpmux HOST:PORT (on the frps host this is the loopback port)")
    ap.add_argument("--domain", help="instance name: the tcpmux custom domain")
    ap.add_argument("--duration", type=int, default=3900, help="seconds to run, counted from the first good probe (default 3900)")
    ap.add_argument("--wait-up", type=int, default=0,
                    help="wait up to this many seconds for the first good /health probe before the run clock "
                         "starts (start the watcher before the job; 0 = do not wait)")
    ap.add_argument("--stream-seconds", type=int, default=3720, help="seconds asked of each /stream (default 3720)")
    ap.add_argument("--streams", type=int, default=1,
                    help="how many /stream requests to keep open at once, all through the same tcpmux route and the "
                         "same frpc (default 1). A cut stream is reopened on its own lane. Gaps, cuts and completion "
                         "are recorded per stream")
    ap.add_argument("--gap-threshold", type=float, default=2.0, help="report token gaps above this (s)")
    ap.add_argument("--fault-window", type=float, default=60.0,
                    help="a gap or cut within this many seconds after a mark is attributed to that fault (default 60)")
    ap.add_argument("--stall-timeout", type=float, default=10.0, help="no data for this long = stream cut (s)")
    ap.add_argument("--connect-timeout", type=float, default=5.0, help="opening a stream connection (s)")
    ap.add_argument("--health-interval", type=float, default=2.0, help="seconds between /health probes")
    ap.add_argument("--health-timeout", type=float, default=3.0, help="one probe's total deadline (s)")
    ap.add_argument("--fail-threshold", type=int, default=2, help="consecutive failures that declare the path down")
    ap.add_argument("--health-path", default="/health")
    ap.add_argument("--health-expect", default='{"status": "ok"}',
                    help="the exact /health body; a different or truncated body is a failed probe "
                         "(default: the fake engine's; pass an empty string for an engine that answers with none)")
    ap.add_argument("--direct-url", help="http://HOST:PORT of the engine, reachable from here (loopback in a job)")
    ap.add_argument("--direct-samples", type=int, default=30)
    ap.add_argument("--direct-only", action="store_true", help="only measure --direct-url, then exit")
    ap.add_argument("--stub-log", help="plugin_stub.py's JSON-lines log, for CloseProxy/Ping/Login after marks")
    ap.add_argument("--out", default="stream_watch_summary.json")
    args = ap.parse_args(argv)
    if args.streams < 1:
        ap.error("--streams must be at least 1")
    args.health_expect_bytes = args.health_expect.encode("utf-8")
    if args.direct_only:
        if not args.direct_url:
            ap.error("--direct-only needs --direct-url")
    else:
        if not args.tcpmux or not args.domain:
            ap.error("--tcpmux and --domain are required")
        host, _, port = args.tcpmux.rpartition(":")
        if not host or not port.isdigit():
            ap.error("--tcpmux wants HOST:PORT")
        args.tcpmux_host, args.tcpmux_port = host, int(port)
    return args


def direct_target(url):
    u = urlparse(url)
    if u.scheme != "http" or not u.hostname or not u.port:
        raise SystemExit("--direct-url wants http://HOST:PORT")
    return Target("direct", u.hostname, u.port)


def write_json(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w") as fh:
        json.dump(obj, fh, indent=2, sort_keys=True)
        fh.write("\n")
    os.rename(tmp, path)


def digest(summary):
    lines = ["stream_watch: %s -> %s (%.0f s)" % (summary["started_utc"], summary["ended_utc"], summary["run_s"]),
             "  streams completed %d, longest %.0f s, cuts %d, reconnects after a cut %d" % (
                 summary["streams_completed"], summary["longest_completed_stream_s"], len(summary["cuts"]),
                 summary["reconnects_after_cut"]),
             "  token gaps: max %s s, max outside fault windows %s s, over %.1f s outside fault windows: %d" % (
                 summary["gaps"]["stats_s"].get("max"), summary["gaps"]["stats_unfaulted_s"].get("max"),
                 summary["gaps"]["threshold_s"], summary["gaps"]["over_threshold_unfaulted"]),
             "  probes %d ok %d, failure classes %s" % (summary["health"]["probes"], summary["health"]["ok"],
                                                          json.dumps(summary["health"]["fail_classes"], sort_keys=True))]
    if summary.get("streams_requested", 1) > 1:
        for r in summary["per_stream"]:
            lines.append("  stream %d: first stream completed %s, completed %d of %d, cuts %d, max gap %s s (outside fault windows %s s)" % (
                r["lane"], "yes" if r["first_stream_completed"] else "NO", r["completed"], r["streams"], r["cuts"],
                r["max_gap_s"], r["max_gap_unfaulted_s"]))
        o = summary["overall"]
        lines.append("  overall: %d streams, first stream completed on %d, cuts %d, max gap %s s (outside fault windows %s s)" % (
            o["streams"], o["streams_whose_first_stream_completed"], o["cuts"], o["max_gap_s"], o["max_gap_unfaulted_s"]))
    for m in summary["marks"]:
        lines.append("  mark " + json.dumps(m, sort_keys=True))
    lines.append("  this digest is collection, not a verdict: run verdict.py on the summary for PASS or FAIL per criterion")
    if "added_latency_ms" in summary:
        lines.append("  added latency (tunnel minus direct, ms): " + json.dumps(summary["added_latency_ms"], sort_keys=True))
    return "\n".join(lines)


def wait_for_first_up(st, target):
    """Probe until /health answers once. Returns the seconds waited, or None on timeout."""
    a = st.args
    t0 = time.monotonic()
    while not st.stop.is_set() and time.monotonic() - t0 < a.wait_up:
        try:
            _, _, sock, rd = http_get_health(target, time.monotonic() + a.health_timeout, a.health_path,
                                             target.domain, expect=a.health_expect_bytes)
            close_quiet(sock, rd)
            return round(time.monotonic() - t0, 1)
        except Fail:
            st.stop.wait(a.health_interval)
    return None


def main(argv):
    args = parse_args(argv)
    if args.direct_only:
        res = measure(direct_target(args.direct_url), args.direct_samples, args.health_path, "engine",
                      expect=args.health_expect_bytes)
        res.update({"tool": "stream_watch", "mode": "direct-only", "utc": utc(time.time())})
        write_json(args.out, res)
        print("direct baseline: " + json.dumps(res, sort_keys=True))
        return 0 if res["errors"] == 0 else 1

    st = State(args)
    target = Target("tunnel", args.tcpmux_host, args.tcpmux_port, args.domain)
    signal.signal(signal.SIGUSR1, lambda s, f: st.add_mark(time.time()))

    def stop(signum, frame):
        # The handler runs in the main thread, possibly inside a read on the buffered reader, so it
        # must not close the reader (that raises "reentrant call"). It sets the flag and shuts the
        # sockets of all lanes down (interrupt_streams): each blocked read returns EOF, run_stream
        # marks its stream interrupted and closes both in its normal unwinding.
        st.stop.set()
        st.interrupt_streams()
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    waited = None
    if args.wait_up > 0:
        waited = wait_for_first_up(st, target)
        if waited is None:
            write_json(args.out, {"tool": "stream_watch", "never_up": True, "waited_s": args.wait_up,
                                  "utc": utc(time.time())})
            print("stream_watch: the instance never answered /health within %d s" % args.wait_up)
            return 3
    started = time.time()
    t_end = time.monotonic() + args.duration
    print("stream_watch: run clock started at %s (%s); ends after %d s" % (
        utc(started), "waited %s s for the first good probe" % waited if waited is not None else "no wait", args.duration),
        flush=True)
    pre = {}
    if args.direct_url:
        pre["direct"] = measure(direct_target(args.direct_url), args.direct_samples, args.health_path, "engine",
                                expect=args.health_expect_bytes)
    pre["tunnel"] = measure(target, args.direct_samples, args.health_path, args.domain,
                            expect=args.health_expect_bytes)

    th = threading.Thread(target=health_loop, args=(st, target, t_end))
    th.daemon = True
    th.start()
    lane_threads = []
    for lane in range(2, args.streams + 1):               # lane 1 stays on this thread
        lt = threading.Thread(target=stream_loop, args=(st, target, t_end, lane))
        lt.daemon = True
        lt.start()
        lane_threads.append(lt)
    stream_loop(st, target, t_end, 1)
    st.stop.wait(max(0.0, t_end - time.monotonic()) if not st.stop.is_set() else 0)
    st.stop.set()
    st.interrupt_streams()                                 # a stream still being read now is interrupted, not cut
    for lt in lane_threads:
        lt.join(timeout=args.stall_timeout + 2)
    th.join(timeout=args.health_timeout + 2)

    summary = analyse(st, started, time.time(), args)
    summary["latency_baseline"] = pre
    summary["waited_for_first_up_s"] = waited
    if "direct" in pre and pre["tunnel"]["new_connection_ms"].get("n") and pre["direct"]["new_connection_ms"].get("n"):
        summary["added_latency_ms"] = {
            "new_connection_p50": round(pre["tunnel"]["new_connection_ms"]["p50"] - pre["direct"]["new_connection_ms"]["p50"], 2),
            "kept_alive_request_p50": round(pre["tunnel"]["kept_alive_request_ms"]["p50"] - pre["direct"]["kept_alive_request_ms"]["p50"], 2)}
    write_json(args.out, summary)
    print(digest(summary))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
