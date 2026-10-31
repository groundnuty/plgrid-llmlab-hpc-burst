"""Tests of stream_watch.py: strict HTTP validation (B2), fault preconditions (B3), the signal handler (nit).

Expected values come from the review text and the task brief. Where the review reproduced a bug, the test
feeds the same bytes (Content-Length: 100 then EOF; a stream cut before its final blank line).
"""
import contextlib
import io
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _kit  # noqa: E402

sw = _kit.load("stream_watch")


class OneShotServer(object):
    """A loopback TCP server: for each connection, read one request head, send 'reply' (bytes), close."""

    def __init__(self, reply, keep_open=False):
        self.reply, self.keep_open = reply, keep_open
        self.srv = socket.socket()
        self.srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.srv.bind(("127.0.0.1", 0))
        self.srv.listen(8)
        self.port = self.srv.getsockname()[1]
        self.stop = False
        self.held = []
        t = threading.Thread(target=self.run)
        t.daemon = True
        t.start()

    def run(self):
        self.srv.settimeout(0.2)
        while not self.stop:
            try:
                conn, _ = self.srv.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            conn.settimeout(2)
            try:
                buf = b""
                while b"\r\n\r\n" not in buf:
                    chunk = conn.recv(4096)
                    if not chunk:
                        break
                    buf += chunk
                conn.sendall(self.reply)
            except OSError:
                pass
            if self.keep_open:
                self.held.append(conn)
            else:
                conn.close()

    def close(self):
        self.stop = True
        self.srv.close()
        for c in self.held:
            c.close()


def health(reply, expect=None):
    srv = OneShotServer(reply)
    try:
        target = sw.Target("direct", "127.0.0.1", srv.port)
        kw = {} if expect is None else {"expect": expect}
        res = sw.http_get_health(target, time.monotonic() + 3, "/health", "engine", **kw)
        sw.close_quiet(res[2], res[3])
        return res
    finally:
        srv.close()


OK_BODY = b'{"status": "ok"}'
OK_REPLY = b"HTTP/1.1 200 OK\r\nContent-Length: %d\r\n\r\n" % len(OK_BODY) + OK_BODY


class HealthIsStrict(unittest.TestCase):
    def test_a_complete_reply_with_the_exact_body_is_healthy(self):
        health(OK_REPLY, expect=OK_BODY)

    def test_content_length_100_then_eof_is_not_healthy(self):
        # the review reproduced HTTP 200, Content-Length: 100, EOF, counted healthy
        with self.assertRaises(sw.Fail) as cm:
            health(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n")
        self.assertEqual(cm.exception.cls, "no_response")

    def test_a_body_shorter_than_its_length_is_not_healthy(self):
        with self.assertRaises(sw.Fail):
            health(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n" + b"x" * 10)

    def test_a_different_body_is_not_healthy(self):
        body = b'{"status": "degraded"}'
        with self.assertRaises(sw.Fail) as cm:
            health(b"HTTP/1.1 200 OK\r\nContent-Length: %d\r\n\r\n" % len(body) + body, expect=OK_BODY)
        self.assertEqual(cm.exception.cls, "http_error")

    def test_a_non_200_status_is_not_healthy(self):
        with self.assertRaises(sw.Fail) as cm:
            health(b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n")
        self.assertEqual(cm.exception.cls, "http_error")

    def test_a_reply_with_no_framing_is_not_healthy(self):
        with self.assertRaises(sw.Fail):
            health(b"HTTP/1.1 200 OK\r\n\r\n" + OK_BODY)

    def test_a_complete_chunked_reply_is_healthy(self):
        reply = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n%x\r\n" % len(OK_BODY) + OK_BODY + b"\r\n0\r\n\r\n"
        health(reply, expect=OK_BODY)

    def test_a_chunked_reply_cut_before_the_final_blank_line_is_not_healthy(self):
        reply = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n%x\r\n" % len(OK_BODY) + OK_BODY + b"\r\n0\r\n"
        with self.assertRaises(sw.Fail):
            health(reply, expect=OK_BODY)


def token_chunks(n):
    out = b""
    for i in range(1, n + 1):
        line = ("token %d 2026-10-30T12:00:%02d.000Z\n" % (i, i % 60)).encode("ascii")
        out += b"%x\r\n" % len(line) + line + b"\r\n"
    return out


STREAM_HEAD = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nContent-Type: text/plain\r\n\r\n"


def run_stream_against(reply, seconds):
    srv = OneShotServer(reply)
    try:
        args = sw.parse_args(["--tcpmux", "127.0.0.1:1", "--domain", "x", "--stall-timeout", "3"])
        st = sw.State(args)
        target = sw.Target("direct", "127.0.0.1", srv.port)
        sw.run_stream(st, target, 1, seconds)
        return st.streams[0]
    finally:
        srv.close()


class StreamFramingIsStrict(unittest.TestCase):
    def test_a_complete_stream_completes(self):
        rec = run_stream_against(STREAM_HEAD + token_chunks(3) + b"0\r\n\r\n", 3)
        self.assertTrue(rec["completed"], rec)

    def test_eof_before_the_final_blank_line_is_a_cut(self):
        # the review reproduced completed:true for this stream
        rec = run_stream_against(STREAM_HEAD + token_chunks(3) + b"0\r\n", 3)
        self.assertFalse(rec["completed"], rec)
        self.assertIn("blank line", rec["reason"])

    def test_eof_before_the_zero_chunk_is_a_cut(self):
        rec = run_stream_against(STREAM_HEAD + token_chunks(3), 3)
        self.assertFalse(rec["completed"], rec)

    def test_a_short_chunk_is_a_cut(self):
        rec = run_stream_against(STREAM_HEAD + token_chunks(2) + b"40\r\ntoken 3 short", 3)
        self.assertFalse(rec["completed"], rec)

    def test_a_chunk_not_followed_by_crlf_is_a_cut(self):
        line = b"token 2 2026-10-30T12:00:02.000Z\n"
        rec = run_stream_against(STREAM_HEAD + token_chunks(1) + b"%x\r\n" % len(line) + line, 2)
        self.assertFalse(rec["completed"], rec)

    def test_a_stream_with_fewer_tokens_than_asked_is_not_complete(self):
        rec = run_stream_against(STREAM_HEAD + token_chunks(2) + b"0\r\n\r\n", 3)
        self.assertFalse(rec["completed"], rec)


class FaultPreconditions(unittest.TestCase):
    """Review B3: the engine was killed and never restarted, then frpc was killed. The frpc mark had
    nothing to detect and came out empty. It must be recorded as invalid, never as a detection."""

    def state(self):
        args = sw.parse_args(["--tcpmux", "127.0.0.1:1", "--domain", "x"])
        st = sw.State(args)
        st.announce = lambda text: None            # the operator lines go to stdout; not wanted in test output
        return st, args

    def ok_probes(self, st, start, stop):
        t = float(start)
        while t <= stop:
            st.record_probe(t, True, "ok", 0.01, 0.01, "")
            t += 2.0

    def fail_probes(self, st, times, cls="no_response"):
        for t in times:
            st.record_probe(float(t), False, cls, 0.1, 0.0, "x")

    def marks(self, st, args):
        return sw.analyse(st, 100.0, 300.0, args)["marks"]

    def test_the_review_sequence_gives_an_invalid_frpc_mark(self):
        st, args = self.state()
        self.ok_probes(st, 100, 130)
        st.add_mark(131.0)                         # engine killed, from 'up'
        self.fail_probes(st, [132, 134])           # declared down at 134
        st.add_mark(140.0)                         # frpc killed: the engine was never restarted
        self.fail_probes(st, [142, 144])
        m = self.marks(st, args)
        self.assertTrue(m[0]["valid"])
        self.assertEqual(m[0]["probe_declared_down_s"], 3.0)
        self.assertFalse(m[1]["valid"])
        self.assertNotIn("probe_declared_down_s", m[1])
        self.assertNotIn("probe_class", m[1])
        self.assertIn("invalid_reason", m[1])
        self.assertEqual(m[1]["precondition"], "down")

    def test_after_the_engine_is_back_the_frpc_mark_is_a_valid_detection(self):
        st, args = self.state()
        self.ok_probes(st, 100, 130)
        st.add_mark(131.0)
        self.fail_probes(st, [132, 134])
        self.ok_probes(st, 150, 154)               # engine restarted, path up again
        st.add_mark(155.0)
        self.fail_probes(st, [156, 158], cls="path_down")
        m = self.marks(st, args)
        self.assertTrue(m[1]["valid"])
        self.assertEqual(m[1]["probe_declared_down_s"], 3.0)
        self.assertEqual(m[1]["probe_class"], "path_down")

    def test_a_mark_taken_while_a_probe_is_failing_is_invalid(self):
        st, args = self.state()
        self.ok_probes(st, 100, 130)
        self.fail_probes(st, [132])                # one failure, not yet declared
        st.add_mark(133.0)
        self.assertFalse(self.marks(st, args)[0]["valid"])

    def test_a_mark_with_no_recent_good_probe_is_invalid(self):
        st, args = self.state()
        self.ok_probes(st, 100, 110)
        st.add_mark(200.0)                         # the probe has been silent for 90 s
        self.assertFalse(self.marks(st, args)[0]["valid"])

    def test_a_mark_before_any_probe_is_invalid(self):
        st, args = self.state()
        st.add_mark(100.0)
        m = self.marks(st, args)[0]
        self.assertFalse(m["valid"])
        self.assertEqual(m["precondition"], "never_up")


class SignalHandlerSurvives(unittest.TestCase):
    """The handler closed a buffered reader that the main thread was reading: 'reentrant call'. It must
    set the flag and shut the socket down, and the watcher must still write an interrupted summary."""

    def run_sigterm(self, streams):
        """Start the watcher with --streams N against a server that sends one token per /stream and then goes
        silent, SIGTERM it once every stream is blocked in its read, and return the summary it wrote."""
        started = threading.Event()
        started_n = [0]
        count_lock = threading.Lock()
        stop = threading.Event()
        srv = socket.socket()
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("127.0.0.1", 0))
        srv.listen(16)
        srv.settimeout(0.2)
        port = srv.getsockname()[1]

        def serve_conn(conn):
            conn.settimeout(30)
            try:
                buf = b""
                while b"\r\n\r\n" not in buf:
                    buf += conn.recv(4096)
                conn.sendall(b"HTTP/1.1 200 OK\r\n\r\n")             # the CONNECT
                while not stop.is_set():
                    buf = b""
                    while b"\r\n\r\n" not in buf:
                        d = conn.recv(4096)
                        if not d:
                            return
                        buf += d
                    if buf.startswith(b"GET /stream"):
                        conn.sendall(STREAM_HEAD + token_chunks(1))
                        with count_lock:
                            started_n[0] += 1
                            if started_n[0] >= streams:
                                started.set()
                        while not stop.is_set():                     # then silence: the client blocks in readline
                            time.sleep(0.1)
                        return
                    conn.sendall(OK_REPLY)
            except OSError:
                return
            finally:
                conn.close()

        def accept_loop():
            while not stop.is_set():
                try:
                    conn, _ = srv.accept()
                except socket.timeout:
                    continue
                except OSError:
                    return
                t = threading.Thread(target=serve_conn, args=(conn,))
                t.daemon = True
                t.start()
        threading.Thread(target=accept_loop, daemon=True).start()

        tmp = tempfile.mkdtemp(prefix="s1-sw-")
        out = os.path.join(tmp, "summary.json")
        proc = subprocess.Popen([sys.executable, _kit.path("stream_watch.py"), "--tcpmux", "127.0.0.1:%d" % port,
                                 "--domain", "x", "--duration", "60", "--stream-seconds", "50", "--stall-timeout", "60",
                                 "--direct-samples", "2", "--streams", str(streams), "--out", out],
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            self.assertTrue(started.wait(15), "the watcher never opened all %d stream(s)" % streams)
            time.sleep(0.5)
            proc.send_signal(signal.SIGTERM)
            try:
                so, se = proc.communicate(timeout=15)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.communicate()
                self.fail("the watcher did not exit after SIGTERM")
            self.assertEqual(proc.returncode, 0, se.decode("utf-8", "replace")[-600:])
            self.assertNotIn(b"Traceback", se)
            with open(out) as fh:
                return json.load(fh)
        finally:
            stop.set()
            srv.close()
            if proc.poll() is None:
                proc.kill()
                proc.communicate()
            for f in os.listdir(tmp):
                os.unlink(os.path.join(tmp, f))
            os.rmdir(tmp)

    def test_sigterm_during_a_blocked_stream_read_writes_an_interrupted_summary(self):
        summary = self.run_sigterm(1)
        self.assertTrue(summary["streams"], summary)
        self.assertTrue(summary["streams"][0]["interrupted"], summary["streams"][0])
        self.assertEqual(summary["cuts"], [], "an interrupted stream is not a cut")

    def test_sigterm_with_three_streams_interrupts_every_one_of_them(self):
        # the handler has to shut down the socket of every lane, not only of the one on the main thread
        summary = self.run_sigterm(3)
        self.assertEqual(sorted(s["lane"] for s in summary["streams"]), [1, 2, 3], summary["streams"])
        self.assertTrue(all(s["interrupted"] for s in summary["streams"]), summary["streams"])
        self.assertEqual(summary["cuts"], [], "an interrupted stream is not a cut")


class StreamsOption(unittest.TestCase):
    def test_the_default_is_one_stream(self):
        self.assertEqual(sw.parse_args(["--tcpmux", "127.0.0.1:1", "--domain", "x"]).streams, 1)

    def test_a_count_is_accepted(self):
        self.assertEqual(sw.parse_args(["--tcpmux", "127.0.0.1:1", "--domain", "x", "--streams", "4"]).streams, 4)

    def test_fewer_than_one_stream_is_refused(self):
        for bad in ("0", "-2"):
            with contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    sw.parse_args(["--tcpmux", "127.0.0.1:1", "--domain", "x", "--streams", bad])


def stream_rec(lane, sid, completed, duration, tokens, interrupted=False, end_epoch=250.0, reason=None):
    return {"id": sid, "lane": lane, "requested_s": 3, "start_utc": sw.utc(end_epoch - duration), "tokens": tokens,
            "seq_breaks": 0, "ttfb_s": 0.1, "completed": completed, "interrupted": interrupted, "reason": reason,
            "end_utc": sw.utc(end_epoch), "end_epoch": end_epoch, "duration_s": duration}


class PerStreamReport(unittest.TestCase):
    """The summary reports, per stream and overall, the max gap, the cuts and the completion. The records and
    gaps are written by hand; the expected numbers follow from them."""

    def state(self, streams=3):
        args = sw.parse_args(["--tcpmux", "127.0.0.1:1", "--domain", "x", "--streams", str(streams)])
        return sw.State(args), args

    def test_gaps_cuts_and_completion_are_reported_per_stream_and_overall(self):
        st, args = self.state()
        # (epoch, gap_s, seq, stream id, lane)
        st.gaps += [(150.0, 1.0, 2, 1, 1), (151.0, 1.2, 3, 1, 1), (150.5, 1.1, 2, 1, 2), (152.0, 3.5, 5, 1, 3)]
        st.streams += [stream_rec(1, 1, True, 10.0, 3),
                       stream_rec(2, 1, False, 1.5, 1, reason="cut: EOF"),     # lane 2 is cut ...
                       stream_rec(3, 1, True, 10.0, 3),
                       stream_rec(2, 2, True, 10.0, 3)]                        # ... and reopens
        s = sw.analyse(st, 100.0, 300.0, args)
        self.assertEqual(s["streams_requested"], 3)
        lanes = dict((r["lane"], r) for r in s["per_stream"])
        self.assertEqual(sorted(lanes), [1, 2, 3])
        self.assertEqual((lanes[1]["cuts"], lanes[1]["first_stream_completed"], lanes[1]["max_gap_s"]), (0, True, 1.2))
        self.assertEqual((lanes[2]["cuts"], lanes[2]["first_stream_completed"], lanes[2]["max_gap_s"]), (1, False, 1.1))
        self.assertEqual((lanes[2]["streams"], lanes[2]["completed"]), (2, 1))
        self.assertEqual((lanes[3]["cuts"], lanes[3]["first_stream_completed"], lanes[3]["max_gap_s"]), (0, True, 3.5))
        self.assertEqual(lanes[3]["gaps_over_threshold_unfaulted"], 1)
        self.assertEqual(lanes[1]["gaps_over_threshold_unfaulted"], 0)
        o = s["overall"]
        self.assertEqual((o["streams"], o["streams_whose_first_stream_completed"], o["cuts"], o["max_gap_s"]), (3, 2, 1, 3.5))
        self.assertEqual(len(s["cuts"]), 1)
        self.assertEqual((s["cuts"][0]["lane"], s["cuts"][0]["stream"]), (2, 1))
        self.assertEqual(s["gaps"]["over_threshold_unfaulted"], 1)
        self.assertEqual(s["gaps"]["over_threshold"][0]["lane"], 3)
        self.assertEqual(s["reconnects_after_cut"], 1, "lane 2 was cut and reopened once")

    def test_a_lane_that_was_cut_last_has_not_reconnected(self):
        # interleaved records: the old 'every record but the last' rule counted lane 3's cut as a reconnect
        st, args = self.state()
        st.streams += [stream_rec(3, 1, False, 2.0, 1, reason="cut"), stream_rec(1, 1, True, 10.0, 3)]
        self.assertEqual(sw.analyse(st, 100.0, 300.0, args)["reconnects_after_cut"], 0)

    def test_a_lane_with_no_stream_is_reported_as_not_completed(self):
        st, args = self.state(streams=2)
        st.streams += [stream_rec(1, 1, True, 10.0, 3)]
        lanes = dict((r["lane"], r) for r in sw.analyse(st, 100.0, 300.0, args)["per_stream"])
        self.assertEqual((lanes[2]["streams"], lanes[2]["first_stream_completed"], lanes[2]["max_gap_s"]), (0, False, None))

    def test_one_stream_is_reported_the_same_way(self):
        st, args = self.state(streams=1)
        st.streams += [stream_rec(1, 1, True, 10.0, 3)]
        s = sw.analyse(st, 100.0, 300.0, args)
        self.assertEqual(len(s["per_stream"]), 1)
        self.assertEqual(s["streams_completed"], 1)

    def test_the_digest_names_the_stream_that_was_not_completed(self):
        st, args = self.state()
        st.streams += [stream_rec(1, 1, True, 10.0, 3), stream_rec(2, 1, False, 1.5, 1, reason="cut"),
                       stream_rec(3, 1, True, 10.0, 3)]
        text = sw.digest(sw.analyse(st, 100.0, 300.0, args))
        self.assertIn("stream 2: first stream completed NO", text)
        self.assertIn("stream 1: first stream completed yes", text)
        self.assertIn("overall: 3 streams, first stream completed on 2, cuts 1", text)


class TcpmuxStandIn(object):
    """A loopback stand-in for frps' tcpmux port: 'CONNECT name:80' is answered 200 and the connection is then
    piped to the engine. Several connections at once, as the real one serves them."""

    def __init__(self, engine_port):
        self.engine_port = engine_port
        self.srv = socket.socket()
        self.srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.srv.bind(("127.0.0.1", 0))
        self.srv.listen(32)
        self.port = self.srv.getsockname()[1]
        self.stop = False
        t = threading.Thread(target=self.accept_loop)
        t.daemon = True
        t.start()

    def accept_loop(self):
        self.srv.settimeout(0.2)
        while not self.stop:
            try:
                conn, _ = self.srv.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            t = threading.Thread(target=self.serve, args=(conn,))
            t.daemon = True
            t.start()

    @staticmethod
    def pipe(src, dst):
        try:
            while True:
                data = src.recv(65536)
                if not data:
                    break
                dst.sendall(data)
        except OSError:
            pass
        finally:
            try:
                dst.shutdown(socket.SHUT_WR)
            except OSError:
                pass

    def serve(self, conn):
        up = None
        try:
            buf = b""
            while b"\r\n\r\n" not in buf:
                data = conn.recv(4096)
                if not data:
                    return
                buf += data
            up = socket.create_connection(("127.0.0.1", self.engine_port), timeout=5)
            up.settimeout(None)
            conn.sendall(b"HTTP/1.1 200 Connection established\r\n\r\n")
            t = threading.Thread(target=self.pipe, args=(conn, up))
            t.daemon = True
            t.start()
            self.pipe(up, conn)
        except OSError:
            pass
        finally:
            for s in (conn, up):
                try:
                    if s is not None:
                        s.close()
                except OSError:
                    pass

    def close(self):
        self.stop = True
        self.srv.close()


class ThreeStreamsLive(unittest.TestCase):
    """stream_watch.py --streams 3 as a real process against the real fake_streamer, through a tcpmux stand-in.
    Three sequential streams would also 'complete', so the check that tells them apart is the overlap in time."""

    def test_three_streams_run_at_once_and_each_completes(self):
        tmp = tempfile.mkdtemp(prefix="s1-sw3-")
        portfile, reqlog, out = (os.path.join(tmp, n) for n in ("port", "requests", "summary.json"))
        eng = subprocess.Popen([sys.executable, _kit.path("fake_streamer.py"), "--port", "0", "--port-file", portfile,
                                "--request-log", reqlog], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        mux = None
        try:
            end = time.time() + 10
            while not (os.path.exists(portfile) and os.path.getsize(portfile)) and time.time() < end:
                time.sleep(0.05)
            with open(portfile) as fh:
                mux = TcpmuxStandIn(int(fh.read().strip()))
            res = subprocess.run([sys.executable, _kit.path("stream_watch.py"), "--tcpmux", "127.0.0.1:%d" % mux.port,
                                  "--domain", "x", "--wait-up", "10", "--duration", "12", "--streams", "3",
                                  "--stream-seconds", "8", "--direct-samples", "2", "--out", out],
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=90)
            self.assertEqual(res.returncode, 0, res.stderr.decode("utf-8", "replace")[-600:])
            with open(out) as fh:
                s = json.load(fh)
            self.assertEqual(s["streams_requested"], 3)
            recs = s["streams"]
            self.assertEqual(sorted(r["lane"] for r in recs), [1, 2, 3], recs)
            for r in recs:
                self.assertTrue(r["completed"], r)
                self.assertEqual((r["requested_s"], r["tokens"]), (8, 8), r)
            self.assertEqual(s["cuts"], [])
            self.assertTrue(all(r["first_stream_completed"] and r["cuts"] == 0 for r in s["per_stream"]), s["per_stream"])
            starts = [r["end_epoch"] - r["duration_s"] for r in recs]
            ends = [r["end_epoch"] for r in recs]
            self.assertLess(max(starts), min(ends) - 3, "the streams ran one after another, not at once")
            with open(reqlog) as fh:
                at = [float(ln.split()[0]) for ln in fh if "/stream" in ln]
            self.assertEqual(len(at), 3)
            self.assertLess(max(at) - min(at), 2.0, "the engine saw the three /stream requests more than 2 s apart")
        finally:
            if mux is not None:
                mux.close()
            eng.terminate()
            eng.wait(10)
            for f in os.listdir(tmp):
                os.unlink(os.path.join(tmp, f))
            os.rmdir(tmp)



class PingIntervals(unittest.TestCase):
    """The control-delay input of verdict.py's timeout budget: gaps between the instance's Ping calls at the plugin,
    before the first mark only (the faults stop Pings on purpose), for the frp user only."""

    def test_gaps_before_the_first_mark_for_the_instance_only(self):
        d = tempfile.mkdtemp(prefix="s1-sw-ping-")
        try:
            log = os.path.join(d, "stub.jsonl")
            rows = [(100.0, "inst"), (110.0, "inst"), (121.5, "inst"), (131.5, "inst"), (125.0, "other"),
                    (170.0, "inst"), (200.0, "inst")]                      # the last two fall after the mark at 140
            with open(log, "w") as fh:
                for t, user in rows:
                    fh.write(json.dumps({"epoch": t, "op": "Ping", "user": user}) + "\n")
            args = sw.parse_args(["--tcpmux", "127.0.0.1:1", "--domain", "5e1f0000-0000-4000-8000-000000000001",
                                  "--stub-log", log, "--stub-user", "inst"])
            st = sw.State(args)
            st.announce = lambda text: None
            st.add_mark(140.0)
            got = sw.analyse(st, 100.0, 300.0, args)["stub"]["ping_interval_unfaulted_s"]
            self.assertEqual((got["n"], got["max"], got["min"]), (3, 11.5, 10.0))   # 10.0, 11.5, 10.0 by hand
        finally:
            for f in os.listdir(d):
                os.unlink(os.path.join(d, f))
            os.rmdir(d)


class StubUserIsTheFrpUser(unittest.TestCase):
    """The tcpmux domain is the Instance UID, but plugin calls (Login, Ping) carry only the frp user. Matching the
    stub log on the domain would find nothing and every stub-based check would read as absent, so --stub-log
    without --stub-user is refused."""

    def test_stub_log_without_stub_user_is_refused(self):
        with self.assertRaises(SystemExit):
            sw.parse_args(["--tcpmux", "127.0.0.1:29400", "--domain", "5e1f0000-0000-4000-8000-000000000001",
                           "--stub-log", "/nonexistent/stub.jsonl"])

    def test_stub_user_is_kept_apart_from_the_domain(self):
        a = sw.parse_args(["--tcpmux", "127.0.0.1:29400", "--domain", "5e1f0000-0000-4000-8000-000000000001",
                           "--stub-log", "/nonexistent/stub.jsonl", "--stub-user", "s1-helios-cpu"])
        self.assertEqual((a.domain, a.stub_user), ("5e1f0000-0000-4000-8000-000000000001", "s1-helios-cpu"))


if __name__ == "__main__":
    unittest.main()
