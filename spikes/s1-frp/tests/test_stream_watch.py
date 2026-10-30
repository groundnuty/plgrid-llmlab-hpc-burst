"""Tests of stream_watch.py: strict HTTP validation (B2), fault preconditions (B3), the signal handler (nit).

Expected values come from the review text and the task brief. Where the review reproduced a bug, the test
feeds the same bytes (Content-Length: 100 then EOF; a stream cut before its final blank line).
"""
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

    def test_sigterm_during_a_blocked_stream_read_writes_an_interrupted_summary(self):
        started = threading.Event()
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
                                 "--direct-samples", "2", "--out", out],
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            self.assertTrue(started.wait(15), "the watcher never opened the stream")
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
                summary = json.load(fh)
            self.assertTrue(summary["streams"], summary)
            self.assertTrue(summary["streams"][0]["interrupted"], summary["streams"][0])
            self.assertEqual(summary["cuts"], [], "an interrupted stream is not a cut")
        finally:
            stop.set()
            srv.close()
            if proc.poll() is None:
                proc.kill()
                proc.communicate()
            for f in os.listdir(tmp):
                os.unlink(os.path.join(tmp, f))
            os.rmdir(tmp)


if __name__ == "__main__":
    unittest.main()
