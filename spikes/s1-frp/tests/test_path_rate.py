"""Tests for path_rate.py (serve, measure, probe) and make-test-ca.sh.

    cd spikes/s1-frp/tests && python3 -m unittest -v test_path_rate

Servers run as subprocesses on 127.0.0.1 in 29400-29499 (_kit.free_port) and are stopped by PID in a
cleanup. The rate expectations are the caps passed on the command line (bytes/s x seconds), never a
value the code computed. TLS tests keep verification on; the "wrong" cases use a second CA and a wrong name.
"""
import io
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest

import _kit

SERVER_NAME = "dial-home.hpcburst.invalid"
TOKEN = b"S1-PROBE-TOKEN-OK\n"
# The job parses both lines with sed, so the key ORDER is part of the contract.
MEASURE_KEYS = ["forward", "direction", "ok", "requested_s", "max_bps", "bytes", "duration_s", "bps",
                "timed_by", "sender_s", "detail"]
PROBE_KEYS = ["far_end", "ms", "detail"]


def read_text(path):
    with open(path) as fh:
        return fh.read()


def within(value, expected, tolerance=0.15):
    return abs(value - expected) <= tolerance * expected


class FarEnd(object):
    """A listener that misbehaves on purpose: close at once, answer fixed bytes, or stay silent."""

    def __init__(self, port, reply=None, hold=False):
        self.port, self.reply, self.hold, self.stop = port, reply, hold, threading.Event()
        self.srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.srv.bind(("127.0.0.1", port))
        self.srv.listen(8)
        self.srv.settimeout(0.2)
        self.thread = threading.Thread(target=self.run)
        self.thread.daemon = True
        self.thread.start()

    def run(self):
        held = []
        while not self.stop.is_set():
            try:
                conn, _ = self.srv.accept()
            except socket.timeout:
                continue
            if self.reply is not None:
                conn.settimeout(2)
                try:
                    conn.recv(65536)
                    conn.sendall(self.reply)
                except OSError:
                    pass
            if self.hold:
                held.append(conn)
            else:
                conn.close()
        for conn in held:
            conn.close()

    def close(self):
        self.stop.set()
        self.thread.join(5)
        self.srv.close()


class FakeSink(object):
    """A sink that counts the chunked body it gets, but answers with timing the client could not produce."""
    ANSWER = {"first_byte_epoch": 1000.0, "last_byte_epoch": 1100.0, "duration_s": 100.0, "bps": 12345.5}

    def __init__(self, port):
        self.mod = _kit.load("path_rate")
        self.counted = None
        self.srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.srv.bind(("127.0.0.1", port))
        self.srv.listen(2)
        self.port = port
        self.thread = threading.Thread(target=self.run)
        self.thread.daemon = True
        self.thread.start()

    def run(self):
        conn, _ = self.srv.accept()
        rfile = conn.makefile("rb")
        while rfile.readline() not in (b"\r\n", b""):         # request head
            pass
        timer = self.mod.BodyTimer()
        self.mod.read_chunked(rfile, timer)
        self.counted = timer.total
        body = json.dumps(dict(self.ANSWER, bytes=timer.total)).encode()
        conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: %d\r\nConnection: close\r\n\r\n" % len(body) + body)
        conn.close()
        rfile.close()

    def close(self):
        self.thread.join(10)
        self.srv.close()


class PathRateBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="s1-path-rate-")
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.procs = []
        self.addCleanup(self.stop_all)        # runs before the rmtree above: LIFO

    def stop_all(self):
        for proc in self.procs:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(5)

    def script(self, *args):
        return [sys.executable, _kit.path("path_rate.py")] + [str(a) for a in args]

    def start_server(self, *extra):
        """serve on a free port; returns (port, log path). Retries when another process took the port."""
        for attempt in range(5):
            port = _kit.free_port()
            errfile = os.path.join(self.tmp, "serve-%d-%d.err" % (port, attempt))
            logfile = os.path.join(self.tmp, "serve-%d.log" % port)
            err = open(errfile, "w")
            self.addCleanup(err.close)
            proc = subprocess.Popen(self.script("serve", "--listen", "127.0.0.1:%d" % port, "--log", logfile,
                                                "--max-seconds", 120, *extra), stdout=err, stderr=err)
            self.procs.append(proc)
            deadline = time.time() + 10
            while time.time() < deadline and proc.poll() is None:
                if "path_rate serving" in read_text(errfile):
                    return port, logfile
                time.sleep(0.05)
            if proc.poll() is None:
                self.fail("server did not report listening: %s" % read_text(errfile))
        self.fail("no server could start: %s" % read_text(errfile))

    def certs(self, tag="a"):
        out = os.path.join(self.tmp, "certs-" + tag)
        subprocess.check_output(["bash", _kit.path("make-test-ca.sh"), out], stderr=subprocess.STDOUT)
        return out

    def run_cli(self, *args, **kw):
        proc = subprocess.Popen(self.script(*args), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                universal_newlines=True)
        try:
            out, err = proc.communicate(timeout=kw.get("timeout", 60))
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.communicate()
            self.fail("command timed out: %s" % (args,))
        return proc.returncode, out, err

    def json_line(self, out, keys):
        """The one JSON object on stdout, with exactly the contract's keys in the contract's order."""
        lines = out.splitlines()
        self.assertEqual(len(lines), 1, "stdout must be exactly one line, got %r" % out)
        self.assertEqual(json.loads(lines[0], object_pairs_hook=lambda pairs: [k for k, _ in pairs]), keys)
        return json.loads(lines[0])

    def measure(self, port, direction, seconds, max_bps, tls_dir=None, name=SERVER_NAME, expect_rc=0):
        args = ["measure", "--host", "127.0.0.1", "--port", port, "--forward", "t1", "--direction", direction,
                "--seconds", seconds, "--max-bps", max_bps]
        if tls_dir:
            args += ["--tls", "--ca", os.path.join(tls_dir, "ca.crt"), "--server-name", name]
        rc, out, err = self.run_cli(*args)
        self.assertEqual(rc, expect_rc, "stdout %r stderr %r" % (out, err))
        return self.json_line(out, MEASURE_KEYS)

    def probe(self, port, tls_dir=None, name=SERVER_NAME, extra=()):
        args = ["probe", "--host", "127.0.0.1", "--port", port] + list(extra)
        if tls_dir:
            args += ["--tls", "--ca", os.path.join(tls_dir, "ca.crt"), "--server-name", name]
        rc, out, err = self.run_cli(*args)
        obj = self.json_line(out, PROBE_KEYS)
        self.assertIsInstance(obj["ms"], int)
        self.assertEqual(rc, 0 if obj["far_end"] == "token" else 1)
        return obj

    def raw_exchange(self, port, payload_parts, shutdown_write=False):
        """Send raw bytes (parts, with a pause between them) and return (status line, body bytes)."""
        sock = socket.create_connection(("127.0.0.1", port), 10)
        self.addCleanup(sock.close)
        for i, part in enumerate(payload_parts):
            if i:
                time.sleep(0.3)
            sock.sendall(part)
        if shutdown_write:
            sock.shutdown(socket.SHUT_WR)
        data = b""
        while True:
            chunk = sock.recv(65536)
            if not chunk:
                break
            data += chunk
        head, _, body = data.partition(b"\r\n\r\n")
        return head.split(b"\r\n")[0].decode(), body


class RateTests(PathRateBase):
    def test_up_is_timed_by_the_vm_and_follows_the_cap(self):
        port, _ = self.start_server()
        got = self.measure(port, "up", 3, 2000000)
        self.assertTrue(got["ok"], got)
        self.assertEqual(got["timed_by"], "receiver-vm")
        self.assertEqual((got["forward"], got["direction"], got["requested_s"], got["max_bps"]), ("t1", "up", 3, 2000000))
        self.assertTrue(within(got["bps"], 2000000), got)
        self.assertTrue(within(got["bytes"], 3 * 2000000), got)
        self.assertGreaterEqual(got["duration_s"], 2.5)
        self.assertIsNotNone(got["sender_s"])

    def test_down_is_timed_by_the_job_and_follows_the_cap(self):
        port, _ = self.start_server()
        got = self.measure(port, "down", 3, 2000000)
        self.assertTrue(got["ok"], got)
        self.assertEqual(got["timed_by"], "receiver-job")
        self.assertTrue(within(got["bps"], 2000000), got)
        self.assertTrue(within(got["bytes"], 3 * 2000000), got)
        self.assertGreaterEqual(got["duration_s"], 2.5)
        self.assertIsNone(got["sender_s"])

    def test_tls_up_and_down_with_the_right_ca(self):
        certs = self.certs()
        port, _ = self.start_server("--tls-cert", certs + "/server.crt", "--tls-key", certs + "/server.key")
        for direction in ("up", "down"):
            got = self.measure(port, direction, 1, 1000000, tls_dir=certs)
            self.assertTrue(got["ok"], got)
            self.assertTrue(within(got["bps"], 1000000, 0.2), got)

    def test_up_reports_the_receivers_timing_not_its_own(self):
        sink = FakeSink(_kit.free_port())
        self.addCleanup(sink.close)
        got = self.measure(sink.port, "up", 1, 500000)
        self.assertEqual((got["duration_s"], got["bps"]), (100.0, 12345.5))   # the fake's figures, copied verbatim
        self.assertEqual(got["bytes"], sink.counted)
        self.assertTrue(got["ok"])
        self.assertLess(got["sender_s"], 10)                                  # the client's own clock is separate

    def test_source_is_paced_to_max_bps(self):
        import http.client
        port, _ = self.start_server()
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=20)
        start = time.time()
        conn.request("GET", "/s1/source?seconds=1&max_bps=1000000")
        resp = conn.getresponse()
        body = resp.read()
        self.assertEqual(resp.status, 200)
        self.assertEqual(resp.getheader("Transfer-Encoding"), "chunked")
        self.assertTrue(within(len(body), 1000000), len(body))
        self.assertGreaterEqual(time.time() - start, 0.85)


class SinkTests(PathRateBase):
    def head(self, *lines):
        return ("POST /s1/sink HTTP/1.1\r\nHost: x\r\n" + "".join(l + "\r\n" for l in lines) + "\r\n").encode()

    def sink_json(self, reply):
        status, body = reply
        self.assertEqual(status, "HTTP/1.1 200 OK")
        return json.loads(body.decode())

    def test_content_length_body_is_counted_exactly(self):
        port, _ = self.start_server()
        size = 300007
        half = b"x" * 150000
        reply = self.raw_exchange(port, [self.head("Content-Length: %d" % size) + half, b"y" * (size - 150000)])
        got = self.sink_json(reply)
        self.assertEqual(got["bytes"], size)
        self.assertGreaterEqual(got["duration_s"], 0.25)          # the test paused 0.3 s between the halves
        self.assertLessEqual(got["first_byte_epoch"], got["last_byte_epoch"])

    def test_empty_bodies_give_zero_bytes_and_no_rate(self):
        port, _ = self.start_server()
        for payload in (self.head("Content-Length: 0"), self.head("Transfer-Encoding: chunked") + b"0\r\n\r\n"):
            got = self.sink_json(self.raw_exchange(port, [payload]))
            self.assertEqual((got["bytes"], got["bps"], got["duration_s"]), (0, None, 0.0))

    def test_chunked_body_with_irregular_chunks_is_counted_exactly(self):
        port, _ = self.start_server()
        sizes = [1, 4097, 70000, 65536, 3, 200000]                 # two chunks larger than 64 KiB, as Envoy may send
        body = b"".join(b"%X;note=1\r\n" % n + b"z" * n + b"\r\n" if i == 1 else b"%x\r\n" % n + b"z" * n + b"\r\n"
                        for i, n in enumerate(sizes))
        reply = self.raw_exchange(port, [self.head("Transfer-Encoding: chunked") + body[:100000], body[100000:] +
                                         b"0\r\nX-Trailer: 1\r\n\r\n"])
        self.assertEqual(self.sink_json(reply)["bytes"], sum(sizes))

    def test_bad_bodies_are_refused_not_counted(self):
        port, _ = self.start_server()
        self.assertTrue(self.raw_exchange(port, [self.head()])[0].endswith("411 Length Required"))
        self.assertTrue(self.raw_exchange(port, [self.head("Content-Length: abc")])[0].endswith("400 Bad Request"))
        self.assertTrue(self.raw_exchange(port, [self.head("Content-Length: -5")])[0].endswith("400 Bad Request"))
        short = self.head("Content-Length: 100") + b"x" * 10      # the sender dies 90 bytes early
        self.assertTrue(self.raw_exchange(port, [short], shutdown_write=True)[0].endswith("400 Bad Request"))
        garbage = self.head("Transfer-Encoding: chunked") + b"zz\r\nxx\r\n"
        self.assertTrue(self.raw_exchange(port, [garbage])[0].endswith("400 Bad Request"))

    def test_decoder_handles_chunk_forms_directly(self):
        mod = _kit.load("path_rate")
        timer = mod.BodyTimer()
        wire = b"a;ext=1\r\n" + b"0123456789" + b"\r\n" + b"1F\r\n" + b"q" * 31 + b"\r\n" + b"0\r\nT: 1\r\n\r\n"
        mod.read_chunked(io.BytesIO(wire), timer)
        self.assertEqual(timer.total, 41)
        for broken in (b"5\r\nabc", b"5\r\nabcde\r\n", b"zz\r\n", b"3\r\nabcXY"):
            with self.assertRaises(mod.ProtocolError):
                mod.read_chunked(io.BytesIO(broken), mod.BodyTimer())


class TlsTests(PathRateBase):
    def tls_server(self):
        certs = self.certs("a")
        port, _ = self.start_server("--tls-cert", certs + "/server.crt", "--tls-key", certs + "/server.key")
        return port, certs

    def test_probe_succeeds_with_the_right_ca_and_name(self):
        port, certs = self.tls_server()
        self.assertEqual(self.probe(port, certs)["far_end"], "token")

    def test_probe_fails_with_a_different_ca(self):
        port, _ = self.tls_server()
        self.assertEqual(self.probe(port, self.certs("other"))["far_end"], "tls_failed")

    def test_probe_fails_with_the_right_ca_and_a_wrong_name(self):
        port, certs = self.tls_server()
        self.assertEqual(self.probe(port, certs, name="wrong.hpcburst.invalid")["far_end"], "tls_failed")

    def test_plain_http_to_the_tls_server_is_not_a_token(self):
        port, _ = self.tls_server()
        self.assertNotEqual(self.probe(port)["far_end"], "token")

    def test_measure_with_a_wrong_ca_fails_with_a_json_line(self):
        port, _ = self.tls_server()
        other = self.certs("other")
        for direction in ("up", "down"):
            got = self.measure(port, direction, 1, 1000000, tls_dir=other, expect_rc=1)
            self.assertFalse(got["ok"])
            self.assertIn("SSL", got["detail"])
            self.assertEqual((got["bytes"], got["bps"]), (0, None))

    def test_a_missing_ca_file_is_a_tls_failure_naming_the_file(self):
        port = _kit.free_port()
        missing = os.path.join(self.tmp, "no-such-ca.crt")
        rc, out, _ = self.run_cli("probe", "--host", "127.0.0.1", "--port", port, "--tls", "--ca", missing,
                                  "--server-name", SERVER_NAME)
        got = self.json_line(out, PROBE_KEYS)
        self.assertEqual((rc, got["far_end"]), (1, "tls_failed"))
        self.assertIn(missing, got["detail"])
        rc, out, _ = self.run_cli("measure", "--host", "127.0.0.1", "--port", port, "--forward", "f", "--direction",
                                  "down", "--seconds", 1, "--tls", "--ca", missing, "--server-name", SERVER_NAME)
        got = self.json_line(out, MEASURE_KEYS)
        self.assertEqual((rc, got["ok"]), (1, False))
        self.assertIn(missing, got["detail"])

    def test_tls_probe_of_a_listener_that_closes_at_once_is_closed(self):
        fe = FarEnd(_kit.free_port())
        self.addCleanup(fe.close)
        got = self.probe(fe.port, self.certs("a"))
        self.assertEqual(got["far_end"], "closed", got)

    def test_tls_probe_of_a_far_end_that_answers_non_tls_bytes_is_tls_failed(self):
        fe = FarEnd(_kit.free_port(), reply=b"HELLO\n")
        self.addCleanup(fe.close)
        got = self.probe(fe.port, self.certs("a"))
        self.assertEqual(got["far_end"], "tls_failed", got)

    def test_tls_measure_detail_tells_closed_from_tls_failed(self):
        certs = self.certs("a")
        closing = FarEnd(_kit.free_port())
        self.addCleanup(closing.close)
        hello = FarEnd(_kit.free_port(), reply=b"HELLO\n")
        self.addCleanup(hello.close)
        for fe, word in ((closing, "closed"), (hello, "tls_failed")):
            got = self.measure(fe.port, "down", 1, 1000000, tls_dir=certs, expect_rc=1)
            self.assertFalse(got["ok"])
            self.assertTrue(got["detail"].startswith(word + ":"), got["detail"])

    def test_the_test_ca_script_refuses_to_overwrite_keys(self):
        certs = self.certs("a")
        for name in ("ca.key", "server.key"):
            self.assertEqual(oct(os.stat(os.path.join(certs, name)).st_mode & 0o777), oct(0o600))
        self.assertEqual(os.stat(certs).st_mode & 0o777, 0o700)
        before = read_text(os.path.join(certs, "ca.key"))
        proc = subprocess.Popen(["bash", _kit.path("make-test-ca.sh"), certs], stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, universal_newlines=True)
        out, err = proc.communicate()
        self.assertEqual(proc.returncode, 1)
        self.assertIn("refusing to overwrite", err)
        self.assertEqual(read_text(os.path.join(certs, "ca.key")), before)
        self.assertEqual(sorted(os.listdir(certs)), ["ca.crt", "ca.key", "server.crt", "server.key"])


class ProbeStateTests(PathRateBase):
    def far_end(self, **kw):
        port = _kit.free_port()
        fe = FarEnd(port, **kw)
        self.addCleanup(fe.close)
        return port

    def test_token(self):
        port, _ = self.start_server()
        self.assertEqual(self.probe(port)["far_end"], "token")

    def test_closed_when_the_listener_closes_at_once(self):
        self.assertEqual(self.probe(self.far_end())["far_end"], "closed")

    def test_refused_when_nothing_listens(self):
        self.assertEqual(self.probe(_kit.free_port())["far_end"], "refused")

    def test_data_when_the_reply_is_not_http(self):
        self.assertEqual(self.probe(self.far_end(reply=TOKEN))["far_end"], "data")

    def test_data_when_http_200_has_another_body(self):
        reply = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello"
        self.assertEqual(self.probe(self.far_end(reply=reply))["far_end"], "data")

    def test_http_error_for_a_non_200_status(self):
        reply = b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        self.assertEqual(self.probe(self.far_end(reply=reply))["far_end"], "http_error")

    def test_timeout_when_the_far_end_stays_silent(self):
        got = self.probe(self.far_end(hold=True), extra=("--timeout", 1))
        self.assertEqual(got["far_end"], "timeout")
        self.assertGreaterEqual(got["ms"], 900)

    def test_data_when_a_partial_reply_is_followed_by_silence(self):
        got = self.probe(self.far_end(reply=b"HTTP/1.1 200 OK\r\nContent-Le", hold=True), extra=("--timeout", 1))
        self.assertEqual(got["far_end"], "data")

    def test_classify_failure_maps_each_exception_to_one_state(self):
        import ssl
        mod = _kit.load("path_rate")
        cases = [(ConnectionResetError("reset"), "closed"), (BrokenPipeError("pipe"), "closed"),
                 (ssl.SSLEOFError(8, "EOF occurred in violation of protocol"), "closed"),
                 (ssl.SSLZeroReturnError(6, "closed"), "closed"),
                 (ConnectionRefusedError("no"), "refused"), (socket.timeout("slow"), "timeout"),
                 (ssl.SSLError(1, "[SSL: WRONG_VERSION_NUMBER] wrong version number"), "tls_failed"),
                 (ssl.CertificateError("hostname mismatch"), "tls_failed")]
        for exc, want in cases:
            self.assertEqual(mod.classify_failure(exc, 5)[0], want, repr(exc))
        self.assertIsNone(mod.classify_failure(ValueError("not a connection problem"), 5))

    def test_classify_reply_needs_more_bytes_until_the_body_is_complete(self):
        mod = _kit.load("path_rate")
        head = b"HTTP/1.1 200 OK\r\nContent-Length: 18\r\n\r\n"
        self.assertIsNone(mod.classify_reply(b"", False))
        self.assertIsNone(mod.classify_reply(b"HT", False))
        self.assertIsNone(mod.classify_reply(head + TOKEN[:5], False))
        self.assertEqual(mod.classify_reply(head + TOKEN, False)[0], "token")
        self.assertEqual(mod.classify_reply(head + TOKEN[:5], True)[0], "data")


class MeasureFailureTests(PathRateBase):
    def test_refused_gives_a_failure_line(self):
        rc, out, _ = self.run_cli("measure", "--host", "127.0.0.1", "--port", _kit.free_port(), "--forward", "f",
                                  "--direction", "up", "--seconds", 1)
        got = self.json_line(out, MEASURE_KEYS)
        self.assertEqual(rc, 1)
        self.assertFalse(got["ok"])
        self.assertIsNone(got["max_bps"])
        self.assertTrue(got["detail"])

    def test_a_404_far_end_is_a_failure_in_both_directions(self):
        reply = b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        fe = FarEnd(_kit.free_port(), reply=reply)
        self.addCleanup(fe.close)
        for direction in ("up", "down"):
            got = self.measure(fe.port, direction, 2, 1000000, expect_rc=1)
            self.assertFalse(got["ok"], got)
            self.assertIsNone(got["bps"])
            self.assertTrue(got["detail"])


class ServerTests(PathRateBase):
    def http(self, port, method, target, body=None):
        import http.client
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=20)
        conn.request(method, target, body=body)
        resp = conn.getresponse()
        data = resp.read()
        conn.close()
        return resp, data

    def test_probe_endpoint_serves_the_exact_token(self):
        port, _ = self.start_server()
        resp, data = self.http(port, "GET", "/s1/probe")
        self.assertEqual((resp.status, data), (200, TOKEN))
        self.assertEqual(resp.getheader("Content-Type"), "text/plain")
        self.assertEqual(resp.getheader("Content-Length"), "18")

    def test_bad_source_requests_get_400(self):
        port, _ = self.start_server()
        for query in ("seconds=0", "seconds=abc", "seconds=601", "", "seconds=nan", "seconds=2&max_bps=abc",
                      "seconds=2&max_bps=0", "seconds=2&max_bps=-5"):
            self.assertEqual(self.http(port, "GET", "/s1/source?" + query)[0].status, 400, query)

    def test_unknown_paths_and_methods_get_404(self):
        port, _ = self.start_server()
        for method, target in (("GET", "/"), ("GET", "/s1/nothing"), ("GET", "/s1/sink"), ("POST", "/s1/probe"),
                               ("PUT", "/s1/probe"), ("DELETE", "/s1/source?seconds=1")):
            self.assertEqual(self.http(port, method, target)[0].status, 404, (method, target))

    def test_log_has_one_json_line_per_request_and_never_the_body(self):
        port, logfile = self.start_server()
        marker = b"BODY-MARKER-8731" * 50
        self.http(port, "GET", "/s1/probe")
        self.http(port, "POST", "/s1/sink", body=marker)
        self.http(port, "GET", "/s1/missing")
        lines = [json.loads(l) for l in read_text(logfile).splitlines()]
        self.assertEqual([(l["method"], l["path"], l["status"], l["bytes"]) for l in lines],
                         [("GET", "/s1/probe", 200, 18), ("POST", "/s1/sink", 200, len(marker)),
                          ("GET", "/s1/missing", 404, 0)])
        for entry in lines:
            self.assertRegex(entry["time"], r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z$")
        self.assertNotIn("BODY-MARKER", read_text(logfile))

    def test_serve_refuses_non_loopback_and_bad_arguments_with_exit_2(self):
        port = _kit.free_port()
        for args in (["--listen", "0.0.0.0:%d" % port], ["--listen", "[::1]:%d" % port],
                     ["--listen", "127.0.0.1:0"], ["--listen", "localhost:%d" % port],
                     ["--listen", "127.0.0.1:%d" % port, "--tls-cert", "x.crt"]):
            rc, _, err = self.run_cli("serve", *args, timeout=15)
            self.assertEqual(rc, 2, (args, err))

    def test_serve_exits_by_itself_after_max_seconds(self):
        port = _kit.free_port()
        proc = subprocess.Popen(self.script("serve", "--listen", "127.0.0.1:%d" % port, "--max-seconds", 2),
                                stderr=subprocess.PIPE, universal_newlines=True)
        self.procs.append(proc)
        started = time.time()
        proc.wait(15)
        proc.stderr.close()
        self.assertEqual(proc.returncode, 0)
        self.assertGreaterEqual(time.time() - started, 1.5)


if __name__ == "__main__":
    unittest.main()
