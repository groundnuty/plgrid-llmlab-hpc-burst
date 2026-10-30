"""Tests of fake_streamer.py (request log, exact health body, framing the strict watcher accepts) and
probe_responder.py (the token, loopback only, bounded lifetime)."""
import os
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _kit  # noqa: E402

sw = _kit.load("stream_watch")


def wait_port_file(path, timeout=10):
    end = time.time() + timeout
    while time.time() < end:
        if os.path.exists(path) and os.path.getsize(path):
            with open(path) as fh:
                return int(fh.read().strip())
        time.sleep(0.05)
    raise AssertionError("no port file %s" % path)


class Engine(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="s1-eng-")
        self.reqlog = os.path.join(self.tmp, "requests")
        portfile = os.path.join(self.tmp, "port")
        self.proc = subprocess.Popen([sys.executable, _kit.path("fake_streamer.py"), "--port", "0",
                                      "--port-file", portfile, "--request-log", self.reqlog],
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.port = wait_port_file(portfile)

    def tearDown(self):
        self.proc.terminate()
        self.proc.wait(10)
        for f in os.listdir(self.tmp):
            os.unlink(os.path.join(self.tmp, f))
        os.rmdir(self.tmp)

    def test_health_answers_the_exact_fixed_body(self):
        with urllib.request.urlopen("http://127.0.0.1:%d/health" % self.port, timeout=5) as r:
            self.assertEqual(r.read(), b'{"status": "ok"}')

    def test_every_request_is_appended_to_the_request_log(self):
        for path in ("/health", "/health?x=1", "/nope"):
            try:
                urllib.request.urlopen("http://127.0.0.1:%d%s" % (self.port, path), timeout=5).read()
            except Exception:
                pass
        with open(self.reqlog) as fh:
            lines = [ln.split() for ln in fh if ln.strip()]
        self.assertEqual([ln[1:] for ln in lines], [["GET", "/health"], ["GET", "/health"], ["GET", "/nope"]])
        self.assertAlmostEqual(float(lines[0][0]), time.time(), delta=30)

    def test_the_strict_watcher_accepts_a_real_two_second_stream(self):
        args = sw.parse_args(["--tcpmux", "127.0.0.1:1", "--domain", "x"])
        st = sw.State(args)
        sw.run_stream(st, sw.Target("direct", "127.0.0.1", self.port), 1, 2)
        self.assertTrue(st.streams[0]["completed"], st.streams[0])

    def test_the_strict_health_check_accepts_the_engine(self):
        target = sw.Target("direct", "127.0.0.1", self.port)
        res = sw.http_get_health(target, time.monotonic() + 3, "/health", "engine", expect=b'{"status": "ok"}')
        sw.close_quiet(res[2], res[3])


class Responder(unittest.TestCase):
    def start(self, *extra):
        self.tmp = tempfile.mkdtemp(prefix="s1-resp-")
        ready = os.path.join(self.tmp, "ready")
        proc = subprocess.Popen([sys.executable, _kit.path("probe_responder.py"), "--port", "0", "--ready-file", ready] + list(extra),
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return proc, wait_port_file(ready)

    def cleanup(self, proc):
        if proc.poll() is None:
            proc.terminate()
        proc.wait(10)
        for f in os.listdir(self.tmp):
            os.unlink(os.path.join(self.tmp, f))
        os.rmdir(self.tmp)

    def test_it_sends_the_fixed_token_line_and_closes(self):
        proc, port = self.start()
        try:
            s = socket.create_connection(("127.0.0.1", port), 5)
            s.settimeout(5)
            data = b""
            while not data.endswith(b"\n"):
                chunk = s.recv(100)
                if not chunk:
                    break
                data += chunk
            self.assertEqual(data, b"S1-PROBE-TOKEN-OK\n")
            self.assertEqual(s.recv(10), b"", "it closes after the token")
            s.close()
        finally:
            self.cleanup(proc)

    def test_it_listens_on_loopback_only(self):
        proc, port = self.start()
        try:
            with open("/proc/net/tcp") as fh:
                rows = [ln.split() for ln in fh.readlines()[1:]]
            mine = [r for r in rows if r[1].endswith(":%04X" % port) and r[3] == "0A"]
            self.assertEqual([r[1] for r in mine], ["0100007F:%04X" % port])
        finally:
            self.cleanup(proc)

    def test_it_stops_by_itself_after_max_seconds(self):
        proc, _ = self.start("--max-seconds", "1")
        try:
            self.assertEqual(proc.wait(10), 0)
        finally:
            self.cleanup(proc)


if __name__ == "__main__":
    unittest.main()
