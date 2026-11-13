"""Tests of the S2c placement middleware with bare ASGI apps (standard library only).

Run: python3 -I -B spikes/s2c/py/test_s2c_placement.py

Times are checked with an event clock: it returns the number of events seen so far (a receive, a send),
so a start or an end time says exactly how many messages had gone out when it was taken. That keeps the
ordering tests off the wall clock.
"""
import asyncio
import json
import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import s2c_placement  # noqa: E402


def body_for(turn):
    msgs = [{"role": "system", "content": "Deployment R1. sys"}]
    for t in range(turn + 1):
        msgs.append({"role": "user", "content": f"Tool result: words [s2b:R1:7:{t}]"})
        if t < turn:
            msgs.append({"role": "assistant", "content": "reply"})
    return json.dumps({"model": "m", "messages": msgs, "max_tokens": 4}).encode()


class Rig:
    """One request through a middleware: the events it saw and what it sent."""

    def __init__(self, log_path):
        self.log_path = log_path
        self.events = []
        self.sent = []
        self.log_size_at_send = []

    def clock(self):
        return float(len(self.events))

    def run(self, mw, path, body, chunks=1, headers=(), method="POST"):
        pieces = [body[i * len(body) // chunks:(i + 1) * len(body) // chunks] for i in range(chunks)]
        queue = [{"type": "http.request", "body": p, "more_body": i < chunks - 1} for i, p in enumerate(pieces)]
        queue.append({"type": "http.disconnect"})

        async def receive():
            self.events.append("receive")
            return queue.pop(0)

        async def send(m):
            self.log_size_at_send.append(os.path.getsize(self.log_path) if os.path.exists(self.log_path) else 0)
            self.events.append("send " + m["type"])
            self.sent.append(m)

        scope = {"type": "http", "method": method, "path": path, "headers": list(headers)}
        asyncio.run(mw(scope, receive, send))
        return self.sent


class App:
    """Reads the whole body, then waits for one more receive (like a streaming endpoint), then answers with
    a start message and `parts` body messages, the last one final."""

    def __init__(self, parts=1, raise_after=None, final=True):
        self.body = b""
        self.after = None
        self.parts = parts
        self.raise_after = raise_after  # number of body messages sent before raising
        self.final = final

    async def __call__(self, scope, receive, send):
        while True:
            m = await receive()
            self.body += m.get("body", b"")
            if not m.get("more_body", False):
                break
        self.after = await receive()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        for i in range(self.parts):
            if self.raise_after is not None and i == self.raise_after:
                raise RuntimeError("engine failed")
            last = i == self.parts - 1
            await send({"type": "http.response.body", "body": b"x", "more_body": not (last and self.final)})
        if self.raise_after is not None and self.raise_after >= self.parts:
            raise RuntimeError("engine failed")


class PlacementTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.log = os.path.join(self.dir.name, "placement.tsv")
        self.saved = {k: os.environ.get(k) for k in ("S2B_PLACEMENT_LOG", "S2B_WORKER")}
        os.environ["S2B_PLACEMENT_LOG"] = self.log
        os.environ["S2B_WORKER"] = "2"
        self.rig = Rig(self.log)
        self.made = []

    def tearDown(self):
        for mw in self.made:
            if mw.log is not None:
                mw.log.close()
        for k, v in self.saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        self.dir.cleanup()

    def mw(self, app, clock=None):
        mw = s2c_placement.Placement(app, clock=clock or self.rig.clock)
        self.made.append(mw)
        return mw

    def lines(self):
        with open(self.log) as f:
            return [line.rstrip("\n").split("\t") for line in f]

    def test_logs_highest_turn_and_replays_body(self):
        app = App()
        b = body_for(3)
        sent = self.rig.run(self.mw(app), "/v1/chat/completions", b, chunks=3, headers=[(b"x-session-id", b"R1-s007")])
        self.assertEqual(app.body, b, "the application must receive the body unchanged")
        self.assertEqual(app.after, {"type": "http.disconnect"}, "later receives must reach the real channel")
        self.assertEqual(sent[0]["status"], 200)
        (line,) = self.lines()
        self.assertEqual(len(line), 7)
        self.assertEqual(line[2:], ["2", "R1", "7", "3", "R1-s007"])

    def test_start_and_end_times_are_columns_one_and_two_in_six_decimals(self):
        # Event clock: the middleware is entered with 0 events, so the start is 0.000000. At the end the events
        # are 3 body-chunk receives, the app's receive past the body (the disconnect), the response start and
        # the final body: 6.
        app = App()
        self.rig.run(self.mw(app), "/v1/chat/completions", body_for(1), chunks=3)
        (line,) = self.lines()
        self.assertEqual(line[0], "0.000000")
        self.assertEqual(line[1], "6.000000")
        self.assertRegex(line[0], r"^\d+\.\d{6}$")
        self.assertRegex(line[1], r"^\d+\.\d{6}$")

    def test_default_clock_is_the_wall_clock(self):
        mw = s2c_placement.Placement(App())
        self.made.append(mw)
        before = time.time()
        self.rig.run(mw, "/v1/chat/completions", body_for(0))
        after = time.time()
        (line,) = self.lines()
        start, end = float(line[0]), float(line[1])
        self.assertLessEqual(before - 1e-6, start)
        self.assertLessEqual(start, end)
        self.assertLessEqual(end, after + 1e-6)

    def test_missing_header_logged_as_dash(self):
        self.rig.run(self.mw(App()), "/v1/chat/completions", body_for(0))
        self.assertEqual(self.lines()[0][2:], ["2", "R1", "7", "0", "-"])

    def test_end_time_is_written_after_the_last_body_message_not_before(self):
        # The app sends: start, body (more), body (more), body (final). With one body chunk in, the events are
        # receive(1), receive(disconnect) = 2, then sends: start = 3, body = 4, body = 5, final body = 6.
        # An end taken before the final body went out would read 5; after the first body, 4.
        app = App(parts=3)
        self.rig.run(self.mw(app), "/v1/chat/completions", body_for(0))
        (line,) = self.lines()
        self.assertEqual(line[0], "0.000000")
        self.assertEqual(line[1], "6.000000")
        self.assertEqual(self.rig.events[-1], "send http.response.body")
        # Observable state, not times: no line existed while any message was being sent, and the line
        # exists once the final one was out.
        self.assertEqual(self.rig.log_size_at_send, [0, 0, 0, 0])

    def test_a_streamed_response_of_several_body_messages_writes_one_line(self):
        app = App(parts=5)
        self.rig.run(self.mw(app), "/v1/chat/completions", body_for(2))
        self.assertEqual(len(self.lines()), 1)
        self.assertEqual(len([e for e in self.rig.events if e == "send http.response.body"]), 5)

    def test_a_request_without_a_tag_writes_nothing(self):
        app = App()
        b = b'{"messages":[{"role":"user","content":"hi"}]}'
        sent = self.rig.run(self.mw(app), "/v1/chat/completions", b)
        self.assertEqual(os.path.getsize(self.log), 0)
        self.assertEqual(app.body, b, "an untagged body is replayed unchanged as well")
        self.assertEqual(sent[-1]["type"], "http.response.body")

    def test_other_paths_pass_through_unlogged(self):
        app = App()
        self.rig.run(self.mw(app), "/v1/completions", body_for(1))
        self.assertEqual(app.body, body_for(1))
        self.assertEqual(os.path.getsize(self.log), 0)

    def test_get_requests_pass_through_unlogged(self):
        app = App()
        self.rig.run(self.mw(app), "/v1/chat/completions", body_for(1), method="GET")
        self.assertEqual(os.path.getsize(self.log), 0)

    def test_a_non_http_scope_passes_through(self):
        seen = []

        async def app(scope, receive, send):
            seen.append(scope["type"])

        mw = self.mw(app)
        asyncio.run(mw({"type": "lifespan"}, None, None))
        self.assertEqual(seen, ["lifespan"])
        self.assertEqual(os.path.getsize(self.log), 0)

    def test_an_app_that_raises_midstream_logs_once_and_the_error_propagates(self):
        # start, one body (more), then the failure: events = 2 receives + start + body = 4 when it is raised.
        app = App(parts=3, raise_after=1)
        with self.assertRaises(RuntimeError):
            self.rig.run(self.mw(app), "/v1/chat/completions", body_for(0))
        (line,) = self.lines()
        self.assertEqual(line[1], "4.000000")

    def test_an_app_that_raises_after_the_final_body_still_logs_one_line(self):
        app = App(parts=2, raise_after=2)
        with self.assertRaises(RuntimeError):
            self.rig.run(self.mw(app), "/v1/chat/completions", body_for(0))
        (line,) = self.lines()
        self.assertEqual(line[1], "5.000000")  # 2 receives + start + 2 bodies, taken at the final body

    def test_an_app_that_returns_without_a_final_body_logs_once_at_the_return(self):
        # An aborted stream: start and one body with more_body still true, then the app returns.
        app = App(parts=1, final=False)
        self.rig.run(self.mw(app), "/v1/chat/completions", body_for(0))
        (line,) = self.lines()
        self.assertEqual(line[1], "4.000000")  # 2 receives + start + body

    def test_no_log_file_configured_passes_everything_through(self):
        del os.environ["S2B_PLACEMENT_LOG"]
        app = App()
        self.rig.run(self.mw(app), "/v1/chat/completions", body_for(1))
        self.assertEqual(app.body, body_for(1))
        self.assertFalse(os.path.exists(self.log))

    def test_two_requests_make_two_lines_in_completion_order(self):
        self.rig.run(self.mw(App()), "/v1/chat/completions", body_for(0))
        self.rig.run(self.mw(App()), "/v1/chat/completions", body_for(1))
        self.assertEqual([l[5] for l in self.lines()], ["0", "1"])

    def test_current_tag_takes_the_highest_turn(self):
        self.assertEqual(s2c_placement.current_tag(body_for(3)), ("R1", 7, 3))
        self.assertIsNone(s2c_placement.current_tag(b"no tag here"))


if __name__ == "__main__":
    unittest.main()
