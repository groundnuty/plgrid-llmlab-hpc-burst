"""Tests of the placement middleware with a bare ASGI app (standard library only).

Run: python3 -I spikes/s2b/py/test_s2b_placement.py
"""
import asyncio
import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import s2b_placement  # noqa: E402


def body_for(turn):
    msgs = [{"role": "system", "content": "Deployment R1. sys"}]
    for t in range(turn + 1):
        msgs.append({"role": "user", "content": f"Tool result: words [s2b:R1:7:{t}]"})
        if t < turn:
            msgs.append({"role": "assistant", "content": "reply"})
    return json.dumps({"model": "m", "messages": msgs, "max_tokens": 4}).encode()


class App:
    """Records the body it receives and then waits for one more receive (like a streaming endpoint)."""

    def __init__(self):
        self.body = b""
        self.after = None

    async def __call__(self, scope, receive, send):
        while True:
            m = await receive()
            self.body += m.get("body", b"")
            if not m.get("more_body", False):
                break
        self.after = await receive()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})


def run(mw, path, body, chunks=1, headers=()):
    pieces = [body[i * len(body) // chunks:(i + 1) * len(body) // chunks] for i in range(chunks)]
    queue = [{"type": "http.request", "body": p, "more_body": i < chunks - 1} for i, p in enumerate(pieces)]
    queue.append({"type": "http.disconnect"})
    sent = []

    async def receive():
        return queue.pop(0)

    async def send(m):
        sent.append(m)

    scope = {"type": "http", "method": "POST", "path": path, "headers": list(headers)}
    asyncio.run(mw(scope, receive, send))
    return sent


class PlacementTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.log = os.path.join(self.dir.name, "placement.tsv")
        os.environ["S2B_PLACEMENT_LOG"] = self.log
        os.environ["S2B_WORKER"] = "2"

    def tearDown(self):
        self.dir.cleanup()

    def lines(self):
        with open(self.log) as f:
            return [line.rstrip("\n").split("\t") for line in f]

    def test_logs_highest_turn_and_replays_body(self):
        app = App()
        mw = s2b_placement.Placement(app)
        b = body_for(3)
        sent = run(mw, "/v1/chat/completions", b, chunks=3, headers=[(b"x-session-id", b"R1-s007")])
        self.assertEqual(app.body, b, "the application must receive the body unchanged")
        self.assertEqual(app.after, {"type": "http.disconnect"}, "later receives must reach the real channel")
        self.assertEqual(sent[0]["status"], 200)
        (line,) = self.lines()
        self.assertEqual(line[1:], ["2", "R1", "7", "3", "R1-s007"])

    def test_missing_header_logged_as_dash(self):
        mw = s2b_placement.Placement(App())
        run(mw, "/v1/chat/completions", body_for(0))
        self.assertEqual(self.lines()[0][1:], ["2", "R1", "7", "0", "-"])

    def test_other_paths_pass_through_unlogged(self):
        app = App()
        mw = s2b_placement.Placement(app)
        run(mw, "/v1/completions", body_for(1))
        self.assertEqual(app.body, body_for(1))
        self.assertFalse(os.path.exists(self.log) and os.path.getsize(self.log) > 0)

    def test_untagged_body_not_logged(self):
        mw = s2b_placement.Placement(App())
        run(mw, "/v1/chat/completions", b'{"messages":[{"role":"user","content":"hi"}]}')
        self.assertEqual(os.path.getsize(self.log), 0)


if __name__ == "__main__":
    unittest.main()
