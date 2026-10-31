"""ASGI middleware for spike S2 phase B: record which worker served which session turn.

Loaded into each vLLM worker with ``--middleware s2b_placement.Placement`` (vLLM adds a class with
``app.add_middleware``). For every POST to .../chat/completions it reads the request body, finds the
workload's tags ``[s2b:<run>:<session>:<turn>]`` and appends one line to the file named by
S2B_PLACEMENT_LOG:

    <unix time> <TAB> <worker> <TAB> <run> <TAB> <session> <TAB> <turn> <TAB> <x-session-id header or ->

The tag of the highest turn is the current one: earlier turns' user messages are part of the history.
The body is replayed unchanged to the application. Placement is thus taken from the worker itself,
whatever the router does with headers. Standard library only.
"""
import os
import re
import time

TAG = re.compile(rb"\[s2b:([A-Za-z0-9_.-]+):(\d+):(\d+)\]")


def current_tag(body):
    """Return (run, session, turn) of the highest-turn tag in body, or None."""
    best = None
    for run, s, t in TAG.findall(body):
        if best is None or int(t) > best[2]:
            best = (run.decode(), int(s), int(t))
    return best


class Placement:
    def __init__(self, app):
        self.app = app
        self.worker = os.environ.get("S2B_WORKER", "?")
        path = os.environ.get("S2B_PLACEMENT_LOG")
        self.log = open(path, "a", buffering=1) if path else None

    async def __call__(self, scope, receive, send):
        if (self.log is None or scope.get("type") != "http" or scope.get("method") != "POST"
                or not scope.get("path", "").endswith("/chat/completions")):
            await self.app(scope, receive, send)
            return
        msgs, body = [], bytearray()
        while True:
            m = await receive()
            msgs.append(m)
            if m.get("type") != "http.request":
                break
            body += m.get("body", b"")
            if not m.get("more_body", False):
                break
        tag = current_tag(bytes(body))
        sid = "-"
        for k, v in scope.get("headers", []):
            if k.lower() == b"x-session-id":
                sid = v.decode("latin-1")
        if tag is not None:
            self.log.write(f"{time.time():.6f}\t{self.worker}\t{tag[0]}\t{tag[1]}\t{tag[2]}\t{sid}\n")
        pending = iter(msgs)

        async def replay():
            for m in pending:
                return m
            return await receive()

        await self.app(scope, replay, send)
