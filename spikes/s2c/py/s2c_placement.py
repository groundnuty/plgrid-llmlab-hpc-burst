"""ASGI middleware for spike S2c: record which worker served which session turn, and for how long.

Descends from spike S2b's py/s2b_placement.py (same tag, same body replay). Loaded into each vLLM worker
with ``--middleware=s2c_placement.Placement`` (vLLM adds a class with ``app.add_middleware``). For every
POST to .../chat/completions it reads the request body, finds the workload's tags
``[s2b:<run>:<session>:<turn>]`` and appends one line to the file named by S2B_PLACEMENT_LOG, when the
response ends:

    <start unix> <TAB> <end unix> <TAB> <worker> <TAB> <run> <TAB> <session> <TAB> <turn> <TAB> <x-session-id or ->

Differences from S2b: the line is written once the response is over, and it carries two times. The start
is taken when the request reaches the middleware; the end is taken after the last response body message
was handed to the server (a final ``http.response.body`` without ``more_body``). A request that raises,
or whose application returns without a final body (an aborted stream), is logged once as well, with the
end taken then. A request with no tag writes nothing. The body is replayed unchanged to the application.
The interval [start, end) is what the analysis counts as the request being in flight on this worker.
Placement is thus taken from the worker itself, whatever the router does with headers. Standard library
only.
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


def session_header(scope):
    """The x-session-id request header, or "-"."""
    sid = "-"
    for k, v in scope.get("headers", []):
        if k.lower() == b"x-session-id":
            sid = v.decode("latin-1")
    return sid


class Placement:
    def __init__(self, app, clock=time.time):
        # clock is a seam for tests; vLLM calls Placement(app) and gets the wall clock.
        self.app = app
        self.clock = clock
        self.worker = os.environ.get("S2B_WORKER", "?")
        path = os.environ.get("S2B_PLACEMENT_LOG")
        self.log = open(path, "a", buffering=1) if path else None

    async def __call__(self, scope, receive, send):
        if (self.log is None or scope.get("type") != "http" or scope.get("method") != "POST"
                or not scope.get("path", "").endswith("/chat/completions")):
            await self.app(scope, receive, send)
            return
        start = self.clock()
        msgs, body = [], bytearray()
        while True:
            m = await receive()
            msgs.append(m)
            if m.get("type") != "http.request":
                break
            body += m.get("body", b"")
            if not m.get("more_body", False):
                break
        pending = iter(msgs)

        async def replay():
            for m in pending:
                return m
            return await receive()

        tag = current_tag(bytes(body))
        if tag is None:
            await self.app(scope, replay, send)
            return
        prefix = f"{self.worker}\t{tag[0]}\t{tag[1]}\t{tag[2]}\t{session_header(scope)}"
        written = False

        def write_line():
            nonlocal written
            if not written:
                written = True
                self.log.write(f"{start:.6f}\t{self.clock():.6f}\t{prefix}\n")

        async def ending_send(m):
            await send(m)  # the end time is taken after the message is out, never before
            if m.get("type") == "http.response.body" and not m.get("more_body", False):
                write_line()

        try:
            await self.app(scope, replay, ending_send)
        finally:
            write_line()  # raised, or returned without a final body: logged once, here
