#!/usr/bin/env python3
"""Spike S1c: checks of the harness and its fixtures, each against a value set by the check itself
or read in a component's source, never against the harness's own output. Exit 1 on any failure.

    python3 s1c_selftest.py
"""
import os
import signal
import socket
import subprocess
import sys
import time
import urllib.request

from s1c_lib import PORTS, TOX, Env, http_json, listeners, render_frpc, render_frps, tox_clear, tox_set
from s1c_stack import Stack, run_bench

RESULTS = []


def check(name, ok, detail):
    RESULTS.append((name, ok))
    print("%s  %s: %s" % ("PASS" if ok else "FAIL", name, detail))
    sys.stdout.flush()


def direct_ttft(env, port, cond):
    tox_set("direct", cond, 512)
    s = run_bench(env, ["load", "-target", "127.0.0.1:%d" % port, "-n", "4", "-window", "6s", "-ramp", "1s",
                        "-max-tokens", "20"], env.p("raw", "selftest_%s.json" % cond))
    tox_clear("direct")
    return s


def t_engine(env):
    s = run_bench(env, ["load", "-target", "127.0.0.1:%d" % PORTS["sim"], "-n", "16", "-window", "6s", "-ramp", "1s",
                        "-max-tokens", "40"], env.p("raw", "selftest_raw.json"))
    r, g = s["rate_tok_s_per_stream"]["p50"], s["gap_ms"]["p50"]
    check("engine paces one token per 50 ms (sim flag)", 19.0 <= r <= 20.5 and 48 <= g <= 55 and s["failed"] == 0
          and s["completed"] == s["requests"] > 0, "rate p50 %.2f tok/s, gap p50 %.1f ms, %d/%d complete"
          % (r, g, s["completed"], s["requests"]))


def t_latency(env):
    a = direct_ttft(env, PORTS["tox_direct"], "clean")["ttft_ms"]["p50"]
    b = direct_ttft(env, PORTS["tox_direct"], "lat20")["ttft_ms"]["p50"]
    check("latency toxic adds two one-way delays of 10 ms to a direct TTFT", 17 <= b - a <= 30,
          "clean %.1f ms, lat20 %.1f ms, added %.1f ms (want about 20)" % (a, b, b - a))


def t_bandwidth(env):
    rate = 512   # kB/s, KB = 1000 B (toxics/bandwidth.go: bytes * 1 ms / rate)
    for stream in ("upstream", "downstream"):
        http_json("POST", TOX + "/proxies/bulkdirect/toxics", {"name": "bw_" + stream, "type": "bandwidth",
                  "stream": stream, "attributes": {"rate": rate}})
    t = time.time()
    with urllib.request.urlopen("http://127.0.0.1:%d/bulk?bytes=1536000" % PORTS["tox_bulk"], timeout=30) as r:
        n = len(r.read())
    bps = n / (time.time() - t)
    tox_clear("bulkdirect")
    check("bandwidth toxic caps a bulk transfer at 512,000 B/s", n == 1536000 and abs(bps - 512000) / 512000 < 0.15,
          "%d B at %.0f B/s" % (n, bps))


def fixture_pair():
    srv = socket.socket()
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", PORTS["fixture_srv"]))
    srv.listen(4)
    cli = socket.create_connection(("127.0.0.1", PORTS["tox_fixture"]), timeout=5)
    srv.settimeout(5)
    conn, _ = srv.accept()
    cli.sendall(b"x")
    conn.settimeout(5)
    assert conn.recv(1) == b"x"
    return srv, cli, conn


def eof_within(conn, secs):
    conn.settimeout(secs)
    try:
        return conn.recv(1) == b""
    except socket.timeout:
        return False


def t_fixtures(env, st):
    http_json("POST", TOX + "/proxies", {"name": "fixture", "listen": "127.0.0.1:%d" % PORTS["tox_fixture"],
                                         "upstream": "127.0.0.1:%d" % PORTS["fixture_srv"], "enabled": True})
    leaks = []
    for i in range(10):   # repeated: the first version passed twice and then failed on a race
        srv, cli, conn = fixture_pair()
        st.P.freeze("tox")
        try:
            cli.close()
            silent = not eof_within(conn, 1)
        finally:
            st.P.signal("tox", signal.SIGCONT)
        late = eof_within(conn, 3)
        if not (silent and late):
            leaks.append((i, silent, late))
        conn.close()
        srv.close()
    check("a frozen toxiproxy (SIGSTOP, all threads stopped) is silent: a client close does not reach the server",
          not leaks, "10 tries; failures (try, silent while frozen, EOF after SIGCONT): %s" % (leaks or "none"))
    srv, cli, conn = fixture_pair()
    for stream in ("upstream", "downstream"):
        http_json("POST", TOX + "/proxies/fixture/toxics", {"name": "to_" + stream, "type": "timeout",
                  "stream": stream, "attributes": {"timeout": 0}})
    cli.close()
    passed = eof_within(conn, 3)
    check("toxiproxy's timeout toxic (0) is NOT silent: it passes a close on (toxics/timeout.go)", passed,
          "server saw EOF: %s" % passed)
    conn.close()
    srv.close()
    tox_clear("fixture")
    srv, cli, conn = fixture_pair()
    for stream in ("upstream", "downstream"):
        http_json("POST", TOX + "/proxies/fixture/toxics", {"name": "bw0_" + stream, "type": "bandwidth",
                  "stream": stream, "attributes": {"rate": 0}})
    t = time.time()
    cli.sendall(b"y" * (1 << 20))
    got = 0
    while got < (1 << 20):
        got += len(conn.recv(1 << 16))
    dt = time.time() - t
    check("toxiproxy bandwidth rate 0 does not block (toxics/bandwidth.go: rate <= 0 is unlimited)", dt < 1.0,
          "1 MiB through rate 0 in %.3f s" % dt)
    for s in (cli, conn, srv):
        s.close()
    http_json("DELETE", TOX + "/proxies/fixture")


def t_wss_needs_front(env, st):
    """frps v0.71.1 serves plain websocket only (server/service.go:296-301); a TLS ClientHello goes to
    its frp-TLS listener (:361-364), which expects frp messages, not a websocket upgrade."""
    st.P.start("frps", [env.b("frps"), "-c", render_frps(env, extra={
        "transport.tls.certFile": env.p("certs", "server.crt"), "transport.tls.keyFile": env.p("certs", "server.key")})])
    st.P.start("frpc", [env.b("frpc"), "-c", render_frpc(env, PORTS["frps"], "wss")])
    time.sleep(8)
    online = st.api().get("inst.engine")
    st.P.stop("frpc")
    st.P.stop("frps")
    with open(env.p("logs", "frpc-frp.log"), errors="replace") as fh:
        errs = [l.strip()[24:140] for l in fh if "[W]" in l or "[E]" in l][-1:]
    check("frpc wss straight to frps (TLS cert on frps) does not register", online != "online",
          "proxy status %r; last frpc warning: %s" % (online, errs))


def t_probe_and_listeners(env, st):
    st.arm_up("tcpmux")
    def probe(host):
        out = env.p("raw", "selftest_probe_%s.csv" % host)
        subprocess.run([env.b("s1cbench"), "probe", "-target", "127.0.0.1:%d" % PORTS["mux"], "-mux", host,
                        "-duration", "1s", "-interval", "400ms", "-out", out], check=True)
        with open(out) as fh:
            return sorted({l.split(",")[3] for l in fh})
    a, b = probe("inst"), probe("nobody")
    check("probe: a registered instance answers ok, an unknown one is refused (frps 404)",
          a == ["ok"] and b == ["refused"], "inst %s, nobody %s" % (a, b))
    rows = listeners()
    check("every listener in 29400-29499 is on 127.0.0.1", rows and all(r[0] == "127.0.0.1" for r in rows),
          "%d listeners: %s" % (len(rows), sorted({"%s:%d" % r[:2] for r in rows})))
    st.arm_down()


def main():
    env = Env()
    st = Stack(env)
    try:
        st.base_up()
        t_engine(env)
        t_latency(env)
        t_bandwidth(env)
        t_fixtures(env, st)
        t_wss_needs_front(env, st)
        t_probe_and_listeners(env, st)
    finally:
        st.P.stop_all()
    left = listeners()
    check("nothing left listening on 29400-29499 after the checks", not left, str(left))
    bad = [n for n, ok in RESULTS if not ok]
    print("%d checks, %d failed" % (len(RESULTS), len(bad)))
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
