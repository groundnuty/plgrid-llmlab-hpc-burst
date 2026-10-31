#!/usr/bin/env python3
"""Spike S1c (issue #66): frp tunnel capacity, head-of-line blocking and dead-tunnel detection.

    python3 s1c.py matrix --pass N     capacity matrix: 4 arms x {clean, lat20, bw} x {1,16,64,256}
    python3 s1c.py hol --pass N        head-of-line: fast streams + one bulk / slow-read reply
    python3 s1c.py heartbeat --reps N  detection after SIGKILL / a frozen path, per heartbeat setting
    python3 s1c.py leftovers           nothing of ours runs or listens on 29400-29499

Needs S1C_SCRATCH (bin/ with frps, frpc, toxiproxy-server, s1cbench), S1C_SIM and S1C_ENVOY.
Results (small CSV) go to results/ next to this file; raw JSON stays in S1C_SCRATCH/raw.
"""
import argparse
import csv
import json
import os
import signal
import sys

from s1c_lib import HB, PORTS, Env, listeners, load_gate, tox_clear, tox_set
from s1c_stack import (DIAL, TOX_OF_ARM, Meter, Stack, count_lines, note_load, run_bench, wire_bytes)
import s1c_heartbeat

HERE = os.path.dirname(os.path.abspath(__file__))
RESULTS = os.environ.get("S1C_RESULTS") or os.path.join(HERE, "results")
ARMS = ("direct", "tcpmux", "wss", "ssh")
NS = (1, 16, 64, 256)
BW_KBPS = 512          # matrix cap: 512,000 B/s per toxiproxy connection (KB = 1000 B)
HOL_RATE = 1_000_000   # head-of-line cap: 1,000,000 B/s shared by every connection of the shaper


def append_row(path, row):
    new = not os.path.exists(path)
    with open(path, "a", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(row))
        if new:
            w.writeheader()
        w.writerow(row)


# --- capacity matrix ------------------------------------------------------------------------------

def matrix_cell(st, env, pas, arm, cond, n, args):
    l0 = load_gate()
    target = ["-target", "127.0.0.1:%d" % PORTS["mux"], "-mux", "inst"]
    if arm == "direct":
        port = {"raw": PORTS["sim"], "bw-shared": PORTS["sh_sim"]}.get(cond, PORTS["tox_direct"])
        target = ["-target", "127.0.0.1:%d" % port]
    if cond in ("clean", "lat20", "bw"):
        tox_set(TOX_OF_ARM[arm], cond, BW_KBPS)
    link_port = DIAL.get((arm, "tox"))
    w0 = wire_bytes(link_port) if link_port else 0
    m = Meter(st.roles())
    tag = "p%d_%s_%s_%d" % (pas, arm, cond, n)
    s = run_bench(env, ["load"] + target + ["-n", str(n), "-window", "%ds" % args.window, "-ramp", "3s",
                                            "-max-tokens", "60", "-label", tag], env.p("raw", tag + ".json"))
    res = m.done()
    w1 = wire_bytes(link_port) if link_port else 0
    if cond in ("clean", "lat20", "bw"):
        tox_clear(TOX_OF_ARM[arm])
    toks = s["all_tokens"]
    row = dict(pass_=pas, arm=arm, cond=cond, n=n, requests=s["requests"], completed=s["completed"],
               failed=s["failed"], ttft_p50=round(s["ttft_ms"]["p50"], 1), ttft_p95=round(s["ttft_ms"]["p95"], 1),
               gap_p50=round(s["gap_ms"]["p50"], 1), gap_p95=round(s["gap_ms"]["p95"], 1),
               gap_p99=round(s["gap_ms"]["p99"], 1), gap_max=round(s["gap_ms"]["max"], 1),
               rate_p50=round(s["rate_tok_s_per_stream"]["p50"], 2), rate_p5=round(s["rate_tok_s_per_stream"]["p5"], 2),
               tok_s=round(s["tokens_per_s"], 1), body_B_tok=round(s["body_bytes_per_token"], 1),
               wire_B_tok=round((w1 - w0) / toks, 1) if link_port and toks else "",
               load_start=l0, load_end=note_load("cell %s done" % tag),
               errors=";".join("%s=%d" % kv for kv in s["errors"].items())[:300])
    for k in ("frpc", "frps", "envoy", "ssh", "sshd", "tox", "shaper", "sim"):
        row["cpu_" + k] = res.get("cpu_" + k, "")
        row["rss_" + k] = res.get("rss_" + k, "")
    append_row(os.path.join(RESULTS, "matrix.csv"), row)


def cmd_matrix(args):
    env = Env()
    st = Stack(env)
    os.makedirs(RESULTS, exist_ok=True)
    try:
        load_gate()
        st.base_up()
        for arm in args.arms:
            st.arm_up(arm)
            conds = ["clean", "lat20", "bw"] + (["raw", "bw-shared"] if arm == "direct" else [])
            for cond in conds:
                if cond == "bw-shared":
                    st.shaper_up(BW_KBPS * 1000)
                for n in args.ns:
                    matrix_cell(st, env, args.pass_, arm, cond, n, args)
                if cond == "bw-shared":
                    st.P.stop("shaper")
            st.arm_down()
    finally:
        st.P.stop_all()


# --- head-of-line ---------------------------------------------------------------------------------

HOL_CONFIGS = {   # name: (arm, link, tcpMux, heartbeat setting)
    "tcpmux-shared": ("tcpmux", "shaper", True, "s1"),
    "wss-shared": ("wss", "shaper", True, "s1"),
    "ssh-shared": ("ssh", "shaper", True, "s1"),
    "tcp-separate": ("tcpmux", "shaper", False, "s1"),
    "direct-separate": ("direct", "shaper", None, "s1"),
    "tcpmux-shared-toxiproxy": ("tcpmux", "tox", True, "s1"),
    "tcpmux-shared-tight": ("tcpmux", "shaper", True, "tight"),
}
HOL_VARIANTS = {  # name: (cap B/s, bulk bytes, read rate B/s, give up after s)
    "bulk": (HOL_RATE, 4 << 20, 0, 20),
    "slow": (HOL_RATE, 64 << 20, 64 << 10, 12),
    "nocap": (0, 4 << 20, 0, 20),
}


def hol_run(st, env, pas, cfg, var):
    arm, link, tcpmux, setting = HOL_CONFIGS[cfg]
    cap, nbytes, rrate, gave = HOL_VARIANTS[var]
    load_gate()
    if link == "shaper":
        st.shaper_up(cap)
    st.arm_up(arm, via=link, tcpmux=tcpmux, setting=setting)
    if link == "tox":
        tox_clear("frp")
        if cap:
            tox_set("frp", "bw", cap // 1000)
    m = Meter(st.roles())
    if arm == "direct":
        tgt = ["-fast-target", "127.0.0.1:%d" % PORTS["sh_sim"], "-bulk-target", "127.0.0.1:%d" % PORTS["sh_bulk"]]
    else:
        mux = "127.0.0.1:%d" % PORTS["mux"]
        tgt = ["-fast-target", mux, "-fast-mux", "inst", "-bulk-target", mux, "-bulk-mux", "bulk"]
    tag = "hol_p%d_%s_%s" % (pas, cfg, var)
    s = run_bench(env, ["hol"] + tgt + ["-n", "16", "-total", "30s", "-bulk-at", "8s", "-bulk-bytes", str(nbytes),
                                        "-bulk-read-rate", str(rrate), "-bulk-max", "%ds" % gave, "-label", tag],
                  env.p("raw", tag + ".json"))
    res = m.done()
    needles = ("login to server success", "heartbeat timeout", "keepalive failed", "session shutdown")
    rec = count_lines(env.p("logs", "frpc-frp.log"), needles)
    if link == "tox":
        tox_clear("frp")
    st.arm_down()
    if link == "shaper":
        st.P.stop("shaper")
    row = dict(pass_=pas, config=cfg, variant=var, cap_Bps=cap, bulk_bytes=nbytes, read_rate=rrate,
               bulk_read=s["bulk_bytes_read"], bulk_Bps=round(s["bulk_Bps"]), bulk_s=round(s["bulk_end_s"] - s["bulk_start_s"], 2),
               fast_complete=s["fast_complete"], fast_n=s["fast_streams"])
    for ph in ("before", "during", "after"):
        row[ph + "_gap_p50"] = round(s[ph]["gap_ms"]["p50"], 1)
        row[ph + "_gap_p99"] = round(s[ph]["gap_ms"]["p99"], 1)
        row[ph + "_gap_max"] = round(s[ph]["gap_ms"]["max"], 1)
        row[ph + "_late_p95"] = round(s[ph]["lateness_ms"]["p95"], 1)
        row[ph + "_late_max"] = round(s[ph]["lateness_ms"]["max"], 1)
    row["frpc_logins"] = rec["login to server success"]
    row["frpc_hb_timeouts"] = rec["heartbeat timeout"]
    row["cpu_frpc"], row["cpu_frps"] = res.get("cpu_frpc", ""), res.get("cpu_frps", "")
    row["load_end"] = note_load("hol %s done" % tag)
    append_row(os.path.join(RESULTS, "hol.csv"), row)


def cmd_hol(args):
    env = Env()
    st = Stack(env)
    os.makedirs(RESULTS, exist_ok=True)
    try:
        load_gate()
        st.base_up()
        for cfg in args.configs:
            for var in args.variants:
                hol_run(st, env, args.pass_, cfg, var)
    finally:
        st.P.stop_all()


# --- leftovers ------------------------------------------------------------------------------------

def cmd_leftovers(_args):
    env = Env()
    rows = listeners()
    print("$ ss -ltnH, ports 29400-29499:", rows if rows else "none")
    try:
        with open(env.p("run", "pids.json")) as fh:
            rec = json.load(fh)
    except (OSError, ValueError):
        rec = {}
    alive = [k for k, (pid, stt) in rec.items() if os.path.exists("/proc/%d" % pid)]
    print("recorded processes still alive:", alive if alive else "none")
    return 1 if rows or alive else 0


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    m = sub.add_parser("matrix")
    m.add_argument("--pass", dest="pass_", type=int, default=1)
    m.add_argument("--arms", nargs="+", default=list(ARMS), choices=ARMS)
    m.add_argument("--ns", nargs="+", type=int, default=list(NS))
    m.add_argument("--window", type=int, default=24)
    h = sub.add_parser("hol")
    h.add_argument("--pass", dest="pass_", type=int, default=1)
    h.add_argument("--configs", nargs="+", default=list(HOL_CONFIGS), choices=list(HOL_CONFIGS))
    h.add_argument("--variants", nargs="+", default=list(HOL_VARIANTS), choices=list(HOL_VARIANTS))
    b = sub.add_parser("heartbeat")
    b.add_argument("--reps", type=int, default=3)
    b.add_argument("--settings", nargs="+", default=list(HB), choices=list(HB))
    b.add_argument("--scenarios", nargs="+", default=list(s1c_heartbeat.SCENARIOS), choices=list(s1c_heartbeat.SCENARIOS))
    sub.add_parser("leftovers")
    args = ap.parse_args()
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
    fn = {"matrix": cmd_matrix, "hol": cmd_hol, "leftovers": cmd_leftovers,
          "heartbeat": lambda a: s1c_heartbeat.run(a, RESULTS)}[args.cmd]
    sys.exit(fn(args) or 0)


if __name__ == "__main__":
    main()
