#!/usr/bin/env python3
"""Spike S16's results: from the hosted runs' raw files to the committed CSVs, and the tables.

    python3 summarize.py extract <artifacts dir> results     every raw run under the dir, to results/
    python3 summarize.py tables  [results]                    the README's tables, from results/ alone
    python3 summarize.py verify  [results]                    every CPU figure again from the samples
    python3 summarize.py budget  [results]                    the per-front rates with the measured reserves

extract reads each run's raw directory (S16_RESULTS/raw/<run_id>/, as s16.py wrote it) and writes,
per item, one CSV row per run with the measures analysis.py computes, plus the evidence behind them:
each re-login run's per-Instance times and pauses, every plugin call, the link's per-second bytes,
every tunnel's emptiest second, and the /proc samples. The large files are xz-compressed (preset 6)
and read back byte for byte before they are kept. tables and verify read only results/.
"""
import csv
import glob
import io
import json
import lzma
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import analysis  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))


# --- writing --------------------------------------------------------------------------------------

def flat(d, prefix=""):
    out = {}
    for k, v in d.items():
        key = prefix + k
        if isinstance(v, dict) and k in ("registered_s", "routed_s", "first_token_s", "pause_start_rel_cut",
                                                    "first_request_start_s", "first_request_ttft_s"):
            for kk, vv in v.items():
                out[key + "_" + kk] = vv
        elif isinstance(v, (dict, list)):
            out[key] = json.dumps(v, sort_keys=True)
        else:
            out[key] = v
    return out


def write_csv(path, rows, compress=False):
    if not rows:
        return
    keys = []
    for r in rows:
        for k in r:
            if k not in keys:
                keys.append(k)
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=keys, lineterminator="\n")
    w.writeheader()
    for r in rows:
        w.writerow({k: fmt(r.get(k)) for k in keys})
    data = buf.getvalue().encode()
    if compress:
        packed = lzma.compress(data, preset=6)
        if lzma.decompress(packed) != data:
            raise SystemExit("xz did not read %s back byte for byte" % path)
        with open(path + ".xz", "wb") as fh:
            fh.write(packed)
    else:
        with open(path, "wb") as fh:
            fh.write(data)


def fmt(v):
    if v is None:
        return ""
    if isinstance(v, float):
        if abs(v) >= 1e5:  # Unix times and byte counts: to the microsecond, never rounded to 6 digits
            return ("%.6f" % v).rstrip("0").rstrip(".")
        return "%.6g" % v
    return v


def read_csv(path):
    if os.path.exists(path + ".xz"):
        with lzma.open(path + ".xz", "rt") as fh:
            return list(csv.DictReader(fh))
    if os.path.exists(path):
        with open(path) as fh:
            return list(csv.DictReader(fh))
    return []


def num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


# --- extract --------------------------------------------------------------------------------------

def sample_rows(rid, samples):
    """Per sample and role: each PID's ticks (frpc summed as one row, its PIDs being fixed for a
    run), and the host's /proc/stat counters."""
    srows, hrows = [], []
    for s in samples:
        for role, ticks in s["ticks"].items():
            if role == "frpc":
                srows.append({"run_id": rid, "t": s["t"], "role": role, "pid": "*", "ticks": sum(ticks.values()),
                              "pids": len(ticks), "rss_kb": s["rss_kb"].get(role), "fds": s["fds"].get(role)})
            else:
                for pid, tk in ticks.items():
                    srows.append({"run_id": rid, "t": s["t"], "role": role, "pid": pid, "ticks": tk, "pids": 1,
                                  "rss_kb": s["rss_kb"].get(role), "fds": s["fds"].get(role)})
        st = s["stat"]
        hrows.append({"run_id": rid, "t": s["t"], "user": st[0], "nice": st[1], "system": st[2], "idle": st[3], "iowait": st[4],
                      "irq": st[5], "softirq": st[6], "steal": st[7], "mem_avail_kb": s.get("mem_avail_kb"),
                      "load1": s["load"][0]})
    return srows, hrows


def rebuild_samples(srows, hrows):
    """samples.json's shape again, from the committed rows."""
    by = {}
    for h in hrows:
        t = float(h["t"])
        by[t] = {"t": t, "ticks": {}, "stat": [int(h[k]) for k in ("user", "nice", "system", "idle", "iowait", "irq", "softirq", "steal")]}
    for r in srows:
        s = by.get(float(r["t"]))
        if s is not None:
            s["ticks"].setdefault(r["role"], {})[r["pid"]] = int(r["ticks"])
    return [by[t] for t in sorted(by)]


def tunnel_rows(rid, load, n, t0, t1):
    ps = analysis.per_second(load, n, t0, t1)
    return [{"run_id": rid, "i": i, "tokens": sum(row), "emptiest_second": min(row) if row else 0} for i, row in enumerate(ps)]


def cmd_extract(src, dst):
    os.makedirs(dst, exist_ok=True)
    out = {k: [] for k in ("relogin", "relogin_instances", "calls", "caplink", "link_series", "rssh", "rfront",
                           "tunnels", "samples", "host")}
    for mpath in sorted(glob.glob(os.path.join(src, "**", "raw", "*", "manifest.json"), recursive=True)):
        d = os.path.dirname(mpath)
        run = analysis.Run(d)
        m = run.m
        rid, item = m.get("run_id"), m.get("item")
        if m.get("rep", 0) == 0 and os.environ.get("S16_INCLUDE_SMOKE") != "1":
            continue  # VM smoke runs (rep 0) are never results; S16_INCLUDE_SMOKE=1 reads them, for a check
        base = {"run_id": rid, "rep": m.get("rep"), "cpu_model": m["host"]["cpu_model"], "cpus": m["host"]["cpus"]}
        samples = run.samples()
        srows, hrows = sample_rows(rid, samples)
        out["samples"] += srows
        out["host"] += hrows
        if item == "relogin":
            r = analysis.relogin(run)
            inst = r.pop("per_instance", [])
            out["relogin"].append(dict(base, variant=m.get("variant"), pre_start=m.get("pre_start"), pre_end=m.get("pre_end"),
                                       **flat(r)))
            for p in inst:
                out["relogin_instances"].append(dict({"run_id": rid}, **flat(p)))
            t_cut = r.get("t_cut") or 0
            for c in run.lines("admit-calls.jsonl") + run.lines("front-calls.jsonl"):
                out["calls"].append({"run_id": rid, "plugin": c["plugin"], "op": c.get("op"), "proxy": c.get("proxy"),
                                     "start_rel_cut_ms": (c["start_ns"] / 1e9 - t_cut) * 1e3,
                                     "latency_ms": (c["end_ns"] - c["start_ns"]) / 1e6, "reject": int(bool(c.get("reject"))),
                                     "reason": c.get("reason") or ""})
            if m.get("pre_start"):
                out["tunnels"] += tunnel_rows(rid, run.load(), run.n, m["pre_start"], m["pre_end"])
        elif item == "caplink":
            r = analysis.caplink(run)
            out["caplink"].append(dict(base, window_start=m.get("window_start"), window_end=m.get("window_end"),
                                       rate_Bps=m.get("rate_Bps"), filler_conns=m.get("filler_conns"), **flat(r)))
            for s in run.lines("link-series.jsonl"):
                out["link_series"].append(dict({"run_id": rid}, **s))
            if m.get("window_start"):
                out["tunnels"] += tunnel_rows(rid, run.load(), run.n, m["window_start"], m["window_end"])
        elif item == "rssh":
            r = analysis.rssh(run)
            common = dict(base, cipher_asked=r["cipher_asked"], negotiated=json.dumps(r.get("negotiated"), sort_keys=True),
                          relays=r["relays"], status=r["status"])
            out["rssh"].append(dict(common, level="idle", streams=0, window_start=m.get("idle_start"), window_end=m.get("idle_end"),
                                    ssh_cores=r.get("idle_ssh_cores"), sshd_cores=r.get("idle_sshd_cores"), ktok_s=0.0, valid=True))
            for lv, raw in zip(r["levels"], m.get("levels", [])):
                out["rssh"].append(dict(common, level="load", window_start=raw["window_start"], window_end=raw["window_end"], **lv))
        elif item == "rfront":
            r = analysis.rfront(run)
            out["rfront"].append(dict(base, window_start=m.get("window_start"), window_end=m.get("window_end"), **flat(r)))
            if m.get("window_start"):
                out["tunnels"] += tunnel_rows(rid, run.load(), run.n, m["window_start"], m["window_end"])
    for name in ("relogin", "caplink", "rssh", "rfront"):
        write_csv(os.path.join(dst, name + ".csv"), out[name])
    for name in ("relogin_instances", "calls", "link_series", "tunnels", "samples", "host"):
        write_csv(os.path.join(dst, name + ".csv"), out[name], compress=True)
    print("extracted: " + ", ".join("%s %d" % (k, len(v)) for k, v in out.items()))
    sys.exit(scan(dst))


def scan(dst):
    """The committed files must name no host path (a login or a job directory can ride in one)."""
    import re
    leak = re.compile(r"/(home|tmp|Users|root|var|mnt|scratch|net)/")
    bad = []
    for p in sorted(glob.glob(os.path.join(dst, "*.csv")) + glob.glob(os.path.join(dst, "*.csv.xz"))):
        text = lzma.open(p, "rt").read() if p.endswith(".xz") else open(p).read()
        if leak.search(text):
            bad.append(os.path.basename(p))
    print("scan: %s" % ("clean" if not bad else "a host path in " + ", ".join(bad)))
    return 1 if bad else 0


# --- tables ---------------------------------------------------------------------------------------

def rng(vals, digits=2):
    v = [x for x in vals if x is not None]
    if not v:
        return "-"
    lo, hi = min(v), max(v)
    f = "%%.%df" % digits
    return f % lo if round(lo, digits) == round(hi, digits) else (f + "–" + f) % (lo, hi)


HOST_BOUND = 0.85  # S14's rule: a run with all CPUs at least 85% busy measures the runner


def vendor(model):
    m = (model or "").upper()
    return "AMD" if "AMD" in m else "Intel" if "INTEL" in m else (model or "?")


def bound(v):
    x = num(v)
    return x is not None and x >= HOST_BOUND


def md(header, rows):
    print("| " + " | ".join(header) + " |")
    print("|" + "|".join("---" if i == 0 else "---:" for i in range(len(header))) + "|")
    for r in rows:
        print("| " + " | ".join(str(x) for x in r) + " |")
    print()


def cmd_tables(res):
    rl = read_csv(os.path.join(res, "relogin.csv"))
    if rl:
        print("### Re-login\n")
        print("Ranges over the runs that count (valid); runs that don't are in the runs and valid columns.\n")
        groups = {}
        for r in rl:
            groups.setdefault((r["variant"], int(r["n"])), []).append(r)
        rows = []
        for (var, n), g in sorted(groups.items(), key=lambda x: (x[0][0], x[0][1])):
            # Only runs that count: every tunnel streamed in every second before the event, with
            # a request in flight across it (the issue's rule); the others are counted, not used.
            f = lambda k: [num(r.get(k)) for r in g if r["valid"] == "True"]
            rows.append([var, n, len(g), "/".join(sorted({vendor(r["cpu_model"]) for r in g})), sum(1 for r in g if r["valid"] == "True"),
                         sum(1 for r in g if bound(r.get("recover_peak_host_busy"))), rng(f("pre_emptiest_second"), 0),
                         rng(f("inflight_tunnels"), 0), rng(f("cut_requests"), 0), rng(f("registered_s_max")), rng(f("routed_s_p50")),
                         rng(f("routed_s_max")), rng(f("first_token_s_p50")), rng(f("first_token_s_max")),
                         rng(f("first_request_ttft_s_p50")), rng(f("first_request_ttft_s_max")), rng(f("not_back_60s"), 0),
                         rng(f("paused"), 0), rng(f("front_term_to_exit_s"), 1), rng(f("recover_front_host_cores")),
                         rng(f("recover_peak_front_host_cores")), rng(f("recover_peak_host_busy"))])
        md(["event", "tunnels", "runs", "runners", "valid", "host-bound second in the recovery", "emptiest pre-second", "in flight at the cut", "cut", "registered again max s",
            "routed again p50 s", "routed again max s", "first token p50 s", "first token max s",
            "first request's TTFT p50 s", "first request's TTFT max s", "not routed again in 60 s", "paused",
            "front TERM to exit s", "front host cores (10 s)", "peak front host cores", "peak host busy"], rows)
        calls = read_csv(os.path.join(res, "calls.csv"))
        lat = {}
        info = {r["run_id"]: r for r in rl}
        for c in calls:
            r = info.get(c["run_id"])
            if not r or r["variant"] == "none" or r["valid"] != "True":
                continue
            s = num(c["start_rel_cut_ms"])
            if s is None or s < -1000 or s > 90000:
                continue
            lat.setdefault((r["variant"], int(r["n"]), c["plugin"], c["op"]), []).append((num(c["latency_ms"]), int(c["reject"])))
        rows = []
        for (var, n, plugin, op), v in sorted(lat.items()):
            xs = [x for x, _ in v]
            rows.append([var, n, plugin, op, len(xs), "%.2f" % analysis.percentile(xs, 50), "%.2f" % analysis.percentile(xs, 99),
                         "%.2f" % max(xs), sum(1 for x in xs if x > analysis.PLUGIN_RULE_MS), sum(rj for _, rj in v)])
        print("#### The plugins' calls, from 1 s before the cut to 90 s after (the valid runs pooled)\n")
        md(["event", "tunnels", "plugin", "op", "calls", "p50 ms", "p99 ms", "max ms", "over 1 s", "refused"], rows)
    cl = read_csv(os.path.join(res, "caplink.csv"))
    if cl:
        print("### The capped link\n")
        rows = []
        for r in sorted(cl, key=lambda r: (int(r["n"]), r["discipline"] + (r["buffer"] or ""), -float(r["cap_mbps"] or 0), int(r["rep"]))):
            capped = num(r["cap_mbps"]) or 0
            counted = ("excluded: not at its cap" if capped and r.get("link_at_cap") != "True"
                       else "excluded: host-bound" if bound(r.get("host_busy")) else "counted")
            rows.append([r["n"], r["streams"], r["cap_mbps"] or "uncapped", r["discipline"] + ((" " + r["buffer"]) if r["buffer"] else ""),
                         "%s (%s)" % (r["rep"], vendor(r["cpu_model"])), counted, r["valid"], "yes" if bound(r.get("host_busy")) else "no", "%.0f" % num(r["offered_tok_s"]), "%.0f" % num(r["delivered_tok_s"]),
                         r.get("link_up_min") and "%.3f" % num(r["link_up_min"]) or "-",
                         r.get("link_down_min") and "%.3f" % num(r["link_down_min"]) or "-", r["link_at_cap"],
                         "%.1f" % num(r["gap_p99_ms"]) if r.get("gap_p99_ms") else "-", r["drops"], r["frpc_relogins"],
                         r["heartbeat_timeout_lines"], r["probes_failed"], "%.2f" % num(r["host_busy"]) if r.get("host_busy") else "-"])
        md(["tunnels", "streams", "cap Mb/s", "queue", "rep (runner)", "counted", "valid", "host-bound", "offered tok/s", "delivered tok/s", "link up min", "link down min",
            "at cap", "gap p99 ms", "list-seconds not online", "frpc re-logins", "heartbeat timeouts", "probes failed", "host busy"], rows)
        cfg = {}
        for r in cl:
            capped = num(r["cap_mbps"]) or 0
            k = (int(r["n"]), r["streams"], r["cap_mbps"] or "uncapped", r["discipline"] + ((" " + r["buffer"]) if r["buffer"] else ""))
            c = cfg.setdefault(k, [0, 0, 0])
            if (capped and r.get("link_at_cap") != "True") or bound(r.get("host_busy")):
                c[1] += 1
            else:
                c[0] += 1
                c[2] += r.get("hb_failed") == "True"
        print("#### Runs counted per configuration (a capped run counts only if every second of its window was at 95% of the cap or more, and a heartbeat finding only where the host stayed under 85% busy)\n")
        md(["tunnels", "streams", "cap Mb/s", "queue", "counted", "excluded", "counted with a heartbeat failure"],
           [list(k) + v for k, v in sorted(cfg.items(), key=lambda x: (x[0][0], x[0][3], -(num(x[0][2]) or 0)))])
    rs = read_csv(os.path.join(res, "rssh.csv"))
    if rs:
        print("### R_ssh\n")
        rows = []
        for r in rs:
            rows.append([r["cipher_asked"], "%s (%s)" % (r["rep"], vendor(r["cpu_model"])), r["level"], r["streams"], "%.2f" % num(r["ktok_s"]), r["valid"],
                         "yes" if bound(r.get("host_busy")) else "no",
                         "%.4f" % num(r["ssh_cores"]) if r.get("ssh_cores") else "-", "%.4f" % num(r["sshd_cores"]) if r.get("sshd_cores") else "-",
                         "%.3f" % num(r["ssh_cores_per_ktok"]) if r.get("ssh_cores_per_ktok") else "-"])
        md(["cipher asked", "rep (runner)", "phase", "streams per tunnel", "k tok/s", "valid", "host-bound", "ssh cores", "sshd cores", "ssh cores per 1k tok/s"], rows)
        fits = {}
        for r in rs:
            if r["valid"] == "True" and r.get("ssh_cores") and not bound(r.get("host_busy")):
                fits.setdefault((r["cipher_asked"], r["rep"], vendor(r["cpu_model"])), []).append((num(r["ktok_s"]), num(r["ssh_cores"])))
        rows = []
        for (c, rep, ven), pts in sorted(fits.items()):
            f = analysis.linfit(pts)
            if f:
                rows.append([c, "%s (%s)" % (rep, ven), "%.4f" % f["slope"], "%.5f" % f["intercept"], "%.5f" % (f["intercept"] / 8), f["n"]])
        print("#### R_ssh fits: ssh cores = R_ssh × (tokens/s ÷ 1000) + fixed, over the idle phase and the valid levels that were not host-bound\n")
        md(["cipher asked", "rep (runner)", "R_ssh (cores per 1k tok/s)", "fixed cores (8 relays)", "fixed per relay", "points"], rows)
        neg = sorted({(r["cipher_asked"], r["negotiated"]) for r in rs})
        print("Negotiated (from every relay's ssh -v log):\n")
        for c, n in neg:
            print("- %s: %s" % (c, n))
        print()
    rf = read_csv(os.path.join(res, "rfront.csv"))
    if rf:
        print("### R_front\n")
        rows_in = [{"n": int(r["n"]), "streams": int(r["streams"]), "rep": r["rep"], "via": r["via"],
                    "valid": r["valid"] == "True" and not bound(r.get("host_busy")),
                    "ktok_s": num(r["ktok_s"]), "front_cores": num(r["front_cores"]), "frps_cores": num(r["frps_cores"])} for r in rf]
        pairs = analysis.rfront_pairs(rows_in)
        frps = {(r["n"], r["streams"], r["rep"], r["via"]): r["frps_cores"] for r in rows_in}
        rows = []
        for p in pairs:
            dfrps = (frps.get((p["n"], p["streams"], p["rep"], "front")) or 0) - (frps.get((p["n"], p["streams"], p["rep"], "tcpmux")) or 0)
            ven = vendor(next((r["cpu_model"] for r in rf if r["rep"] == p["rep"]), ""))
            rows.append([p["n"], p["streams"], "%s (%s)" % (p["rep"], ven), "%.2f" % p["ktok_s"], "%.3f" % p["front_cores_via_front"],
                         "%.3f" % p["front_cores_via_tcpmux"], "%.3f" % p["r_front_per_ktok"], "%.3f" % dfrps,
                         "%.3f" % ((p["delta_cores"] + dfrps) / p["ktok_s"])])
        print("Pairs whose arms were both valid and not host-bound (all CPUs at least 85% busy).\n")
        md(["tunnels", "streams", "rep (runner)", "k tok/s", "front cores via the front", "front cores via tcpmux", "R_front per 1k tok/s",
            "frps cores difference", "front + frps difference per 1k tok/s"], rows)


# --- budget ---------------------------------------------------------------------------------------

# Spike S14's per-runner fits of the measured stack (frps plus the dial-home listener; plus the
# relay owner and its ssh stand-ins in host-dialed mode), front cores = b x tunnels + a x q + c, as
# `python3 spikes/s14/summarize.py fits` prints them from S14's committed results at eaa225ed.
S14_FITS = {
    "direct": {"AMD": [(0.00060, 0.0814, 0.0168), (0.00064, 0.0831, 0.0155)], "Intel": [(0.00039, 0.0491, 0.0218)]},
    "host-dialed": {"AMD": [(0.00156, 0.1275, 0.0255), (0.00151, 0.1340, 0.0274)], "Intel": [(0.00080, 0.0793, 0.0440)]},
}
S14_SHARE = {"direct": 0.45, "host-dialed": 0.25}  # S14's provisional R_front = s x a x q
BUDGET = 0.7  # HBEP-6: the steady-state terms under 70% of the front's cores


def per_core(b, a, c, n, extra):
    """tokens/s one front core carries at n tunnels: b n + (a + extra) q + c = 0.7."""
    return max(0.0, (BUDGET - b * n - c) / (a + extra) * 1000.0)


def measured_slopes(res):
    """R_front and R_ssh per runner class: the slopes of least-squares fits, R_front of the front
    process's extra cores against tokens/s / 1000 over the valid, not host-bound pairs; R_ssh of ssh's
    cores over the idle phase and the valid levels, per cipher."""
    rf = read_csv(os.path.join(res, "rfront.csv"))
    rows_in = [{"n": int(r["n"]), "streams": int(r["streams"]), "rep": r["rep"], "via": r["via"],
                "valid": r["valid"] == "True" and not bound(r.get("host_busy")), "ktok_s": num(r["ktok_s"]),
                "front_cores": num(r["front_cores"]), "cpu": vendor(r["cpu_model"])} for r in rf]
    cpu = {r["rep"]: r["cpu"] for r in rows_in}
    pts = {}
    for p in analysis.rfront_pairs(rows_in):
        pts.setdefault(cpu.get(p["rep"]), []).append((p["ktok_s"], p["delta_cores"]))
    r_front = {k: analysis.linfit(v) for k, v in pts.items()}
    pts = {}
    for r in read_csv(os.path.join(res, "rssh.csv")):
        if r["valid"] == "True" and r.get("ssh_cores") and not bound(r.get("host_busy")):
            pts.setdefault((vendor(r["cpu_model"]), r["cipher_asked"]), []).append((num(r["ktok_s"]), num(r["ssh_cores"])))
    r_ssh = {k: analysis.linfit(v) for k, v in pts.items()}
    # ssh's cost per token falls as a relay carries more (per-packet cost): the worst per class,
    # at one stream per relay (20 tokens/s), is the reserve for relays that carry little.
    low = {}
    for r in read_csv(os.path.join(res, "rssh.csv")):
        if r["level"] == "load" and r["streams"] == "1" and r.get("ssh_cores_per_ktok") and r["valid"] == "True":
            k = vendor(r["cpu_model"])
            low[k] = max(low.get(k, 0.0), num(r["ssh_cores_per_ktok"]))
    return r_front, r_ssh, low


def lstsq(rows, ys):
    """Least squares by the normal equations, solved with partial pivoting (no numpy)."""
    k = len(rows[0])
    m = [[sum(x[i] * x[j] for x in rows) for j in range(k)] + [sum(x[i] * y for x, y in zip(rows, ys))] for i in range(k)]
    for i in range(k):
        p = max(range(i, k), key=lambda r: abs(m[r][i]))
        m[i], m[p] = m[p], m[i]
        for r in range(k):
            if r != i:
                f = m[r][i] / m[i][i]
                for j in range(i, k + 1):
                    m[r][j] -= f * m[i][j]
    return [m[i][k] / m[i][i] for i in range(k)]


def host_features(q, n):
    """The refit's terms: tokens/s / 1000, tunnels, their product (a per-token cost that grows
    with the tunnel count, as the front's did), and a constant."""
    return [q, n, n * q, 1.0]


def front_host_points(res):
    """S16's own measurements of the front host in direct mode, frps plus the front process
    (its proxy, dial-home listener and probe), per runner class: the R_front runs' proxied arm
    (10-100 tunnels, 0.2-7.9 k tokens/s) and the re-login runs' no-event pre-windows (1-600
    tunnels at 5 tokens/s each), the valid runs only and none host-bound."""
    tokens = {}
    for r in read_csv(os.path.join(res, "tunnels.csv")):
        tokens[r["run_id"]] = tokens.get(r["run_id"], 0) + int(r["tokens"])
    pts = {}
    for r in read_csv(os.path.join(res, "rfront.csv")):
        if r["via"] == "front" and r["valid"] == "True" and not bound(r.get("host_busy")):
            pts.setdefault(vendor(r["cpu_model"]), []).append((num(r["ktok_s"]), int(r["n"]), num(r["front_cores"]) + num(r["frps_cores"])))
    for r in read_csv(os.path.join(res, "relogin.csv")):
        if r["variant"] == "none" and r["valid"] == "True" and r.get("pre_start"):
            secs = int(float(r["pre_end"]) - float(r["pre_start"]))
            q = tokens.get(r["run_id"], 0) / secs / 1000.0
            pts.setdefault(vendor(r["cpu_model"]), []).append((q, int(r["n"]), num(r["pre_front_cores"]) + num(r["pre_frps_cores"])))
    return pts


def front_host_fit(points):
    """cores = A q + B n + D n q + C over the points, with the observed/predicted range over the
    points of at least 0.05 cores (smaller ones are within a tick or two of nothing) and the rms."""
    coef = lstsq([host_features(q, n) for q, n, _ in points], [y for _, _, y in points])
    pred = lambda q, n: sum(a * b for a, b in zip(coef, host_features(q, n)))
    ratios = [y / pred(q, n) for q, n, y in points if y >= 0.05 and pred(q, n) > 0]
    rms = (sum((y - pred(q, n)) ** 2 for q, n, y in points) / len(points)) ** 0.5
    return {"A": coef[0], "B": coef[1], "D": coef[2], "C": coef[3], "n_max": max(n for _, n, _ in points),
            "points": len(points), "ratio_min": min(ratios), "ratio_max": max(ratios), "rms": rms}


def refit_per_core(f, n):
    """tokens/s one front core carries at n tunnels under the refit: A q + B n + D n q + C = 0.7."""
    return max(0.0, (BUDGET - f["B"] * n - f["C"]) / (f["A"] + f["D"] * n) * 1000.0)


def cmd_budget(res):
    r_front, r_ssh, low = measured_slopes(res)
    print("Measured slopes (cores per 1,000 tokens/s):")
    for k, f in sorted(r_front.items()):
        print("  R_front %-5s %s" % (k, "%.4f (fixed %.4f, %d pairs)" % (f["slope"], f["intercept"], f["n"]) if f else "-"))
    for (k, c), f in sorted(r_ssh.items()):
        print("  R_ssh   %-5s %-24s %s" % (k, c, "%.4f (fixed %.5f, %d points)" % (f["slope"], f["intercept"], f["n"]) if f else "-"))
    for k, v in sorted(low.items()):
        print("  R_ssh   %-5s at one stream per relay (the worst level, every cipher): %.4f" % (k, v))
    print("\nS16's refit of the front host in direct mode (frps + the front process), cores = A q + B n + D n q + C:")
    for ven, pts in sorted(front_host_points(res).items()):
        f = front_host_fit(pts)
        rates = ", ".join("%d tunnels %5.0f%s" % (n, refit_per_core(f, n), "" if n <= f["n_max"] else " (past the fit's range)")
                          for n in (100, 300, 600))
        print("  %-5s A %.4f, B %.6f, D %.6f, C %.4f; %d points, 1-%d tunnels; observed/predicted %.2f-%.2f, rms %.3f cores"
              % (ven, f["A"], f["B"], f["D"], f["C"], f["points"], f["n_max"], f["ratio_min"], f["ratio_max"], f["rms"]))
        print("        tokens/s per front core at 70%%: %s" % rates)
    print("\nTokens/s per front core at 70%, with S14's per-runner a, b, c (S14_FITS), against S14's provisional reserves:")
    for mode in ("direct", "host-dialed"):
        for ven, fits in S14_FITS[mode].items():
            rf = (r_front.get(ven) or {}).get("slope")
            ssh = {c: f["slope"] for (v, c), f in r_ssh.items() if v == ven and f}
            for n in (100, 300, 600):
                cells = []
                for b, a, c in fits:
                    prov = S14_SHARE[mode] * a + (0.045 if mode == "host-dialed" else 0.0)
                    row = "stack %5.0f, provisional %5.0f" % (per_core(b, a, c, n, 0.0), per_core(b, a, c, n, prov))
                    if rf is not None:
                        if mode == "direct":
                            row += ", measured %5.0f" % per_core(b, a, c, n, rf)
                        else:
                            sl = max(ssh.values()) if ssh else 0.0
                            row += ", measured (fit %.3f) %5.0f" % (sl, per_core(b, a, c, n, rf + sl))
                            if ven in low:
                                row += ", measured (one stream per relay, %.3f) %5.0f" % (low[ven], per_core(b, a, c, n, rf + low[ven]))
                    cells.append(row)
                print("  %-11s %-5s %3d tunnels: %s" % (mode, ven, n, " | ".join(cells)))


# --- verify ---------------------------------------------------------------------------------------

def cmd_verify(res):
    """Every CPU figure in the run CSVs, again from samples.csv.xz and host.csv.xz."""
    srows, hrows = read_csv(os.path.join(res, "samples.csv")), read_csv(os.path.join(res, "host.csv"))
    by_s, by_h = {}, {}
    for r in srows:
        by_s.setdefault(r["run_id"], []).append(r)
    for r in hrows:
        by_h.setdefault(r["run_id"], []).append(r)
    checked, bad = 0, []

    def check(rid, what, got, want):
        nonlocal checked
        if want in (None, ""):
            return
        checked += 1
        if got is None or abs(got - float(want)) > 5e-5 * max(1.0, abs(float(want))):
            bad.append((rid, what, got, want))

    for r in read_csv(os.path.join(res, "rfront.csv")):
        if not r.get("window_start"):
            continue
        s = rebuild_samples(by_s.get(r["run_id"], []), by_h.get(r["run_id"], []))
        a, b = float(r["window_start"]), float(r["window_end"])
        for role in ("front", "frps", "admit", "link", "load", "node", "frpc"):
            check(r["run_id"], role, analysis.cores(s, role, a, b), r.get(role + "_cores"))
        check(r["run_id"], "busy", analysis.host_busy(s, a, b), r.get("host_busy"))
    for r in read_csv(os.path.join(res, "rssh.csv")):
        if not r.get("window_start"):
            continue
        s = rebuild_samples(by_s.get(r["run_id"], []), by_h.get(r["run_id"], []))
        a, b = float(r["window_start"]), float(r["window_end"])
        check(r["run_id"], "ssh %s" % r["streams"], analysis.cores(s, "ssh", a, b), r.get("ssh_cores"))
        check(r["run_id"], "sshd %s" % r["streams"], analysis.cores(s, "sshd", a, b), r.get("sshd_cores"))
    for r in read_csv(os.path.join(res, "relogin.csv")):
        s = rebuild_samples(by_s.get(r["run_id"], []), by_h.get(r["run_id"], []))
        if not r.get("t_cut") or not r.get("pre_start"):
            continue
        a, b = float(r["pre_start"]), float(r["pre_end"])
        c = float(r["t_cut"])
        for role in analysis.FRONT_HOST_ROLES:
            check(r["run_id"], "pre " + role, analysis.cores(s, role, a, b), r.get("pre_%s_cores" % role))
            check(r["run_id"], "recover " + role, analysis.cores(s, role, c, c + analysis.RECOVER_S), r.get("recover_%s_cores" % role))
    for r in read_csv(os.path.join(res, "caplink.csv")):
        s = rebuild_samples(by_s.get(r["run_id"], []), by_h.get(r["run_id"], []))
        if r.get("window_start"):
            check(r["run_id"], "busy", analysis.host_busy(s, float(r["window_start"]), float(r["window_end"])), r.get("host_busy"))
    print("verify: %d CPU figures recomputed from the samples, %d mismatches" % (checked, len(bad)))
    for b in bad[:20]:
        print("  mismatch: %s %s: recomputed %s, committed %s" % b)
    # A check that recomputed nothing proves nothing.
    return 1 if bad or checked == 0 else 0


def main():
    if len(sys.argv) < 2:
        raise SystemExit(__doc__)
    cmd = sys.argv[1]
    if cmd == "extract":
        cmd_extract(sys.argv[2], sys.argv[3] if len(sys.argv) > 3 else os.path.join(HERE, "results"))
    elif cmd == "tables":
        cmd_tables(sys.argv[2] if len(sys.argv) > 2 else os.path.join(HERE, "results"))
    elif cmd == "budget":
        cmd_budget(sys.argv[2] if len(sys.argv) > 2 else os.path.join(HERE, "results"))
    elif cmd == "verify":
        sys.exit(cmd_verify(sys.argv[2] if len(sys.argv) > 2 else os.path.join(HERE, "results")))
    else:
        raise SystemExit(__doc__)


if __name__ == "__main__":
    main()
