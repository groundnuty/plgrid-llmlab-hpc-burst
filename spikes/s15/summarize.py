#!/usr/bin/env python3
"""Spike S15: the README's tables, from the committed results.

    python3 summarize.py extract <raw dir> <rep dir>   raw files -> streams.csv, transfers.csv, links.csv
    python3 summarize.py tables [results dir]          the tables, as Markdown, from results/rep*/

extract keeps, from each run's raw file, every token's arrival per stream request (streams.csv), every
transfer with its receiver-side steps (transfers.csv) and the link's per-connection bytes (links.csv),
so the tables recompute the gaps from the arrivals; runs.csv, samples.csv and host.csv are the
driver's.
"""
import csv
import glob
import json
import os
import statistics
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import analysis  # noqa: E402

MODES = ("direct", "host-dialed")
LAYOUTS = ("a", "b25", "b50", "b75", "c", "d")
KINDS = ("download", "embeddings", "upload")
TR_FIELDS = ["kind", "start_ns", "sent_ns", "resp_ns", "recv_first_ns", "recv_last_ns", "end_ns", "bytes", "status", "err"]


def extract(raw_dir, rep_dir):
    os.makedirs(rep_dir, exist_ok=True)
    srows, trows, lrows = [], [], []
    for path in sorted(glob.glob(os.path.join(raw_dir, "*.json"))):
        with open(path) as fh:
            raw = json.load(fh)
        rid, load = raw["row"]["run_id"], raw.get("load")
        if not load:
            continue
        for j, runs in enumerate(load["streams_runs"]):
            for k, r in enumerate(runs):
                srows.append({"run_id": rid, "stream": j, "request": k, "start_ns": r["start_ns"], "end_ns": r["end_ns"],
                              "status": r["status"], "err": r.get("err", ""), "tokens_ns": " ".join(str(t) for t in r["tokens_ns"])})
        for i, tr in enumerate(load["transfers"]):
            row = {"run_id": rid, "transfer": i, "reference_start_ns": load["reference_start_ns"],
                   "reference_end_ns": load["reference_end_ns"], "streams": load["streams"]}
            row.update({k: tr.get(k, "") for k in TR_FIELDS})
            row["steps_bytes"] = " ".join(str(x) for x in tr.get("steps_bytes") or [])
            trows.append(row)
        if not load["transfers"]:
            trows.append({"run_id": rid, "transfer": -1, "reference_start_ns": load["reference_start_ns"],
                          "reference_end_ns": load["reference_end_ns"], "streams": load["streams"]})
        for c in raw.get("link") or []:
            lrows.append({"run_id": rid, **c})
    for name, rows in (("streams.csv", srows), ("transfers.csv", trows), ("links.csv", lrows)):
        if not rows:
            continue
        fields = []
        for r in rows:
            for k in r:
                if k not in fields:
                    fields.append(k)
        with open(os.path.join(rep_dir, name), "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=fields)
            w.writeheader()
            w.writerows(rows)
    print("%s: %d stream requests, %d transfers, %d link connections" % (rep_dir, len(srows), len(trows), len(lrows)))


def num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def loads(root):
    """Each run's load, rebuilt from streams.csv and transfers.csv, by run_id; and runs.csv's rows."""
    runs, out = [], {}
    for d in sorted(glob.glob(os.path.join(root, "rep*"))):
        with open(os.path.join(d, "runs.csv")) as fh:
            runs += list(csv.DictReader(fh))
        with open(os.path.join(d, "transfers.csv")) as fh:
            for r in csv.DictReader(fh):
                L = out.setdefault(r["run_id"], {"streams_runs": [[] for _ in range(int(r["streams"]))], "transfers": [],
                                                 "streams": int(r["streams"]),
                                                 "reference_start_ns": int(r["reference_start_ns"]),
                                                 "reference_end_ns": int(r["reference_end_ns"])})
                if int(r["transfer"]) < 0:
                    continue
                tr = {k: r[k] for k in TR_FIELDS}
                for k in ("start_ns", "sent_ns", "resp_ns", "recv_first_ns", "recv_last_ns", "end_ns", "bytes", "status"):
                    tr[k] = int(tr[k]) if tr[k] not in ("", None) else 0
                tr["steps_bytes"] = [int(x) for x in r["steps_bytes"].split()] if r.get("steps_bytes") else []
                L["transfers"].append(tr)
        sp = os.path.join(d, "streams.csv")
        if os.path.exists(sp):
            with open(sp) as fh:
                for r in csv.DictReader(fh):
                    L = out.get(r["run_id"])
                    if L is None:
                        continue
                    L["streams_runs"][int(r["stream"])].append(
                        {"start_ns": int(r["start_ns"]), "end_ns": int(r["end_ns"]), "status": int(r["status"]),
                         "tokens_ns": [int(t) for t in r["tokens_ns"].split()]})
    return runs, out


def pooled_gaps(lds):
    """The gaps of several runs pooled: (reference gaps, gaps during a transfer), in ms."""
    ref, bulk = [], []
    for L in lds:
        spans = [analysis.transfer_window(tr) for tr in L["transfers"]]
        rs0, re0 = L["reference_start_ns"], L["reference_end_ns"]
        for rs in L["streams_runs"]:
            for r in rs:
                tk = r["tokens_ns"]
                for a, b in zip(tk, tk[1:]):
                    g = (b - a) / 1e6
                    if any(a < we and b > ws for ws, we in spans):
                        bulk.append(g)
                    elif rs0 <= a and b <= re0:
                        ref.append(g)
    return ref, bulk


def rng(xs, fmt):
    xs = [x for x in xs if x is not None]
    if not xs:
        return "-"
    lo, mid, hi = min(xs), statistics.median(xs), max(xs)
    if fmt.format(lo) == fmt.format(hi):
        return fmt.format(mid)
    return (fmt + " ({}–{})").format(mid, fmt.format(lo), fmt.format(hi))


def f1(x):
    return "-" if x is None else "%.1f" % x


def tables(root):
    runs, lds = loads(root)
    meas = [r for r in runs if r["layout"] != "cal"]
    cal = [r for r in runs if r["layout"] == "cal"]
    g = {}
    for r in meas:
        g.setdefault((r["mode"], r["layout"], r["kind"]), []).append(r)
    keys = sorted(g, key=lambda k: (MODES.index(k[0]), KINDS.index(k[2]) if k[2] in KINDS else 9, LAYOUTS.index(k[1])))
    p = print
    p("### Path rate (calibration)\n")
    p("One uncapped 64 MiB download in layout (a), with no stream, per mode and repetition; the median of its transfers at the receiver. Layout (b)'s caps are 25, 50 and 75% of it, in frp's MB of 1,048,576 bytes.\n")
    p("| mode | path rate, MB/s (reps) |")
    p("|---|---|")
    for mode in MODES:
        p("| %s | %s |" % (mode, " / ".join("%.1f" % (num(r["recv_rate_median"]) / 1e6) for r in cal if r["mode"] == mode and num(r.get("recv_rate_median")))))
    p("\n### The token streams during a transfer\n")
    p("Inter-token gaps of the 8 streams, pooled over the repetitions' valid runs (5 transfers each): during a transfer (a gap overlapping a transfer's span) against the same run's reference window with no transfer. The engine double sends a token every 50 ms.\n")
    p("| mode | transfer | layout | runs valid | reference gap p50 / p99 / max, ms | gap during the transfer p50 / p99 / max, ms | gaps during |")
    p("|---|---|---|---:|---|---|---:|")
    for k in keys:
        rs = g[k]
        valid = [r for r in rs if r.get("valid") == "True"]
        ref, bulk = pooled_gaps([lds[r["run_id"]] for r in valid if r["run_id"] in lds])
        q = lambda xs: "%s / %s / %s" % (f1(analysis.pct(xs, 50)), f1(analysis.pct(xs, 99)), f1(max(xs) if xs else None))  # noqa: E731
        p("| %s | %s | %s | %d/%d | %s | %s | %d |" % (k[0], k[2], k[1], len(valid), len(rs), q(ref), q(bulk), len(bulk)))
    p("\n### The bulk transfer at the receiver\n")
    p("Median over the repetitions (range in brackets). The rate is bytes over the receiver's first-to-last byte; the first second's bytes and the rate after it show a cap's burst and whether it binds. For an upload the receiver is the node.\n")
    p("| mode | transfer | layout | cap, MB/s | rate, MB/s | time, s | first second, MB | rate after the first second, MB/s | binds |")
    p("|---|---|---|---:|---:|---:|---:|---:|---|")
    for k in keys:
        rs = [r for r in g[k] if r["status"] == "ok"]
        cap = [num(r.get("cap_bytes_s")) for r in rs]
        cap = [c for c in cap if c]
        rate = [num(r.get("recv_rate_median")) for r in rs]
        after = [num(r.get("after_first_second_rate_median")) for r in rs]
        binds = "-"
        if cap and any(a for a in after if a):
            binds = "yes" if all(a <= 1.1 * c for a, c in zip([a for a in after if a], cap)) else "no"
        elif cap:
            binds = "no (done in the first second)"
        mb = lambda xs: [x / 1e6 for x in xs if x]  # noqa: E731
        p("| %s | %s | %s | %s | %s | %s | %s | %s | %s |" % (
            k[0], k[2], k[1], rng(mb(cap), "{:.1f}"), rng(mb(rate), "{:.1f}"),
            rng([num(r.get("recv_time_median_s")) for r in rs], "{:.2f}"),
            rng(mb([num(r.get("first_second_bytes_median")) for r in rs]), "{:.1f}"),
            rng(mb(after), "{:.1f}"), binds))
    p("\n### Uploads: when the receiver had the last byte\n")
    p("How long after the client's last write the node read the upload's last byte: the part of an upload a front-side timing does not see (#273).\n")
    p("| mode | layout | receiver's last byte after the client's last write, ms |")
    p("|---|---|---:|")
    for k in keys:
        if k[2] != "upload":
            continue
        p("| %s | %s | %s |" % (k[0], k[1], rng([num(r.get("upload_recv_last_after_sent_ms_median")) for r in g[k]], "{:.0f}")))
    p("\n### Connections and the front's CPU\n")
    p("The link's connections over the run (the emulator's per-connection counts), those that carried more than 1 MiB, and the bulk frpc's established connections at their most. Front CPU is frps (+ the second frps) + listener (+ owner + ssh stand-ins), in cores, over the reference window and over the transfers' phase.\n")
    p("| mode | transfer | layout | link connections | over 1 MiB | bulk frpc connections | front cores, reference | front cores, transfers | host busy, transfers |")
    p("|---|---|---|---:|---:|---:|---:|---:|---:|")
    for k in keys:
        rs = [r for r in g[k] if r["status"] == "ok"]
        col = lambda c, f: rng([num(r.get(c)) for r in rs], f)  # noqa: E731
        p("| %s | %s | %s | %s | %s | %s | %s | %s | %s |" % (k[0], k[2], k[1], col("link_conns", "{:.0f}"), col("link_conns_over_1mib", "{:.0f}"),
                                                            col("conns_max_frpc-bulk", "{:.0f}"), col("cpu_ref_front", "{:.3f}"),
                                                            col("cpu_bulk_front", "{:.3f}"), col("host_busy_bulk", "{:.2f}")))
    p("\n### Tunnel losses\n")
    p("Summed over all %d runs: frps's lists (a proxy not online or registered again), frpc logins beyond the first (both frpc), relay sessions beyond the first, and heartbeat-timeout lines.\n" % len(runs))
    s = lambda c: sum(int(float(r[c])) for r in runs if r.get(c) not in (None, ""))  # noqa: E731
    p("| drops | frpc re-logins | relay restarts | heartbeat-timeout lines | unreadable lists | runs not ok |")
    p("|---:|---:|---:|---:|---:|---:|")
    p("| %d | %d | %d | %d | %d | %d |" % (s("drops_api"), s("frpc_relogins"), s("relay_restarts"), s("hb_timeout_lines"), s("api_unreadable"),
                                         sum(1 for r in runs if r["status"] != "ok")))
    bad = [r for r in meas if r.get("valid") != "True"]
    if bad:
        p("\nRuns not valid:")
        for r in bad:
            p("- rep %s, %s, %s, %s: %s" % (r["rep"], r["mode"], r["layout"], r["kind"], r.get("invalid_reason") or r["status"]))
    p("\n### Runners\n")
    for rep, model, cpus, mem, kernel in sorted({(r["rep"], r["cpu_model"], r["cpus"], r["mem_total_mb"], r["kernel"]) for r in runs}):
        p("- rep %s: %s, %s CPUs, %s MB, Linux %s" % (rep, model, cpus, mem, kernel))


if __name__ == "__main__":
    if len(sys.argv) >= 4 and sys.argv[1] == "extract":
        extract(sys.argv[2], sys.argv[3])
    else:
        tables(sys.argv[2] if len(sys.argv) > 2 else os.path.join(os.path.dirname(os.path.abspath(__file__)), "results"))
