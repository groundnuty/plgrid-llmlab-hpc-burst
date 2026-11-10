#!/usr/bin/env python3
"""Spike S15: the README's tables, from the committed results.

    python3 summarize.py extract <raw dir> <rep dir>   raw files -> streams.csv, transfers.csv, links.csv, conns.csv
    python3 summarize.py tables [results dir]          the tables, as Markdown, from results/rep*/
    python3 summarize.py verify [results dir]          every value the tables take from runs.csv, recomputed from the committed files
    python3 summarize.py compress [results dir]        samples, host and streams CSVs to .csv.xz, the committed form

extract keeps, from each run's raw file, every token's arrival per stream request (streams.csv), every
transfer with its receiver-side steps (transfers.csv) and the link's per-connection bytes (links.csv),
so the tables recompute the gaps from the arrivals; runs.csv, samples.csv and host.csv are the
driver's.
"""
import csv
import glob
import json
import lzma
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
    srows, trows, lrows, crows = [], [], [], []
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
        for c in raw.get("conns") or []:
            for role, n in sorted(c["conns"].items()):
                crows.append({"run_id": rid, "t": "%.6f" % c["t"], "role": role, "established": n})
    for name, rows in (("streams.csv", srows), ("transfers.csv", trows), ("links.csv", lrows), ("conns.csv", crows)):
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
    print("%s: %d stream requests, %d transfers, %d link connections, %d connection samples" % (rep_dir, len(srows), len(trows), len(lrows), len(crows)))


# The large files are committed xz-compressed (compress); every reader takes either form.
COMPRESSED = ("samples.csv", "host.csv", "streams.csv", "conns.csv")


def open_csv(path):
    """A result CSV as text: path itself, or path.xz when only the compressed form exists."""
    if os.path.exists(path):
        return open(path, newline="")
    if os.path.exists(path + ".xz"):
        return lzma.open(path + ".xz", "rt", newline="")
    raise FileNotFoundError(path)


def compress(root):
    """Writes each repetition's large CSVs as .csv.xz (xz preset 6) and removes the plain ones,
    after checking that the compressed file reads back byte for byte."""
    for d in sorted(glob.glob(os.path.join(root, "rep*"))):
        for name in COMPRESSED:
            src = os.path.join(d, name)
            if not os.path.exists(src):
                continue
            with open(src, "rb") as fh:
                data = fh.read()
            with lzma.open(src + ".xz", "wb", preset=6) as out:
                out.write(data)
            with lzma.open(src + ".xz", "rb") as back:
                if back.read() != data:
                    raise RuntimeError("%s.xz does not read back as %s" % (src, src))
            os.remove(src)
            print("%s: %d -> %d bytes" % (src + ".xz", len(data), os.path.getsize(src + ".xz")))


def samples_by_run(d):
    """A repetition's committed samples, by run and role, in time order."""
    by = {}
    with open_csv(os.path.join(d, "samples.csv")) as fh:
        for x in csv.DictReader(fh):
            by.setdefault(x["run_id"], {}).setdefault(x["role"], []).append((float(x["t"]), int(x["ticks"])))
    return by


def cores_between(ss, a, b):
    """A role's cores between a and b (unix s), from its samples' summed ticks (None with fewer than 2)."""
    w = [x for x in ss if a <= x[0] <= b]
    if len(w) < 2 or w[-1][0] <= w[0][0]:
        return None
    return (w[-1][1] - w[0][1]) / 100 / (w[-1][0] - w[0][0])


def active_front_cores(by_role, t0, transfers):
    """The front's cores over the transfers' own spans only, the pauses between them left out: the
    front roles' ticks between the first and last sample inside each span, summed, over the summed
    time between those samples. Spans holding fewer than two samples (every 0.5 s) are skipped."""
    ticks, secs = 0, 0.0
    for tr in transfers:
        ws, we = analysis.transfer_window(tr)
        a, b = t0 + ws / 1e9, t0 + we / 1e9
        span = None
        for role in FRONT:
            w = [x for x in by_role.get(role, []) if a <= x[0] <= b]
            if len(w) < 2:
                continue
            ticks += w[-1][1] - w[0][1]
            span = max(span or 0, w[-1][0] - w[0][0])
        if span:
            secs += span
    return ticks / 100 / secs if secs else None


FRONT = ("frps", "frps2", "listener", "owner", "bridges")


def verify(root):
    """Recomputes from the committed files every value the tables take from runs.csv, and reports the
    largest difference or each mismatch: the CPU figures (samples.csv.xz), the transfers' rates, times,
    bursts and upload tails (transfers.csv), the validity and the gaps (streams.csv.xz and
    transfers.csv), the link's connection counts (links.csv) and the bulk frpc's established
    connections (conns.csv)."""
    runs, lds = loads(root)
    by_id = {r["run_id"]: r for r in runs}
    bad, checked = [], {"cpu": 0, "transfer": 0, "validity": 0, "gap": 0, "link": 0, "conns": 0}
    worst_cpu = 0.0

    def same(rid, key, got, want, tol=0.0):
        if want in ("", None) and got in ("", None):
            return
        if want in ("", None) or got in ("", None) or abs(float(got) - float(want)) > tol:
            bad.append("%s %s: recomputed %s, runs.csv %s" % (rid, key, got, want))

    for d in sorted(glob.glob(os.path.join(root, "rep*"))):
        sb = samples_by_run(d)
        links = {}
        with open_csv(os.path.join(d, "links.csv")) as fh:
            for c in csv.DictReader(fh):
                links.setdefault(c["run_id"], []).append(int(c["bytes_up"]) + int(c["bytes_down"]))
        conns = {}
        cp = os.path.join(d, "conns.csv")
        if os.path.exists(cp) or os.path.exists(cp + ".xz"):
            with open_csv(cp) as fh:
                for c in csv.DictReader(fh):
                    k = (c["run_id"], c["role"])
                    conns[k] = max(conns.get(k, 0), int(c["established"]))
        for rid in sorted({r for r in sb}):
            r = by_id.get(rid)
            if r is None:
                continue
            for role, ss in sb[rid].items():
                for label in ("ref", "bulk"):
                    want, a, b = r.get("cpu_%s_%s" % (label, role)), r.get(label + "_start_s"), r.get(label + "_end_s")
                    if want in ("", None) or not a or not b:
                        continue
                    got = cores_between(ss, float(a), float(b))
                    if got is not None:
                        worst_cpu = max(worst_cpu, abs(got - float(want)))
                        checked["cpu"] += 1
            L = lds.get(rid)
            if L is None:
                continue
            kind, size = r["kind"], 64 << 20
            m = analysis.transfer_metrics(L["transfers"], kind, size)
            for k, v in m.items():
                same(rid, k, v, r.get(k), tol=1.0 if "rate" in k or "bytes" in k else 0.001)
            checked["transfer"] += 1
            if r["layout"] != "cal":
                v = analysis.validity(L, kind, size)
                if str(v["valid"]) != r.get("valid"):
                    bad.append("%s valid: recomputed %s, runs.csv %s" % (rid, v["valid"], r.get("valid")))
                checked["validity"] += 1
                for k, val in analysis.gap_stats(L).items():
                    same(rid, k, val, r.get(k), tol=0.001)
                checked["gap"] += 1
            lk = links.get(rid, [])
            same(rid, "link_conns", len(lk), r.get("link_conns"))
            same(rid, "link_conns_over_1mib", sum(1 for x in lk if x > (1 << 20)), r.get("link_conns_over_1mib"))
            checked["link"] += 1
            for role in ("frpc", "frpc-bulk"):
                want = r.get("conns_max_" + role)
                if want not in ("", None):
                    same(rid, "conns_max_" + role, conns.get((rid, role)), want)
                    checked["conns"] += 1
    print("recomputed from the committed files: %s" % ", ".join("%d %s" % (n, k) for k, n in checked.items()))
    print("CPU: the largest difference from runs.csv is %.6f cores" % worst_cpu)
    print("mismatches: %s" % ("none" if not bad else "\n  " + "\n  ".join(bad[:30])))
    return 1 if bad else 0


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
        if os.path.exists(sp) or os.path.exists(sp + ".xz"):
            with open_csv(sp) as fh:
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
    p("How long after the client's last write the node read the upload's last byte: the part of an upload a front-side timing does not see (#273). The first column is the median over the repetitions of each run's median transfer (range in brackets); the second is every single transfer's, minimum to maximum.\n")
    p("| mode | layout | median of each run's median, ms | every transfer, min–max, ms |")
    p("|---|---|---:|---:|")
    for k in keys:
        if k[2] != "upload":
            continue
        lags = [(tr["recv_last_ns"] - tr["sent_ns"]) / 1e6 for r in g[k] if r["run_id"] in lds
                for tr in lds[r["run_id"]]["transfers"] if tr.get("sent_ns") and analysis.transfer_ok(tr, "upload", 64 << 20)]
        p("| %s | %s | %s | %s |" % (k[0], k[1], rng([num(r.get("upload_recv_last_after_sent_ms_median")) for r in g[k]], "{:.0f}"),
                                     "%.0f–%.0f" % (min(lags), max(lags)) if lags else "-"))
    p("\n### Connections and the front's CPU\n")
    p("The link's connections over the run (the emulator's per-connection counts), those that carried more than 1 MiB, and the bulk frpc's established connections at their most. Front CPU is frps (+ the second frps) + listener (+ owner + ssh stand-ins), in cores: over the reference window; over the transfers' phase, first transfer's start to last's end, which includes the 4 s pauses between them; and over the transfers' own spans only, from the samples (every 0.5 s) inside each span, which leaves the pauses out.\n")
    sbs = {os.path.basename(d)[3:]: samples_by_run(d) for d in sorted(glob.glob(os.path.join(root, "rep*")))}
    p("| mode | transfer | layout | link connections | over 1 MiB | bulk frpc connections | front cores, reference | front cores, transfers' phase (with pauses) | front cores, during transfers only | host busy, transfers' phase |")
    p("|---|---|---|---:|---:|---:|---:|---:|---:|---:|")
    for k in keys:
        rs = [r for r in g[k] if r["status"] == "ok"]
        col = lambda c, f: rng([num(r.get(c)) for r in rs], f)  # noqa: E731
        # A run's t0 (the transfers' offsets are from it) is its reference start less that start's offset.
        act = [active_front_cores(sbs.get(r["rep"], {}).get(r["run_id"], {}),
                                  num(r["ref_start_s"]) - lds[r["run_id"]]["reference_start_ns"] / 1e9, lds[r["run_id"]]["transfers"])
               for r in rs if r["run_id"] in lds and r.get("ref_start_s")]
        p("| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |" % (k[0], k[2], k[1], col("link_conns", "{:.0f}"), col("link_conns_over_1mib", "{:.0f}"),
                                                                 col("conns_max_frpc-bulk", "{:.0f}"), col("cpu_ref_front", "{:.3f}"),
                                                                 col("cpu_bulk_front", "{:.3f}"), rng(act, "{:.3f}"), col("host_busy_bulk", "{:.2f}")))
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
    here = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
    if len(sys.argv) >= 4 and sys.argv[1] == "extract":
        extract(sys.argv[2], sys.argv[3])
    elif len(sys.argv) >= 2 and sys.argv[1] == "verify":
        sys.exit(verify(sys.argv[2] if len(sys.argv) > 2 else here))
    elif len(sys.argv) >= 2 and sys.argv[1] == "compress":
        compress(sys.argv[2] if len(sys.argv) > 2 else here)
    else:
        tables(sys.argv[2] if len(sys.argv) > 2 else os.path.join(os.path.dirname(os.path.abspath(__file__)), "results"))
