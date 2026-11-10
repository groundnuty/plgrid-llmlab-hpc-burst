#!/usr/bin/env python3
"""Spike S14: the README's tables, from the committed results.

    python3 summarize.py extract <raw dir> <out dir>   runs' raw files -> tunnels.csv and hists.json
    python3 summarize.py tables [results dir]          the tables, as Markdown, from results/rep*/

extract keeps, from each run's raw file, the evidence per tunnel (requests, tokens, the emptiest
second, the longest silence, probes) and the latency histograms at 1 ms, so the tables can pool the
repetitions without the raw files, which are too large to commit. tables reads only runs.csv,
tunnels.csv and hists.json.
"""
import csv
import glob
import json
import os
import statistics
import sys

MODES = ("baseline", "direct", "host-dialed")
FRONT = ("frps", "listener", "owner", "bridges")


def extract(raw_dir, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    rows, hists = [], {}
    for path in sorted(glob.glob(os.path.join(raw_dir, "*.json"))):
        with open(path) as fh:
            raw = json.load(fh)
        run_id, load = raw["row"]["run_id"], raw.get("load")
        if not load:
            continue
        hists[run_id] = {"ttft": load["ttft_hist_ms"], "gap": load["gap_hist_ms"]}
        for t in load["per_tunnel"]:
            rows.append({"run_id": run_id, "tunnel": t["index"], "requests": t["requests"], "completed": t["completed"],
                         "failed": t["failed"], "tokens": t["tokens"], "min_bucket": min(t["buckets"]) if t["buckets"] else 0,
                         "longest_silence_ms": t["longest_silence_ms"], "gap_p99_ms": t["gap_p99_ms"],
                         "probes_ok": t["probes_ok"], "probes_failed": t["probes_failed"]})
    with open(os.path.join(out_dir, "tunnels.csv"), "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    with open(os.path.join(out_dir, "hists.json"), "w") as fh:
        json.dump(hists, fh, sort_keys=True)
    print("%d runs, %d tunnel rows -> %s" % (len(hists), len(rows), out_dir))


def num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def load_results(root):
    runs, hists, tunnels = [], {}, {}
    for d in sorted(glob.glob(os.path.join(root, "rep*"))):
        with open(os.path.join(d, "runs.csv")) as fh:
            runs += list(csv.DictReader(fh))
        hp = os.path.join(d, "hists.json")
        if os.path.exists(hp):
            with open(hp) as fh:
                hists.update(json.load(fh))
        tp = os.path.join(d, "tunnels.csv")
        if os.path.exists(tp):
            with open(tp) as fh:
                for r in csv.DictReader(fh):
                    tunnels.setdefault(r["run_id"], []).append(r)
    return runs, hists, tunnels


def pooled_pct(hs, p):
    """The p-th percentile (nearest rank) of 1 ms histograms pooled, as the bin's lower edge + 0.5."""
    merged = {}
    for h in hs:
        for k, v in h.items():
            merged[int(k)] = merged.get(int(k), 0) + v
    n = sum(merged.values())
    if n == 0:
        return None
    rank, seen = max(1, -(-p * n // 100)), 0
    for k in sorted(merged):
        seen += merged[k]
        if seen >= rank:
            return k + 0.5
    return None


def med(xs):
    xs = [x for x in xs if x is not None]
    return statistics.median(xs) if xs else None


def rng(xs, fmt):
    xs = [x for x in xs if x is not None]
    if not xs:
        return "-"
    lo, mid, hi = min(xs), statistics.median(xs), max(xs)
    if fmt.format(lo) == fmt.format(hi):
        return fmt.format(mid)
    return (fmt + " ({}–{})").format(mid, fmt.format(lo), fmt.format(hi))


def groups(runs):
    g = {}
    for r in runs:
        g.setdefault((r["mode"], int(r["tunnels"]), int(r["streams"])), []).append(r)
    return g


def front_cores(r):
    vals = [num(r.get("cpu_" + k)) for k in FRONT]
    vals = [v for v in vals if v is not None]
    return sum(vals) if vals else None


def tables(root):
    runs, hists, tunnels = load_results(root)
    g = groups(runs)
    keys = sorted(g, key=lambda k: (k[2], k[1], MODES.index(k[0])))
    out = []
    p = out.append
    p("### Validity and load\n")
    p("Per configuration: the repetitions run, those valid (every tunnel received a token in every second of the 30 s "
      "window), whether any ran on a host-bound runner (all CPUs at least 85% busy), the tokens per second, and per tunnel "
      "the requests started in the window and the tokens received (minimum–maximum over all tunnels of all repetitions).\n")
    p("| mode | tunnels | streams/tunnel | runs | valid | host-bound | tok/s | requests per tunnel | tokens per tunnel | emptiest second, tokens | longest silence, ms | failed requests | probes failed |")
    p("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for k in keys:
        rs = g[k]
        trs = [t for r in rs for t in tunnels.get(r["run_id"], [])]
        ok = [r for r in rs if r["status"] == "ok"]
        valid = sum(1 for r in ok if r.get("valid") == "True")
        bound = sum(1 for r in ok if r.get("host_bound") == "True")
        def mm(col, cast=int):
            v = [cast(float(t[col])) for t in trs]
            return "%s–%s" % (min(v), max(v)) if v else "-"
        p("| %s | %d | %d | %d | %d | %d | %s | %s | %s | %s | %s | %s | %s |" % (
            k[0], k[1], k[2], len(rs), valid, bound, rng([num(r.get("tokens_per_s")) for r in ok], "{:.0f}"),
            mm("requests"), mm("tokens"), mm("min_bucket"), mm("longest_silence_ms", lambda x: round(x)),
            sum(int(r.get("failed") or 0) for r in ok), sum(int(r.get("probes_failed") or 0) for r in ok)))
    p("\n### The front host's cost\n")
    p("Median over the repetitions (range in brackets when they differ). CPU is cores averaged over the window; "
      "front = frps + listener (+ owner + the ssh stand-ins in host-dialed mode). RSS and descriptors are the window's "
      "peak, summed over a role's processes (2 per tunnel for the stand-ins).\n")
    p("| mode | tunnels | streams | frps cores | listener cores | owner cores | stand-ins cores | front cores | front % core per 1k tok/s | frps RSS MB | listener RSS MB | owner RSS MB | stand-ins RSS MB | frps fds | listener fds | owner fds | host busy |")
    p("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for k in keys:
        if k[0] == "baseline":
            continue
        ok = [r for r in g[k] if r["status"] == "ok"]
        fc = [front_cores(r) for r in ok]
        per1k = [100 * c / (num(r["tokens_per_s"]) / 1000) for c, r in zip(fc, ok) if c is not None and num(r["tokens_per_s"])]
        col = lambda c, f: rng([num(r.get(c)) for r in ok], f)  # noqa: E731
        p("| %s | %d | %d | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |" % (
            k[0], k[1], k[2], col("cpu_frps", "{:.3f}"), col("cpu_listener", "{:.3f}"), col("cpu_owner", "{:.3f}"),
            col("cpu_bridges", "{:.3f}"), rng(fc, "{:.3f}"), rng(per1k, "{:.1f}"), col("rss_mb_frps", "{:.0f}"),
            col("rss_mb_listener", "{:.0f}"), col("rss_mb_owner", "{:.0f}"), col("rss_mb_bridges", "{:.0f}"),
            col("fds_frps", "{:.0f}"), col("fds_listener", "{:.0f}"), col("fds_owner", "{:.0f}"), col("host_busy", "{:.2f}")))
    p("\n### Latency\n")
    p("Pooled at 1 ms resolution (each value is a 1 ms bin's middle) over the repetitions whose host was not CPU-bound; "
      "where every repetition was host-bound, over all of them, marked \"(host-bound)\": those rows measure the runner. "
      "TTFT runs from before the client's connect, CONNECT included, to the first content token; the engine double waits "
      "50 ms before each token. The baseline dials each engine's socket directly, on the same host under the same load.\n")
    p("| tunnels | streams | mode | reps pooled | TTFT p50 ms | TTFT p99 ms | gap p50 ms | gap p99 ms | probe p99 ms |")
    p("|---:|---:|---|---|---:|---:|---:|---:|---:|")
    for k in keys:
        ok = [r for r in g[k] if r["status"] == "ok"]
        free = [r for r in ok if r.get("host_bound") != "True"]
        pooled = ",".join(r["rep"] for r in (free or ok)) + ("" if free else " (host-bound)")
        ok = free or ok
        th = [hists[r["run_id"]]["ttft"] for r in ok if r["run_id"] in hists]
        gh = [hists[r["run_id"]]["gap"] for r in ok if r["run_id"] in hists]
        f = lambda x: "-" if x is None else "%.1f" % x  # noqa: E731
        p("| %d | %d | %s | %s | %s | %s | %s | %s | %s |" % (k[1], k[2], k[0], pooled, f(pooled_pct(th, 50)), f(pooled_pct(th, 99)),
                                                      f(pooled_pct(gh, 50)), f(pooled_pct(gh, 99)),
                                                      rng([num(r.get("probe_p99_ms")) for r in ok], "{:.0f}")))
    p("\n### Heartbeat loss\n")
    p("Summed over the repetitions. A drop is a tunnel frps's proxy list showed not online, or registered again, after "
      "readiness (read every 10 s); a re-login is an frpc login beyond its first; a relay restart a relay session beyond "
      "its first (the owner's count), and a job re-session the same seen by the job side.\n")
    p("| mode | tunnels | streams | time to all online, s | drops | frpc re-logins | relay restarts | job re-sessions | heartbeat-timeout lines | unreadable lists |")
    p("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for k in keys:
        if k[0] == "baseline":
            continue
        ok = [r for r in g[k] if r["status"] == "ok"]
        s = lambda c: sum(int(float(r[c])) for r in ok if r.get(c) not in (None, ""))  # noqa: E731
        hd = k[0] == "host-dialed"
        p("| %s | %d | %d | %s | %d | %d | %s | %s | %d | %d |" % (
            k[0], k[1], k[2], rng([num(r.get("all_online_s")) for r in ok], "{:.1f}"), s("drops_api"), s("frpc_relogins"),
            s("relay_restarts") if hd else "-", s("job_resessions") if hd else "-", s("hb_timeout_lines"), s("api_unreadable")))
    fails = [r for r in runs if r["status"] != "ok"]
    if fails:
        p("\n### Runs that did not finish\n")
        for r in fails:
            p("- rep %s, %s, %s tunnels x %s: %s" % (r["rep"], r["mode"], r["tunnels"], r["streams"], r["status"]))
    p("\n### Runners\n")
    seen = sorted({(r["rep"], r["cpu_model"], r["cpus"], r["mem_total_mb"], r["kernel"]) for r in runs})
    for rep, model, cpus, mem, kernel in seen:
        p("- rep %s: %s, %s CPUs, %s MB, Linux %s" % (rep, model, cpus, mem, kernel))
    print("\n".join(out))


def fit(points):
    """Least squares y = a*x1 + b*x2 + c over (x1, x2, y) points; returns (a, b, c)."""
    n = len(points)
    if n < 3:
        return None
    sx1 = sum(p[0] for p in points); sx2 = sum(p[1] for p in points); sy = sum(p[2] for p in points)  # noqa: E702
    m = [[sum(p[0] * p[0] for p in points), sum(p[0] * p[1] for p in points), sx1],
         [sum(p[0] * p[1] for p in points), sum(p[1] * p[1] for p in points), sx2],
         [sx1, sx2, n]]
    v = [sum(p[0] * p[2] for p in points), sum(p[1] * p[2] for p in points), sy]
    # Gaussian elimination on the 3x3 normal equations.
    for i in range(3):
        piv = max(range(i, 3), key=lambda r: abs(m[r][i]))
        m[i], m[piv], v[i], v[piv] = m[piv], m[i], v[piv], v[i]
        if abs(m[i][i]) < 1e-12:
            return None
        for r in range(i + 1, 3):
            f = m[r][i] / m[i][i]
            m[r] = [a - f * b for a, b in zip(m[r], m[i])]
            v[r] -= f * v[i]
    x = [0.0, 0.0, 0.0]
    for i in (2, 1, 0):
        x[i] = (v[i] - sum(m[i][j] * x[j] for j in range(i + 1, 3))) / m[i][i]
    return tuple(x)


def fits(root):
    """Front cost per tunnel and per 1,000 tokens/s, fitted over the valid runs that were not host-bound,
    for all repetitions and for each alone, with the token rate one front core carries at 70% busy."""
    runs, _, _ = load_results(root)
    reps = sorted({r["rep"] for r in runs})
    for mode in ("direct", "host-dialed"):
        for label, rs0 in [("all reps", runs)] + [("rep " + x, [r for r in runs if r["rep"] == x]) for x in reps]:
            rs = [r for r in rs0 if r["mode"] == mode and r["status"] == "ok" and r.get("valid") == "True"
                  and r.get("host_bound") != "True"]
            f = fit([(int(r["tunnels"]), num(r["tokens_per_s"]) / 1000, front_cores(r)) for r in rs])
            if not f:
                continue
            per_t, per_k, base = f
            core = lambda t: 1000 * (0.7 - base - per_t * t) / per_k  # noqa: E731
            print("%-11s %-8s %2d runs: front cores = %.5f per tunnel + %.4f per 1k tok/s + %.4f; "
                  "one core at 70%%: %.0f tok/s at 100 tunnels, %.0f at 300" % (mode, label, len(rs), per_t, per_k, base,
                                                                              core(100), core(300)))
            if label != "all reps":
                continue
            rss = fit([(int(r["tunnels"]), int(r["tunnels"]) * int(r["streams"]) / 1000,
                        sum(num(r.get("rss_mb_" + k)) or 0 for k in FRONT)) for r in rs])
            fds = fit([(int(r["tunnels"]), int(r["tunnels"]) * int(r["streams"]) / 1000,
                        sum(num(r.get("fds_" + k)) or 0 for k in FRONT)) for r in rs])
            if rss:
                print("%-11s %-8s front RSS MB = %.3f per tunnel + %.2f per 1k streams + %.1f" % ((mode, label) + rss))
            if fds:
                print("%-11s %-8s front fds = %.2f per tunnel + %.1f per 1k streams + %.1f" % ((mode, label) + fds))


if __name__ == "__main__":
    if len(sys.argv) >= 4 and sys.argv[1] == "extract":
        extract(sys.argv[2], sys.argv[3])
    elif len(sys.argv) >= 2 and sys.argv[1] == "fits":
        fits(sys.argv[2] if len(sys.argv) > 2 else os.path.join(os.path.dirname(os.path.abspath(__file__)), "results"))
    else:
        tables(sys.argv[2] if len(sys.argv) > 2 else os.path.join(os.path.dirname(os.path.abspath(__file__)), "results"))
