#!/usr/bin/env python3
"""Spike S14: the README's tables, from the committed results.

    python3 summarize.py extract <raw dir> <out dir>   runs' raw files -> tunnels.csv and hists.json
    python3 summarize.py tables [results dir]          the tables, as Markdown, from results/rep*/
    python3 summarize.py fits [results dir]            the cost fits, per repetition
    python3 summarize.py budget [results dir]          the token rate one front core carries under the budget
    python3 summarize.py refresh <raw dir> <rep dir>   window bounds, idle columns and samples again, from the raw files

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


def idle_front(r):
    vals = [num(r.get("idle_cpu_" + k)) for k in FRONT]
    vals = [v for v in vals if v is not None]
    return sum(vals) if vals else None


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
    p("\n### Idle cost\n")
    p("Every tunnel registered and no stream running, for 30 s before the load at 1 stream per tunnel: the tunnels' "
      "keepalives and heartbeats, the front's probe of each tunnel every 10 s and the controller's list read every 10 s. "
      "Median over the repetitions (range when they differ), in thousandths of a core.\n")
    p("| mode | tunnels | frps | listener | owner | stand-ins | front | front per tunnel | probes failed | tokens |")
    p("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for k in keys:
        if k[0] == "baseline" or k[2] != 1:
            continue
        ok = [r for r in g[k] if r["status"] == "ok" and r.get("idle_cpu_frps") not in (None, "")]
        if not ok:
            continue
        mc = lambda c: rng([1000 * num(r.get(c)) if num(r.get(c)) is not None else None for r in ok], "{:.1f}")  # noqa: E731
        front = [1000 * idle_front(r) for r in ok]
        p("| %s | %d | %s | %s | %s | %s | %s | %s | %d | %d |" % (
            k[0], k[1], mc("idle_cpu_frps"), mc("idle_cpu_listener"), mc("idle_cpu_owner"), mc("idle_cpu_bridges"),
            rng(front, "{:.1f}"), rng([f / k[1] for f in front], "{:.2f}"),
            sum(int(r.get("idle_probes_failed") or 0) for r in ok), sum(int(r.get("idle_tokens") or 0) for r in ok)))
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
            idle = [r for r in rs0 if r["mode"] == mode and r["status"] == "ok" and idle_front(r) is not None]
            if idle:
                # Idle: cores = b * tunnels + c, a one-variable least squares.
                xs = [int(r["tunnels"]) for r in idle]
                ys = [idle_front(r) for r in idle]
                n, mx, my = len(xs), sum(xs) / len(xs), sum(ys) / len(ys)
                sxx = sum((x - mx) ** 2 for x in xs)
                if sxx > 0:
                    b = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / sxx
                    print("%-11s %-8s %2d idle runs: idle front cores = %.5f per tunnel + %.4f" % (mode, label, n, b, my - b * mx))
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


# The per-front budget (README, Verdict): with q = tokens/s / 1000 and C front cores,
#     a*q + b*tunnels + c + R_front + R_ssh <= 0.7*C,
# where R_front = S_FRONT[mode] * a * q is a chosen budgeting floor for the front's own proxy: the
# listener's measured share of the stack in that mode (direct 0.39-0.44, host-dialed 0.20-0.24),
# rounded up, which in both modes is about the listener's own cost per token; and R_ssh = R_SSH * q in
# host-dialed mode is a provisional assumption for ssh's encryption, to be validated.
S_FRONT = {"direct": 0.45, "host-dialed": 0.25}
R_SSH = 0.045
R_SSH_HIGH = 0.14   # S1c's 16-stream sample, for the sensitivity


def per_rep_fits(root, mode):
    runs, _, _ = load_results(root)
    out = {}
    for rep in sorted({r["rep"] for r in runs}):
        rs = [r for r in runs if r["rep"] == rep and r["mode"] == mode and r["status"] == "ok" and r.get("valid") == "True"
              and r.get("host_bound") != "True"]
        f = fit([(int(r["tunnels"]), num(r["tokens_per_s"]) / 1000, front_cores(r)) for r in rs])
        if f:
            out[rep] = f
    return out


def admitted(b, a, c, tunnels, per_q, cores=1.0):
    """The token rate (tokens/s) at which the budget's left side reaches 70% of cores."""
    return 1000 * (0.7 * cores - c - b * tunnels) / per_q


def budget(root):
    """The token rate one front core carries under the budget, per runner, mode and tunnel count."""
    for mode in ("direct", "host-dialed"):
        fits = per_rep_fits(root, mode)
        ssh = R_SSH if mode == "host-dialed" else 0.0
        for tunnels in (100, 300):
            alone = [admitted(b, a, c, tunnels, a) for b, a, c in fits.values()]
            res = [admitted(b, a, c, tunnels, a * (1 + S_FRONT[mode]) + ssh) for b, a, c in fits.values()]
            line = "%-11s %3d tunnels: stack alone %5.0f-%5.0f; with R_front = %.2f*a*q%s: %5.0f-%5.0f tok/s per core" % (
                mode, tunnels, min(alone), max(alone), S_FRONT[mode], " and R_ssh = %.3f*q" % ssh if ssh else "", min(res), max(res))
            if ssh:
                hi = [admitted(b, a, c, tunnels, a * (1 + S_FRONT[mode]) + R_SSH_HIGH) for b, a, c in fits.values()]
                line += "; with R_ssh = %.2f*q: %5.0f-%5.0f" % (R_SSH_HIGH, min(hi), max(hi))
            print(line)


def refresh(raw_dir, rep_dir):
    """Rebuilds a repetition's window bounds, idle columns, samples.csv and host.csv from its raw files,
    with the driver's own functions, keeping every other value of runs.csv. Two fixes made this
    necessary. Before them, the probes-only client could exit up to a probe interval before its window's
    end, with the load starting then, so the idle phase really ended at the client's end (at 1 tunnel,
    3-7 s early) and idle_end_s named the wrong end. And times were kept to the millisecond, while the
    idle phase starts within a millisecond of a sample, so a recompute could take in one sample the
    driver did not."""
    import types
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import s14
    runs_csv = os.path.join(rep_dir, "runs.csv")
    with open(runs_csv) as fh:
        rows = list(csv.DictReader(fh))
        header = list(rows[0])
    by = {r["run_id"]: r for r in rows}
    raws = []
    for path in glob.glob(os.path.join(raw_dir, "*.json")):
        with open(path) as fh:
            raws.append(json.load(fh))
    raws.sort(key=lambda raw: raw["samples"][0]["t"] if raw.get("samples") else 0)
    for name in ("samples.csv", "host.csv"):
        if os.path.exists(os.path.join(rep_dir, name)):
            os.remove(os.path.join(rep_dir, name))
    idle_changed = 0
    for raw in raws:
        r = by.get(raw["row"]["run_id"])
        if r is None:
            continue
        s14.write_samples(rep_dir, r["run_id"], raw.get("samples") or [])
        load = raw.get("load")
        if load:
            r["window_start_s"] = round(load["window_start_unix_ns"] / 1e9, 6)
            r["window_end_s"] = round(load["window_end_unix_ns"] / 1e9, 6)
        if raw.get("idle"):
            new = dict(r)
            s14.summarize_idle(new, raw["idle"], types.SimpleNamespace(samples=raw["samples"]))
            if any(str(new[k]) != str(r.get(k, "")) for k in new if k.startswith("idle_cpu_")):
                idle_changed += 1
            r.update({k: v for k, v in new.items() if k.startswith("idle_")})
    with open(runs_csv + ".tmp", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=header)
        w.writeheader()
        w.writerows(rows)
    os.replace(runs_csv + ".tmp", runs_csv)
    print("%s: %d runs, idle CPU changed in %d" % (rep_dir, len(raws), idle_changed))


if __name__ == "__main__":
    if len(sys.argv) >= 2 and sys.argv[1] == "budget":
        budget(sys.argv[2] if len(sys.argv) > 2 else os.path.join(os.path.dirname(os.path.abspath(__file__)), "results"))
        sys.exit(0)
    if len(sys.argv) >= 4 and sys.argv[1] == "refresh":
        refresh(sys.argv[2], sys.argv[3])
        sys.exit(0)
    if len(sys.argv) >= 4 and sys.argv[1] == "extract":
        extract(sys.argv[2], sys.argv[3])
    elif len(sys.argv) >= 2 and sys.argv[1] == "fits":
        fits(sys.argv[2] if len(sys.argv) > 2 else os.path.join(os.path.dirname(os.path.abspath(__file__)), "results"))
    else:
        tables(sys.argv[2] if len(sys.argv) > 2 else os.path.join(os.path.dirname(os.path.abspath(__file__)), "results"))
