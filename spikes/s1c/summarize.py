#!/usr/bin/env python3
"""Spike S1c: Markdown tables from results/{matrix,hol,heartbeat}.csv (the README's tables).

    python3 summarize.py [results_dir]
"""
import csv
import os
import re
import statistics
import sys
from collections import defaultdict

ARMS = ("direct", "tcpmux", "wss", "ssh")


def rows(path):
    if not os.path.exists(path):
        return []
    with open(path) as fh:
        return list(csv.DictReader(fh))


def f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def both(vals, fmt="%.0f"):
    """'p1 / p2' for the passes present."""
    return " / ".join(fmt % v if v is not None else "-" for v in vals) if vals else "-"


LOAD_OK = 4.0
FLAGGED = set()   # keys whose every attempt ended with the 1-minute load above LOAD_OK


def ok(r):
    """A usable attempt: load <= 4.0 at its start and end, and (head-of-line rows) a valid injection."""
    return (all(f(r.get(k)) is None or f(r.get(k)) <= LOAD_OK for k in ("load_start", "load_end"))
            and r.get("valid", "yes") in ("yes", ""))


def pick(rs, key):
    """key(row) -> pass -> row: the last usable attempt (ok()), else the last attempt (and the key is
    flagged). Rows are in measurement order; a re-run supersedes earlier rows."""
    tries = defaultdict(list)
    for r in rs:
        tries[(key(r), int(r["pass_"]))].append(r)
    by = defaultdict(dict)
    for (k, p), lst in tries.items():
        good = [r for r in lst if ok(r)]
        by[k][p] = (good or lst)[-1]
        if not good:
            FLAGGED.add((k, p))
    return by


def matrix(rs):
    by = pick(rs, lambda r: (r["arm"], r["cond"], int(r["n"])))   # (arm, cond, n) -> pass -> row
    passes = sorted({int(r["pass_"]) for r in rs})
    out = []
    for cond, title in (("clean", "No impairment"), ("lat20", "20 ms added round trip (10 ms each way)"),
                        ("bw", "Bandwidth cap 512,000 B/s per connection, both directions")):
        out.append("\n#### %s\n" % title)
        out.append("Values are pass 1 / pass 2. Added TTFT is against the direct arm in the same condition, "
                   "concurrency and pass. CPU is percent of one core; path = frpc + frps (+ Envoy for wss, + ssh "
                   "and sshd for ssh), averaged over the whole load run. Front CPU per 1k tok/s is frps (+ Envoy for wss, + sshd "
                   "for ssh) per 1,000 tokens/s, with CPU and tokens over the same interval (the whole run); total tok/s "
                   "is the steady 24 s window. RSS is the peak during the cell, shown for the clean cells only: the processes "
                   "stay up across cells, and Go keeps the memory it grew to, so later cells show the earlier peak.\n")
        out.append("| arm | streams | TTFT p50 ms | added p50 ms | added p95 ms | gap p99 ms | tok/s per stream p50 (p5) "
                   "| total tok/s | failed | path CPU % | front CPU % per 1k tok/s | frpc / frps RSS MB | TCP payload B/token |")
        out.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
        for arm in ARMS:
            for n in (1, 16, 64, 256):
                cell = by.get((arm, cond, n))
                if not cell:
                    continue
                base = by.get(("direct", cond, n), {})
                def g(key, p):
                    return f(cell[p][key]) if p in cell else None
                add50 = [g("ttft_p50", p) - f(base[p]["ttft_p50"]) if p in cell and p in base else None for p in passes]
                add95 = [g("ttft_p95", p) - f(base[p]["ttft_p95"]) if p in cell and p in base else None for p in passes]
                cpu = []
                for p in passes:
                    if p not in cell:
                        cpu.append(None)
                        continue
                    cpu.append(sum(f(cell[p].get("cpu_" + k)) or 0 for k in ("frpc", "frps", "envoy", "ssh", "sshd")))
                p1 = cell[passes[0]] if passes[0] in cell else list(cell.values())[0]
                rate = both([g("rate_p50", p) for p in passes], "%.1f") + " (%s)" % both([g("rate_p5", p) for p in passes], "%.1f")
                rss = "-" if arm == "direct" or cond != "clean" else "%s / %s" % (p1["rss_frpc"], p1["rss_frps"])
                fk = [g("front_cpu_per_ktok", p) for p in passes]
                out.append("| %s | %d | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |" % (
                    arm, n, both([g("ttft_p50", p) for p in passes]),
                    "-" if arm == "direct" else both(add50, "%+.0f"), "-" if arm == "direct" else both(add95, "%+.0f"),
                    both([g("gap_p99", p) for p in passes]), rate, both([g("tok_s", p) for p in passes]),
                    both([g("failed", p) for p in passes]), "-" if arm == "direct" else both(cpu, "%.0f"),
                    "-" if arm == "direct" else both(fk, "%.1f"), rss, p1["tcp_payload_B_tok"] or "-"))
    extra = [(c, t) for c, t in (("raw", "direct, no toxiproxy"), ("bw-shared", "direct through the shared-bottleneck shaper at 512,000 B/s"))]
    out.append("\n#### Direct-arm controls\n")
    out.append("| condition | streams | TTFT p50 ms | gap p99 ms | tok/s per stream p50 | total tok/s | failed |")
    out.append("|---|---:|---:|---:|---:|---:|---:|")
    for cond, title in extra:
        for n in (1, 16, 64, 256):
            cell = by.get(("direct", cond, n))
            if not cell:
                continue
            g = lambda k: [f(cell[p][k]) if p in cell else None for p in passes]
            out.append("| %s | %d | %s | %s | %s | %s | %s |" % (title, n, both(g("ttft_p50")), both(g("gap_p99")),
                       both(g("rate_p50"), "%.1f"), both(g("tok_s")), both(g("failed"))))
    return "\n".join(out)


def hol(rs):
    by = pick(rs, lambda r: (r["config"], r["variant"]))
    out = ["Values are run 1 / run 2 (/ more runs where listed). The stall is the largest lateness of a fast-stream "
           "token arriving from the bulk start to 5 s after the bulk ends.\n",
           "| path | variant | bulk bytes read | bulk B/s | bulk s | fast gap p99 before the bulk, ms "
           "| stall: fast lateness max, ms | fast streams complete | heartbeat timeouts frpc / frps |",
           "|---|---|---:|---:|---:|---:|---:|---:|---:|"]
    for (cfg, var), cell in by.items():
        passes = sorted(cell)   # the runs of this row (some rows have extra runs)
        g = lambda k: [f(cell[p][k]) for p in passes]
        gaps = both(g("before_gap_p99"))
        out.append("| %s | %s | %s | %s | %s | %s | %s | %s | %s |" % (
            cfg, var, both(g("bulk_read")), both(g("bulk_Bps")), both(g("bulk_s"), "%.1f"), gaps, both(g("stall_late_max")),
            " / ".join("%s/%s" % (cell[p]["fast_complete"], cell[p]["fast_n"]) for p in passes),
            " / ".join("%s-%s" % (cell[p]["frpc_hb_timeouts"], cell[p]["frps_hb_timeouts"] or "?") for p in passes)))
    return "\n".join(out)


def heartbeat(rs):
    by = defaultdict(list)
    for r in rs:
        by[(r["setting"], r["scenario"])].append(r)
    def mmm(vals):
        v = sorted(x for x in (f(a) for a in vals) if x is not None)
        if not v:
            return "-"
        return "%.1f / %.1f / %.1f" % (v[0], statistics.median(v), v[-1])
    out = ["| setting | scenario | reps | frps marks proxy gone, s (min / median / max) | first refused probe, s "
           "| probes hung meanwhile: median count, median wait s, longest wait s (cut at 15 s: runs) | API errors | censored "
           "| frps cause (rep 1) | frpc noticed, s | routing back after heal, s |",
           "|---|---|---:|---:|---:|---:|---:|---:|---|---:|---:|"]
    for (st, sc), lst in by.items():
        hung = [f(r["hung_med_s"]) for r in lst if f(r["hung_med_s"]) is not None]
        mx = [f(r["hung_max_s"]) for r in lst if f(r["hung_max_s"]) is not None]
        cut = sum(1 for r in lst if int(f(r.get("hung_timeouts")) or 0) > 0)
        out.append("| %s | %s | %d | %s | %s | %s | %s | %s | %s | %s | %s |" % (
            st, sc, len(lst), mmm(r["api_gone_s"] for r in lst), mmm(r["first_refused_s"] for r in lst),
            "%d, %.1f, %.1f (%d)" % (statistics.median([int(r["hung_n"]) for r in lst]), statistics.median(hung), max(mx), cut)
            if hung else "0",
            sum(int(f(r.get("api_errors")) or 0) for r in lst), sum(1 for r in lst if r.get("valid")),
            re.sub(r"\[[0-9a-f]{16}\] |\d{4}/\d\d/\d\d \d\d:\d\d:\d\d ", "", lst[0]["frps_cause"]).replace("|", "/")[:120],
            mmm(r["frpc_detect_s"] for r in lst), mmm(r["heal_probe_ok_s"] for r in lst)))
    return "\n".join(out)


def main():
    d = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
    print("### Capacity matrix\n" + matrix(rows(os.path.join(d, "matrix.csv"))))
    print("\n### Head-of-line\n" + hol(rows(os.path.join(d, "hol.csv"))))
    print("\n### Heartbeats\n" + heartbeat(rows(os.path.join(d, "heartbeat.csv"))))
    print("\nMeasured only above a 1-minute load of %.1f (every attempt): %s" % (
        LOAD_OK, ", ".join("%s pass %d" % ("/".join(map(str, k)), p) for k, p in sorted(FLAGGED)) or "none"))


if __name__ == "__main__":
    main()
