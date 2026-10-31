#!/usr/bin/env python3
"""Summarise one spike S2b job's results: one row per arm.

usage: analyze.py RESULTS_DIR [--json OUT.json] [--md OUT.md]

RESULTS_DIR holds arms/<name>/ (meta.json, client.jsonl, before-w*.prom, after-w*.prom, samples.tsv,
ps.txt) and placement-<regime>-w<i>.tsv, as job.sh writes them. Standard library only.

Hit rate (the pass criterion) comes from the workers' own counters: the sum over workers of the
growth of vllm:prefix_cache_hits_total divided by that of vllm:prefix_cache_queries_total during the
arm. Placement comes from the workers' placement logs, never from the router.
"""
import glob
import gzip
import json
import math
import os
import re
import sys
from collections import Counter, defaultdict

def opened(path):
    """Open path, or path + '.gz' (collect.sh gzips the larger files), as text; None if neither exists."""
    if os.path.exists(path):
        return open(path)
    if os.path.exists(path + ".gz"):
        return gzip.open(path + ".gz", "rt")
    return None


PROM = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+(\S+)")


def prom_sum(text, name):
    """Sum of every sample of metric `name` (all label sets). None if the metric is absent."""
    total, seen = 0.0, False
    for line in text.splitlines():
        m = PROM.match(line)
        if m and m.group(1) == name:
            total += float(m.group(3))
            seen = True
    return total if seen else None


def delta(arm_dir, name, nwork):
    """Per-worker growth of a counter between before-w<i>.prom and after-w<i>.prom."""
    out = []
    for i in range(nwork):
        try:
            b = prom_sum(open(os.path.join(arm_dir, f"before-w{i}.prom")).read(), name)
            a = prom_sum(open(os.path.join(arm_dir, f"after-w{i}.prom")).read(), name)
        except FileNotFoundError:
            return None
        if a is None:
            return None
        out.append(a - (b or 0.0))
    return out


def pct(values, q):
    """Nearest-rank percentile: the smallest value with at least q of the values at or below it."""
    if not values:
        return None
    v = sorted(values)
    return v[max(0, math.ceil(q * len(v)) - 1)]


def client_stats(records):
    ok = [r for r in records if not r.get("err")]
    ttft = [r["ttft_ms"] for r in ok]
    with_cache = [r for r in ok if r.get("cached_tokens", -1) >= 0]
    prompt = sum(r["prompt_tokens"] for r in with_cache)
    return {
        "requests_ok": len(ok),
        "failed_attempts": len(records) - len(ok),
        "ttft_p50_ms": pct(ttft, 0.50),
        "ttft_p95_ms": pct(ttft, 0.95),
        "latency_p50_ms": pct([r["latency_ms"] for r in ok], 0.50),
        "client_hit_rate": (sum(r["cached_tokens"] for r in with_cache) / prompt) if prompt else None,
        "prompt_tokens": sum(r["prompt_tokens"] for r in ok),
    }


def reuse_bound(records, block=16):
    """An ESTIMATE of the share of prompt tokens that a perfect router with an unbounded cache could serve
    from cache. It is not a theoretical ceiling.

    For turn t >= 1 of a session: the previous turn's prompt plus its reply, less the last sampled token
    (its KV is only computed when the next step feeds it back), at most this prompt.
    For turn 0: the agent type's shared system prompt, for every session but the first of its type. Its
    length is inferred from the run itself: the largest cached-token count any turn-0 request of that type
    reported. That depends on the router and cache under test (a type that never hit would count as 0), so
    the estimate is not independent of the arms it is compared with. A ceiling would need the tokenised
    histories and the materialised KV, which the client does not record.
    Everything is rounded down to whole cache blocks; re-tokenised replies can differ slightly."""
    ok = sorted((r for r in records if not r.get("err")), key=lambda r: r["start_unix"])
    by_sess = defaultdict(dict)
    for r in ok:
        by_sess[r["session"]][r["turn"]] = r
    sys_tok = defaultdict(int)
    for r in ok:
        if r["turn"] == 0:
            sys_tok[r["agent"]] = max(sys_tok[r["agent"]], r.get("cached_tokens", 0) or 0)
    seen_agent, reusable, total = set(), 0, 0
    for r in ok:
        total += r["prompt_tokens"]
        if r["turn"] == 0:
            if r["agent"] in seen_agent:
                reusable += min(sys_tok[r["agent"]], r["prompt_tokens"]) // block * block
            seen_agent.add(r["agent"])
        else:
            prev = by_sess[r["session"]].get(r["turn"] - 1)
            if prev:
                reusable += min(prev["prompt_tokens"] + prev["completion_tokens"] - 1, r["prompt_tokens"]) // block * block
    return reusable / total if total else None


def placement(rows, nwork):
    """rows: (worker, session, turn, header) of one run. Returns spread and stickiness figures."""
    where = {}
    headers = 0
    for w, s, t, h in rows:
        where[(s, t)] = w  # a retried turn keeps its last placement
        headers += h != "-"
    if not where:
        return None
    sessions = defaultdict(dict)
    for (s, t), w in where.items():
        sessions[s][t] = w
    first = Counter(turns[min(turns)] for turns in sessions.values())
    reqs = Counter(where.values())
    same = moves = 0
    for turns in sessions.values():
        for t in sorted(turns):
            if t - 1 in turns:
                same += turns[t] == turns[t - 1]
                moves += 1
    per_worker = [reqs.get(i, 0) for i in range(nwork)]
    mean = sum(per_worker) / nwork
    return {
        "sessions_per_worker": [first.get(i, 0) for i in range(nwork)],
        "requests_per_worker": per_worker,
        "request_imbalance_max_over_mean": max(per_worker) / mean if mean else None,
        "sticky_sessions": sum(len(set(t.values())) == 1 for t in sessions.values()) / len(sessions),
        "mean_workers_per_session": sum(len(set(t.values())) for t in sessions.values()) / len(sessions),
        "same_worker_as_previous_turn": same / moves if moves else None,
        "header_seen_by_worker": headers / len(rows),
    }


def load_placement(results, regime, nwork):
    rows = defaultdict(list)
    for i in range(nwork):
        f_in = opened(os.path.join(results, f"placement-{regime}-w{i}.tsv"))
        if f_in:
            for line in f_in:
                f = line.rstrip("\n").split("\t")
                if len(f) == 6:
                    rows[f[2]].append((int(f[1]), int(f[3]), int(f[4]), f[5]))
    return rows


def samples(arm_dir):
    """Maxima over the 2 s samples of each worker. They are sampled maxima: a spike shorter than the
    sampling interval can fall between two samples, so they bound what was seen, not what happened."""
    kv, waiting = defaultdict(float), defaultdict(float)
    f_in = opened(os.path.join(arm_dir, "samples.tsv"))
    if f_in is None:
        return {}
    for line in f_in:
        f = line.rstrip("\n").split("\t")
        if len(f) != 4:
            continue
        w, name, v = int(f[1]), f[2].split("{")[0], float(f[3])
        if name == "vllm:kv_cache_usage_perc":
            kv[w] = max(kv[w], v)
        elif name == "vllm:num_requests_waiting":
            waiting[w] = max(waiting[w], v)
    return {"kv_usage_max": [round(kv[w], 3) for w in sorted(kv)], "waiting_max": [int(waiting[w]) for w in sorted(waiting)]}


def analyse(results):
    rows = []
    for meta_path in sorted(glob.glob(os.path.join(results, "arms", "*", "meta.json"))):
        d = os.path.dirname(meta_path)
        meta = json.load(open(meta_path))
        n = meta["nwork"]
        f_in = opened(os.path.join(d, "client.jsonl"))
        recs = [json.loads(x) for x in f_in] if f_in else []
        hits = delta(d, "vllm:prefix_cache_hits_total", n)
        queries = delta(d, "vllm:prefix_cache_queries_total", n)
        pre = delta(d, "vllm:num_preemptions_total", n)
        row = dict(meta)
        row["worker_hit_rate"] = (sum(hits) / sum(queries)) if hits and queries and sum(queries) else None
        row["worker_hit_rate_per_worker"] = [round(h / q, 3) if q else None for h, q in zip(hits, queries)] if hits and queries else None
        row["preemptions"] = int(sum(pre)) if pre else None
        row.update(client_stats(recs))
        row["reuse_bound"] = reuse_bound(recs)
        row["placement"] = placement(load_placement(results, meta["regime"], n).get(meta["name"], []), n)
        row["samples"] = samples(d)
        row["wall_s"] = round(meta["end_unix"] - meta["start_unix"], 1)
        rows.append(row)
    order = {"rr": 0, "vr-hash": 1, "vr-cache": 2, "llmd-sess": 3, "llmd-prefix": 4}
    rows.sort(key=lambda r: (r["regime"], r["rep"], order.get(r["arm"], 9)))
    return rows


def fmt(v, spec):
    return "n/a" if v is None else format(v, spec)


def markdown(rows):
    out = ["| Run | Router | Hit rate (workers) | Hit rate (client) | Reuse bound (estimate) | TTFT p50 / p95 ms | Sessions per worker | Sticky sessions | Requests max/mean | Preemptions | OK / failed | Wall s |",
           "|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for r in rows:
        p = r["placement"] or {}
        out.append("| {} | {} | {} | {} | {} | {} / {} | {} | {} | {} | {} | {} / {} | {} |".format(
            r["name"], r["arm"], fmt(r["worker_hit_rate"], ".3f"), fmt(r["client_hit_rate"], ".3f"), fmt(r["reuse_bound"], ".3f"),
            fmt(r["ttft_p50_ms"], ".0f"), fmt(r["ttft_p95_ms"], ".0f"), p.get("sessions_per_worker", "n/a"),
            fmt(p.get("sticky_sessions"), ".2f"), fmt(p.get("request_imbalance_max_over_mean"), ".2f"),
            fmt(r["preemptions"], "d"), r["requests_ok"], r["failed_attempts"], r["wall_s"]))
    return "\n".join(out)


def main(argv):
    if len(argv) < 2:
        sys.exit(__doc__)
    rows = analyse(argv[1])
    if "--json" in argv:
        json.dump(rows, open(argv[argv.index("--json") + 1], "w"), indent=1)
    md = markdown(rows)
    if "--md" in argv:
        open(argv[argv.index("--md") + 1], "w").write(md + "\n")
    print(md)


if __name__ == "__main__":
    main(sys.argv)
