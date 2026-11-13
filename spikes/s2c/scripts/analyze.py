#!/usr/bin/env python3
"""Summarise one spike S2c job's results, and check one arm's router counters. Standard library only.

usage: analyze.py RESULTS_DIR [--json OUT.json] [--md OUT.md]
       analyze.py --check-arm ARMDIR

Descends from spike S2b's analyze.py. RESULTS_DIR holds arms/<name>/ (meta.json, client.jsonl,
before-w*.prom, after-w*.prom, and the router's own counters) and placement-<regime>-w<i>.tsv, as job.sh
writes them (the contract is in spikes/s2c/README.md).

Summary mode prints one row per arm (markdown), writes the rows as JSON when asked, and applies the issue's
pass rule to every arm but the baseline, against the rr arm of the SAME regime and rep:
  hit rate above rr's, no worker above 40% of the arm's requests, TTFT p95 no worse than rr's.
A part that cannot be evaluated (a missing counter) is "unknown", and a verdict is "pass" only when all three
parts passed. The summary also runs the arm checks and lists each failing one.

Hit rate (the pass criterion) comes from the workers' own counters: the sum over workers of the growth of
vllm:prefix_cache_hits_total over that of vllm:prefix_cache_queries_total during the arm. Placement comes
from the workers' placement logs (start, end, worker, run, session, turn, header), never from the router.
Requests per worker count every logged request, retries included, as the worker served them. Sessions, and
stickiness, keep S2b's rule: a retried turn keeps its last placement.

--check-arm runs the five checks on one arm, from files only, and prints one line each, "ok <check>" or
"FAIL <check>: <reason>":
  router-count-per-worker   the router's count per worker equals the worker's own placement count for the arm
  router-total-equals-client  the router's counts sum to the number of client records (attempts)
  policy-label              vllm-router arms: policy decisions grew only under the arm's policy label
  envoy-config              Envoy arms: run.json hashes to start.json's config_sha256, its hash_balance_factor
                            equals meta's bound (absent for "none"), start.json's diff lists at most that path
  other-router-silent       the other router family's counters, whichever files exist, did not grow
Exit 0 all ok, 1 any FAIL, 2 unreadable input (a missing or malformed file is never a pass; the reason goes
to stderr). Summary mode exits 2 when any arm's input was unreadable, else 1 when any arm's check failed.
"""
import argparse
import glob
import gzip
import hashlib
import json
import math
import os
import re
import sys
import zlib
from collections import Counter, defaultdict

ENVOY_CLUSTER = "workers"
FACTOR_PATH = "static_resources.clusters[0].common_lb_config.consistent_hashing_lb_config.hash_balance_factor"
POLICY_LABELS = ("round_robin", "consistent_hash")
# The 40% limit of the pass rule as an exact fraction, so that exactly 40% passes and no float decides it.
SHARE_LIMIT_NUM, SHARE_LIMIT_DEN = 2, 5
CHECKS = ("router-count-per-worker", "router-total-equals-client", "policy-label", "envoy-config",
          "other-router-silent")
ARM_ORDER = {"rr": 0, "vr-hash": 1, "envoy-bound": 2, "envoy-nobound": 3}


class Unreadable(Exception):
    """An input file is missing or malformed. Never a pass: callers exit 2."""


# ---------------------------------------------------------------- files

def read_bytes(path):
    """Bytes of path, or of path + '.gz' (a collector may gzip the larger files). Unreadable if neither reads."""
    for p, opener in ((path, lambda q: open(q, "rb")), (path + ".gz", lambda q: gzip.open(q, "rb"))):
        if os.path.exists(p):
            try:
                with opener(p) as f:
                    return f.read()
            except (OSError, EOFError, zlib.error) as e:
                raise Unreadable(f"{p}: {e}")
    raise Unreadable(f"{path}: missing")


def read_text(path):
    raw = read_bytes(path)
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError as e:
        raise Unreadable(f"{path}: not UTF-8 ({e})")


def exists(path):
    return os.path.exists(path) or os.path.exists(path + ".gz")


def read_json(path):
    try:
        return json.loads(read_text(path))
    except ValueError as e:
        raise Unreadable(f"{path}: not JSON ({e})")


# ---------------------------------------------------------------- Prometheus text

SAMPLE = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(.*)\})?\s+(\S+)")
LABEL = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"')


def prom_samples(text, name, path="<metrics>"):
    """[(labels dict, value)] of every sample of metric `name`, labels in any order."""
    out = []
    for line in text.splitlines():
        m = SAMPLE.match(line)
        if m and m.group(1) == name:
            try:
                value = float(m.group(3))
            except ValueError:
                raise Unreadable(f"{path}: bad value in {line!r}")
            out.append((dict(LABEL.findall(m.group(2) or "")), value))
    return out


def prom_sum(text, name):
    """Sum of every sample of metric `name` (all label sets). None if the metric is absent."""
    s = prom_samples(text, name)
    return sum(v for _, v in s) if s else None


def delta(arm_dir, name, nwork):
    """Per-worker growth of a vLLM counter between before-w<i>.prom and after-w<i>.prom; None when the
    files or the metric are absent (a hit rate is then unknown, never 0)."""
    out = []
    for i in range(nwork):
        paths = [os.path.join(arm_dir, f"{k}-w{i}.prom") for k in ("before", "after")]
        if not all(exists(p) for p in paths):
            return None
        b, a = (prom_sum(read_text(p), name) for p in paths)
        if a is None:
            return None
        out.append(a - (b or 0.0))
    return out


def pct(values, q):
    """Nearest-rank percentile: the smallest value with at least q of the values at or below it. The product
    is rounded before the ceiling so float noise (0.07 * 100 = 7.000000000000001) cannot move the rank."""
    if not values:
        return None
    v = sorted(values)
    return v[max(0, math.ceil(round(q * len(v), 9)) - 1)]


# ---------------------------------------------------------------- client and placement

def load_client(arm_dir):
    """The arm's client records, one per attempt."""
    path = os.path.join(arm_dir, "client.jsonl")
    out = []
    for n, line in enumerate(read_text(path).splitlines(), 1):
        if line.strip():
            try:
                rec = json.loads(line)
            except ValueError as e:
                raise Unreadable(f"{path}:{n}: not JSON ({e})")
            if not isinstance(rec, dict):
                raise Unreadable(f"{path}:{n}: not an object")
            out.append(rec)
    return out


def client_stats(records):
    ok = [r for r in records if not r.get("err")]
    ttft = [r["ttft_ms"] for r in ok]
    # cached_tokens is -1 (or absent) when the engine reported no prompt_tokens_details; such a request
    # does not count toward the client's cached-token rate, and no report at all gives None.
    with_cache = [r for r in ok if isinstance(r.get("cached_tokens"), (int, float)) and r["cached_tokens"] >= 0]
    prompt = sum(r["prompt_tokens"] for r in with_cache)
    return {
        "requests_ok": len(ok),
        "failed_attempts": len(records) - len(ok),
        "ttft_p50_ms": pct(ttft, 0.50),
        "ttft_p95_ms": pct(ttft, 0.95),
        "ttft_p99_ms": pct(ttft, 0.99),
        "client_hit_rate": (sum(r["cached_tokens"] for r in with_cache) / prompt) if prompt else None,
        "prompt_tokens": sum(r["prompt_tokens"] for r in ok),
    }


def load_placement(results, regime, nwork):
    """{run label: [(start, end, worker, session, turn, header)]} from placement-<regime>-w<i>.tsv, i < nwork.
    Every file must exist and every line parse: a half-read log would make a count look right or wrong."""
    rows = defaultdict(list)
    for i in range(nwork):
        path = os.path.join(results, f"placement-{regime}-w{i}.tsv")
        for n, line in enumerate(read_text(path).splitlines(), 1):
            if not line:
                continue
            f = line.split("\t")
            try:
                if len(f) != 7:
                    raise ValueError(f"{len(f)} fields, want 7")
                start, end, worker, session, turn = float(f[0]), float(f[1]), int(f[2]), int(f[4]), int(f[5])
                if end < start:
                    raise ValueError("ends before it starts")
                if worker != i:
                    raise ValueError(f"worker {worker} in the file of worker {i}")
            except ValueError as e:
                raise Unreadable(f"{path}:{n}: {e}")
            rows[f[3]].append((start, end, worker, session, turn, f[6]))
    return rows


def peak_in_flight(intervals):
    """Most [start, end) intervals open at one instant. [0,1) and [1,2) touch and give 1; an empty
    interval is open at no instant."""
    events = []
    for s, e in intervals:
        if e > s:
            events += [(s, 1), (e, -1)]
    events.sort()  # at equal times -1 sorts first: an interval ends before another starts
    cur = peak = 0
    for _, d in events:
        cur += d
        peak = max(peak, cur)
    return peak


def placement_stats(rows, nwork):
    """rows: (start, end, worker, session, turn, header) of one run. None when the run has no rows."""
    if not rows:
        return None
    rows = sorted(rows, key=lambda r: (r[0], r[1]))
    where = {}
    for _, _, w, s, t, _ in rows:
        where[(s, t)] = w  # a retried turn keeps its last placement
    sessions = defaultdict(dict)
    for (s, t), w in where.items():
        sessions[s][t] = w
    first = Counter(turns[min(turns)] for turns in sessions.values())
    reqs = Counter(r[2] for r in rows)
    same = moves = 0
    for turns in sessions.values():
        for t in sorted(turns):
            if t - 1 in turns:
                same += turns[t] == turns[t - 1]
                moves += 1
    per_worker = [reqs.get(i, 0) for i in range(nwork)]
    total = sum(per_worker)
    mean = total / nwork
    return {
        "requests_per_worker": per_worker,
        "request_share_per_worker": [c / total for c in per_worker],
        "sessions_per_worker": [first.get(i, 0) for i in range(nwork)],
        "busiest_over_mean": max(per_worker) / mean,
        "sticky_share": sum(len(set(t.values())) == 1 for t in sessions.values()) / len(sessions),
        "mean_workers_per_session": sum(len(set(t.values())) for t in sessions.values()) / len(sessions),
        "same_worker_as_previous_turn": same / moves if moves else None,
        "peak_in_flight_per_worker": [peak_in_flight([(r[0], r[1]) for r in rows if r[2] == i]) for i in range(nwork)],
        "header_seen_by_worker": sum(r[5] != "-" for r in rows) / len(rows),
    }


# ---------------------------------------------------------------- the routers' own counters

HOST = re.compile(r"^worker-(\d+)$")
WORKER_URL = re.compile(r"^https?://[^/:]+:(\d+)/?$")


def envoy_counts(path, nwork):
    """rq_total of each worker host, by worker index, from one /clusters?format=json file. Envoy writes counter
    values as strings and leaves out the value of a zero counter, so an absent value is 0."""
    doc = read_json(path)
    clusters = doc.get("cluster_statuses") if isinstance(doc, dict) else None
    cluster = next((c for c in clusters or [] if isinstance(c, dict) and c.get("name") == ENVOY_CLUSTER), None)
    if cluster is None:
        raise Unreadable(f"{path}: no cluster {ENVOY_CLUSTER!r}")
    counts = {}
    for h in cluster.get("host_statuses") or []:
        m = HOST.match(str(h.get("hostname")))
        if not m or int(m.group(1)) >= nwork or int(m.group(1)) in counts:
            raise Unreadable(f"{path}: host {h.get('hostname')!r} is not a distinct worker-<i> with i < {nwork}")
        rq = [s for s in h.get("stats") or [] if s.get("name") == "rq_total"]
        if len(rq) != 1:
            raise Unreadable(f"{path}: {h['hostname']} has no rq_total")
        try:
            counts[int(m.group(1))] = int(rq[0].get("value", 0))
        except (TypeError, ValueError):
            raise Unreadable(f"{path}: {h['hostname']} rq_total is not a number")
    missing = [i for i in range(nwork) if i not in counts]
    if missing:
        raise Unreadable(f"{path}: no host for worker {missing}")
    return [counts[i] for i in range(nwork)]


def envoy_growth(arm_dir, nwork):
    b, a = (envoy_counts(os.path.join(arm_dir, f"clusters-{k}.json"), nwork) for k in ("before", "after"))
    return [x - y for x, y in zip(a, b)]


def relay_map(arm_dir, nwork):
    """{relay port: worker index} from relay-map.tsv; it must name every worker once."""
    path = os.path.join(arm_dir, "relay-map.tsv")
    ports = {}
    for n, line in enumerate(read_text(path).splitlines(), 1):
        if line.strip():
            f = line.split("\t")
            try:
                port, idx = int(f[0]), int(f[1])
                if len(f) != 2 or port in ports or not 0 <= idx < nwork:
                    raise ValueError("want '<port><TAB><worker index>', each port once, index below nwork")
            except (ValueError, IndexError) as e:
                raise Unreadable(f"{path}:{n}: {e}")
            ports[port] = idx
    if sorted(ports.values()) != list(range(nwork)):
        raise Unreadable(f"{path}: must map every worker 0..{nwork - 1} exactly once")
    return ports


def worker_index(label, ports):
    """Worker index of a router's worker="http://127.0.0.1:<port>" label, None when the port is not mapped."""
    m = WORKER_URL.match(label)
    return ports.get(int(m.group(1))) if m else None


def vr_growth(arm_dir, ports, nwork):
    """Growth between vr-before.prom and vr-after.prom of the vllm-router's counters. Both files must exist; a
    series absent from either is 0 (the router creates a series with the first request). Returns
    (counts per worker index, {unmapped worker label: growth}, {policy label: growth})."""
    texts = [read_text(os.path.join(arm_dir, f"vr-{k}.prom")) for k in ("before", "after")]
    paths = [os.path.join(arm_dir, f"vr-{k}.prom") for k in ("before", "after")]

    def totals(name, key):
        res = []
        for text, p in zip(texts, paths):
            t = Counter()
            for labels, v in prom_samples(text, name, p):
                t[key(labels)] += v
            res.append(t)
        return {k: res[1].get(k, 0.0) - res[0].get(k, 0.0) for k in set(res[0]) | set(res[1])}

    counts, unmapped = [0] * nwork, {}
    for label, g in totals("vllm_router_processed_requests_total", lambda l: l.get("worker", "")).items():
        i = worker_index(label, ports)
        if i is not None:
            counts[i] += int(g)
        elif g != 0:
            unmapped[label] = int(g)
    policy = {k: int(g) for k, g in totals("vllm_router_policy_decisions_total", lambda l: l.get("policy", "")).items()}
    return counts, unmapped, policy


# ---------------------------------------------------------------- the five checks

def load_meta(arm_dir):
    path = os.path.join(arm_dir, "meta.json")
    meta = read_json(path)
    if not isinstance(meta, dict):
        raise Unreadable(f"{path}: not an object")
    for k in ("name", "regime", "rep", "arm", "router", "nwork"):
        if k not in meta:
            raise Unreadable(f"{path}: no {k!r}")
    if meta["router"] not in ("vllm-router", "envoy") or not isinstance(meta["nwork"], int) or meta["nwork"] < 1:
        raise Unreadable(f"{path}: router {meta['router']!r} / nwork {meta['nwork']!r} not usable")
    if meta["router"] == "vllm-router" and not isinstance(meta.get("policy"), str):
        raise Unreadable(f"{path}: a vllm-router arm needs a policy")
    if meta["router"] == "envoy" and not (meta.get("bound") == "none" or str(meta.get("bound")).isdigit()):
        raise Unreadable(f"{path}: an envoy arm needs bound 'none' or a number, got {meta.get('bound')!r}")
    return meta


def optional_pair(arm_dir, names):
    """True when every file of the pair exists, False when none does; one without the other is unreadable."""
    have = [exists(os.path.join(arm_dir, n)) for n in names]
    if any(have) and not all(have):
        raise Unreadable(f"{arm_dir}: {[n for n, h in zip(names, have) if not h]} missing beside its pair")
    return all(have)


def load_arm(arm_dir):
    """Everything the checks need, read from files. Raises Unreadable."""
    arm_dir = os.path.abspath(arm_dir)
    meta = load_meta(arm_dir)
    ctx = {"dir": arm_dir, "meta": meta, "nwork": meta["nwork"]}
    ctx["client_records"] = len(load_client(arm_dir))
    rows = load_placement(os.path.dirname(os.path.dirname(arm_dir)), meta["regime"], meta["nwork"])
    ctx["placement_counts"] = [sum(1 for r in rows.get(meta["name"], []) if r[2] == i) for i in range(meta["nwork"])]
    ctx["unmapped"], ctx["policy"] = {}, {}
    if meta["router"] == "envoy":
        ctx["router_counts"] = envoy_growth(arm_dir, meta["nwork"])
        # An Envoy arm has no relay-map.tsv: any growth of a stray vllm-router stack is growth, mapped or not.
        ctx["other_growth"] = (vr_growth(arm_dir, {}, meta["nwork"])
                               if optional_pair(arm_dir, ("vr-before.prom", "vr-after.prom")) else None)
        run = os.path.join(arm_dir, "envoy", "run.json")
        ctx["run_bytes"] = read_bytes(run)  # hashed as the bytes Envoy was started with, never re-encoded
        ctx["run_json"] = read_json(run)
        ctx["start_json"] = read_json(os.path.join(arm_dir, "envoy", "start.json"))
    else:
        ports = relay_map(arm_dir, meta["nwork"])
        ctx["router_counts"], ctx["unmapped"], ctx["policy"] = vr_growth(arm_dir, ports, meta["nwork"])
        ctx["other_growth"] = (envoy_growth(arm_dir, meta["nwork"])
                               if optional_pair(arm_dir, ("clusters-before.json", "clusters-after.json")) else None)
    return ctx


def check_router_count(ctx):
    bad = [f"worker {i}: router {r}, placement {p}"
           for i, (r, p) in enumerate(zip(ctx["router_counts"], ctx["placement_counts"])) if r != p]
    bad += [f"router served {u} (not in relay-map.tsv): {n}" for u, n in sorted(ctx["unmapped"].items())]
    return not bad, "; ".join(bad)


def check_router_total(ctx):
    total = sum(ctx["router_counts"]) + sum(ctx["unmapped"].values())
    if ctx["client_records"] == 0:
        return False, "no client records"
    if total != ctx["client_records"]:
        return False, f"router counts sum to {total}, client has {ctx['client_records']} records"
    return True, ""


def check_policy(ctx):
    if ctx["meta"]["router"] != "vllm-router":
        return True, "n/a: envoy arm"
    want = ctx["meta"]["policy"]
    bad = [f"{n} decisions under policy {p!r}" for p, n in sorted(ctx["policy"].items()) if p != want and n != 0]
    if not ctx["policy"].get(want):
        bad.append(f"no decisions under policy {want!r}")
    return not bad, "; ".join(bad)


def read_factor(run_json, path):
    """hash_balance_factor of clusters[0] in run.json, None when the field is absent at any level."""
    node = run_json
    try:
        clusters = node["static_resources"]["clusters"]
        node = clusters[0]
    except (KeyError, IndexError, TypeError):
        raise Unreadable(f"{path}: no static_resources.clusters[0]")
    for k in ("common_lb_config", "consistent_hashing_lb_config", "hash_balance_factor"):
        if not isinstance(node, dict) or k not in node:
            return None
        node = node[k]
    if isinstance(node, dict):  # a protobuf wrapper, {"value": 125}
        node = node.get("value")
    if isinstance(node, bool) or not isinstance(node, int):
        raise Unreadable(f"{path}: hash_balance_factor {node!r} is not an integer")
    return node


def check_envoy_config(ctx):
    if ctx["meta"]["router"] != "envoy":
        return True, "n/a: vllm-router arm"
    run_path = os.path.join(ctx["dir"], "envoy", "run.json")
    start = ctx["start_json"]
    if not isinstance(start, dict) or not isinstance(start.get("config_sha256"), str) or start.get("mode") not in ("product", "replica") \
            or not isinstance(start.get("diff"), list):
        raise Unreadable(f"{os.path.join(ctx['dir'], 'envoy', 'start.json')}: needs mode, config_sha256 and diff")
    bad = []
    sha = hashlib.sha256(ctx["run_bytes"]).hexdigest()
    if sha != start["config_sha256"].lower():
        bad.append(f"run.json sha256 {sha[:12]}.. differs from start.json config_sha256 {start['config_sha256'][:12]}..")
    bound = ctx["meta"]["bound"]
    want = None if bound == "none" else int(bound)
    got = read_factor(ctx["run_json"], run_path)
    if got != want:
        bad.append(f"run.json hash_balance_factor is {'absent' if got is None else got}, meta bound is {bound}")
    extra = [d for d in start["diff"] if d != FACTOR_PATH]
    if extra:
        bad.append(f"start.json diff lists {extra}, only {FACTOR_PATH} is allowed")
    if start["mode"] == "product" and start["diff"]:
        bad.append("start.json diff is not empty in product mode")
    return not bad, "; ".join(bad)


def check_other_router(ctx):
    g = ctx["other_growth"]
    if g is None:
        return True, "no counters of the other router family"
    if ctx["meta"]["router"] == "envoy":
        counts, unmapped, policy = g
        grew = sum(map(abs, counts)) + sum(map(abs, unmapped.values())) + sum(map(abs, policy.values()))
        return grew == 0, "" if grew == 0 else f"vllm-router counters grew by {grew}"
    return not any(g), "" if not any(g) else f"Envoy rq_total grew by {g}"


def run_checks(ctx):
    """[{"check", "ok", "reason"}] for the five checks on a loaded arm. Raises Unreadable."""
    fns = (check_router_count, check_router_total, check_policy, check_envoy_config, check_other_router)
    out = []
    for name, fn in zip(CHECKS, fns):
        ok, reason = fn(ctx)
        out.append({"check": name, "ok": ok, "reason": reason})
    return out


def check_line(c):
    if not c["ok"]:
        return f"FAIL {c['check']}: {c['reason']}"
    return f"ok {c['check']} ({c['reason']})" if c["reason"] else f"ok {c['check']}"


# ---------------------------------------------------------------- the pass rule

def pass_rule(row, base):
    """The issue's pass rule for one non-baseline arm against the rr arm of its regime and rep."""
    if base is None:
        return {"verdict": "no rr baseline", "parts": {}}
    p = row.get("placement")
    hit, base_hit = row.get("worker_hit_rate"), base.get("worker_hit_rate")
    counts = (p or {}).get("requests_per_worker")
    p95, base_p95 = row.get("ttft_p95_ms"), base.get("ttft_p95_ms")
    total = sum(counts) if counts else 0
    parts = {
        "hit_rate_above_rr": {"value": hit, "rr": base_hit,
                              "passed": None if hit is None or base_hit is None else hit > base_hit},
        "no_worker_above_40pct": {"value": max(counts) / total if total else None, "limit": SHARE_LIMIT_NUM / SHARE_LIMIT_DEN,
                                  "passed": None if not total else max(counts) * SHARE_LIMIT_DEN <= total * SHARE_LIMIT_NUM},
        "ttft_p95_not_worse_than_rr": {"value": p95, "rr": base_p95,
                                       "passed": None if p95 is None or base_p95 is None else p95 <= base_p95},
    }
    passed = [x["passed"] for x in parts.values()]
    verdict = "fail" if False in passed else "pass" if all(x is True for x in passed) else "unknown"
    return {"verdict": verdict, "parts": parts}


def attach_pass_rules(rows):
    """Sets row["pass_rule"] on every row. The baseline is the rr arm with the same regime and rep."""
    base = {(r["regime"], r["rep"]): r for r in rows if r.get("arm") == "rr" and not r.get("skipped")}
    for r in rows:
        if r.get("skipped"):
            continue
        r["pass_rule"] = {"verdict": "baseline", "parts": {}} if r["arm"] == "rr" else pass_rule(r, base.get((r["regime"], r["rep"])))


# ---------------------------------------------------------------- the summary

def arm_row(arm_dir, meta):
    """One arm's row. Each source is read on its own: an unreadable one leaves its fields None and its error in
    row["errors"], so one bad file hides nothing else."""
    n = meta["nwork"]
    row, errors = dict(meta), []
    row["wall_s"] = round(meta["end_unix"] - meta["start_unix"], 1) if "start_unix" in meta and "end_unix" in meta else None
    results = os.path.dirname(os.path.dirname(arm_dir))
    try:
        row.update(client_stats(load_client(arm_dir)))
    except Unreadable as e:
        errors.append(str(e))
    try:
        hits, queries = delta(arm_dir, "vllm:prefix_cache_hits_total", n), delta(arm_dir, "vllm:prefix_cache_queries_total", n)
        pre = delta(arm_dir, "vllm:num_preemptions_total", n)
        row["worker_hit_rate"] = (sum(hits) / sum(queries)) if hits and queries and sum(queries) else None
        row["worker_hit_rate_per_worker"] = [round(h / q, 3) if q else None for h, q in zip(hits, queries)] if hits and queries else None
        row["preemptions"] = int(sum(pre)) if pre else None
    except Unreadable as e:
        errors.append(str(e))
    try:
        row["placement"] = placement_stats(load_placement(results, meta["regime"], n).get(meta["name"], []), n)
    except Unreadable as e:
        row["placement"] = None
        errors.append(str(e))
    try:
        ctx = load_arm(arm_dir)
        row["router_counts"] = ctx["router_counts"]
        row["checks"] = run_checks(ctx)
    except Unreadable as e:
        row["checks"] = None
        errors.append(str(e))
    row["errors"] = errors
    return row


def analyse(results):
    """Rows of every arm under results/arms, with the pass rules attached."""
    rows = []
    for arm_dir in sorted(glob.glob(os.path.join(results, "arms", "*", ""))):
        arm_dir = os.path.abspath(arm_dir)
        name = os.path.basename(arm_dir)
        if os.path.exists(os.path.join(arm_dir, "SKIPPED")):
            rows.append({"name": name, "skipped": True, "reason": read_text(os.path.join(arm_dir, "SKIPPED")).strip()[:200],
                         "errors": [], "checks": None})
            continue
        try:
            rows.append(arm_row(arm_dir, load_meta(arm_dir)))
        except Unreadable as e:
            rows.append({"name": name, "skipped": False, "errors": [str(e)], "checks": None, "arm": None, "regime": "?", "rep": 0})
    rows.sort(key=lambda r: (str(r.get("regime", "~")), r.get("rep", 0), ARM_ORDER.get(r.get("arm"), 9), r["name"]))
    attach_pass_rules([r for r in rows if r.get("arm")])
    return rows


def status_of(rows):
    """2 when any arm had unreadable input, else 1 when any check failed, else 0."""
    if any(r["errors"] for r in rows):
        return 2
    return 1 if any(c and not c["ok"] for r in rows for c in r["checks"] or []) else 0


def fmt(v, spec):
    return "n/a" if v is None else format(v, spec)


def markdown(rows):
    out = ["| Run | Router | Hit rate (workers) | Hit rate (client) | TTFT p50 / p95 / p99 ms | Requests per worker | Share per worker "
           "| Sessions per worker | Sticky | Busiest / mean | Peak in flight | Router counts | OK / failed | Checks | Pass rule |",
           "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
    notes = []
    for r in rows:
        if r.get("skipped"):
            out.append(f"| {r['name']} | skipped | " + " | ".join(["n/a"] * 12) + f" | skipped: {r['reason']} |")
            continue
        p = r.get("placement") or {}
        checks = r["checks"]
        failed = [c["check"] for c in checks or [] if not c["ok"]]
        cell = "unreadable" if checks is None else ("FAIL " + ", ".join(failed) if failed else "ok")
        out.append("| {} | {} | {} | {} | {} / {} / {} | {} | {} | {} | {} | {} | {} | {} | {} / {} | {} | {} |".format(
            r["name"], r.get("arm"), fmt(r.get("worker_hit_rate"), ".3f"), fmt(r.get("client_hit_rate"), ".3f"),
            fmt(r.get("ttft_p50_ms"), ".0f"), fmt(r.get("ttft_p95_ms"), ".0f"), fmt(r.get("ttft_p99_ms"), ".0f"),
            p.get("requests_per_worker", "n/a"), [round(s, 3) for s in p["request_share_per_worker"]] if p else "n/a",
            p.get("sessions_per_worker", "n/a"), fmt(p.get("sticky_share"), ".2f"), fmt(p.get("busiest_over_mean"), ".2f"),
            p.get("peak_in_flight_per_worker", "n/a"), r.get("router_counts", "n/a"),
            r.get("requests_ok", "n/a"), r.get("failed_attempts", "n/a"), cell, (r.get("pass_rule") or {}).get("verdict", "n/a")))
        for c in checks or []:
            if not c["ok"]:
                notes.append(f"- {r['name']}: {check_line(c)}")
        notes += [f"- {r['name']}: unreadable input: {e}" for e in r["errors"]]
    rules = []
    for r in rows:
        pr = r.get("pass_rule")
        if pr and pr["verdict"] not in ("baseline",):
            parts = "; ".join(f"{k} {'?' if v['passed'] is None else 'yes' if v['passed'] else 'NO'} ({fmt(v['value'], '.3f')})"
                              for k, v in pr["parts"].items())
            rules.append(f"- {r['name']}: {pr['verdict']}" + (f" ({parts})" if parts else ""))
    text = "\n".join(out)
    if rules:
        text += "\n\nPass rule against the rr arm of the same regime and rep:\n" + "\n".join(rules)
    if notes:
        text += "\n\nProblems:\n" + "\n".join(notes)
    return text


def check_arm_main(arm_dir):
    try:
        checks = run_checks(load_arm(arm_dir))
    except Unreadable as e:
        print(f"unreadable: {e}", file=sys.stderr)
        return 2
    for c in checks:
        print(check_line(c))
    return 0 if all(c["ok"] for c in checks) else 1


def summary_main(results, json_out, md_out):
    rows = analyse(results)
    if json_out:
        with open(json_out, "w") as f:
            json.dump(rows, f, indent=1)
    md = markdown(rows)
    if md_out:
        with open(md_out, "w") as f:
            f.write(md + "\n")
    print(md)
    return status_of(rows)


def main(argv):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("results_dir", nargs="?")
    ap.add_argument("--json")
    ap.add_argument("--md")
    ap.add_argument("--check-arm", metavar="ARMDIR")
    a = ap.parse_args(argv[1:])
    if a.check_arm:
        if a.results_dir or a.json or a.md:
            ap.error("--check-arm takes no other argument")
        return check_arm_main(a.check_arm)
    if not a.results_dir:
        ap.error("RESULTS_DIR or --check-arm ARMDIR is required")
    return summary_main(a.results_dir, a.json, a.md)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
