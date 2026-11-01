"""Reads one run of TestRealEngine and prints the numbers the README reports.

From reports.jsonl.gz: the timeline, the staging totals, and the Health samples
(distinct by metricsReadAt) with their freshness. From summary.json's warm-up
rounds: the per-shape means, the change between rounds, the settling verdict
under HBEP-7's rule, TTFT and TPOT, all recomputed here rather than taken from
the driver. From raw/: the engine's own bytes (metric names, cache_config_info,
streams), judged against the HBEP text, never against the agent's parsing.

Usage: python3 -I spikes/s94/analyze.py spikes/s94/results/r3
"""
import gzip
import json
import os
import re
import sys
from collections import defaultdict
from datetime import datetime

run = sys.argv[1]
S = json.load(open(os.path.join(run, "summary.json")))

HBEP8 = ["vllm:num_requests_running", "vllm:num_requests_waiting", "vllm:generation_tokens_total",
         "vllm:kv_cache_usage_perc", "vllm:num_preemptions_total", "vllm:prefix_cache_queries_total",
         "vllm:prefix_cache_hits_total", "vllm:request_time_per_output_token_seconds_sum",
         "vllm:request_time_per_output_token_seconds_count", "vllm:cache_config_info", "http_requests_total"]
HEALTH = ["probeOK", "metricsReadAt", "runningRequests", "waitingRequests", "generatedTokens", "kvCacheUsage",
          "preemptions", "prefixCacheQueriedTokens", "prefixCacheHitTokens", "timePerOutputTokenSecondsSum",
          "timePerOutputTokenCount", "responsesByStatus"]
LINE = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{(.*)\})?\s+(\S+)')
LABEL = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"')
UNIT = {"h": 3600, "m": 60, "s": 1, "ms": 1e-3, "µs": 1e-6, "us": 1e-6, "ns": 1e-9}


def seconds(d):
    """A Go duration string ("1.5s", "25.4ms", "1m2.3s") in seconds."""
    return sum(float(n) * UNIT[u] for n, u in re.findall(r"([\d.]+)(h|ms|µs|us|ns|m|s)", d or ""))


def when(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def opened(path):
    """The file, or its .gz copy (the committed metrics captures are gzipped)."""
    if os.path.exists(path):
        return open(path, encoding="utf-8", errors="replace")
    return gzip.open(path + ".gz", "rt", encoding="utf-8", errors="replace")


def prom(path):
    series, types = defaultdict(list), {}
    for line in opened(path):
        line = line.strip()
        if line.startswith("# TYPE"):
            _, _, name, kind = line.split(None, 3)
            types[name] = kind
            continue
        if not line or line[0] == "#":
            continue
        m = LINE.match(line)
        if m:
            series[m.group(1)].append((dict(LABEL.findall(m.group(3) or "")), float(m.group(4))))
    return series, types


def sse(path):
    """A stream's events, how it ends, and what its finish chunk carried."""
    raw = open(path, "rb").read().decode("utf-8", "replace")
    events = []
    for b in raw.split("\n\n"):
        if b.strip():
            events.append("\n".join(l[5:].lstrip(" ") for l in b.split("\n") if l.startswith("data:")))
    out = {"events": len(events), "endsWithBlankLine": raw.endswith("\n\n"), "last": events[-1] if events else None}
    keys, finish, first_kind, errors = set(), [], None, []
    for i, e in enumerate(events):
        if e == "[DONE]":
            out["doneAt"] = i
            continue
        try:
            j = json.loads(e)
        except ValueError:
            errors.append(e[:80])
            continue
        if "error" in j:
            errors.append(json.dumps(j)[:200])
        if j.get("usage"):
            out["usageAt"], out["usage"] = i, j["usage"]
        if j.get("system_fingerprint"):
            out.setdefault("fingerprintAt", []).append(i)
        for c in j.get("choices", []):
            d = c.get("delta", {})
            keys |= set(d.keys())
            if c.get("finish_reason"):
                finish.append(c["finish_reason"])
                out["finishAt"] = i
                out["finishDelta"] = d
                out["finishDeltaEmpty"] = not any(v not in (None, "", []) for v in d.values())
            if first_kind is None:
                for k in ("reasoning", "reasoning_content", "content", "tool_calls"):
                    if d.get(k):
                        first_kind = k
                        break
    out.update({"deltaKeys": sorted(keys), "finishReasons": finish, "firstNonEmptyDelta": first_kind, "errors": errors})
    return out


def timeline(reports):
    """Per Instance: when each phase first appears, from the first report on it."""
    by = defaultdict(list)
    for r in reports:
        if r.get("instance"):
            by[r["instance"]["uid"]].append(r)
    out = {}
    for uid, rs in by.items():
        first, samples, staging, oldest = {}, {}, None, 0.0
        for r in rs:
            i, t = r["instance"], when(r["time"])
            w = (i.get("workers") or [{}])[0]
            c = i.get("checks") or {}
            marks = {
                "staging " + str((i.get("staging") or {}).get("phase")): True,
                "worker " + str(w.get("phase")): w.get("phase"),
                "verify done": (c.get("verify") or {}).get("done"),
                "qualify done": (c.get("qualify") or {}).get("done"),
                "warm-up done": (c.get("warmUp") or {}).get("done"),
            }
            for k, v in marks.items():
                if v and k not in first:
                    first[k] = t
            if i.get("staging"):
                staging = i["staging"]
            h = w.get("health")
            if h:
                samples.setdefault(h.get("metricsReadAt") or h["time"], (t, h))
                if h.get("metricsReadAt"):
                    oldest = max(oldest, (t - when(h["metricsReadAt"])).total_seconds())
        t0 = min(first.values())
        reads = sorted(when(k) for k, (_, h) in samples.items() if h.get("metricsReadAt"))
        out[uid] = {
            "reports": len(rs),
            "reportsWithHealth": sum(1 for r in rs if ((r["instance"].get("workers") or [{}])[0]).get("health")),
            "distinctSamples": len(samples),
            "samplesWithAFieldMissing": sum(1 for _, h in samples.values() if any(f not in h for f in HEALTH)),
            "samplesWithAFailedProbe": sum(1 for _, h in samples.values() if not h.get("probeOK")),
            "secondsBetweenReads": sorted({round((b - a).total_seconds(), 1) for a, b in zip(reads, reads[1:])}),
            "oldestReadInAReport": round(oldest, 1),
            "stagingFinal": staging,
            "phases": {k: round((v - t0).total_seconds(), 1) for k, v in sorted(first.items(), key=lambda x: x[1])},
        }
    return out


def shape_means(r):
    total, n, seen = defaultdict(float), defaultdict(int), set()
    for q in r["requests"]:
        seen.add(q["shape"])
        if not q.get("error"):
            total[q["shape"]] += seconds(q["total"])
            n[q["shape"]] += 1
    return {s: (total[s] / n[s] if n[s] else None) for s in seen}


def settled(prev, cur):
    """HBEP-7: no shape more than 10% slower than in the round before, and every shape with a success."""
    return all(m is not None and prev.get(s) is not None and m <= 1.1 * prev[s] for s, m in cur.items())


def throughput(r, shape):
    ttft = [seconds(q["timeToFirstToken"]) for q in r["requests"] if q["shape"] == shape and not q.get("error") and q["outputTokens"] >= 1]
    tpot = [(seconds(q["total"]) - seconds(q["timeToFirstToken"])) / (q["outputTokens"] - 1)
            for q in r["requests"] if q["shape"] == shape and not q.get("error") and q["outputTokens"] >= 2]
    mean = lambda xs: sum(xs) / len(xs) if xs else None
    return {"ttftMs": round(mean(ttft) * 1e3, 1) if ttft else None, "n": len(ttft), "tpotMs": round(mean(tpot) * 1e3, 2) if tpot else None}


reports = [json.loads(l) for l in gzip.open(os.path.join(run, "reports.jsonl.gz"), "rt")]
tl = timeline(reports)
print("reports:", len(reports))

for label, inst in S.items():
    if not isinstance(inst, dict) or "uid" not in inst:
        continue
    print(f"\n######## {label}")
    print("timeline and Health (from reports.jsonl.gz):", json.dumps(tl.get(inst["uid"]), indent=1))
    v = inst.get("verify", {})
    print("verify: version", v.get("engineVersion"), "digest", v.get("imageDigest"), "overhead", v.get("templateOverhead"),
          "fingerprint", repr(v.get("fingerprint")), "error", v.get("error"))
    print("  files:", [(f["path"], f["outcome"]) for f in v.get("files", [])])
    print("  settings:", json.dumps(inst.get("settings"), indent=1))
    q = inst.get("qualify", {})
    print("qualify:", "kv", q.get("kvCapacityTokens"), "names", q.get("servedModelNames"), "fp", repr(q.get("fingerprint")))
    for c in q.get("checks", []):
        print("  ", c.get("name"), "passed" if c.get("passed") else "FAILED", c.get("reason", ""), c.get("contextTokens", ""), (c.get("message") or "")[:300])
    print("  behaviour context expected (HBEP-7 by hand):", inst.get("behaviourContextExpected"))
    w = inst.get("warmUp", {})
    rounds = w.get("rounds", [])
    print("warm-up: done", w.get("done"), "| driver's settled-at round", inst.get("warmUpSettledAtRound"))
    prev = None
    for r in rounds:
        cur = shape_means(r)
        row = "; ".join(f"{s}={'failed' if m is None else f'{m * 1e3:.1f}ms'}" for s, m in sorted(cur.items()))
        print(f"  round {r['round']}: {row}")
        for t in r["requests"]:
            if t.get("error"):
                print(f"     error, {t['shape']}: {t['error'][:160]!r}, outputTokens {t['outputTokens']}, total {t['total']}")
        if prev is not None:
            ch = {s: f"{(m / prev[s] - 1) * 100:+.2f}%" for s, m in sorted(cur.items()) if m is not None and prev.get(s)}
            print(f"     change from round {r['round'] - 1}: {ch}; settled (recomputed): {settled(prev, cur)}")
        prev = cur
    if rounds:
        last = rounds[-1]
        print("  last round, recomputed:", {s: throughput(last, s) for s in ("concurrent", "short", "typical", "longest", "longOutput", "toolCall")})
    print("health (last sample):", json.dumps(inst.get("health"), indent=1))
    raw = os.path.join(run, "raw", label)
    for phase in ("before", "after"):
        p = os.path.join(raw, f"{phase}-metrics.txt")
        if not os.path.exists(p) and not os.path.exists(p + ".gz"):
            continue
        ser, types = prom(p)
        print(f"metrics ({phase}): families", len(types))
        for n in HBEP8:
            vals = ser.get(n)
            print(f"   {n}: {'PRESENT' if vals else 'ABSENT'}", "" if not vals else (sum(x for _, x in vals), len(vals), sorted(vals[0][0].keys()) if n != "vllm:cache_config_info" else ""))
        if phase == "before":
            cc = ser.get("vllm:cache_config_info")
            if cc:
                keep = ("block_size", "cache_dtype", "enable_prefix_caching", "gpu_memory_utilization", "kv_cache_size_tokens", "num_gpu_blocks")
                print("   cache_config_info:", json.dumps({k: cc[0][0].get(k) for k in keep}))
            hs = ser.get("http_requests_total")
            if hs:
                print("   http_requests_total labels:", sorted({tuple(sorted(l.items())) for l, _ in hs}))
    for name in ("version.json", "models.json", "chat.json"):
        p = os.path.join(raw, f"before-{name}")
        if os.path.exists(p):
            print(f"{name}:", open(p).read()[:500].replace("\n", " "))
    for name in ("stream-stop.sse", "stream-length.sse", "stream-tool.sse"):
        p = os.path.join(raw, f"before-{name}")
        if os.path.exists(p):
            print(f"{name}:", json.dumps(sse(p)))
print("\nregistration:", S.get("registration"), "agentExit:", S.get("agentExit"))
