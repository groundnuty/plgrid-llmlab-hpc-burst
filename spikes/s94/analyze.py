"""Reads one run of TestRealEngine (summary.json and raw/) and prints the facts the
README reports. Raw engine bytes are judged against the HBEP text, never against
the agent's parsing. Usage: python3 -I spikes/s94/analyze.py spikes/s94/results/r3"""
import gzip
import json
import os
import re
import sys
from collections import defaultdict

run = sys.argv[1]
S = json.load(open(os.path.join(run, "summary.json")))

HBEP8 = ["vllm:num_requests_running", "vllm:num_requests_waiting", "vllm:generation_tokens_total",
         "vllm:kv_cache_usage_perc", "vllm:num_preemptions_total", "vllm:prefix_cache_queries_total",
         "vllm:prefix_cache_hits_total", "vllm:request_time_per_output_token_seconds_sum",
         "vllm:request_time_per_output_token_seconds_count", "vllm:cache_config_info", "http_requests_total"]
LINE = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{(.*)\})?\s+(\S+)')
LABEL = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"')


def opened(path):
    """The file, or its .gz copy (the committed metrics captures are gzipped)."""
    if os.path.exists(path):
        return open(path, encoding="utf-8", errors="replace")
    return gzip.open(path + ".gz", "rt", encoding="utf-8", errors="replace")


def prom(path):
    series = defaultdict(list)
    types = {}
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
    raw = open(path, "rb").read().decode("utf-8", "replace")
    blocks = raw.split("\n\n")
    tail_ok = raw.endswith("\n\n")
    events = []
    for b in blocks:
        if not b.strip():
            continue
        data = [l[5:].lstrip(" ") if l.startswith("data:") else None for l in b.split("\n")]
        data = [d for d in data if d is not None]
        events.append("\n".join(data))
    out = {"events": len(events), "endsWithBlankLine": tail_ok, "last": events[-1] if events else None}
    keys, finish, usage_at, finish_at, first_kind, errors = set(), [], None, None, None, []
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
            usage_at = i
            out["usage"] = j["usage"]
        for c in j.get("choices", []):
            d = c.get("delta", {})
            keys |= set(d.keys())
            if c.get("finish_reason"):
                finish.append(c["finish_reason"])
                finish_at = i
            if first_kind is None:
                for k in ("reasoning", "reasoning_content", "content", "tool_calls"):
                    if d.get(k):
                        first_kind = k
                        break
    out.update({"deltaKeys": sorted(keys), "finishReasons": finish, "finishAt": finish_at, "usageAt": usage_at,
                "firstNonEmptyDelta": first_kind, "errors": errors})
    return out


for label, inst in S.items():
    if not isinstance(inst, dict) or "uid" not in inst:
        continue
    print(f"\n######## {label}")
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
    print("warm-up: done", w.get("done"), "settled at", inst.get("warmUpSettledAtRound"))
    for r in w.get("rounds", []):
        per = defaultdict(list)
        errs = defaultdict(int)
        for t in r["requests"]:
            if t.get("error"):
                errs[t["shape"]] += 1
                continue
            per[t["shape"]].append(t)
        def sec(d):
            m = re.match(r"^([\d.]+)(ms|s|µs|us|m)", d) if d else None
            if not d or d == "0s":
                return 0.0
            total = 0.0
            for num, unit in re.findall(r"([\d.]+)(h|ms|µs|us|ns|m|s)", d):
                total += float(num) * {"h": 3600, "m": 60, "s": 1, "ms": 1e-3, "µs": 1e-6, "us": 1e-6, "ns": 1e-9}[unit]
            return total
        row = []
        for s, ts in per.items():
            tot = sum(sec(t["total"]) for t in ts) / len(ts)
            out = sum(t["outputTokens"] for t in ts) / len(ts)
            row.append(f"{s}: n={len(ts)} total={tot:.3f}s out={out:.0f}")
        print(f"  round {r['round']}:", "; ".join(row), dict(errs) or "")
        for s, n in errs.items():
            msgs = {t.get("error")[:160] for t in r["requests"] if t["shape"] == s and t.get("error")}
            print("     errors", s, msgs)
    print("throughput (last round, concurrent):", inst.get("throughputLastRound"))
    print("by shape:", json.dumps(inst.get("throughputLastRoundByShape")))
    h = inst.get("health")
    print("health:", json.dumps(h, indent=1))
    raw = os.path.join(run, "raw", label)
    for when in ("before", "after"):
        p = os.path.join(raw, f"{when}-metrics.txt")
        if not os.path.exists(p) and not os.path.exists(p + ".gz"):
            continue
        ser, types = prom(p)
        print(f"metrics ({when}): families", len(types))
        for n in HBEP8:
            vals = ser.get(n)
            print(f"   {n}: {'PRESENT' if vals else 'ABSENT'}", "" if not vals else (sum(x for _, x in vals), [l for l, _ in vals][:2] if n in ("vllm:cache_config_info", "http_requests_total") else len(vals)))
        if when == "before":
            alts = sorted(k for k in types if re.search(r"preempt|prefix_cache|time_per_output|kv_cache|generation_tokens|requests_running|requests_waiting", k))
            print("   related families:", alts)
            cc = ser.get("vllm:cache_config_info")
            if cc:
                print("   cache_config_info labels:", json.dumps(cc[0][0]))
            hs = ser.get("http_requests_total")
            if hs:
                print("   http_requests_total labels:", sorted({tuple(sorted(l.items())) for l, _ in hs})[:6])
    for name in ("version.json", "models.json", "chat.json"):
        p = os.path.join(raw, f"before-{name}")
        if os.path.exists(p):
            b = open(p).read()
            print(f"{name}:", b[:600].replace("\n", " "))
    for name in ("stream-stop.sse", "stream-length.sse", "stream-tool.sse"):
        p = os.path.join(raw, f"before-{name}")
        if os.path.exists(p):
            print(f"{name}:", json.dumps(sse(p)))
print("\nregistration:", S.get("registration"), "agentExit:", S.get("agentExit"))
