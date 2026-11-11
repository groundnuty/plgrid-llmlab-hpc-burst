"""Spike S16's result rules (issue #412), shared by the driver (s16.py) and summarize.py.

Every rule here reads the raw files of one run (S16_RESULTS/raw/<run_id>/) and decides nothing from
the code under test: the expected values are the issue's and HBEP-6's (the 1 s plugin rule, the
60 s re-login rule, the cap), and the validity rule is S14's (every tunnel receives a token in
every second of the window). Times are Unix seconds (float) unless named _ms or _ns.
"""
import glob
import json
import math
import os
import re

CLK = 100  # USER_HZ on Linux (sysconf SC_CLK_TCK), as on every runner this spike used
PLUGIN_RULE_MS = 1000.0   # HBEP-6, Admission: every plugin answer within 1 s
RELOGIN_RULE_S = 60.0     # issue #412: any tunnel not back within 60 s is a finding
CAP_FILL = 0.95
RECOVER_S = 10.0          # the front host's CPU is read over the 10 s after a cut
FRONT_HOST_ROLES = ("frps", "admit", "front")           # a capped run's every second at 95% of the cap or more (team-lead, 11 Nov 2026)
TOKEN_RATE = {50: 20.0, 200: 5.0}  # tokens/s per stream at the double's delay (ms)


# --- reading --------------------------------------------------------------------------------------

def read_json(path, default=None):
    try:
        with open(path) as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return default


def read_lines(path):
    out = []
    try:
        with open(path) as fh:
            for line in fh:
                line = line.strip()
                if line:
                    try:
                        out.append(json.loads(line))
                    except ValueError:
                        pass
    except OSError:
        pass
    return out


class Run:
    """One run's raw files."""

    def __init__(self, d):
        self.dir = d
        self.m = read_json(os.path.join(d, "manifest.json"), {})
        self.n = self.m.get("n", 0)

    def lines(self, name):
        return read_lines(os.path.join(self.dir, name))

    def json(self, name, default=None):
        return read_json(os.path.join(self.dir, name), default)

    def samples(self):
        return self.json("samples.json", [])

    def polls(self):
        return self.json("polls.json", [])

    def events(self, name):
        return [e for e in self.m.get("events", []) if e["event"] == name]

    def load(self, name="load.jsonl"):
        return self.lines(name)

    def regs(self):
        """Every front generation's registration changes, as (gen, record)."""
        out = []
        for p in sorted(glob.glob(os.path.join(self.dir, "reg-*.jsonl"))):
            g = int(re.search(r"reg-(\d+)\.jsonl$", p).group(1))
            out += [(g, r) for r in read_lines(p)]
        return out


# --- statistics -----------------------------------------------------------------------------------

def percentile(xs, p):
    """Nearest rank on a sorted copy, as spike S1c's and S14's stats.go."""
    if not xs:
        return None
    s = sorted(xs)
    rank = max(1, min(len(s), int(math.ceil(p / 100.0 * len(s)))))
    return s[rank - 1]


def dist(xs):
    if not xs:
        return {"n": 0, "p50": None, "p99": None, "max": None}
    return {"n": len(xs), "p50": percentile(xs, 50), "p99": percentile(xs, 99), "max": max(xs)}


# --- CPU ------------------------------------------------------------------------------------------

def cores(samples, role, t0, t1):
    """A role's CPU over [t0, t1], in cores: per PID, its ticks between its first and last sample
    inside the window, summed, over the window's sampled span. A PID that lives only part of the
    window counts only the ticks between its own samples (a front replaced inside the window loses
    at most one sample interval of its last ticks)."""
    inside = [s for s in samples if t0 <= s["t"] <= t1]
    if len(inside) < 2:
        return None
    span = inside[-1]["t"] - inside[0]["t"]
    first, last = {}, {}
    for s in inside:
        for pid, tk in (s["ticks"].get(role) or {}).items():
            first.setdefault(pid, tk)
            last[pid] = tk
    ticks = sum(last[p] - first[p] for p in last)
    return ticks / CLK / span if span > 0 else None


def peak_cores(samples, roles, t0, t1):
    """The roles' busiest interval between two consecutive samples inside [t0, t1], in cores
    (a PID present in both samples counts)."""
    inside = [s for s in samples if t0 <= s["t"] <= t1]
    best = None
    for a, b in zip(inside, inside[1:]):
        dt = b["t"] - a["t"]
        if dt <= 0:
            continue
        tk = 0
        for role in roles:
            ta, tb = a["ticks"].get(role) or {}, b["ticks"].get(role) or {}
            tk += sum(tb[p] - ta[p] for p in tb if p in ta)
        c = tk / CLK / dt
        best = c if best is None else max(best, c)
    return best


def peak_busy(samples, t0, t1):
    inside = [s for s in samples if t0 <= s["t"] <= t1]
    best = None
    for a, b in zip(inside, inside[1:]):
        d = [y - x for x, y in zip(a["stat"], b["stat"])]
        total = sum(d[:8])
        if total > 0:
            v = 1 - (d[3] + d[4]) / total
            best = v if best is None else max(best, v)
    return best


def host_busy(samples, t0, t1):
    """The host's busy fraction over [t0, t1] from /proc/stat: 1 - (idle + iowait) / total."""
    inside = [s for s in samples if t0 <= s["t"] <= t1]
    if len(inside) < 2:
        return None
    a, b = inside[0]["stat"], inside[-1]["stat"]
    d = [y - x for x, y in zip(a, b)]
    total = sum(d[:8])
    return 1 - (d[3] + d[4]) / total if total > 0 else None


# --- tokens ---------------------------------------------------------------------------------------

def token_times(rec):
    """A request's token arrivals, Unix seconds."""
    s = rec["s"] / 1e9
    return [s + k / 1e4 for k in rec.get("k") or []]


def per_second(load, n, t0, t1):
    """Tokens each tunnel received in each whole second of [t0, t1)."""
    secs = int(math.floor(t1 - t0))
    out = [[0] * secs for _ in range(n)]
    for r in load:
        for t in token_times(r):
            if t0 <= t < t0 + secs:
                out[r["i"]][int(t - t0)] += 1
    return out


def tunnel_validity(load, n, t0, t1):
    """S14's rule: valid only if every tunnel received a token in every second of the window.
    Returns (valid, the emptiest second's tokens over all tunnels, the tunnels with an empty second)."""
    ps = per_second(load, n, t0, t1)
    if not ps or not ps[0]:
        return False, 0, list(range(n))
    empty = [i for i, row in enumerate(ps) if min(row) == 0]
    return not empty, min(min(row) for row in ps), empty


def tokens_per_s(load, t0, t1):
    k = sum(1 for r in load for t in token_times(r) if t0 <= t < t1)
    return k / (t1 - t0) if t1 > t0 else 0.0


def gaps_ms(load, t0, t1):
    """Every gap between two consecutive tokens of one request, both inside the window, in ms."""
    out = []
    for r in load:
        ts = [t for t in token_times(r) if t0 <= t < t1]
        out += [(b - a) * 1e3 for a, b in zip(ts, ts[1:])]
    return out


# --- item 1: mass re-login ------------------------------------------------------------------------

def cut_time(run):
    """When the tunnels were cut: frps's exit (the supervisor's "stopped" after the restart signal),
    or the old front's exit (its passthrough closes the tunnels then, after its shutdown grace).
    For a run with no event, the moment the event would have been."""
    t_ev = run.m.get("t_event")
    kind = run.m.get("event_kind")
    if kind == "frps":
        st = [e["t_ns"] / 1e9 for e in run.lines("frps-events.jsonl") if e["event"] == "stopped" and e["t_ns"] / 1e9 >= t_ev]
        return min(st) if st else None
    if kind == "front":
        ex = [e["t_ns"] / 1e9 for e in run.lines("front-life.jsonl") if e["event"] == "exit" and e["t_ns"] / 1e9 >= t_ev]
        return min(ex) if ex else None
    return t_ev


def relogin(run):
    """The re-login measures of one run. Expected values: HBEP-6's 1 s plugin rule, the issue's
    60 s rule; the floor is frpc's reconnect backoff (README, frp facts)."""
    m, n = run.m, run.n
    t_ev, t_cut = m.get("t_event"), cut_time(run)
    out = {"run_id": m.get("run_id"), "event": m.get("event_kind"), "n": n, "status": m.get("status"),
           "t_event": t_ev, "t_cut": t_cut}
    load = run.load()
    pre0, pre1 = m.get("pre_start"), m.get("pre_end")
    if pre0 is None or t_cut is None:
        out["valid"] = False
        return out
    valid, emptiest, empty = tunnel_validity(load, n, pre0, pre1)
    out.update(pre_valid=valid, pre_emptiest_second=emptiest, pre_empty_tunnels=len(empty))
    # The request in flight across the event, per tunnel, and its end.
    inflight, cuts = {}, {}
    for r in load:
        s, e = r["s"] / 1e9, r["end"] / 1e9
        if s < t_ev < e and r["i"] not in inflight:
            inflight[r["i"]] = r
            cuts[r["i"]] = {"end": e, "err": (r.get("e") or "")[:80], "tokens": len(r.get("k") or []), "done": r.get("done")}
    out["inflight_tunnels"] = len(inflight)
    out["cut_requests"] = sum(1 for c in cuts.values() if not c["done"])
    kinds = {}
    for c in cuts.values():
        k = "completed" if c["done"] else re.sub(r"[0-9]+", "#", c["err"])[:60]
        kinds[k] = kinds.get(k, 0) + 1
    out["cut_kinds"] = kinds
    # Registered again: the front's plugin answered an accepted NewProxy for the UID after the cut
    # (frps registers the proxy only after both plugins answer: HBEP-6, Admission).
    calls = run.lines("admit-calls.jsonl") + run.lines("front-calls.jsonl")
    reg_again, admit_again = {}, {}
    for c in calls:
        t = c["end_ns"] / 1e9
        if t < t_cut or c.get("op") != "NewProxy" or c.get("reject"):
            continue
        d = reg_again if c["plugin"] == "front" else admit_again
        d.setdefault(c.get("proxy"), t)
    # Routed again: the front counts the UID registered again (a probe passed in the new epoch);
    # after a front restart, in the new front's first registration.
    gen_cut = max([g for g, _ in run.regs()] or [1])
    routed = {}
    for g, r in run.regs():
        t = r["t_ns"] / 1e9
        if r["reg"] and t >= t_cut and (m.get("event_kind") != "front" or g == gen_cut):
            routed.setdefault(r["i"], t)
    # First token through the front after the cut.
    first_tok = {}
    for r in load:
        for t in token_times(r):
            if t > t_cut:
                first_tok[r["i"]] = min(first_tok.get(r["i"], t), t)
                break
    proxies = sorted(set(reg_again))
    out["registered_again"] = len(reg_again)
    uids = m.get("uids") or []
    rereg = {uids.index(p): t for p, t in reg_again.items() if p in uids}
    pz = pauses(run, t_cut, rereg)
    out["paused"] = sum(1 for p in pz if p["paused"])
    kinds = {}
    for p in pz:
        for f in p["failures"]:
            kinds[f["kind"]] = kinds.get(f["kind"], 0) + 1
    out["pause_failure_kinds"] = kinds
    starts = [p["pause_start_rel_cut"] for p in pz if p["paused"]]
    out["pause_start_rel_cut"] = dist(starts)
    out["per_instance"] = [dict(p, routed_rel_cut=(routed[p["i"]] - t_cut) if p["i"] in routed else None,
                                first_token_rel_cut=(first_tok[p["i"]] - t_cut) if p["i"] in first_tok else None,
                                rereg_rel_cut=(rereg[p["i"]] - t_cut) if p["i"] in rereg else None) for p in pz]
    out["client"] = m.get("client")
    out["routed_again"] = len(routed)
    if m.get("event_kind") in ("frps", "front"):
        rr = [routed[i] - t_cut for i in routed]
        rg = [reg_again[p] - t_cut for p in proxies]
        out["registered_s"] = dist(rg)
        out["routed_s"] = dist(rr)
        out["first_token_s"] = dist([first_tok[i] - t_cut for i in first_tok])
        out["not_back_60s"] = n - sum(1 for x in rr if x <= RELOGIN_RULE_S)
    # The plugins' latency over the recovery: from 1 s before the cut until the post window ends
    # (at most 90 s after the cut), so the teardown's CloseProxy calls stay out.
    lat = {}
    t_end = min(t_cut + 90, m.get("post_end") or t_cut + 90)
    for c in calls:
        s = c["start_ns"] / 1e9
        if t_cut - 1 <= s <= t_end:
            key = "%s %s" % (c["plugin"], c.get("op") or "?")
            lat.setdefault(key, []).append((c["end_ns"] - c["start_ns"]) / 1e6)
    out["plugin_ms"] = {k: dict(dist(v), over_rule=sum(1 for x in v if x > PLUGIN_RULE_MS)) for k, v in sorted(lat.items())}
    out["refused"] = {}
    for c in calls:
        if c.get("reject") and c["start_ns"] / 1e9 <= t_end:
            k = "%s %s" % (c["plugin"], c.get("op"))
            out["refused"][k] = out["refused"].get(k, 0) + 1
    # The front host's CPU (frps, the admission plugin and the front) over the pre-window and over
    # the RECOVER_S after the cut, and its busiest sample interval there.
    samples = run.samples()
    for name, (a, b) in (("pre", (pre0, pre1)), ("recover", (t_cut, t_cut + RECOVER_S))):
        tot = 0.0
        for role in FRONT_HOST_ROLES:
            c = cores(samples, role, a, b)
            out["%s_%s_cores" % (name, role)] = c
            tot += c or 0
        out["%s_front_host_cores" % name] = tot
        out["%s_host_busy" % name] = host_busy(samples, a, b)
    out["recover_peak_front_host_cores"] = peak_cores(samples, FRONT_HOST_ROLES, t_cut, t_cut + RECOVER_S)
    out["recover_peak_host_busy"] = peak_busy(samples, t_cut, t_cut + RECOVER_S)
    life = run.lines("front-life.jsonl")
    if m.get("event_kind") == "front":
        terms = [e["t_ns"] / 1e9 for e in life if e["event"] == "term" and e["t_ns"] / 1e9 >= t_ev]
        out["front_term_to_exit_s"] = (t_cut - min(terms)) if terms else None
    out["frpc_log"] = m.get("frpc_log")
    out["valid"] = bool(valid and len(inflight) == n)
    return out


# --- the front's pauses (HBEP-5, Load bound and cap, Pauses) ---------------------------------------

LOG_RE = re.compile(r'(\w+)=("(?:[^"\\]|\\.)*"|\S+)')
INSTANCE_RE = re.compile(r"/md-(\d+)-0$")
PAUSE_S = 30.0  # HBEP-5's pauseDuration


def parse_ts(s):
    """An RFC 3339 UTC time as the front writes it (2026-11-11T22:25:48.153Z), Unix seconds."""
    from datetime import datetime, timezone
    s = s.rstrip("Z")
    fmt = "%Y-%m-%dT%H:%M:%S.%f" if "." in s else "%Y-%m-%dT%H:%M:%S"
    return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc).timestamp()


def log_records(path):
    """The front's slog text lines as dicts (values unquoted)."""
    out = []
    try:
        with open(path, errors="replace") as fh:
            for line in fh:
                d = {}
                for k, v in LOG_RE.findall(line):
                    d[k] = json.loads(v) if v.startswith('"') else v
                if "time" in d:
                    d["t"] = parse_ts(d["time"])
                    out.append(d)
    except OSError:
        pass
    return out


def failure_kind(err):
    """What a failed connection to an Instance was, from the front's log: frps down (its tcpmux
    port refused the dial), frps up with no proxy for the UID (404), a dial the epoch check aborted
    (which HBEP-5 does not count), a time-out, or other."""
    e = err or ""
    if "connection refused" in e:
        return "dial refused (frps down)"
    if "404" in e:
        return "set-up 404 (no proxy yet)"
    if "epoch" in e or "registration" in e:
        return "epoch moved (not counted)"
    if "timeout" in e or "deadline" in e:
        return "set-up time-out"
    return "other: " + re.sub(r"[0-9]+", "#", e)[:60]


def pauses(run, t_cut, rereg):
    """Per Instance: whether the front paused it after the cut, when (relative to the cut and to
    its re-registration), until when, and the failures that counted toward it, each with its time.
    rereg maps a tunnel index to its re-registration time (the front's plugin's accepted NewProxy)."""
    recs = log_records(os.path.join(run.dir, "front.log"))
    usage = {u.get("requestID"): u for u in run.lines("usage.jsonl")}

    def idx(name):
        m = INSTANCE_RE.search(name or "")
        return int(m.group(1)) if m else None

    fails, pause = {}, {}
    for r in recs:
        if r["t"] < t_cut - 1:
            continue
        msg = r.get("msg", "")
        if msg == "target paused after failed connections":
            i = idx(r.get("target"))
            if i is not None and i not in pause:
                pause[i] = {"start": r["t"], "until": parse_ts(r["until"]) if r.get("until") else None}
        elif msg.startswith("no connection to the target"):
            i = idx(r.get("target"))
            if i is not None:
                fails.setdefault(i, []).append((r["t"], failure_kind(r.get("error"))))
        elif msg == "stream cut" and r.get("cutBy") in ("upstream", "tunnel"):
            u = usage.get(r.get("request")) or {}
            i = idx(u.get("instance"))
            if i is not None:
                fails.setdefault(i, []).append((r["t"], "stream cut (cutBy %s)" % r.get("cutBy")))
    for u in usage.values():
        if u.get("emitter") == "instance" and (u.get("status") or 0) >= 500:
            i = idx(u.get("instance"))
            if i is not None:
                t = parse_ts(u["endTime"]) if u.get("endTime") else None
                if t and t >= t_cut - 1:
                    fails.setdefault(i, []).append((t, "5xx from the Instance"))
    out = []
    for i in range(run.n):
        p = pause.get(i)
        fl = sorted(fails.get(i, []))
        rr = rereg.get(i)
        # The failures logged up to 50 ms after the pause line: the dial that tips the count and
        # its resend are logged around it.
        counted = [f for f in fl if p is None or f[0] <= p["start"] + 0.05]
        out.append({"i": i, "paused": p is not None,
                    "pause_start_rel_cut": (p["start"] - t_cut) if p else None,
                    "pause_start_rel_rereg": (p["start"] - rr) if p and rr else None,
                    "pause_until_rel_cut": (p["until"] - t_cut) if p and p["until"] else None,
                    "failures": [{"kind": k, "rel_cut": t - t_cut, "rel_rereg": (t - rr) if rr else None} for t, k in counted]})
    return out


# --- item 2: the capped link ----------------------------------------------------------------------

def heartbeat_witnesses(run, t_ready, t1):
    """S14's witnesses that apply in direct mode, after every tunnel was routed:
    - drops: a proxy frps's list (read every second) showed not online, or registered again (a new
      lastStartTime), after readiness;
    - frpc re-logins: login successes in the frpc logs beyond one per tunnel;
    - heartbeat-timeout lines in frps's and the frpc logs;
    plus the front's failed probes after readiness."""
    drops, last = 0, {}
    unreadable = 0
    for p in run.polls():
        if p["t"] < t_ready or p["t"] > t1:
            continue
        if "error" in p:
            unreadable += 1
            continue
        seen = {}
        for i, status, start, _close in p["proxies"]:
            if i >= 0:
                seen[i] = (status, start)
        for i in range(run.n):
            st = seen.get(i)
            if st is None or st[0] != "online":
                drops += 1
            elif i in last and last[i][1] != st[1]:
                drops += 1
            if st:
                last[i] = st
    fl = run.m.get("frpc_log") or {}
    fs = run.m.get("frps_log") or {}
    probes_failed = sum(1 for p in run.lines("probes.jsonl") if t_ready < p.get("t_ns", 0) / 1e9 <= t1 and p.get("pass") is False)
    return {"drops": drops, "unreadable_lists": unreadable,
            "frpc_relogins": max(0, fl.get("login to server success", 0) - run.n),
            "heartbeat_timeout_lines": fl.get("heartbeat timeout", 0) + fs.get("heartbeat timeout", 0),
            "probes_failed": probes_failed}


def link_fill(run, t0, t1):
    """The link's per-second bytes inside the window against its cap, each direction: the
    smallest second's share of the cap. "At its cap" needs every second at CAP_FILL or more."""
    rate = run.m.get("rate_Bps") or 0
    rows = [r for r in run.lines("link-series.jsonl") if t0 + 1 <= r["t"] <= t1]
    if not rate or not rows:
        return {"seconds": len(rows), "up_min": None, "down_min": None, "at_cap": False}
    up = [r["up_bytes"] / rate for r in rows]
    down = [r["down_bytes"] / rate for r in rows]
    return {"seconds": len(rows), "up_min": min(up), "down_min": min(down), "up_mean": sum(up) / len(up),
            "down_mean": sum(down) / len(down), "up_queue_max": max(r.get("up_queue_max", 0) for r in rows),
            "down_queue_max": max(r.get("down_queue_max", 0) for r in rows),
            "at_cap": min(up) >= CAP_FILL and min(down) >= CAP_FILL}


def caplink(run):
    m = run.m
    t0, t1 = m.get("window_start"), m.get("window_end")
    out = {"run_id": m.get("run_id"), "n": run.n, "streams": m.get("streams"), "cap_mbps": m.get("cap_mbps"),
           "discipline": m.get("discipline"), "buffer": m.get("buffer"), "status": m.get("status")}
    if t0 is None:
        out["valid"] = False
        return out
    load = run.load()
    rate = TOKEN_RATE.get(m.get("token_delay_ms"), 20.0)
    offered = run.n * m["streams"] * rate
    delivered = tokens_per_s(load, t0, t1)
    valid, emptiest, empty = tunnel_validity(load, run.n, t0, t1)
    g = gaps_ms(load, t0, t1)
    ready = [e["t"] for e in m["events"] if e["event"] == "all-routed"]
    hb = heartbeat_witnesses(run, ready[0] if ready else t0, t1)
    series = [r for r in run.lines("link-series.jsonl") if t0 + 1 <= r["t"] <= t1]
    up = sum(r["up_bytes"] for r in series)
    secs = len(series)
    out.update(offered_tok_s=offered, delivered_tok_s=delivered, delivered_share=delivered / offered if offered else None,
               up_bytes_per_token=(up / secs) / delivered if secs and delivered and not m.get("filler_conns") else None,
               valid=valid, emptiest_second=emptiest, empty_tunnels=len(empty), gap_p50_ms=percentile(g, 50),
               gap_p99_ms=percentile(g, 99), gap_max_ms=max(g) if g else None, **hb)
    out.update({"link_" + k: v for k, v in link_fill(run, t0, t1).items()})
    fsrc = run.json("filler-src.json") or {}
    out["filler_sent_bytes"], out["filler_recv_bytes"] = fsrc.get("sent_bytes"), fsrc.get("recv_bytes")
    samples = run.samples()
    out["host_busy"] = host_busy(samples, t0, t1)
    out["hb_failed"] = hb["drops"] + hb["frpc_relogins"] + hb["heartbeat_timeout_lines"] > 0
    return out


def demand_binds(offered_tok_s, bytes_per_token, rate_Bps):
    """The tunnels' own demand binds when the token bytes they offer (tokens/s times the bytes per
    token the uncapped control carried up the link) reach the cap."""
    return bool(rate_Bps and bytes_per_token and offered_tok_s * bytes_per_token >= rate_Bps)


def caplink_stop(run_dir, bytes_per_token):
    """Whether a discipline's ladder stops at this run: its first heartbeat failure, or the cap
    where the tunnels' own demand binds (both included in the ladder), or a run that failed."""
    r = caplink(Run(run_dir))
    binds = demand_binds(r.get("offered_tok_s") or 0, bytes_per_token, Run(run_dir).m.get("rate_Bps"))
    stop = r.get("status") != "ok" or r.get("hb_failed") or binds
    return {"stop": bool(stop), "hb_failed": r.get("hb_failed"), "demand_binds": binds,
            "at_cap": r.get("link_at_cap"), "status": r.get("status")}


# --- item 3: R_ssh --------------------------------------------------------------------------------

KEX_RE = re.compile(r"kex: algorithm: (\S+)")
CIPHER_RE = re.compile(r"kex: (server->client|client->server) cipher: (\S+) MAC: (\S+) compression: (\S+)")


def ssh_negotiated(logdir):
    """The KEX, cipher and MAC each relay's ssh negotiated, from its -v log, with counts."""
    out = {}
    for p in sorted(glob.glob(os.path.join(logdir, "ssh-*.log"))):
        kex, ciphers = None, []
        try:
            with open(p, errors="replace") as fh:
                for line in fh:
                    m = KEX_RE.search(line)
                    if m:
                        kex = m.group(1)
                    m = CIPHER_RE.search(line)
                    if m:
                        ciphers.append("%s %s %s %s" % m.groups())
        except OSError:
            continue
        if kex is None and not ciphers:
            continue  # an ssh that negotiated nothing: sshrun's "ssh -G" for the login host's name
        key = "kex=%s; %s" % (kex, "; ".join(sorted(set(ciphers))))
        out[key] = out.get(key, 0) + 1
    return out


def rssh(run):
    m = run.m
    samples = run.samples()
    relays = 2 * run.n
    out = {"run_id": m.get("run_id"), "cipher_asked": m.get("cipher_asked"), "status": m.get("status"),
           "negotiated": m.get("negotiated"), "relays": relays, "levels": []}
    if m.get("idle_start"):
        a, b = m["idle_start"], m["idle_end"]
        out["idle_ssh_cores"] = cores(samples, "ssh", a, b)
        out["idle_sshd_cores"] = cores(samples, "sshd", a, b)
        out["idle_ssh_cores_per_relay"] = (out["idle_ssh_cores"] or 0) / relays
    for lv in m.get("levels", []):
        a, b = lv["window_start"], lv["window_end"]
        load = run.load(lv["load"] + ".jsonl")
        q = tokens_per_s(load, a, b) / 1000.0
        valid, emptiest, _ = tunnel_validity(load, run.n, a, b)
        c_ssh, c_sshd = cores(samples, "ssh", a, b), cores(samples, "sshd", a, b)
        out["levels"].append({"streams": lv["streams"], "ktok_s": q, "valid": valid, "emptiest_second": emptiest,
                              "ssh_cores": c_ssh, "sshd_cores": c_sshd,
                              "ssh_cores_per_ktok": (c_ssh / q) if c_ssh is not None and q > 0 else None,
                              "host_busy": host_busy(samples, a, b)})
    pts = [(lv["ktok_s"], lv["ssh_cores"]) for lv in out["levels"] if lv["valid"] and lv["ssh_cores"] is not None]
    if m.get("idle_start") and out.get("idle_ssh_cores") is not None:
        pts.append((0.0, out["idle_ssh_cores"]))
    out["fit"] = linfit(pts)
    return out


def linfit(pts):
    """Least squares y = slope * x + intercept, with the points it used."""
    if len(pts) < 2:
        return None
    n = len(pts)
    mx = sum(p[0] for p in pts) / n
    my = sum(p[1] for p in pts) / n
    sxx = sum((p[0] - mx) ** 2 for p in pts)
    if sxx == 0:
        return None
    slope = sum((p[0] - mx) * (p[1] - my) for p in pts) / sxx
    return {"slope": slope, "intercept": my - slope * mx, "n": n}


# --- item 4: R_front ------------------------------------------------------------------------------

def rfront(run):
    m = run.m
    t0, t1 = m.get("window_start"), m.get("window_end")
    out = {"run_id": m.get("run_id"), "n": run.n, "streams": m.get("streams"), "via": m.get("via"), "status": m.get("status")}
    if t0 is None:
        out["valid"] = False
        return out
    load = run.load()
    samples = run.samples()
    valid, emptiest, _ = tunnel_validity(load, run.n, t0, t1)
    out.update(valid=valid, emptiest_second=emptiest, ktok_s=tokens_per_s(load, t0, t1) / 1000.0,
               host_busy=host_busy(samples, t0, t1))
    for role in ("front", "frps", "admit", "link", "load", "node", "frpc"):
        out[role + "_cores"] = cores(samples, role, t0, t1)
    g = gaps_ms(load, t0, t1)
    out["gap_p99_ms"] = percentile(g, 99)
    return out


def rfront_pairs(rows):
    """R_front per level: the front process's cores with the client through its proxy, less the
    same with the client straight to frps's tcpmux port (the front still running), per 1,000
    tokens/s of the proxied run."""
    by = {}
    for r in rows:
        by.setdefault((r["n"], r["streams"], r.get("rep")), {})[r["via"]] = r
    out = []
    for (n, s, rep), d in sorted(by.items()):
        f, t = d.get("front"), d.get("tcpmux")
        if not f or not t or not f.get("valid") or not t.get("valid") or f.get("front_cores") is None or t.get("front_cores") is None:
            continue
        delta = f["front_cores"] - t["front_cores"]
        out.append({"n": n, "streams": s, "rep": rep, "ktok_s": f["ktok_s"], "front_cores_via_front": f["front_cores"],
                    "front_cores_via_tcpmux": t["front_cores"], "delta_cores": delta,
                    "r_front_per_ktok": delta / f["ktok_s"] if f["ktok_s"] > 0 else None})
    return out
