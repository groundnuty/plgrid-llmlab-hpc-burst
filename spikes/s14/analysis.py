"""Spike S14: the result rules, kept apart from the harness so they are unit-tested on their own
(tests/test_analysis.py). Nothing here reads a process or a clock."""

# A configuration is valid only when every tunnel streamed throughout the window: every tunnel
# received at least one token in every bucket (1 s) of the window. Registered tunnels that carry no
# stream prove nothing (issue #313), so one silent bucket on one tunnel invalidates the whole
# configuration.


def tunnel_validity(per_tunnel, tunnels):
    """per_tunnel: the load client's per_tunnel list. Returns a dict with valid (bool), the count
    of tunnels with no token at all, the count with at least one empty bucket, and the token and
    request counts' minimum and maximum over tunnels."""
    if len(per_tunnel) != tunnels:
        return dict(valid=False, reason="the client reported %d of %d tunnels" % (len(per_tunnel), tunnels),
                    silent=tunnels, gapped=tunnels, tokens_min=0, tokens_max=0, requests_min=0, requests_max=0)
    silent = sum(1 for t in per_tunnel if t["tokens"] == 0)
    gapped = sum(1 for t in per_tunnel if not t["buckets"] or min(t["buckets"]) == 0)
    toks = [t["tokens"] for t in per_tunnel]
    reqs = [t["requests"] for t in per_tunnel]
    reason = ""
    if silent:
        reason = "%d tunnels received no token in the window" % silent
    elif gapped:
        reason = "%d tunnels had a %s-s bucket with no token" % (gapped, "1")
    return dict(valid=not silent and not gapped, reason=reason, silent=silent, gapped=gapped,
                tokens_min=min(toks), tokens_max=max(toks), requests_min=min(reqs), requests_max=max(reqs))


def window_samples(samples, start, end):
    """The samples taken inside [start, end], in time order."""
    return [s for s in samples if start <= s["t"] <= end]


def cores(samples, role, clk_tck):
    """CPU of a role in cores over the samples (already cut to the window): for each PID, the ticks
    between the first and the last sample it appears in, summed, over the time between the first
    and the last sample. A PID that exits inside the window loses its ticks after its last sample;
    the roles measured keep their processes for the whole run."""
    if len(samples) < 2:
        return None
    dt = samples[-1]["t"] - samples[0]["t"]
    if dt <= 0:
        return None
    first, last = {}, {}
    for s in samples:
        for pid, ticks in s["ticks"].get(role, {}).items():
            first.setdefault(pid, ticks)
            last[pid] = ticks
    if not last:
        return None
    return sum(last[p] - first[p] for p in last) / clk_tck / dt


def peak(samples, role, key):
    """The largest value of a per-role sum (rss_kb, fds) over the samples."""
    vals = [s[key].get(role) for s in samples if s[key].get(role) is not None]
    return max(vals) if vals else None


def busy_cores(stat_first, stat_last):
    """Cores busy on the whole host between two /proc/stat cpu lines (lists of jiffies: user nice
    system idle iowait irq softirq steal ...), as a fraction of all and in cores."""
    d = [b - a for a, b in zip(stat_first, stat_last)]
    total = sum(d[:8])
    if total <= 0:
        return None
    idle = d[3] + d[4]
    return 1.0 - idle / total


def drops(polls, uids, after):
    """Tunnels that dropped after `after` (the readiness time), from frps's proxy list polled every
    10 s: a proxy seen not online or absent, or one whose lastStartTime changed (it registered
    again). polls: [{"t": time, "proxies": {name: {"status", "lastStartTime"}}}]. Returns the set of
    UIDs that dropped, and how many polls could not be read."""
    seen_start, dropped, unreadable = {}, set(), 0
    for p in polls:
        if p["t"] < after:
            continue
        if p.get("error"):
            unreadable += 1
            continue
        for uid in uids:
            pr = p["proxies"].get(uid)
            if pr is None or pr.get("status") != "online":
                dropped.add(uid)
                continue
            st = pr.get("lastStartTime")
            if uid in seen_start and seen_start[uid] != st:
                dropped.add(uid)
            seen_start[uid] = st
    return dropped, unreadable


def logins(text):
    """frpc's successful logins in its log: more than one means its session dropped and came back."""
    return text.count("login to server success")


def host_bound(busy_fraction, threshold=0.85):
    """A configuration whose host was this busy measures the host, not the tunnel: its latency is
    reported but flagged."""
    return busy_fraction is not None and busy_fraction >= threshold
