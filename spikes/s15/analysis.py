"""Spike S15: the result rules, apart from the harness so they are unit-tested on their own
(tests/test_analysis.py). Nothing here reads a process or a clock. window_samples, cores, peak,
busy_cores, drops, logins and host_bound are spike S14's (spikes/s14/analysis.py at 3f863bad)."""
import math

TOKEN_MS = 50.0  # the engine double's pace: one token per 50 ms


def pct(xs, p):
    """Nearest-rank percentile of xs (None when empty)."""
    if not xs:
        return None
    s = sorted(xs)
    k = max(1, min(len(s), int(math.ceil(p / 100.0 * len(s)))))
    return s[k - 1]


def transfer_window(tr):
    """A transfer's span on the client's timeline (ns): from before its dial to the later of the client's
    end and the receiver's last byte."""
    return tr["start_ns"], max(tr["end_ns"], tr.get("recv_last_ns") or 0)


def transfer_ok(tr, kind, size):
    """A transfer counts only if it completed: status 200, no error, every byte at the receiver (an
    embeddings reply: its whole Content-Length, which the client checks)."""
    if tr.get("err") or tr.get("status") != 200:
        return False
    if kind in ("upload", "download"):
        return tr.get("bytes") == size
    return (tr.get("bytes") or 0) > 0


def validity(load, kind, size):
    """A configuration is valid when every transfer completed and, through every transfer's whole span,
    every one of the streams had a request in flight that delivered tokens before and after it (no
    stream paused or between requests), and in the reference window every stream received a token in
    every second. Empty seconds inside a transfer are the stall being measured, not a pause, so they do
    not invalidate it (the reasoning sent to the parent, 10 Nov 2026)."""
    runs = load["streams_runs"]
    if len(runs) != load["streams"]:
        return dict(valid=False, reason="%d of %d streams reported" % (len(runs), load["streams"]), in_flight_all=False)
    trs = load["transfers"]
    if kind != "none":
        for i, tr in enumerate(trs):
            if not transfer_ok(tr, kind, size):
                return dict(valid=False, reason="transfer %d failed: %s" % (i, tr.get("err") or tr.get("status")), in_flight_all=False)
    for i, tr in enumerate(trs):
        ws, we = transfer_window(tr)
        for j, rs in enumerate(runs):
            ok = any(r["start_ns"] < ws and r["end_ns"] > we and r["tokens_ns"] and r["tokens_ns"][0] < ws
                     and r["tokens_ns"][-1] > we for r in rs)
            if not ok:
                return dict(valid=False, reason="stream %d was not streaming through transfer %d" % (j, i), in_flight_all=False)
    rs0, re0 = load["reference_start_ns"], load["reference_end_ns"]
    nb = int((re0 - rs0) // 1_000_000_000)
    for j, rs in enumerate(runs):
        buckets = [0] * nb
        for r in rs:
            for t in r["tokens_ns"]:
                if rs0 <= t < rs0 + nb * 1_000_000_000:
                    buckets[(t - rs0) // 1_000_000_000] += 1
        if nb and min(buckets) == 0:
            return dict(valid=False, reason="stream %d had a second with no token in the reference window" % j, in_flight_all=False)
    return dict(valid=True, reason="", in_flight_all=True)


def gap_stats(load):
    """The streams' inter-token gaps (ms): during a transfer (a gap whose interval overlaps a transfer's
    span, S1c's attribution) and in the reference window (both tokens inside it)."""
    spans = [transfer_window(tr) for tr in load["transfers"]]
    rs0, re0 = load["reference_start_ns"], load["reference_end_ns"]
    bulk, ref = [], []
    for rs in load["streams_runs"]:
        for r in rs:
            tk = r["tokens_ns"]
            for a, b in zip(tk, tk[1:]):
                g = (b - a) / 1e6
                if any(a < we and b > ws for ws, we in spans):
                    bulk.append(g)
                elif rs0 <= a and b <= re0:
                    ref.append(g)
    out = {"gap_ref_n": len(ref), "gap_bulk_n": len(bulk)}
    for label, xs in (("ref", ref), ("bulk", bulk)):
        for p in (50, 99):
            v = pct(xs, p)
            out["gap_%s_p%d_ms" % (label, p)] = round(v, 3) if v is not None else ""
        out["gap_%s_max_ms" % label] = round(max(xs), 3) if xs else ""
    out["stall_max_ms"] = round(max(bulk) - TOKEN_MS, 3) if bulk else ""
    return out


def transfer_metrics(trs, kind, size):
    """Each completed transfer's time and rate at the receiver, its first second's bytes (a rate cap's
    burst) and the rate after it; for uploads, how long after the client's last write the receiver had
    the last byte (what a front-side timing would miss)."""
    ok = [tr for tr in trs if transfer_ok(tr, kind, size)]
    out = {"transfers": len(trs), "transfers_ok": len(ok)}
    rates, times, totals, bursts, steady, lag = [], [], [], [], [], []
    for tr in ok:
        d = (tr["recv_last_ns"] - tr["recv_first_ns"]) / 1e9
        if d > 0:
            rates.append(tr["bytes"] / d)
            times.append(d)
        totals.append((tr["end_ns"] - tr["start_ns"]) / 1e9)
        steps = tr.get("steps_bytes") or []
        if len(steps) > 10 and d > 1.0:
            bursts.append(steps[9])
            steady.append((tr["bytes"] - steps[9]) / (d - 1.0))
        if kind == "upload" and tr.get("sent_ns"):
            lag.append((tr["recv_last_ns"] - tr["sent_ns"]) / 1e6)
    med = lambda xs: statistics_median(xs)  # noqa: E731
    out["recv_rate_median"] = round(med(rates)) if rates else ""
    out["recv_rate_min"] = round(min(rates)) if rates else ""
    out["recv_rate_max"] = round(max(rates)) if rates else ""
    out["recv_time_median_s"] = round(med(times), 4) if times else ""
    out["total_time_median_s"] = round(med(totals), 4) if totals else ""
    out["first_second_bytes_median"] = round(med(bursts)) if bursts else ""
    out["after_first_second_rate_median"] = round(med(steady)) if steady else ""
    out["upload_recv_last_after_sent_ms_median"] = round(med(lag), 3) if lag else ""
    out["bulk_bytes_median"] = round(med([tr["bytes"] for tr in ok])) if ok else ""
    return out


def statistics_median(xs):
    s = sorted(xs)
    n = len(s)
    return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2


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
