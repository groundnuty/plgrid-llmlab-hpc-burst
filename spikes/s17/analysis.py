"""Spike S17: the result rules, apart from the harness so they are unit-tested on their own
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


OVERLAP_MIN = 0.8  # the least share of its shortest span that a batch's transfers must have in common


def batch_overlap(trs):
    """Per batch of concurrent transfers (the load client's `batch`), the share of its shortest transfer's
    span that every transfer of the batch has in common: (earliest end - latest start) over the shortest
    span, negative when two did not overlap at all. A batch of one is 1. Returns {batch: fraction}."""
    by = {}
    for tr in trs:
        by.setdefault(tr.get("batch", 0), []).append(transfer_window(tr))
    out = {}
    for b, spans in by.items():
        if len(spans) < 2:
            out[b] = 1.0
            continue
        shortest = min(e - s for s, e in spans)
        common = min(e for _, e in spans) - max(s for s, _ in spans)
        out[b] = common / shortest if shortest > 0 else 0.0
    return out


def batch_metrics(trs, concurrent):
    """Each batch's time (the first start to the last end) and aggregate rate (the bytes of every transfer over
    that time), and the overlap shares: how long the transfers took together."""
    ok = [tr for tr in trs if not tr.get("err") and tr.get("status") == 200]
    by = {}
    for tr in ok:
        by.setdefault(tr.get("batch", 0), []).append(tr)
    times, rates = [], []
    for b, group in by.items():
        if len(group) != concurrent:
            continue
        start = min(tr["start_ns"] for tr in group)
        end = max(max(tr["end_ns"], tr.get("recv_last_ns") or 0) for tr in group)
        if end > start:
            times.append((end - start) / 1e9)
            rates.append(sum(tr["bytes"] for tr in group) / ((end - start) / 1e9))
    ov = list(batch_overlap(trs).values())
    out = {"batches": len(by), "batch_time_median_s": round(statistics_median(times), 4) if times else "",
           "batch_rate_median": round(statistics_median(rates)) if rates else "",
           "overlap_min": round(min(ov), 4) if ov else "", "overlap_median": round(statistics_median(ov), 4) if ov else ""}
    return out


def validity(load, kind, size, concurrent=1):
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
    if concurrent > 1:
        # A concurrent run proves nothing if its transfers did not overlap in time (issue #413).
        for b, frac in sorted(batch_overlap(trs).items()):
            n_in = sum(1 for tr in trs if tr.get("batch", 0) == b)
            if n_in != concurrent:
                return dict(valid=False, reason="batch %d has %d of %d transfers" % (b, n_in, concurrent), in_flight_all=False)
            if frac < OVERLAP_MIN:
                return dict(valid=False, reason="batch %d's transfers overlapped %.0f%% of the shortest's span, under %.0f%%" % (b, 100 * frac, 100 * OVERLAP_MIN), in_flight_all=False)
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


def validity_many(load):
    """The many-Instances run is valid when every bulk request completed in full and ended before its
    Instance's next was due (so the requests were made at the set rate, never queued behind one another),
    and every token stream had one request in flight that delivered tokens before the first bulk request
    and after the last one ended (so every stream was streaming through every request)."""
    bulk = load["bulk"]
    if not bulk:
        return dict(valid=False, reason="no bulk request ran", in_flight_all=False)
    for i, r in enumerate(bulk):
        if r.get("err") or r.get("status") != 200 or r.get("bytes") != load["bytes"]:
            return dict(valid=False, reason="bulk request %d failed: %s" % (i, r.get("err") or r.get("status")), in_flight_all=False)
        if r["end_ns"] - r["start_ns"] >= load["interval_ns"]:
            return dict(valid=False, reason="bulk request %d took %.1f s, not under the %.1f s interval" % (
                i, (r["end_ns"] - r["start_ns"]) / 1e9, load["interval_ns"] / 1e9), in_flight_all=False)
    ws = min(r["start_ns"] for r in bulk)
    we = max(r["end_ns"] for r in bulk)
    for j, rs in enumerate(load["streams_runs"]):
        ok = any(r["start_ns"] < ws and r["end_ns"] > we and r["tokens_ns"] and r["tokens_ns"][0] < ws and r["tokens_ns"][-1] > we
                 for r in rs)
        if not ok:
            return dict(valid=False, reason="stream %d was not streaming through the bulk requests" % j, in_flight_all=False)
    return dict(valid=True, reason="", in_flight_all=True)


def many_metrics(load):
    """Time to first byte (from before the dial to the first body byte) and total time of the bulk requests,
    and the streams' inter-token gaps inside the window (both tokens in it), pooled."""
    bulk = [r for r in load["bulk"] if not r.get("err") and r.get("status") == 200 and r.get("bytes") == load["bytes"]]
    ttfb = [(r["first_ns"] - r["start_ns"]) / 1e6 for r in bulk if r.get("first_ns")]
    total = [(r["end_ns"] - r["start_ns"]) / 1e6 for r in bulk]
    out = {"bulk_requests": len(load["bulk"]), "bulk_ok": len(bulk)}
    for label, xs in (("ttfb", ttfb), ("total", total)):
        for p in (50, 99):
            v = pct(xs, p)
            out["%s_p%d_ms" % (label, p)] = round(v, 3) if v is not None else ""
        out["%s_max_ms" % label] = round(max(xs), 3) if xs else ""
    w0, w1 = load["window_start_ns"], load["window_end_ns"]
    gaps = []
    for rs in load["streams_runs"]:
        for r in rs:
            tk = r["tokens_ns"]
            for a, b in zip(tk, tk[1:]):
                if w0 <= a and b <= w1:
                    gaps.append((b - a) / 1e6)
    out["gap_n"] = len(gaps)
    for p in (50, 99):
        v = pct(gaps, p)
        out["gap_p%d_ms" % p] = round(v, 3) if v is not None else ""
    out["gap_max_ms"] = round(max(gaps), 3) if gaps else ""
    return out


def handshake_rates(stats, t_from, t_to):
    """TLS connections accepted and ClientHellos received per second between two times (unix s), from the
    dial-home listener's counter rows (unix ns, accepted, hellos), taken every 500 ms. None when the window
    holds fewer than two rows."""
    rows = [r for r in stats if t_from <= r[0] / 1e9 <= t_to]
    if len(rows) < 2:
        return None
    dt = (rows[-1][0] - rows[0][0]) / 1e9
    if dt <= 0:
        return None
    return {"accepts_per_s": (rows[-1][1] - rows[0][1]) / dt, "hellos_per_s": (rows[-1][2] - rows[0][2]) / dt,
            "accepts": rows[-1][1] - rows[0][1], "hellos": rows[-1][2] - rows[0][2], "seconds": dt}
