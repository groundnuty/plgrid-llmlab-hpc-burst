"""Spike S17: the result rules, apart from the harness so they are unit-tested on their own
(tests/test_analysis.py). Nothing here reads a process or a clock. window_samples, cores, peak,
busy_cores, drops, logins and host_bound are spike S14's (spikes/s14/analysis.py at 3f863bad)."""
import math
import re

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


def batch_spans(trs):
    """Per batch of concurrent transfers: (first start, last end) in ns, where a transfer's end is the later of the
    client's end and the receiver's last byte (transfer_window)."""
    by = {}
    for tr in trs:
        ws, we = transfer_window(tr)
        b = tr.get("batch", 0)
        lo, hi = by.get(b, (ws, we))
        by[b] = (min(lo, ws), max(hi, we))
    return by


def late_batch_stalls(load, window_ms=400, min_gap_ms=100, min_streams=6):
    """Stalls of the token streams near the end of a batch of transfers (issue #413's review: every ssh run that
    moved 1 GiB through the relay's one ssh connection stalled all its streams just before the batch that crossed
    it ended). For each batch, the streams that have a gap of more than min_gap_ms inside the batch whose end falls
    in the batch's last window_ms; a batch counts when at least min_streams streams do. Returns {batch: {streams,
    min_ms, max_ms, before_end_ms, gib}}: the streams' count, the stall's smallest and largest gap, how long before
    the batch's end the last of those gaps ended, and the bytes the transfers had moved by the batch's end, in GiB."""
    spans = batch_spans(load["transfers"])
    out = {}
    for b, (bs, be) in sorted(spans.items()):
        found, ends = [], []
        for rs in load["streams_runs"]:
            best = None
            for r in rs:
                tk = r["tokens_ns"]
                for a, z in zip(tk, tk[1:]):
                    g = (z - a) / 1e6
                    if g > min_gap_ms and a >= bs and be - window_ms * 1_000_000 <= z <= be:
                        if best is None or g > best[0]:
                            best = (g, z)
            if best:
                found.append(best[0])
                ends.append(best[1])
        if len(found) >= min_streams:
            done = sum(tr["bytes"] for tr in load["transfers"] if tr.get("batch", 0) <= b)
            out[b] = {"streams": len(found), "min_ms": min(found), "max_ms": max(found),
                      "before_end_ms": (be - max(ends)) / 1e6, "gib": done / (1 << 30)}
    return out


def batch_ttfb(trs):
    """Per batch, the transfers' times to first byte (from before the dial to the first byte at the receiver, ms)
    in increasing order: the first to answer first."""
    by = {}
    for tr in trs:
        if tr.get("err") or tr.get("status") != 200 or not tr.get("recv_first_ns"):
            continue
        by.setdefault(tr.get("batch", 0), []).append((tr["recv_first_ns"] - tr["start_ns"]) / 1e6)
    return {b: sorted(v) for b, v in by.items()}


# --- the relay's ssh: key exchanges and stalls (issue #413, the review of PR 416) -----------------------------------

_KEX = {
    "algorithm": re.compile(r"kex: algorithm: (\S+)"),
    "host_key": re.compile(r"kex: host key algorithm: (\S+)"),
    "in": re.compile(r"kex: server->client cipher: (\S+) MAC: (\S+) compression: (\S+)"),
    "out": re.compile(r"kex: client->server cipher: (\S+) MAC: (\S+) compression: (\S+)"),
}
_NEWKEYS = re.compile(r"ssh_set_newkeys: rekeying (in|out), input (\d+) bytes (\d+) blocks, output (\d+) bytes (\d+) blocks")


def parse_ssh_debug(text):
    """The facts of one relay client's debug log (ssh -v), and nothing else of it (the log names the host and the user):
    the client's and the server's version, the relay channel the command line ran ("control" or "serving"), each key
    exchange's negotiated algorithms (the first and every rekey), who started each exchange, the rekey limits in
    blocks, and, at each rekey, the bytes the connection had received and sent."""
    rec = {"local_version": "", "remote_version": "", "channel": "", "kex": [], "exchanges": 0, "initiator": [],
           "rekey_in": [], "rekey_out": [], "rekeys": []}
    first_kexinit = None
    for line in text.splitlines():
        m = re.search(r"Local version string SSH-2\.0-(.*)$", line)
        if m:
            rec["local_version"] = m.group(1).strip()
        m = re.search(r"remote software version (.*)$", line)
        if m:
            rec["remote_version"] = m.group(1).strip()
        if "Sending command:" in line:
            m = re.search(r"\brelay (control|serving)\b", line)
            if m:
                rec["channel"] = m.group(1)
        if "SSH2_MSG_KEXINIT sent" in line and first_kexinit is None:
            first_kexinit = "client"
        elif "SSH2_MSG_KEXINIT received" in line and first_kexinit is None:
            first_kexinit = "server"
        m = _KEX["algorithm"].search(line)
        if m:
            rec["kex"].append({"algorithm": m.group(1)})
            rec["initiator"].append(first_kexinit or "")
            first_kexinit = None
            continue
        if rec["kex"]:
            k = rec["kex"][-1]
            m = _KEX["host_key"].search(line)
            if m:
                k["host_key"] = m.group(1)
            m = _KEX["in"].search(line)
            if m:
                k["cipher_in"], k["mac_in"], k["compression_in"] = m.groups()
            m = _KEX["out"].search(line)
            if m:
                k["cipher_out"], k["mac_out"], k["compression_out"] = m.groups()
        if "SSH2_MSG_NEWKEYS received" in line:
            rec["exchanges"] += 1
        m = re.search(r"rekey in after (\d+) blocks", line)
        if m:
            rec["rekey_in"].append(int(m.group(1)))
        m = re.search(r"rekey out after (\d+) blocks", line)
        if m:
            rec["rekey_out"].append(int(m.group(1)))
        m = _NEWKEYS.search(line)
        if m:
            rec["rekeys"].append({"dir": m.group(1), "input_bytes": int(m.group(2)), "output_bytes": int(m.group(4))})
    return rec


STEP_NS = 100_000_000  # a download's received-bytes steps are 100 ms wide (the load client's StepMs)


def bytes_moved_at(load, t_ns):
    """The bytes the transfers had received by t_ns, as (low, high): a transfer's steps give its cumulative bytes at
    the end of each 100 ms after its first byte, so inside a step the count lies between the steps' two ends. A
    transfer with no steps counts only once it ended."""
    lo = hi = 0
    for tr in load["transfers"]:
        first, last, total = tr.get("recv_first_ns") or 0, tr.get("recv_last_ns") or 0, tr.get("bytes") or 0
        steps = tr.get("steps_bytes") or []
        if not first or t_ns < first:
            continue
        if t_ns >= last or not steps:
            lo += total if t_ns >= last else 0
            hi += total
            continue
        i = int((t_ns - first) // STEP_NS)
        lo += steps[i - 1] if 0 < i <= len(steps) else 0
        hi += steps[i] if i < len(steps) else total
    return lo, hi


def all_stream_stalls(load, min_gap_ms=100, min_streams=6, cluster_ms=300):
    """Stalls of the token streams that hit most of them at once while transfers ran: gaps of more than min_gap_ms
    whose starts lie within cluster_ms of one another, in at least min_streams distinct streams, from the first
    transfer's start to the last one's end. Returns a list, in time order, of {start_ns (the earliest gap's start),
    streams, min_ms, max_ms (the streams' longest gap each), moved_lo, moved_hi (the bytes the transfers had received
    then, bytes_moved_at)}."""
    spans = batch_spans(load["transfers"])
    if not spans:
        return []
    w0 = min(s for s, _ in spans.values())
    w1 = max(e for _, e in spans.values())
    gaps = []
    for si, rs in enumerate(load["streams_runs"]):
        for r in rs:
            tk = r["tokens_ns"]
            for a, z in zip(tk, tk[1:]):
                if (z - a) / 1e6 > min_gap_ms and w0 <= a <= w1:
                    gaps.append((a, z, si))
    gaps.sort()
    clusters = []
    for g in gaps:
        if clusters and g[0] - clusters[-1][0][0] <= cluster_ms * 1_000_000:
            clusters[-1].append(g)
        else:
            clusters.append([g])
    out = []
    for c in clusters:
        per = {}
        for a, z, si in c:
            per[si] = max(per.get(si, 0), (z - a) / 1e6)
        if len(per) >= min_streams:
            lo, hi = bytes_moved_at(load, c[0][0])
            out.append({"start_ns": c[0][0], "streams": len(per), "min_ms": min(per.values()), "max_ms": max(per.values()),
                        "moved_lo": lo, "moved_hi": hi})
    return out


MIB = 1 << 20
# A stall's bytes are the transfers' payload at the client; the rekey's are what the ssh connection had received. The
# connection is ahead of the client by what sits in the buffers between them (frp's 6 MiB window, the relay's, the
# sockets'), so a stall matches a rekey when its bytes lie from REKEY_BELOW below the connection's count to REKEY_ABOVE
# above it (the steps' resolution, a few MiB for four transfers, is inside the range the stall carries).
REKEY_BELOW, REKEY_ABOVE = 16 * MIB, 2 * MIB


def match_rekeys(stalls, rekey_bytes, below=REKEY_BELOW, above=REKEY_ABOVE):
    """Pairs each rekey (the bytes the connection had received when its new keys took effect) with the nearest
    unpaired all-stream stall (by its bytes' midpoint) whose bytes (moved_lo..moved_hi) overlap [rekey - below, rekey + above]. Returns
    (pairs, unmatched_rekeys, unmatched_stalls): pairs are (rekey_bytes, stall); the others are what found no partner."""
    free = list(stalls)
    pairs, lonely = [], []
    for rb in rekey_bytes:
        near = [st for st in free if st["moved_hi"] >= rb - below and st["moved_lo"] <= rb + above]
        hit = min(near, key=lambda st: abs((st["moved_lo"] + st["moved_hi"]) / 2 - rb), default=None)
        if hit is None:
            lonely.append(rb)
        else:
            free.remove(hit)
            pairs.append((rb, hit))
    return pairs, lonely, free
