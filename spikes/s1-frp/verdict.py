#!/usr/bin/env python3
"""PASS or FAIL per issue #18 criterion, from the watcher summary and the job's logs (spike S1).
Stdlib only, Python 3.6 compatible. Runs on the frps host after a full run.

stream_watch.py exits 0 whenever it collected a summary, even if streams were cut or a fault went
unseen: collection succeeded, the experiment may not have. This script is the check. It prints one
line per criterion and ends with OVERALL: PASS or OVERALL: FAIL (exit 0 or 1).

    verdict.py --summary summary-run.json --job-log job.export.log \\
               [--probe probe.jsonl] [--ssh-counters ssh-counters.json]

The criteria are those of issue #18, as the README lists them; the numbers are the heartbeat
budget fixed before any run:

  C1  frpc registered through the stub: "login to server success" in the job log, and a Login and a
      NewProxy call in the stub log
  C2  every stream completed, uncut, and lasted at least 3600 s. With stream_watch.py --streams N there
      are N streams (lanes); one cut or missing stream fails C2 whatever the others did. A summary
      without lane tags is one stream.
  C3  no stream cut over the hour: before the first mark, no cut, no token gap over the threshold,
      no failed stream open and no failed /health probe
  C4a frozen frpc (first mark): CloseProxy within 30 s, probe declared down within 10 s
  C4b engine killed (second mark): probe declared down within 10 s as no_response, no CloseProxy,
      Pings continued
  C4c frpc killed (third mark): CloseProxy within 5 s, probe declared down within 10 s
  C5  the transport did not fail for good (no "RESULT: TRANSPORT FAILED" in the job log), and frpc's login
      through it is recorded (probe frpc_login) or step 0 proved the path with an exchange

C4 reads the marks in the run-book's order. A mark taken while the probe was not 'up' is invalid and
fails its criterion: it is never counted as a detection.
"""
import argparse
import datetime
import json
import sys

MIN_STREAM_S = 3600
FREEZE_CLOSE_S = 30
KILL_CLOSE_S = 5
DOWN_S = 10


def parse_utc(text):
    return datetime.datetime.strptime(text, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=datetime.timezone.utc)


def before(text, limit):
    """True when the UTC stamp 'text' is earlier than the datetime 'limit'."""
    try:
        return parse_utc(text) < limit
    except (TypeError, ValueError):
        return True            # an unreadable time cannot be shown to be after the first mark


def num(value):
    return value if isinstance(value, (int, float)) else None


def c1_registered(summary, job_log):
    ops = (summary.get("stub") or {}).get("ops")
    if ops is None:
        return False, "the summary has no stub section (the watcher ran without --stub-log)"
    in_log = "login to server success" in job_log
    login, newproxy = ops.get("Login", 0), ops.get("NewProxy", 0)
    ok = in_log and login >= 1 and newproxy >= 1
    return ok, "job log login line: %s; stub Login %d, NewProxy %d" % ("yes" if in_log else "NO", login, newproxy)


def first_mark(summary):
    marks = summary.get("marks") or []
    if marks:
        return parse_utc(marks[0]["mark_utc"])
    return None


def lane_of(record):
    """The lane (which of the concurrent streams) a stream or cut record belongs to; untagged is lane 1."""
    return record.get("lane", 1)


def lane_count(summary):
    """How many streams the run was told to keep open: the declared count, else the highest lane seen."""
    declared = summary.get("streams_requested")
    if isinstance(declared, int) and not isinstance(declared, bool) and declared >= 1:
        return declared
    seen = [lane_of(r) for r in summary.get("streams", [])]
    return max(seen or [1])


def lane_problems(summary, lane, limit):
    """Why this lane fails C2, as a list of phrases (empty: it passes). A lane passes when it has a
    completed stream of at least MIN_STREAM_S and no cut before the first mark (limit; None = no mark,
    so every cut counts). A lane with no record at all never opened a stream."""
    records = [s for s in summary.get("streams", []) if lane_of(s) == lane]
    if not records:
        return ["no stream record (it never delivered a token)"]
    cut_records = [s for s in records if not s.get("completed") and not s.get("interrupted")
                   and (limit is None or before(s.get("end_utc"), limit))]
    cut_entries = [c for c in summary.get("cuts", []) if lane_of(c) == lane
                   and (limit is None or before(c.get("end_utc"), limit))]
    problems = []
    if cut_records or cut_entries:
        first = (cut_records or cut_entries)[0]
        problems.append("cut before the first mark (%s)" % (first.get("reason") or "no reason recorded"))
    done = [s for s in records if s.get("completed") and (s.get("duration_s") or 0) >= MIN_STREAM_S]
    if not done:
        best = max([s.get("duration_s") or 0 for s in records if s.get("completed")] or [0])
        problems.append("no completed stream of at least %d s (longest completed %.0f s)" % (MIN_STREAM_S, best))
    return problems


def c2_stream(summary):
    limit = first_mark(summary)
    n = lane_count(summary)
    failed = []
    for lane in range(1, n + 1):
        problems = lane_problems(summary, lane, limit)
        if problems:
            failed.append("stream %d: %s" % (lane, "; ".join(problems)))
    if failed:
        return False, "%d of %d stream(s) fail: %s" % (len(failed), n, " | ".join(failed))
    return True, "all %d stream(s) completed at least %d s with no cut before the first mark" % (n, MIN_STREAM_S)


def c3_no_cuts(summary):
    limit = first_mark(summary)
    if limit is None:
        return False, "no mark was taken, so the end of the quiet hour cannot be told"
    cuts = [c for c in summary.get("cuts", []) if before(c.get("end_utc"), limit)]
    over = (summary.get("gaps") or {}).get("over_threshold_unfaulted", 0)
    opens = [k for k, v in (summary.get("stream_open_failures") or {}).items() if before(v.get("first_utc"), limit)]
    probes = [p for p in (summary.get("health") or {}).get("failures_sample", []) if before(p.get("utc"), limit)]
    ok = not cuts and over == 0 and not opens and not probes
    return ok, "before the first mark: %d cut(s), %d gap(s) over the threshold, %d failed open class(es), %d failed probe(s)" % (
        len(cuts), over, len(opens), len(probes))


def mark_or_none(summary, index):
    marks = summary.get("marks") or []
    return marks[index] if index < len(marks) else None


def need_valid(mark, label):
    if mark is None:
        return "no %s mark was taken" % label
    if not mark.get("valid"):
        return "the %s mark is invalid (%s)" % (label, mark.get("invalid_reason", "precondition not up"))
    return None


def c4a_freeze(summary):
    m = mark_or_none(summary, 0)
    bad = need_valid(m, "frozen-frpc")
    if bad:
        return False, bad
    close, down = num(m.get("stub_close_proxy_s")), num(m.get("probe_declared_down_s"))
    ok = close is not None and close <= FREEZE_CLOSE_S and down is not None and down <= DOWN_S
    return ok, "CloseProxy after %s s (budget %d), probe down after %s s (budget %d)" % (close, FREEZE_CLOSE_S, down, DOWN_S)


def c4b_engine(summary):
    m = mark_or_none(summary, 1)
    bad = need_valid(m, "engine-kill")
    if bad:
        return False, bad
    down, cls = num(m.get("probe_declared_down_s")), m.get("probe_class")
    close, pings = m.get("stub_close_proxy_s"), m.get("stub_pings_in_30s_after") or 0
    ok = down is not None and down <= DOWN_S and cls == "no_response" and close is None and pings >= 1
    return ok, "probe down after %s s as %s (need <= %d s, no_response); CloseProxy %s (need none); %s Ping(s) in 30 s (need >= 1)" % (
        down, cls, DOWN_S, close, pings)


def c4c_frpc(summary):
    m = mark_or_none(summary, 2)
    bad = need_valid(m, "frpc-kill")
    if bad:
        return False, bad
    close, down = num(m.get("stub_close_proxy_s")), num(m.get("probe_declared_down_s"))
    ok = close is not None and close <= KILL_CLOSE_S and down is not None and down <= DOWN_S
    return ok, "CloseProxy after %s s (budget %d), probe down after %s s (budget %d)" % (close, KILL_CLOSE_S, down, DOWN_S)


def c5_transport(probe_lines, job_log):
    if "RESULT: TRANSPORT FAILED" in job_log:
        return False, "the job log records a permanent failure of the ssh forward"
    login = next((p for p in probe_lines if p.get("probe") == "frpc_login"), None)
    step0 = next((p for p in probe_lines if p.get("probe") == "summary"), None)
    if login is not None and login.get("ok") is True:
        return True, "frpc logged in through the transport (probe frpc_login ok)"
    if step0 is not None and step0.get("transport_path_ok") is True:
        return True, "step 0 proved the transport path with an exchange"
    return False, "neither a frpc login nor a proven step 0 is recorded in probe.jsonl"


def info_lines(summary, ssh_counters):
    out = []
    stub = summary.get("stub") or {}
    out.append("frp Login calls %s, reconnects of frp %s (a freeze and a kill each cost one by design)" % (
        stub.get("logins"), stub.get("reconnects")))
    if ssh_counters:
        out.append("ssh forward: attempts %s, established %s, disconnects %s, recoveries %s" % (
            ssh_counters.get("attempts"), ssh_counters.get("established"),
            ssh_counters.get("disconnects"), ssh_counters.get("recoveries")))
    gaps = (summary.get("gaps") or {}).get("stats_unfaulted_s") or {}
    out.append("token gaps outside fault windows: max %s s, p99 %s s" % (gaps.get("max"), gaps.get("p99")))
    for r in summary.get("per_stream") or []:
        out.append("stream %s: max gap %s s (outside fault windows %s s), cuts %s, first stream completed %s" % (
            r.get("lane"), r.get("max_gap_s"), r.get("max_gap_unfaulted_s"), r.get("cuts"), r.get("first_stream_completed")))
    out.append("added latency: %s" % json.dumps(summary.get("added_latency_ms"), sort_keys=True))
    return out


def evaluate(summary, job_log, probe_lines, ssh_counters):
    """Returns [(criterion id, ok, text)]. ok is True or False, never None."""
    results = []
    for cid, func in (("C1", lambda: c1_registered(summary, job_log)), ("C2", lambda: c2_stream(summary)),
                      ("C3", lambda: c3_no_cuts(summary)), ("C4a", lambda: c4a_freeze(summary)),
                      ("C4b", lambda: c4b_engine(summary)), ("C4c", lambda: c4c_frpc(summary)),
                      ("C5", lambda: c5_transport(probe_lines, job_log))):
        try:
            ok, text = func()
        except (KeyError, TypeError, ValueError) as exc:
            ok, text = False, "the summary could not be read for this criterion (%s: %s)" % (exc.__class__.__name__, exc)
        results.append((cid, bool(ok), text))
    return results


def read_text(path):
    with open(path) as fh:
        return fh.read()


def read_probe(path):
    lines = []
    for raw in read_text(path).splitlines():
        raw = raw.strip()
        if raw:
            try:
                lines.append(json.loads(raw))
            except ValueError:
                pass
    return lines


def main(argv):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--summary", required=True, help="stream_watch.py --out file")
    ap.add_argument("--job-log", required=True, help="job.export.log (or job.log)")
    ap.add_argument("--probe", help="probe.jsonl (step 0)")
    ap.add_argument("--ssh-counters", help="ssh-counters.json")
    args = ap.parse_args(argv)
    try:
        summary = json.loads(read_text(args.summary))
        job_log = read_text(args.job_log)
        probe_lines = read_probe(args.probe) if args.probe else []
        counters = json.loads(read_text(args.ssh_counters)) if args.ssh_counters else None
    except (OSError, ValueError) as exc:
        sys.stderr.write("verdict: cannot read an input: %s\n" % exc)
        print("OVERALL: FAIL (an input is missing or unreadable)")
        return 1
    if summary.get("never_up"):
        print("FAIL C0  the watcher never saw the instance answer /health (never_up)")
        print("OVERALL: FAIL")
        return 1
    results = evaluate(summary, job_log, probe_lines, counters)
    for cid, ok, text in results:
        print("%s %-3s %s" % ("PASS" if ok else "FAIL", cid, text))
    for line in info_lines(summary, counters):
        print("INFO     %s" % line)
    overall = all(ok for _, ok, _ in results)
    print("OVERALL: %s" % ("PASS" if overall else "FAIL"))
    return 0 if overall else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
