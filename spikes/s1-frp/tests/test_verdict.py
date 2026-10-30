"""Tests of verdict.py. The summaries are written by hand from the README's pass criteria and heartbeat
budget (issue #18), not produced by stream_watch.py: a verdict computed from its own output proves nothing."""
import copy
import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _kit  # noqa: E402

verdict = _kit.load("verdict")

FIRST_MARK = "2026-10-30T13:02:00.000Z"
BEFORE = "2026-10-30T12:30:00.000Z"
AFTER = "2026-10-30T13:02:30.000Z"

GOOD = {
    "streams": [{"id": 1, "requested_s": 3660, "completed": True, "interrupted": False, "duration_s": 3661.2, "tokens": 3660}],
    "cuts": [],
    "gaps": {"over_threshold_unfaulted": 0, "threshold_s": 2.0, "stats_unfaulted_s": {"max": 1.2, "p99": 1.05}},
    "stream_open_failures": {},
    "health": {"failures_sample": [{"utc": AFTER, "class": "no_response"}]},
    "marks": [
        {"mark_utc": FIRST_MARK, "valid": True, "precondition": "up", "probe_declared_down_s": 8.0,
         "stub_close_proxy_s": 20.0, "stub_login_after_s": 63.0, "probe_recovered_s": 64.0},
        {"mark_utc": "2026-10-30T13:05:00.000Z", "valid": True, "precondition": "up", "probe_declared_down_s": 4.0,
         "probe_class": "no_response", "stub_close_proxy_s": None, "stub_pings_in_30s_after": 3},
        {"mark_utc": "2026-10-30T13:08:00.000Z", "valid": True, "precondition": "up", "probe_declared_down_s": 4.5,
         "probe_class": "path_down", "stub_close_proxy_s": 1.5},
    ],
    "stub": {"ops": {"Login": 2, "NewProxy": 2, "CloseProxy": 2, "Ping": 400}, "logins": 2, "reconnects": 1},
}
JOB_LOG = "2026-10-30T11:00:03Z frpc: login to server success\n"
PROBE = [{"probe": "summary", "transport_path_ok": None}, {"probe": "frpc_login", "ok": True}]


def verdicts(summary, job_log=JOB_LOG, probe=PROBE):
    return dict((cid, ok) for cid, ok, _ in verdict.evaluate(summary, job_log, probe, None))


def changed(**edits):
    s = copy.deepcopy(GOOD)
    for path, value in edits.items():
        node = s
        keys = path.split("__")
        for k in keys[:-1]:
            node = node[int(k)] if isinstance(node, list) else node[k]
        if isinstance(node, list):
            node[int(keys[-1])] = value
        else:
            node[keys[-1]] = value
    return s


class Criteria(unittest.TestCase):
    def test_the_good_run_passes_every_criterion(self):
        self.assertEqual(verdicts(GOOD), dict((k, True) for k in ("C1", "C2", "C3", "C4a", "C4b", "C4c", "C5")))

    def test_c1_needs_the_login_line_and_the_stub_calls(self):
        self.assertFalse(verdicts(GOOD, job_log="nothing\n")["C1"])
        self.assertFalse(verdicts(changed(stub__ops={"Ping": 4}))["C1"])
        self.assertFalse(verdicts(changed(stub__ops={"Login": 1, "Ping": 4}))["C1"], "NewProxy is needed too")

    def test_c2_needs_a_completed_stream_of_at_least_3600_s(self):
        self.assertFalse(verdicts(changed(streams__0__duration_s=3599.0))["C2"])
        self.assertTrue(verdicts(changed(streams__0__duration_s=3600.0))["C2"])
        self.assertFalse(verdicts(changed(streams__0__completed=False))["C2"])

    def test_c3_a_cut_before_the_first_mark_fails(self):
        self.assertFalse(verdicts(changed(cuts=[{"end_utc": BEFORE, "reason": "cut"}]))["C3"])

    def test_c3_a_cut_after_the_first_mark_is_a_fault_effect(self):
        self.assertTrue(verdicts(changed(cuts=[{"end_utc": AFTER, "reason": "cut"}]))["C3"])

    def test_c3_a_token_gap_over_the_threshold_outside_fault_windows_fails(self):
        self.assertFalse(verdicts(changed(gaps__over_threshold_unfaulted=1))["C3"])

    def test_c3_a_failed_stream_open_before_the_first_mark_fails(self):
        self.assertFalse(verdicts(changed(stream_open_failures={"path_down": {"count": 1, "first_utc": BEFORE}}))["C3"])
        self.assertTrue(verdicts(changed(stream_open_failures={"path_down": {"count": 1, "first_utc": AFTER}}))["C3"])

    def test_c3_a_failed_probe_before_the_first_mark_fails(self):
        self.assertFalse(verdicts(changed(health__failures_sample=[{"utc": BEFORE, "class": "no_response"}]))["C3"])

    def test_c3_cannot_pass_without_a_mark(self):
        self.assertFalse(verdicts(changed(marks=[]))["C3"])

    def test_c4a_freeze_budget_is_30_s_for_closeproxy_and_10_s_for_the_probe(self):
        self.assertTrue(verdicts(changed(marks__0__stub_close_proxy_s=30.0))["C4a"])
        self.assertFalse(verdicts(changed(marks__0__stub_close_proxy_s=30.5))["C4a"])
        self.assertFalse(verdicts(changed(marks__0__stub_close_proxy_s=None))["C4a"])
        self.assertFalse(verdicts(changed(marks__0__probe_declared_down_s=10.5))["C4a"])

    def test_c4b_engine_kill_needs_no_response_no_closeproxy_and_pings(self):
        self.assertFalse(verdicts(changed(marks__1__probe_class="path_down"))["C4b"])
        self.assertFalse(verdicts(changed(marks__1__stub_close_proxy_s=2.0))["C4b"], "CloseProxy means the path died, not the engine")
        self.assertFalse(verdicts(changed(marks__1__stub_pings_in_30s_after=0))["C4b"])
        self.assertFalse(verdicts(changed(marks__1__probe_declared_down_s=11.0))["C4b"])

    def test_c4c_frpc_kill_budget_is_5_s_for_closeproxy_and_10_s_for_the_probe(self):
        self.assertTrue(verdicts(changed(marks__2__stub_close_proxy_s=5.0))["C4c"])
        self.assertFalse(verdicts(changed(marks__2__stub_close_proxy_s=5.1))["C4c"])
        self.assertFalse(verdicts(changed(marks__2__probe_declared_down_s=10.1))["C4c"])

    def test_an_invalid_mark_fails_its_criterion_whatever_else_it_holds(self):
        # review B3: a fault injected while the path was already down
        bad = changed(marks__2__valid=False, marks__2__precondition="down", marks__2__invalid_reason="path was down")
        v = verdicts(bad)
        self.assertFalse(v["C4c"])
        self.assertTrue(v["C4a"] and v["C4b"])
        text = [t for cid, ok, t in verdict.evaluate(bad, JOB_LOG, PROBE, None) if cid == "C4c"][0]
        self.assertIn("invalid", text)

    def test_a_missing_third_mark_fails_c4c(self):
        s = copy.deepcopy(GOOD)
        s["marks"] = s["marks"][:2]
        self.assertFalse(verdicts(s)["C4c"])

    def test_c5_needs_the_frpc_login_probe_or_a_proven_step0(self):
        self.assertFalse(verdicts(GOOD, probe=[{"probe": "summary", "transport_path_ok": None}])["C5"])
        self.assertTrue(verdicts(GOOD, probe=[{"probe": "summary", "transport_path_ok": True}])["C5"])
        self.assertFalse(verdicts(GOOD, job_log=JOB_LOG + "RESULT: TRANSPORT FAILED (auth)\n")["C5"])


def lane_stream(lane, **kw):
    """The first stream of one lane, as stream_watch.py --streams N records it (written by hand from the README)."""
    rec = {"id": 1, "lane": lane, "requested_s": 3660, "completed": True, "interrupted": False, "duration_s": 3661.0,
           "tokens": 3660, "end_utc": "2026-10-30T13:01:30.000Z"}
    rec.update(kw)
    return rec


def three_streams(*records, **edits):
    """GOOD with three declared streams; 'records' replaces the stream list (default: three good lanes)."""
    return changed(streams=list(records) or [lane_stream(1), lane_stream(2), lane_stream(3)], streams_requested=3, **edits)


def c2_text(summary):
    return [t for cid, ok, t in verdict.evaluate(summary, JOB_LOG, PROBE, None) if cid == "C2"][0]


class PerStreamC2(unittest.TestCase):
    """C2 (no cut, full length) holds for every one of the --streams N concurrent streams, not for the best one.
    The case that matters is a cut in one stream while the others complete: the old rule ('some stream completed')
    passed it."""

    def test_three_good_streams_pass(self):
        self.assertTrue(verdicts(three_streams())["C2"])

    def test_a_cut_in_one_stream_fails_c2_even_when_the_others_complete(self):
        cut = lane_stream(2, completed=False, duration_s=812.0, tokens=811, end_utc=BEFORE,
                          reason="cut: EOF before the final chunk")
        s = three_streams(lane_stream(1), cut, lane_stream(3), cuts=[{"stream": 1, "lane": 2, "end_utc": BEFORE,
                                                                       "reason": "cut: EOF before the final chunk"}])
        v = verdicts(s)
        self.assertFalse(v["C2"])
        text = c2_text(s)
        self.assertIn("stream 2", text)
        self.assertNotIn("stream 1:", text)
        self.assertNotIn("stream 3:", text)
        self.assertIn("1 of 3", text)
        self.assertTrue(v["C1"] and v["C4a"] and v["C4b"] and v["C4c"] and v["C5"], "only the stream criteria react")

    def test_the_cut_is_found_in_the_stream_record_alone(self):
        # the cuts list is derived from the records; C2 must not depend on it
        cut = lane_stream(3, completed=False, duration_s=100.0, tokens=99, end_utc=BEFORE, reason="stall")
        self.assertFalse(verdicts(three_streams(lane_stream(1), lane_stream(2), cut))["C2"])

    def test_a_declared_stream_that_never_opened_fails(self):
        s = three_streams(lane_stream(1), lane_stream(3))          # three declared, lane 2 has no record
        self.assertFalse(verdicts(s)["C2"])
        self.assertIn("stream 2: no stream record", c2_text(s))

    def test_two_records_for_three_declared_streams_fails(self):
        self.assertFalse(verdicts(three_streams(lane_stream(1), lane_stream(2)))["C2"])

    def test_a_short_stream_fails_and_is_named(self):
        s = three_streams(lane_stream(1), lane_stream(2), lane_stream(3, duration_s=3599.0))
        self.assertFalse(verdicts(s)["C2"])
        self.assertIn("stream 3", c2_text(s))

    def test_an_interrupted_stream_is_not_a_completed_one(self):
        s = three_streams(lane_stream(1), lane_stream(2, completed=False, interrupted=True, duration_s=2000.0), lane_stream(3))
        self.assertFalse(verdicts(s)["C2"])

    def test_cuts_after_the_first_mark_are_fault_effects_and_do_not_fail_c2(self):
        # the faults cut the streams that were opened after the first hour, by design
        later = lane_stream(1, id=2, completed=False, duration_s=40.0, tokens=39, end_utc=AFTER, reason="cut")
        s = three_streams(lane_stream(1), lane_stream(2), lane_stream(3), later,
                          cuts=[{"stream": 2, "lane": 1, "end_utc": AFTER, "reason": "cut"}])
        self.assertTrue(verdicts(s)["C2"])

    def test_a_summary_without_lane_tags_is_one_stream(self):
        self.assertTrue(verdicts(GOOD)["C2"])
        self.assertFalse(verdicts(changed(streams=[]))["C2"], "no stream at all")

    def test_the_command_line_prints_fail_c2_naming_the_cut_stream(self):
        cut = lane_stream(2, completed=False, duration_s=812.0, end_utc=BEFORE, reason="cut")
        d = tempfile.mkdtemp(prefix="s1-vd-")
        try:
            for name, text in (("summary.json", json.dumps(three_streams(lane_stream(1), cut, lane_stream(3)))),
                               ("job.log", JOB_LOG)):
                with open(os.path.join(d, name), "w") as fh:
                    fh.write(text)
            import io
            out, old = io.StringIO(), sys.stdout
            sys.stdout = out
            try:
                rc = verdict.main(["--summary", os.path.join(d, "summary.json"), "--job-log", os.path.join(d, "job.log")])
            finally:
                sys.stdout = old
        finally:
            for f in os.listdir(d):
                os.unlink(os.path.join(d, f))
            os.rmdir(d)
        self.assertEqual(rc, 1)
        line = [ln for ln in out.getvalue().splitlines() if ln.startswith("FAIL C2")][0]
        self.assertIn("stream 2", line)


class CommandLine(unittest.TestCase):
    def run_main(self, summary, job_log):
        d = tempfile.mkdtemp(prefix="s1-vd-")
        try:
            paths = {}
            for name, text in (("summary.json", json.dumps(summary)), ("job.log", job_log),
                               ("probe.jsonl", "\n".join(json.dumps(p) for p in PROBE) + "\n")):
                paths[name] = os.path.join(d, name)
                with open(paths[name], "w") as fh:
                    fh.write(text)
            import io
            out, old = io.StringIO(), sys.stdout
            sys.stdout = out
            try:
                rc = verdict.main(["--summary", paths["summary.json"], "--job-log", paths["job.log"], "--probe", paths["probe.jsonl"]])
            finally:
                sys.stdout = old
            return rc, out.getvalue()
        finally:
            for f in os.listdir(d):
                os.unlink(os.path.join(d, f))
            os.rmdir(d)

    def test_prints_pass_or_fail_per_criterion_and_exits_0_only_when_all_pass(self):
        rc, out = self.run_main(GOOD, JOB_LOG)
        self.assertEqual(rc, 0)
        self.assertIn("PASS C1", out)
        self.assertIn("OVERALL: PASS", out)
        rc, out = self.run_main(changed(marks__2__stub_close_proxy_s=9.0), JOB_LOG)
        self.assertEqual(rc, 1)
        self.assertIn("FAIL C4c", out)
        self.assertIn("PASS C4a", out)
        self.assertIn("OVERALL: FAIL", out)

    def test_a_missing_input_fails_loudly(self):
        self.assertEqual(verdict.main(["--summary", "/nonexistent/s.json", "--job-log", "/nonexistent/j.log"]), 1)


if __name__ == "__main__":
    unittest.main()
