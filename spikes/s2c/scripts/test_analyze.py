"""Tests of analyze.py. Expected values are worked out by hand from the fixtures, not from the code.

Run: python3 -I -B spikes/s2c/scripts/test_analyze.py   (TMPDIR for the scratch copies; no .pyc with -B)

The fixture tree testdata/results-b is a hand-written regime B with four arms of 10 requests each over 4
workers: B1-rr and B1-vr-hash (vllm-router, relay ports 41001..41004), B1-envoy-bound and B1-envoy-nobound.
It is committed text; no helper writes a fixture and its expectation together. Each test that needs a failing
case copies the tree and changes one thing.

Hand arithmetic for the fixture (request k of an arm, worker in brackets; sessions s0..s4 have two turns):
 B1-rr   : s0t0[0] s1t0[1] s2t0[2] s0t1[3] s3t0[0] s1t1[1] s2t1[2] s3t1[3] s4t0[0] s4t1[1]
           requests per worker w0: k0,k4,k8 = 3; w1: k1,k5,k9 = 3; w2: k2,k6 = 2; w3: k3,k7 = 2   -> [3,3,2,2]
           sessions by first turn: s0,s3,s4 on w0, s1 on w1, s2 on w2                              -> [3,1,1,0]
           sticky: only s1 and s2 stayed on one worker                                              -> 2/5 = 0.4
           busiest 3 over mean 10/4 = 2.5                                                           -> 1.2
           intervals, w0: [1000,1004.5) and [1004,1005.5) overlap, [1008,1009.5) apart              -> peak 2
                      w1: [1001,1005) and [1005,1006.5) only touch, [1009,1010.5) apart            -> peak 1
                      w2, w3: two intervals each, apart                                            -> peak 1
           workers' counters: hits +10,+20,+30,+40 over queries +100 each                           -> 100/400 = 0.25
           client: cached 20 on 6 requests of 100 prompt tokens each                                -> 120/1000 = 0.12
           TTFT sorted 100..190 step 10 (n=10): p50 rank ceil(5.0)=5 -> 140; p95 rank 10 -> 190; p99 rank 10 -> 190
 B1-vr-hash: sessions sticky on w0:{s0} w1:{s1,s4} w2:{s2} w3:{s3}
           requests w0: 2, w1: 4, w2: 2, w3: 2 -> [2,4,2,2]; sessions [1,2,1,1]; sticky 5/5 = 1.0; busiest 4/2.5 = 1.6
           w1 intervals [2001,2004) [2004,2006.5) [2006,2008.5) [2009,2010): the first two touch, the middle two
           overlap in [2006,2006.5)                                                                 -> peak [1,2,1,1]
           hits +60,+70,+80,+90 over +100 each                                                      -> 300/400 = 0.75
           client reported no cached tokens (-1 everywhere)                                         -> None
           TTFT 60..150 step 10: p50 100, p95 150, p99 150. Busiest share 4/10 = exactly 40%.
 B1-envoy-bound: k: s0t0[0] s1t0[1] s2t0[2] s3t0[3] s4t0[0] s0t1[0] s1t1[1] s2t1[2] s3t1[3] s4t1[1]
           requests [3,3,2,2]; sessions [2,1,1,1]; sticky 4/5 = 0.8 (s4 moved); busiest 1.2
           w0 intervals [3000,3002) [3004,3006) [3005,3007): the last two overlap                   -> peak [2,1,1,1]
           hits +45,+50,+55,+60 over +100 each                                                      -> 210/400 = 0.525
           client: cached 10 on every one of 10 requests                                            -> 100/1000 = 0.1
           TTFT 90..180 step 10: p50 130, p95 180, p99 180
           Envoy rq_total before [4,0,1,9] after [7,3,3,11]                                         -> [3,3,2,2]
 B1-envoy-nobound: s1,s2,s3 all on w1: requests w0: 2, w1: 6, w2: 2, w3: 0 -> [2,6,2,0]; shares 0.2 0.6 0.2 0
           sessions [1,3,1,0]; sticky 1.0; busiest 6/2.5 = 2.4
           w1 intervals [4001,4003) [4002,4004) [4003,4005) ...: the first and third touch at 4003, so at
           most two are open at once; the rest are one at a time                                    -> peak [1,2,1,0]
           no vLLM counters in the arm dir                                                           -> hit rate None
           client: cached -1 on 5 requests, 50 on 5 (counted: 5 x 100 prompt tokens)                -> 250/500 = 0.5
           TTFT 50..140 step 10: p50 90, p95 140, p99 140
           Envoy rq_total before [0,5,0,2] after [2,11,2,2]                                         -> [2,6,2,0]
"""
import copy
import gzip
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import analyze  # noqa: E402

ANALYZE = os.path.join(HERE, "analyze.py")
FIXTURE = os.path.join(HERE, "testdata", "results-b")
REAL_BEFORE = os.path.join(HERE, "testdata", "envoy-real-clusters-before.json")
REAL_AFTER = os.path.join(HERE, "testdata", "envoy-real-clusters-after.json")
FACTOR_PATH = "static_resources.clusters[0].common_lb_config.consistent_hashing_lb_config.hash_balance_factor"
CHECKS = ["router-count-per-worker", "router-total-equals-client", "policy-label", "envoy-config", "other-router-silent"]


def cli(*args):
    p = subprocess.run([sys.executable, "-I", "-B", ANALYZE, *args], capture_output=True, text=True, timeout=60)
    return p.returncode, p.stdout, p.stderr


def statuses(stdout):
    """[(ok|FAIL, check name)] of the lines of a --check-arm run."""
    return [(w[0], w[1].rstrip(":")) for w in (line.split(" ", 2) for line in stdout.splitlines())]


ALL_OK = [("ok", c) for c in CHECKS]


def only_fails(check):
    return [("FAIL" if c == check else "ok", c) for c in CHECKS]


# ---------------------------------------------------------------- units

class PromTest(unittest.TestCase):
    def test_labels_in_any_order_and_other_names_ignored(self):
        t = ('# HELP x_total h\n# TYPE x_total counter\nx_total{a="1",b="2"} 5\nx_total{b="3",a="4"} 7 1700000000\n'
             'x_total_created 9\nxx_total 1\nplain 3\n')
        self.assertEqual(analyze.prom_samples(t, "x_total"), [({"a": "1", "b": "2"}, 5.0), ({"b": "3", "a": "4"}, 7.0)])
        self.assertEqual(analyze.prom_samples(t, "plain"), [({}, 3.0)])
        self.assertEqual(analyze.prom_sum(t, "x_total"), 12.0)  # 5 + 7
        self.assertIsNone(analyze.prom_sum(t, "absent_total"))

    def test_an_escaped_quote_in_a_label_value(self):
        t = 'm{w="a\\"b",p="x"} 2\n'
        self.assertEqual(analyze.prom_samples(t, "m"), [({"w": 'a\\"b', "p": "x"}, 2.0)])

    def test_a_bad_value_is_unreadable_not_zero(self):
        with self.assertRaises(analyze.Unreadable):
            analyze.prom_samples("m{a=\"1\"} oops\n", "m")


class PctTest(unittest.TestCase):
    def test_nearest_rank(self):
        v = list(range(1, 11))  # 1..10: p50 rank 5, p95 rank ceil(9.5) = 10
        self.assertEqual(analyze.pct(v, 0.50), 5)
        self.assertEqual(analyze.pct(v, 0.95), 10)
        self.assertEqual(analyze.pct([7], 0.95), 7)
        self.assertIsNone(analyze.pct([], 0.5))

    def test_float_noise_does_not_move_the_rank(self):
        # 0.07 * 100 is 7.000000000000001 in floats; the rank of the 7th percentile of 1..100 is 7, not 8.
        self.assertEqual(analyze.pct(list(range(1, 101)), 0.07), 7)

    def test_p99_of_many(self):
        # 1..200: p99 rank ceil(198.0) = 198, p95 rank ceil(190.0) = 190
        self.assertEqual(analyze.pct(list(range(1, 201)), 0.99), 198)
        self.assertEqual(analyze.pct(list(range(1, 201)), 0.95), 190)


def rec(s, t, start, prompt, cached, ttft=100.0, err=""):
    return {"run": "R", "session": s, "turn": t, "agent": 0, "start_unix": start, "ttft_ms": ttft,
            "latency_ms": 500.0, "prompt_tokens": prompt, "cached_tokens": cached, "completion_tokens": 10,
            "status": 200, "attempt": 1, "err": err}


class ClientStatsTest(unittest.TestCase):
    def test_cached_rate_counts_only_requests_that_reported(self):
        recs = [rec(0, 0, 1.0, 100, 50), rec(0, 1, 2.0, 300, 250), rec(0, 2, 3.0, 50, -1), rec(1, 0, 4.0, 10, 0, err="x")]
        s = analyze.client_stats(recs)
        self.assertEqual((s["requests_ok"], s["failed_attempts"]), (3, 1))
        self.assertAlmostEqual(s["client_hit_rate"], 0.75)  # (50 + 250) / (100 + 300); -1 and the failed one skipped

    def test_no_cached_tokens_reported_gives_none_not_zero(self):
        recs = [rec(0, 0, 1.0, 100, -1), rec(0, 1, 2.0, 100, -1)]
        self.assertIsNone(analyze.client_stats(recs)["client_hit_rate"])
        absent = {k: v for k, v in rec(0, 0, 1.0, 100, 0).items() if k != "cached_tokens"}
        self.assertIsNone(analyze.client_stats([absent])["client_hit_rate"])
        self.assertEqual(analyze.client_stats([rec(0, 0, 1.0, 100, 0)])["client_hit_rate"], 0.0)  # reported 0 is 0

    def test_ttft_percentiles_skip_failed_attempts(self):
        recs = [rec(0, i, float(i), 10, 0, ttft=float(10 * (i + 1))) for i in range(10)] + [rec(1, 0, 20.0, 10, 0, ttft=9999.0, err="boom")]
        s = analyze.client_stats(recs)
        self.assertEqual((s["ttft_p50_ms"], s["ttft_p95_ms"], s["ttft_p99_ms"]), (50.0, 100.0, 100.0))  # 10..100, n=10


class PeakInFlightTest(unittest.TestCase):
    def test_touching_intervals_do_not_overlap(self):
        self.assertEqual(analyze.peak_in_flight([(0, 1), (1, 2)]), 1)
        self.assertEqual(analyze.peak_in_flight([(1, 2), (0, 1)]), 1, "input order must not matter")
        self.assertEqual(analyze.peak_in_flight([(0, 1), (1, 2), (2, 3)]), 1)

    def test_overlapping_intervals(self):
        self.assertEqual(analyze.peak_in_flight([(0, 2), (1, 3)]), 2)
        self.assertEqual(analyze.peak_in_flight([(0, 10), (1, 2), (3, 4)]), 2)  # two short ones are never open together
        self.assertEqual(analyze.peak_in_flight([(0, 5), (1, 5), (2, 5)]), 3)  # all three open from 2 to 5
        self.assertEqual(analyze.peak_in_flight([(0, 2), (2, 4), (1, 3)]), 2)  # [0,2) and [2,4) touch; (1,3) meets each

    def test_empty_and_zero_length(self):
        self.assertEqual(analyze.peak_in_flight([]), 0)
        self.assertEqual(analyze.peak_in_flight([(1, 1), (1, 1)]), 0)
        self.assertEqual(analyze.peak_in_flight([(0, 2), (1, 1)]), 1)


class PlacementStatsTest(unittest.TestCase):
    def test_spread_and_stickiness(self):
        # session 0: all three turns on worker 1 (sticky); session 1: workers 0, 2, 2 (not sticky).
        rows = [(1.0, 2.0, 1, 0, 0, "R-s000"), (3.0, 4.0, 1, 0, 1, "R-s000"), (5.0, 6.0, 1, 0, 2, "R-s000"),
                (1.5, 2.5, 0, 1, 0, "-"), (3.5, 4.5, 2, 1, 1, "-"), (5.5, 6.5, 2, 1, 2, "-")]
        p = analyze.placement_stats(rows, 4)
        self.assertEqual(p["sessions_per_worker"], [1, 1, 0, 0])
        self.assertEqual(p["requests_per_worker"], [1, 3, 2, 0])
        self.assertEqual(p["request_share_per_worker"], [1 / 6, 3 / 6, 2 / 6, 0.0])
        self.assertAlmostEqual(p["busiest_over_mean"], 3 / 1.5)  # mean 6/4 = 1.5
        self.assertAlmostEqual(p["sticky_share"], 0.5)
        self.assertAlmostEqual(p["mean_workers_per_session"], 1.5)
        self.assertAlmostEqual(p["same_worker_as_previous_turn"], 0.75)  # s0 (1,1),(1,1) same; s1 (0,2) moved, (2,2) same
        self.assertAlmostEqual(p["header_seen_by_worker"], 0.5)
        self.assertEqual(p["peak_in_flight_per_worker"], [1, 1, 1, 0])

    def test_a_retried_turn_keeps_its_last_placement_but_every_request_is_counted(self):
        # The same (session 0, turn 0) reached worker 0 at t=0 and worker 3 at t=2; listed latest first.
        rows = [(2.0, 3.0, 3, 0, 0, "-"), (0.0, 1.0, 0, 0, 0, "-")]
        p = analyze.placement_stats(rows, 4)
        self.assertEqual(p["requests_per_worker"], [1, 0, 0, 1])  # both attempts were served
        self.assertEqual(p["sessions_per_worker"], [0, 0, 0, 1])  # the session's first turn kept the later placement

    def test_no_rows_gives_none(self):
        self.assertIsNone(analyze.placement_stats([], 4))


class LoadPlacementTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.tmp.cleanup()

    def put(self, i, text, name=None):
        with open(os.path.join(self.tmp.name, name or f"placement-B-w{i}.tsv"), "w") as f:
            f.write(text)

    def test_seven_fields_are_read_and_filtered_by_run(self):
        self.put(0, "10.0\t11.5\t0\tB1-rr\t3\t2\tB1-rr-s003\n12.0\t13.0\t0\tB2-rr\t0\t0\t-\n")
        self.put(1, "")
        rows = analyze.load_placement(self.tmp.name, "B", 2)
        self.assertEqual(rows["B1-rr"], [(10.0, 11.5, 0, 3, 2, "B1-rr-s003")])
        self.assertEqual(rows["B2-rr"], [(12.0, 13.0, 0, 0, 0, "-")])

    def test_old_six_field_lines_are_unreadable(self):
        self.put(0, "10.0\t0\tB1-rr\t3\t2\tB1-rr-s003\n")
        self.put(1, "")
        with self.assertRaises(analyze.Unreadable):
            analyze.load_placement(self.tmp.name, "B", 2)

    def test_end_before_start_a_missing_file_and_a_foreign_worker_are_unreadable(self):
        self.put(0, "10.0\t9.0\t0\tB1-rr\t3\t2\t-\n")
        self.put(1, "")
        with self.assertRaises(analyze.Unreadable):
            analyze.load_placement(self.tmp.name, "B", 2)
        self.put(0, "")
        os.remove(os.path.join(self.tmp.name, "placement-B-w1.tsv"))
        with self.assertRaises(analyze.Unreadable):
            analyze.load_placement(self.tmp.name, "B", 2)
        self.put(1, "10.0\t11.0\t0\tB1-rr\t3\t2\t-\n")  # worker 0's line in worker 1's file
        with self.assertRaises(analyze.Unreadable):
            analyze.load_placement(self.tmp.name, "B", 2)


class EnvoyCountsTest(unittest.TestCase):
    """The real-shape files are Envoy 1.40.2's own /clusters?format=json (two pipe upstreams, 7 requests sent
    through it; the paths in them are replaced by invented ones). Envoy wrote rq_total "3" for worker-0 and "4"
    for worker-1, and no value at all for a zero counter."""

    def test_real_after_file(self):
        self.assertEqual(analyze.envoy_counts(REAL_AFTER, 2), [3, 4])

    def test_real_before_file_has_no_values_and_reads_as_zero(self):
        with open(REAL_BEFORE) as f:
            self.assertNotIn('"value"', f.read())
        self.assertEqual(analyze.envoy_counts(REAL_BEFORE, 2), [0, 0])

    def test_growth_is_after_minus_before(self):
        with tempfile.TemporaryDirectory() as d:
            shutil.copy(REAL_BEFORE, os.path.join(d, "clusters-before.json"))
            shutil.copy(REAL_AFTER, os.path.join(d, "clusters-after.json"))
            self.assertEqual(analyze.envoy_growth(d, 2), [3, 4])

    def test_shapes_that_are_not_a_worker_cluster_are_unreadable(self):
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "c.json")
            for doc in ({"cluster_statuses": []}, {"cluster_statuses": [{"name": "other", "host_statuses": []}]}, []):
                with open(p, "w") as f:
                    json.dump(doc, f)
                with self.assertRaises(analyze.Unreadable):
                    analyze.envoy_counts(p, 2)
        with self.assertRaises(analyze.Unreadable):  # three workers expected, the file has two hosts
            analyze.envoy_counts(REAL_AFTER, 3)
        with self.assertRaises(analyze.Unreadable):  # a host beyond nwork
            analyze.envoy_counts(REAL_AFTER, 1)


class RelayMapTest(unittest.TestCase):
    def test_worker_index_tolerates_slash_and_host(self):
        ports = {41001: 0, 41002: 1}
        self.assertEqual(analyze.worker_index("http://127.0.0.1:41001", ports), 0)
        self.assertEqual(analyze.worker_index("http://127.0.0.1:41002/", ports), 1)
        self.assertEqual(analyze.worker_index("http://localhost:41001", ports), 0)
        self.assertIsNone(analyze.worker_index("http://127.0.0.1:41009", ports))
        self.assertIsNone(analyze.worker_index("not a url", ports))

    def test_map_must_name_every_worker_once(self):
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "relay-map.tsv")
            for text in ("41001\t0\n", "41001\t0\n41001\t1\n", "41001\t0\n41002\t5\n", "41001 0\n41002 1\n", "x\t0\n41002\t1\n"):
                with open(p, "w") as f:
                    f.write(text)
                with self.assertRaises(analyze.Unreadable, msg=text):
                    analyze.relay_map(d, 2)
            with open(p, "w") as f:
                f.write("41002\t1\n\n41001\t0\n")
            self.assertEqual(analyze.relay_map(d, 2), {41002: 1, 41001: 0})


class ReadFactorTest(unittest.TestCase):
    def doc(self, lb):
        return {"static_resources": {"clusters": [{"name": "workers", "common_lb_config": lb}]}}

    def test_present_absent_and_wrapped(self):
        f = analyze.read_factor
        self.assertEqual(f(self.doc({"consistent_hashing_lb_config": {"hash_balance_factor": 125}}), "p"), 125)
        self.assertEqual(f(self.doc({"consistent_hashing_lb_config": {"hash_balance_factor": {"value": 200}}}), "p"), 200)
        self.assertIsNone(f(self.doc({"consistent_hashing_lb_config": {}}), "p"))  # an empty parent object
        self.assertIsNone(f(self.doc({}), "p"))  # no parent at all
        self.assertIsNone(f({"static_resources": {"clusters": [{"name": "workers"}]}}, "p"))

    def test_no_cluster_or_a_non_integer_is_unreadable(self):
        for doc in ({}, {"static_resources": {"clusters": []}}, {"static_resources": {}},
                    self.doc({"consistent_hashing_lb_config": {"hash_balance_factor": "125"}}),
                    self.doc({"consistent_hashing_lb_config": {"hash_balance_factor": True}})):
            with self.assertRaises(analyze.Unreadable):
                analyze.read_factor(doc, "p")


def row(arm="vr-hash", regime="B", rep=1, hit=0.6, p95=100.0, counts=(3, 3, 2, 2)):
    return {"regime": regime, "rep": rep, "arm": arm, "name": f"{regime}{rep}-{arm}", "worker_hit_rate": hit,
            "ttft_p95_ms": p95, "placement": {"requests_per_worker": list(counts)} if counts else None}


BASE = row("rr", hit=0.5, p95=100.0)


class PassRuleTest(unittest.TestCase):
    def parts(self, r):
        res = analyze.pass_rule(r, BASE)
        return res["verdict"], [res["parts"][k]["passed"] for k in
                                ("hit_rate_above_rr", "no_worker_above_40pct", "ttft_p95_not_worse_than_rr")]

    def test_all_three_parts_pass(self):
        self.assertEqual(self.parts(row()), ("pass", [True, True, True]))  # 0.6 > 0.5; 3/10 <= 40%; 100 <= 100

    def test_hit_rate_fails_alone(self):
        self.assertEqual(self.parts(row(hit=0.5)), ("fail", [False, True, True]))  # equal is not above
        self.assertEqual(self.parts(row(hit=0.4)), ("fail", [False, True, True]))

    def test_the_share_fails_alone(self):
        self.assertEqual(self.parts(row(counts=(5, 2, 2, 1))), ("fail", [True, False, True]))  # 5/10 = 50%
        self.assertEqual(self.parts(row(counts=(41, 20, 20, 19))), ("fail", [True, False, True]))  # 41/100
        self.assertEqual(self.parts(row(counts=(4, 2, 2, 2))), ("pass", [True, True, True]))  # exactly 4/10 = 40% is not above
        self.assertEqual(self.parts(row(counts=(40, 20, 20, 20))), ("pass", [True, True, True]))

    def test_ttft_fails_alone(self):
        self.assertEqual(self.parts(row(p95=101.0)), ("fail", [True, True, False]))  # worse than 100
        self.assertEqual(self.parts(row(p95=99.0)), ("pass", [True, True, True]))

    def test_an_unknown_part_is_never_a_pass(self):
        self.assertEqual(self.parts(row(hit=None)), ("unknown", [None, True, True]))
        self.assertEqual(self.parts(row(p95=None)), ("unknown", [True, True, None]))
        self.assertEqual(self.parts(row(counts=None)), ("unknown", [True, None, True]))
        self.assertEqual(self.parts(row(hit=None, counts=(5, 2, 2, 1))), ("fail", [None, False, True]))
        base_none = dict(BASE, worker_hit_rate=None)
        self.assertEqual(analyze.pass_rule(row(), base_none)["verdict"], "unknown")

    def test_no_baseline(self):
        self.assertEqual(analyze.pass_rule(row(), None)["verdict"], "no rr baseline")

    def test_the_baseline_is_the_rr_arm_of_the_same_regime_and_rep(self):
        rows = [row("rr", "B", 1, hit=0.5, p95=100.0), row("rr", "A", 1, hit=0.9, p95=50.0),
                row("vr-hash", "B", 1, hit=0.6, p95=80.0),      # beats B1-rr; would fail against A1-rr
                row("vr-hash", "B", 2, hit=0.6, p95=80.0),      # no B2-rr
                row("envoy-bound", "C", 1, hit=0.6, p95=80.0)]  # no regime C rr
        analyze.attach_pass_rules(rows)
        verdicts = {r["name"]: r["pass_rule"]["verdict"] for r in rows}
        self.assertEqual(verdicts, {"B1-rr": "baseline", "A1-rr": "baseline", "B1-vr-hash": "pass",
                                    "B2-vr-hash": "no rr baseline", "C1-envoy-bound": "no rr baseline"})


# ---------------------------------------------------------------- fixture-tree tests

class ResultsCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.res = os.path.join(self.tmp.name, "results")
        shutil.copytree(FIXTURE, self.res)

    def tearDown(self):
        self.tmp.cleanup()

    def arm(self, name):
        return os.path.join(self.res, "arms", name)

    def path(self, arm, rel):
        return os.path.normpath(os.path.join(self.arm(arm), rel))

    def read(self, arm, rel):
        with open(self.path(arm, rel)) as f:
            return f.read()

    def write(self, arm, rel, text):
        os.makedirs(os.path.dirname(self.path(arm, rel)), exist_ok=True)
        with open(self.path(arm, rel), "w") as f:
            f.write(text)

    def edit(self, arm, rel, old, new, count=1):
        text = self.read(arm, rel)
        self.assertEqual(text.count(old), count, f"{rel} should hold {old!r} {count} time(s)")
        self.write(arm, rel, text.replace(old, new))

    def edit_json(self, arm, rel, fn):
        doc = json.loads(self.read(arm, rel))
        fn(doc)
        self.write(arm, rel, json.dumps(doc, indent=1) + "\n")

    def remove(self, arm, rel):
        os.remove(self.path(arm, rel))

    def set_run_json(self, arm, text):
        """Replace run.json and make start.json's config_sha256 match it, so that a test of the factor is not
        also a test of the hash. (The hash check is tested on its own below, and on the committed start.json
        files, whose sha256 came from the sha256sum program.)"""
        self.write(arm, "envoy/run.json", text)
        self.edit_json(arm, "envoy/start.json", lambda d: d.update(config_sha256=hashlib.sha256(text.encode()).hexdigest()))

    def check(self, arm):
        return cli("--check-arm", self.arm(arm))

    def expect_only_fail(self, arm, check, *reasons):
        rc, out, err = self.check(arm)
        self.assertEqual((rc, statuses(out)), (1, only_fails(check)), out + err)
        for r in reasons:
            self.assertIn(r, out)

    def expect_unreadable(self, arm, mention=None):
        rc, out, err = self.check(arm)
        self.assertEqual(rc, 2, out + err)
        self.assertNotIn("ok ", out, "an unreadable input must not print a passing line")
        self.assertIn("unreadable:", err)
        if mention:
            self.assertIn(mention, err)


def set_rq(doc, worker, value):
    for h in doc["cluster_statuses"][0]["host_statuses"]:
        if h["hostname"] == f"worker-{worker}":
            for s in h["stats"]:
                if s["name"] == "rq_total":
                    s["value"] = str(value)


class PassingArmsTest(ResultsCase):
    def test_every_fixture_arm_passes_all_five_checks(self):
        for name in ("B1-rr", "B1-vr-hash", "B1-envoy-bound", "B1-envoy-nobound"):
            with self.subTest(name):
                rc, out, err = self.check(name)
                self.assertEqual((rc, statuses(out), err), (0, ALL_OK, ""))

    def test_the_two_families_print_their_not_applicable_lines(self):
        _, out, _ = self.check("B1-rr")
        self.assertIn("ok envoy-config (n/a: vllm-router arm)", out.splitlines())
        _, out, _ = self.check("B1-envoy-bound")
        self.assertIn("ok policy-label (n/a: envoy arm)", out.splitlines())

    def test_a_trailing_slash_and_gzipped_inputs(self):
        rc, out, _ = cli("--check-arm", self.arm("B1-rr") + os.sep)
        self.assertEqual((rc, statuses(out)), (0, ALL_OK))
        p = os.path.join(self.res, "placement-B-w0.tsv")
        with open(p, "rb") as f_in, gzip.open(p + ".gz", "wb") as f_out:
            f_out.write(f_in.read())
        os.remove(p)
        rc, out, _ = self.check("B1-rr")
        self.assertEqual((rc, statuses(out)), (0, ALL_OK))

    def test_an_other_family_pair_with_zero_growth_passes(self):
        self.write("B1-envoy-bound", "vr-before.prom", 'vllm_router_processed_requests_total{worker="http://127.0.0.1:41001"} 4\n')
        self.write("B1-envoy-bound", "vr-after.prom", 'vllm_router_processed_requests_total{worker="http://127.0.0.1:41001"} 4\n')
        self.assertEqual(self.check("B1-envoy-bound")[:2], (0, "\n".join(
            ["ok router-count-per-worker", "ok router-total-equals-client", "ok policy-label (n/a: envoy arm)",
             "ok envoy-config", "ok other-router-silent"]) + "\n"))
        shutil_pair = "clusters-before.json", "clusters-after.json"
        for n in shutil_pair:  # an Envoy pair beside a vllm-router arm with no growth: before == after
            self.write("B1-vr-hash", n, self.read("B1-envoy-bound", "clusters-before.json"))
        self.assertEqual(statuses(self.check("B1-vr-hash")[1]), ALL_OK)

    def test_a_policy_series_of_the_other_policy_with_no_growth_is_fine(self):
        line = 'vllm_router_policy_decisions_total{policy="round_robin",worker="http://127.0.0.1:41001"} 5\n'
        self.edit("B1-vr-hash", "vr-before.prom", "vllm_router_active_workers 4\n", "vllm_router_active_workers 4\n" + line)
        self.edit("B1-vr-hash", "vr-after.prom", "vllm_router_active_workers 4\n", "vllm_router_active_workers 4\n" + line)
        self.assertEqual(statuses(self.check("B1-vr-hash")[1]), ALL_OK)


class Check1Test(ResultsCase):
    def test_vllm_router_count_off_on_two_workers_with_the_sum_kept(self):
        # processed after: w0 10 -> 11 (growth 11-7 = 4, placement 3), w1 8 -> 7 (growth 7-5 = 2, placement 3); sum stays 10
        self.edit("B1-rr", "vr-after.prom", 'processed_requests_total{worker="http://127.0.0.1:41001"} 10', 'processed_requests_total{worker="http://127.0.0.1:41001"} 11')
        self.edit("B1-rr", "vr-after.prom", 'processed_requests_total{worker="http://127.0.0.1:41002"} 8', 'processed_requests_total{worker="http://127.0.0.1:41002"} 7')
        self.expect_only_fail("B1-rr", "router-count-per-worker", "worker 0: router 4, placement 3", "worker 1: router 2, placement 3")

    def test_envoy_count_off_with_the_sum_kept(self):
        # rq_total after: w0 7 -> 8 (growth 8-4 = 4 vs placement 3), w1 3 -> 2 (growth 2-0 = 2 vs 3)
        self.edit_json("B1-envoy-bound", "clusters-after.json", lambda d: (set_rq(d, 0, 8), set_rq(d, 1, 2)))
        self.expect_only_fail("B1-envoy-bound", "router-count-per-worker", "worker 0: router 4, placement 3", "worker 1: router 2, placement 3")

    def test_the_router_served_a_worker_that_is_not_in_the_map(self):
        # w3 processed after 4 -> 3 (growth 1, placement 2) and an unmapped port grew by 1: 3+3+2+1 + 1 = 10 = client
        self.edit("B1-rr", "vr-after.prom", 'processed_requests_total{worker="http://127.0.0.1:41004"} 4', 'processed_requests_total{worker="http://127.0.0.1:41004"} 3')
        self.edit("B1-rr", "vr-after.prom", "vllm_router_requests_total", 'vllm_router_processed_requests_total{worker="http://127.0.0.1:41009"} 1\nvllm_router_requests_total')
        self.expect_only_fail("B1-rr", "router-count-per-worker", "worker 3: router 1, placement 2", "41009")

    def test_a_worker_with_no_series_at_all_counts_zero_and_fails_against_a_busy_worker(self):
        # w2's series is in neither scrape: growth 0 (an absent series is 0), placement says 2 -> FAIL, never a pass
        for f in ("vr-before.prom", "vr-after.prom"):
            text = self.read("B1-rr", f)
            kept = "".join(l for l in text.splitlines(True) if not l.startswith('vllm_router_processed_requests_total{worker="http://127.0.0.1:41003"}'))
            self.assertNotEqual(kept, text)
            self.write("B1-rr", f, kept)
        rc, out, _ = self.check("B1-rr")
        self.assertEqual(rc, 1)
        self.assertIn("FAIL router-count-per-worker: worker 2: router 0, placement 2", out)

    def test_placement_counts_only_the_arms_own_run_label(self):
        # Another arm's lines in the same placement files must not count: add three B1-rr lines under another label.
        p = os.path.join(self.res, "placement-B-w0.tsv")
        with open(p, "a") as f:
            f.write("1100.000000\t1101.000000\t0\tB9-other\t0\t0\t-\n" * 3)
        self.assertEqual(statuses(self.check("B1-rr")[1]), ALL_OK)


class Check2Test(ResultsCase):
    def test_sum_differs_from_the_clients_records(self):
        lines = self.read("B1-rr", "client.jsonl").splitlines()
        self.write("B1-rr", "client.jsonl", "\n".join(lines[:-1]) + "\n")  # 9 records, router sum 10, placement unchanged
        self.expect_only_fail("B1-rr", "router-total-equals-client", "router counts sum to 10, client has 9 records")

    def test_more_client_records_than_the_router_counted(self):
        lines = self.read("B1-envoy-nobound", "client.jsonl").splitlines()
        self.write("B1-envoy-nobound", "client.jsonl", "\n".join(lines + lines[:1]) + "\n")
        self.expect_only_fail("B1-envoy-nobound", "router-total-equals-client", "client has 11 records")

    def test_failed_attempts_count_as_records(self):
        lines = self.read("B1-rr", "client.jsonl").splitlines()
        failed = json.loads(lines[0])
        failed["err"] = "boom"
        self.write("B1-rr", "client.jsonl", "\n".join([json.dumps(failed)] + lines[1:]) + "\n")
        self.assertEqual(statuses(self.check("B1-rr")[1]), ALL_OK)  # still 10 records for 10 router counts

    def test_zero_records_never_equal_a_zero_router(self):
        self.write("B1-rr", "client.jsonl", "")
        self.expect_only_fail("B1-rr", "router-total-equals-client", "no client records")


class Check3Test(ResultsCase):
    def test_every_decision_under_the_other_policys_label(self):
        # vr-hash decisions 2+4+2+2 = 10 now labelled round_robin; none under consistent_hash
        self.edit("B1-vr-hash", "vr-after.prom", 'policy="consistent_hash"', 'policy="round_robin"', count=4)
        self.expect_only_fail("B1-vr-hash", "policy-label", "10 decisions under policy 'round_robin'", "no decisions under policy 'consistent_hash'")

    def test_one_extra_decision_under_the_other_label(self):
        self.edit("B1-rr", "vr-after.prom", "vllm_router_requests_total",
                  'vllm_router_policy_decisions_total{policy="consistent_hash",worker="http://127.0.0.1:41001"} 1\nvllm_router_requests_total')
        self.expect_only_fail("B1-rr", "policy-label", "1 decisions under policy 'consistent_hash'")

    def test_no_decisions_at_all_is_not_a_pass(self):
        self.edit("B1-vr-hash", "vr-after.prom", 'vllm_router_policy_decisions_total{worker="http://127.0.0.1:4100', "ignored_total{worker=\"http://127.0.0.1:4100", count=4)
        self.expect_only_fail("B1-vr-hash", "policy-label", "no decisions under policy 'consistent_hash'")


class Check4Test(ResultsCase):
    BOUND_JSON = '{"static_resources": {"clusters": [{"name": "workers", "common_lb_config": {"consistent_hashing_lb_config": {"hash_balance_factor": 125}}}]}}\n'
    NONE_JSON = '{"static_resources": {"clusters": [{"name": "workers", "common_lb_config": {"consistent_hashing_lb_config": {}}}]}}\n'

    def test_run_json_hash_differs_from_start_json(self):
        self.write("B1-envoy-bound", "envoy/run.json", self.read("B1-envoy-bound", "envoy/run.json") + " \n")  # same factor, other bytes
        self.expect_only_fail("B1-envoy-bound", "envoy-config", "differs from start.json config_sha256")

    def test_the_hash_is_over_the_files_exact_bytes(self):
        # CRLF line endings: the sha256 of the bytes on disk is what the launcher records. A reader that
        # normalised newlines before hashing would compute another digest and fail this arm.
        raw = self.BOUND_JSON.replace('", "', '",\r\n "').replace("\n", "\r\n").replace("\r\r\n", "\r\n").encode()
        self.assertIn(b"\r\n", raw)
        with open(self.path("B1-envoy-bound", "envoy/run.json"), "wb") as f:
            f.write(raw)
        self.edit_json("B1-envoy-bound", "envoy/start.json", lambda d: d.update(config_sha256=hashlib.sha256(raw).hexdigest()))
        self.assertEqual(statuses(self.check("B1-envoy-bound")[1]), ALL_OK)

    def test_bound_present_when_meta_says_none(self):
        self.set_run_json("B1-envoy-nobound", self.BOUND_JSON)
        self.expect_only_fail("B1-envoy-nobound", "envoy-config", "hash_balance_factor is 125, meta bound is none")

    def test_bound_absent_when_meta_says_125(self):
        self.set_run_json("B1-envoy-bound", self.NONE_JSON)
        self.expect_only_fail("B1-envoy-bound", "envoy-config", "hash_balance_factor is absent, meta bound is 125")

    def test_another_factor_than_the_bound(self):
        self.set_run_json("B1-envoy-bound", self.BOUND_JSON.replace("125", "50"))
        self.expect_only_fail("B1-envoy-bound", "envoy-config", "hash_balance_factor is 50, meta bound is 125")

    def test_absence_reads_the_same_with_or_without_the_parent_object(self):
        self.set_run_json("B1-envoy-nobound", '{"static_resources": {"clusters": [{"name": "workers", "common_lb_config": {}}]}}\n')
        self.assertEqual(statuses(self.check("B1-envoy-nobound")[1]), ALL_OK)
        self.set_run_json("B1-envoy-nobound", '{"static_resources": {"clusters": [{"name": "workers"}]}}\n')
        self.assertEqual(statuses(self.check("B1-envoy-nobound")[1]), ALL_OK)

    def test_a_wrapped_factor_value_matches_its_integer(self):
        self.set_run_json("B1-envoy-bound", self.BOUND_JSON.replace('125', '{"value": 125}'))
        self.assertEqual(statuses(self.check("B1-envoy-bound")[1]), ALL_OK)

    def test_a_stray_diff_entry(self):
        self.edit_json("B1-envoy-nobound", "envoy/start.json", lambda d: d.update(diff=[FACTOR_PATH, "static_resources.listeners[0].name"]))
        self.expect_only_fail("B1-envoy-nobound", "envoy-config", "static_resources.listeners[0].name")
        self.edit_json("B1-envoy-nobound", "envoy/start.json", lambda d: d.update(diff=["static_resources.listeners[0].name"]))
        self.expect_only_fail("B1-envoy-nobound", "envoy-config", "only static_resources")

    def test_a_product_start_lists_no_diff(self):
        self.edit_json("B1-envoy-bound", "envoy/start.json", lambda d: d.update(diff=[FACTOR_PATH]))
        self.expect_only_fail("B1-envoy-bound", "envoy-config", "not empty in product mode")

    def test_a_replica_with_an_empty_diff_is_allowed(self):
        self.edit_json("B1-envoy-nobound", "envoy/start.json", lambda d: d.update(diff=[]))
        self.assertEqual(statuses(self.check("B1-envoy-nobound")[1]), ALL_OK)

    def test_unusable_start_json_is_exit_2(self):
        self.edit_json("B1-envoy-bound", "envoy/start.json", lambda d: d.update(mode="other"))
        self.expect_unreadable("B1-envoy-bound", "start.json")


class Check5Test(ResultsCase):
    def test_vllm_router_counters_grew_during_an_envoy_arm(self):
        self.write("B1-envoy-bound", "vr-before.prom", 'vllm_router_processed_requests_total{worker="http://127.0.0.1:41001"} 0\n')
        self.write("B1-envoy-bound", "vr-after.prom", 'vllm_router_processed_requests_total{worker="http://127.0.0.1:41001"} 3\n')
        self.expect_only_fail("B1-envoy-bound", "other-router-silent", "vllm-router counters grew by 3")

    def test_policy_decisions_alone_count_as_growth(self):
        self.write("B1-envoy-nobound", "vr-before.prom", "vllm_router_active_workers 4\n")
        self.write("B1-envoy-nobound", "vr-after.prom", 'vllm_router_policy_decisions_total{policy="round_robin",worker="http://127.0.0.1:41002"} 2\n')
        self.expect_only_fail("B1-envoy-nobound", "other-router-silent", "grew by 2")

    def test_envoy_requests_during_a_vllm_router_arm(self):
        # the envoy-bound arm's own scrapes: rq_total grew [7-4, 3-0, 3-1, 11-9] = [3,3,2,2]
        for n in ("clusters-before.json", "clusters-after.json"):
            self.write("B1-vr-hash", n, self.read("B1-envoy-bound", n))
        self.expect_only_fail("B1-vr-hash", "other-router-silent", "Envoy rq_total grew by [3, 3, 2, 2]")

    def test_one_file_of_the_other_pair_is_exit_2(self):
        self.write("B1-envoy-bound", "vr-after.prom", "vllm_router_active_workers 4\n")
        self.expect_unreadable("B1-envoy-bound", "vr-before.prom")
        self.write("B1-vr-hash", "clusters-before.json", self.read("B1-envoy-bound", "clusters-before.json"))
        self.expect_unreadable("B1-vr-hash", "clusters-after.json")


class UnreadableInputTest(ResultsCase):
    def test_a_missing_required_file_is_exit_2_naming_it(self):
        missing = [("B1-rr", "meta.json"), ("B1-rr", "client.jsonl"), ("B1-rr", "vr-before.prom"), ("B1-rr", "vr-after.prom"),
                   ("B1-rr", "relay-map.tsv"), ("B1-rr", "../../placement-B-w2.tsv"), ("B1-vr-hash", "vr-after.prom"),
                   ("B1-envoy-bound", "clusters-before.json"), ("B1-envoy-bound", "clusters-after.json"),
                   ("B1-envoy-bound", "envoy/run.json"), ("B1-envoy-bound", "envoy/start.json"),
                   ("B1-envoy-nobound", "../../placement-B-w0.tsv")]
        for arm, rel in missing:
            with self.subTest(arm=arm, file=rel):
                self.setUp()  # a fresh copy each time
                self.remove(arm, rel)
                self.expect_unreadable(arm, os.path.basename(rel))

    def test_malformed_files_are_exit_2(self):
        cases = [
            ("B1-envoy-bound", "clusters-after.json", lambda a, r: self.write(a, r, self.read(a, r)[:200])),     # truncated JSON
            ("B1-envoy-bound", "clusters-after.json", lambda a, r: self.write(a, r, "{}")),                       # no cluster
            ("B1-envoy-bound", "clusters-after.json", lambda a, r: self.edit(a, r, '"worker-3"', '"worker-9"')),  # a host that is not a worker
            ("B1-envoy-bound", "envoy/start.json", lambda a, r: self.write(a, r, "not json")),
            ("B1-envoy-bound", "envoy/run.json", lambda a, r: self.write(a, r, "{}")),
            ("B1-rr", "client.jsonl", lambda a, r: self.write(a, r, self.read(a, r) + "garbage\n")),
            ("B1-rr", "relay-map.tsv", lambda a, r: self.write(a, r, "41001\t0\n41002\t1\n")),                   # workers 2, 3 not mapped
            ("B1-rr", "vr-after.prom", lambda a, r: self.edit(a, r, "} 10\n", "} ten\n", count=2)),              # a value that is no number
            ("B1-rr", "meta.json", lambda a, r: self.edit_json(a, r, lambda d: d.pop("nwork"))),
            ("B1-rr", "meta.json", lambda a, r: self.edit_json(a, r, lambda d: d.update(router="nginx"))),
            ("B1-vr-hash", "meta.json", lambda a, r: self.edit_json(a, r, lambda d: d.update(policy=None))),
            ("B1-envoy-bound", "meta.json", lambda a, r: self.edit_json(a, r, lambda d: d.update(bound="some"))),
            ("B1-rr", "../../placement-B-w1.tsv", lambda a, r: self.write(a, r, "1.0\t2.0\t1\tB1-rr\t0\t0\n")),   # the old 6-field line
            ("B1-rr", "../../placement-B-w1.tsv", lambda a, r: self.write(a, r, "3.0\t2.0\t1\tB1-rr\t0\t0\t-\n")),  # ends before it starts
            ("B1-rr", "../../placement-B-w1.tsv", lambda a, r: self.write(a, r, "1.0\t2.0\t3\tB1-rr\t0\t0\t-\n")),  # worker 3 in w1's file
        ]
        for n, (arm, rel, mutate) in enumerate(cases):
            with self.subTest(case=n, arm=arm, file=rel):
                self.setUp()
                mutate(arm, rel)
                self.expect_unreadable(arm)

    def test_an_arm_dir_outside_a_results_layout_is_exit_2(self):
        stray = os.path.join(self.tmp.name, "elsewhere", "B1-rr")
        shutil.copytree(self.arm("B1-rr"), stray)
        rc, out, err = cli("--check-arm", stray)
        self.assertEqual(rc, 2, out + err)

    def test_usage_errors(self):
        self.assertEqual(cli()[0], 2)
        self.assertEqual(cli("--check-arm", self.arm("B1-rr"), self.res)[0], 2)


class SummaryTest(ResultsCase):
    def rows(self):
        rc, out, err = cli(self.res, "--json", os.path.join(self.tmp.name, "rows.json"), "--md", os.path.join(self.tmp.name, "rows.md"))
        with open(os.path.join(self.tmp.name, "rows.json")) as f:
            return rc, out, err, {r["name"]: r for r in json.load(f)}

    def test_every_figure_of_the_fixture_arms(self):
        rc, out, err, rows = self.rows()
        self.assertEqual((rc, err), (0, ""))
        self.assertEqual(list(rows), ["B1-rr", "B1-vr-hash", "B1-envoy-bound", "B1-envoy-nobound"])
        rr, vh, eb, en = rows["B1-rr"], rows["B1-vr-hash"], rows["B1-envoy-bound"], rows["B1-envoy-nobound"]
        for r, (hit, client, p50, p95, p99) in ((rr, (0.25, 0.12, 140, 190, 190)), (vh, (0.75, None, 100, 150, 150)),
                                                (eb, (0.525, 0.1, 130, 180, 180)), (en, (None, 0.5, 90, 140, 140))):
            with self.subTest(r["name"]):
                for got, want in ((r["worker_hit_rate"], hit), (r["client_hit_rate"], client)):
                    self.assertEqual(got is None, want is None)
                    if want is not None:
                        self.assertAlmostEqual(got, want)
                self.assertEqual((r["ttft_p50_ms"], r["ttft_p95_ms"], r["ttft_p99_ms"]), (p50, p95, p99))
                self.assertEqual((r["requests_ok"], r["failed_attempts"], r["wall_s"], r["errors"]), (10, 0, 12.0, []))
        self.assertEqual(rr["worker_hit_rate_per_worker"], [0.1, 0.2, 0.3, 0.4])  # 10/100 .. 40/100
        self.assertEqual(vh["worker_hit_rate_per_worker"], [0.6, 0.7, 0.8, 0.9])
        self.assertEqual(eb["worker_hit_rate_per_worker"], [0.45, 0.5, 0.55, 0.6])
        self.assertIsNone(en["worker_hit_rate_per_worker"])
        self.assertEqual(rr["preemptions"], 3)  # 2 + 1 + 0 + 0
        self.assertIsNone(vh["preemptions"])
        for r, per_worker, sessions, sticky, busiest, peaks, router in (
                (rr, [3, 3, 2, 2], [3, 1, 1, 0], 0.4, 1.2, [2, 1, 1, 1], [3, 3, 2, 2]),
                (vh, [2, 4, 2, 2], [1, 2, 1, 1], 1.0, 1.6, [1, 2, 1, 1], [2, 4, 2, 2]),
                (eb, [3, 3, 2, 2], [2, 1, 1, 1], 0.8, 1.2, [2, 1, 1, 1], [3, 3, 2, 2]),
                (en, [2, 6, 2, 0], [1, 3, 1, 0], 1.0, 2.4, [1, 2, 1, 0], [2, 6, 2, 0])):
            with self.subTest(r["name"]):
                p = r["placement"]
                self.assertEqual((p["requests_per_worker"], p["sessions_per_worker"], p["peak_in_flight_per_worker"], r["router_counts"]),
                                 (per_worker, sessions, peaks, router))
                self.assertEqual(p["request_share_per_worker"], [c / 10 for c in per_worker])
                self.assertAlmostEqual(p["sticky_share"], sticky)
                self.assertAlmostEqual(p["busiest_over_mean"], busiest)
                self.assertTrue(all(c["ok"] for c in r["checks"]))
        verdicts = {n: r["pass_rule"]["verdict"] for n, r in rows.items()}
        self.assertEqual(verdicts, {"B1-rr": "baseline", "B1-vr-hash": "pass", "B1-envoy-bound": "pass", "B1-envoy-nobound": "fail"})
        # vr-hash: hit 0.75 > 0.25; busiest 4 of 10 is exactly 40%; p95 150 <= 190. nobound: hit unknown, 6 of 10 = 60%, 140 <= 190
        self.assertEqual([vh["pass_rule"]["parts"][k]["passed"] for k in vh["pass_rule"]["parts"]], [True, True, True])
        self.assertEqual([en["pass_rule"]["parts"][k]["passed"] for k in en["pass_rule"]["parts"]], [None, False, True])
        with open(os.path.join(self.tmp.name, "rows.md")) as f:
            self.assertEqual(f.read(), out)
        self.assertIn("| B1-vr-hash | vr-hash | 0.750 | n/a | 100 / 150 / 150 | [2, 4, 2, 2] |", out)
        self.assertIn("- B1-envoy-nobound: fail", out)

    def test_a_failing_check_exits_1_and_is_listed(self):
        self.edit_json("B1-envoy-bound", "clusters-after.json", lambda d: (set_rq(d, 0, 8), set_rq(d, 1, 2)))
        rc, out, _, rows = self.rows()
        self.assertEqual(rc, 1)
        self.assertIn("- B1-envoy-bound: FAIL router-count-per-worker: worker 0: router 4, placement 3", out)
        self.assertIn("| FAIL router-count-per-worker |", out)
        self.assertEqual([c["ok"] for c in rows["B1-envoy-bound"]["checks"]], [False, True, True, True, True])

    def test_an_unreadable_arm_exits_2_and_hides_nothing_else(self):
        self.remove("B1-rr", "vr-after.prom")
        rc, out, _, rows = self.rows()
        self.assertEqual(rc, 2)
        self.assertIsNone(rows["B1-rr"]["checks"])
        self.assertTrue(any("vr-after.prom" in e for e in rows["B1-rr"]["errors"]))
        self.assertAlmostEqual(rows["B1-rr"]["worker_hit_rate"], 0.25)  # the workers' counters were still readable
        self.assertEqual(rows["B1-vr-hash"]["pass_rule"]["verdict"], "pass")
        self.assertIn("unreadable input", out)

    def test_an_arm_dir_without_meta_json_exits_2(self):
        os.makedirs(self.arm("B1-lost"))
        rc, out, _, rows = self.rows()
        self.assertEqual(rc, 2)
        self.assertEqual(rows["B1-lost"]["errors"][0].endswith("meta.json: missing"), True)

    def test_a_skipped_arm_is_listed_and_does_not_fail_the_summary(self):
        os.makedirs(self.arm("B1-extra"))
        self.write("B1-extra", "SKIPPED", "worker 2 never became ready\n")
        rc, out, _, rows = self.rows()
        self.assertEqual(rc, 0)
        self.assertTrue(rows["B1-extra"]["skipped"])
        self.assertIn("skipped: worker 2 never became ready", out)

    def test_a_pass_rule_without_the_rr_arm(self):
        shutil.rmtree(self.arm("B1-rr"))
        rc, out, _, rows = self.rows()
        self.assertEqual(rc, 0)
        self.assertEqual({n: r["pass_rule"]["verdict"] for n, r in rows.items()},
                         {"B1-vr-hash": "no rr baseline", "B1-envoy-bound": "no rr baseline", "B1-envoy-nobound": "no rr baseline"})


class ProbeTest(unittest.TestCase):
    """The dead-worker probe's arm: its row comes from probe.json alone, its checks are the probe's own lines,
    and a FAIL line fails the summary (1); a probe arm without probe.json is unreadable (2)."""

    META = {"name": "H1-probe", "regime": "H", "rep": 1, "arm": "probe", "router": "envoy", "policy": None, "bound": "125",
            "kv_blocks": 6000, "start_unix": 1.0, "end_unix": 2.0, "client_rc": 0, "nwork": 4, "load_args": "", "mode": "double",
            "reset": "post"}
    PASS = ["ok the victim was killed mid-stream", "ok siblings' streams uncut", "ok no 503 while the siblings are healthy",
            "ok nothing reaches the restarted worker before its verification", "ok the verified worker takes its key's requests again"]

    def tree(self, checks, with_probe=True):
        d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, d)
        arm = os.path.join(d, "arms", "H1-probe")
        os.makedirs(arm)
        with open(os.path.join(arm, "meta.json"), "w") as f:
            json.dump(self.META, f)
        if with_probe:
            with open(os.path.join(arm, "probe.json"), "w") as f:
                json.dump({"victim": "worker-3", "checks": checks, "samples": [{"status": 200}] * 3, "non_200": 0,
                           "victim_healthy_s": 30.0, "victim_verified_s": 45.5, "victim_admitted_s": 50.0,
                           "max_latency_during_outage_ms": 812.0}, f)
        return d

    def test_passing_probe(self):
        rows = analyze.analyse(self.tree(self.PASS))
        self.assertEqual(len(rows), 1)
        self.assertEqual([c["ok"] for c in rows[0]["checks"]], [True] * 5)
        self.assertEqual(rows[0]["checks"][2]["check"], "no 503 while the siblings are healthy")
        self.assertNotIn("pass_rule", rows[0])  # the pass rule is the arms', not the probe's
        self.assertEqual(analyze.status_of(rows), 0)
        md = analyze.markdown(rows)
        self.assertIn("victim worker-3", md)
        self.assertIn("admitted 50.0 s after the kill", md)  # the probe's own figure, printed as given

    def test_failing_probe(self):
        checks = self.PASS[:2] + ["FAIL no 503 while the siblings are healthy: 2 of 40 probe requests failed"] + self.PASS[3:]
        rows = analyze.analyse(self.tree(checks))
        self.assertEqual(analyze.status_of(rows), 1)
        self.assertEqual(rows[0]["checks"][2], {"check": "no 503 while the siblings are healthy", "ok": False,
                                                 "reason": "2 of 40 probe requests failed"})
        self.assertIn("FAIL no 503 while the siblings are healthy", analyze.markdown(rows))

    def test_probe_without_its_record(self):
        rows = analyze.analyse(self.tree([], with_probe=False))
        self.assertEqual(analyze.status_of(rows), 2)

    def test_probe_with_a_malformed_line(self):
        rows = analyze.analyse(self.tree(["maybe fine"]))
        self.assertEqual(analyze.status_of(rows), 2)


class ClientRecordTest(unittest.TestCase):
    """A measured record without its timing is unreadable input, never a crash; a failed attempt needs none."""

    def write(self, lines):
        d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, d)
        with open(os.path.join(d, "client.jsonl"), "w") as f:
            f.write("\n".join(json.dumps(x) for x in lines) + "\n")
        return d

    def test_missing_ttft_is_unreadable(self):
        d = self.write([{"run": "B1-rr", "latency_ms": 5, "prompt_tokens": 10}])
        with self.assertRaises(analyze.Unreadable):
            analyze.load_client(d)

    def test_failed_attempt_needs_no_timing(self):
        d = self.write([{"run": "B1-rr", "err": "http 503"}])
        self.assertEqual(len(analyze.load_client(d)), 1)


class RelayControlTest(unittest.TestCase):
    """The relay control's row: TTFT percentiles per path from its own records, the relayed minus the direct, and
    its three checks. Hand arithmetic: direct TTFTs 10, 20, 30, 40 (n=4): p50 rank ceil(2.0)=2 -> 20, p95 and p99
    rank 4 -> 40; relayed 11, 22, 33, 44: p50 22, p95 44, p99 44; added p50 2, p95 4, p99 4."""

    META = {"name": "A1-relay-control", "regime": "A", "rep": 1, "arm": "relay-control", "router": "relay-control", "policy": None,
            "bound": None, "kv_blocks": 33010, "start_unix": 1.0, "end_unix": 2.0, "client_rc": 0, "nwork": 4, "load_args": "", "mode": "double",
            "reset": "post"}

    def tree(self, relayed_ttfts, conns="1"):
        d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, d)
        arm = os.path.join(d, "arms", "A1-relay-control")
        os.makedirs(arm)
        with open(os.path.join(arm, "meta.json"), "w") as f:
            json.dump(self.META, f)
        for path, ttfts in (("direct", [10, 20, 30, 40]), ("relayed", relayed_ttfts)):
            with open(os.path.join(arm, f"client-{path}.jsonl"), "w") as f:
                for x in ttfts:
                    f.write(json.dumps({"ttft_ms": x, "latency_ms": x + 100, "prompt_tokens": 50, "cached_tokens": -1}) + "\n")
        with open(os.path.join(arm, "relay-counts.tsv"), "w") as f:
            f.write("127.0.0.1:30422\t" + conns + "\n")
        return d

    def test_added_latency(self):
        rows = analyze.analyse(self.tree([11, 22, 33, 44]))
        c = rows[0]["relay_control"]
        self.assertEqual((c["direct"]["ttft_p50_ms"], c["direct"]["ttft_p95_ms"]), (20, 40))
        self.assertEqual((c["relayed"]["ttft_p50_ms"], c["relayed"]["ttft_p95_ms"], c["relayed"]["ttft_p99_ms"]), (22, 44, 44))
        self.assertEqual(c["added_ms"], {"p50": 2, "p95": 4, "p99": 4})
        self.assertEqual(analyze.status_of(rows), 0)
        self.assertNotIn("pass_rule", rows[0])
        self.assertIn("Relay control A1-relay-control", analyze.markdown(rows))

    def test_a_relay_that_carried_nothing_fails(self):
        rows = analyze.analyse(self.tree([11, 22, 33, 44], conns="0"))
        self.assertEqual(analyze.status_of(rows), 1)

    def test_a_path_without_records_fails(self):
        rows = analyze.analyse(self.tree([]))
        self.assertEqual(analyze.status_of(rows), 1)


if __name__ == "__main__":
    unittest.main()
