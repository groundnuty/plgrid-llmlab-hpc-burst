"""Tests of analyze.py. Expected values are worked out by hand from the fixtures, not from the code.

Run: python3 -I spikes/s2b/scripts/test_analyze.py
"""
import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import analyze  # noqa: E402

PROM_BEFORE = """# HELP vllm:prefix_cache_hits_total x
vllm:prefix_cache_hits_total{engine="0",model_name="m"} 100.0
vllm:prefix_cache_queries_total{engine="0",model_name="m"} 400.0
vllm:prefix_cache_queries_created{engine="0",model_name="m"} 1.7e9
"""
PROM_AFTER = """vllm:prefix_cache_hits_total{engine="0",model_name="m"} 700.0
vllm:prefix_cache_queries_total{engine="0",model_name="m"} 1400.0
vllm:prefix_cache_queries_created{engine="0",model_name="m"} 1.7e9
"""


class PromTest(unittest.TestCase):
    def test_sum_ignores_other_names(self):
        self.assertEqual(analyze.prom_sum(PROM_BEFORE, "vllm:prefix_cache_queries_total"), 400.0)
        self.assertIsNone(analyze.prom_sum(PROM_BEFORE, "vllm:num_preemptions_total"))

    def test_label_sets_are_summed(self):
        t = 'x_total{a="1"} 2\nx_total{a="2"} 3\nx_total_created 9\n'
        self.assertEqual(analyze.prom_sum(t, "x_total"), 5.0)


class PctTest(unittest.TestCase):
    def test_nearest_rank(self):
        v = list(range(1, 11))  # 1..10
        self.assertEqual(analyze.pct(v, 0.50), 5)
        self.assertEqual(analyze.pct(v, 0.95), 10)
        self.assertEqual(analyze.pct([7], 0.95), 7)
        self.assertIsNone(analyze.pct([], 0.5))


def rec(s, t, agent, start, prompt, cached, completion=10, err=""):
    return {"run": "R", "session": s, "turn": t, "agent": agent, "start_unix": start, "ttft_ms": 100.0 + t,
            "latency_ms": 500.0, "prompt_tokens": prompt, "cached_tokens": cached, "completion_tokens": completion,
            "status": 200, "attempt": 1, "err": err}


class ReuseBoundTest(unittest.TestCase):
    def test_history_and_shared_system_prompt(self):
        # Session 0 (agent 0): turn 0 prompt 100, turn 1 prompt 150 (prev 100 + 10 reply = 110 -> 96 in blocks of 16).
        # Session 1 (agent 0) starts later: turn 0 prompt 100, reported 64 cached (the shared system prompt).
        recs = [rec(0, 0, 0, 1.0, 100, 0), rec(0, 1, 0, 2.0, 150, 96), rec(1, 0, 0, 3.0, 100, 64)]
        # reusable = 0 (first session of its type) + 96 + 64 = 160 of 350 prompt tokens
        self.assertAlmostEqual(analyze.reuse_bound(recs), 160 / 350)

    def test_failed_requests_ignored(self):
        recs = [rec(0, 0, 0, 1.0, 100, 0), rec(0, 1, 0, 2.0, 150, 0, err="boom")]
        self.assertAlmostEqual(analyze.reuse_bound(recs), 0.0)


class ClientStatsTest(unittest.TestCase):
    def test_client_hit_rate_and_failures(self):
        recs = [rec(0, 0, 0, 1.0, 100, 50), rec(0, 1, 0, 2.0, 300, 250), rec(0, 2, 0, 3.0, 50, -1), rec(1, 0, 1, 4.0, 10, 0, err="x")]
        s = analyze.client_stats(recs)
        self.assertEqual(s["requests_ok"], 3)
        self.assertEqual(s["failed_attempts"], 1)
        # only requests that reported cached tokens count: (50 + 250) / (100 + 300)
        self.assertAlmostEqual(s["client_hit_rate"], 0.75)
        self.assertEqual(s["ttft_p50_ms"], 101.0)


class PlacementTest(unittest.TestCase):
    def test_spread_and_stickiness(self):
        # session 0: all three turns on worker 1 (sticky); session 1: worker 0, 2, 2 (not sticky).
        rows = [(1, 0, 0, "R-s000"), (1, 0, 1, "R-s000"), (1, 0, 2, "R-s000"),
                (0, 1, 0, "-"), (2, 1, 1, "-"), (2, 1, 2, "-")]
        p = analyze.placement(rows, 4)
        self.assertEqual(p["sessions_per_worker"], [1, 1, 0, 0])
        self.assertEqual(p["requests_per_worker"], [1, 3, 2, 0])
        self.assertAlmostEqual(p["request_imbalance_max_over_mean"], 3 / 1.5)
        self.assertAlmostEqual(p["sticky_sessions"], 0.5)
        self.assertAlmostEqual(p["mean_workers_per_session"], 1.5)
        # turn pairs: s0 (1,1),(1,1) same; s1 (0,2) moved, (2,2) same -> 3 of 4
        self.assertAlmostEqual(p["same_worker_as_previous_turn"], 0.75)
        self.assertAlmostEqual(p["header_seen_by_worker"], 0.5)

    def test_retry_keeps_last_placement(self):
        rows = [(0, 0, 0, "-"), (3, 0, 0, "-")]
        self.assertEqual(analyze.placement(rows, 4)["requests_per_worker"], [0, 0, 0, 1])


class EndToEndTest(unittest.TestCase):
    def test_worker_hit_rate_from_counters(self):
        with tempfile.TemporaryDirectory() as d:
            arm = os.path.join(d, "arms", "B1-rr")
            os.makedirs(arm)
            for i in range(2):
                open(os.path.join(arm, f"before-w{i}.prom"), "w").write(PROM_BEFORE)
                open(os.path.join(arm, f"after-w{i}.prom"), "w").write(PROM_AFTER)
            json.dump({"name": "B1-rr", "regime": "B", "rep": 1, "arm": "rr", "kv_blocks_override": "6000",
                       "start_unix": 10.0, "end_unix": 70.0, "client_rc": 0, "nwork": 2, "load_args": ""},
                      open(os.path.join(arm, "meta.json"), "w"))
            with open(os.path.join(arm, "client.jsonl"), "w") as f:
                f.write(json.dumps(rec(0, 0, 0, 1.0, 100, 0)) + "\n")
            with open(os.path.join(d, "placement-B-w1.tsv"), "w") as f:
                f.write("1.0\t1\tB1-rr\t0\t0\tB1-rr-s000\n1.1\t1\tOTHER\t0\t0\t-\n")
            (row,) = analyze.analyse(d)
            # each worker: hits +600, queries +1000 -> 1200 / 2000
            self.assertAlmostEqual(row["worker_hit_rate"], 0.6)
            self.assertEqual(row["worker_hit_rate_per_worker"], [0.6, 0.6])
            self.assertEqual(row["placement"]["requests_per_worker"], [0, 1])
            self.assertEqual(row["wall_s"], 60.0)
            self.assertIn("| B1-rr | rr | 0.600 |", analyze.markdown([row]))


if __name__ == "__main__":
    unittest.main()
