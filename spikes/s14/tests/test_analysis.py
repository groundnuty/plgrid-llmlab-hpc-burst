"""Unit tests of spike S14's result rules (analysis.py). Expected values come from the inputs each
test sets, not from the harness."""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import analysis  # noqa: E402


def tun(tokens, buckets, requests=1):
    return {"tokens": tokens, "buckets": buckets, "requests": requests}


class Validity(unittest.TestCase):
    def test_every_tunnel_streaming_in_every_bucket_is_valid(self):
        v = analysis.tunnel_validity([tun(40, [20, 20]), tun(39, [19, 20], 2)], 2)
        self.assertTrue(v["valid"])
        self.assertEqual((v["silent"], v["gapped"], v["tokens_min"], v["requests_max"]), (0, 0, 39, 2))

    def test_a_registered_tunnel_that_carried_nothing_invalidates_the_configuration(self):
        v = analysis.tunnel_validity([tun(40, [20, 20]), tun(0, [0, 0], 0)], 2)
        self.assertFalse(v["valid"])
        self.assertEqual(v["silent"], 1)

    def test_one_silent_second_on_one_tunnel_invalidates_it(self):
        v = analysis.tunnel_validity([tun(40, [20, 20]), tun(20, [20, 0])], 2)
        self.assertFalse(v["valid"])
        self.assertEqual((v["silent"], v["gapped"]), (0, 1))

    def test_a_missing_tunnel_invalidates_it(self):
        self.assertFalse(analysis.tunnel_validity([tun(40, [20, 20])], 2)["valid"])


class Cpu(unittest.TestCase):
    def test_cores_sums_each_pid_over_its_own_samples(self):
        # Two frpc over 10 s: pid 1 uses 500 ticks (5 s at 100/s), pid 2 appears at t=5 and uses
        # 100 ticks after it: (5 + 1) s of CPU over 10 s is 0.6 cores.
        s = [{"t": 0.0, "ticks": {"frpc": {1: 0}}},
             {"t": 5.0, "ticks": {"frpc": {1: 250, 2: 1000}}},
             {"t": 10.0, "ticks": {"frpc": {1: 500, 2: 1100}}}]
        self.assertAlmostEqual(analysis.cores(s, "frpc", 100), 0.6)

    def test_cores_needs_two_samples(self):
        self.assertIsNone(analysis.cores([{"t": 0.0, "ticks": {"frps": {1: 5}}}], "frps", 100))

    def test_window_samples_cuts_to_the_window(self):
        s = [{"t": t} for t in (0, 1, 2, 3, 4)]
        self.assertEqual([x["t"] for x in analysis.window_samples(s, 1, 3)], [1, 2, 3])

    def test_busy_counts_idle_and_iowait_as_idle(self):
        a = [0, 0, 0, 0, 0, 0, 0, 0]
        b = [30, 0, 10, 50, 10, 0, 0, 0]  # 40 busy of 100
        self.assertAlmostEqual(analysis.busy_cores(a, b), 0.4)

    def test_host_bound(self):
        self.assertTrue(analysis.host_bound(0.9))
        self.assertFalse(analysis.host_bound(0.5))
        self.assertFalse(analysis.host_bound(None))


class Drops(unittest.TestCase):
    def test_an_offline_or_reregistered_proxy_after_readiness_is_a_drop(self):
        on = lambda st: {"status": "online", "lastStartTime": st}  # noqa: E731
        polls = [
            {"t": 0, "proxies": {"a": {"status": "offline"}}},  # before readiness: ignored
            {"t": 10, "proxies": {"a": on("x"), "b": on("y"), "c": on("z")}},
            {"t": 20, "proxies": {"a": on("x"), "b": on("y2"), "c": {"status": "offline"}}},
            {"t": 30, "error": "timeout"},
        ]
        dropped, unreadable = analysis.drops(polls, ["a", "b", "c"], after=5)
        self.assertEqual(dropped, {"b", "c"})
        self.assertEqual(unreadable, 1)

    def test_an_absent_proxy_is_a_drop(self):
        dropped, _ = analysis.drops([{"t": 10, "proxies": {}}], ["a"], after=0)
        self.assertEqual(dropped, {"a"})

    def test_logins(self):
        log = ("[I] login to server success, get run id [r1]\n[W] heartbeat timeout\n"
               "[I] login to server success, get run id [r1]\n")
        self.assertEqual(analysis.logins(log), 2)


if __name__ == "__main__":
    unittest.main()
