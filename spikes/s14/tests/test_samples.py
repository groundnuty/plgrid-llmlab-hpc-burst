"""Unit test of the driver's sample writer (s14.write_samples): what the committed samples.csv and host.csv
hold for known samples."""
import csv
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import s14  # noqa: E402


class Samples(unittest.TestCase):
    def test_each_role_keeps_its_pid_count_ticks_memory_and_descriptors(self):
        samples = [
            {"t": 10.0, "ticks": {"frpc": {11: 5, 12: 7}, "frps": {3: 100}}, "rss_kb": {"frpc": 2000, "frps": 900},
             "hwm_kb": {"frpc": 1100, "frps": 950}, "fds": {"frpc": 16, "frps": 12},
             "stat": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], "mem_avail_kb": 123, "load": [0.5, 0.4, 0.3]},
            # A frpc PID missing from a sample shows as one PID fewer.
            {"t": 12.0, "ticks": {"frpc": {11: 9}, "frps": {3: 140}}, "rss_kb": {"frpc": 1000, "frps": 910},
             "hwm_kb": {"frpc": 1100, "frps": 950}, "fds": {"frpc": 8, "frps": 12},
             "stat": [2, 2, 4, 6, 5, 6, 7, 8, 9, 10], "mem_avail_kb": 120, "load": [0.6, 0.4, 0.3]},
        ]
        with tempfile.TemporaryDirectory() as d:
            s14.write_samples(d, "run-a", samples)
            s14.write_samples(d, "run-b", samples[:1])
            with open(os.path.join(d, "samples.csv")) as fh:
                rows = list(csv.DictReader(fh))
            with open(os.path.join(d, "host.csv")) as fh:
                host = list(csv.DictReader(fh))
        self.assertEqual(len(rows), 6)
        first = [r for r in rows if r["run_id"] == "run-a" and r["t"] == "10.000000" and r["role"] == "frpc"][0]
        self.assertEqual((first["pids"], first["ticks"], first["rss_kb"], first["fds"]), ("2", "12", "2000", "16"))
        second = [r for r in rows if r["run_id"] == "run-a" and r["t"] == "12.000000" and r["role"] == "frpc"][0]
        self.assertEqual((second["pids"], second["ticks"]), ("1", "9"))
        self.assertEqual(len(host), 3)
        self.assertEqual((host[1]["user"], host[1]["idle"], host[1]["steal"], host[1]["mem_avail_kb"], host[1]["load1"]),
                         ("2", "6", "8", "120", "0.6"))


class Idle(unittest.TestCase):
    def test_the_idle_window_ends_where_the_client_ended_if_that_came_first(self):
        # The client ended at t=20 of a nominal 0-30 window, and the load started then: the samples
        # after 20 are the load's, not idle.
        def sample(t, ticks):
            return {"t": t, "ticks": {"frps": {7: ticks}}}
        samples = [sample(0, 0), sample(10, 10), sample(20, 20), sample(25, 500), sample(30, 1000)]
        idle = {"window_start_unix_ns": 0, "window_end_unix_ns": 30_000_000_000, "end_unix_ns": 20_000_000_000,
                "probes_ok": 2, "probes_failed": 0, "per_tunnel": [{"probes_ok": 2, "tokens": 0}]}
        row = {}
        s14.summarize_idle(row, idle, type("S", (), {"samples": samples})())
        self.assertEqual(row["idle_end_s"], 20.0)
        self.assertAlmostEqual(row["idle_cpu_frps"], 0.01)  # 20 ticks at 100 per second over 20 s

    def test_a_client_that_waited_out_its_window_keeps_the_whole_window(self):
        samples = [{"t": t, "ticks": {"frps": {7: t}}} for t in (0, 10, 20, 30)]
        idle = {"window_start_unix_ns": 0, "window_end_unix_ns": 30_000_000_000, "end_unix_ns": 30_000_500_000,
                "probes_ok": 3, "probes_failed": 0, "per_tunnel": [{"probes_ok": 3, "tokens": 0}]}
        row = {}
        s14.summarize_idle(row, idle, type("S", (), {"samples": samples})())
        self.assertEqual((row["idle_end_s"], row["idle_samples"]), (30.0, 4))


if __name__ == "__main__":
    unittest.main()
