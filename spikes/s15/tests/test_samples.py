"""Unit test of the driver's sample writer (s15.write_samples): what the committed samples.csv and host.csv
hold for known samples."""
import csv
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import s15  # noqa: E402


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
            s15.write_samples(d, "run-a", samples)
            s15.write_samples(d, "run-b", samples[:1])
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


if __name__ == "__main__":
    unittest.main()
