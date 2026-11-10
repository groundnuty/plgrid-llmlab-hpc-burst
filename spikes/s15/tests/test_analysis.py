"""Unit tests of spike S15's result rules (analysis.py). Expected values come from the inputs each test
sets, not from the harness."""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import analysis  # noqa: E402

S = 1_000_000_000
MS = 1_000_000


def stream(start, end, every_ms=50, stall=None):
    """One request streaming from start to end (ns), a token every every_ms, with an optional stall
    (from, to) during which no token arrives."""
    toks, t = [], start + 50 * MS
    while t < end:
        if not (stall and stall[0] <= t < stall[1]):
            toks.append(t)
        t += every_ms * MS
    return {"start_ns": start, "end_ns": end, "tokens_ns": toks, "status": 200}


def load(runs, transfers, ref=(1 * S, 11 * S)):
    return {"streams": len(runs), "streams_runs": runs, "transfers": transfers,
            "reference_start_ns": ref[0], "reference_end_ns": ref[1]}


def tr(start, end, nbytes=64 << 20, err="", status=200, first=None, last=None, sent=None, steps=None):
    return {"kind": "download", "start_ns": start, "end_ns": end, "bytes": nbytes, "err": err, "status": status,
            "recv_first_ns": first if first is not None else start, "recv_last_ns": last if last is not None else end,
            "sent_ns": sent, "steps_bytes": steps or []}


class Validity(unittest.TestCase):
    def test_a_stall_inside_a_transfer_is_the_measurement_not_a_pause(self):
        runs = [[stream(0, 40 * S, stall=(15 * S, 16 * S + 500 * MS))] for _ in range(2)]
        v = analysis.validity(load(runs, [tr(15 * S, 17 * S)]), "download", 64 << 20)
        self.assertTrue(v["valid"], v["reason"])

    def test_a_stream_between_requests_during_a_transfer_invalidates_it(self):
        runs = [[stream(0, 40 * S)], [stream(0, 15 * S + 200 * MS), stream(16 * S, 40 * S)]]
        v = analysis.validity(load(runs, [tr(15 * S, 17 * S)]), "download", 64 << 20)
        self.assertFalse(v["valid"])
        self.assertIn("stream 1", v["reason"])

    def test_a_transfer_that_ends_before_the_streams_start_invalidates_it(self):
        runs = [[stream(20 * S, 60 * S)]]
        v = analysis.validity(load(runs, [tr(12 * S, 14 * S)], ref=(21 * S, 31 * S)), "download", 64 << 20)
        self.assertFalse(v["valid"])

    def test_a_failed_or_short_transfer_invalidates_it(self):
        runs = [[stream(0, 40 * S)]]
        for bad in (tr(15 * S, 17 * S, err="cut"), tr(15 * S, 17 * S, nbytes=10), tr(15 * S, 17 * S, status=502)):
            self.assertFalse(analysis.validity(load(runs, [bad]), "download", 64 << 20)["valid"])

    def test_an_empty_second_in_the_reference_window_invalidates_it(self):
        runs = [[stream(0, 40 * S, stall=(3 * S, 4 * S + 200 * MS))]]
        v = analysis.validity(load(runs, [tr(15 * S, 17 * S)]), "download", 64 << 20)
        self.assertFalse(v["valid"])
        self.assertIn("reference", v["reason"])


class Gaps(unittest.TestCase):
    def test_gaps_overlapping_a_transfer_are_its_and_the_rest_are_the_references(self):
        runs = [[stream(0, 40 * S, stall=(15 * S, 15 * S + 300 * MS))]]
        g = analysis.gap_stats(load(runs, [tr(15 * S, 17 * S)]))
        self.assertEqual(g["gap_bulk_max_ms"], 350.0)  # the stall: 300 ms with no token, plus one 50 ms step
        self.assertEqual(g["gap_ref_max_ms"], 50.0)
        self.assertEqual(g["stall_max_ms"], 300.0)
        self.assertEqual(g["gap_ref_n"], 200)  # tokens at 1.00, 1.05, ... 11.00 s, both ends inside: 201 tokens, 200 gaps


class Metrics(unittest.TestCase):
    def test_rate_burst_and_the_rate_after_the_first_second_at_the_receiver(self):
        # 30 MB over 3 s at the receiver: 10 MB in the first second (a burst), then 10 MB/s.
        steps = [1_000_000 * (i + 1) for i in range(10)] + [10_000_000 + 1_000_000 * (i + 1) for i in range(20)]
        t = tr(0, 3 * S + 10 * MS, nbytes=30_000_000, first=0, last=3 * S, steps=steps)
        m = analysis.transfer_metrics([t], "embeddings", 64 << 20)
        self.assertEqual(m["transfers_ok"], 1)
        self.assertEqual(m["recv_rate_median"], 10_000_000)
        self.assertEqual(m["first_second_bytes_median"], 10_000_000)
        self.assertEqual(m["after_first_second_rate_median"], 10_000_000)

    def test_an_upload_reports_how_long_after_the_clients_last_write_the_receiver_had_it(self):
        t = tr(0, 2 * S, first=10 * MS, last=1500 * MS, sent=1200 * MS)
        t["kind"] = "upload"
        m = analysis.transfer_metrics([t], "upload", 64 << 20)
        self.assertEqual(m["upload_recv_last_after_sent_ms_median"], 300.0)

    def test_a_failed_transfer_is_not_counted(self):
        m = analysis.transfer_metrics([tr(0, S, err="cut")], "download", 64 << 20)
        self.assertEqual((m["transfers"], m["transfers_ok"], m["recv_rate_median"]), (1, 0, ""))


if __name__ == "__main__":
    unittest.main()
