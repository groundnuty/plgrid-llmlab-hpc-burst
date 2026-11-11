"""Unit tests of spike S17's result rules (analysis.py). Expected values come from the inputs each test
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



def btr(start, end, batch, nbytes=64 << 20, **kw):
    t = tr(start, end, nbytes, **kw)
    t["batch"] = batch
    return t


class Concurrent(unittest.TestCase):
    """Issue #413: a concurrent run whose transfers did not overlap in time proves nothing."""

    def runs(self):
        return [[stream(0, 60 * S)] for _ in range(2)]

    def test_transfers_that_overlap_fully_are_valid(self):
        batch = [btr(15 * S, 20 * S, 0), btr(15 * S + MS, 20 * S, 0), btr(15 * S, 19 * S + 800 * MS, 0), btr(15 * S + 2 * MS, 20 * S, 0)]
        v = analysis.validity(load(self.runs(), batch), "download", 64 << 20, concurrent=4)
        self.assertTrue(v["valid"], v["reason"])
        self.assertGreater(min(analysis.batch_overlap(batch).values()), 0.99)

    def test_transfers_one_after_the_other_are_not_valid(self):
        batch = [btr(15 * S, 17 * S, 0), btr(17 * S + MS, 19 * S, 0)]  # back to back: nothing in common
        v = analysis.validity(load(self.runs(), batch), "download", 64 << 20, concurrent=2)
        self.assertFalse(v["valid"])
        self.assertIn("overlapped", v["reason"])
        self.assertLessEqual(analysis.batch_overlap(batch)[0], 0.0)

    def test_a_staggered_batch_below_the_threshold_is_not_valid(self):
        # The second starts when the first has run 40% of its time: 60% in common, under 80%.
        batch = [btr(15 * S, 20 * S, 0), btr(17 * S, 20 * S, 0)]
        self.assertAlmostEqual(analysis.batch_overlap(batch)[0], 1.0, places=2)  # in common with the SHORTER span: all of it
        batch = [btr(15 * S, 20 * S, 0), btr(17 * S, 22 * S, 0)]  # equal spans, 3 s of 5 in common
        self.assertAlmostEqual(analysis.batch_overlap(batch)[0], 0.6, places=2)
        v = analysis.validity(load(self.runs(), batch), "download", 64 << 20, concurrent=2)
        self.assertFalse(v["valid"])

    def test_a_batch_with_a_missing_transfer_is_not_valid(self):
        v = analysis.validity(load(self.runs(), [btr(15 * S, 20 * S, 0)]), "download", 64 << 20, concurrent=2)
        self.assertFalse(v["valid"])
        self.assertIn("1 of 2", v["reason"])

    def test_every_batch_is_held_to_it(self):
        good = [btr(15 * S, 20 * S, 0), btr(15 * S, 20 * S, 0)]
        bad = [btr(25 * S, 27 * S, 1), btr(28 * S, 30 * S, 1)]
        v = analysis.validity(load(self.runs(), good + bad), "download", 64 << 20, concurrent=2)
        self.assertFalse(v["valid"])
        self.assertIn("batch 1", v["reason"])

    def test_a_single_transfer_batch_needs_no_overlap(self):
        v = analysis.validity(load(self.runs(), [btr(15 * S, 20 * S, 0), btr(25 * S, 30 * S, 1)]), "download", 64 << 20, concurrent=1)
        self.assertTrue(v["valid"], v["reason"])

    def test_batch_metrics_report_the_batch_time_and_aggregate_rate(self):
        batch = [btr(10 * S, 14 * S, 0, last=14 * S), btr(10 * S, 14 * S, 0, last=14 * S)]
        m = analysis.batch_metrics(batch, 2)
        self.assertEqual(m["batch_time_median_s"], 4.0)
        self.assertEqual(m["batch_rate_median"], round(2 * (64 << 20) / 4.0))
        self.assertEqual(m["overlap_min"], 1.0)


def breq(instance, start, end, first=None, nbytes=1 << 20, err="", status=200):
    return {"instance": instance, "start_ns": start, "resp_ns": start + MS, "first_ns": first if first is not None else start + 7 * MS,
            "end_ns": end, "bytes": nbytes, "status": status, "err": err}


def many(bulk, runs, window=(10 * S, 40 * S), interval=10 * S, nbytes=1 << 20):
    return {"bulk": bulk, "streams_runs": runs, "interval_ns": interval, "bytes": nbytes,
            "window_start_ns": window[0], "window_end_ns": window[1]}


class ManyInstances(unittest.TestCase):
    def runs(self, n=3):
        return [[stream(0, 100 * S)] for _ in range(n)]

    def test_requests_at_the_set_rate_beside_streaming_streams_are_valid(self):
        bulk = [breq(i, 11 * S + i * S, 11 * S + i * S + 50 * MS) for i in range(3)]
        self.assertTrue(analysis.validity_many(many(bulk, self.runs()))["valid"])

    def test_a_failed_request_is_not_valid(self):
        bulk = [breq(0, 11 * S, 11 * S + 50 * MS), breq(1, 12 * S, 12 * S + 50 * MS, err="EOF")]
        v = analysis.validity_many(many(bulk, self.runs()))
        self.assertFalse(v["valid"])
        self.assertIn("failed", v["reason"])

    def test_a_short_body_is_not_valid(self):
        v = analysis.validity_many(many([breq(0, 11 * S, 11 * S + 50 * MS, nbytes=1000)], self.runs()))
        self.assertFalse(v["valid"])

    def test_a_request_that_outlasts_the_interval_is_not_valid(self):
        v = analysis.validity_many(many([breq(0, 11 * S, 22 * S)], self.runs()))
        self.assertFalse(v["valid"])
        self.assertIn("interval", v["reason"])

    def test_a_stream_that_ended_before_the_last_request_is_not_valid(self):
        runs = self.runs()
        runs[1] = [stream(0, 15 * S)]
        v = analysis.validity_many(many([breq(0, 11 * S, 11 * S + 50 * MS), breq(1, 20 * S, 20 * S + 50 * MS)], runs))
        self.assertFalse(v["valid"])
        self.assertIn("stream 1", v["reason"])

    def test_metrics_are_the_time_to_first_byte_and_the_gaps_in_the_window(self):
        bulk = [breq(i, 11 * S + i * S, 11 * S + i * S + (20 + i) * MS, first=11 * S + i * S + (5 + 5 * i) * MS) for i in range(3)]
        m = analysis.many_metrics(many(bulk, self.runs()))
        self.assertEqual((m["bulk_requests"], m["bulk_ok"]), (3, 3))
        self.assertEqual(m["ttfb_p50_ms"], 10.0)
        self.assertEqual(m["ttfb_max_ms"], 15.0)
        self.assertEqual(m["total_max_ms"], 22.0)
        self.assertEqual(m["gap_p50_ms"], 50.0)

    def test_handshake_rates_are_the_counter_deltas_over_the_window(self):
        rows = [(int(t * 1e9), 2 * t, 3 * t) for t in range(100, 111)]  # accepts 2/s, hellos 3/s
        r = analysis.handshake_rates(rows, 102.0, 108.0)
        self.assertAlmostEqual(r["accepts_per_s"], 2.0)
        self.assertAlmostEqual(r["hellos_per_s"], 3.0)
        self.assertEqual((r["accepts"], r["hellos"]), (12, 18))
        self.assertIsNone(analysis.handshake_rates(rows, 500.0, 600.0))



class LateBatchStalls(unittest.TestCase):
    """The per-GiB stall of the real ssh path (the review of PR 416): every stream stalls just before a batch ends."""

    def streams(self, n=8, stall=None):
        return [[stream(0, 60 * S, stall=stall)] for _ in range(n)]

    def batches(self):
        # Four batches of four 64 MiB transfers, 10 s each, from 10 s: bytes moved by the end of batch b = (b+1) * 256 MiB.
        return [btr(10 * S + b * 10 * S, 19 * S + b * 10 * S, b) for b in range(4) for _ in range(4)]

    def test_all_streams_stalled_in_the_last_400_ms_of_a_batch_are_found_with_the_bytes_moved(self):
        # Batch 3 ends at 49 s; every stream's tokens stop from 48.7 s to 48.85 s: a gap of about 150 ms, ending 150 ms before it.
        runs = self.streams(stall=(48_650 * MS, 48_850 * MS))
        found = analysis.late_batch_stalls(load(runs, self.batches()))
        self.assertEqual(list(found), [3])
        self.assertEqual(found[3]["streams"], 8)
        self.assertGreater(found[3]["min_ms"], 150)
        self.assertLess(found[3]["max_ms"], 260)
        self.assertAlmostEqual(found[3]["gib"], 4 * 4 * 64 / 1024)
        self.assertGreater(found[3]["before_end_ms"], 100)
        self.assertLess(found[3]["before_end_ms"], 200)

    def test_a_stall_not_near_a_batch_end_is_not_found(self):
        runs = self.streams(stall=(14 * S, 14 * S + 200 * MS))  # in the middle of batch 0
        self.assertEqual(analysis.late_batch_stalls(load(runs, self.batches())), {})

    def test_a_stall_of_a_few_streams_is_not_found(self):
        runs = self.streams(stall=(48_650 * MS, 48_850 * MS))
        for rs in runs[:4]:
            rs[0] = stream(0, 60 * S)  # four of the eight streams do not stall
        self.assertEqual(analysis.late_batch_stalls(load(runs, self.batches())), {})

    def test_a_gap_of_100_ms_or_less_is_not_a_stall(self):
        runs = self.streams(stall=(48_700 * MS, 48_740 * MS))  # the gap is about 90 ms
        self.assertEqual(analysis.late_batch_stalls(load(runs, self.batches())), {})

    def test_batch_ttfb_is_each_transfers_first_byte_in_increasing_order(self):
        trs = [btr(10 * S, 12 * S, 0, first=10 * S + 9 * MS), btr(10 * S, 12 * S, 0, first=10 * S + 5 * MS),
               btr(20 * S, 22 * S, 1, first=20 * S + 7 * MS), btr(20 * S, 22 * S, 1, err="reset")]
        self.assertEqual(analysis.batch_ttfb(trs), {0: [5.0, 9.0], 1: [7.0]})


if __name__ == "__main__":
    unittest.main()
