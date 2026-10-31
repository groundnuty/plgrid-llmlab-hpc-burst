"""Spike S1c: unit tests for the result rules (s1c_analysis.py, append_row, summarize.pick).
Each test sets its own inputs; the expected values follow from those inputs.

    python3 tests/test_analysis.py                  (from spikes/s1c)
"""
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from s1c_analysis import detection, hol_validity, per_ktok   # noqa: E402
from s1c_lib import append_row                                # noqa: E402
import summarize                                              # noqa: E402

T0 = 1000.0


class Detection(unittest.TestCase):
    def test_api_error_is_not_detection(self):
        # The first version took any non-"online" state as detection, so this run read 0.2 s.
        rows = [("api", T0 - 5, T0 - 5, "online", "p"), ("api", T0 + 0.2, T0 + 0.2, "api-error", "p"),
                ("api", T0 + 10, T0 + 10, "offline", "p")]
        d = detection(rows, T0)
        self.assertEqual(d["api_gone"], T0 + 10)
        self.assertEqual(d["api_errors"], 1)

    def test_absent_counts_and_missing_is_censored(self):
        self.assertEqual(detection([("api", T0 + 3, T0 + 3, "absent", "p")], T0)["api_gone"], T0 + 3)
        d = detection([("api", T0 + 1, T0 + 1, "api-401", "p")], T0)
        self.assertIsNone(d["api_gone"])
        self.assertEqual(d["api_errors"], 1)

    def test_probe_deadline_is_counted_as_censored_wait(self):
        rows = [("probe", T0 + 0.1, T0 + 15.1, "timeout", "no reply"),
                ("probe", T0 + 0.3, T0 + 10.3, "closed", "EOF"),
                ("probe", T0 + 12, T0 + 12.01, "refused", "404")]
        d = detection(rows, T0)
        self.assertEqual((d["hung_n"], d["hung_timeouts"]), (2, 1))
        self.assertAlmostEqual(d["hung_max"], 15.0)
        self.assertEqual(d["first_refused"], T0 + 12)


class HolValidity(unittest.TestCase):
    def test_refused_bulk_is_invalid(self):
        # A refused bulk CONNECT used to yield a clean-looking run: no bytes, no delay.
        s = {"bulk_err": "refused: HTTP/1.1 404 Not Found", "bulk_bytes_read": 0}
        self.assertTrue(hol_validity(s, 4 << 20, 0, 20))
        self.assertTrue(hol_validity({"bulk_err": "", "bulk_bytes_read": 1000}, 4 << 20, 0, 20))

    def test_full_reply_and_abandoned_slow_reader_are_valid(self):
        self.assertEqual(hol_validity({"bulk_err": "", "bulk_bytes_read": 4 << 20}, 4 << 20, 0, 20), "")
        slow = {"bulk_err": "gave up after 12s (by design for a slow reader)", "bulk_bytes_read": 65536 * 12}
        self.assertEqual(hol_validity(slow, 64 << 20, 65536, 12), "")

    def test_slow_reader_that_did_not_read_or_did_not_give_up_is_invalid(self):
        self.assertTrue(hol_validity({"bulk_err": "gave up", "bulk_bytes_read": 0}, 64 << 20, 65536, 12))
        self.assertTrue(hol_validity({"bulk_err": "", "bulk_bytes_read": 65536 * 12}, 64 << 20, 65536, 12))


class CpuPerToken(unittest.TestCase):
    def test_same_interval(self):
        # 30% of a core for 30 s while 136,110 tokens arrived (4,537 tokens/s over the run):
        # 30 / 4.537 = 6.61% per 1,000 tokens/s. The first version divided by the steady-window rate
        # (5,044 tokens/s), which gives 5.95, about 10% too low.
        self.assertAlmostEqual(per_ktok(30.0, 30.0, 136110), 6.612, places=2)
        self.assertIsNone(per_ktok(30.0, 30.0, 0))


class Rows(unittest.TestCase):
    def test_append_row_refuses_other_columns(self):
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "x.csv")
            append_row(p, {"a": 1, "b": 2})
            append_row(p, {"b": 3, "a": 4})            # same columns, other order: kept in file order
            with self.assertRaises(ValueError):
                append_row(p, {"a": 1, "c": 2})
            with open(p) as fh:
                self.assertEqual(fh.read().split(), ["a,b", "1,2", "4,3"])

    def test_pick_skips_invalid_and_loaded_attempts(self):
        rs = [dict(pass_="1", k="x", load_end="2.0", valid="yes", v="good"),
              dict(pass_="1", k="x", load_end="2.0", valid="bulk error: refused", v="invalid"),
              dict(pass_="1", k="x", load_end="4.5", valid="yes", v="loaded")]
        self.assertEqual(summarize.pick(rs, lambda r: r["k"])["x"][1]["v"], "good")


if __name__ == "__main__":
    unittest.main()
