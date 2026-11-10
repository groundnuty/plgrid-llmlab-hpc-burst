"""Unit test of the committed form of the large results: compress writes .csv.xz that read back as the
CSV, and the readers take either form."""
import csv
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import summarize  # noqa: E402


class Compressed(unittest.TestCase):
    def test_compress_keeps_every_byte_and_the_readers_take_the_xz(self):
        with tempfile.TemporaryDirectory() as root:
            rep = os.path.join(root, "rep1")
            os.makedirs(rep)
            rows = [["run_id", "t", "role", "ticks"]] + [["r", "%.6f" % (i / 2), "frps", str(i)] for i in range(500)]
            plain = os.path.join(rep, "samples.csv")
            with open(plain, "w", newline="") as fh:
                csv.writer(fh).writerows(rows)
            with open(plain, "rb") as fh:
                before = fh.read()
            summarize.compress(root)
            self.assertFalse(os.path.exists(plain), "the plain copy is removed")
            self.assertTrue(os.path.exists(plain + ".xz"))
            with summarize.open_csv(plain) as fh:
                self.assertEqual(fh.read().encode(), before)
            with self.assertRaises(FileNotFoundError):
                summarize.open_csv(os.path.join(rep, "missing.csv"))


if __name__ == "__main__":
    unittest.main()
