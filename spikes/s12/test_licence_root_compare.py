"""Tests for licence-root-compare.sh, the report comparison at the end of licence-root-test.sh.
The reports are CSV files written by hand (name, version, licence URL, licence); the expected
verdict of each case is stated in the case.

Run: python3 -m unittest -v test_licence_root_compare   (from this directory)
"""
import os
import subprocess
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(HERE, "licence-root-compare.sh")
MOD = "example.test/proj"

DEPS = ["example.test/dep-a,v1.0.0,u,MIT", "example.test/dep-b,v2.0.0,u,Apache-2.0"]
# V1: no root LICENSE, so each of the three own packages is its own row.
V1 = [MOD + ",Unknown,,Unknown", MOD + "/a,Unknown,,Unknown", MOD + "/b,Unknown,,Unknown"] + DEPS
# V3: a root LICENSE covers all three own packages, which collapse into one row.
V3 = [MOD + ",Unknown,u,Apache-2.0"] + DEPS


def compare(v1, v3, want_own=3):
    """v1 and v3 are lists of lines, or None for a report that does not exist."""
    with tempfile.TemporaryDirectory() as d:
        paths = []
        for name, lines in (("v1.csv", v1), ("v3.csv", v3)):
            f = os.path.join(d, name)
            if lines is not None:
                with open(f, "w") as h:
                    h.write("\n".join(lines) + "\n")
            paths.append(f)
        p = subprocess.run(["bash", SCRIPT, MOD, str(want_own)] + paths, capture_output=True, text=True)
    return p.returncode, p.stdout + p.stderr


class CompareTest(unittest.TestCase):
    def test_matching_reports_pass(self):
        rc, out = compare(V1, V3)
        self.assertEqual(rc, 0, out)
        self.assertIn("dependency rows identical with and without the root LICENSE: yes (2)", out)

    def test_a_different_dependency_row_fails(self):
        # The dependency rows differ in the licence of dep-b: the script printed NO and exited 0 before.
        rc, out = compare(V1, V3[:2] + ["example.test/dep-b,v2.0.0,u,BSD-3-Clause"])
        self.assertEqual(rc, 1, out)
        self.assertIn("identical with and without the root LICENSE: NO", out)

    def test_a_missing_dependency_row_fails(self):
        rc, out = compare(V1, V3[:2])
        self.assertEqual(rc, 1, out)

    def test_a_wrong_own_row_count_in_v1_fails(self):
        rc, out = compare(V1, V3, want_own=4)
        self.assertEqual(rc, 1, out)
        self.assertIn("V1 should have one row per package", out)

    def test_a_wrong_own_row_count_in_v3_fails(self):
        rc, out = compare(V1, V3 + [MOD + "/b,Unknown,,Unknown"])
        self.assertEqual(rc, 1, out)
        self.assertIn("V3 should have exactly one own row", out)

    def test_a_missing_report_fails(self):
        for v1, v3 in ((None, V3), (V1, None), ([], V3)):
            rc, out = compare(v1, v3)
            self.assertEqual(rc, 1, (v1, v3, out))
            self.assertIn("has no root report", out)

    def test_a_dependency_whose_path_starts_like_the_module_is_not_an_own_row(self):
        # example.test/proj-extra is another repository: a prefix match would count it as own.
        extra = ["example.test/proj-extra,v1.0.0,u,MIT"]
        rc, out = compare(V1 + extra, V3 + extra)
        self.assertEqual(rc, 0, out)
        self.assertIn("own-package rows: V1 3 (want 3), V3 1 (want 1)", out)


if __name__ == "__main__":
    unittest.main()
