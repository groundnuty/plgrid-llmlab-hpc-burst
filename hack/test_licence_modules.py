"""Tests for licence-modules.sh, the search that decides which modules the licence check runs on.
Each tree is built by hand in a temporary directory; the expected list is stated in the case.

Run: python3 -I -m unittest discover -s hack -p 'test_licence*.py' -v   (from the repo root)
"""
import json
import os
import subprocess
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(HERE, "licence-modules.sh")


def mktree(root, gomods, other_dirs=()):
    for d in gomods:
        os.makedirs(os.path.join(root, d), exist_ok=True)
        with open(os.path.join(root, d, "go.mod"), "w") as f:
            f.write("module example.test/%s\n\ngo 1.26\n" % (d.replace("/", "-") or "root"))
    for d in other_dirs:
        os.makedirs(os.path.join(root, d), exist_ok=True)


def run(root, *flags):
    p = subprocess.run(["bash", SCRIPT, *flags, root], capture_output=True, text=True)
    return p.returncode, p.stdout, p.stderr


class ModulesTest(unittest.TestCase):
    def listing(self, gomods, other_dirs=(), flags=()):
        with tempfile.TemporaryDirectory() as d:
            mktree(d, gomods, other_dirs)
            rc, out, err = run(d, *flags)
        self.assertEqual(rc, 0, err)
        return out.split()

    def test_finds_the_root_and_the_third_party_modules(self):
        got = self.listing([".", "third_party/forked/kubeai", "third_party/forked/kcp"])
        self.assertEqual(got, [".", "third_party/forked/kcp", "third_party/forked/kubeai"])

    def test_finds_a_module_in_an_unexpected_directory(self):
        # The point of the search: a shipped module that is not under third_party/ is included.
        # A search of third_party/ alone (the earlier proposal) misses all three of these.
        got = self.listing([".", "third_party/forked/kubeai", "internal/x/deep", "tools/gen", "newtop"])
        self.assertEqual(got, [".", "internal/x/deep", "newtop", "third_party/forked/kubeai", "tools/gen"])

    def test_spikes_are_outside_the_check(self):
        got = self.listing([".", "spikes/s1", "spikes/s10c/inner", "third_party/forked/kubeai"])
        self.assertEqual(got, [".", "third_party/forked/kubeai"])

    def test_a_spikes_folder_that_is_not_top_level_is_searched(self):
        # Only the top-level spikes/ is exempt (HBEP-15); a module in a folder that merely
        # has the same name elsewhere ships like any other.
        got = self.listing([".", "third_party/forked/z/spikes/w", "spikesy"])
        self.assertEqual(got, [".", "spikesy", "third_party/forked/z/spikes/w"])

    def test_nothing_is_excluded_quietly(self):
        got = self.listing([".", "internal/testdata/m", "vendor/v"])
        self.assertEqual(got, [".", "internal/testdata/m", "vendor/v"])

    def test_dot_git_is_not_searched(self):
        got = self.listing([".", ".git/hooks/m"])
        self.assertEqual(got, ["."])

    def test_a_repository_without_the_root_module_lists_the_others(self):
        got = self.listing(["third_party/forked/kubeai"])
        self.assertEqual(got, ["third_party/forked/kubeai"])

    def test_json_is_a_one_line_matrix(self):
        with tempfile.TemporaryDirectory() as d:
            mktree(d, [".", "third_party/forked/kubeai", "spikes/s1"])
            rc, out, err = run(d, "--json")
        self.assertEqual(rc, 0, err)
        self.assertEqual(out.count("\n"), 1, out)
        self.assertEqual(json.loads(out),
                         {"include": [{"module": "."}, {"module": "third_party/forked/kubeai"}]})

    def test_no_go_mod_at_all_fails(self):
        with tempfile.TemporaryDirectory() as d:
            mktree(d, ["spikes/s1"], ["third_party"])
            rc, out, err = run(d)
        self.assertEqual(rc, 1, err)
        self.assertEqual(out, "")
        self.assertIn("no go.mod found", err)

    def test_an_unusual_directory_name_is_refused(self):
        for name in ("has space", "semi;colon", "quote'd", 'dq"x', "dollar$x"):
            with tempfile.TemporaryDirectory() as d:
                mktree(d, [".", "third_party/" + name])
                rc, out, err = run(d)
            self.assertEqual(rc, 1, (name, out, err))
            self.assertEqual(out, "")
            self.assertIn("unusual characters", err)

    def test_bad_usage(self):
        p = subprocess.run(["bash", SCRIPT], capture_output=True, text=True)
        self.assertEqual(p.returncode, 2)
        p = subprocess.run(["bash", SCRIPT, "/nonexistent-dir-for-test"], capture_output=True, text=True)
        self.assertEqual(p.returncode, 2)


if __name__ == "__main__":
    unittest.main()
