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

    def test_a_directory_symlink_to_a_module_is_refused_and_named(self):
        # find does not follow a directory symlink, so tools/cli -> ../spikes/cli would leave the
        # module behind it out of the check, and out of the matrix, without a word (review of PR 77, B2).
        with tempfile.TemporaryDirectory() as d:
            mktree(d, [".", "spikes/cli"], ["tools"])
            os.symlink("../spikes/cli", os.path.join(d, "tools", "cli"))
            for flags in ((), ("--json",)):
                rc, out, err = run(d, *flags)
                self.assertEqual(rc, 1, (flags, out, err))
                self.assertEqual(out, "")
                self.assertIn("directory symlink", err)
                self.assertIn("./tools/cli", err)

    def test_every_directory_symlink_is_named_whatever_its_target(self):
        with tempfile.TemporaryDirectory() as d:
            mktree(d, [".", "real/m"], ["a", "b"])
            os.symlink("../real", os.path.join(d, "a", "inside"))  # a target inside the checked tree
            os.symlink("/tmp", os.path.join(d, "b", "outside"))    # a target outside the repository
            rc, out, err = run(d)
        self.assertEqual(rc, 1, err)
        self.assertEqual(out, "")
        self.assertIn("./a/inside", err)
        self.assertIn("./b/outside", err)

    def test_a_symlink_to_a_file_or_a_dangling_one_is_not_a_directory_symlink(self):
        # Only a link that find would have to follow into a directory hides a module. A go.mod
        # that is itself a symlink to a file is found as the module it marks.
        with tempfile.TemporaryDirectory() as d:
            mktree(d, [".", "real"], ["linked", "other"])
            os.symlink("../real/go.mod", os.path.join(d, "linked", "go.mod"))
            os.symlink("nowhere", os.path.join(d, "other", "dangling"))
            rc, out, err = run(d)
        self.assertEqual(rc, 0, err)
        self.assertEqual(out.split(), [".", "linked", "real"])

    def test_a_symlink_inside_spikes_is_outside_the_check(self):
        with tempfile.TemporaryDirectory() as d:
            mktree(d, [".", "spikes/s1"], ["third_party"])
            os.symlink("s1", os.path.join(d, "spikes", "alias"))
            rc, out, err = run(d)
        self.assertEqual(rc, 0, err)
        self.assertEqual(out.split(), ["."])

    def test_a_newline_in_a_directory_name_is_refused_not_split_into_two_paths(self):
        # find -printf '%h\n' made "alpha<newline>beta" the two accepted paths alpha and beta; with
        # real alpha and beta modules beside it the matrix then held those two and never the module
        # that was actually there (review of PR 77, B3).
        with tempfile.TemporaryDirectory() as d:
            mktree(d, [".", "alpha", "beta", "alpha\nbeta"])
            for flags in ((), ("--json",)):
                rc, out, err = run(d, *flags)
                self.assertEqual(rc, 1, (flags, out, err))
                self.assertEqual(out, "")
                self.assertIn("newline or another control character", err)
                self.assertIn("alpha", err)

    def test_other_control_characters_in_a_directory_name_are_refused(self):
        for name in ("tab\there", "bell\ax", "cr\rx"):
            with tempfile.TemporaryDirectory() as d:
                mktree(d, [".", name])
                rc, out, err = run(d)
            self.assertEqual(rc, 1, (name, out, err))
            self.assertEqual(out, "")
            self.assertIn("newline or another control character", err)

    def test_a_search_that_cannot_read_a_directory_fails(self):
        # find exits non-zero over an unreadable directory; a module inside it would be missed.
        # (Skipped for root, which reads everything.)
        if os.geteuid() == 0:
            self.skipTest("root reads every directory")
        with tempfile.TemporaryDirectory() as d:
            mktree(d, [".", "locked/m"])
            os.chmod(os.path.join(d, "locked"), 0)
            try:
                rc, out, err = run(d)
            finally:
                os.chmod(os.path.join(d, "locked"), 0o755)
        self.assertEqual(rc, 1, err)
        self.assertEqual(out, "")

    def test_bad_usage(self):
        p = subprocess.run(["bash", SCRIPT], capture_output=True, text=True)
        self.assertEqual(p.returncode, 2)
        p = subprocess.run(["bash", SCRIPT, "/nonexistent-dir-for-test"], capture_output=True, text=True)
        self.assertEqual(p.returncode, 2)


if __name__ == "__main__":
    unittest.main()
