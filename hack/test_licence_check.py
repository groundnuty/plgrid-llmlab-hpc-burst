"""Tests for licence-check.sh, the strict licence check, with a fake go-licenses so that they run
offline and need no Go. The fake prints what the case's files say, in the format go-licenses
v2.0.1 prints; the expected verdict is stated in each case. The real go-licenses runs in CI, in
the licences job, on the real tree.

The script is copied into a temporary hack/ with its own known-zero list, because it reads its
allowlist and known zeros from next to itself and nothing in the environment can change them.

Also checks that allowed-licenses.txt is HBEP-15's allowlist, read from the HBEP's text.

Run: python3 -I -m unittest discover -s hack -p 'test_licence*.py' -v   (from the repo root)
"""
import os
import re
import shutil
import stat
import subprocess
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
HBEP = os.path.join(HERE, "..", "hbep", "0015-dependencies-and-provenance", "README.md")
SCRIPTS = ("licence-check.sh", "licence-modules.sh", "licence_gate.py", "allowed-licenses.txt")

# What go-licenses does, decided by files in the module directory it runs in:
#   .report (rows of "report"), .report-rc, .check (output of "check"), .check-rc,
#   .honours-list: "check" fails with a BSD-3-Clause line when BSD-3-Clause is off the allowlist.
FAKE = r"""#!/usr/bin/env bash
cmd=$1; shift
case $cmd in
  report)
    [ -f .report ] && cat .report
    exit "$(cat .report-rc 2>/dev/null || echo 0)" ;;
  check)
    list=
    for a in "$@"; do case $a in --allowed_licenses=*) list=${a#*=} ;; esac; done
    if [ -f .honours-list ] && [[ ",$list," != *",BSD-3-Clause,"* ]]; then
      echo "Not allowed license 'BSD-3-Clause' found for library 'example.test/dep'."; exit 1
    fi
    [ -f .check ] && cat .check
    exit "$(cat .check-rc 2>/dev/null || echo 0)" ;;
esac
exit 3
"""
ROW = "example.test/m,v0.0.0,https://example.test/LICENSE,Apache-2.0\n"


class Case:
    """A temporary repository with a copy of the scripts in its hack/."""

    def __init__(self, known_zero=""):
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = os.path.join(self.tmp.name, "repo")
        self.hack = os.path.join(self.repo, "hack")
        os.makedirs(self.hack)
        for name in SCRIPTS:
            shutil.copy(os.path.join(HERE, name), self.hack)
        with open(os.path.join(self.hack, "known-zero-modules.txt"), "w") as f:
            f.write(known_zero)
        self.fake = os.path.join(self.tmp.name, "fake-go-licenses")
        with open(self.fake, "w") as f:
            f.write(FAKE)
        os.chmod(self.fake, os.stat(self.fake).st_mode | stat.S_IXUSR)
        self.out = os.path.join(self.tmp.name, "out")

    def module(self, d, **files):
        """A module directory with a go.mod and the fake's files: module(".", report=ROW, check_rc="1")."""
        path = os.path.join(self.repo, d)
        os.makedirs(path, exist_ok=True)
        with open(os.path.join(path, "go.mod"), "w") as f:
            f.write("module example.test/m\n\ngo 1.26\n")
        for key, value in files.items():
            with open(os.path.join(path, "." + key.replace("_", "-")), "w") as f:
                f.write(value)

    def run(self, *args, modules=()):
        env = dict(os.environ, GOMODCACHE="/nonexistent-modcache")
        p = subprocess.run(["bash", os.path.join(self.hack, "licence-check.sh"), *args,
                            self.fake, self.repo, self.out, *modules],
                           capture_output=True, text=True, env=env)
        return p.returncode, p.stdout + p.stderr

    def close(self):
        self.tmp.cleanup()


class CheckTest(unittest.TestCase):
    def case(self, known_zero=""):
        c = Case(known_zero)
        self.addCleanup(c.close)
        return c

    def test_every_module_found_is_checked_and_a_clean_tree_passes(self):
        c = self.case()
        c.module(".", report=ROW)
        c.module("third_party/forked/x", report=ROW)
        c.module("spikes/s1")  # outside the check: no report, would fail as an empty run
        rc, out = c.run()
        self.assertEqual(rc, 0, out)
        self.assertIn("##### module .\n", out)
        self.assertIn("##### module third_party/forked/x\n", out)
        self.assertNotIn("spikes/s1", out.replace("spikes/: outside", ""))
        self.assertIn("licence check: PASS", out)

    def test_a_module_with_no_package_that_is_not_listed_fails(self):
        c = self.case()
        c.module(".", report=ROW)
        c.module("third_party/forked/empty", report="")
        rc, out = c.run()
        self.assertEqual(rc, 1, out)
        self.assertIn("UNEXPECTED: go-licenses saw no package in third_party/forked/empty", out)
        self.assertIn("licence check: FAIL", out)

    def test_a_listed_known_zero_passes_and_prints_its_reason(self):
        c = self.case("third_party/forked/empty: its one file is behind a build tag\n")
        c.module(".", report=ROW)
        c.module("third_party/forked/empty", report="")
        rc, out = c.run()
        self.assertEqual(rc, 0, out)
        self.assertIn("known zero: third_party/forked/empty has no package to check; "
                      "reason: its one file is behind a build tag", out)

    def test_the_known_zero_list_excuses_only_the_module_it_names(self):
        c = self.case("third_party/forked/empty: a reason\n")
        c.module(".", report="")  # the root saw nothing and is not listed
        c.module("third_party/forked/empty", report="")
        rc, out = c.run()
        self.assertEqual(rc, 1, out)
        self.assertIn("UNEXPECTED: go-licenses saw no package in .,", out)

    def test_a_known_zero_that_has_packages_is_only_noted(self):
        c = self.case("third_party/forked/x: a reason\n")
        c.module("third_party/forked/x", report=ROW)
        rc, out = c.run()
        self.assertEqual(rc, 0, out)
        self.assertIn("NOTE: third_party/forked/x is listed as a known zero", out)

    def test_a_known_zero_entry_without_a_reason_is_refused(self):
        for entry in ("third_party/forked/x:\n", "third_party/forked/x:   \n", "third_party/forked/x\n"):
            c = self.case(entry)
            c.module("third_party/forked/x", report=ROW)
            rc, out = c.run()
            self.assertEqual(rc, 2, (entry, out))
            self.assertIn("an entry needs '<module dir>: <reason>'", out)

    def test_a_package_without_a_licence_fails_whichever_package_it_is(self):
        # No exemption for the module's own packages (the spike's local-only option is gone).
        c = self.case()
        c.module(".", report=ROW, check="Did not find license for library 'example.test/m/pkg'.\n", check_rc="1")
        rc, out = c.run()
        self.assertEqual(rc, 1, out)
        self.assertIn("UNEXPECTED: Did not find license for library 'example.test/m/pkg'.", out)

    def test_an_error_diagnostic_fails_even_when_go_licenses_exits_zero(self):
        c = self.case()
        c.module("third_party/forked/x", report=ROW,
                 check="E1031 13:15:01.131651 2332600 library.go:246] Failed to parse /m/LICENSE: open /m/LICENSE: denied\n",
                 check_rc="0")
        rc, out = c.run()
        self.assertEqual(rc, 1, out)
        self.assertIn("UNEXPECTED: E1031", out)

    def test_a_failing_report_fails(self):
        c = self.case()
        c.module(".", report=ROW, report_rc="1")
        rc, out = c.run()
        self.assertEqual(rc, 1, out)
        self.assertIn("go-licenses report failed", out)

    def test_a_named_module_without_a_go_mod_fails(self):
        c = self.case()
        c.module(".", report=ROW)
        rc, out = c.run(modules=["third_party/forked/missing"])
        self.assertEqual(rc, 1, out)
        self.assertIn("UNEXPECTED: no go.mod in third_party/forked/missing", out)

    def test_the_negative_control_must_fail_and_name_bsd_3_clause(self):
        c = self.case()
        c.module(".", report=ROW, honours_list="")
        rc, out = c.run("--negative-control")
        self.assertEqual(rc, 0, out)
        self.assertIn("negative control: failed and named BSD-3-Clause packages, as it should", out)

    def test_a_negative_control_that_does_not_fail_fails_the_run(self):
        c = self.case()
        c.module(".", report=ROW)  # a go-licenses that ignores the allowlist
        rc, out = c.run("--negative-control")
        self.assertEqual(rc, 1, out)
        self.assertIn("NEGATIVE CONTROL FAILED", out)

    def test_the_exemption_option_is_refused(self):
        c = self.case()
        c.module(".", report=ROW)
        rc, out = c.run("--local-permit-own")
        self.assertEqual(rc, 2, out)
        self.assertIn("usage:", out)

    def test_the_environment_cannot_change_the_lists(self):
        # The spike's script read $ALLOWED_LICENSES and $KNOWN_ZERO_MODULES; this one does not.
        c = self.case()
        c.module("third_party/forked/empty", report="")
        listed = os.path.join(c.tmp.name, "zero.txt")
        with open(listed, "w") as f:
            f.write("third_party/forked/empty: a reason\n")
        env = dict(os.environ, GOMODCACHE="/nonexistent-modcache", KNOWN_ZERO_MODULES=listed)
        p = subprocess.run(["bash", os.path.join(c.hack, "licence-check.sh"), c.fake, c.repo, c.out],
                           capture_output=True, text=True, env=env)
        self.assertEqual(p.returncode, 1, p.stdout + p.stderr)


class AllowlistTest(unittest.TestCase):
    def test_allowed_licenses_is_the_list_in_hbep_15(self):
        with open(HBEP) as f:
            text = f.read()
        m = re.search(r"^- \*\*Allowed:\*\* Apache-2\.0, plus the licences of the CNCF allowlist, as listed on "
                      r"[0-9-]+: (.+), and the Google patent licence for Go\.$", text, re.M)
        self.assertIsNotNone(m, "HBEP-15's Allowed line changed shape; update this test")
        want = {"Apache-2.0"} | set(m.group(1).split(", "))
        with open(os.path.join(HERE, "allowed-licenses.txt")) as f:
            got = [l.strip() for l in f if l.strip() and not l.startswith("#")]
        self.assertEqual(len(got), len(set(got)), "a licence is listed twice")
        self.assertEqual(set(got), want)


if __name__ == "__main__":
    unittest.main()
