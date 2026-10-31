"""Tests for licence_gate.py. The go-licenses output lines are written by hand, in the format
go-licenses v2.0.1 printed on this repository (see results/go-licenses-check.txt); the expected
verdict of each case is stated in the case. The end-to-end tests, which run go-licenses itself on
fixture modules, are in licence-fixtures.sh.

Run: python3 -m unittest -v test_licence_gate   (from this directory)
"""
import os
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))

ROOT = "example.test/proj"
# What go list prints for the main module, with test variants.
OWN = "\n".join([ROOT, ROOT + "/pkg", ROOT + "/pkg [" + ROOT + "/pkg.test]", ROOT + "/pkg.test"]) + "\n"


def nf(pkg):
    return "Did not find license for library '%s'." % pkg


def na(pkg, lic="BSD-4-Clause"):
    return "Not allowed license '%s' found for library '%s'." % (lic, pkg)


# The error go-licenses v2.0.1 logs (library.go:246) when it cannot read or classify a licence file;
# the shape is the one printed by a run on a fixture (licence-fixtures.sh, "unreadable licence").
def parse_error(path="/tmp/x/dep/sub/LICENSE", reason="no such file or directory"):
    return "E1031 13:15:01.131651 2332600 library.go:246] Failed to parse %s: open %s: %s" % (path, path, reason)


def gate(output, rc, own=OWN, permit=1):
    """permit=None leaves --permit-own off the command line, to test the default."""
    with tempfile.TemporaryDirectory() as d:
        f = os.path.join(d, "own.txt")
        with open(f, "w") as h:
            h.write(own)
        flag = [] if permit is None else ["--permit-own", str(permit)]
        p = subprocess.run([sys.executable, "-I", os.path.join(HERE, "licence_gate.py"), "--rc", str(rc),
                            "--own-packages", f] + flag,
                           input=output, capture_output=True, text=True)
    return p.returncode, p.stdout + p.stderr


class GateTest(unittest.TestCase):
    def test_own_packages_without_a_licence_are_permitted(self):
        rc, out = gate("\n".join([nf(ROOT), nf(ROOT + "/pkg")]) + "\n", 1)
        self.assertEqual(rc, 0, out)
        self.assertIn("permitted=2, unexpected=0", out)

    def test_a_package_named_twice_is_listed_once(self):
        rc, out = gate("\n".join([nf(ROOT), nf(ROOT), nf("example.test/dep"), nf("example.test/dep")]) + "\n", 1)
        self.assertEqual(rc, 1, out)
        self.assertIn("permitted=1, unexpected=1", out)

    def test_clean_run_passes(self):
        rc, out = gate("", 0)
        self.assertEqual(rc, 0, out)

    def test_exemption_off_makes_own_packages_fail(self):
        rc, out = gate(nf(ROOT) + "\n", 1, permit=0)
        self.assertEqual(rc, 1, out)
        self.assertIn("UNEXPECTED: " + nf(ROOT), out)

    def test_the_default_is_strict(self):
        # The CI job runs the gate without the local-only option: an own package with no LICENSE fails.
        rc, out = gate(nf(ROOT) + "\n", 1, permit=None)
        self.assertEqual(rc, 1, out)
        self.assertIn("UNEXPECTED: " + nf(ROOT), out)
        self.assertIn("permitted=0, unexpected=1", out)

    def test_dependency_with_an_unknown_licence_sharing_the_prefix_fails(self):
        # A prefix match on example.test/proj would hide both of these.
        for pkg in (ROOT + "-extra", ROOT + "-extra/lib", ROOT + "/sub", ROOT + "/sub/lib", ROOT + "kit"):
            rc, out = gate("\n".join([nf(ROOT), nf(pkg)]) + "\n", 1)
            self.assertEqual(rc, 1, (pkg, out))
            self.assertIn("UNEXPECTED: " + nf(pkg), out)

    def test_dependency_with_an_off_list_licence_sharing_the_prefix_fails(self):
        for pkg in (ROOT + "-extra", ROOT + "/sub"):
            rc, out = gate("\n".join([nf(ROOT), na(pkg)]) + "\n", 1)
            self.assertEqual(rc, 1, (pkg, out))

    def test_own_package_with_an_off_list_licence_fails(self):
        # Only a missing LICENSE is permitted, not a LICENSE that is off the list.
        rc, out = gate(na(ROOT + "/pkg") + "\n", 1)
        self.assertEqual(rc, 1, out)

    def test_fails_closed_on_an_unrecognised_message(self):
        rc, out = gate("go: go.mod requires go >= 1.99 (running go 1.27.3; GOTOOLCHAIN=local)\n", 1)
        self.assertEqual(rc, 1, out)
        rc, out = gate(nf(ROOT) + "\nsomething unexpected\n", 1)
        self.assertEqual(rc, 1, out)

    def test_fails_closed_on_a_failure_with_no_message(self):
        rc, out = gate("", 1)
        self.assertEqual(rc, 1, out)
        self.assertIn("without naming a package", out)

    def test_failure_line_next_to_a_zero_exit_fails(self):
        rc, out = gate(na("example.test/dep") + "\n", 0)
        self.assertEqual(rc, 1, out)

    def test_scanner_error_next_to_a_zero_exit_fails(self):
        # B1 of the review: go-licenses logs the error, falls back to another candidate file (the
        # module's own LICENSE) and exits 0, so the run looks clean. It must not pass.
        rc, out = gate(parse_error() + "\n", 0)
        self.assertEqual(rc, 1, out)
        self.assertIn("UNEXPECTED: " + parse_error(), out)
        self.assertIn("permitted=0, unexpected=1", out)

    def test_scanner_error_fails_in_every_mode(self):
        for permit in (None, 0, 1):
            rc, out = gate(parse_error() + "\n", 0, permit=permit)
            self.assertEqual(rc, 1, (permit, out))

    def test_scanner_error_among_permitted_failures_still_fails(self):
        # The own package's missing LICENSE is permitted; the unreadable licence file is not.
        rc, out = gate("\n".join([nf(ROOT), parse_error()]) + "\n", 1)
        self.assertEqual(rc, 1, out)
        self.assertIn("permitted=1, unexpected=1", out)

    def test_other_klog_errors_and_fatals_fail(self):
        for line in ("E1031 13:15:01.131651 2332600 library.go:159] Package x/y does not have module info.",
                     "F1031 13:15:01.131651 2332600 save.go:53] something fatal"):
            rc, out = gate(line + "\n", 0)
            self.assertEqual(rc, 1, (line, out))

    def test_scanner_error_without_the_log_header_fails(self):
        rc, out = gate("Failed to parse /tmp/x/LICENSE: open /tmp/x/LICENSE: permission denied\n", 0)
        self.assertEqual(rc, 1, out)

    def test_klog_warnings_and_info_do_not_fail_a_clean_run(self):
        # Only error severity counts: the control must still pass.
        lines = ["W1031 13:15:02.142673 2332683 library.go:169] module x does not have dir and it is not vendored",
                 "I1031 13:15:02.142673 2332683 library.go:100] loading packages"]
        rc, out = gate("\n".join(lines) + "\n", 0)
        self.assertEqual(rc, 0, out)

    def test_assembly_warnings_are_ignored(self):
        noise = "warning: ignoring non-Go code:\n/some/mod/cache/x@v1/asm_linux_amd64.s\n"
        rc, out = gate(noise + nf(ROOT) + "\n", 1)
        self.assertEqual(rc, 0, out)
        rc, out = gate(noise, 0)
        self.assertEqual(rc, 0, out)


if __name__ == "__main__":
    unittest.main()
