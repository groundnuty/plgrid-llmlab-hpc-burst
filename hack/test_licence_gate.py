"""Tests for licence_gate.py. The go-licenses output lines are written by hand, in the format
go-licenses v2.0.1 prints on this repository; the expected verdict of each case is stated in the
case. The gate has no exemption, so every "Did not find license" and "Not allowed license" line
fails the run, whatever package it names.

Run: python3 -I -m unittest discover -s hack -p 'test_licence*.py' -v   (from the repo root)
"""
import os
import subprocess
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
GATE = os.path.join(HERE, "licence_gate.py")

ROOT = "example.test/proj"


def nf(pkg):
    return "Did not find license for library '%s'." % pkg


def na(pkg, lic="BSD-4-Clause"):
    return "Not allowed license '%s' found for library '%s'." % (lic, pkg)


# The error go-licenses v2.0.1 logs (library.go:246) when it cannot read or classify a licence file;
# the shape is the one printed by a run on a fixture (spikes/s12, "unreadable licence").
def parse_error(path="/tmp/x/dep/sub/LICENSE", reason="no such file or directory"):
    return "E1031 13:15:01.131651 2332600 library.go:246] Failed to parse %s: open %s: %s" % (path, path, reason)


def gate(output, rc, *flags):
    p = subprocess.run([sys.executable, "-I", GATE, "--rc", str(rc), *flags],
                       input=output, capture_output=True, text=True)
    return p.returncode, p.stdout + p.stderr


class GateTest(unittest.TestCase):
    def test_clean_run_passes(self):
        rc, out = gate("", 0)
        self.assertEqual(rc, 0, out)
        self.assertIn("unexpected=0", out)

    def test_an_own_package_without_a_licence_fails(self):
        # No exemption: the module's own package needs a LICENSE like any other.
        rc, out = gate(nf(ROOT) + "\n" + nf(ROOT + "/pkg") + "\n", 1)
        self.assertEqual(rc, 1, out)
        self.assertIn("UNEXPECTED: " + nf(ROOT), out)
        self.assertIn("UNEXPECTED: " + nf(ROOT + "/pkg"), out)
        self.assertIn("unexpected=2", out)

    def test_a_dependency_without_a_licence_fails(self):
        rc, out = gate(nf("example.test/dep") + "\n", 1)
        self.assertEqual(rc, 1, out)
        self.assertIn("UNEXPECTED: " + nf("example.test/dep"), out)

    def test_a_package_with_an_off_list_licence_fails(self):
        rc, out = gate(na(ROOT + "/pkg") + "\n" + na("example.test/dep") + "\n", 1)
        self.assertEqual(rc, 1, out)
        self.assertIn("unexpected=2", out)

    def test_a_package_named_twice_is_listed_once(self):
        rc, out = gate("\n".join([nf(ROOT), nf(ROOT), nf("example.test/dep"), nf("example.test/dep")]) + "\n", 1)
        self.assertEqual(rc, 1, out)
        self.assertIn("unexpected=2", out)

    def test_the_exemption_option_does_not_exist(self):
        # The spike's gate took --permit-own 1; the shipped gate refuses it (argparse: exit 2),
        # so the exemption cannot be switched on by a flag in a workflow.
        for value in ("0", "1"):
            rc, out = gate(nf(ROOT) + "\n", 1, "--permit-own", value)
            self.assertEqual(rc, 2, out)
            self.assertIn("unrecognized arguments", out)

    def test_the_own_packages_option_does_not_exist(self):
        rc, out = gate("", 0, "--own-packages", "/dev/null")
        self.assertEqual(rc, 2, out)

    def test_the_rc_is_required(self):
        p = subprocess.run([sys.executable, "-I", GATE], input="", capture_output=True, text=True)
        self.assertEqual(p.returncode, 2, p.stderr)

    def test_fails_closed_on_an_unrecognised_message(self):
        rc, out = gate("go: go.mod requires go >= 1.99 (running go 1.27.3; GOTOOLCHAIN=local)\n", 1)
        self.assertEqual(rc, 1, out)
        rc, out = gate(nf(ROOT) + "\nsomething unexpected\n", 1)
        self.assertEqual(rc, 1, out)
        self.assertIn("UNEXPECTED: something unexpected", out)

    def test_fails_closed_on_a_failure_with_no_message(self):
        rc, out = gate("", 1)
        self.assertEqual(rc, 1, out)
        self.assertIn("without naming a package", out)

    def test_failure_line_next_to_a_zero_exit_fails(self):
        rc, out = gate(na("example.test/dep") + "\n", 0)
        self.assertEqual(rc, 1, out)
        rc, out = gate(nf("example.test/dep") + "\n", 0)
        self.assertEqual(rc, 1, out)

    def test_scanner_error_next_to_a_zero_exit_fails(self):
        # go-licenses logs the error, falls back to another candidate file (the module's own
        # LICENSE) and exits 0, so the run looks clean. It must not pass.
        rc, out = gate(parse_error() + "\n", 0)
        self.assertEqual(rc, 1, out)
        self.assertIn("UNEXPECTED: " + parse_error(), out)
        self.assertIn("unexpected=1", out)

    def test_scanner_error_among_other_failures_still_fails(self):
        rc, out = gate("\n".join([nf(ROOT), parse_error()]) + "\n", 1)
        self.assertEqual(rc, 1, out)
        self.assertIn("unexpected=2", out)

    def test_other_klog_errors_and_fatals_fail(self):
        for line in ("E1031 13:15:01.131651 2332600 library.go:159] Package x/y does not have module info.",
                     "F1031 13:15:01.131651 2332600 save.go:53] something fatal"):
            rc, out = gate(line + "\n", 0)
            self.assertEqual(rc, 1, (line, out))

    def test_scanner_error_without_the_log_header_fails(self):
        rc, out = gate("Failed to parse /tmp/x/LICENSE: open /tmp/x/LICENSE: permission denied\n", 0)
        self.assertEqual(rc, 1, out)

    def test_a_scanner_error_on_a_line_ending_in_dot_s_is_not_noise(self):
        # The gate sees the raw output. A diagnostic about an assembly file's directory must not be
        # taken for the "ignoring non-Go code" warning that the gate drops.
        line = "E1031 13:15:01.131651 2332600 library.go:246] Failed to parse /tmp/x/asm.s"
        self.assertTrue(line.endswith(".s"))
        rc, out = gate(line + "\n", 0)
        self.assertEqual(rc, 1, out)
        self.assertIn("UNEXPECTED: " + line, out)

    def test_klog_warnings_and_info_do_not_fail_a_clean_run(self):
        # Only error severity counts.
        lines = ["W1031 13:15:02.142673 2332683 library.go:169] module x does not have dir and it is not vendored",
                 "I1031 13:15:02.142673 2332683 library.go:100] loading packages"]
        rc, out = gate("\n".join(lines) + "\n", 0)
        self.assertEqual(rc, 0, out)

    def test_assembly_warnings_are_ignored(self):
        noise = "warning: ignoring non-Go code:\n/some/mod/cache/x@v1/asm_linux_amd64.s\n"
        rc, out = gate(noise, 0)
        self.assertEqual(rc, 0, out)
        # ... but they hide nothing: a licence failure next to them still fails.
        rc, out = gate(noise + nf(ROOT) + "\n", 1)
        self.assertEqual(rc, 1, out)
        self.assertIn("UNEXPECTED: " + nf(ROOT), out)


if __name__ == "__main__":
    unittest.main()
