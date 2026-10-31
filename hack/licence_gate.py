#!/usr/bin/env python3
"""Decide whether a "go-licenses check" run passed. Strict: there is no exemption.

Every "Did not find license" and every "Not allowed license" line is a failure, for a dependency
and for a package of the module under check alike. The module's own packages pass because the
repository's Apache-2.0 LICENSE (root) or the copy's own LICENSE (a third_party folder) is found,
not because the gate excuses them. The spike that this gate comes from (spikes/s12) had a
local-only option for repositories with no LICENSE; it was not carried over, and --permit-own is
now an unknown option (exit 2).

Usage:
  go-licenses check ./... --allowed_licenses=... 2>&1 | licence_gate.py --rc <go-licenses exit status>

Exit status: 0 when the run passed; 1 when it did not; 2 on bad usage. It fails closed: a non-zero
go-licenses exit with a line the gate does not recognise, or with no recognised line at all
(a build error, a Go version mismatch), is a failure. So is a "Did not find" or "Not allowed"
line next to a zero exit.

Scanner errors fail the gate whatever the exit status. go-licenses v2.0.1 logs an error when it
cannot read or classify a licence file ("Failed to parse <file>: ...", library.go) and carries
on with the next candidate file, such as the module's own LICENSE, so a package whose own
licence file is unreadable can still check clean, with exit 0. Such a pass is unproven, so a
klog error or fatal line (the "E1031 hh:mm:ss.uuuuuu pid file.go:line] " header) or a line that
starts "Failed to parse " is a failure with a zero exit as well as with a non-zero one. The gate
sees go-licenses' raw output: nothing is filtered in front of it, because a filter could drop
such a line.
"""
import argparse
import re
import sys

NOT_FOUND = re.compile(r"^Did not find license for library '[^']+'\.?$")
NOT_ALLOWED = re.compile(r"^Not allowed license '[^']+' found for library '[^']+'\.?$")
# klog's header: severity letter, MMDD, time, process id, file:line, "]". E is error, F is fatal.
KLOG_ERROR = re.compile(r"^[EF]\d{4} \d\d:\d\d:\d\d\.\d+\s+\d+ \S+:\d+\] ")
# The message of the library.go error, in case the log header is missing or has another shape.
SCANNER_ERROR = re.compile(r"^Failed to parse ")


def is_noise(line):
    """go-licenses warnings that are not about a licence (assembly files it cannot read)."""
    return "non-Go code" in line or line.endswith(".s")


def is_scanner_error(line):
    """A go-licenses error diagnostic (a klog error or fatal line, or "Failed to parse ...")."""
    return bool(KLOG_ERROR.match(line) or SCANNER_ERROR.match(line))


def classify(lines, rc):
    """Return the failures of a run: a list of strings, each once. Empty means the run passed."""
    failures = []
    for raw in lines:
        line = raw.strip()
        if not line:
            continue
        if is_scanner_error(line):
            failures.append(line)  # whatever the exit status: the scan itself reported an error
        elif is_noise(line):
            continue
        elif NOT_FOUND.match(line) or NOT_ALLOWED.match(line):
            failures.append(line)
        elif rc != 0:
            failures.append(line)  # fail closed: an unknown message next to a failure
    if rc != 0 and not failures:
        failures.append("go-licenses exited %d without naming a package" % rc)
    # go-licenses names a package once per import path it is reached by; list each once.
    return list(dict.fromkeys(failures))


def main(argv):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--rc", type=int, required=True, help="exit status of go-licenses check")
    args = ap.parse_args(argv)
    failures = classify(sys.stdin.read().splitlines(), args.rc)
    for line in failures:
        print("UNEXPECTED: %s" % line)
    print("gate: go-licenses exit=%d, unexpected=%d: %s" % (
        args.rc, len(failures), "ok" if not failures else "FAIL"))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
