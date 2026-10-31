#!/usr/bin/env python3
"""Decide whether a "go-licenses check" run failed only for a reason we allow.

Strict by default: every "Did not find license" and "Not allowed license" line is a failure.

LOCAL ONLY, never for CI: with --permit-own 1 the one reason it allows is that a package of the
module under check, the main module itself, has no LICENSE ("Did not find license for library
'<package>'"). The repository has no root LICENSE yet (HBEP-15 open question 1), so its own
packages cannot pass; the spike's scripts use this to run on such a repository. go-licenses' own
--ignore flag cannot express that exemption: it matches a path prefix (strings.HasPrefix on
every package), so it also hides a dependency, or another repository, whose path merely starts
with the same text. This gate instead checks each failing library against the exact list of
packages that "go list" assigns to the main module (the package's Module.Main), so a dependency
is never exempt, however its path looks. The copy that moves to hack/ has no such option.

Usage:
  go-licenses check ./... --allowed_licenses=... 2>&1 | licence_gate.py \\
      --rc <go-licenses exit status> --own-packages <file> [--permit-own 0|1 (1 is local only)]

--own-packages is the output of
  go list -deps -test -f '{{if .Module}}{{if .Module.Main}}{{.ImportPath}}{{end}}{{end}}' ./...
(test variants such as "pkg [pkg.test]" are accepted).

Exit status: 0 when every failure is permitted; 1 otherwise. It fails closed: a non-zero
go-licenses exit with a line the gate does not recognise, or with no recognised line at all
(a build error, a Go version mismatch), is an unexpected failure. So is a "Did not find"
or "Not allowed" line next to a zero exit. Permitted failures are printed as such.
The default, --permit-own 0, is what the CI job runs.

Scanner errors fail the gate whatever the exit status. go-licenses v2.0.1 logs an error when it
cannot read or classify a licence file ("Failed to parse <file>: ...", library.go) and carries
on with the next candidate file, such as the module's own LICENSE, so a package whose own
licence file is unreadable can still check clean, with exit 0. Such a pass is unproven, so a
klog error or fatal line (the "E1031 hh:mm:ss.uuuuuu pid file.go:line] " header) or a line that
starts "Failed to parse " is an unexpected failure with a zero exit as well as with a non-zero one.
"""
import argparse
import re
import sys

NOT_FOUND = re.compile(r"^Did not find license for library '([^']+)'\.?$")
NOT_ALLOWED = re.compile(r"^Not allowed license '[^']+' found for library '[^']+'\.?$")
# klog's header: severity letter, MMDD, time, process id, file:line, "]". E is error, F is fatal.
KLOG_ERROR = re.compile(r"^[EF]\d{4} \d\d:\d\d:\d\d\.\d+\s+\d+ \S+:\d+\] ")
# The message of the library.go error, in case the log header is missing or has another shape.
SCANNER_ERROR = re.compile(r"^Failed to parse ")


def own_packages(text):
    """Normalise the output of go list: "pkg [pkg.test]" -> "pkg"."""
    return {line.split(" [", 1)[0].strip() for line in text.splitlines() if line.strip()}


def is_noise(line):
    """go-licenses warnings that are not about a licence (assembly files it cannot read)."""
    return "non-Go code" in line or line.endswith(".s")


def is_scanner_error(line):
    """A go-licenses error diagnostic (a klog error or fatal line, or "Failed to parse ...")."""
    return bool(KLOG_ERROR.match(line) or SCANNER_ERROR.match(line))


def classify(lines, own, permit_own, rc):
    """Return (permitted, unexpected): lists of strings."""
    permitted, unexpected = [], []
    for raw in lines:
        line = raw.strip()
        if not line:
            continue
        if is_scanner_error(line):
            unexpected.append(line)  # whatever the exit status: the scan itself reported an error
            continue
        if is_noise(line):
            continue
        m = NOT_FOUND.match(line)
        if m and permit_own and m.group(1) in own:
            permitted.append(m.group(1))
        elif m or NOT_ALLOWED.match(line):
            unexpected.append(line)
        elif rc != 0:
            unexpected.append(line)  # fail closed: an unknown message next to a failure
    if rc != 0 and not permitted and not unexpected:
        unexpected.append("go-licenses exited %d without naming a package" % rc)
    # go-licenses names a package once per import path it is reached by; list each once.
    return list(dict.fromkeys(permitted)), list(dict.fromkeys(unexpected))


def main(argv):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--rc", type=int, required=True, help="exit status of go-licenses check")
    ap.add_argument("--own-packages", required=True, help="file with the output of go list (see above)")
    ap.add_argument("--permit-own", type=int, choices=(0, 1), default=0,
                    help="1 (LOCAL ONLY, not for CI): an own package without a LICENSE is permitted; "
                         "0 (default): it is a failure")
    args = ap.parse_args(argv)
    with open(args.own_packages) as f:
        own = own_packages(f.read())
    permitted, unexpected = classify(sys.stdin.read().splitlines(), own, bool(args.permit_own), args.rc)
    for pkg in permitted:
        print("permitted (a package of this module, which has no LICENSE): %s" % pkg)
    for line in unexpected:
        print("UNEXPECTED: %s" % line)
    ok = not unexpected
    print("gate: go-licenses exit=%d, permitted=%d, unexpected=%d: %s" % (
        args.rc, len(permitted), len(unexpected), "ok" if ok else "FAIL"))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
