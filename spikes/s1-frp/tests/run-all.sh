#!/usr/bin/env bash
# run-all.sh [--no-e2e] -- every test of the kit, then a table of name and result.
#   S1_KIT_DIR=<dir> run-all.sh    runs the same tests against another copy of the kit (an older commit, a mutant)
# Needs python3, bash, and for the sshd tests /usr/sbin/sshd; the e2e test also needs the pinned frp binaries
# (bash fetch-frp.sh). Takes about 12 minutes with the e2e test, 8 without.
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
S1_TEST_RESULTS="$(mktemp "${TMPDIR:-/tmp}/s1-results.XXXXXX")"; export S1_TEST_RESULTS
S1_PORT_STATE="$(mktemp "${TMPDIR:-/tmp}/s1-ports.XXXXXX")"; export S1_PORT_STATE   # loopback ports 29400-29499, round-robin

# the Python tests, one result line per test
python3 -W ignore -m unittest -v test_stream_watch test_verdict test_engine_and_responder test_path_rate 2>&1 \
  | python3 -c '
import re, sys
res = open(sys.argv[1], "a")
current = None
for line in sys.stdin:
    line = line.rstrip("\n")
    m = re.match(r"^(test_\w+) \(([\w.]+)\)", line)
    if m:                                   # a test header; a docstring puts the verdict on the next line
        current = m.group(2).split(".", 1)[-1]
    done = re.search(r"(?:\.\.\. |^)(ok|FAIL|ERROR)$", line)
    if done and current:
        res.write("%s\t%s\n" % ("PASS" if done.group(1) == "ok" else "FAIL", current))
        current = None
    elif not m and not done and not line.startswith(("stream_watch: probe", "Ran ", "OK", "FAILED", "----", "====")) and line.strip():
        sys.stderr.write(line + "\n")
' "$S1_TEST_RESULTS"

bash job_tests.sh
bash sshd_key_tests.sh
[ "${1:-}" = --no-e2e ] || bash e2e_job.sh

echo
echo "=== results ($(wc -l < "$S1_TEST_RESULTS") tests, kit: ${S1_KIT_DIR:-$(cd .. && pwd)}) ==="
sort -k2 "$S1_TEST_RESULTS" | awk -F'\t' '{ printf "%-5s %s\n", $1, $2 }'
fails="$(grep -c '^FAIL' "$S1_TEST_RESULTS")"
echo "--- $(grep -c '^PASS' "$S1_TEST_RESULTS") passed, $fails failed"
rm -f "$S1_TEST_RESULTS" "$S1_PORT_STATE"
[ "$fails" -eq 0 ]
