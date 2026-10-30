#!/usr/bin/env bash
# Run the launcher's tests, including the ones that start a real k3s as an ordinary user.
#
# usage: HPCBURST_BIN_DIR=DIR hack/test-launcher.sh [go test flags]
#
# DIR holds the pinned binaries, laid out as hack/fetch-k3s.sh lays them out (k3s-1.35.9/k3s and
# k3s-1.36.5/k3s: the upgrade test starts the first and upgrades to the second). Without DIR the
# k3s tests would skip, and a skip says nothing about the launcher, so this script refuses to run
# without it and turns any skip of those tests into a failure.
#
# Optional:
#   HPCBURST_TEST_ROOT_K3S=1  state that a system k3s runs on this machine: the tests fail if it is
#                             missing, and assert after each test that it still listens on 6443 and 10250.
#   TMPDIR                    where the tests create their state directories (about 0.6 GB at peak,
#                             removed after each test; the shared machine's rules may name a disk).
#
# The tests use loopback ports 29220-29239 only. They run one at a time.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

: "${HPCBURST_BIN_DIR:?set HPCBURST_BIN_DIR to a directory with the pinned binaries (hack/fetch-k3s.sh DIR)}"
export HPCBURST_REQUIRE_INTEGRATION=1

out=$(mktemp)
trap 'rm -f "$out"' EXIT
rc=0
# -timeout 60m: go test's default of 10 minutes would kill the test binary in the middle of a run
# (the tests wait for a quiet machine, and the k3s tests take minutes), skipping the clean-up that
# stops the k3s it started. A caller's own -timeout comes later on the command line and wins.
go test -count=1 -p 1 -timeout 60m -v ./internal/launcher/... ./cmd/hpcburst/... "$@" 2>&1 | tee "$out" || rc=$?
echo
echo "summary: $(grep -c -E '^\s*--- PASS' "$out") passed, $(grep -c -E '^\s*--- FAIL' "$out") failed, $(grep -c -E '^\s*--- SKIP' "$out") skipped"
grep -E '^\s*--- SKIP' "$out" || true
exit "$rc"
