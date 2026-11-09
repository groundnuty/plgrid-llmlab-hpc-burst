#!/usr/bin/env bash
# The root module's tests: the one definition that hack/check.sh and CI's go job both run (issue #282).
#
# internal/lifecycle stands close to go test's 10-minute package timeout, and that timeout stays: a
# raised one would hide a real hang for longer. So the one test named below runs after the packages,
# under a timeout of its own, and the packages' run leaves it out by its exact name. It takes about 46 s
# because each of its 200 rounds stops a manager in the state a 200 ms poll leaves it in, which is part
# of what it proves, so it is not shortened (PR #291).
#
#   hack/go-test.sh packages   every package without that test, at go test's default timeout
#   hack/go-test.sh own        that test alone, which must really run and pass
#   hack/go-test.sh all        both in that order; the second runs even if the first fails
#
# A plain go test ./... still runs every test, in one package run.
set -uo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd) || exit 1
cd "$root" || exit 1

own_package=./internal/lifecycle
own_test=TestLifecycleManagerReleasesItsLeaseOnEveryStop
# About 3x the test's worst on CI, 48.8 s on main's healthy runs (the science agent, 9 Nov 2026).
own_timeout=150s

packages() {
  go test -count=1 -v -skip "^${own_test}\$" ./...
}

# own fails unless the test really passed: go test exits 0 when -run matches nothing, so a rename would
# otherwise drop the test without a word, and a skip reports as success.
own() {
  local out status
  out=$(mktemp) || return 1
  go test -count=1 -v -timeout "$own_timeout" -run "^${own_test}\$" "$own_package" 2>&1 | tee "$out"
  status=${PIPESTATUS[0]}
  if [ "$status" -eq 0 ] && ! grep -q -E "^--- PASS: ${own_test} \(" "$out"; then
    # It starts with FAIL so that hack/check.sh's filter of the output keeps it.
    echo "FAIL hack/go-test.sh: ${own_test} did not run and pass in ${own_package}, though go test exited 0: it was renamed, removed or skipped, and its name here must match"
    status=1
  fi
  rm -f "$out"
  return "$status"
}

case "${1:-}" in
  packages) packages ;;
  own) own ;;
  all)
    packages
    first=$?
    own
    second=$?
    [ "$first" -eq 0 ] || exit "$first"
    exit "$second"
    ;;
  *)
    echo "usage: hack/go-test.sh packages|own|all" >&2
    exit 2
    ;;
esac
