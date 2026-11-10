#!/usr/bin/env bash
# The root module's tests: the one definition that hack/check.sh and CI's go job both run (issue #282).
#
# go test's 10-minute package timeout stays: a raised one would hide a real hang for longer. So the tests
# that would bring a package close to it run after the packages, in groups, each group under a timeout of
# its own, and the packages' run leaves them out by their exact names:
#   - lease: internal/lifecycle's one test that takes about 46 s, because each of its 200 rounds stops a
#     manager in the state a 200 ms poll leaves it in, which is part of what it proves (PR #291);
#   - livechecks-hostdialed and livechecks-dry: hack/livechecks' dry runs through hack/live-checks.sh, the
#     tests that build and run the real conformance and alpha test binaries and a real agent, which took
#     that package to the 10 minutes in the crd and envtest job (the science agent's ruling on PR #306).
#
#   hack/go-test.sh packages        every package without the groups' tests, at go test's default timeout
#   hack/go-test.sh own [group]     one group, or every group in turn; each test must really run and pass
#   hack/go-test.sh all             both in that order; the groups run even if the packages fail
#
# A plain go test ./... still runs every test, in one package run.
set -uo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd) || exit 1
cd "$root" || exit 1

# group | package | its tests, by exact name | its timeout. A timeout is about 3x the group's worst run on
# CI (PR #291's rule): lease's 48.8 s on main's healthy runs (the science agent, 9 Nov 2026);
# livechecks-hostdialed's 45.1 s and livechecks-dry's 88.2 s, both in the crd and envtest job of PR #306's
# run on a25f9438 (42.8 s and 82.0 s in its go job).
groups=(
  "lease|./internal/lifecycle|TestLifecycleManagerReleasesItsLeaseOnEveryStop|150s"
  "livechecks-hostdialed|./hack/livechecks|TestEachHostDialedFaultFailsTheRowThroughTheScript TestEveryHostDialedRowPassesThroughTheScriptOverTheFake TestAHostDialedRowWhoseSlotIsHeldIsNotRunThroughTheScript TestADryHostDialedBinaryRefusesLiveSettings|150s"
  "livechecks-dry|./hack/livechecks|TestEveryAlphaRowPassesItsDryRunAndEveryFaultFailsIt TestADryBinaryRefusesLiveSettings TestAJobPendingPastTheBoundIsNotRunOnEveryRow TestALiveRunWithNoListenerFailsLoudlyAndUsesNoCluster TestAnAccountingRowWithNoSlotThatPendedPastItsWaitIsNotRun TestEveryRowThatPostsNamesWhereTheFrontRanAndRefusesWithoutIt TestACatchUpThatCannotTellIsAFailureOnATwoJobRow TestASecondJobThatNeverStartedIsNotRunOnTheTwoJobRows TestEveryReachRowReportsEachOutcomeThroughTheScript TestADryReachBinaryRefusesLiveSettingsAndARealGateway TestAReachRowWhoseSlotIsHeldIsNotRun|270s"
)

# field <row> <n>: the row's n-th field (1-based).
field() { local IFS='|'; local -a f; read -r -a f <<<"$1"; printf '%s' "${f[$(($2 - 1))]}"; }

# regexp <names...>: the anchored alternation of the exact names, as -run and -skip take it.
regexp() { local IFS='|'; printf '^(%s)$' "$*"; }

# every_name: every group's tests, one per line.
every_name() { local g; for g in "${groups[@]}"; do tr ' ' '\n' <<<"$(field "$g" 3)"; done; }

# declared_once <names...>: each name is a test declared in exactly one test file of the root module
# (spikes and third_party are modules of their own). -skip applies to every package, so a second test of
# the same name would vanish from every run without a word, and a rename would leave its group running
# nothing.
declared_once() {
  local name n bad=0
  for name in "$@"; do
    n=$(grep -rl --include='*_test.go' --exclude-dir=.git --exclude-dir=spikes --exclude-dir=third_party -E "^func ${name}\(" . | wc -l)
    if [ "$n" -ne 1 ]; then
      # It starts with FAIL so that hack/check.sh's filter of the output keeps it.
      echo "FAIL hack/go-test.sh: ${name} is declared in ${n} test files of the root module, not one: its name here must be a test's own, and unique"
      bad=1
    fi
  done
  return "$bad"
}

packages() {
  local -a names
  mapfile -t names < <(every_name)
  declared_once "${names[@]}" || return 1
  go test -count=1 -v -skip "$(regexp "${names[@]}")" ./...
}

# run_group <row>: the group's tests alone, under its timeout. It fails unless every one of them really
# passed and nothing in the run skipped: go test exits 0 when -run matches nothing, a skip reports as
# success, and a subtest that skips (a missing sshd) leaves its parent's PASS line in place.
run_group() {
  local g=$1 name pkg timeout out status bad=0 t
  name=$(field "$g" 1) pkg=$(field "$g" 2) timeout=$(field "$g" 4)
  local -a tests
  read -r -a tests <<<"$(field "$g" 3)"
  declared_once "${tests[@]}" || return 1
  out=$(mktemp) || return 1
  go test -count=1 -v -timeout "$timeout" -run "$(regexp "${tests[@]}")" "$pkg" 2>&1 | tee "$out"
  status=${PIPESTATUS[0]}
  if [ "$status" -eq 0 ]; then
    for t in "${tests[@]}"; do
      if ! grep -q -E "^--- PASS: ${t} \(" "$out"; then
        echo "FAIL hack/go-test.sh: ${t} did not run and pass in ${pkg}, though go test exited 0: it was renamed, removed or skipped, and its name here must match"
        bad=1
      fi
    done
    if grep -q -E '^[[:space:]]*--- SKIP: ' "$out"; then
      echo "FAIL hack/go-test.sh: the ${name} group skipped a test or a subtest in ${pkg}, which is coverage that did not run:"
      grep -E '^[[:space:]]*--- SKIP: ' "$out" | sed 's/^[[:space:]]*/  /'
      bad=1
    fi
    [ "$bad" -eq 0 ] || status=1
  fi
  rm -f "$out"
  return "$status"
}

# own [group]: that group, or every group in turn, each even if an earlier one failed.
own() {
  local g want=${1:-} status=0 ran=0 rc
  for g in "${groups[@]}"; do
    if [ -z "$want" ] || [ "$(field "$g" 1)" = "$want" ]; then
      ran=1
      run_group "$g"
      rc=$?
      [ "$status" -ne 0 ] || status=$rc
    fi
  done
  if [ "$ran" -eq 0 ]; then
    echo "usage: hack/go-test.sh own [group]: there is no group ${want}" >&2
    return 2
  fi
  return "$status"
}

case "${1:-}" in
  packages)
    [ "$#" -eq 1 ] || { echo "usage: hack/go-test.sh packages" >&2; exit 2; }
    packages
    ;;
  own)
    [ "$#" -le 2 ] || { echo "usage: hack/go-test.sh own [group]" >&2; exit 2; }
    own "${2:-}"
    ;;
  all)
    [ "$#" -eq 1 ] || { echo "usage: hack/go-test.sh all" >&2; exit 2; }
    packages
    first=$?
    own
    second=$?
    [ "$first" -eq 0 ] || exit "$first"
    exit "$second"
    ;;
  *)
    echo "usage: hack/go-test.sh packages|own [group]|all" >&2
    exit 2
    ;;
esac
