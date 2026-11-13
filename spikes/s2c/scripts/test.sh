#!/usr/bin/env bash
# test.sh: every test of spike S2c, the dry run included (loopback and Unix sockets only, no GPU, no
# cluster). Exits non-zero on any failure.
#
#   HPCBURST_ENVOY_DIR=<dir with envoy-1.40.2/envoy> S2C_VR_BIN=<vllm-router 0.1.17> \
#     spikes/s2c/scripts/test.sh
#
# HPCBURST_ENVOY_DIR is what hack/fetch-envoy.sh DIR fills; S2C_VR_BIN is what scripts/setup-vr.sh
# prints. Both are required: the dry run and the real-Envoy test fail without them instead of skipping.
# S2C_BUILD_WRAP, when set, prefixes every Go compile (a shared host's build lock, for example); the
# test binaries then run outside it. Run it under a memory cap on a shared host (README, Reproduce).
set -euo pipefail
cd "$(dirname "$0")/.."
command -v go > /dev/null || { echo "test.sh: go is not on PATH" >&2; exit 2; }
missing=()
[[ -x ${HPCBURST_ENVOY_DIR:-/nonexistent}/envoy-1.40.2/envoy ]] || missing+=(HPCBURST_ENVOY_DIR)
[[ -x ${S2C_VR_BIN:-/nonexistent} ]] || missing+=(S2C_VR_BIN)
if ((${#missing[@]} > 0)); then echo "test.sh: set ${missing[*]} (see the header)" >&2; exit 2; fi
wrap=(${S2C_BUILD_WRAP:-})
out=$(mktemp -d)
trap 'rm -rf "$out"' EXIT

# Go: vet and build the whole module (a spike is its own module: the root's vet never sees it), then
# compile each package's tests and run the binaries outside any build lock.
"${wrap[@]}" go vet ./...
"${wrap[@]}" go build ./...
mkdir -p "$out/bin"
"${wrap[@]}" go build -o "$out/bin/s2crouter" ./cmd/s2crouter
"${wrap[@]}" go build -o "$out/bin/s2cload" ./cmd/s2cload
for p in cmd/s2crouter cmd/s2cload dryrun; do
  "${wrap[@]}" go test -c -o "$out/${p//\//_}.test" "./$p"
done
export HPCBURST_REQUIRE_ENVOY=1 S2C_REQUIRE_DRYRUN=1 S2C_DRY_BIN=$out/bin

# From here every step runs whatever an earlier one did, and the run fails at the end, naming each failed step.
failed=()
step() { local name=$1; shift; if "$@"; then echo "test.sh: ok   $name"; else echo "test.sh: FAIL $name"; failed+=("$name"); fi; }
dryrun() {
  (cd dryrun && "$out/dryrun.test" -test.v -test.timeout 40m) > "$out/dryrun.log" 2>&1; local rc=$?
  grep -E '^(--- |    --- |PASS|FAIL|ok)|dryrun_test.go:|failures:' "$out/dryrun.log" || true
  return $rc
}
step "launcher tests" bash -c "cd cmd/s2crouter && '$out/cmd_s2crouter.test'"
step "client tests" bash -c "cd cmd/s2cload && '$out/cmd_s2cload.test'"
step "dry run" dryrun
step "placement middleware" python3 -I -B py/test_s2c_placement.py
step "analysis" python3 -I -B scripts/test_analyze.py
step "publication check's change" python3 -I -B scripts/test_pubcheck.py
step "job status" bash scripts/test_job_status.sh
step "queue check" bash scripts/test_queue_check.sh
# Publication check over every file git would commit under spikes/s2c, and over the diff against origin/main
# when it exists. A finding fails the run.
step "publication check" python3 -I -B scripts/pubcheck.py --ips --git .
if git rev-parse --verify -q origin/main > /dev/null; then step "publication check of the diff" python3 -I -B scripts/pubcheck.py --ips --diff origin/main; fi
if ((${#failed[@]} > 0)); then echo "test.sh: FAILED: ${failed[*]}"; exit 1; fi
echo "test.sh: all passed"
