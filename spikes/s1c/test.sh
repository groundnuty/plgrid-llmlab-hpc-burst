#!/usr/bin/env bash
# Spike S1c: every check of the harness, in one command.
#
#   S1C_SCRATCH=<dir> S1C_SIM=<llm-d-inference-sim> S1C_ENVOY=<envoy> bash test.sh
#
# 1. go vet, the s1cbench unit tests, and a rebuild of $S1C_SCRATCH/bin/s1cbench from this source,
#    so the integration checks below never run a stale binary;
# 2. the result rules' unit tests (tests/test_analysis.py);
# 3. the engine double's identity: its sha256 and the module and VCS revision Go embedded in it
#    (spike S2's build of llm-d-inference-sim v1.0.0 at 7bda03f with its loopback patch);
# 4. s1c_selftest.py: the engine's pacing, the latency and bandwidth toxics, the frozen-path fixture
#    (and why toxiproxy's own toxics are not one), that frps cannot serve wss itself, the probe
#    against frps, and loopback-only listeners;
# 5. that nothing is left running or listening on 29400-29499.
set -euo pipefail
: "${S1C_SCRATCH:?}" "${S1C_SIM:?}" "${S1C_ENVOY:?}"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$here"
go vet ./...
nice -n 10 go test -count=1 ./...
CGO_ENABLED=0 nice -n 10 go build -trimpath -o "$S1C_SCRATCH/bin/s1cbench.new" ./cmd/s1cbench
mv "$S1C_SCRATCH/bin/s1cbench.new" "$S1C_SCRATCH/bin/s1cbench"
echo "rebuilt s1cbench from this source"
python3 tests/test_analysis.py
echo "engine: sha256 $(sha256sum "$S1C_SIM" | cut -d' ' -f1)"
go version -m "$S1C_SIM" | grep -E $'^\tmod\t|vcs.revision|vcs.modified' | sed 's/^\t/  /'
go version -m "$S1C_SIM" | grep -q 'vcs.revision=7bda03f9fef85139617cb4f724dccafe1cf8cbbf' \
  || { echo "test.sh: the engine is not built from llm-d-inference-sim 7bda03f" >&2; exit 1; }
python3 s1c_selftest.py
python3 s1c.py leftovers
rm -rf __pycache__ tests/__pycache__
