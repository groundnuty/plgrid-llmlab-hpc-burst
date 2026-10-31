#!/usr/bin/env bash
# Spike S1c: every check of the harness, in one command.
#
#   S1C_SCRATCH=<dir> S1C_SIM=<llm-d-inference-sim> S1C_ENVOY=<envoy> bash test.sh
#
# 1. go vet and the s1cbench unit tests (SSE parsing, the CONNECT dialer, probe outcome classes, the
#    shared-rate shaper, the slow reader); 2. s1c_selftest.py, which checks the engine's pacing, the
#    latency and bandwidth toxics, the frozen-path fixture (and why toxiproxy's own toxics are not
#    one), that frps cannot serve wss itself, the probe against frps, and loopback-only listeners;
#    3. that nothing is left running or listening on 29400-29499.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$here"
go vet ./...
nice -n 10 go test -count=1 ./...
python3 s1c_selftest.py
python3 s1c.py leftovers
