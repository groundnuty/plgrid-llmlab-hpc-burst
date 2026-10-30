#!/usr/bin/env bash
# Run every test of spike S2 phase B (no network, no GPU, a few seconds). Exits non-zero on any failure.
set -euo pipefail
cd "$(dirname "$0")/.."
command -v go > /dev/null || { echo "test.sh: go is not on PATH" >&2; exit 2; }
go vet ./...
go test ./...
python3 -I py/test_s2b_placement.py
python3 -I py/test_s2b_listeners.py
python3 -I scripts/test_analyze.py
python3 -I scripts/analyze.py results/gh200-1gpu | diff - results/gh200-1gpu/summary.md && echo "summary.md reproduces from the committed results"
