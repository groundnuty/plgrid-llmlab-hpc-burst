#!/usr/bin/env bash
# Run every test of spike S2 phase B (no network, no GPU, a few seconds). Exits non-zero on any failure.
# python3 -B: no bytecode is written, because a .pyc records the path of its source and would carry the
# machine's private path into any commit that picks it up (.gitignore keeps them out as well).
set -euo pipefail
cd "$(dirname "$0")/.."
command -v go > /dev/null || { echo "test.sh: go is not on PATH" >&2; exit 2; }
go vet ./...
go test ./...
python3 -I -B py/test_s2b_placement.py
python3 -I -B py/test_s2b_listeners.py
python3 -I -B scripts/test_analyze.py
python3 -I -B scripts/test_pubcheck.py
python3 -I -B scripts/analyze.py results/gh200-1gpu | diff - results/gh200-1gpu/summary.md && echo "summary.md reproduces from the committed results"
# Publication check over every file git would commit under spikes/s2b (and, when origin/main exists, over the
# whole diff against it), compressed contents included. A finding fails the run.
python3 -I -B scripts/pubcheck.py --ips --git .
if git rev-parse --verify -q origin/main > /dev/null; then python3 -I -B scripts/pubcheck.py --ips --diff origin/main; fi
