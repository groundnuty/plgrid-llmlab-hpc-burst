#!/usr/bin/env bash
# The checks a change to the root module must pass: formatting, vet, module
# tidiness, generated files, and the tests. It prints every skipped test at the
# end, because a skip reports as success.
#
# The control-plane tests need Kubernetes binaries; hack/test-binaries.sh finds
# or fetches them (set HPCBURST_TOOLS_CACHE to choose where they are cached).
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$root"

echo "== gofmt" >&2
# Only the root module's own Go files: spikes and forked code are separate
# modules, and gofmt on a directory would descend into them.
files=$(go list -f '{{$d := .Dir}}{{range .GoFiles}}{{$d}}/{{.}}{{"\n"}}{{end}}{{range .TestGoFiles}}{{$d}}/{{.}}{{"\n"}}{{end}}{{range .XTestGoFiles}}{{$d}}/{{.}}{{"\n"}}{{end}}' ./...)
unformatted=$(echo "$files" | xargs gofmt -l)
if [ -n "$unformatted" ]; then
  echo "unformatted files:" >&2
  echo "$unformatted" >&2
  exit 1
fi

echo "== go vet" >&2
go vet ./...

echo "== go mod tidy -diff" >&2
go mod tidy -diff

echo "== generated files" >&2
hack/verify-generated.sh

echo "== go test" >&2
log=$(mktemp)
trap 'rm -f "$log"' EXIT
set +e
go test -count=1 -v ./... 2>&1 | tee "$log" | grep -E '^(ok|FAIL|---\s+FAIL|panic|\?)' >&2
status=${PIPESTATUS[0]}
set -e
if [ "$status" -ne 0 ]; then
  echo "== output of the failing tests" >&2
  # For each "--- FAIL: <Name>" (subtests too), print that test's lines from its "=== RUN"
  # line to its "--- FAIL" line, at most 2000 lines per test; output of parallel tests may
  # interleave. A "panic:" block runs to the next "FAIL<tab>" or "exit status" line. awk
  # exits 0 when nothing matches, so set -e does not abort here.
  awk -v cap=2000 '
    function name_of(line,   n) {
      n = line
      sub(/^[ \t]*--- (FAIL|PASS|SKIP): /, "", n)
      sub(/ .*$/, "", n)
      return n
    }
    NR == FNR {
      if ($0 ~ /^[ \t]*--- FAIL: /) failing[name_of($0)] = 1
      next
    }
    /^=== RUN[ \t]+/ {
      n = $0
      sub(/^=== RUN[ \t]+/, "", n)
      sub(/[ \t].*$/, "", n)
      if (n in failing) { active[n] = 1; cnt[n] = 0 }
    }
    {
      show = 0
      for (n in active) {
        if (cnt[n] < cap) { cnt[n]++; show = 1 }
        else if (cnt[n] == cap) { cnt[n]++; print "... output of " n " truncated at " cap " lines" }
      }
      if (show) print
    }
    /^[ \t]*--- (FAIL|PASS|SKIP): / { n = name_of($0); if (n in active) delete active[n] }
    /^panic:/ { pan = 1; pcnt = 0 }
    pan && /^(FAIL\t|exit status)/ { pan = 0 }
    pan { if (pcnt < cap) { pcnt++; if (!show) print } }
  ' "$log" "$log" >&2
fi
echo "== skipped tests (each one is coverage that did not run)" >&2
grep -E -B1 -e '--- SKIP' "$log" >&2 || echo "none" >&2
exit "$status"
