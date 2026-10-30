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
echo "== skipped tests (each one is coverage that did not run)" >&2
grep -E -B1 -e '--- SKIP' "$log" >&2 || echo "none" >&2
exit "$status"
