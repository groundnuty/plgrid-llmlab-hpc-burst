#!/usr/bin/env bash
# The one command for spike S3: fetch the pinned binaries, compile the e2e binary once and the
# harness, then measure every host option one at a time (never two at once) and print each
# summary. Results go to results/. Needs Go on PATH and SCRATCH set. Expect about 15 minutes per option on a quiet VM.
#   SCRATCH=<dir> scripts/run-all.sh             # envtest-bare, envtest-gc, kcp
#   SCRATCH=<dir> scripts/run-all.sh kcp         # one option
set -euo pipefail
cd "$(dirname "$0")/.."
SCRATCH=${SCRATCH:?set SCRATCH to a writable directory with at least 6 GB free}
command -v go >/dev/null || { echo "Go 1.27.x must be on PATH" >&2; exit 1; }
export SCRATCH
[ "${SKIP_FETCH:-0}" = 1 ] || scripts/fetch.sh   # SKIP_FETCH=1 when the binaries are already in $SCRATCH
go vet ./...
go test ./cmd/s3bench/                                  # the upgrade comparison can say "not kept"
nice -n 10 go test -c -o "$SCRATCH/bin/e2e.test" ./e2e   # the single e2e binary every option runs
nice -n 10 go build -o "$SCRATCH/bin/s3bench" ./cmd/s3bench
sha256sum "$SCRATCH/bin/e2e.test" | cut -c1-16 | sed 's/^/e2e binary sha256 (first 16): /'
options=("$@"); [ ${#options[@]} -gt 0 ] || options=(envtest-bare envtest-gc kcp)
for o in "${options[@]}"; do
  "$SCRATCH/bin/s3bench" -option "$o" -out results
done
