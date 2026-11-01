#!/usr/bin/env bash
# The one-host alpha rehearsal (issue #146): builds the four binaries and the
# rehearsal test, then runs the whole stack on this host, the real hpcburst up
# over a fake Slurm cluster in run mode reached over real ssh and sshd.
#
#   HPCBURST_BIN_DIR=<dir with k3s-1.36.5/k3s> HPCBURST_FRP_DIR=<dir with frps, frpc> hack/rehearsal.sh
#
# Optional: HPCBURST_SSHD (default /usr/sbin/sshd), REHEARSAL_RUN (a -test.run
# pattern, default every rehearsal test), REHEARSAL_OUT (where the binaries go,
# default a new temporary directory). It binds 127.0.0.1 only, on ports
# 29500-29539 and kernel-chosen ones, and never touches a system k3s.
set -euo pipefail
cd "$(dirname "$0")/.."
: "${HPCBURST_BIN_DIR:?set HPCBURST_BIN_DIR to a directory with k3s-1.36.5/k3s (hack/fetch-k3s.sh DIR 1.36.5)}"
: "${HPCBURST_FRP_DIR:?set HPCBURST_FRP_DIR to a directory with the pinned frps and frpc (hack/fetch-frp.sh DIR)}"
out="${REHEARSAL_OUT:-$(mktemp -d)}"
mkdir -p "$out"
echo "building into $out"
go build -o "$out/" ./cmd/hpcburst ./cmd/hpcburst-controller ./cmd/hpcburst-front ./cmd/hb-agent
go test -c -o "$out/rehearsal.test" ./internal/rehearsal/scenario
cd internal/rehearsal/scenario
HPCBURST_REHEARSAL=1 HPCBURST_REHEARSAL_BIN="$out" HPCBURST_REQUIRE_FRP=1 HPCBURST_REQUIRE_SSHD=1 \
	"$out/rehearsal.test" -test.v -test.timeout 15m -test.run "${REHEARSAL_RUN:-^TestRehearsal}"
