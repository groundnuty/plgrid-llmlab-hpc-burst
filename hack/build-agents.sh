#!/usr/bin/env bash
# Cross-compile the allocation agent (cmd/hb-agent) for each cpuArch and place it where the
# Provider's controller finds it.
#
# usage: hack/build-agents.sh DIR [CPUARCH...]
#
# DIR is the controller's agent-builds directory (<state>/agent-builds, agentapi.Host.AgentBuildsDir),
# the one hack/fetch-frp.sh --agent-builds DIR places frpc in. For each cpuArch (x86_64 and aarch64,
# or only those named) the build is DIR/hb-agent-<cpuArch> (agentapi.AgentBuildName), mode 755. Its
# sha256 is the agentDigest the lifecycle controller sets, and the script prints it. The aarch64 build
# is the one Helios's GH200 nodes need; nothing else makes it.
#
# The build is a static binary for linux, the same bytes whoever runs it, wherever the checkout is
# and whatever its git state: CGO_ENABLED=0 (no C library, so it runs on any node), -trimpath (no
# build directory in the binary), -buildvcs=false (no commit or dirty flag in it), and the
# architecture levels pinned to Go's defaults, so a GOAMD64 in the caller's environment cannot make
# an x86_64 build that older nodes cannot run. GOWORK=off keeps a go.work file from changing which
# modules are built, and GOFLAGS=-mod=readonly keeps the build from editing go.mod. The bytes depend
# on the Go toolchain, so the script prints `go version` next to each digest: a build with another
# toolchain is another digest. go.mod's go line is the floor, and whether a newer toolchain is
# fetched is the caller's GOTOOLCHAIN (CI sets local).
#
# GOARCH comes from hack/cpu-arch.sh, the one table of the architecture names (the launcher's
# cpuArchOf is the Go one). A cpuArch it does not know stops the run before anything is built.
#
# Each binary is built inside DIR under a name of its own and renamed into place, so two runs that
# start at once never write the same file and a reader never sees a half-written binary. A DIR that
# already holds a build with the same bytes keeps it. A cpuArch whose build fails leaves the file
# already at its name as it was, and the run exits 1; the cpuArchs built before it stay.
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
# shellcheck source=hack/cpu-arch.sh
. "$root/hack/cpu-arch.sh"

die() { echo "build-agents.sh: $*" >&2; exit 1; }

[ $# -ge 1 ] && [ -n "$1" ] || die "usage: hack/build-agents.sh DIR [CPUARCH...]"
dir=$1
shift
if [ $# -gt 0 ]; then
  archs=("$@")
else
  mapfile -t archs < <(cpu_archs)
fi
# Everything that can be refused is refused before the first build.
for cpu in "${archs[@]}"; do
  go_arch "$cpu" > /dev/null || die "cpuArch $cpu is not one of: $(cpu_archs | paste -sd' ' -); nothing was built"
done
command -v go > /dev/null || die "go is not on PATH"

mkdir -p "$dir"
# The stage is this run's own directory, and the only thing it removes. DIR itself is never
# removed, since another run may be using it.
stage=$(mktemp -d "$dir/.build.XXXXXX")
trap 'rm -rf "$stage"' EXIT

goversion=$(cd "$root" && go version)
for cpu in "${archs[@]}"; do
  out=$dir/hb-agent-$cpu
  (
    cd "$root"
    CGO_ENABLED=0 GOOS=linux GOARCH=$(go_arch "$cpu") GOAMD64=v1 GOARM64=v8.0 GOEXPERIMENT= GOWORK=off GOFLAGS=-mod=readonly \
      go build -trimpath -buildvcs=false -o "$stage/hb-agent-$cpu" ./cmd/hb-agent
  ) || die "building cmd/hb-agent for $cpu failed; $out is as it was"
  chmod 755 "$stage/hb-agent-$cpu"
  sum=$(sha256sum "$stage/hb-agent-$cpu" | cut -d' ' -f1)
  if [ -x "$out" ] && [ "$(sha256sum "$out" | cut -d' ' -f1)" = "$sum" ]; then
    echo "ok       hb-agent-$cpu ($out) sha256 $sum ($goversion)"
    continue
  fi
  mv -fT "$stage/hb-agent-$cpu" "$out"
  echo "built    hb-agent-$cpu ($out) sha256 $sum ($goversion)"
done
