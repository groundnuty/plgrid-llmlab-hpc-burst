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
# modules are built, and -mod=readonly (appended to the caller's GOFLAGS, whose -p stays) keeps the
# build from editing go.mod.
#
# The toolchain is the module's pinned Go: GOTOOLCHAIN is set to go.mod's go line, exactly (go 1.26.0
# gives go1.26.0, which is what CI installs from go.mod), whatever GOTOOLCHAIN or Go the caller has.
# A Go of another version is not used, so the bytes, and the agentDigest, do not depend on the
# machine. When that Go is not the one on PATH, the go command downloads it into the module cache
# (GOMODCACHE) once, and when it cannot, the script exits 1 before it builds anything. The script
# prints `go version` next to each digest.
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
# The pinned toolchain, from go.mod's go line (a line without a patch number is its .0 release).
gover=$(awk '$1 == "go" { print $2; exit }' "$root/go.mod")
[[ $gover =~ ^[0-9]+\.[0-9]+(\.[0-9]+)?$ ]] || die "$root/go.mod has no go line of the form 1.26.0, so there is no toolchain to pin ('$gover'); nothing was built"
[[ $gover =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || gover=$gover.0
export GOTOOLCHAIN=go$gover
goversion=$(cd "$root" && go version 2>&1) && [[ $goversion == "go version go$gover "* ]] ||
  die "cannot run Go $gover, the toolchain go.mod pins (GOTOOLCHAIN=$GOTOOLCHAIN): ${goversion//$'\n'/ }; nothing was built"

mkdir -p "$dir"
# The stage is this run's own directory, and the only thing it removes. DIR itself is never
# removed, since another run may be using it.
stage=$(mktemp -d "$dir/.build.XXXXXX")
trap 'rm -rf "$stage"' EXIT

for cpu in "${archs[@]}"; do
  out=$dir/hb-agent-$cpu
  (
    cd "$root"
    CGO_ENABLED=0 GOOS=linux GOARCH=$(go_arch "$cpu") GOAMD64=v1 GOARM64=v8.0 GOEXPERIMENT= GOWORK=off GOFLAGS="${GOFLAGS:+$GOFLAGS }-mod=readonly" \
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
