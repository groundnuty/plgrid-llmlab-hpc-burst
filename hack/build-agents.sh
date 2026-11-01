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
# and whatever its git state or its caller's environment. CGO_ENABLED=0 (no C library, so it runs on
# any node), -trimpath (no build directory in the binary), -buildvcs=false (no commit or dirty flag
# in it), the architecture levels pinned to Go's defaults (a GOAMD64 in the caller's environment
# cannot make an x86_64 build that older nodes cannot run), GOEXPERIMENT empty and GOFIPS140=off, and
# GOWORK=off (a go.work file cannot change which modules are built).
#
# Nothing the caller or the machine has saved can change the bytes. GOENV=off makes the go command
# ignore its configuration file (go env -w), which an empty GOEXPERIMENT= would not. The variables
# that file may also have held for the toolchain's sake, GOPATH, GOMODCACHE, GOCACHE and GOPROXY, are
# read once before that and passed explicitly. GOFLAGS is replaced by -mod=readonly and the caller's
# -p=N, if it has one (parallelism does not change the bytes); every other entry (-ldflags, -gcflags,
# -tags, -overlay and the like can) is dropped, and the script says so on stderr.
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
# DIR may be relative to the working directory; it is made absolute first, since the build runs in the
# repository. Each binary is built inside DIR under a name of its own and renamed into place, so two
# runs that start at once never write the same file and a reader never sees a half-written binary. A
# DIR that already holds a build with the same bytes keeps it, and has its mode set to 755 if it
# was not. A cpuArch whose build fails leaves the file already at its name as it was, and the run exits
# 1; the cpuArchs built before it stay.
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
# shellcheck source=hack/cpu-arch.sh
. "$root/hack/cpu-arch.sh"

die() { echo "build-agents.sh: $*" >&2; exit 1; }
stage= goerr=
cleanup() {
  [ -z "$goerr" ] || rm -f "$goerr"
  # The stage is this run's own directory, and the only thing it removes. DIR itself is never
  # removed, since another run may be using it.
  [ -z "$stage" ] || rm -rf "$stage"
}
trap cleanup EXIT

[ $# -ge 1 ] && [ -n "$1" ] || die "usage: hack/build-agents.sh DIR [CPUARCH...]"
case $1 in
  /*) dir=$1 ;;
  *) dir=$PWD/$1 ;;
esac
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

# The go command's flags: the script's own, and the caller's -p=N, if any.
gofl=-mod=readonly dropped=()
read -ra callerflags <<< "${GOFLAGS:-}"
for f in ${callerflags[@]+"${callerflags[@]}"}; do
  if [[ $f =~ ^-p=[0-9]+$ ]]; then gofl="$gofl $f"; else dropped+=("$f"); fi
done
[ ${#dropped[@]} -eq 0 ] || echo "build-agents.sh: ignoring GOFLAGS entries that can change the bytes (only -p=N is kept): ${dropped[*]}" >&2
export GOFLAGS=$gofl

# The pinned toolchain, from go.mod's go line: the version after "go", up to a comment, such as 1.26.0,
# 1.26 (its .0 release) or 1.27rc1.
gover=$(awk '$1 == "go" { v = $2; sub(/\/\/.*/, "", v); print v; exit }' "$root/go.mod")
[[ $gover =~ ^[0-9]+\.[0-9]+(\.[0-9]+|(alpha|beta|rc)[0-9]+)?$ ]] || die "$root/go.mod has no go line of the form 1.26.0, 1.26 or 1.27rc1, so there is no toolchain to pin ('$gover'); nothing was built"
[[ $gover =~ ^[0-9]+\.[0-9]+$ ]] && gover=$gover.0
export GOTOOLCHAIN=go$gover
# stdout alone is the version: a download of the toolchain announces itself on stderr first.
goerr=$(mktemp)
goversion=$(cd "$root" && go version 2> "$goerr") && [[ $goversion == "go version go$gover "* ]] ||
  die "cannot run Go $gover, the toolchain go.mod pins (GOTOOLCHAIN=$GOTOOLCHAIN): ${goversion//$'\n'/ } $(tr '\n' ' ' < "$goerr"); nothing was built"
cat "$goerr" >&2
# What the go command's configuration file may hold for it, read before that file is ignored.
for v in GOPATH GOMODCACHE GOCACHE GOPROXY; do
  val=$(cd "$root" && go env "$v") || die "go env $v failed; nothing was built"
  export "$v=$val"
done
export GOENV=off GOEXPERIMENT= GOFIPS140=off

mkdir -p "$dir"
stage=$(mktemp -d "$dir/.build.XXXXXX")

for cpu in "${archs[@]}"; do
  out=$dir/hb-agent-$cpu
  (
    cd "$root"
    CGO_ENABLED=0 GOOS=linux GOARCH=$(go_arch "$cpu") GOAMD64=v1 GOARM64=v8.0 GOWORK=off \
      go build -trimpath -buildvcs=false -o "$stage/hb-agent-$cpu" ./cmd/hb-agent
  ) || die "building cmd/hb-agent for $cpu failed; $out is as it was"
  chmod 755 "$stage/hb-agent-$cpu"
  sum=$(sha256sum "$stage/hb-agent-$cpu" | cut -d' ' -f1)
  if [ -f "$out" ] && [ "$(sha256sum "$out" | cut -d' ' -f1)" = "$sum" ]; then
    note=
    if [ "$(stat -c %a "$out")" != 755 ]; then
      chmod 755 "$out"
      note=", mode set to 755"
    fi
    echo "ok       hb-agent-$cpu ($out) sha256 $sum ($goversion)$note"
    continue
  fi
  mv -fT "$stage/hb-agent-$cpu" "$out"
  echo "built    hb-agent-$cpu ($out) sha256 $sum ($goversion)"
done
