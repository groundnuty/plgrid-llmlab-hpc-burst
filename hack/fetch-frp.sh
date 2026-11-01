#!/usr/bin/env bash
# Download the pinned frp release and install its frps and frpc into a directory.
#
# usage: hack/fetch-frp.sh DIR
#        hack/fetch-frp.sh --agent-builds DIR
#
# The first form is for the tests (below). The second, for operators, places frpc for every
# architecture that internal/launcher/frp-pins.txt pins as DIR/frpc-<cpuArch>, where DIR is the
# controller's agent-builds directory (<state>/agent-builds, agentapi.Host.AgentBuildsDir); the
# Provider's controller stages that file for a partition of the cpuArch, and only when its sha256 is
# the pinned frpc digest (HBEP-4, Submission: Files first). It needs no frps and runs on any host
# architecture, since it never runs what it fetches. See "agent-builds mode" below.
#
# frp v0.71.1 linux_amd64 (https://github.com/fatedier/frp, Apache-2.0) carries the tunnel of
# HBEP-6: frpc in the agent, frps in the launcher. The tests that start them read the directory
# from HPCBURST_FRP_DIR (internal/testenv, FrpBinaries). The tarball's sha256 is the one spike S1c
# pinned (spikes/s1c/fetch.sh; source: the release's frp_sha256_checksums.txt and GitHub's digest
# of the asset). Nothing is extracted before the tarball matches it. A download with another hash
# is deleted and the script exits 1, leaving no file in DIR (a DIR it had to create stays, empty:
# a run never removes the directory itself, because another run may be using it). The two binaries are checked against their own pins too, which are the sha256 of
# frps and frpc as extracted from that verified tarball: that is what makes a second run a no-op.
#
# Both files are staged inside DIR under a name of their own and renamed into place, so two runs
# that start at once never write the same file and a reader never sees a half-written binary.
#
# FRP_TARBALL names an already downloaded tarball to use instead of the download. It is copied
# and verified exactly like a download, and the original is never touched. FRP_RELEASE_BASE
# overrides the download site, for mirrors.
#
# agent-builds mode (--agent-builds DIR) reads frp-pins.txt itself, so it holds no copy of an
# architecture's sums that could drift. For each of the pinned release's lines it fetches the
# line's tarball from the release, checks the tarball's sha256 against the line's tarball column
# before extracting anything, extracts frpc, checks frpc's sha256 against the line's frpc column,
# and renames it into place as DIR/frpc-<cpuArch>, mode 755. frp names its architectures amd64 and
# arm64, the platform profiles x86_64 and aarch64, and cpu_arch (hack/cpu-arch.sh, the one table that
# hack/build-agents.sh reads too; the launcher's archTable is the Go one, and a test compares the two); a line whose
# architecture it does not know stops the run before anything is fetched. An architecture is
# fetched on its own: one whose tarball or frpc does not match is deleted and the run exits 1, and
# one that has been placed already stays. A DIR that already holds a matching frpc-<cpuArch> is left
# as it is, and nothing is fetched for it. FRP_TARBALL does not apply (two tarballs); a mirror is
# FRP_RELEASE_BASE, whose layout is <base>/v<version>/<tarball>, and FRP_PINS_FILE names another pins
# file, for tests.
set -euo pipefail

FRP_VERSION=0.71.1
FRP_TARBALL_SHA=1d9875210972a5d1a62b2dc4c48df45ab0af05d3ca1b5e494a014428259df803
FRPS_SHA=2322762939e66e66850bbc9103b6d73fec39e83bd8ad6c71ebeeb120d56d6ab3
FRPC_SHA=35c6054d4549bbcf2f02875543895fd9e44c2aae2925f61cd8ff5a693703c28b

base=${FRP_RELEASE_BASE:-https://github.com/fatedier/frp/releases/download}
name=frp_${FRP_VERSION}_linux_amd64

die() { echo "fetch-frp.sh: $*" >&2; exit 1; }

# sha_ok FILE SHA: FILE exists and has that sha256.
sha_ok() { [ -f "$1" ] && echo "$2  $1" | sha256sum -c --status -; }

# cpu_arch FRP_ARCH: the platform profiles' cpuArch for frp's architecture name; fails for any other.
# The one table is hack/cpu-arch.sh's, which hack/build-agents.sh reads too.
# shellcheck source=hack/cpu-arch.sh
. "$(dirname "${BASH_SOURCE[0]}")/cpu-arch.sh"

# agent_builds DIR: agent-builds mode, see the header.
agent_builds() {
  local dir=$1 pins=${FRP_PINS_FILE:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/internal/launcher/frp-pins.txt}
  local rows arch tarball tsha csha cpu out got name
  [ -f "$pins" ] || die "the pins file $pins is not a file"
  # The release's lines: arch, tarball, tarball sha, frpc sha (columns 2, 3, 4 and 6).
  rows=$(awk -v v="$FRP_VERSION" '$1 !~ /^#/ && NF && $1 == v { print $2, $3, $4, $6 }' "$pins")
  [ -n "$rows" ] || die "frp $FRP_VERSION is not pinned in $pins"
  # Everything that can be refused is refused before the first download.
  while read -r arch tarball tsha csha; do
    cpu_arch "$arch" > /dev/null || die "$pins pins frp $FRP_VERSION for $arch, which has no cpuArch name; nothing was fetched"
    [[ $tsha =~ ^[0-9a-f]{64}$ && $csha =~ ^[0-9a-f]{64}$ ]] || die "$pins: the sums of the $arch line are not sha256 of 64 lowercase hex characters"
    [[ $tarball =~ ^[A-Za-z0-9._-]+\.tar\.gz$ ]] || die "$pins: the tarball name of the $arch line is not a plain file name"
  done <<< "$rows"

  mkdir -p "$dir"
  # The stage is this run's own directory, and the only thing it removes, as below. It is not local:
  # the exit trap runs after this function has returned.
  stage=$(mktemp -d "$dir/.frp.XXXXXX")
  trap 'rm -rf "$stage"' EXIT
  while read -r arch tarball tsha csha; do
    cpu=$(cpu_arch "$arch")
    out=$dir/frpc-$cpu
    if sha_ok "$out" "$csha" && [ -x "$out" ]; then
      echo "ok       frpc-$cpu ($out)"
      continue
    fi
    echo "fetching frp $FRP_VERSION $arch from $base/v$FRP_VERSION/$tarball"
    curl -fsSL --retry 3 --connect-timeout 15 --max-time 600 -o "$stage/$tarball" "$base/v$FRP_VERSION/$tarball" ||
      die "download of $base/v$FRP_VERSION/$tarball failed"
    got=$(sha256sum "$stage/$tarball" | cut -d' ' -f1)
    if [ "$got" != "$tsha" ]; then
      rm -f "$stage/$tarball"
      die "checksum mismatch for $tarball: got $got, the pin says $tsha; the download is deleted and nothing was extracted"
    fi
    name=${tarball%.tar.gz}
    mkdir "$stage/$arch"
    tar -xzf "$stage/$tarball" -C "$stage/$arch" --strip-components=1 "$name/frpc"
    sha_ok "$stage/$arch/frpc" "$csha" || die "frpc from the verified $tarball does not match its pin $csha"
    chmod 755 "$stage/$arch/frpc"
    mv -fT "$stage/$arch/frpc" "$out"
    echo "verified frpc-$cpu ($out) sha256 $csha"
  done <<< "$rows"
}

if [ "${1:-}" = "--agent-builds" ]; then
  [ $# -eq 2 ] && [ -n "$2" ] || die "usage: hack/fetch-frp.sh --agent-builds DIR"
  agent_builds "$2"
  exit 0
fi

[ $# -eq 1 ] && [ -n "$1" ] || die "usage: hack/fetch-frp.sh DIR | hack/fetch-frp.sh --agent-builds DIR"
dest=$1

case "$(uname -s) $(uname -m)" in
  "Linux x86_64" | "Linux amd64") ;;
  *) die "only linux_amd64 is pinned, and this machine is $(uname -s) $(uname -m)" ;;
esac

if sha_ok "$dest/frps" "$FRPS_SHA" && sha_ok "$dest/frpc" "$FRPC_SHA" && [ -x "$dest/frps" ] && [ -x "$dest/frpc" ]; then
  echo "ok       frp $FRP_VERSION ($dest)"
  exit 0
fi

mkdir -p "$dest"
stage=$(mktemp -d "$dest/.frp.XXXXXX")
# The stage is this run's own directory, and the only thing a run removes: on any exit it goes, with
# the download inside it. DIR itself is never removed, since a concurrent run may have just made it.
trap 'rm -rf "$stage"' EXIT

tgz=$stage/$name.tar.gz
if [ -n "${FRP_TARBALL:-}" ]; then
  [ -f "$FRP_TARBALL" ] || die "FRP_TARBALL=$FRP_TARBALL is not a file"
  cp "$FRP_TARBALL" "$tgz"
  echo "using    $FRP_TARBALL"
else
  url=$base/v$FRP_VERSION/$name.tar.gz
  echo "fetching frp $FRP_VERSION from $url"
  curl -fsSL --retry 3 --connect-timeout 15 --max-time 600 -o "$tgz" "$url" ||
    die "download of $url failed"
fi

got=$(sha256sum "$tgz" | cut -d' ' -f1)
if [ "$got" != "$FRP_TARBALL_SHA" ]; then
  rm -f "$tgz"
  die "checksum mismatch for $name.tar.gz: got $got, the pin says $FRP_TARBALL_SHA; the download is deleted and nothing was extracted"
fi

tar -xzf "$tgz" -C "$stage" --strip-components=1 "$name/frps" "$name/frpc"
sha_ok "$stage/frps" "$FRPS_SHA" || die "frps from the verified tarball does not match its pin $FRPS_SHA"
sha_ok "$stage/frpc" "$FRPC_SHA" || die "frpc from the verified tarball does not match its pin $FRPC_SHA"
chmod 755 "$stage/frps" "$stage/frpc"
# -T: DEST is the file's own path, never a directory to move into (an existing directory or a symlink
# to one at that path is an error, not a place to hide the binary).
mv -fT "$stage/frps" "$dest/frps"
mv -fT "$stage/frpc" "$dest/frpc"
echo "verified frp $FRP_VERSION ($dest/frps, $dest/frpc)"
