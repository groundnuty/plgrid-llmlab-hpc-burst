#!/usr/bin/env bash
# Download the pinned Envoy release binary for this machine's architecture and check its sha256.
#
# usage: hack/fetch-envoy.sh [DIR]
#
# DIR defaults to $HPCBURST_ENVOY_DIR. Each pinned version for this architecture lands at
# DIR/envoy-X.Y.Z/envoy, mode 755, where the tests look for it (HPCBURST_ENVOY_DIR=DIR,
# internal/testenv's EnvoyBinary; envoypin.BinaryPath). Envoy (https://github.com/envoyproxy/envoy,
# Apache-2.0) is HBEP-5's candidate router for an Instance, run as released.
#
# The pins are in internal/agent/envoypin/envoy-pins.txt, the file package envoypin embeds and checks
# a binary against before anything runs it, so this script holds no copy of a sum that could drift
# (HBEP-15). Every line of that file is checked for its form before anything is downloaded, by the
# same rules as the Go reader: four fields, a version such as 1.40.2, Go's amd64 or arm64, the
# release's own asset name for them, and a sum of 64 lowercase hex characters.
#
# A file that is already in place with its pinned sha256 is left alone, and nothing is downloaded for
# it. Any other file there is downloaded again. A download goes to a name of its own inside the
# version's directory, is checked, and only then renamed into place, so two runs that start at once
# never write the same file and a reader never sees half a binary. A download with another sha256 is
# deleted and the script exits 1. A directory it had to create stays, even empty: another run may be
# using it.
#
# DIR may be a restored CI cache (the workflow saves it between runs, so that a GitHub outage does not
# fail every job). It is never trusted: the binary is checked against its pin on every run.
#
# ENVOY_RELEASE_BASE overrides the download site (default: the envoyproxy/envoy GitHub releases),
# whose layout is <base>/v<version>/<asset>, for mirrors and for tests. ENVOY_PINS_FILE overrides the
# pins file, for tests only.
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
pins=${ENVOY_PINS_FILE:-$here/../internal/agent/envoypin/envoy-pins.txt}
base=${ENVOY_RELEASE_BASE:-https://github.com/envoyproxy/envoy/releases/download}

die() { echo "fetch-envoy.sh: $*" >&2; exit 1; }

dest=${1:-${HPCBURST_ENVOY_DIR:-}}
[ -n "$dest" ] || die "usage: hack/fetch-envoy.sh DIR (or set HPCBURST_ENVOY_DIR)"
[ $# -le 1 ] || die "usage: hack/fetch-envoy.sh DIR: one directory, no more arguments"
[ -f "$pins" ] || die "the pins file $pins is not a file"

# The pins name an architecture as Go does; this machine's name for it is the kernel's (x86_64,
# aarch64) or, on some systems, Go's own. hack/cpu-arch.sh is the one table of the two.
# shellcheck source=hack/cpu-arch.sh
. "$here/cpu-arch.sh"
machine=$(uname -m)
if go_arch "$machine" > /dev/null; then
  arch=$(go_arch "$machine")
elif cpu_arch "$machine" > /dev/null; then
  arch=$machine
else
  die "no pinned Envoy binary for $machine"
fi

# release_arch GOARCH: Envoy's name for it in its release assets.
release_arch() {
  case $1 in
    amd64) echo x86_64 ;;
    arm64) echo aarch_64 ;;
    *) return 1 ;;
  esac
}

# Every line is checked before the first download, whichever architecture it pins.
n=0
mine=()
while IFS= read -r line || [ -n "$line" ]; do
  n=$((n + 1))
  read -r -a f <<< "$line" || true
  [ ${#f[@]} -eq 0 ] && continue
  case ${f[0]} in '#'*) continue ;; esac
  [ ${#f[@]} -eq 4 ] || die "$pins line $n: ${#f[@]} fields, want 4 (version arch asset sha256)"
  version=${f[0]} parch=${f[1]} asset=${f[2]} sum=${f[3]}
  [[ $version =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || die "$pins line $n: version $version is not of the form 1.40.2"
  ra=$(release_arch "$parch") || die "$pins line $n: architecture $parch is not one Go names amd64 or arm64"
  [ "$asset" = "envoy-$version-linux-$ra" ] || die "$pins line $n: asset $asset is not Envoy $version's $parch release binary, envoy-$version-linux-$ra"
  [[ $sum =~ ^[0-9a-f]{64}$ ]] || die "$pins line $n: sha256 $sum is not 64 lowercase hex characters"
  [ "$parch" = "$arch" ] && mine+=("$version $asset $sum")
done < "$pins"
[ ${#mine[@]} -gt 0 ] || die "$pins pins no Envoy binary for $arch ($machine)"

part=
trap '[ -z "$part" ] || rm -f "$part"' EXIT
for row in "${mine[@]}"; do
  read -r version asset sum <<< "$row"
  dir=$dest/envoy-$version
  target=$dir/envoy
  if [ -f "$target" ] && [ -x "$target" ] && echo "$sum  $target" | sha256sum -c --status -; then
    echo "ok       envoy $version ($target)"
    continue
  fi
  mkdir -p "$dir"
  url=$base/v$version/$asset
  part=$(mktemp "$dir/.envoy.part.XXXXXX")
  echo "fetching envoy $version from $url"
  curl -fsSL --retry 3 --connect-timeout 15 --max-time 900 -o "$part" "$url" ||
    die "download of $url failed, and $target is not there with its pinned sha256"
  got=$(sha256sum "$part" | cut -d' ' -f1)
  if [ "$got" != "$sum" ]; then
    rm -f "$part"
    part=
    die "checksum mismatch for $asset: got $got, the pin says $sum; the download is deleted"
  fi
  chmod 755 "$part"
  mv -fT "$part" "$target"
  part=
  echo "verified envoy $version ($target) sha256 $sum"
done
