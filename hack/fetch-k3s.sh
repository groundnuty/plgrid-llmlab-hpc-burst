#!/usr/bin/env bash
# Download the pinned k3s release binaries and check their sha256.
#
# usage: hack/fetch-k3s.sh [DEST_DIR] [VERSION...]
#
# DEST_DIR defaults to $HPCBURST_BIN_DIR, else $XDG_CACHE_HOME/hpcburst/k3s, else
# ~/.cache/hpcburst/k3s. Each binary lands at DEST_DIR/k3s-X.Y.Z/k3s, which is where
# `hpcburst up --bin-dir DEST_DIR` looks for it. With no VERSION, every version pinned for this
# machine's architecture is fetched; VERSION is as in `hpcburst up --version` (1.36.5, v1.36.5+k3s1).
#
# The pins are in internal/launcher/k3s-pins.txt, the same file the launcher embeds and checks
# before it starts a binary. A file that is already there with the right hash is left alone; a
# download with the wrong hash is deleted and the script fails. Nothing is written outside DEST_DIR.
#
# K3S_RELEASE_BASE overrides the download site (default: the k3s-io/k3s GitHub releases), for
# mirrors and for tests. K3S_PINS_FILE overrides the pins file, for tests only.
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
pins=${K3S_PINS_FILE:-$here/../internal/launcher/k3s-pins.txt}
base=${K3S_RELEASE_BASE:-https://github.com/k3s-io/k3s/releases/download}

dest=${1:-${HPCBURST_BIN_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/hpcburst/k3s}}
[ $# -gt 0 ] && shift
want=("$@")

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
  echo "fetch-k3s.sh: no pinned k3s binary for $machine" >&2
  exit 1
fi
[ -r "$pins" ] || { echo "fetch-k3s.sh: cannot read $pins" >&2; exit 1; }

matches() { # does version $1 (v1.36.5+k3s1) match one of the requested names?
  local v=${1#v} w
  [ ${#want[@]} -eq 0 ] && return 0
  for w in "${want[@]}"; do
    w=${w#v}
    if [[ $w == *+* ]]; then
      [ "$v" = "$w" ] && return 0
    else
      [ "${v%%+*}" = "$w" ] && return 0
    fi
  done
  return 1
}

fetched=0
while read -r version parch asset sum; do
  case $version in '' | '#'*) continue ;; esac
  [ "$parch" = "$arch" ] || continue
  matches "$version" || continue
  if [[ ! $sum =~ ^[0-9a-f]{64}$ ]]; then
    echo "fetch-k3s.sh: $pins has a bad sha256 for $version: $sum" >&2
    exit 1
  fi
  core=${version#v}; core=${core%%+*}
  dir=$dest/k3s-$core
  target=$dir/k3s
  if [ -f "$target" ] && echo "$sum  $target" | sha256sum -c --status -; then
    echo "ok       $version ($target)"
    fetched=$((fetched + 1))
    continue
  fi
  mkdir -p "$dir"
  url=$base/${version//+/%2B}/$asset
  part=$target.part.$$
  trap 'rm -f "$part"' EXIT
  echo "fetching $version from $url"
  curl -fsSL --retry 3 -o "$part" "$url"
  if ! echo "$sum  $part" | sha256sum -c --status -; then
    got=$(sha256sum "$part" | cut -d' ' -f1)
    rm -f "$part"
    echo "fetch-k3s.sh: checksum mismatch for $version: got $got, the pin says $sum" >&2
    exit 1
  fi
  chmod 755 "$part"
  mv "$part" "$target"
  trap - EXIT
  echo "verified $version ($target)"
  fetched=$((fetched + 1))
done < "$pins"

if [ "$fetched" -eq 0 ]; then
  echo "fetch-k3s.sh: no pinned version matches '${want[*]:-}' for $arch (see $pins)" >&2
  exit 1
fi
