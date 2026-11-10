#!/usr/bin/env bash
# Spike S15: put the pinned frp binaries into $S15_BIN and build the harness there.
#
#   S15_BIN=<dir> bash fetch.sh
#
# frp v0.71.1 is spike S1c's pin (spikes/s1c/fetch.sh). The tarball is checked against its sha256
# before anything is extracted, and frps and frpc after extraction against
# internal/launcher/frp-pins.txt, the project's one source for them. A mismatch deletes the download
# and exits 1. S15_FRP_DIR (a directory holding frps and frpc) may name binaries already on disk
# instead; they are checked the same way. The link emulator is built from this module (cmd/s15link).
# This script is spike S14's fetch.sh (at 3f863bad) without toxiproxy.
set -euo pipefail

: "${S15_BIN:?set S15_BIN to the binaries directory}"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
pins="$here/../../internal/launcher/frp-pins.txt"
mkdir -p "$S15_BIN"
DL="$S15_BIN/.dl"
mkdir -p "$DL"

FRP_VERSION=0.71.1
FRP_SHA=1d9875210972a5d1a62b2dc4c48df45ab0af05d3ca1b5e494a014428259df803
read -r FRPS_SHA FRPC_SHA < <(awk '$1 == "0.71.1" && $2 == "amd64" { print $5, $6 }' "$pins")
[ -n "$FRPS_SHA" ] && [ -n "$FRPC_SHA" ] || { echo "fetch: no amd64 line for frp 0.71.1 in $pins" >&2; exit 1; }

verify() {  # verify <file> <sha256>
  local got
  got="$(sha256sum "$1" | cut -d' ' -f1)"
  if [ "$got" != "$2" ]; then
    echo "fetch: sha256 mismatch for $1: want $2, got $got" >&2
    return 1
  fi
  echo "OK   $(basename "$1") sha256 $got"
}

fetch() {  # fetch <url> <dest> <sha256>
  if [ -f "$2" ] && verify "$2" "$3" >/dev/null; then echo "have $(basename "$2") (verified)"; return; fi
  curl -fsSL --retry 3 --connect-timeout 15 -o "$2.part" "$1"
  if ! verify "$2.part" "$3"; then rm -f "$2.part"; echo "fetch: deleted the bad download" >&2; return 1; fi
  mv "$2.part" "$2"
}

if [ -n "${S15_FRP_DIR:-}" ]; then
  cp "$S15_FRP_DIR/frps" "$S15_FRP_DIR/frpc" "$S15_BIN/"
else
  tgz="$DL/frp_${FRP_VERSION}_linux_amd64.tar.gz"
  fetch "https://github.com/fatedier/frp/releases/download/v${FRP_VERSION}/frp_${FRP_VERSION}_linux_amd64.tar.gz" "$tgz" "$FRP_SHA"
  x="$(mktemp -d "$DL/frp.XXXXXX")"
  tar -xzf "$tgz" -C "$x"
  cp "$x/frp_${FRP_VERSION}_linux_amd64/frps" "$x/frp_${FRP_VERSION}_linux_amd64/frpc" "$S15_BIN/"
  rm -rf "$x"
fi
verify "$S15_BIN/frps" "$FRPS_SHA"
verify "$S15_BIN/frpc" "$FRPC_SHA"


cd "$here"
CGO_ENABLED=0 go build -trimpath -o "$S15_BIN/s15" ./cmd/s15
CGO_ENABLED=0 go build -trimpath -o "$S15_BIN/s15bridge" ./cmd/s15bridge
CGO_ENABLED=0 go build -trimpath -o "$S15_BIN/s15link" ./cmd/s15link
CGO_ENABLED=0 go test -c -trimpath -o "$S15_BIN/s15node.test" ./node
"$S15_BIN/frps" --version | sed 's/^/frps /'
go version -m "$S15_BIN/s15" | grep -E 'fatedier/yamux|^\s+go\s' | sed 's/^\s*/s15 /'
echo "ready: $S15_BIN"
