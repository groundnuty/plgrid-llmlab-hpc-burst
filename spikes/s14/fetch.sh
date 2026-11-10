#!/usr/bin/env bash
# Spike S14: put the pinned third-party binaries into $S14_BIN and build the harness there.
#
#   S14_BIN=<dir> bash fetch.sh
#
# frp v0.71.1 and toxiproxy v2.13.0 are spike S1c's pins (spikes/s1c/fetch.sh). Each tarball is
# checked against its sha256 before anything is extracted, and frps and frpc after extraction
# against internal/launcher/frp-pins.txt, the project's one source for them. A mismatch deletes the
# download and exits 1. S14_FRP_DIR (a directory holding frps and frpc) and S14_TOXI (a
# toxiproxy-server) may name binaries already on disk instead; they are checked the same way.
set -euo pipefail

: "${S14_BIN:?set S14_BIN to the binaries directory}"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
pins="$here/../../internal/launcher/frp-pins.txt"
mkdir -p "$S14_BIN"
DL="$S14_BIN/.dl"
mkdir -p "$DL"

FRP_VERSION=0.71.1
FRP_SHA=1d9875210972a5d1a62b2dc4c48df45ab0af05d3ca1b5e494a014428259df803
TOXI_VERSION=2.13.0
TOXI_SHA=08fd7b3d677bd11358d16a5ce8fd9e1a08f69aa24984bc16781c84200d6b3b4d
# The toxiproxy-server executable inside that tarball. The release publishes no sum for it; this
# value was computed with sha256sum on the file S1c's fetch.sh extracted after the tarball's own
# sha256 matched TOXI_SHA.
TOXI_SERVER_SHA=bf529e8bb9a05cd293f45a3f1d0aa66ff9469680dcdca7e0ebeae21f1b83de89

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

if [ -n "${S14_FRP_DIR:-}" ]; then
  cp "$S14_FRP_DIR/frps" "$S14_FRP_DIR/frpc" "$S14_BIN/"
else
  tgz="$DL/frp_${FRP_VERSION}_linux_amd64.tar.gz"
  fetch "https://github.com/fatedier/frp/releases/download/v${FRP_VERSION}/frp_${FRP_VERSION}_linux_amd64.tar.gz" "$tgz" "$FRP_SHA"
  x="$(mktemp -d "$DL/frp.XXXXXX")"
  tar -xzf "$tgz" -C "$x"
  cp "$x/frp_${FRP_VERSION}_linux_amd64/frps" "$x/frp_${FRP_VERSION}_linux_amd64/frpc" "$S14_BIN/"
  rm -rf "$x"
fi
verify "$S14_BIN/frps" "$FRPS_SHA"
verify "$S14_BIN/frpc" "$FRPC_SHA"

if [ -n "${S14_TOXI:-}" ]; then
  cp "$S14_TOXI" "$S14_BIN/toxiproxy-server"
else
  tgz="$DL/toxiproxy_${TOXI_VERSION}_linux_amd64.tar.gz"
  fetch "https://github.com/Shopify/toxiproxy/releases/download/v${TOXI_VERSION}/toxiproxy_${TOXI_VERSION}_linux_amd64.tar.gz" "$tgz" "$TOXI_SHA"
  x="$(mktemp -d "$DL/toxi.XXXXXX")"
  tar -xzf "$tgz" -C "$x"
  cp "$x/toxiproxy-server" "$S14_BIN/"
  rm -rf "$x"
fi
verify "$S14_BIN/toxiproxy-server" "$TOXI_SERVER_SHA"

cd "$here"
CGO_ENABLED=0 go build -trimpath -o "$S14_BIN/s14" ./cmd/s14
CGO_ENABLED=0 go build -trimpath -o "$S14_BIN/s14bridge" ./cmd/s14bridge
CGO_ENABLED=0 go test -c -trimpath -o "$S14_BIN/s14node.test" ./node
"$S14_BIN/frps" --version | sed 's/^/frps /'
"$S14_BIN/toxiproxy-server" -version
go version -m "$S14_BIN/s14" | grep -E 'fatedier/yamux|^\s+go\s' | sed 's/^\s*/s14 /'
echo "ready: $S14_BIN"
