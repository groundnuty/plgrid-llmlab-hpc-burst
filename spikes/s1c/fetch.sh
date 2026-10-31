#!/usr/bin/env bash
# Spike S1c: put the pinned third-party binaries into $S1C_SCRATCH/bin and build s1cbench there.
#
#   S1C_SCRATCH=<dir> bash fetch.sh
#
# Nothing is extracted before its tarball matches the sha256 written below; a mismatch deletes the
# download and exits 1. FRP_TARBALL may name an already downloaded frp tarball (it is verified the
# same way). The engine double (llm-d-inference-sim, built by spike S2 with its loopback patch) and
# Envoy are not fetched here: point S1C_SIM and S1C_ENVOY at them; Envoy is checked against the
# release's checksums.txt.asc value below.
set -euo pipefail

: "${S1C_SCRATCH:?set S1C_SCRATCH to the scratch directory}"
BIN="$S1C_SCRATCH/bin"
DL="$S1C_SCRATCH/dl"
mkdir -p "$BIN" "$DL"

# frp v0.71.1 linux_amd64, the version and sha256 spike S1 pinned (spikes/s1-frp/fetch-frp.sh on
# branch spike/s1-frp-dial-home; source: the release's frp_sha256_checksums.txt and GitHub's digest).
FRP_VERSION=0.71.1
FRP_SHA=1d9875210972a5d1a62b2dc4c48df45ab0af05d3ca1b5e494a014428259df803
# toxiproxy v2.13.0 linux_amd64 (released 2026-10-14). Source: the release's checksums.txt (itself
# sha256 dc0558603352f0f2f812495ac891852bbfc7e5acdd824fd983d0c189c419541b, equal to GitHub's digest
# of that asset), cross-checked against GitHub's digest of the tarball. Both agree.
TOXI_VERSION=2.13.0
TOXI_SHA=08fd7b3d677bd11358d16a5ce8fd9e1a08f69aa24984bc16781c84200d6b3b4d
# Envoy 1.33.2 linux x86_64 release binary, from the release's checksums.txt.asc (the PGP signature
# was not verified). The same binary spike S2 used.
ENVOY_SHA=bd2ff87c5efc95fd7880d710915f573c1aab4d9c55d8da886dcd91df43fad741

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
  if [ -f "$2" ] && verify "$2" "$3" >/dev/null; then echo "have $2 (verified)"; return; fi
  curl -fsSL --retry 3 --connect-timeout 15 -o "$2.part" "$1"
  if ! verify "$2.part" "$3"; then rm -f "$2.part"; echo "fetch: deleted the bad download" >&2; return 1; fi
  mv "$2.part" "$2"
}

frp_tgz="$DL/frp_${FRP_VERSION}_linux_amd64.tar.gz"
if [ -n "${FRP_TARBALL:-}" ]; then verify "$FRP_TARBALL" "$FRP_SHA"; cp "$FRP_TARBALL" "$frp_tgz"; fi
fetch "https://github.com/fatedier/frp/releases/download/v${FRP_VERSION}/frp_${FRP_VERSION}_linux_amd64.tar.gz" "$frp_tgz" "$FRP_SHA"
x="$(mktemp -d "$DL/frp.XXXXXX")"
tar -xzf "$frp_tgz" -C "$x"
cp "$x/frp_${FRP_VERSION}_linux_amd64/frps" "$x/frp_${FRP_VERSION}_linux_amd64/frpc" "$BIN/"
rm -rf "$x"

toxi_tgz="$DL/toxiproxy_${TOXI_VERSION}_linux_amd64.tar.gz"
fetch "https://github.com/Shopify/toxiproxy/releases/download/v${TOXI_VERSION}/toxiproxy_${TOXI_VERSION}_linux_amd64.tar.gz" "$toxi_tgz" "$TOXI_SHA"
x="$(mktemp -d "$DL/toxi.XXXXXX")"
tar -xzf "$toxi_tgz" -C "$x"
cp "$x/toxiproxy-server" "$BIN/"
rm -rf "$x"

if [ -n "${S1C_ENVOY:-}" ]; then verify "$S1C_ENVOY" "$ENVOY_SHA"; fi
# The engine double is spike S2's build: llm-d-inference-sim v1.0.0, commit 7bda03f, with S2's
# loopback patch (so Go records the tree as modified). Go embeds the module and VCS revision in the
# binary; check them and print its sha256 (a local build, so there is no published digest).
if [ -n "${S1C_SIM:-}" ] && [ -x "$S1C_SIM" ]; then
  echo "engine sha256 $(sha256sum "$S1C_SIM" | cut -d' ' -f1)"
  go version -m "$S1C_SIM" | grep -q 'vcs.revision=7bda03f9fef85139617cb4f724dccafe1cf8cbbf' \
    || { echo "fetch: S1C_SIM is not built from llm-d-inference-sim 7bda03f" >&2; exit 1; }
  go version -m "$S1C_SIM" | grep -q 'vcs.modified=true' \
    || echo "fetch: note: S1C_SIM has no local changes recorded; spike S2's loopback patch may be missing" >&2
else
  echo "fetch: note: S1C_SIM is not set to an executable (spike S2's patched llm-d-inference-sim)" >&2
fi

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
(cd "$here" && CGO_ENABLED=0 nice -n 10 go build -trimpath -o "$BIN/s1cbench" ./cmd/s1cbench)
"$BIN/frps" --version | sed 's/^/frps /'
"$BIN/toxiproxy-server" -version
echo "ready: $BIN"
