#!/usr/bin/env bash
# Download the pinned frp release and install its frps and frpc into a directory.
#
# usage: hack/fetch-frp.sh DIR
#
# frp v0.71.1 linux_amd64 (https://github.com/fatedier/frp, Apache-2.0) carries the tunnel of
# HBEP-6: frpc in the agent, frps in the launcher. The tests that start them read the directory
# from HPCBURST_FRP_DIR (internal/testenv, FrpBinaries). The tarball's sha256 is the one spike S1c
# pinned (spikes/s1c/fetch.sh; source: the release's frp_sha256_checksums.txt and GitHub's digest
# of the asset). Nothing is extracted before the tarball matches it. A download with another hash
# is deleted and the script exits 1, leaving nothing in DIR (a DIR it had to create is removed
# again). The two binaries are checked against their own pins too, which are the sha256 of
# frps and frpc as extracted from that verified tarball: that is what makes a second run a no-op.
#
# Both files are staged inside DIR under a name of their own and renamed into place, so two runs
# that start at once never write the same file and a reader never sees a half-written binary.
#
# FRP_TARBALL names an already downloaded tarball to use instead of the download. It is copied
# and verified exactly like a download, and the original is never touched. FRP_RELEASE_BASE
# overrides the download site, for mirrors.
set -euo pipefail

FRP_VERSION=0.71.1
FRP_TARBALL_SHA=1d9875210972a5d1a62b2dc4c48df45ab0af05d3ca1b5e494a014428259df803
FRPS_SHA=2322762939e66e66850bbc9103b6d73fec39e83bd8ad6c71ebeeb120d56d6ab3
FRPC_SHA=35c6054d4549bbcf2f02875543895fd9e44c2aae2925f61cd8ff5a693703c28b

base=${FRP_RELEASE_BASE:-https://github.com/fatedier/frp/releases/download}
name=frp_${FRP_VERSION}_linux_amd64

die() { echo "fetch-frp.sh: $*" >&2; exit 1; }

[ $# -eq 1 ] && [ -n "$1" ] || die "usage: hack/fetch-frp.sh DIR"
dest=$1

case "$(uname -s) $(uname -m)" in
  "Linux x86_64" | "Linux amd64") ;;
  *) die "only linux_amd64 is pinned, and this machine is $(uname -s) $(uname -m)" ;;
esac

# sha_ok FILE SHA: FILE exists and has that sha256.
sha_ok() { [ -f "$1" ] && echo "$2  $1" | sha256sum -c --status -; }

if sha_ok "$dest/frps" "$FRPS_SHA" && sha_ok "$dest/frpc" "$FRPC_SHA" && [ -x "$dest/frps" ] && [ -x "$dest/frpc" ]; then
  echo "ok       frp $FRP_VERSION ($dest)"
  exit 0
fi

created=0
[ -d "$dest" ] || created=1
mkdir -p "$dest"
stage=$(mktemp -d "$dest/.frp.XXXXXX")
# The stage is this run's own directory. On any exit it goes, with the download inside it, and a
# DIR this run created goes too when it is still empty (rmdir refuses otherwise).
cleanup() {
  rm -rf "$stage"
  [ "$created" -eq 0 ] || rmdir "$dest" 2>/dev/null || true
}
trap cleanup EXIT

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
mv -f "$stage/frps" "$dest/frps"
mv -f "$stage/frpc" "$dest/frpc"
echo "verified frp $FRP_VERSION ($dest/frps, $dest/frpc)"
