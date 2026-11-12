#!/usr/bin/env bash
# Unpacks a release archive (hack/release-archive.sh) into the names the rehearsal's prebuilt mode takes
# (issue #146), so that the rehearsal runs the draft's own bytes (HBEP-15, Releases: how it's made, step
# 3). hack/rehearsal.sh's archive input (REHEARSAL_ARCHIVE) calls it, and then sets HPCBURST_BIN_DIR and
# HPCBURST_FRP_DIR itself; it refuses REHEARSAL_PREBUILT=1, HPCBURST_BIN_DIR and HPCBURST_FRP_DIR beside
# the archive.
#
# usage: hack/release-unpack.sh ARCHIVE OUT FRP_DIR
#
# OUT must not hold archive/ yet. It receives
#   OUT/archive/            the archive as it unpacks: bin/, agent-builds/, config/, profiles/.
#                           OUT/archive/bin is the directory `hpcburst up --bin-dir` and the rehearsal's
#                           HPCBURST_BIN_DIR read: it holds k3s-1.36.5/k3s and frp-0.71.1/frps where they
#                           are looked for
#   OUT/hpcburst, OUT/hpcburst-controller, OUT/hpcburst-front   the host binaries, which the prebuilt mode
#                           takes by these names (linked to the archive's, or copied)
#   OUT/hb-agent            the x86_64 agent build, under the name the prebuilt mode takes
# and FRP_DIR (created if needed; the directory HPCBURST_FRP_DIR names) receives frps, from
# bin/frp-0.71.1/frps, and frpc, from agent-builds/frpc-x86_64, both mode 755. The archive carries frps
# in bin/ and each architecture's frpc in agent-builds/, and the tests that start them read both from
# one directory (internal/testenv, FrpBinaries).
#
# It refuses an archive that has an entry outside OUT (an absolute name, a .. component), a link or a
# device, or that lacks any of the entries above. When a SHA256SUMS lies beside the archive, the archive
# must match its line, and the unpacked files must be exactly the files it lists, each with its sum.
set -euo pipefail

die() { echo "release-unpack.sh: $*" >&2; exit 1; }

[ $# -eq 3 ] && [ -n "$1" ] && [ -n "$2" ] && [ -n "$3" ] || die "usage: hack/release-unpack.sh ARCHIVE OUT FRP_DIR"
[ -f "$1" ] || die "the archive $1 is not a file"
archive=$(cd "$(dirname "$1")" && pwd)/$(basename "$1")
mkdir -p "$2" "$3"
out=$(cd "$2" && pwd)
frp=$(cd "$3" && pwd)
[ ! -e "$out/archive" ] || die "$out/archive is already there; unpack into a fresh OUT"
# The checks' temporary files are this run's own, and the only things it removes.
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

# What the archive must hold, and where each file goes.
required=(
  bin/hpcburst bin/hpcburst-controller bin/hpcburst-front
  bin/k3s-1.36.5/k3s bin/frp-0.71.1/frps
  agent-builds/hb-agent-x86_64 agent-builds/frpc-x86_64
)

# The entries first, before anything is written.
listing=$(tar -tvzf "$archive") || die "$archive is not a gzip tar archive"
bad=$(awk '$1 !~ /^[-d]/ { print "not a file or directory: " $0 }' <<< "$listing")
[ -z "$bad" ] || die "the archive has entries other than files and directories: $bad"
names=$(tar -tzf "$archive")
while IFS= read -r n; do
  case $n in
    /* | ../* | */../* | */.. | ..) die "the archive has an entry outside the unpacked tree: $n" ;;
  esac
done <<< "$names"
for f in "${required[@]}"; do
  grep -qxF "$f" <<< "$names" || die "the archive has no $f"
done

# The checksums beside it, when there are any: the archive's own line, before it is unpacked.
sums=$(dirname "$archive")/SHA256SUMS
if [ -f "$sums" ]; then
  line=$(grep -F "  $(basename "$archive")" "$sums" || true)
  [ "$(wc -l <<< "$line")" -eq 1 ] && [ -n "$line" ] || die "$sums has no single line for $(basename "$archive")"
  ( cd "$(dirname "$archive")" && echo "$line" | sha256sum -c --status - ) || die "$(basename "$archive") does not match its line in $sums"
  echo "ok       $(basename "$archive") matches $sums"
fi

mkdir "$out/archive"
tar -xzf "$archive" -C "$out/archive" --no-same-owner
for f in "${required[@]}"; do
  [ -f "$out/archive/$f" ] && [ ! -L "$out/archive/$f" ] || die "$f did not unpack as a regular file"
done

if [ -f "$sums" ]; then
  # Every file the archive unpacked is a file SHA256SUMS lists, and the other way round, each with its sum.
  tail -n +2 "$sums" > "$tmp/sums"
  ( cd "$out/archive" && find . -type f | sed 's|^\./||' | LC_ALL=C sort ) > "$tmp/found"
  awk '{ print $2 }' "$tmp/sums" | LC_ALL=C sort > "$tmp/listed"
  diff "$tmp/listed" "$tmp/found" > "$tmp/diff" || die "the unpacked files are not the files $sums lists (< listed, > unpacked):
$(cat "$tmp/diff")"
  ( cd "$out/archive" && sha256sum -c --quiet "$tmp/sums" ) || die "an unpacked file does not match $sums"
  echo "ok       every unpacked file matches $sums"
fi

# The names the prebuilt mode takes: a link where the file system allows it, a copy where not.
place() { # SRC DST
  rm -f "$2"
  ln "$1" "$2" 2> /dev/null || cp "$1" "$2"
  chmod 755 "$2"
}
for b in hpcburst hpcburst-controller hpcburst-front; do
  place "$out/archive/bin/$b" "$out/$b"
done
place "$out/archive/agent-builds/hb-agent-x86_64" "$out/hb-agent"
place "$out/archive/bin/frp-0.71.1/frps" "$frp/frps"
place "$out/archive/agent-builds/frpc-x86_64" "$frp/frpc"
echo "unpacked $archive"
echo "         binaries and hb-agent in $out, k3s and frps in $out/archive/bin, frps and frpc in $frp"
