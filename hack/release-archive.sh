#!/usr/bin/env bash
# Builds the release archive of HBEP-15 (Releases) from a checkout, and its SHA256SUMS. The release
# workflow calls it; so can you, to see what a release would carry.
#
# usage: hack/release-archive.sh [--tag TAG] [--seed DIR] [--no-fetch] OUT
#
# OUT is a directory outside the checkout (a build inside it would make the tree dirty, and every
# binary would report +dirty). It receives
#   hpcburst-VERSION-linux-amd64.tar.gz   the archive
#   SHA256SUMS                            the archive's line, then one line for every file in it
# VERSION is what the binaries report: TAG for a tag, or the pseudo-version Go stamps for a commit that
# no tag names (the workflow's workflow_dispatch run, which proves the archive and publishes nothing).
#
# The archive (no wrapper directory; unpack it into an empty one):
#   bin/hpcburst, bin/hpcburst-controller, bin/hpcburst-front     built here with the pinned Go (go.mod's
#                                           go line, as hack/build-agents.sh pins it), CGO_ENABLED=0,
#                                           -trimpath and VCS stamping on
#   bin/k3s-1.36.5/k3s                      the pinned k3s, where `hpcburst up --bin-dir bin` looks
#   bin/frp-0.71.1/frps                     the pinned frps, likewise (its frpc is agent-builds/frpc-x86_64)
#   agent-builds/hb-agent-x86_64, hb-agent-aarch64     hack/build-agents.sh --version VERSION, so that
#                                           each agent reports the version its host binaries do
#   agent-builds/frpc-x86_64, frpc-aarch64             hack/fetch-frp.sh --agent-builds
#   config/crd/, profiles/                  from the commit, as git archive gives them
#
# What it refuses (each before an archive is written):
#   - a dirty checkout, one whose .git is a file (a worktree: Go 1.26 stamps nothing there), or OUT inside
#     the checkout;
#   - a Go other than the pinned one;
#   - with --tag, a tag that is missing, or that does not name this commit, or whose notes file
#     docs/releases/TAG.md is missing or empty;
#   - a k3s, frps or frpc whose sha256 is not the one internal/launcher/k3s-pins.txt or frp-pins.txt
#     pins. The fetch scripts check their downloads too; this is the check at the point of packing,
#     on the very files that go in, whatever put them there (a seed, a cache, a script that changed);
#   - a host binary whose `go version -m` does not show the tag exactly, without +dirty, with
#     vcs.modified=false. Without --tag it must show a pseudo-version of this commit. All three
#     binaries must show the same version.
#   - an x86_64 agent whose `-version` does not name that version (on a linux x86_64 machine, which can
#     run it; elsewhere the script says it was not run). The version reaches the agent through the
#     linker's -X, which ignores a variable that is not there, so the agent itself is asked.
#
# --tag TAG     the version the binaries must report. Without it the run is a dry one: the same archive,
#               with the pseudo-version in its name.
# --seed DIR    copy DIR's bin/k3s-1.36.5, bin/frp-0.71.1 and agent-builds/frpc-* into the stage first
#               (a restored cache, or the files a test provides). They are checked like any other.
# --no-fetch    download nothing: the pinned k3s, frps and frpc must come from --seed. The host binaries
#               and the agents are still built.
#
# The archive is the same bytes on any machine, for the same inputs: sorted entries, the commit's time,
# root as owner, and gzip without a name or time. SHA256SUMS lists the archive first, then each file as
# `sha256sum` prints it, by its name in the archive, so that in a directory holding the archive and its
# unpacked tree, `sha256sum -c SHA256SUMS` checks both.
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
# shellcheck source=hack/cpu-arch.sh
. "$root/hack/cpu-arch.sh"

# What the archive carries. A bump is a change here, in the pins files and in HBEP-15 together.
K3S_DIR_VERSION=1.36.5           # the directory name hack/fetch-k3s.sh gives it
K3S_PIN_VERSION=v1.36.5+k3s1     # its line in internal/launcher/k3s-pins.txt
FRP_VERSION=0.71.1
HOST_BINARIES=(hpcburst hpcburst-controller hpcburst-front)

die() { echo "release-archive.sh: $*" >&2; exit 1; }

tag= seed= nofetch=0
while [ $# -gt 0 ]; do
  case $1 in
    --tag) [ $# -ge 2 ] || die "--tag needs a value"; tag=$2; shift 2 ;;
    --seed) [ $# -ge 2 ] || die "--seed needs a directory"; seed=$2; shift 2 ;;
    --no-fetch) nofetch=1; shift ;;
    -*) die "unknown option $1 (usage: hack/release-archive.sh [--tag TAG] [--seed DIR] [--no-fetch] OUT)" ;;
    *) break ;;
  esac
done
[ $# -eq 1 ] && [ -n "$1" ] || die "usage: hack/release-archive.sh [--tag TAG] [--seed DIR] [--no-fetch] OUT"
# OUT is resolved, not created, until the checkout has been looked at.
out=$(realpath -m -- "$1")
[ -z "$seed" ] || { [ -d "$seed" ] && seed=$(cd "$seed" && pwd); } || die "--seed $seed is not a directory"

command -v go > /dev/null || die "go is not on PATH"
command -v git > /dev/null || die "git is not on PATH"
cd "$root"

# ---- refuse what would make the archive wrong, before building anything ---------------------------
case $out/ in
  "$root"/*) die "OUT $out is inside the checkout $root: a file there would make the tree dirty, and every binary would report +dirty" ;;
esac
# Go 1.26 stamps no version, and no VCS information at all, in a git worktree or a submodule, where .git is
# a file: the binaries would say (devel). A clone, as actions/checkout makes, has a .git directory.
[ -d "$root/.git" ] || die "$root/.git is not a directory (a git worktree or a submodule?): Go 1.26 stamps no version in one, so its binaries would report (devel); build in a clone"
[ -z "$(git status --porcelain)" ] || die "the checkout is not clean, so its binaries would report +dirty:
$(git status --porcelain | head -5)"
head=$(git rev-parse HEAD)
if [ -n "$tag" ]; then
  tagged=$(git rev-parse --verify --quiet "refs/tags/$tag^{commit}") || die "the tag $tag does not exist in this checkout"
  [ "$tagged" = "$head" ] || die "the checkout is $head, and the tag $tag names $tagged"
  [ -f "docs/releases/$tag.md" ] && [ -s "docs/releases/$tag.md" ] || die "docs/releases/$tag.md is missing or empty"
fi
if [ "$nofetch" = 0 ]; then
  case "$(uname -s) $(uname -m)" in
    "Linux x86_64" | "Linux amd64") ;;
    *) die "the archive is linux amd64 and its pinned k3s and frps are fetched for the machine: this is $(uname -s) $(uname -m); use --seed and --no-fetch" ;;
  esac
fi

# The pinned toolchain, as hack/build-agents.sh pins it: go.mod's go line, exactly. A Go of another
# version is not used; the go command downloads the pinned one, and when it cannot, the script stops.
gover=$(awk '$1 == "go" { v = $2; sub(/\/\/.*/, "", v); print v; exit }' go.mod)
[[ $gover =~ ^[0-9]+\.[0-9]+(\.[0-9]+|(alpha|beta|rc)[0-9]+)?$ ]] || die "go.mod has no go line of the form 1.26.0, 1.26 or 1.27rc1 ('$gover')"
[[ $gover =~ ^[0-9]+\.[0-9]+$ ]] && gover=$gover.0
export GOTOOLCHAIN=go$gover
goerr=$(mktemp) work=
# The temporary files are this run's own, and the only things it removes. The stage holds the archive's
# tree, and the temporary files of the fetches.
cleanup() { rm -f "$goerr"; [ -z "$work" ] || rm -rf "$work"; }
trap cleanup EXIT
goversion=$(go version 2> "$goerr") && [[ $goversion == "go version go$gover "* ]] ||
  die "cannot run Go $gover, the toolchain go.mod pins (GOTOOLCHAIN=$GOTOOLCHAIN): ${goversion//$'\n'/ } $(tr '\n' ' ' < "$goerr")"
cat "$goerr" >&2
# What the go command's configuration file may hold for it, read before a build runs in its own
# environment.
for v in GOPATH GOMODCACHE GOCACHE GOPROXY; do
  val=$(go env "$v") || die "go env $v failed"
  export "$v=$val"
done

mkdir -p "$out"
work=$(mktemp -d "$out/.release.XXXXXX")
stage=$work/stage
mkdir -p "$stage/bin" "$stage/agent-builds"

# ---- the pinned k3s and frp ------------------------------------------------------------------------
k3s_rel=bin/k3s-$K3S_DIR_VERSION/k3s
frps_rel=bin/frp-$FRP_VERSION/frps
if [ -n "$seed" ]; then
  for d in "bin/k3s-$K3S_DIR_VERSION" "bin/frp-$FRP_VERSION"; do
    [ ! -d "$seed/$d" ] || { mkdir -p "$stage/$(dirname "$d")"; cp -a "$seed/$d" "$stage/$d"; }
  done
  for f in "$seed"/agent-builds/frpc-*; do
    [ ! -f "$f" ] || cp -a "$f" "$stage/agent-builds/"
  done
fi
if [ "$nofetch" = 0 ]; then
  bash hack/fetch-k3s.sh "$stage/bin" "$K3S_DIR_VERSION"
  # frps and frpc land together (see below for the frpc).
  mkdir -p "$stage/bin/frp-$FRP_VERSION"
  bash hack/fetch-frp.sh "$stage/bin/frp-$FRP_VERSION"
fi
# The archive carries frps here, and the amd64 frpc in agent-builds: whatever put an frpc beside this
# frps, a fetch or a seed (a cache holds both), it does not go in.
rm -f "$stage/bin/frp-$FRP_VERSION/frpc"
if [ "$nofetch" = 0 ]; then
  bash hack/fetch-frp.sh --agent-builds "$stage/agent-builds"
fi

# ---- every pinned file against its pins file, here, on the files that go in, before anything is compiled
sha_of() { sha256sum "$1" | cut -d' ' -f1; }
pinned() { # NAME FILE SHA: FILE has the pinned sha256 SHA
  [[ $3 =~ ^[0-9a-f]{64}$ ]] || die "the pins file has no sha256 for $1"
  [ -f "$2" ] && [ ! -L "$2" ] || die "$1 is missing from the archive: $2"
  got=$(sha_of "$2")
  [ "$got" = "$3" ] || die "$1 is not the pinned file: sha256 $got, and the pins file says $3"
  echo "pinned   $1 sha256 $3"
}
k3s_sha=$(awk -v v="$K3S_PIN_VERSION" '$1 !~ /^#/ && NF && $1 == v && $2 == "amd64" && $3 == "k3s" { print $4 }' internal/launcher/k3s-pins.txt)
pinned "k3s $K3S_PIN_VERSION" "$stage/$k3s_rel" "$k3s_sha"
frps_sha=$(awk -v v="$FRP_VERSION" '$1 !~ /^#/ && NF && $1 == v && $2 == "amd64" { print $5 }' internal/launcher/frp-pins.txt)
pinned "frps $FRP_VERSION" "$stage/$frps_rel" "$frps_sha"
while read -r arch csha; do
  cpu=$(cpu_arch "$arch") || die "frp-pins.txt pins frp $FRP_VERSION for $arch, which has no cpuArch name"
  pinned "frpc $FRP_VERSION $cpu" "$stage/agent-builds/frpc-$cpu" "$csha"
done < <(awk -v v="$FRP_VERSION" '$1 !~ /^#/ && NF && $1 == v { print $2, $6 }' internal/launcher/frp-pins.txt)

# ---- the host binaries, with the build information the release checks -----------------------------
for b in "${HOST_BINARIES[@]}"; do
  CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GOAMD64=v1 GOWORK=off GOFLAGS=-mod=readonly \
    go build -trimpath -buildvcs=true -o "$stage/bin/$b" "./cmd/$b" || die "building cmd/$b failed"
  chmod 755 "$stage/bin/$b"
done

version=
for b in "${HOST_BINARIES[@]}"; do
  info=$(go version -m "$stage/bin/$b") || die "go version -m $b failed"
  got=$(awk '$1 == "mod" { print $3; exit }' <<< "$info")
  modified=$(awk '$1 == "build" && $2 ~ /^vcs\.modified=/ { sub(/^vcs\.modified=/, "", $2); print $2; exit }' <<< "$info")
  [ -n "$got" ] || die "$b has no main module version in its build information"
  [ "$modified" = false ] || die "$b reports vcs.modified=${modified:-<missing>} ($got): the tree was not clean, or Go stamped no VCS information"
  [[ $got != *+dirty ]] || die "$b reports $got"
  if [ -n "$tag" ]; then
    [ "$got" = "$tag" ] || die "$b reports $got, and the tag is $tag (Go stamps a tag only on the commit it names, with a major version the module path allows)"
  else
    [[ $got =~ [-.][0-9]{14}-${head:0:12}$ ]] || die "$b reports $got, which is not a pseudo-version of this commit ${head:0:12}"
  fi
  [ -z "$version" ] || [ "$version" = "$got" ] || die "the host binaries report different versions: $version and $got"
  version=$got
done
echo "versions $version on ${HOST_BINARIES[*]}"

# ---- the agents -----------------------------------------------------------------------------------------
# The version the host binaries report: the tag, or on a dry run the pseudo-version, so a dry run builds
# the agents the way a tag does (issue #469).
bash hack/build-agents.sh --version "$version" "$stage/agent-builds"
# The linker's -X ignores a variable that does not exist, so the agent is asked what it reports.
case "$(uname -s) $(uname -m)" in
  "Linux x86_64" | "Linux amd64")
    said=$("$stage/agent-builds/hb-agent-x86_64" -version 2>&1) || die "hb-agent-x86_64 -version failed: $said"
    [[ $said == "hb-agent $version, protocol "* ]] || die "hb-agent-x86_64 reports '$said', and the host binaries report $version"
    echo "agent    hb-agent-x86_64 reports: $said"
    ;;
  *) echo "agent    hb-agent-x86_64 -version was not run: this is $(uname -s) $(uname -m)" ;;
esac

# ---- the commit's own files --------------------------------------------------------------------------
git archive --format=tar HEAD config/crd profiles | tar -x -C "$stage"

# ---- the layout: exactly the entries the archive is specified to have ------------------------------
expected=()
for b in "${HOST_BINARIES[@]}"; do expected+=("bin/$b"); done
expected+=("$k3s_rel" "$frps_rel")
for cpu in $(cpu_archs); do expected+=("agent-builds/hb-agent-$cpu" "agent-builds/frpc-$cpu"); done
executables=("${expected[@]}")
git ls-tree -r --name-only HEAD config/crd profiles > "$work/committed"
[ -n "$(grep '^config/crd/' "$work/committed" || true)" ] || die "the commit has no config/crd/"
[ -n "$(grep '^profiles/' "$work/committed" || true)" ] || die "the commit has no profiles/"
mapfile -t committed < "$work/committed"
expected+=("${committed[@]}")
( cd "$stage" && find . -type f | sed 's|^\./||' | LC_ALL=C sort ) > "$work/found"
printf '%s\n' "${expected[@]}" | LC_ALL=C sort > "$work/want"
if ! diff "$work/want" "$work/found" > "$work/diff"; then
  die "the stage is not the specified layout (< wanted, > found):
$(cat "$work/diff")"
fi
find "$stage" -type d -exec chmod 755 {} +
while read -r f; do
  chmod 644 "$stage/$f"
done < "$work/found"
for f in "${executables[@]}"; do chmod 755 "$stage/$f"; done

# ---- the archive and its checksums ---------------------------------------------------------------------
name=hpcburst-$version-linux-amd64.tar.gz
mtime=$(git log -1 --format=%ct HEAD)
LC_ALL=C tar --sort=name --mtime="@$mtime" --owner=0 --group=0 --numeric-owner -C "$stage" -cf - bin agent-builds config profiles |
  gzip -n -9 > "$work/$name"
{
  ( cd "$work" && sha256sum -- "$name" )
  ( cd "$stage" && while IFS= read -r f; do sha256sum -- "$f"; done < "$work/found" )
} > "$work/SHA256SUMS"
mv -f "$work/$name" "$out/$name"
mv -f "$work/SHA256SUMS" "$out/SHA256SUMS"
echo "archive  $out/$name"
echo "sha256   $(sha_of "$out/$name")"
echo "files    $(wc -l < "$out/SHA256SUMS" | tr -d ' ') lines in SHA256SUMS (the archive and $(wc -l < "$work/found" | tr -d ' ') files)"
