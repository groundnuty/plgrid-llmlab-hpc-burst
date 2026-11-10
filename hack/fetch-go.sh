#!/usr/bin/env bash
# Install the pinned Go toolchain that CI runs, from a pinned tarball, and check it.
#
# usage: hack/fetch-go.sh --version
#        hack/fetch-go.sh DOWNLOAD_DIR INSTALL_DIR
#        hack/fetch-go.sh --check INSTALL_DIR
#
# The release is the one go.mod's go line names, which must be three numbers (go 1.26.0; a range or a
# patchless line is refused, since a pin is by exact version). hack/go-pins.txt holds its tarball's
# sha256 (from go.dev's download listing) and the sha256 of go/bin/go inside it. A go.mod that names a
# release with no line there stops the run before anything is fetched, so moving go.mod means adding
# the pin in the same change. This replaces actions/setup-go in CI (issue #294): that action fetches
# the toolchain from GitHub's release assets and checks no digest of it. GOTOOLCHAIN=local, which the
# workflow sets, stays what stops the go command from fetching another toolchain on its own.
#
# --version prints go.mod's version (1.26.0) and does nothing else. CI reads it for the cache key.
#
# DOWNLOAD_DIR INSTALL_DIR makes INSTALL_DIR/go the pinned toolchain, in two steps.
#   1. DOWNLOAD_DIR/<tarball> must have the pinned sha256. When it does, nothing is downloaded. When
#      it is missing, or it has another digest, it is deleted and downloaded again from
#      GO_RELEASE_BASE/<tarball> (default https://go.dev/dl), and a download that does not match the
#      pin is deleted and fails the run, with nothing extracted. DOWNLOAD_DIR is what CI restores
#      from, and saves to, its cache (issue #294): the cache decides only whether a download is
#      needed, never what runs. The tarball is staged inside DOWNLOAD_DIR under a name of its own and
#      renamed into place, so a reader never sees a half-written file.
#   2. The tarball is extracted into a stage inside INSTALL_DIR, go/bin/go and go/VERSION are checked
#      (the binary against its pin, VERSION against the release), and the stage replaces
#      INSTALL_DIR/go. A tree already there is never trusted or reused: it is removed, so every file
#      that runs comes from the verified tarball of this run, not only the binary the pin names. The
#      installed go is then run once, and must report this release for linux/amd64.
#
# --check INSTALL_DIR is the step after the workflow has put INSTALL_DIR/go/bin on PATH and exported
# GOROOT=INSTALL_DIR/go: the go that PATH finds must be INSTALL_DIR/go/bin/go (the runner image has
# other Gos), its sha256 must be the pin, it must report this release, and the root that `go env GOROOT`
# gives, in the environment as the job has it, must be INSTALL_DIR/go. That last check is what an
# inherited GOROOT would fail: the go command would then build with the compiler and standard library of
# another tree, whatever binary runs it. The step is what shows that the job runs the pinned toolchain,
# and the workflow runs every later step that uses go only if it succeeded.
#
# For tests: GO_RELEASE_BASE can be a file:// mirror (a dead mirror is a file:// path that does not
# exist), GO_PINS_FILE names another pins file, and GO_MOD_FILE another go.mod.
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
gomod=${GO_MOD_FILE:-$root/go.mod}
pins=${GO_PINS_FILE:-$root/hack/go-pins.txt}
base=${GO_RELEASE_BASE:-https://go.dev/dl}

die() { echo "fetch-go.sh: $*" >&2; exit 1; }

# sha_ok FILE SHA: FILE exists and has that sha256.
sha_ok() { [ -f "$1" ] && echo "$2  $1" | sha256sum -c --status -; }

# The pinned release: go.mod's go line, the version after "go" up to a comment, as hack/build-agents.sh
# reads it. Exactly three numbers.
[ -f "$gomod" ] || die "$gomod is not a file"
gover=$(awk '$1 == "go" { v = $2; sub(/\/\/.*/, "", v); print v; exit }' "$gomod")
[[ $gover =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] ||
  die "$gomod has no go line of the form 1.26.0 ('$gover'); a pin is by exact version, so nothing was fetched"

if [ "${1:-}" = "--version" ]; then
  [ $# -eq 1 ] || die "usage: hack/fetch-go.sh --version"
  echo "$gover"
  exit 0
fi

# The pin's line: tarball name, tarball sha256, go binary sha256.
[ -f "$pins" ] || die "the pins file $pins is not a file"
read -r tarball tsha bsha < <(awk -v v="$gover" '$1 !~ /^#/ && NF && $1 == v && $2 == "amd64" { print $3, $4, $5 }' "$pins") || true
[ -n "${tarball:-}" ] || die "go $gover is not pinned for amd64 in $pins: add its line there from https://go.dev/dl/; nothing was fetched"
[[ $tarball =~ ^[A-Za-z0-9._-]+\.tar\.gz$ ]] || die "$pins: the tarball name of the go $gover line is not a plain file name"
[[ ${tsha:-} =~ ^[0-9a-f]{64}$ && ${bsha:-} =~ ^[0-9a-f]{64}$ ]] ||
  die "$pins: the sums of the go $gover line are not sha256 of 64 lowercase hex characters"

case "$(uname -s) $(uname -m)" in
  "Linux x86_64" | "Linux amd64") ;;
  *) die "only linux_amd64 is pinned, and this machine is $(uname -s) $(uname -m)" ;;
esac

# installed_go_reports GO: GO runs and says it is this release.
installed_go_reports() {
  local got
  got=$(env -u GOROOT GOTOOLCHAIN=local GOFLAGS= GOENV=off "$1" version 2>&1) || die "$1 version failed: $got"
  [ "$got" = "go version go$gover linux/amd64" ] || die "$1 reports '$got', want 'go version go$gover linux/amd64'"
}

if [ "${1:-}" = "--check" ]; then
  [ $# -eq 2 ] && [ -n "$2" ] || die "usage: hack/fetch-go.sh --check INSTALL_DIR"
  want=$2/go/bin/go
  onpath=$(command -v go) || die "no go on PATH, want $want"
  [ "$(readlink -f "$onpath")" = "$(readlink -f "$want")" ] || die "the go on PATH is $onpath, not the pinned $want"
  sha_ok "$want" "$bsha" || die "$want does not have the pinned sha256 $bsha"
  installed_go_reports "$onpath"
  # The effective root: GOROOT is not cleared here, so an inherited one is seen as the go command sees it.
  root=$(GOTOOLCHAIN=local "$onpath" env GOROOT 2>&1) || die "$onpath env GOROOT failed: $root"
  [ "$(readlink -f "$root")" = "$(readlink -f "$2/go")" ] ||
    die "go env GOROOT is $root, not the pinned $2/go: the go command would build with another tree's compiler and standard library"
  echo "ok       go $gover on PATH ($onpath)"
  exit 0
fi

[ $# -eq 2 ] && [ -n "$1" ] && [ -n "$2" ] || die "usage: hack/fetch-go.sh --version | DOWNLOAD_DIR INSTALL_DIR | --check INSTALL_DIR"
dl=$1
inst=$2
archive=$dl/$tarball

mkdir -p "$dl" "$inst"
# Each run's stages are its own, and the only directories it removes on any exit (the download inside
# one goes too). DOWNLOAD_DIR and INSTALL_DIR are never removed.
stages=()
cleanup() { [ ${#stages[@]} -eq 0 ] || rm -rf "${stages[@]}"; }
trap cleanup EXIT

# 1. The tarball.
if sha_ok "$archive" "$tsha"; then
  echo "ok       go $gover tarball ($archive)"
else
  if [ -e "$archive" ]; then
    echo "refusing $archive: its sha256 is not the pin, so it is deleted and downloaded again"
    rm -f "$archive"
  fi
  stage=$(mktemp -d "$dl/.go.XXXXXX")
  stages+=("$stage")
  echo "fetching go $gover from $base/$tarball"
  curl -fsSL --retry 3 --connect-timeout 15 --max-time 600 -o "$stage/$tarball" "$base/$tarball" ||
    die "download of $base/$tarball failed"
  got=$(sha256sum "$stage/$tarball" | cut -d' ' -f1)
  if [ "$got" != "$tsha" ]; then
    rm -f "$stage/$tarball"
    die "checksum mismatch for $tarball: got $got, the pin says $tsha; the download is deleted and nothing was extracted"
  fi
  mv -fT "$stage/$tarball" "$archive"
  echo "verified $tarball sha256 $tsha"
fi

# 2. The tree.
stage=$(mktemp -d "$inst/.go.XXXXXX")
stages+=("$stage")
tar -xzf "$archive" -C "$stage" || die "extracting the verified $tarball failed"
sha_ok "$stage/go/bin/go" "$bsha" || die "go/bin/go from the verified $tarball does not match its pin $bsha; nothing was installed"
[ "$(head -n 1 "$stage/go/VERSION")" = "go$gover" ] || die "go/VERSION from the verified $tarball is not go$gover; nothing was installed"
rm -rf "$inst/go"
mv -T "$stage/go" "$inst/go"
installed_go_reports "$inst/go/bin/go"
echo "installed go $gover in $inst/go from the verified $tarball"
