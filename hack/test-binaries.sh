#!/usr/bin/env bash
# Finds, or fetches, the Kubernetes binaries the control-plane tests run, and
# checks their sha256. It prints two lines for the tests to read:
#
#   HPCBURST_ENVTEST_ASSETS=<directory holding etcd and kube-apiserver>
#   HPCBURST_KUBE_CONTROLLER_MANAGER=<path of kube-controller-manager>
#
# Everything else goes to stderr. A binary whose sha256 differs from the pin is
# an error and is never used. The pins are upstream's own published values:
#   kube-apiserver, kube-controller-manager: https://dl.k8s.io/release/v1.36.2/bin/linux/amd64/<name>.sha256
#   etcd: https://github.com/etcd-io/etcd/releases/download/v3.6.8/SHA256SUMS (the tarball),
#         and the sha256 of the etcd binary inside that tarball.
#
# Where it looks, in order:
#   1. HPCBURST_ENVTEST_ASSETS and HPCBURST_KUBE_CONTROLLER_MANAGER, when both are set
#      (they are verified like everything else);
#   2. the cache directory: $HPCBURST_TOOLS_CACHE, else ${XDG_CACHE_HOME:-$HOME/.cache}/hpcburst.
#      It uses the layout  bin/kube-controller-manager-<version>  and
#      envtest/k8s/<version>-linux-amd64/{etcd,kube-apiserver}.
#   3. a download into that cache, when a file is missing. It refuses to download
#      when the cache's filesystem has less than 1 GiB free.
#
# The cache directory may be a restored CI cache (the workflow saves it between runs, so that a
# GitHub or dl.k8s.io outage does not fail every job). Nothing in it is trusted: each file is checked
# against its pin on every run. A cached file with another sha256 is refused and downloaded again;
# a file that the environment names (HPCBURST_ENVTEST_ASSETS, HPCBURST_KUBE_CONTROLLER_MANAGER) is
# never replaced, so a mismatch there stays an error.
#
# K8S_RELEASE_BASE (default https://dl.k8s.io/release; the layout is <base>/v<version>/bin/linux/amd64/<name>)
# and ETCD_RELEASE_BASE (default https://github.com/etcd-io/etcd/releases/download; the layout is
# <base>/v<version>/etcd-v<version>-linux-amd64.tar.gz) override the download sites, for mirrors and
# for tests. TEST_BINARIES_PINS_FILE names a file of NAME=VALUE lines (the four SHA_ names below)
# that replace the pins, for this script's tests only; the real run never sets it.
set -euo pipefail

K8S_VERSION=1.36.2
ETCD_VERSION=3.6.8
SHA_KUBE_APISERVER=6770be17296ef36b656ad84e52b043007fb9a47ba0445c224097323291b1b33b
SHA_KUBE_CONTROLLER_MANAGER=6332e45474baf6362f50394d8f7c6002274db04d68fe51dbe2229ef5c0b21e3c
SHA_ETCD_TARBALL=cf9cfe91a4856cb90eed9c99e6aee4b708db2c7888b88a6f116281f04b0ea693
SHA_ETCD=b8956dc9f7479b1f15c46d03edae5dd9db508932840f91a9818e67717fcb1850

log() { echo "test-binaries.sh: $*" >&2; }
die() { log "$*"; exit 1; }

if [ -n "${TEST_BINARIES_PINS_FILE:-}" ]; then
  [ -f "$TEST_BINARIES_PINS_FILE" ] || die "TEST_BINARIES_PINS_FILE=$TEST_BINARIES_PINS_FILE is not a file"
  while IFS== read -r pin_name pin_value; do
    case $pin_name in
      '' | '#'*) ;;
      SHA_KUBE_APISERVER | SHA_KUBE_CONTROLLER_MANAGER | SHA_ETCD_TARBALL | SHA_ETCD) printf -v "$pin_name" %s "$pin_value" ;;
      *) die "$TEST_BINARIES_PINS_FILE names $pin_name, which is not one of the four pins" ;;
    esac
  done < "$TEST_BINARIES_PINS_FILE"
fi

cache=${HPCBURST_TOOLS_CACHE:-${XDG_CACHE_HOME:-$HOME/.cache}/hpcburst}
assets=${HPCBURST_ENVTEST_ASSETS:-$cache/envtest/k8s/$K8S_VERSION-linux-amd64}
kcm=${HPCBURST_KUBE_CONTROLLER_MANAGER:-$cache/bin/kube-controller-manager-$K8S_VERSION}

# The script owns a file that sits in its cache, and does not own one that the environment names.
assets_owned=1
kcm_owned=1
[ -z "${HPCBURST_ENVTEST_ASSETS:-}" ] || assets_owned=
[ -z "${HPCBURST_KUBE_CONTROLLER_MANAGER:-}" ] || kcm_owned=

# verify FILE SHA [OWNED]: succeeds when FILE exists and has that sha256, and returns 1 when it does
# not exist. A mismatch is fatal, except for a file the script owns (OWNED non-empty): that one is
# refused (reported, never used) and verify returns 1, so that the caller downloads it again.
verify() {
  local file=$1 want=$2 owned=${3:-} got
  [ -f "$file" ] || return 1
  got=$(sha256sum "$file" | cut -d' ' -f1)
  if [ "$got" != "$want" ]; then
    if [ -n "$owned" ]; then
      log "$file has sha256 $got, expected $want: refusing it and downloading it again"
      return 1
    fi
    die "$file has sha256 $got, expected $want"
  fi
}

need_free() {
  local dir=$1 avail
  mkdir -p "$dir"
  avail=$(df -Pk "$dir" | awk 'NR==2 {print $4}')
  if [ "$avail" -lt 1048576 ]; then
    log "refusing to download into $dir: only $((avail / 1024)) MiB free (set HPCBURST_TOOLS_CACHE to a roomier filesystem)"
    exit 1
  fi
}

# fetch URL DEST SHA: downloads into a temporary file of its own next to DEST, checks its
# sha256, and only then renames it into place. It runs in a subshell whose EXIT trap removes the
# temporary file on any failure. Callers hold the cache lock (below), so two test packages that
# start at once on an empty cache never write the same file.
fetch() {
  need_free "$(dirname "$2")"
  log "downloading $1"
  (
    tmp=$(mktemp "$2.XXXXXX")
    trap 'rm -f "$tmp"' EXIT
    # 9>&-: the download never holds the cache lock, even if this script is killed under it.
    if ! curl -fsSL --retry 3 --max-time 900 -o "$tmp" "$1" 9>&-; then
      log "download of $1 failed"
      exit 1
    fi
    got=$(sha256sum "$tmp" | cut -d' ' -f1)
    if [ "$got" != "$3" ]; then
      log "$1 has sha256 $got, expected $3"
      exit 1
    fi
    chmod 0555 "$tmp"
    mv "$tmp" "$2"
    trap - EXIT
  ) || exit 1
}

dl=${K8S_RELEASE_BASE:-https://dl.k8s.io/release}/v$K8S_VERSION/bin/linux/amd64
etcd_url=${ETCD_RELEASE_BASE:-https://github.com/etcd-io/etcd/releases/download}/v$ETCD_VERSION/etcd-v$ETCD_VERSION-linux-amd64.tar.gz

# have_all: every binary is present with its pinned sha256 (a present file with another sha256 is
# fatal when the environment names it, and is refused and downloaded again when it sits in the
# cache; see verify). When they are all present, nothing below downloads, so binaries supplied
# through HPCBURST_ENVTEST_ASSETS and HPCBURST_KUBE_CONTROLLER_MANAGER need no cache at all.
have_all() {
  verify "$assets/kube-apiserver" "$SHA_KUBE_APISERVER" "$assets_owned" &&
    verify "$kcm" "$SHA_KUBE_CONTROLLER_MANAGER" "$kcm_owned" &&
    verify "$assets/etcd" "$SHA_ETCD" "$assets_owned"
}

if have_all; then
  log "ok       the pinned kube-apiserver, kube-controller-manager and etcd are present with their sha256 ($assets, $kcm)"
else
  # One downloader at a time per cache: go test runs packages in parallel, and in CI the cache
  # starts empty, so several TestMains reach this point together. Each one re-checks after it
  # gets the lock, so a file another caller has just finished is not downloaded again. The lock
  # is released when this script exits.
  mkdir -p "$cache"
  exec 9>"$cache/.test-binaries.lock"
  flock 9

  verify "$assets/kube-apiserver" "$SHA_KUBE_APISERVER" "$assets_owned" || {
    mkdir -p "$assets"
    fetch "$dl/kube-apiserver" "$assets/kube-apiserver" "$SHA_KUBE_APISERVER"
    verify "$assets/kube-apiserver" "$SHA_KUBE_APISERVER"
  }

  verify "$kcm" "$SHA_KUBE_CONTROLLER_MANAGER" "$kcm_owned" || {
    mkdir -p "$(dirname "$kcm")"
    fetch "$dl/kube-controller-manager" "$kcm" "$SHA_KUBE_CONTROLLER_MANAGER"
    verify "$kcm" "$SHA_KUBE_CONTROLLER_MANAGER"
  }

  verify "$assets/etcd" "$SHA_ETCD" "$assets_owned" || {
    # Staged beside its destination, so the final mv is a rename on one filesystem.
    mkdir -p "$assets"
    tmp=$(mktemp -d "$assets/.etcd.XXXXXX")
    trap 'rm -rf "$tmp"' EXIT
    need_free "$tmp"
    log "downloading $etcd_url"
    curl -fsSL --retry 3 --max-time 900 -o "$tmp/etcd.tgz" "$etcd_url" 9>&- || die "download of $etcd_url failed"
    verify "$tmp/etcd.tgz" "$SHA_ETCD_TARBALL"
    tar -xzf "$tmp/etcd.tgz" -C "$tmp" --strip-components=1 "etcd-v$ETCD_VERSION-linux-amd64/etcd" 9>&-
    verify "$tmp/etcd" "$SHA_ETCD"
    chmod 0555 "$tmp/etcd"
    mv "$tmp/etcd" "$assets/etcd"
  }
fi

echo "HPCBURST_ENVTEST_ASSETS=$assets"
echo "HPCBURST_KUBE_CONTROLLER_MANAGER=$kcm"
