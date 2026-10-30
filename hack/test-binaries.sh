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
set -euo pipefail

K8S_VERSION=1.36.2
ETCD_VERSION=3.6.8
SHA_KUBE_APISERVER=6770be17296ef36b656ad84e52b043007fb9a47ba0445c224097323291b1b33b
SHA_KUBE_CONTROLLER_MANAGER=6332e45474baf6362f50394d8f7c6002274db04d68fe51dbe2229ef5c0b21e3c
SHA_ETCD_TARBALL=cf9cfe91a4856cb90eed9c99e6aee4b708db2c7888b88a6f116281f04b0ea693
SHA_ETCD=b8956dc9f7479b1f15c46d03edae5dd9db508932840f91a9818e67717fcb1850

log() { echo "test-binaries.sh: $*" >&2; }

cache=${HPCBURST_TOOLS_CACHE:-${XDG_CACHE_HOME:-$HOME/.cache}/hpcburst}
assets=${HPCBURST_ENVTEST_ASSETS:-$cache/envtest/k8s/$K8S_VERSION-linux-amd64}
kcm=${HPCBURST_KUBE_CONTROLLER_MANAGER:-$cache/bin/kube-controller-manager-$K8S_VERSION}

# verify FILE SHA: succeeds when FILE exists and has that sha256. A mismatch is fatal.
verify() {
  local file=$1 want=$2 got
  [ -f "$file" ] || return 1
  got=$(sha256sum "$file" | cut -d' ' -f1)
  if [ "$got" != "$want" ]; then
    log "$file has sha256 $got, expected $want"
    exit 1
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

fetch() { # fetch URL DEST
  need_free "$(dirname "$2")"
  log "downloading $1"
  curl -fsSL --retry 3 -o "$2.part" "$1"
  mv "$2.part" "$2"
  chmod 0555 "$2"
}

dl=https://dl.k8s.io/release/v$K8S_VERSION/bin/linux/amd64

verify "$assets/kube-apiserver" "$SHA_KUBE_APISERVER" || {
  mkdir -p "$assets"; fetch "$dl/kube-apiserver" "$assets/kube-apiserver"; verify "$assets/kube-apiserver" "$SHA_KUBE_APISERVER"; }

verify "$kcm" "$SHA_KUBE_CONTROLLER_MANAGER" || {
  mkdir -p "$(dirname "$kcm")"; fetch "$dl/kube-controller-manager" "$kcm"; verify "$kcm" "$SHA_KUBE_CONTROLLER_MANAGER"; }

verify "$assets/etcd" "$SHA_ETCD" || {
  tmp=$(mktemp -d "$cache.etcd.XXXXXX")
  trap 'rm -rf "$tmp"' EXIT
  mkdir -p "$assets"
  need_free "$tmp"
  log "downloading etcd v$ETCD_VERSION"
  curl -fsSL --retry 3 -o "$tmp/etcd.tgz" "https://github.com/etcd-io/etcd/releases/download/v$ETCD_VERSION/etcd-v$ETCD_VERSION-linux-amd64.tar.gz"
  verify "$tmp/etcd.tgz" "$SHA_ETCD_TARBALL"
  tar -xzf "$tmp/etcd.tgz" -C "$tmp" --strip-components=1 "etcd-v$ETCD_VERSION-linux-amd64/etcd"
  verify "$tmp/etcd" "$SHA_ETCD"
  mv "$tmp/etcd" "$assets/etcd"
  chmod 0555 "$assets/etcd"
}

echo "HPCBURST_ENVTEST_ASSETS=$assets"
echo "HPCBURST_KUBE_CONTROLLER_MANAGER=$kcm"
