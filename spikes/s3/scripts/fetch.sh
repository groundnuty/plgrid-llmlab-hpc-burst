#!/usr/bin/env bash
# Download the pinned binaries of spike S3 into $SCRATCH, verify their sha256, and
# unpack them. Nothing is written outside $SCRATCH. Read-only downloads of public releases.
set -euo pipefail
SCRATCH=${SCRATCH:?set SCRATCH to a writable directory with at least 6 GB free}
command -v go >/dev/null || { echo "Go 1.27.x must be on PATH" >&2; exit 1; }
mkdir -p "$SCRATCH"/{dl,bin}

fetch() { # url sha256 dest
  local url=$1 sum=$2 dest=$3
  if [ ! -f "$dest" ] || ! echo "$sum  $dest" | sha256sum -c --status -; then
    curl -fsSL --retry 3 -o "$dest.part" "$url"
    mv "$dest.part" "$dest"
  fi
  echo "$sum  $dest" | sha256sum -c --status - || { echo "checksum mismatch: $dest" >&2; exit 1; }
}

# kcp (Apache-2.0): previous minor and current minor
for spec in \
  "0.32.5 8033aa3644b0a5a5a14da8f762f631801463def1bcc4864e0f8439f8d19cbd1c" \
  "0.33.3 ab76dbafffc496f2100228f454d4231be32962f648873ecc7452e53db580fbeb"; do
  set -- $spec
  fetch "https://github.com/kcp-dev/kcp/releases/download/v$1/kcp_$1_linux_amd64.tar.gz" "$2" "$SCRATCH/dl/kcp_$1.tar.gz"
  mkdir -p "$SCRATCH/bin/kcp-$1" && tar -xzf "$SCRATCH/dl/kcp_$1.tar.gz" -C "$SCRATCH/bin/kcp-$1"
done

if [ "${WITH_K3S:-0}" = 1 ]; then
# k3s (Apache-2.0), only with WITH_K3S=1: used for the not-measured attempt recorded in the README
# (the harness has no k3s option, because k3s cannot run here without root)
fetch "https://github.com/k3s-io/k3s/releases/download/v1.35.9%2Bk3s1/k3s" 897acb21cca57d477a211102251a6591450ce767344a9faaa121a1007ca9fa9a "$SCRATCH/dl/k3s-1.35.9"
fetch "https://github.com/k3s-io/k3s/releases/download/v1.36.5%2Bk3s1/k3s" d73847bcd3c5fccef0115b372e2f9a91f3032dc84bbf71518a4617565294d313 "$SCRATCH/dl/k3s-1.36.5"
for v in 1.35.9 1.36.5; do mkdir -p "$SCRATCH/bin/k3s-$v"; install -m 0755 "$SCRATCH/dl/k3s-$v" "$SCRATCH/bin/k3s-$v/k3s"; done
fi

# kube-controller-manager (Apache-2.0), same minors as the envtest bundles; envtest ships none
fetch "https://dl.k8s.io/release/v1.35.0/bin/linux/amd64/kube-controller-manager" d175511887be863be36c5b7ef7e859d22f08bd0e80cebca628edd48f0171c3f7 "$SCRATCH/dl/kcm-1.35.0"
fetch "https://dl.k8s.io/release/v1.36.2/bin/linux/amd64/kube-controller-manager" 6332e45474baf6362f50394d8f7c6002274db04d68fe51dbe2229ef5c0b21e3c "$SCRATCH/dl/kcm-1.36.2"
for v in 1.35.0 1.36.2; do install -m 0755 "$SCRATCH/dl/kcm-$v" "$SCRATCH/bin/kube-controller-manager-$v"; done

# kube-apiserver and etcd from controller-runtime's setup-envtest (Apache-2.0), pinned
GOBIN="$SCRATCH/bin" go install sigs.k8s.io/controller-runtime/tools/setup-envtest@v0.25.2
for v in 1.35.0 1.36.2; do "$SCRATCH/bin/setup-envtest" use "$v" --os linux --arch amd64 --bin-dir "$SCRATCH/bin/envtest" -p path >/dev/null; done
echo "fetched into $SCRATCH/bin"
