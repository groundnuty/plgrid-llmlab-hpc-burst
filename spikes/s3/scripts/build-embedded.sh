#!/usr/bin/env bash
# Build the kcp-as-a-library binary (spikes/s3/embedded) against a checkout of kcp at the
# pinned tag. kcp's go.mod replaces k8s.io/* with its Kubernetes fork and a few kcp-dev
# modules with in-repo staging directories; a consumer must repeat those replaces, so this
# script generates the embedded module's go.mod from kcp's own. Everything stays in $SCRATCH.
set -euo pipefail
SCRATCH=${SCRATCH:-/tmp/hpc-burst-tools/spikes/s3}
. "${GO_ENV:-/tmp/hpc-burst-tools/env.sh}"
HERE=$(cd "$(dirname "$0")/.." && pwd)
KCP_TAG=v0.33.3
KCP_SRC=$SCRATCH/src/kcp-${KCP_TAG#v}
if [ ! -d "$KCP_SRC/.git" ]; then
  mkdir -p "$SCRATCH/src"
  git clone --quiet --depth 1 --branch "$KCP_TAG" https://github.com/kcp-dev/kcp.git "$KCP_SRC"
fi
W=$SCRATCH/work/embedded-build
rm -rf "$W" && mkdir -p "$W"
cp "$HERE/embedded/main.go" "$W/main.go"
sed -e "s#@S3_DIR@#$HERE#" -e "s#@KCP_SRC@#$KCP_SRC#" "$HERE/embedded/go.mod.tmpl" > "$W/go.mod"
# Repeat kcp's replace blocks, pointing the staging modules at the checkout.
awk '/^replace \(/{p=1; print; next} p&&/^\)/{p=0; print; next} p{print}' "$KCP_SRC/go.mod" \
  | sed "s#=> \./#=> $KCP_SRC/#" >> "$W/go.mod"
cd "$W"
# Same pins as kcp for everything else: start from kcp's go.sum.
cp "$KCP_SRC/go.sum" "$W/go.sum"
GOFLAGS=-mod=mod go mod tidy
nice -n 10 go build -tags s3embedded -o "$SCRATCH/bin/s3-embedded" .
ls -la "$SCRATCH/bin/s3-embedded"
