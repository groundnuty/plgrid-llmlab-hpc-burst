#!/usr/bin/env bash
# Generates the CRD manifests under config/crd and the deepcopy methods under
# api/v1alpha1 with controller-gen, pinned to v0.20.0 (docs/development.md).
#
#   hack/generate.sh [CRD_OUT_DIR]
#
# CRD_OUT_DIR defaults to config/crd. The deepcopy file is written beside the
# types. hack/verify-generated.sh runs this on a temporary copy of the types and
# diffs the result against the committed files.
#
#   HPCBURST_GENERATE_ROOT   the directory with go.mod and api/ to generate from
#                            (default: the repository root)
#
# controller-gen is installed into a tools directory, never system-wide:
#   HPCBURST_TOOLS_BIN   where controller-gen goes (default: $TMPDIR/hpcburst-tools/bin,
#                        or /tmp/hpcburst-tools/bin; keep it off a nearly full /home)
# Go must be on PATH (the repository needs Go 1.26 or newer).
set -euo pipefail

CONTROLLER_GEN_VERSION=v0.20.0
root=${HPCBURST_GENERATE_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}
out=${1:-$root/config/crd}
bin=${HPCBURST_TOOLS_BIN:-${TMPDIR:-/tmp}/hpcburst-tools/bin}

command -v go >/dev/null || { echo "generate.sh: go is not on PATH" >&2; exit 1; }
mkdir -p "$bin" "$out"

gen=$bin/controller-gen-$CONTROLLER_GEN_VERSION
if [ ! -x "$gen" ]; then
  tmp=$(mktemp -d "$bin/install.XXXXXX")
  trap 'rm -rf "$tmp"' EXIT
  GOBIN=$tmp go install "sigs.k8s.io/controller-tools/cmd/controller-gen@$CONTROLLER_GEN_VERSION"
  mv "$tmp/controller-gen" "$gen"
fi
got=$("$gen" --version)
case $got in
  *"$CONTROLLER_GEN_VERSION"*) ;;
  *) echo "generate.sh: wanted controller-gen $CONTROLLER_GEN_VERSION, found: $got" >&2; exit 1 ;;
esac

cd "$root"
# Deepcopy methods are written next to the types; the CRDs go to $out.
"$gen" object paths="./api/..."
"$gen" crd:crdVersions=v1,allowDangerousTypes=false paths="./api/..." output:crd:artifacts:config="$out"
