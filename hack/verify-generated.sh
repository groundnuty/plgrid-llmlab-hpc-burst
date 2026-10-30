#!/usr/bin/env bash
# Fails when the committed generated files differ from a fresh generation.
#
#   hack/verify-generated.sh [CRD_DIR [DEEPCOPY_FILE]]
#
# It copies api/ (and go.mod, go.sum) into a temporary directory, runs
# hack/generate.sh there, and diffs the result against the committed CRDs
# (default config/crd) and the committed deepcopy file (default
# api/v1alpha1/zz_generated.deepcopy.go). The working tree is never written.
# The arguments let a test point it at an edited copy of the committed files.
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
crd_dir=${1:-$root/config/crd}
deepcopy=${2:-$root/api/v1alpha1/zz_generated.deepcopy.go}

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
mkdir -p "$tmp/tree"
cp -R "$root/api" "$root/go.mod" "$root/go.sum" "$tmp/tree/"
rm -f "$tmp/tree/api/v1alpha1/zz_generated.deepcopy.go"

HPCBURST_GENERATE_ROOT="$tmp/tree" "$root/hack/generate.sh" "$tmp/tree/config/crd" >&2

status=0
if ! diff -ru "$crd_dir" "$tmp/tree/config/crd" >&2; then
  echo "verify-generated.sh: $crd_dir differs from a fresh generation; run hack/generate.sh and commit the result" >&2
  status=1
fi
if ! diff -u "$deepcopy" "$tmp/tree/api/v1alpha1/zz_generated.deepcopy.go" >&2; then
  echo "verify-generated.sh: $deepcopy differs from a fresh generation; run hack/generate.sh and commit the result" >&2
  status=1
fi
[ "$status" -eq 0 ] && echo "verify-generated.sh: generated files are up to date" >&2
exit "$status"
