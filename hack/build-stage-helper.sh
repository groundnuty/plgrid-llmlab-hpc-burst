#!/usr/bin/env bash
# The pre-staging row's helper (issue #376, alpha-athena-now-stage; internal/providerctl/conformance/stagehelper): built for
# the compute node, with the served deployment's image and checkpoint compiled in, as the helper's package comment says.
#
#   hack/build-stage-helper.sh <output> [<goarch>]     builds it (goarch amd64 by default: Athena's A100 nodes are x86_64)
#   hack/build-stage-helper.sh --print                 prints the -ldflags it would build with, and builds nothing
#
# The values are those of examples/modeldeployment-qwen25-instruct.yaml, the deployment row A serves (internal/alpha,
# liveWorld): its engine's image and imageDigest, its checkpoint's repository and revision, and its served flags'
# load-format, auto when it names none (the stager reads an empty one as auto). Each is held to its shape before the build,
# and internal/providerctl/conformance (TestTheStageHelpersBuildIsTheExampleDeployments) checks them against the file read as
# a ModelDeployment. hack/live-checks.sh runs it before the row takes its slot.
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
example="$root/examples/modeldeployment-qwen25-instruct.yaml"

# value <key>: the first "<key>: <value>" line's value, without quotes.
value() { sed -n -E "s/^ +$1: *\"?([^\" ]+)\"?[[:space:]]*\$/\\1/p" "$example" | head -1; }

image=$(value image)
digest=$(value imageDigest)
repo=$(value repository)
rev=$(value revision)
load=$(awk '/- name: load-format$/ { getline; sub(/^ *value: */, ""); gsub(/"/, ""); print; exit }' "$example")
load=${load:-auto}

bad=0
[[ $image =~ ^[a-z0-9][a-z0-9._/:-]*[a-z0-9]$ ]] || { echo "build-stage-helper: the example's image is not a repository reference" >&2; bad=1; }
[[ $digest =~ ^sha256:[0-9a-f]{64}$ ]] || { echo "build-stage-helper: the example's imageDigest is not sha256:<64 hex>" >&2; bad=1; }
[[ $repo =~ ^[A-Za-z0-9][A-Za-z0-9._-]*/[A-Za-z0-9][A-Za-z0-9._-]*$ ]] || { echo "build-stage-helper: the example's repository is not <owner>/<name>" >&2; bad=1; }
[[ $rev =~ ^[0-9a-f]{40}$ ]] || { echo "build-stage-helper: the example's revision is not a full commit id" >&2; bad=1; }
[[ $load =~ ^[a-z_]+$ ]] || { echo "build-stage-helper: the example's load-format is not a word" >&2; bad=1; }
[ "$bad" = 0 ] || exit 2

flags="-X main.image=$image -X main.imageDigest=$digest -X main.checkpointRepo=$repo -X main.checkpointRev=$rev -X main.loadFormat=$load"
if [ "${1:-}" = --print ]; then printf '%s\n' "$flags"; exit 0; fi
[ $# -ge 1 ] || { echo "usage: hack/build-stage-helper.sh <output> [<goarch>] | --print" >&2; exit 2; }
out=$1 arch=${2:-amd64}
[[ $arch =~ ^(amd64|arm64)$ ]] || { echo "build-stage-helper: goarch must be amd64 or arm64" >&2; exit 2; }
cd "$root"
CGO_ENABLED=0 GOOS=linux GOARCH=$arch go build -trimpath -ldflags "$flags" -o "$out" ./internal/providerctl/conformance/stagehelper
