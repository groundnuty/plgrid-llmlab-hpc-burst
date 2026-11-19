#!/usr/bin/env bash
# Generates the boolean-flag data file for one vLLM engine image, from vLLM's
# own argparse parser, run inside that image (hack/vllm-flags/dump.py).
#
#   SIF=<file.sif> DIGEST=sha256:<64 hex> VERSION=<x.y.z> OUT=<file.json> hack/vllm-flags/generate.sh
#
# The result is internal/lifecycle/enginebooleans/vllm-<version>.json. The
# controller renders a spec's boolean flags from it (HBEP-3, What admission
# checks); it cannot be regenerated in CI, because it needs the image.
#
#   SIF      the engine image as a SIF file
#   DIGEST   the image digest the SIF was made from (sha256:<64 hex>)
#   VERSION  the vLLM version the image holds; dump.py refuses a different one
#   OUT      the data file to write
#
# Optional:
#   IMAGE          the image's repository (default docker.io/vllm/vllm-openai)
#   SOURCE_COMMIT  the vLLM source commit, when known
#   NOTE           a free-text note stored in the file
#   SIF_SHA256     the SIF's SHA-256, from whoever pulled it (see below)
#   APPTAINER      the apptainer binary (default apptainer)
#   APPTAINER_ARGS extra words for `apptainer exec` (for example --userns), split on spaces
#
# Which SIF is read is decided by a record, never by its name. Two forms:
#   1. A sidecar <SIF>.sha256 beside the SIF (the framework's staged-image
#      record, format hpcburst-sif/1: image, size, sha256; see
#      internal/agent/stage/image.go). Its image must equal DIGEST, and the
#      file's size and SHA-256 must match it. If SIF_SHA256 is also set it must
#      equal the sidecar's.
#   2. With no sidecar, SIF_SHA256=<64 hex> from the caller, the SIF's SHA-256
#      as recorded by whoever pulled it (`apptainer pull ... docker://...@DIGEST`).
#      The file is hashed and refused on a mismatch. The data file then says so
#      in its "provenance" field.
#
# The data file records that the parser was built on vLLM's CPU platform (the
# node has no GPU). The run then prints dump.py --platform-hits, every place the
# installed vLLM's source lets the platform reach the parser, so that someone
# reads it and decides that none changes a boolean's registration or kind.
#
# It runs the dump as `apptainer exec --cleanenv --no-home` with dump.py bound
# read-only, in a private work directory (under TMPDIR) that is removed at the
# end. The node needs no GPU: the parser is built without one. A failure to
# import vllm exits 3 (the image or the node, not a finding about flags);
# any other failure is a finding and exits 1.
#
# Probe mode: instead of writing a data file, parse command lines with the
# image's own parser:
#
#   SIF=... DIGEST=... VERSION=... PROBE_ARGV=<file.json> PROBE_OUT=<file.json> \
#     [PROBE_DESTS=a,b] [PROBE_EXPECT='{"a": true}'] hack/vllm-flags/generate.sh
#
# PROBE_ARGV is a JSON array of argument lists (what follows `vllm serve`).
# PROBE_OUT gets, for each, whether it parsed and the values of PROBE_DESTS.
# PROBE_EXPECT is a JSON object of dest values every line must show.
set -euo pipefail

die() { echo "generate.sh: $*" >&2; exit 1; }

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
apptainer=${APPTAINER:-apptainer}
image=${IMAGE:-docker.io/vllm/vllm-openai}

: "${SIF:?SIF is required}" "${DIGEST:?DIGEST is required}" "${VERSION:?VERSION is required}"
[[ $DIGEST =~ ^sha256:[0-9a-f]{64}$ ]] || die "DIGEST is not sha256:<64 hex>"
[[ $VERSION =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || die "VERSION is not x.y.z"
[[ -z ${SOURCE_COMMIT:-} || $SOURCE_COMMIT =~ ^[0-9a-f]{7,40}$ ]] || die "SOURCE_COMMIT is not a hex commit"
[[ -z ${SIF_SHA256:-} || $SIF_SHA256 =~ ^[0-9a-f]{64}$ ]] || die "SIF_SHA256 is not 64 hex digits"
[[ -f $SIF ]] || die "SIF is not a file"
[[ -f $here/dump.py ]] || die "dump.py is not beside this script"
probe=${PROBE_ARGV:-}
if [[ -n $probe ]]; then
  [[ -f $probe ]] || die "PROBE_ARGV is not a file"
  : "${PROBE_OUT:?PROBE_OUT is required with PROBE_ARGV}"
else
  : "${OUT:?OUT is required}"
fi

# --- provenance: the SIF against a record ---
actual=$(sha256sum -- "$SIF" | cut -d' ' -f1)
[[ $actual =~ ^[0-9a-f]{64}$ ]] || die "could not hash the SIF"
size=$(stat -c %s -- "$SIF")
sidecar=$SIF.sha256
if [[ -f $sidecar ]]; then
  field() { sed -n "s/.*\"$1\": *\"\([^\"]*\)\".*/\1/p" "$sidecar" | head -n 1; }
  rec_format=$(field format) rec_image=$(field image) rec_sha=$(field sha256)
  rec_size=$(sed -n 's/.*"size": *\([0-9][0-9]*\).*/\1/p' "$sidecar" | head -n 1)
  [[ $rec_format == hpcburst-sif/1 ]] || die "the sidecar is not an hpcburst-sif/1 record"
  [[ $rec_image == "$DIGEST" ]] || die "the sidecar's image is not DIGEST"
  [[ $rec_size == "$size" ]] || die "the SIF's size does not match its sidecar"
  [[ $rec_sha == "$actual" ]] || die "the SIF's SHA-256 does not match its sidecar"
  [[ -z ${SIF_SHA256:-} || $SIF_SHA256 == "$actual" ]] || die "SIF_SHA256 does not match the SIF"
  provenance="staged SIF; sidecar hpcburst-sif/1 checked (image digest, size and SHA-256)"
else
  [[ -n ${SIF_SHA256:-} ]] || die "no sidecar $(basename -- "$sidecar") beside the SIF, so SIF_SHA256 is required"
  [[ $SIF_SHA256 == "$actual" ]] || die "the SIF's SHA-256 does not match SIF_SHA256"
  provenance="apptainer pull by digest; file sha256 checked against the puller's record"
fi

# --- the dump, in the image ---
work=$(mktemp -d "${TMPDIR:-/tmp}/vllm-flags.XXXXXX")
trap 'rm -rf -- "$work"' EXIT
mkdir -p "$work/home"

run() { # run <dump.py args...>: python3 -I dump.py in the image, $work as /hbout
  # shellcheck disable=SC2086 # APPTAINER_ARGS is a list of words by design
  "$apptainer" exec --cleanenv --no-home --pwd /tmp ${APPTAINER_ARGS:-} \
    --bind "$here:/hbgen:ro" --bind "$work:/hbout" \
    --env XDG_CACHE_HOME=/hbout/home/.cache --env XDG_CONFIG_HOME=/hbout/home/.config \
    --env VLLM_CACHE_ROOT=/hbout/home/.cache/vllm --env VLLM_CONFIG_ROOT=/hbout/home/.config/vllm \
    --env TRITON_CACHE_DIR=/hbout/home/.triton --env TORCHINDUCTOR_CACHE_DIR=/hbout/home/.inductor \
    --env VLLM_NO_USAGE_STATS=1 --env DO_NOT_TRACK=1 --env HF_HUB_OFFLINE=1 --env PYTHONDONTWRITEBYTECODE=1 \
    "$SIF" python3 -I /hbgen/dump.py "$@"
}

finish() { # finish <rc> <what>: map dump.py's exit to ours
  local rc=$1
  [[ $rc -eq 0 ]] && return 0
  [[ $rc -eq 3 ]] && { echo "generate.sh: $2: vllm could not be imported in the image (not a finding about flags)" >&2; exit 3; }
  echo "generate.sh: $2 failed (exit $rc)" >&2
  exit 1
}

if [[ -n $probe ]]; then
  cp -- "$probe" "$work/probe-argv.json"
  expect=${PROBE_EXPECT:-}
  [[ -n $expect ]] || expect='{}'
  rc=0
  run --expect-version "$VERSION" --probe-argv /hbout/probe-argv.json --probe-dests "${PROBE_DESTS:-}" \
    --probe-expect "$expect" --out /hbout/probe-out.json || rc=$?
  [[ -f $work/probe-out.json ]] && cp -- "$work/probe-out.json" "$PROBE_OUT"
  finish "$rc" "the probe"
  echo "generate.sh: wrote the probe results"
  exit 0
fi

args=(--expect-version "$VERSION" --image "$image" --image-digest "$DIGEST" --provenance "$provenance" --out /hbout/data.json)
[[ -n ${SOURCE_COMMIT:-} ]] && args+=(--source-commit "$SOURCE_COMMIT")
[[ -n ${NOTE:-} ]] && args+=(--note "$NOTE")
rc=0
run "${args[@]}" || rc=$?
finish "$rc" "the dump"
rc=0
run --check /hbout/data.json || rc=$?
finish "$rc" "the check of the result"
# The parser is built on the CPU platform: print every use of the platform that can reach it, for a person to read.
rc=0
run --platform-hits || rc=$?
finish "$rc" "the platform-hits report"
cp -- "$work/data.json" "$OUT.tmp"
mv -f -- "$OUT.tmp" "$OUT"
echo "generate.sh: wrote the data file (vLLM $VERSION)"
