#!/usr/bin/env bash
# Runs every S4 test: unit tests of the Slurm client and the fake Slurm, then
# the end-to-end scenarios (envtest kube-apiserver + etcd, provider-slurm
# processes, fake Slurm on PATH).
#
# Needs Go 1.27.x on PATH and the envtest binaries:
#   go install sigs.k8s.io/controller-runtime/tools/setup-envtest@v0.24.1
#   export KUBEBUILDER_ASSETS=$(setup-envtest use 1.35.0 -p path)
# Optional: S4_WORK_ROOT (builds, logs, fake state; default: system temp
# directory), S4_MAX_LOAD (wait until the 1-minute load is at most this,
# default 4.0, for a shared host).
#
# Usage: run-tests.sh [go test -run regexp for the e2e tests]
set -euo pipefail
cd "$(dirname "$0")"

: "${KUBEBUILDER_ASSETS:?set KUBEBUILDER_ASSETS, see the header of this script}"
if [ ! -x "$KUBEBUILDER_ASSETS/kube-apiserver" ]; then
  echo "no kube-apiserver in KUBEBUILDER_ASSETS=$KUBEBUILDER_ASSETS" >&2
  exit 2
fi
export S4_WORK_ROOT=${S4_WORK_ROOT:-${TMPDIR:-$(dirname "$(mktemp -u)")}/s4-e2e}
mkdir -p "$S4_WORK_ROOT"
max_load=${S4_MAX_LOAD:-4.0}

# Shared host: wait for the load limit and at least 8 GB available memory.
while :; do
  load=$(cut -d' ' -f1 /proc/loadavg)
  avail=$(awk '/MemAvailable/ {print int($2/1048576)}' /proc/meminfo)
  if awk -v l="$load" -v m="$max_load" 'BEGIN{exit !(l<=m)}' && [ "$avail" -ge 8 ]; then break; fi
  echo "waiting: load $load, available ${avail} GB" >&2
  sleep 60
done
echo "start: load $(cut -d' ' -f1-3 /proc/loadavg), available $(awk '/MemAvailable/ {print int($2/1048576)}' /proc/meminfo) GB"

run=${1:-.}
nice -n 10 go test -count=1 ./internal/... ./fakeslurm/...

# Sample the resident memory of the API server, etcd and the providers.
peaks=$S4_WORK_ROOT/rss-peaks.txt
: >"$peaks"
(
  while :; do
    ps -eo rss=,args= | grep -v awk | awk -v assets="$KUBEBUILDER_ASSETS" -v work="$S4_WORK_ROOT" '
      index($0, assets) && /kube-apiserver/ {a=($1>a)?$1:a}
      index($0, assets) && /etcd/ && !/kube-apiserver/ {e=($1>e)?$1:e}
      index($0, work) && /provider-slurm/ {p=($1>p)?$1:p}
      END {print a+0, e+0, p+0}' >>"$peaks"
    sleep 2
  done
) &
sampler=$!
trap 'kill "$sampler" 2>/dev/null || true' EXIT

status=0
nice -n 10 go test -count=1 -v -timeout 30m -run "$run" ./e2e/ || status=$?

kill "$sampler" 2>/dev/null || true
awk '{a=($1>a)?$1:a; e=($2>e)?$2:e; p=($3>p)?$3:p} END {printf "peak RSS: kube-apiserver %d MiB, etcd %d MiB, one provider-slurm %d MiB\n", a/1024, e/1024, p/1024}' "$peaks"
echo "end: load $(cut -d' ' -f1-3 /proc/loadavg)"

# Leftover check: list (never kill by name) processes started from the
# envtest assets or the work root.
left=$(pgrep -af "$KUBEBUILDER_ASSETS/|$S4_WORK_ROOT/" 2>/dev/null | grep -v -e pgrep -e run-tests.sh || true)
if [ -n "$left" ]; then
  echo "LEFTOVER PROCESSES:"; echo "$left"; status=1
else
  echo "leftover processes from this run: none"
fi
exit $status
