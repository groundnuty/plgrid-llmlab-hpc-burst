#!/usr/bin/env bash
# Runs every S4 test: unit tests of the Slurm client and the fake, then the
# end-to-end scenarios (envtest kube-apiserver + etcd, provider-slurm
# processes, fake Slurm on PATH). Build output, logs and state go under
# /tmp/hpc-burst-tools/spikes/s4 (never under /home).
#
# Usage: spikes/s4/run-tests.sh [go test -run regexp]
set -euo pipefail
cd "$(dirname "$0")"
. /tmp/hpc-burst-tools/env.sh

S4=/tmp/hpc-burst-tools/spikes/s4
export KUBEBUILDER_ASSETS=${KUBEBUILDER_ASSETS:-$S4/envtest-bin/k8s/1.35.0-linux-amd64}
export S4_WORK_ROOT=${S4_WORK_ROOT:-$S4/work}
mkdir -p "$S4_WORK_ROOT"
if [ ! -x "$KUBEBUILDER_ASSETS/kube-apiserver" ]; then
  echo "missing envtest binaries in $KUBEBUILDER_ASSETS; run:" >&2
  echo "  setup-envtest use 1.35.0 --bin-dir $S4/envtest-bin" >&2
  exit 2
fi

# Shared VM: wait for load <= 4.0 and >= 8 GB available memory.
while :; do
  load=$(cut -d' ' -f1 /proc/loadavg)
  avail=$(awk '/MemAvailable/ {print int($2/1048576)}' /proc/meminfo)
  if awk -v l="$load" 'BEGIN{exit !(l<=4.0)}' && [ "$avail" -ge 8 ]; then break; fi
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
    ps -eo rss=,args= | awk -v root="$S4" '
      index($0, root) && /kube-apiserver/ {a=($1>a)?$1:a}
      index($0, root) && /etcd/ && !/kube-apiserver/ {e=($1>e)?$1:e}
      index($0, root) && /provider-slurm/ {p=($1>p)?$1:p}
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

# Leftover check: list (never kill by name) any process started from this
# spike's tool directory.
left=$(pgrep -af "$S4/" | grep -v -e pgrep -e run-tests.sh || true)
if [ -n "$left" ]; then
  echo "LEFTOVER PROCESSES:"; echo "$left"; status=1
else
  echo "leftover processes under $S4: none"
fi
exit $status
