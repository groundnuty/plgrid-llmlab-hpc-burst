#!/bin/bash
# Issue #94, step 1: hb-agent against a real vLLM v0.32.0 on one GH200 GPU,
# with the controller double on the same node over loopback.
# Expects H (the run's scratch directory) and RUN_TAG in the environment.
set -u
OUT="$H/run-$RUN_TAG"
mkdir -p "$OUT"
say() { echo "$(date -u +%H:%M:%S) $*"; }
say "job start"

# Guard 1: the agent puts Apptainer's layer cache and build tree under TMPDIR.
# On Lustre that would be tens of thousands of inodes on a scratch quota at 97%.
# The job's own TMPDIR on Helios is on Lustre; the RAM disk of -C memfs is $MEMFS.
say "job TMPDIR filesystem: $(stat -f -c %T "$TMPDIR" 2>/dev/null); MEMFS: ${MEMFS:+set, }$(stat -f -c %T "${MEMFS:-/nonexistent}" 2>/dev/null)"
if [ -n "${MEMFS:-}" ]; then mkdir -p "$MEMFS/t" && export TMPDIR="$MEMFS/t"; fi
fs=$(stat -f -c %T "$TMPDIR" 2>/dev/null)
say "TMPDIR filesystem: $fs (path length ${#TMPDIR})"
case "$fs" in tmpfs|ramfs) ;; *) say "ABORT: TMPDIR is not memory-backed"; exit 3 ;; esac
df -h "$TMPDIR" | tail -1 | awk '{print "TMPDIR size " $2 ", free " $4}'

# Guard 2: the image is CUDA 13.0.2; the driver must support it.
nvidia-smi --query-gpu=name,driver_version,memory.total --format=csv,noheader
# CUDA 13.0 needs driver 580 or later (NVIDIA's CUDA toolkit release notes).
nvidia-smi | sed -n '1,4p'
nvidia-smi -q 2>/dev/null | grep -i 'cuda' | head -3
drv=$(nvidia-smi --query-gpu=driver_version --format=csv,noheader | head -1 | cut -d. -f1)
say "driver major version: ${drv:-unknown}"
if [ -z "$drv" ] || [ "$drv" -lt 580 ]; then say "ABORT: the driver is older than CUDA 13.0 needs (580)"; exit 3; fi
apptainer --version
say "cpus $(nproc)"; free -g | sed -n 2p

# Network: the Hub and the registry (a HEAD does not count against the pull limit).
curl -sS -o /dev/null -w "hub: HTTP %{http_code} in %{time_total}s\n" "https://huggingface.co/api/models/Qwen/Qwen3-0.6B/revision/c1899de289a04d12100db370d81485cdf75e47ca" || say "hub: unreachable"
tok=$(curl -sS "https://auth.docker.io/token?service=registry.docker.io&scope=repository:vllm/vllm-openai:pull" | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')
hdr=$(curl -sS -I -H "Authorization: Bearer $tok" -H "Accept: application/vnd.docker.distribution.manifest.list.v2+json" \
  "https://registry-1.docker.io/v2/vllm/vllm-openai/manifests/sha256:befccf0e7e4089aa23926bb6f887a85c0d46c2a99e6de5c5ffdf5cb7957c5b34" 2>&1)
echo "$hdr" | grep -i -E '^HTTP|ratelimit|docker-content-digest' | tr -d '\r'
rem=$(echo "$hdr" | tr -d '\r' | sed -n 's/^ratelimit-remaining: *\([0-9]*\).*/\1/Ip')
if [ -n "$rem" ] && [ "$rem" -lt 3 ]; then say "ABORT: Docker Hub pulls left for this address: $rem"; exit 3; fi
unset tok hdr

# The run.
export HB_REAL_ENGINE_PLAN="$H/plan.json" HB_AGENT_BIN="$H/bin/hb-agent" HB_REAL_OUT="$OUT" HB_REAL_STAGING="$H/staging"
export HB_REAL_BUDGET="${HB_REAL_BUDGET:-46m}" HB_REAL_STAGE_BUDGET="${HB_REAL_STAGE_BUDGET:-30m}"
mkdir -p "$H/staging"
cd "$TMPDIR" || exit 3
say "driver start"
"$H/bin/hbagent.test" -test.run '^TestRealEngine$' -test.v -test.timeout 52m
say "driver exit $?"

# What is left: processes of this job other than this script's own, and staging.
mycg=$(sed -n 's/.*\(job_[0-9][0-9]*\).*/\1/p' /proc/self/cgroup | head -1)
left=0
for p in /proc/[0-9]*; do
  pid=${p#/proc/}
  [ "$pid" = "$$" ] && continue
  grep -q "$mycg/" "$p/cgroup" 2>/dev/null || continue
  args=$(tr '\0' ' ' < "$p/cmdline" 2>/dev/null)
  case "$args" in ''|*slurmstepd*|*/proc/*|"bash "*job.sh*|"/bin/bash "*job.sh*) continue ;; esac
  [ "$(awk '{print $4}' "$p/stat" 2>/dev/null)" = "$$" ] && continue
  echo "left in job: $pid $args"; left=$((left+1))
done
say "processes left in the job: $left"
ls -la "$H/staging/images" 2>/dev/null
du -sh "$H/staging"/* 2>/dev/null
say "job end"
