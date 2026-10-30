#!/bin/bash
#SBATCH --job-name=s7-hash-ckpt
#SBATCH --partition=plgrid
#SBATCH --no-requeue
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --time=01:00:00
#
# Spike S7: hash staged checkpoints from a compute node and measure what the reads cost.
# The account and the output file are not in this file: pass them on the sbatch command line,
#   sbatch --account "$ACCOUNT" --output "$S7_OUT.slurm" \
#          --export=ALL,HF_HUB_CACHE=...,S7_DIR=...,S7_OUT=...,S7_PY311=...,S7_MS_SITE=... hash_job.sh
#
# Phases, in the order that keeps the acceptance data safe if the time limit hits:
#   env, stat-before, cpu-ceiling, hash (each checkpoint), stat-after, cheap-check probe,
#   read-size sweep, model_signing (each checkpoint, skipped when it cannot finish).
set -euo pipefail

for v in HF_HUB_CACHE S7_DIR S7_OUT S7_PY311 S7_MS_SITE; do
  if [ -z "${!v:-}" ]; then echo "missing required variable: $v" >&2; exit 2; fi
done
for p in "$HF_HUB_CACHE" "$S7_DIR/hash_tree.py" "$S7_DIR/ms_manifest.py" "$S7_DIR/probe_cheap_check.py" \
         "$S7_DIR/checkpoints.env" "$S7_PY311" "$S7_MS_SITE"; do
  if [ ! -e "$p" ]; then echo "missing path: $p" >&2; exit 2; fi
done
# shellcheck disable=SC1091
source "$S7_DIR/checkpoints.env"

TIME_LIMIT_S="${S7_TIME_LIMIT_S:-3600}"
WORKERS="${SLURM_CPUS_PER_TASK:-4}"
BUF=$((8 * 1024 * 1024))
PY36=/usr/bin/python3
mkdir "$S7_OUT"   # refuses an existing directory: a run never overwrites an earlier one

log() { printf '[%s +%ss] %s\n' "$(date -u +%FT%TZ)" "$SECONDS" "$*"; }

set +e   # the environment record is best effort: a missing tool must not stop the run
{
  echo "date_utc=$(date -u +%FT%TZ)"
  echo "node=$(hostname)"
  echo "kernel=$(uname -r)"
  echo "host_python=$($PY36 -V 2>&1)"
  echo "py311=$("$S7_PY311" -V 2>&1)"
  echo "sha256sum=$(sha256sum --version | head -1)"
  lscpu | grep -E '^(Model name|CPU\(s\)|Thread|Core|Socket)'
  echo "sha_ni=$(grep -o -w -m1 sha_ni /proc/cpuinfo | head -1)"
  echo "cpus_allowed=$(grep Cpus_allowed_list /proc/self/status | cut -f2)"
  grep -E '^(MemTotal|MemAvailable)' /proc/meminfo
  for f in /sys/fs/cgroup/memory.max /sys/fs/cgroup/memory/memory.limit_in_bytes; do
    if [ -r "$f" ]; then echo "cgroup_mem $f=$(cat "$f")"; fi
  done
  echo "loadavg=$(cut -d' ' -f1-3 /proc/loadavg)"
  findmnt -T "$HF_HUB_CACHE" -o FSTYPE,OPTIONS -n | cut -c1-120
  echo "lctl=$(command -v lctl || echo none)"
  lctl get_param llite.*.max_read_ahead_mb llite.*.max_read_ahead_per_file_mb llite.*.max_read_ahead_whole_mb 2>&1 \
    | sed -E 's/^llite\.[^.]*\./llite./'
  echo "slurm_cpus=${SLURM_CPUS_PER_TASK:-unset} slurm_mem=${SLURM_MEM_PER_NODE:-unset}"
} > "$S7_OUT/env.txt" 2>&1
set -e
log "env written"

stat_pass() {  # $1 = tag
  for label in $CKPT_LABELS; do
    rel="CKPT_${label}_REL"
    "$PY36" "$S7_DIR/hash_tree.py" --root "$HF_HUB_CACHE/${!rel}" --label "$label" --stat-only \
      --out "$S7_OUT/stat-$1-$label.jsonl" >/dev/null
  done
  log "stat pass $1 done"
}

stat_pass before

"$PY36" "$S7_DIR/hash_tree.py" --cpu-ceiling --label cpu-ceiling --bufsize "$BUF" --out "$S7_OUT/cpu-ceiling.json" >/dev/null
log "cpu ceiling done"

for label in $CKPT_LABELS; do
  rel="CKPT_${label}_REL"
  log "hash $label start"
  "$PY36" "$S7_DIR/hash_tree.py" --root "$HF_HUB_CACHE/${!rel}" --label "$label" --workers "$WORKERS" \
    --bufsize "$BUF" --fadvise-dontneed --out "$S7_OUT/hash-$label.jsonl" >/dev/null
  log "hash $label done"
done

stat_pass after

"$PY36" "$S7_DIR/probe_cheap_check.py" "$S7_OUT/probe-cheap-check" > "$S7_OUT/probe-cheap-check.json"
log "cheap-check probe done"

sweep() {  # $1 = tag, $2 = shard, $3 = buffer bytes, $4 = extra flag
  "$PY36" "$S7_DIR/hash_tree.py" --root "$HF_HUB_CACHE/$SWEEP_REL" --label "sweep-$1" --workers 1 \
    --bufsize "$3" --only "$2" $4 --out "$S7_OUT/sweep-$1.jsonl" >/dev/null
}
sweep 1MiB "$SWEEP_SHARD_1M" $((1024 * 1024)) --fadvise-dontneed
sweep 8MiB "$SWEEP_SHARD_8M" "$BUF" --fadvise-dontneed
sweep 64MiB "$SWEEP_SHARD_64M" $((64 * 1024 * 1024)) --fadvise-dontneed
sweep warm-1 "$SWEEP_SHARD_WARM" "$BUF" ""
sweep warm-2 "$SWEEP_SHARD_WARM" "$BUF" ""
log "sweep done"

for label in $CKPT_LABELS; do
  rel="CKPT_${label}_REL"
  # Skip when the projected time (bytes / measured rate, times 1.5) exceeds what is left of the limit.
  projected=$("$PY36" - "$S7_OUT/hash-$label.jsonl.summary.json" <<'PYEOF'
import json, sys
s = json.load(open(sys.argv[1]))
print(int(s["wall_s"] * 1.5) + 30)
PYEOF
)
  remaining=$((TIME_LIMIT_S - SECONDS - 120))
  if [ "$projected" -gt "$remaining" ]; then
    log "model_signing $label skipped: projected ${projected}s > remaining ${remaining}s"
    echo "skipped: projected ${projected}s > remaining ${remaining}s" > "$S7_OUT/ms-$label.skipped"
    continue
  fi
  log "model_signing $label start"
  PYTHONPATH="$S7_MS_SITE" "$S7_PY311" "$S7_DIR/ms_manifest.py" --root "$HF_HUB_CACHE/${!rel}" --label "$label" \
    --workers "$WORKERS" --chunk "$BUF" --out "$S7_OUT/ms-$label.json" >/dev/null
  log "model_signing $label done"
done
log "all phases done"
