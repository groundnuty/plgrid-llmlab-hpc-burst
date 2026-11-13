#!/bin/bash
# Spike S2c: compare routers inside one Instance on 4 real vLLM workers (one per A100 GPU), all inside one Slurm job.
#
# Arms (the dispatch table, arm_def below; llm-d or a probe is one new case plus a start function):
#   rr             vllm-router 0.1.17 --policy round_robin       (baseline)
#   vr-hash        vllm-router 0.1.17 --policy consistent_hash   (x-session-id)
#   envoy-bound    Envoy 1.40.2 ring_hash from the product's own configuration, bounded loads (BOUND, 125)
#   envoy-nobound  the same configuration without hash_balance_factor
#   probe          the dead-worker probe behind the bound Envoy arm (run_probe; s2crouter probe), the science agent's ruling on #62
#   relay-control  the relay's own cost: worker 0's traffic straight to its socket, then through the relay (run_control_arm)
# Steps (the regime letter of a PLAN item): B = KV cache capped at KV_BLOCKS_B blocks per worker, S2b's load; H = the same
# cap with LOAD_ARGS_H, S2b's workload at twice the concurrency, so the bound has load to act on (Envoy gives each worker
# ceil(ceil((active + 1) x 1.25) / workers) slots, HBEP-5:133); A = KV_BLOCKS_A (S2b's profiled size), S2b's
# load. The default PLAN runs A last, so the budget guard drops it first (the science agent's ruling on #62).
#
# Topology: NWORK vLLM servers, each started by `s2crouter worker` (the agent's own WorkerExec: Apptainer, a Unix
# socket, one GPU), all in parallel, each waited for on its own socket. Envoy arms run `s2crouter envoy` (its
# listener and admin are Unix sockets in the run dir). vllm-router arms reach the workers through `s2crouter relay`
# (loopback TCP to the same sockets) and count requests per worker in its own /metrics.
#
# Per arm: reset every worker's prefix cache (POST over its socket) -> scrape every worker -> start the router stack
# and wait for it -> router counters -> client (s2cload) with a 2 s sampler -> wait until the workers are idle ->
# scrape, counters -> stop the stack -> meta.json -> copy to results -> analyze.py --check-arm -> check.txt.
# The run label salts every prompt, so no arm can reuse another arm's blocks (the reset is the second guard).
#
# Environment (CONTRACT.md fixes every name): required S2C_HOME, IMAGE, CHECKPOINT_DIR, VR_BIN (every missing
# one is named in one error, exit 2); optional MODE (vllm | double: the dry run) STAGE RESULT_TAG PLAN LOAD_ARGS NWORK
# BOUND KV_BLOCKS_B KV_BLOCKS_A GPU_UTIL MAX_LEN MAX_NUM_SEQS SERVED BUDGET_S ARM_EST_S ARM_EST_S_H ARM_MAX_S ARM_MAX_S_H
# JOB_LIMIT_S END_MARGIN_S ARM_OVERHEAD_S FIRST_WORKER_WAIT_S WORKER_WAIT_S PROBE_EST_S PROBE_MAX_S PORT_BASE
# S2C_FAULT APPTAINER PYTHON, and, for tests that must not wait seconds on a poll: POLL_S SAMPLE_S DRAIN_WAIT_S
# ENVOY_WAIT_S VR_WAIT_S RELAY_WAIT_S. MODE changes only what is recorded (env.txt, meta.json); the dry run's
# engine double arrives as APPTAINER, so the code path is the same.
#
# Readiness is always an observable state with a deadline (the worker's /health and /v1/models, Envoy's READY file,
# the relay's .ready file, the router's /health and active-worker gauge, running requests summing to 0). No fixed
# sleep stands in for one; the only sleeps are the interval of a poll or of the sampler.
#
# Faults (dry-run seams; each must end in exit 1 with the arm's check failing):
#   S2C_FAULT=client-to-other-router  an Envoy arm also starts the vllm-router stack and the client talks to that
#   S2C_FAULT=strip-bound             envoy-bound arms start Envoy with `-fault strip-bound` (no bound, meta says 125)
#   S2C_FAULT=early-admit             the probe's Envoy holds every worker verified (`-fault ignore-verify`), so the
#                                     restarted victim takes requests before its verification: the probe must fail
#
# Exit status: 0 only if every arm in PLAN ran, was measured and passed its check. A skipped arm (budget, workers, router,
# a refused product smoke, a failed prefix-cache reset or scrape), a client that failed or did not report, workers that
# never went idle, a router that did not stop cleanly, or a failing --check-arm makes the final status 1; each is a line of
# failures.txt next to the results, which stay in place. Exit 2: the environment or the stage is wrong, before anything starts.
set -uo pipefail

T0=$(date +%s)
log() { echo "[$(date -u +%T) +$(( $(date +%s) - T0 ))s] $*"; }

# ---- environment: every missing required variable in one error (pattern D) ----
missing=()
for v in S2C_HOME IMAGE CHECKPOINT_DIR VR_BIN; do [[ -n ${!v:-} ]] || missing+=("$v"); done
if ((${#missing[@]} > 0)); then echo "job.sh: required variables not set: ${missing[*]}" >&2; exit 2; fi

MODE=${MODE:-vllm}
STAGE=${STAGE:-$S2C_HOME/stage}
RESULT_TAG=${RESULT_TAG:-run-$(date -u +%Y%m%dT%H%MZ)}
# The order the science agent agreed to on #62: B1, H1 (the bound's step), B2 (a repeat, for the spread), the probe, then
# A1 and the optional relay control, so a short hour drops the relay control, then A, before anything earlier.
PLAN=${PLAN:-"B:1:rr B:1:envoy-bound B:1:envoy-nobound B:1:vr-hash H:1:rr H:1:envoy-bound H:1:envoy-nobound H:1:vr-hash B:2:vr-hash B:2:envoy-nobound B:2:envoy-bound B:2:rr B:2:probe A:1:rr A:1:envoy-bound A:1:envoy-nobound A:1:vr-hash A:1:relay-control"}
LOAD_ARGS=${LOAD_ARGS:-"-sessions 96 -turns 12 -concurrency 32 -agents 0.5,0.3,0.2 -sys-words 2000 -task-words 250 -tool-min 200 -tool-max 800 -think-min 500ms -think-max 2500ms -reply-tokens 48 -seed 1"}
LOAD_ARGS_CONTROL=${LOAD_ARGS_CONTROL:-"-sessions 16 -turns 6 -concurrency 4 -agents 0.5,0.3,0.2 -sys-words 2000 -task-words 250 -tool-min 200 -tool-max 800 -think-min 500ms -think-max 2500ms -reply-tokens 48 -seed 1"}
LOAD_ARGS_H=${LOAD_ARGS_H:-"-sessions 192 -turns 12 -concurrency 64 -agents 0.5,0.3,0.2 -sys-words 2000 -task-words 250 -tool-min 200 -tool-max 800 -think-min 500ms -think-max 2500ms -reply-tokens 48 -seed 1"}
NWORK=${NWORK:-4}
BOUND=${BOUND:-125}
KV_BLOCKS_B=${KV_BLOCKS_B:-6000}
KV_BLOCKS_A=${KV_BLOCKS_A:-33010}
GPU_UTIL=${GPU_UTIL:-0.6}
MAX_LEN=${MAX_LEN:-16384}
MAX_NUM_SEQS=${MAX_NUM_SEQS:-256}
SERVED=${SERVED:-Qwen/Qwen2.5-1.5B-Instruct}
BUDGET_S=${BUDGET_S:-4200}      # stop starting arms when this much job time is used: 70 min of the 75 (the science agent, #62)
ARM_EST_S=${ARM_EST_S:-300}     # time one arm of B or A is expected to need, for the guard
ARM_EST_S_H=${ARM_EST_S_H:-600} # the same for H, which carries twice the requests (#527's review)
ARM_MAX_S=${ARM_MAX_S:-360}     # hard limit on one arm's client in B or A (S2b's clients ran 77 to 90 s)
ARM_MAX_S_H=${ARM_MAX_S_H:-600} # the same in H
PROBE_EST_S=${PROBE_EST_S:-420} # time the probe is expected to need, for the guard
PROBE_MAX_S=${PROBE_MAX_S:-600} # hard limit on the probe, from its start to its end, the victim's return included
# An arm is admitted only if its maximum, the sum of the limits job.sh enforces on it, ends ARM_END_MARGIN_S before the
# job's time limit, so Slurm never cuts an arm and loses its data (#527's review). JOB_LIMIT_S is shape.sh's --time.
JOB_LIMIT_S=${JOB_LIMIT_S:-4500}
END_MARGIN_S=${END_MARGIN_S:-180}   # the last scrape, the workers' stop, the copy of the results and the cleanup
ARM_OVERHEAD_S=${ARM_OVERHEAD_S:-120} # an arm's short steps: resets, scrapes, counters, the router's stop, the check
PROBE_HOLD=${PROBE_HOLD:-15s}   # the restarted victim stays unverified this long after its engine answers
PROBE_STREAM_TOKENS=${PROBE_STREAM_TOKENS:-1500}
FIRST_WORKER_WAIT_S=${FIRST_WORKER_WAIT_S:-900} # the job's first worker start (cold caches)
WORKER_WAIT_S=${WORKER_WAIT_S:-600}             # every later one: a step's restart, the probe's victim
PORT_BASE=${PORT_BASE:-31000}
S2C_FAULT=${S2C_FAULT:-}
APPTAINER=${APPTAINER:-apptainer}
PYTHON=${PYTHON:-python3}
POLL_S=${POLL_S:-1}             # interval of every readiness poll
SAMPLE_S=${SAMPLE_S:-2}         # interval of the sampler
DRAIN_WAIT_S=${DRAIN_WAIT_S:-120}
ENVOY_WAIT_S=${ENVOY_WAIT_S:-120}
VR_WAIT_S=${VR_WAIT_S:-120}
RELAY_WAIT_S=${RELAY_WAIT_S:-60}

bad=()
[[ $MODE == vllm || $MODE == double ]] || bad+=("MODE=$MODE (vllm or double)")
case $S2C_FAULT in "" | client-to-other-router | strip-bound | early-admit) ;; *) bad+=("S2C_FAULT=$S2C_FAULT (client-to-other-router, strip-bound or early-admit)") ;; esac
for v in NWORK KV_BLOCKS_B KV_BLOCKS_A MAX_LEN MAX_NUM_SEQS BUDGET_S ARM_EST_S ARM_EST_S_H ARM_MAX_S ARM_MAX_S_H FIRST_WORKER_WAIT_S WORKER_WAIT_S PORT_BASE DRAIN_WAIT_S ENVOY_WAIT_S VR_WAIT_S RELAY_WAIT_S PROBE_EST_S PROBE_MAX_S PROBE_STREAM_TOKENS JOB_LIMIT_S END_MARGIN_S ARM_OVERHEAD_S; do
  [[ ${!v} =~ ^[0-9]+$ ]] || bad+=("$v=${!v} (a whole number)")
done
[[ $NWORK =~ ^[0-9]+$ ]] && (( NWORK >= 1 )) || bad+=("NWORK must be at least 1")
[[ $BOUND =~ ^[0-9]+$ ]] || bad+=("BOUND=$BOUND (a whole number)")
[[ $POLL_S =~ ^[0-9]+(\.[0-9]+)?$ && $SAMPLE_S =~ ^[0-9]+(\.[0-9]+)?$ ]] || bad+=("POLL_S and SAMPLE_S must be numbers")
[[ $PROBE_HOLD =~ ^[0-9]+(ms|s|m)$ ]] || bad+=("PROBE_HOLD=$PROBE_HOLD (a Go duration such as 15s)")
for v in S2C_HOME STAGE IMAGE CHECKPOINT_DIR VR_BIN; do [[ ${!v} == /* ]] || bad+=("$v must be an absolute path"); done
TMPBASE=${TMPDIR:-/tmp}
[[ $TMPBASE == /* ]] || bad+=("TMPDIR must be an absolute path")
if ((${#bad[@]} > 0)); then printf 'job.sh: bad setting(s): %s\n' "${bad[*]}" >&2; exit 2; fi

RUN=$TMPBASE/s2c-run
CACHE=$TMPBASE/s2c-cache
OUT=$S2C_HOME/results/$RESULT_TAG
ROUTER_BIN=$STAGE/bin/s2crouter
LOAD_BIN=$STAGE/bin/s2cload
# A Unix socket path is at most 107 bytes; the longest of ours is the Envoy admin socket.
if (( ${#RUN} + 18 > 107 )); then echo "job.sh: the run directory $RUN is too long for a Unix socket path" >&2; exit 2; fi

# The stage and the staged inputs, all missing ones in one error.
miss=()
for f in "$ROUTER_BIN" "$LOAD_BIN" "$STAGE/py/s2c_placement.py" "$STAGE/scripts/analyze.py"; do [[ -f $f ]] || miss+=("${f#"$STAGE"/}"); done
[[ -x $ROUTER_BIN ]] || miss+=("bin/s2crouter (not executable)")
[[ -x $LOAD_BIN ]] || miss+=("bin/s2cload (not executable)")
[[ -e $IMAGE ]] || miss+=("IMAGE")
[[ -d $CHECKPOINT_DIR ]] || miss+=("CHECKPOINT_DIR (not a directory)")
[[ -x $VR_BIN ]] || miss+=("VR_BIN (not executable)")
if ((${#miss[@]} > 0)); then echo "job.sh: missing or unusable: ${miss[*]}" >&2; exit 2; fi

# The run directory is this job's own and starts empty (stale sockets and arms of an earlier run would be copied in).
case $RUN in */s2c-run) rm -rf "$RUN" ;; *) echo "job.sh: refusing to clear $RUN" >&2; exit 2 ;; esac
mkdir -p "$RUN"/{logs,arms,py} "$CACHE" "$OUT/arms" "$OUT/logs" || { echo "job.sh: cannot create the run or results directories" >&2; exit 2; }
cp "$STAGE/py/s2c_placement.py" "$RUN/py/" || { echo "job.sh: cannot stage the placement middleware" >&2; exit 2; }

FAILED=()   # one line per problem; any entry makes the final exit status 1
fail() { FAILED+=("$1"); echo "$1" >> "$OUT/failures.txt"; log "FAIL: $1"; }

# ---- processes: recorded by PID; every signal goes through a pidfd, never by name, pattern or process group ----
# `s2crouter signal|stop` (cmd/s2crouter/proc.go) signals a process only if it is this job's own child (its parent is
# this shell), opens a pidfd for it and each descendant, and checks the start time again after the open, so a PID that
# bash has reaped and the kernel has handed to another process is never signalled.
declare -A PIDS=()
descendants() { local c; echo "$1"; for c in $(ps -o pid= --ppid "$1" 2>/dev/null); do descendants "$c"; done; }   # for ps.txt only
alive() { # alive <pid>: a running child of this job (/proc, no signal): not gone, not a zombie, and its parent is this shell
  local s; s=$(cat "/proc/$1/stat" 2>/dev/null) || return 1
  s=${s##*) }; set -- $s
  [[ $1 != Z && $2 == "$$" ]]
}
stop_tree() { # stop_tree <pid>: TERM the tree, KILL what is left after 15 s, each through a verified pidfd
  local pid=$1; [[ -n $pid ]] || return 0
  "$ROUTER_BIN" stop -grace 15s "$pid" 2>/dev/null; wait "$pid" 2>/dev/null; return 0
}
stop_named() { local n; for n in "$@"; do [[ -n ${PIDS[$n]:-} ]] && stop_tree "${PIDS[$n]}"; unset "PIDS[$n]"; done; }
# stop_graceful <name>: TERM the process ALONE (the relay writes its counts, the Envoy launcher stops Envoy, only on TERM),
# give it 20 s, then stop_tree. Returns 0 only when it exited 0 by itself; 124 when it had to be killed.
stop_graceful() {
  local n=$1 pid=${PIDS[$1]:-} i rc=0; [[ -n $pid ]] || return 0
  "$ROUTER_BIN" signal -sig TERM "$pid" 2>/dev/null
  for i in $(seq 1 100); do alive "$pid" || break; sleep 0.2; done
  if alive "$pid"; then rc=124; stop_tree "$pid"; else wait "$pid" 2>/dev/null; rc=$?; fi
  unset "PIDS[$n]"; return $rc
}
sync_out() {
  cp -r "$RUN"/arms/. "$OUT/arms/" 2>/dev/null
  cp "$RUN"/placement-*.tsv "$RUN"/worker-*.json "$RUN"/kv-*.txt "$OUT/" 2>/dev/null
  cp "$RUN"/logs/*.log "$OUT/logs/" 2>/dev/null
  return 0
}
cleanup() { trap - EXIT; log "cleanup"; for n in "${!PIDS[@]}"; do stop_tree "${PIDS[$n]}"; done; sync_out; log "done"; }
trap cleanup EXIT
trap 'exit 1' TERM INT   # a `timeout` or scancel still runs the cleanup above

# poll <limit_s> <pid or ""> <command...>: run the command every POLL_S until it succeeds (0), the process died (2) or the limit passed (1).
poll() {
  local limit=$1 pid=$2 end; shift 2
  end=$(( $(date +%s) + limit ))
  while :; do
    "$@" && return 0
    [[ -z $pid ]] || alive "$pid" || return 2
    (( $(date +%s) < end )) || return 1
    sleep "$POLL_S"
  done
}
file_exists() { [[ -e $1 ]]; }

# ---- ports: the first free block at or above PORT_BASE, loopback only ----
port_free() { ! (exec 3<> "/dev/tcp/127.0.0.1/$1") 2>/dev/null; }
BLOCK=$(( NWORK + 2 )); (( BLOCK < 8 )) && BLOCK=8
BASE=""
for (( b = PORT_BASE; b < PORT_BASE + 40 * BLOCK; b += BLOCK )); do
  ok=1; for (( o = 0; o < BLOCK; o++ )); do port_free $(( b + o )) || { ok=0; break; }; done
  [[ $ok == 1 ]] && { BASE=$b; break; }
done
if [[ -z $BASE ]]; then echo "job.sh: no free block of $BLOCK ports at or above $PORT_BASE" >&2; exit 2; fi
RPORT=$BASE; VR_PROM=$(( BASE + 1 ))
relay_port() { echo $(( BASE + 2 + $1 )); }

# ---- environment record: no host name, path, job ID or account (a line that is not on the allow-list is not written) ----
safe_line() { grep -E -m1 -x '[A-Za-z0-9 ._,+()=-]{1,160}' || echo "unrecognised"; }
{
  ngpu=$(printf '%s' "${CUDA_VISIBLE_DEVICES:-}" | tr ',' '\n' | grep -c .)
  echo "mode=$MODE arch=$(uname -m) kernel=$(uname -r | safe_line) nproc=$(nproc) glibc=$(getconf GNU_LIBC_VERSION | awk '{print $2}')"
  echo "slurm_cpus=${SLURM_CPUS_PER_TASK:-} slurm_mem_per_node=${SLURM_MEM_PER_NODE:-} gpu_entries=$ngpu"
  if command -v nvidia-smi > /dev/null; then
    timeout 30 nvidia-smi --query-gpu=name,memory.total,driver_version --format=csv,noheader 2>/dev/null | sort | uniq -c | sed -E 's/^ *//; s/^/gpu: /' | safe_line
  else echo "gpu: no nvidia-smi"; fi
  echo "apptainer: $(timeout 20 "$APPTAINER" --version 2>/dev/null | safe_line)"
  envoy_bin=""; for f in "$STAGE"/envoy/*/envoy; do [[ -f $f ]] && { envoy_bin=$f; break; }; done
  if [[ -n $envoy_bin ]]; then
    echo "envoy_sha256=$(sha256sum "$envoy_bin" | awk '{print $1}')"
    echo "envoy: $(timeout 20 "$envoy_bin" --version 2>/dev/null | grep -o 'version: [^ ]*' | head -1 | grep -E -x 'version: [A-Za-z0-9._+-]+(/[A-Za-z0-9._+-]+)+' || echo unrecognised)"
  else echo "envoy_sha256=none staged"; fi
  # vllm-router 0.1.17 has no --version: ask the venv's own Python (a failure here never fails the job).
  echo "$(timeout 30 "$(dirname "$VR_BIN")/python" -I -c 'import importlib.metadata as m; print("vllm_router", m.version("vllm-router"))' 2>/dev/null | grep -E -x 'vllm_router [A-Za-z0-9._+-]+' || echo "vllm_router unknown")"
  echo "served=$SERVED nwork=$NWORK gpu_util=$GPU_UTIL max_len=$MAX_LEN max_num_seqs=$MAX_NUM_SEQS kv_blocks_b=$KV_BLOCKS_B kv_blocks_a=$KV_BLOCKS_A bound=$BOUND fault=${S2C_FAULT:-none}"
  echo "probe_hold=$PROBE_HOLD probe_stream_tokens=$PROBE_STREAM_TOKENS budget_s=$BUDGET_S job_limit_s=$JOB_LIMIT_S end_margin_s=$END_MARGIN_S arm_max_s=$ARM_MAX_S arm_max_s_h=$ARM_MAX_S_H"
} > "$OUT/env.txt"
log "env: $(tr '\n' ' ' < "$OUT/env.txt")"

# ---- the product smoke: agent.StartEnvoy unchanged, before any worker (it needs none). Exit 3 = refused. ----
# Envoy arms are skipped when it did not start; the vllm-router arms do not need it.
SMOKE_OK=1; SMOKE_WHY=""
arm_def() { # arm_def <arm>: the dispatch table. Sets ARM_ROUTER ARM_POLICY ARM_BOUND ARM_START; non-zero for an unknown arm.
  ARM_POLICY=""; ARM_BOUND=""
  case $1 in
  rr)            ARM_ROUTER=vllm-router; ARM_POLICY=round_robin;     ARM_START=start_vr ;;
  vr-hash)       ARM_ROUTER=vllm-router; ARM_POLICY=consistent_hash; ARM_START=start_vr ;;
  envoy-bound)   ARM_ROUTER=envoy;       ARM_BOUND=$BOUND;           ARM_START=start_envoy ;;
  envoy-nobound) ARM_ROUTER=envoy;       ARM_BOUND=none;             ARM_START=start_envoy ;;
  probe)         ARM_ROUTER=envoy;       ARM_BOUND=$BOUND;           ARM_START=run_probe ;;
  relay-control) ARM_ROUTER=relay-control;                            ARM_START=run_control_arm ;;
  *) return 1 ;;
  esac
}
need_smoke=0
for item in $PLAN; do IFS=: read -r _ _ a <<< "$item"; arm_def "$a" && [[ $ARM_ROUTER == envoy ]] && need_smoke=1; done
if (( need_smoke )); then
  mkdir -p "$OUT/product-start-dir"
  "$ROUTER_BIN" envoy -product-smoke -run-dir "$RUN" -workers "$NWORK" -stage-dir "$STAGE" -out "$OUT/product-start-dir" > "$OUT/logs/product-smoke.log" 2>&1
  src=$?
  [[ -f $OUT/product-start-dir/product-start.json ]] && cp "$OUT/product-start-dir/product-start.json" "$OUT/product-start.json"
  if [[ $src == 0 ]] && grep -q '"result": *"started"' "$OUT/product-start.json" 2>/dev/null; then
    log "product smoke: the product's own Envoy start works on this node"
  else
    SMOKE_OK=0
    if [[ $src == 3 ]]; then SMOKE_WHY="product smoke refused (the agent's own Envoy start failed on this node)"
    elif [[ $src == 0 ]]; then SMOKE_WHY="product smoke exited 0 without a started record"
    else SMOKE_WHY="product smoke failed (exit $src)"; fi
    log "$SMOKE_WHY; Envoy arms are skipped"
  fi
else
  log "product smoke: not run, PLAN has no Envoy arm"
fi

# ---- workers ----
REGIME=""
WORKERS_STARTED=0
kv_blocks() { case $1 in B | H) echo "$KV_BLOCKS_B" ;; A) echo "$KV_BLOCKS_A" ;; *) return 1 ;; esac; }
load_args() { case $1 in H) echo "$LOAD_ARGS_H" ;; *) echo "$LOAD_ARGS" ;; esac; }
arm_limit() { case $1 in H) echo "$ARM_MAX_S_H" ;; *) echo "$ARM_MAX_S" ;; esac; }   # the client's hard limit in a step
arm_est() { case $1 in H) echo "$ARM_EST_S_H" ;; *) echo "$ARM_EST_S" ;; esac; }
# arm_max <arm> <regime>: the sum of the limits job.sh enforces on the arm (its worker restart is added by run_arm)
arm_max() {
  local c; c=$(arm_limit "$2")
  case $1 in
  rr | vr-hash)  echo $(( RELAY_WAIT_S + VR_WAIT_S + c + DRAIN_WAIT_S + ARM_OVERHEAD_S )) ;;
  envoy-*)       echo $(( ENVOY_WAIT_S + c + DRAIN_WAIT_S + ARM_OVERHEAD_S )) ;;
  probe)         echo $(( ENVOY_WAIT_S + PROBE_MAX_S + WORKER_WAIT_S + ARM_OVERHEAD_S )) ;;
  relay-control) echo $(( RELAY_WAIT_S + 2 * (ARM_MAX_S + DRAIN_WAIT_S) + ARM_OVERHEAD_S )) ;;
  *)             echo $(( ARM_MAX_S + ARM_OVERHEAD_S )) ;;
  esac
}
uget() { curl -sf -m 10 --unix-socket "$RUN/worker-$1.sock" "http://localhost$2"; }
models_list_served() { "$PYTHON" -I -c 'import json, sys; sys.exit(0 if sys.argv[1] in [m["id"] for m in json.load(sys.stdin)["data"]] else 1)' "$SERVED"; }
worker_ready() { # /health answers 200 and /v1/models lists the served name
  [[ $(curl -s -m 10 -o /dev/null -w '%{http_code}' --unix-socket "$RUN/worker-$1.sock" http://localhost/health) == 200 ]] \
    && uget "$1" /v1/models | models_list_served
}
start_worker() { # start_worker <regime> <i> [<record suffix>]: launch worker i (s2crouter worker), PID in PIDS[w<i>]; no wait
  local r=$1 i=$2 sfx=${3:-} kv
  kv=$(kv_blocks "$r") || return 1
  rm -f "$RUN/worker-$i.sock"
  "$ROUTER_BIN" worker -index "$i" -workers "$NWORK" -apptainer "$APPTAINER" -image "$IMAGE" -checkpoint "$CHECKPOINT_DIR" \
    -run-dir "$RUN" -cache-dir "$CACHE" -served "$SERVED" -max-num-seqs "$MAX_NUM_SEQS" \
    -placement-log "/hpcburst/run/placement-$r-w$i.tsv" -record "$RUN/worker-$r-w$i$sfx.json" -- \
    --gpu-memory-utilization="$GPU_UTIL" --max-model-len="$MAX_LEN" --enable-prefix-caching --enable-prompt-tokens-details \
    --disable-uvicorn-access-log --middleware=s2c_placement.Placement --num-gpu-blocks-override="$kv" \
    >> "$RUN/logs/worker-$r-w$i.log" 2>&1 & PIDS[w$i]=$!
}
start_workers() { # start_workers <A|B|H>: all in parallel, each then waited for on its own socket
  local r=$1 i t=$(date +%s)
  kv_blocks "$r" > /dev/null || return 1
  for (( i = 0; i < NWORK; i++ )); do start_worker "$r" "$i"; done
  local limit=$WORKER_WAIT_S w rc left end
  (( WORKERS_STARTED == 0 )) && limit=$FIRST_WORKER_WAIT_S
  WORKERS_STARTED=1
  end=$(( t + limit ))
  for (( i = 0; i < NWORK; i++ )); do
    w=${PIDS[w$i]}
    left=$(( end - $(date +%s) )); (( left < 0 )) && left=0
    poll "$left" "$w" worker_ready "$i"; rc=$?
    if (( rc != 0 )); then
      log "worker $i ($r) $([[ $rc == 2 ]] && echo "exited" || echo "not ready after ${limit}s")"; tail -30 "$RUN/logs/worker-$r-w$i.log"
      return 1
    fi
    log "worker $i ($r) ready after $(( $(date +%s) - t ))s"
  done
  grep -h -E 'num_gpu_blocks|GPU KV cache size|Maximum concurrency|max_num_batched_tokens' "$RUN"/logs/worker-"$r"-w*.log 2>/dev/null \
    | sed -E 's/^.*(INFO|WARNING)[^]]*\] //' | sort | uniq -c > "$RUN/kv-$r.txt"
  REGIME=$r
}
stop_workers() { local i; for (( i = 0; i < NWORK; i++ )); do stop_named "w$i"; done; REGIME=""; }
workers_alive() { local i; for (( i = 0; i < NWORK; i++ )); do [[ -n ${PIDS[w$i]:-} ]] && alive "${PIDS[w$i]}" || return 1; done; }
scrape() { # scrape <dir> <before|after>: every worker's /metrics into <dir>/<tag>-w<i>.prom; non-zero if any is missing or empty
  local i rc=0
  for (( i = 0; i < NWORK; i++ )); do
    uget "$i" /metrics > "$1/$2-w$i.prom" && [[ -s $1/$2-w$i.prom ]] || rc=1
  done
  return $rc
}
running_total() { # the sum of vllm:num_requests_running over the workers; non-zero when a worker gives no such line
  local i v s=0
  for (( i = 0; i < NWORK; i++ )); do
    v=$(uget "$i" /metrics | awk '/^vllm:num_requests_running[{ ]/ { s += $NF; f = 1 } END { if (!f) exit 1; printf "%d\n", s }') || return 1
    s=$(( s + v ))
  done
  echo "$s"
}
drained() { local s; s=$(running_total) && [[ $s == 0 ]]; }
reset_all() { # reset_all <dir>: POST /reset_prefix_cache on every worker; each must say {"success":true} with HTTP 200
  local d=$1 i out ok=1 re='"success": *true'; : > "$d/reset.txt"
  for (( i = 0; i < NWORK; i++ )); do
    out=$(curl -s -m 30 --unix-socket "$RUN/worker-$i.sock" -X POST http://localhost/reset_prefix_cache -w ' http=%{http_code}')
    echo "w$i $out" >> "$d/reset.txt"
    [[ $out =~ $re && $out == *' http=200' ]] || ok=0
  done
  (( ok == 1 ))
}
sampler() { # samples of KV use, queues and cache counters, one line per worker, every SAMPLE_S
  local i
  while true; do
    for (( i = 0; i < NWORK; i++ )); do
      uget "$i" /metrics | grep -E '^vllm:(kv_cache_usage_perc|num_requests_running|num_requests_waiting|num_preemptions_total|prefix_cache_hits_total|prefix_cache_queries_total)' \
        | awk -v t="$(date +%s.%N)" -v w="$i" '{print t "\t" w "\t" $1 "\t" $NF}'
    done
    sleep "$SAMPLE_S"
  done
}

# ---- router stacks. Each start function sets CLIENT_ARGS and appends its counter collector to ACTIVE_COUNTERS. ----
CLIENT_ARGS=(); ACTIVE_COUNTERS=()
vr_ready() {
  [[ $(curl -s -m 10 -o /dev/null -w '%{http_code}' "http://127.0.0.1:$RPORT/health") == 200 ]] || return 1
  curl -sf -m 10 "http://127.0.0.1:$VR_PROM/metrics" | awk -v n="$NWORK" '/^vllm_router_active_workers([{ ])/ { if ($NF + 0 == n) f = 1 } END { exit !f }'
}
vr_stack() { # vr_stack <dir> <policy>: the relays, then vllm-router on them
  local d=$1 pol=$2 i p map="" urls=() rc
  : > "$d/relay-map.tsv"
  for (( i = 0; i < NWORK; i++ )); do
    p=$(relay_port "$i")
    map+="${map:+,}127.0.0.1:$p=$RUN/worker-$i.sock"; urls+=("http://127.0.0.1:$p"); printf '%s\t%s\n' "$p" "$i" >> "$d/relay-map.tsv"
  done
  rm -f "$d/relay-counts.tsv" "$d/relay-counts.tsv.ready"
  "$ROUTER_BIN" relay -map "$map" -counts "$d/relay-counts.tsv" > "$d/relay.log" 2>&1 & PIDS[relay]=$!
  poll "$RELAY_WAIT_S" "${PIDS[relay]}" file_exists "$d/relay-counts.tsv.ready" || { log "the relay did not become ready"; tail -5 "$d/relay.log"; return 1; }
  "$VR_BIN" --host 127.0.0.1 --port "$RPORT" --worker-urls "${urls[@]}" --policy "$pol" --request-timeout-secs 10800 \
    --prometheus-host 127.0.0.1 --prometheus-port "$VR_PROM" > "$d/router.log" 2>&1 & PIDS[router]=$!
  poll "$VR_WAIT_S" "${PIDS[router]}" vr_ready; rc=$?
  (( rc == 0 )) || { log "vllm-router did not become ready (poll status $rc)"; tail -5 "$d/router.log"; return 1; }
  CLIENT_ARGS=(-url "http://127.0.0.1:$RPORT/v1/chat/completions"); ACTIVE_COUNTERS+=(counters_vr)
}
counters_vr() { # counters_vr <dir> <before|after>
  curl -sf -m 10 "http://127.0.0.1:$VR_PROM/metrics" > "$1/vr-$2.prom" && [[ -s $1/vr-$2.prom ]]
}
start_vr() { vr_stack "$1" "$ARM_POLICY"; }
counters_envoy() { # counters_envoy <dir> <before|after>
  curl -sf -m 10 --unix-socket "$RUN/envoy-admin.sock" "http://localhost/clusters?format=json" > "$1/clusters-$2.json" && [[ -s $1/clusters-$2.json ]]
}
start_envoy() { # start_envoy <dir>: the agent's own Envoy configuration, ARM_BOUND
  local d=$1 extra=()
  rm -f "$RUN/router.sock" "$RUN/envoy-admin.sock"; mkdir -p "$d/envoy"
  [[ -f $OUT/product-start.json ]] && extra+=(-reference "$OUT/product-start.json")
  [[ $S2C_FAULT == strip-bound && $ARM_BOUND != none && -z ${ARM_VERIFY_DIR:-} ]] && extra+=(-fault strip-bound)
  if [[ -n ${ARM_VERIFY_DIR:-} ]]; then
    extra+=(-verify-dir "$ARM_VERIFY_DIR")
    [[ $S2C_FAULT == early-admit ]] && extra+=(-fault ignore-verify)
  fi
  "$ROUTER_BIN" envoy -run-dir "$RUN" -workers "$NWORK" -stage-dir "$STAGE" -bound "$ARM_BOUND" -out "$d/envoy" \
    ${extra[@]+"${extra[@]}"} > "$d/envoy.log" 2>&1 & PIDS[envoy]=$!
  poll "$ENVOY_WAIT_S" "${PIDS[envoy]}" file_exists "$d/envoy/READY" || { log "Envoy did not become ready"; tail -5 "$d/envoy.log"; return 1; }
  CLIENT_ARGS=(-unix "$RUN/router.sock" -url http://localhost/v1/chat/completions); ACTIVE_COUNTERS+=(counters_envoy)
  if [[ $S2C_FAULT == client-to-other-router && -z ${ARM_VERIFY_DIR:-} ]]; then vr_stack "$d" round_robin || return 1; fi   # the client now talks to the OTHER router
}
collect_counters() { # collect_counters <dir> <before|after>
  local c rc=0
  for c in ${ACTIVE_COUNTERS[@]+"${ACTIVE_COUNTERS[@]}"}; do "$c" "$1" "$2" || { log "$c $2 failed"; rc=1; }; done
  return $rc
}
stack_ps() { # stack_ps <dir>: CPU and memory of the router processes, before they stop
  local n
  for n in router relay envoy probe; do
    [[ -n ${PIDS[$n]:-} ]] && ps -o pid=,rss=,cputime=,comm= -p "$(descendants "${PIDS[$n]}" | paste -sd,)" 2>/dev/null | sed "s/^/$n /"
  done > "$1/ps.txt"
}
stop_stack() { # stop_stack <name> <check 0|1>: stops what the arm started; with check=1 a relay or Envoy launcher that did not exit 0 is a failure
  local name=$1 check=$2 n rc
  stop_named sampler router
  for n in relay envoy; do
    stop_graceful "$n"; rc=$?
    (( check == 1 && rc != 0 )) && fail "$name: $n did not stop cleanly (exit $rc)"
  done
  return 0
}
skip() { # skip <name> <dir> <why>: the arm is not run
  echo "skipped: $3" > "$2/SKIPPED"; fail "$1 skipped: $3"; sync_out
}
write_meta() { # write_meta <dir> ...: meta.json as CONTRACT.md lists it (numbers as numbers, null where the arm has none)
  local d=$1; shift
  "$PYTHON" -I -c '
import json, sys
(name, regime, rep, arm, router, policy, bound, kv, t1, t2, rc, nwork, load_args, mode) = sys.argv[1:]
print(json.dumps({"name": name, "regime": regime, "rep": int(rep), "arm": arm, "router": router,
    "policy": policy or None, "bound": bound or None, "kv_blocks": int(kv), "start_unix": float(t1), "end_unix": float(t2),
    "client_rc": int(rc), "nwork": int(nwork), "load_args": load_args, "mode": mode, "reset": "post"}, separators=(",", ":")))
' "$@" > "$d/meta.json"
}

# run_probe <regime> <name> <dir>: the dead-worker probe behind the bound Envoy arm. Envoy follows a verify flag per worker
# (s2crouter envoy -verify-dir); s2crouter probe kills the victim (the last worker) mid-stream and writes <dir>/killed;
# the job then restarts the victim, which the probe verifies itself (its engine, a hold, one direct request) before it flags
# it verified. The probe's exit status is its verdict (probe.json lists the checks).
probe_ended() { ! alive "${PIDS[probe]}"; }
run_probe() {
  local r=$1 name=$2 d=$3 v=$(( NWORK - 1 )) i rc old
  mkdir -p "$d/verify"
  for (( i = 0; i < NWORK; i++ )); do : > "$d/verify/worker-$i.verified"; done
  ARM_VERIFY_DIR=$d/verify
  start_envoy "$d"; rc=$?
  ARM_VERIFY_DIR=""
  (( rc == 0 )) || return 1
  old=${PIDS[w$v]}
  local deadline=$(( $(date +%s) + PROBE_MAX_S ))   # one deadline for the whole probe, the victim's return included
  "$ROUTER_BIN" probe -run-dir "$RUN" -workers "$NWORK" -victim "$v" -victim-pid "$old" -served "$SERVED" -verify-dir "$d/verify" \
    -out "$d" -arm "$name" -placement "$RUN/placement-$r-w$v.tsv" -hold "$PROBE_HOLD" -stream-tokens "$PROBE_STREAM_TOKENS" \
    -within "${PROBE_MAX_S}s" > "$d/probe.out" 2>&1 & PIDS[probe]=$!
  poll "$PROBE_MAX_S" "${PIDS[probe]}" file_exists "$d/killed"; rc=$?
  if (( rc == 0 )); then
    wait "$old" 2>/dev/null; unset "PIDS[w$v]"
    log "probe: worker $v killed; restarting it"
    start_worker "$r" "$v" "-restart"
  fi
  local left=$(( deadline - $(date +%s) )); (( left < 0 )) && left=0
  poll "$left" "" probe_ended || log "the probe did not end within ${PROBE_MAX_S}s of its start"
  if alive "${PIDS[probe]}"; then stop_named probe; PROBE_RC=124; else wait "${PIDS[probe]}"; PROBE_RC=$?; unset "PIDS[probe]"; fi
  return 0
}

run_arm() { # run_arm <regime> <rep> <arm>
  local r=$1 rep=$2 arm=$3 name=$1$2-$3 d rc t1 t2 kv used res
  d=$RUN/arms/$name; mkdir -p "$d"
  if ! arm_def "$arm"; then skip "$name" "$d" "unknown arm"; return; fi
  kv=$(kv_blocks "$r") || { skip "$name" "$d" "unknown regime $r"; return; }
  used=$(( $(date +%s) - T0 ))
  local est max; est=$(arm_est "$r"); [[ $arm == probe ]] && est=$PROBE_EST_S
  if (( used + est > BUDGET_S )); then log "SKIP $name: ${used}s used of ${BUDGET_S}s"; skip "$name" "$d" "budget"; return; fi
  max=$(arm_max "$arm" "$r")
  if [[ $REGIME != "$r" ]] || ! workers_alive; then
    if (( WORKERS_STARTED == 0 )); then max=$(( max + FIRST_WORKER_WAIT_S )); else max=$(( max + WORKER_WAIT_S )); fi
  fi
  if (( used + max + END_MARGIN_S > JOB_LIMIT_S )); then
    log "SKIP $name: its maximum of ${max}s from ${used}s does not end ${END_MARGIN_S}s before the ${JOB_LIMIT_S}s limit"
    skip "$name" "$d" "its maximum of ${max}s does not fit the job's remaining time"; return
  fi
  if [[ $ARM_ROUTER == envoy && $SMOKE_OK != 1 ]]; then skip "$name" "$d" "$SMOKE_WHY"; return; fi
  if [[ $REGIME == "$r" ]] && ! workers_alive; then log "a worker of regime $r died"; REGIME=""; fi
  if [[ $REGIME != "$r" ]]; then
    stop_workers
    start_workers "$r" || { stop_workers; skip "$name" "$d" "workers failed for regime $r"; return; }
  fi
  if [[ $arm == probe ]]; then run_probe_arm "$r" "$rep" "$name" "$d" "$kv"; return; fi
  if [[ $arm == relay-control ]]; then run_control_arm "$r" "$rep" "$name" "$d" "$kv"; return; fi
  # An arm that starts on a cache that was not reset would measure the previous arm's blocks: it is not run.
  reset_all "$d" || { skip "$name" "$d" "prefix-cache reset failed"; return; }
  scrape "$d" before || { skip "$name" "$d" "scrape before the arm failed"; return; }
  CLIENT_ARGS=(); ACTIVE_COUNTERS=()
  if ! "$ARM_START" "$d"; then
    stop_stack "$name" 0; skip "$name" "$d" "router stack did not start"; return
  fi
  collect_counters "$d" before || { stop_stack "$name" 0; skip "$name" "$d" "router counters before the arm failed"; return; }
  sampler > "$d/samples.tsv" 2>/dev/null & PIDS[sampler]=$!
  t1=$(date +%s.%N)
  log "RUN $name"
  # shellcheck disable=SC2086
  local largs; largs=$(load_args "$r")
  timeout "$(arm_limit "$r")" "$LOAD_BIN" "${CLIENT_ARGS[@]}" -model "$SERVED" -run "$name" -out "$d/client.jsonl" $largs > "$d/client.out" 2>&1
  rc=$?
  t2=$(date +%s.%N)
  stop_named sampler
  poll "$DRAIN_WAIT_S" "" drained || fail "$name: workers still had running requests after ${DRAIN_WAIT_S}s"
  scrape "$d" after || fail "$name: the metrics scrape after the arm failed"
  collect_counters "$d" after || fail "$name: the router counters after the arm failed"
  stack_ps "$d"
  stop_stack "$name" 1
  if [[ -f $d/relay-map.tsv && ! -s $d/relay-counts.tsv ]]; then fail "$name: the relay left no counts"; fi
  write_meta "$d" "$name" "$r" "$rep" "$arm" "$ARM_ROUTER" "$ARM_POLICY" "$ARM_BOUND" "$kv" "$t1" "$t2" "$rc" "$NWORK" "$largs" "$MODE" \
    || fail "$name: meta.json was not written"
  log "END $name rc=$rc: $(tail -1 "$d/client.out")"
  # The client must exit 0 and report its RESULT line with no failed attempt and no aborted session.
  res=$(grep -E '^RESULT ' "$d/client.out" | tail -1)
  if (( rc != 0 )); then fail "$name: client exit status $rc"
  elif [[ $res != *' failed_attempts=0 '* || $res != *' aborted_sessions=0' ]]; then fail "$name: client reported failures or no result"; fi
  sync_out
  # The check reads the results copy (placement logs sit beside arms/): from files only.
  "$PYTHON" -I "$STAGE/scripts/analyze.py" --check-arm "$OUT/arms/$name" > "$OUT/arms/$name/check.txt" 2>&1
  rc=$?
  if (( rc != 0 )); then fail "$name: analyze.py --check-arm exit $rc: $(grep '^FAIL' "$OUT/arms/$name/check.txt" | head -3 | paste -sd';')"; fi
}

# run_probe_arm <regime> <rep> <name> <dir> <kv>: the probe as an arm of its own (no reset, no client, no --check-arm: its
# verdict is the probe's exit status and probe.json). The victim's restarted worker must be ready afterwards, or the next
# arm restarts all of them.
run_probe_arm() {
  local r=$1 rep=$2 name=$3 d=$4 kv=$5 t1 t2 v=$(( NWORK - 1 ))
  CLIENT_ARGS=(); ACTIVE_COUNTERS=(); PROBE_RC=""
  t1=$(date +%s.%N)
  log "RUN $name (dead-worker probe, victim worker $v)"
  if ! run_probe "$r" "$name" "$d"; then
    stop_stack "$name" 0; skip "$name" "$d" "Envoy did not start for the probe"; return
  fi
  t2=$(date +%s.%N)
  stack_ps "$d"
  stop_stack "$name" 1
  write_meta "$d" "$name" "$r" "$rep" probe envoy "" "$BOUND" "$kv" "$t1" "$t2" "${PROBE_RC:-1}" "$NWORK" "" "$MODE" \
    || fail "$name: meta.json was not written"
  log "END $name probe rc=${PROBE_RC:-none}: $(grep -c '^ok ' "$d/probe.out" 2>/dev/null) ok, $(grep -c '^FAIL' "$d/probe.out" 2>/dev/null) failed"
  if [[ ${PROBE_RC:-1} != 0 ]]; then fail "$name: probe exit ${PROBE_RC:-none}: $(grep -E '^(FAIL|s2crouter)' "$d/probe.out" | head -3 | paste -sd';')"; fi
  # The restarted victim serves the next arm, or the next arm restarts every worker.
  if [[ -n ${PIDS[w$v]:-} ]] && poll "$WORKER_WAIT_S" "${PIDS[w$v]}" worker_ready "$v"; then log "worker $v ($r) ready again after the probe"
  else log "worker $v did not come back after the probe; the next arm restarts the workers"; REGIME=""; fi
  sync_out
}

# run_control_arm <regime> <rep> <name> <dir> <kv>: the relay's cost, measured. Worker 0 alone takes LOAD_ARGS_CONTROL twice,
# each after a cache reset and with its own run label: straight to its socket (direct), then through `s2crouter relay` to
# the same socket (relayed), as the vllm-router arms reach it. No router runs; the analysis compares the TTFTs.
run_control_arm() {
  local r=$1 rep=$2 name=$3 d=$4 kv=$5 path rc res t1 t2 args p0
  p0=$(relay_port 0)
  t1=$(date +%s.%N)
  log "RUN $name (the relay's cost on worker 0)"
  for path in direct relayed; do
    reset_all "$d" || { skip "$name" "$d" "prefix-cache reset failed"; return; }
    mv "$d/reset.txt" "$d/reset-$path.txt"
    if [[ $path == relayed ]]; then
      rm -f "$d/relay-counts.tsv" "$d/relay-counts.tsv.ready"
      "$ROUTER_BIN" relay -map "127.0.0.1:$p0=$RUN/worker-0.sock" -counts "$d/relay-counts.tsv" > "$d/relay.log" 2>&1 & PIDS[relay]=$!
      poll "$RELAY_WAIT_S" "${PIDS[relay]}" file_exists "$d/relay-counts.tsv.ready" || { stop_stack "$name" 0; skip "$name" "$d" "the relay did not become ready"; return; }
      args=(-url "http://127.0.0.1:$p0/v1/chat/completions")
    else
      args=(-unix "$RUN/worker-0.sock" -url http://localhost/v1/chat/completions)
    fi
    # shellcheck disable=SC2086
    timeout "$ARM_MAX_S" "$LOAD_BIN" "${args[@]}" -model "$SERVED" -run "$name-$path" -out "$d/client-$path.jsonl" $LOAD_ARGS_CONTROL > "$d/client-$path.out" 2>&1
    rc=$?
    res=$(grep -E '^RESULT ' "$d/client-$path.out" | tail -1)
    if (( rc != 0 )); then fail "$name: the $path client exit status $rc"
    elif [[ $res != *' failed_attempts=0 '* || $res != *' aborted_sessions=0' ]]; then fail "$name: the $path client reported failures or no result"; fi
    poll "$DRAIN_WAIT_S" "" drained || fail "$name: workers still had running requests after ${DRAIN_WAIT_S}s"
    [[ $path == relayed ]] && stop_stack "$name" 1
  done
  [[ -s $d/relay-counts.tsv ]] || fail "$name: the relay left no counts"
  t2=$(date +%s.%N)
  write_meta "$d" "$name" "$r" "$rep" relay-control relay-control "" "" "$kv" "$t1" "$t2" 0 "$NWORK" "$LOAD_ARGS_CONTROL" "$MODE" \
    || fail "$name: meta.json was not written"
  log "END $name"
  sync_out
}

for item in $PLAN; do
  IFS=: read -r r rep arm <<< "$item"
  if [[ ! $r =~ ^[ABH]$ || ! $rep =~ ^[0-9]+$ || -z $arm ]]; then fail "bad PLAN item '$item' (want <A|B|H>:<rep>:<arm>)"; continue; fi
  run_arm "$r" "$rep" "$arm"
done
stop_workers
sync_out
if (( ${#FAILED[@]} > 0 )); then
  log "FAILED with ${#FAILED[@]} problem(s), results kept in the results directory; see failures.txt"
  exit 1
fi
log "all arms done"
