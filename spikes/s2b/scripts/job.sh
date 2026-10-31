#!/bin/bash -l
# Spike S2 phase B: compare routers inside one Instance on real vLLM, all inside one Slurm job.
#
# Starts NWORK vLLM workers of a small model (on the job's GPU or GPUs), then, for each arm of PLAN,
# starts one router stack on loopback, runs the s2bload workload through it, and records:
#   - each worker's /metrics before and after the arm (prefix-cache hits and queries, preemptions);
#   - a 2 s sample of KV use and queue depth per worker during the arm;
#   - the client's per-request record (TTFT, prompt and cached tokens);
#   - placement: which worker served which session turn (the s2b_placement middleware in each worker).
# Between arms every worker's prefix cache is reset (/reset_prefix_cache, VLLM_SERVER_DEV_MODE=1),
# and the run label salts the start of every system prompt, so no arm can reuse another arm's blocks.
#
# Arms:  rr          vllm-router --policy round_robin                     (baseline)
#        vr-hash     vllm-router --policy consistent_hash (x-session-id)
#        vr-cache    vllm-router --policy cache_aware, thresholds 0.8 / 1 / 1.1
#        llmd-sess   llm-d EPP + Envoy: shipped scorers + session-affinity-scorer on x-session-id (Alpha)
#        llmd-prefix llm-d EPP + Envoy: shipped scorers only (prefix, queue, KV, no-hit-LRU)
# Regimes: A = KV cache as profiled (ample for this model); B = KV capped at KV_BLOCKS_B blocks per worker.
#
# Environment (required): S2B_HOME (stage/, hf_home/, results/). MODE=vllm needs VLLM_VENV.
# MODE=sim replaces vLLM with llm-d-inference-sim (SIM_BIN) for a rehearsal without a GPU.
# The job ends by itself after the last arm; that is its scancel.
# Exit status: 0 only if every arm in PLAN ran and was measured. A skipped arm (budget, workers, router, llm-d
# smoke test), a failed prefix-cache reset, a failed metrics scrape, or a client that failed or did not report
# makes the final status 1. Each is listed in failures.txt next to the results, which stay in place.
set -uo pipefail
S2B_HOME=${S2B_HOME:?set S2B_HOME}
MODE=${MODE:-vllm}
STAGE=${STAGE:-$S2B_HOME/stage}
NWORK=${NWORK:-4}
MODEL_ID=${MODEL_ID:-Qwen/Qwen2.5-1.5B-Instruct}
# The revision the results were taken with (vLLM 0.32.0 uses it for the tokenizer too: tokenizer_revision
# defaults to revision). Passed to every worker, so another cached revision cannot be picked by name.
MODEL_REV=${MODEL_REV:-989aa7980e4cf806f80c7fef2b1adb7bc71aa306}
SERVED=s2b-model
GPU_UTIL=${GPU_UTIL:-0.2}
KV_BLOCKS_B=${KV_BLOCKS_B:-6000}
MAX_LEN=${MAX_LEN:-16384}
PLAN=${PLAN:-"B:1:rr B:1:vr-hash B:1:llmd-sess B:1:llmd-prefix B:1:vr-cache B:2:llmd-sess B:2:vr-hash B:2:rr A:1:rr A:1:vr-hash A:1:llmd-sess A:1:llmd-prefix A:1:vr-cache"}
LOAD_ARGS=${LOAD_ARGS:-"-sessions 96 -turns 12 -concurrency 32 -agents 0.5,0.3,0.2 -sys-words 2000 -task-words 250 -tool-min 200 -tool-max 800 -think-min 500ms -think-max 2500ms -reply-tokens 48 -seed 1"}
BUDGET_S=${BUDGET_S:-4800}     # stop starting arms when this much job time is used
ARM_EST_S=${ARM_EST_S:-360}    # time one arm is expected to need, for the guard
ARM_MAX_S=${ARM_MAX_S:-720}    # hard limit on one arm's client
WORKER_WAIT_S=${WORKER_WAIT_S:-600}
EPP_BIN=${EPP_BIN:-$STAGE/bin/epp}
ENVOY_BIN=${ENVOY_BIN:-$STAGE/bin/envoy}
LOAD_BIN=${LOAD_BIN:-$STAGE/bin/s2bload}
GENCONF=${GENCONF:-$STAGE/genconf.py}
GUIDE_SRC=${GUIDE_SRC:-$STAGE/src}   # holds llm-d/guides/no-kubernetes-deployment/router/
T0=$(date +%s)
RUN=${MEMFS:-${TMPDIR:-/tmp}}/s2b-run
OUT=$S2B_HOME/results/${RESULT_TAG:-run-$(date -u +%Y%m%dT%H%MZ)}
mkdir -p "$RUN"/{logs,tmp,home,cache,arms} "$OUT/arms"
log() { echo "[$(date -u +%T) +$(( $(date +%s) - T0 ))s] $*"; }
FAILED=()   # one line per problem; any entry makes the final exit status 1
fail() { FAILED+=("$1"); log "FAIL: $1"; }

# ---- processes: recorded by PID, stopped by PID (with descendants), never by name ----
declare -A PIDS=()
descendants() { local c; echo "$1"; for c in $(ps -o pid= --ppid "$1" 2>/dev/null); do descendants "$c"; done; }
stop_tree() { # stop_tree <pid>: TERM the tree, KILL what is left after 15 s
  local pid=$1 all p; [[ -n $pid ]] || return 0
  all=$(descendants "$pid" | tr '\n' ' '); kill $all 2>/dev/null
  for _ in $(seq 1 30); do kill -0 "$pid" 2>/dev/null || break; sleep 0.5; done
  for p in $all; do kill -9 "$p" 2>/dev/null; done; wait "$pid" 2>/dev/null; return 0
}
stop_named() { local n; for n in "$@"; do [[ -n ${PIDS[$n]:-} ]] && stop_tree "${PIDS[$n]}"; unset "PIDS[$n]"; done; }
cleanup() { log "cleanup"; for n in "${!PIDS[@]}"; do stop_tree "${PIDS[$n]}"; done; sync_out; log "done"; }
sync_out() { cp -r "$RUN"/arms/. "$OUT/arms/" 2>/dev/null; cp "$RUN"/placement-*.tsv "$RUN"/*.txt "$OUT/" 2>/dev/null; return 0; }
trap cleanup EXIT

port_free() { ! (exec 3<> "/dev/tcp/127.0.0.1/$1") 2>/dev/null; }
wait_http() { local i; for i in $(seq 1 "$2"); do curl -sf -o /dev/null "$1" && return 0; sleep 1; done; return 1; }
for BASE in $(seq 31000 500 39000); do
  ok=1; for o in $(seq 0 16); do port_free $((BASE + o)) || { ok=0; break; }; done
  [[ $ok == 1 ]] && break
done
[[ $ok == 1 ]] || { log "no free port block"; exit 2; }
wport() { echo $((BASE + 1 + $1)); }
RPORT=$((BASE + 10)); ADMIN=$((BASE + 11)); EPP_GRPC=$((BASE + 12)); EPP_HEALTH=$((BASE + 13)); EPP_METRICS=$((BASE + 14)); VR_PROM=$((BASE + 15))

# listeners <out-file> <enforce 0|1> <name...>: list every TCP listener of the named process trees
# (read from /proc by s2b_listeners.py); with enforce=1 any listener off loopback fails the call,
# and so does a tree with no listener at all (what cannot be seen is not a pass).
listeners() {
  local out=$1 enforce=$2 bad=0 n pid addr found; shift 2
  for n in "$@"; do
    pid=${PIDS[$n]:-}; [[ -n $pid ]] || continue
    found=0
    while read -r _ addr; do
      [[ -n $addr ]] || continue
      found=1; echo "$n $addr" >> "$out"
      case $addr in 127.0.0.1:* | "[::1]:"*) ;; *) [[ $enforce == 1 ]] && bad=1 ;; esac
    done < <(python3 -I "$STAGE/py/s2b_listeners.py" $(descendants "$pid"))
    [[ $found == 1 ]] || { echo "$n no-listener-seen" >> "$out"; [[ $enforce == 1 ]] && bad=1; }
  done
  return $bad
}

# ---- environment record (no host names, no paths) ----
{
  echo "mode=$MODE arch=$(uname -m) kernel=$(uname -r) pagesize=$(getconf PAGESIZE) nproc=$(nproc)"
  echo "model=$MODEL_ID revision=$MODEL_REV"
  echo "slurm_cpus=${SLURM_CPUS_PER_TASK:-} slurm_mem_per_node=${SLURM_MEM_PER_NODE:-} gpus=${CUDA_VISIBLE_DEVICES:-}"
  command -v nvidia-smi > /dev/null && nvidia-smi --query-gpu=name,memory.total,driver_version,compute_cap --format=csv,noheader
} > "$RUN/env.txt"

if [[ $MODE == vllm ]]; then
  module load ML-bundle/25.10 > "$RUN/module.log" 2>&1 || { log "module load failed"; cat "$RUN/module.log"; }
  VLLM_VENV=${VLLM_VENV:?set VLLM_VENV}
  source "$VLLM_VENV/bin/activate"
  VR_CMD=${VR_CMD:-$VLLM_VENV/bin/vllm-router}
  export HOME=$RUN/home XDG_CACHE_HOME=$RUN/cache HF_HOME=$S2B_HOME/hf_home HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1
  export VLLM_CACHE_ROOT=$RUN/cache/vllm TRITON_CACHE_DIR=$RUN/cache/triton TORCHINDUCTOR_CACHE_DIR=$RUN/cache/inductor
  export FLASHINFER_WORKSPACE_BASE=$RUN/cache/flashinfer CUTE_DSL_CACHE_DIR=$RUN/cache/cutedsl
  export VLLM_NO_USAGE_STATS=1 DO_NOT_TRACK=1 PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 OMP_NUM_THREADS=8 MAX_JOBS=8 TORCHINDUCTOR_COMPILE_THREADS=8
  export VLLM_USE_FLASHINFER_SAMPLER=0 FLASHINFER_DISABLE_VERSION_CHECK=1   # as in the www-2027 Helios env for this build
  export VLLM_SERVER_DEV_MODE=1 VLLM_HOST_IP=127.0.0.1 TMPDIR=$RUN/tmp PYTHONPATH=$STAGE/py
  python -c 'import vllm, torch, importlib.metadata as m; print("vllm", vllm.__version__, "torch", torch.__version__, "cuda", torch.version.cuda, "vllm_router", m.version("vllm_router"))' >> "$RUN/env.txt" 2>&1
else
  VR_CMD=${VR_CMD:?set VR_CMD in sim mode}
  SIM_BIN=${SIM_BIN:?set SIM_BIN in sim mode}
fi
"$ENVOY_BIN" --version 2>&1 | grep -o 'version: [^ ]*' | head -1 >> "$RUN/env.txt"
log "env: $(tr '\n' ' ' < "$RUN/env.txt")"

# ---- llm-d stack (EPP + Envoy), configs generated from llm-d's shipped guide files ----
llmd_conf() { # llmd_conf <dir> <epp variant guide|session>
  local d=$1 i; mkdir -p "$d"
  { echo "endpoints:"; for i in $(seq 0 $((NWORK - 1))); do
      printf '  - name: w%d\n    address: 127.0.0.1\n    port: "%d"\n    labels:\n      model: %s\n' "$i" "$(wport $i)" "$SERVED"; done; } > "$d/endpoints.yaml"
  python3 -I "$GENCONF" "$GUIDE_SRC" "$d" tuned "$2" "$RPORT" "$ADMIN" "$EPP_GRPC" "$EPP_HEALTH" "$EPP_METRICS" "$d/endpoints.yaml" "$d/envoy-access.log" > /dev/null
}
start_llmd() { # start_llmd <dir> <variant>
  local d=$1 extra=(); [[ $2 == session ]] && extra+=(--allow-experimental-plugins)
  llmd_conf "$d" "$2" || return 1
  "$EPP_BIN" --config-file="$d/epp-config.yaml" --pool-name=file-discovery --pool-namespace=default --grpc-port="$EPP_GRPC" \
    --grpc-health-port="$EPP_HEALTH" --metrics-port="$EPP_METRICS" --secure-serving=false --health-checking=true --tracing=false --v=2 "${extra[@]}" \
    > "$d/epp.log" 2>&1 & PIDS[epp]=$!
  sleep 3
  "$ENVOY_BIN" --service-node envoy-s2b --log-level warn --concurrency 4 --drain-strategy immediate --drain-time-s 60 \
    --disable-hot-restart -c "$d/envoy.yaml" > "$d/envoy.log" 2>&1 & PIDS[envoy]=$!
  wait_http "http://127.0.0.1:$ADMIN/ready" 60
}

# Smoke test before any GPU work: these binaries have never run on this node type.
LLMD_OK=1
smoke=$RUN/smoke; mkdir -p "$smoke"
if llmd_conf "$smoke" session && "$ENVOY_BIN" --mode validate -c "$smoke/envoy.yaml" > "$smoke/validate.log" 2>&1 \
   && start_llmd "$smoke" session && listeners "$RUN/listeners-smoke.txt" 1 epp envoy; then
  log "llm-d smoke: EPP and Envoy started and listen on loopback only"
else
  LLMD_OK=0; log "llm-d smoke FAILED; llm-d arms will be skipped. validate: $(tail -3 "$smoke/validate.log" 2>/dev/null | tr '\n' ' ')"
  tail -20 "$smoke/envoy.log" "$smoke/epp.log" 2>/dev/null
fi
stop_named envoy epp
cp -r "$smoke" "$OUT/" 2>/dev/null

# ---- workers ----
REGIME=""
start_workers() { # start_workers <A|B>: one at a time, so each profiles its memory alone
  local r=$1 i kv=() t
  [[ $r == B ]] && kv=(--num-gpu-blocks-override "$KV_BLOCKS_B")
  for i in $(seq 0 $((NWORK - 1))); do
    t=$(date +%s)
    if [[ $MODE == vllm ]]; then
      S2B_WORKER=$i S2B_PLACEMENT_LOG=$RUN/placement-$r-w$i.tsv vllm serve "$MODEL_ID" --revision "$MODEL_REV" --served-model-name "$SERVED" \
        --host 127.0.0.1 --port "$(wport $i)" --gpu-memory-utilization "$GPU_UTIL" --max-model-len "$MAX_LEN" \
        --enable-prefix-caching --enable-prompt-tokens-details --disable-uvicorn-access-log \
        --middleware s2b_placement.Placement "${kv[@]}" > "$RUN/logs/worker$i-$r.log" 2>&1 & PIDS[w$i]=$!
    else
      "$SIM_BIN" --port "$(wport $i)" --model "$SERVED" --max-model-len "$MAX_LEN" --max-num-seqs 256 \
        --time-to-first-token 20ms --inter-token-latency 2ms > "$RUN/logs/worker$i-$r.log" 2>&1 & PIDS[w$i]=$!
    fi
    if ! wait_http "http://127.0.0.1:$(wport $i)/health" "$WORKER_WAIT_S"; then
      log "worker $i ($r) not healthy after ${WORKER_WAIT_S}s"; tail -30 "$RUN/logs/worker$i-$r.log"; return 1
    fi
    log "worker $i ($r) healthy after $(( $(date +%s) - t ))s"
  done
  listeners "$RUN/listeners-workers-$r.txt" 0 $(for i in $(seq 0 $((NWORK - 1))); do echo w$i; done)
  grep -h -E 'num_gpu_blocks|GPU KV cache size|Maximum concurrency|max_num_batched_tokens' "$RUN"/logs/worker*-"$r".log | sed -E 's/^.*(INFO|WARNING)[^]]*\] //' | sort | uniq -c > "$RUN/kv-$r.txt"
  REGIME=$r
}
stop_workers() { local i; for i in $(seq 0 $((NWORK - 1))); do stop_named w$i; done; REGIME=""; }
scrape() { # scrape <prefix>: every worker's /metrics into <prefix>-w<i>.prom; non-zero if any is missing or empty
  local i rc=0
  for i in $(seq 0 $((NWORK - 1))); do
    curl -sf -m 10 "http://127.0.0.1:$(wport $i)/metrics" > "$1-w$i.prom" && [[ -s $1-w$i.prom ]] || rc=1
  done
  return $rc
}
sampler() { # 2 s samples of KV use, queues and cache counters, one line per worker
  local i
  while true; do
    for i in $(seq 0 $((NWORK - 1))); do
      curl -sf -m 2 "http://127.0.0.1:$(wport $i)/metrics" | grep -E '^vllm:(kv_cache_usage_perc|num_requests_running|num_requests_waiting|num_preemptions_total|prefix_cache_hits_total|prefix_cache_queries_total)' \
        | awk -v t="$(date +%s.%N)" -v w="$i" '{print t "\t" w "\t" $1 "\t" $NF}'
    done
    sleep 2
  done
}

# ---- routers ----
start_router() { # start_router <arm> <dir>
  local arm=$1 d=$2 urls=() i pol=()
  for i in $(seq 0 $((NWORK - 1))); do urls+=("http://127.0.0.1:$(wport $i)"); done
  case $arm in
  rr) pol=(--policy round_robin) ;;
  vr-hash) pol=(--policy consistent_hash) ;;
  vr-cache) pol=(--policy cache_aware --cache-threshold 0.8 --balance-abs-threshold 1 --balance-rel-threshold 1.1) ;;
  llmd-sess | llmd-prefix)
    [[ $LLMD_OK == 1 ]] || return 3
    local v=session; [[ $arm == llmd-prefix ]] && v=guide
    start_llmd "$d" "$v" || return 1
    listeners "$d/listeners.txt" 1 epp envoy || return 2
    return 0 ;;
  *) return 4 ;;
  esac
  $VR_CMD --host 127.0.0.1 --port "$RPORT" --worker-urls "${urls[@]}" "${pol[@]}" --request-timeout-secs 10800 \
    --prometheus-host 127.0.0.1 --prometheus-port "$VR_PROM" > "$d/router.log" 2>&1 & PIDS[router]=$!
  wait_http "http://127.0.0.1:$RPORT/health" 120 || return 1
  listeners "$d/listeners.txt" 1 router || return 2
}

run_arm() { # run_arm <regime> <rep> <arm>
  local r=$1 rep=$2 arm=$3 name=$1$2-$3 d rc i
  d=$RUN/arms/$name; mkdir -p "$d"
  local used=$(( $(date +%s) - T0 ))
  if (( used + ARM_EST_S > BUDGET_S )); then
    log "SKIP $name: ${used}s used of ${BUDGET_S}s"; echo "skipped: budget" > "$d/SKIPPED"; fail "$name skipped: budget"; return
  fi
  if [[ $REGIME != "$r" ]]; then
    stop_workers
    start_workers "$r" || {
      log "SKIP $name: workers failed for regime $r"; echo "skipped: workers" > "$d/SKIPPED"; fail "$name skipped: workers"; stop_workers; return
    }
  fi
  # An arm that starts on a cache that was not reset would measure the previous arm's blocks: it is not run.
  local reset_ok=1 out
  for i in $(seq 0 $((NWORK - 1))); do
    out=$(curl -s -m 30 -X POST "http://127.0.0.1:$(wport $i)/reset_prefix_cache" -w ' http=%{http_code}')
    echo "w$i $out" >> "$d/reset.txt"
    [[ $out == *'"success":true'* && $out == *' http=200' ]] || reset_ok=0
  done
  if [[ $reset_ok != 1 ]]; then
    log "SKIP $name: a prefix-cache reset failed"; echo "skipped: reset failed" > "$d/SKIPPED"; fail "$name skipped: prefix-cache reset failed"; return
  fi
  sleep 1
  if ! scrape "$d/before"; then
    log "SKIP $name: the metrics scrape before the arm failed"; echo "skipped: scrape failed" > "$d/SKIPPED"; fail "$name skipped: scrape before the arm failed"; return
  fi
  start_router "$arm" "$d"; rc=$?
  if [[ $rc != 0 ]]; then
    log "SKIP $name: router failed to start (rc $rc)"; echo "skipped: router rc $rc" > "$d/SKIPPED"; fail "$name skipped: router rc $rc"
    stop_named router envoy epp; sync_out; return
  fi
  sampler > "$d/samples.tsv" 2>/dev/null & PIDS[sampler]=$!
  local t1; t1=$(date +%s.%N)
  log "RUN $name"
  # shellcheck disable=SC2086
  timeout "$ARM_MAX_S" "$LOAD_BIN" -url "http://127.0.0.1:$RPORT/v1/chat/completions" -model "$SERVED" -run "$name" -out "$d/client.jsonl" $LOAD_ARGS > "$d/client.out" 2>&1
  rc=$?
  local t2; t2=$(date +%s.%N)
  stop_named sampler
  sleep 2; scrape "$d/after" || fail "$name: the metrics scrape after the arm failed"
  for n in router envoy epp; do [[ -n ${PIDS[$n]:-} ]] && ps -o pid=,rss=,cputime=,comm= -p "$(descendants "${PIDS[$n]}" | paste -sd,)" | sed "s/^/$n /"; done > "$d/ps.txt" 2>/dev/null
  stop_named router envoy epp
  printf '{"name":"%s","regime":"%s","rep":%s,"arm":"%s","kv_blocks_override":"%s","start_unix":%s,"end_unix":%s,"client_rc":%s,"nwork":%s,"load_args":"%s"}\n' \
    "$name" "$r" "$rep" "$arm" "$([[ $r == B ]] && echo "$KV_BLOCKS_B")" "$t1" "$t2" "$rc" "$NWORK" "$LOAD_ARGS" > "$d/meta.json"
  [[ -f $d/epp.log ]] && gzip -f "$d/epp.log"
  log "END $name rc=$rc: $(tail -1 "$d/client.out")"
  # The client must exit 0 and report its RESULT line with no failed attempt and no aborted session.
  local res; res=$(grep -E '^RESULT ' "$d/client.out" | tail -1)
  if (( rc != 0 )); then fail "$name: client exit status $rc"
  elif [[ $res != *' failed_attempts=0 '* || $res != *' aborted_sessions=0' ]]; then fail "$name: client reported failures or no result"; fi
  sync_out
}

for item in $PLAN; do IFS=: read -r r rep arm <<< "$item"; run_arm "$r" "$rep" "$arm"; done
stop_workers
cp "$RUN"/logs/worker*.log "$OUT/" 2>/dev/null
if (( ${#FAILED[@]} > 0 )); then
  printf '%s\n' "${FAILED[@]}" > "$RUN/failures.txt"; sync_out
  log "FAILED with ${#FAILED[@]} problem(s), results kept in $OUT; see failures.txt"
  exit 1
fi
log "all arms done; results in $OUT"
