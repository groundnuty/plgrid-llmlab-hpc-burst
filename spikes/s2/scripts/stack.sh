#!/usr/bin/env bash
# Start, stop and edit the router stacks of spike S2. Everything binds 127.0.0.1.
#
#   stack.sh start  <stack>          start the simulators and the router of a stack
#   stack.sh stop   <stack>          stop them by recorded PID (never by name)
#   stack.sh remove <stack> <index>  remove worker <index> from the router's discovery
#   stack.sh url    <stack>          print the router's chat completions URL
#   stack.sh metrics <stack>         print the workers' /metrics URLs, comma-separated
#   stack.sh kill-epp <stack>        SIGTERM the EPP of an llm-d stack (fault injection)
#   stack.sh start-epp <stack>       start it again
#   stack.sh restart-router <stack>  restart a vllm-router with its original arguments
#   stack.sh signal-epp <stack> STOP|CONT   freeze or thaw the EPP (fault injection)
#   stack.sh status [stack...]       show recorded PIDs and whether they are alive
#
# Stacks: name idx kind sims | envoy variant | epp variant | router options
#   none           0 none  1
#   llmd-shipped   1 llmd  3  shipped guide     (llm-d's files, ports only changed)
#   llmd-tuned     2 llmd  3  tuned   guide     (route/idle/message timeouts raised)
#   vllmr-default  3 vllmr 3  --request-timeout-secs left at its default
#   vllmr-tuned    4 vllmr 3  --request-timeout-secs 10800
#   llmd-affinity  5 llmd  3  tuned   session   (session-affinity-scorer on x-session-id)
#   vllmr-affinity 6 vllmr 3  --request-timeout-secs 10800, consistent_hash
#   llmd-guide     7 llmd  3  tuned   guide     (shipped scorers only: placement check)
#   llmd-probe     8 llmd  3  probe   guide     (5 s ext_proc timers)
#   llmd-removal  10 llmd  3  tuned   session   (same as llmd-affinity, a separate instance for the removal test)
#   vllmr-removal 11 vllmr 3  --request-timeout-secs 10800, consistent_hash (likewise)
#   llmd-ttft     12 llmd  3  shipped guide     (for the slow-first-token test; start with SIM_TTFT=1100s)
#   vllmr-ttft    13 vllmr 3  default timeouts (likewise)
#   vllmr-short    9 vllmr 3  --request-timeout-secs 60 (positive control: must cut a long stream)
#
# Environment: S2_HOME (required) holds bin/, src/, venv/ (made by setup.sh); runtime state goes
# to $S2_HOME/run/<stack>/. SIM_ITL is the simulators' inter-token latency (default 1s); EPP_V is the EPP log verbosity (default 2).
set -euo pipefail
S2_HOME=${S2_HOME:?set S2_HOME}
HERE=$(cd "$(dirname "$0")" && pwd)
SIM_ITL=${SIM_ITL:-1s}
SIM_TTFT=${SIM_TTFT:-1s}
MODEL=sim-model

declare -A IDX=([none]=0 [llmd-shipped]=1 [llmd-tuned]=2 [vllmr-default]=3 [vllmr-tuned]=4 [llmd-affinity]=5 [vllmr-affinity]=6 [llmd-guide]=7 [llmd-probe]=8 [vllmr-short]=9 [llmd-removal]=10 [vllmr-removal]=11 [llmd-ttft]=12 [vllmr-ttft]=13)
declare -A KIND=([none]=none [llmd-shipped]=llmd [llmd-tuned]=llmd [vllmr-default]=vllmr [vllmr-tuned]=vllmr [llmd-affinity]=llmd [vllmr-affinity]=vllmr [llmd-guide]=llmd [llmd-probe]=llmd [vllmr-short]=vllmr [llmd-removal]=llmd [vllmr-removal]=vllmr [llmd-ttft]=llmd [vllmr-ttft]=vllmr)
declare -A ENVOYV=([llmd-shipped]=shipped [llmd-tuned]=tuned [llmd-affinity]=tuned [llmd-guide]=tuned [llmd-probe]=probe [llmd-removal]=tuned [llmd-ttft]=shipped)
declare -A EPPV=([llmd-shipped]=guide [llmd-tuned]=guide [llmd-affinity]=session [llmd-guide]=guide [llmd-probe]=guide [llmd-removal]=session [llmd-ttft]=guide)

stack=${2:-}
[[ -n "$stack" && -n "${IDX[$stack]:-}" ]] || { [[ "${1:-}" == status && -z "$stack" ]] || { echo "unknown stack '$stack'" >&2; exit 64; }; }
i=${IDX[$stack]:-0}
RUN=$S2_HOME/run/$stack
NSIM=3; [[ "$stack" == none ]] && NSIM=1
sim_port() { if ((i < 10)); then echo $((18100 + i * 10 + $1)); else echo $((19800 + (i - 10) * 10 + $1)); fi; }
ROUTER_PORT=$((19200 + i))
ADMIN_PORT=$((19300 + i)); EPP_GRPC=$((19400 + i)); EPP_HEALTH=$((19500 + i)); EPP_METRICS=$((19600 + i)); VR_PROM=$((19700 + i))

spawn() { # spawn <name> <logfile> <cmd...> ; records the PID of the process itself
  local name=$1 log=$2; shift 2
  nohup nice -n 10 "$@" > "$log" 2>&1 < /dev/null &   # long-running servers run at nice 10 (shared VM)
  local pid=$!
  # "pid starttime": the start time (clock ticks since boot) guards against PID reuse, and a
  # command line check would fail for the vllm-router, which renames its process.
  echo "$pid $(awk '{print $22}' /proc/$pid/stat)" > "$RUN/$name.pid"
}
wait_http() { # wait_http <url> <seconds>
  for _ in $(seq 1 "$2"); do curl -sf -o /dev/null "$1" && return 0; sleep 1; done
  echo "timeout waiting for $1" >&2; return 1
}

start_epp() {
  # session-affinity-scorer is Alpha in llm-d-router v1.0.0 and the EPP refuses to start without this flag.
  local epp_extra=(); [[ "${EPPV[$stack]}" == session ]] && epp_extra+=(--allow-experimental-plugins)
  spawn epp "$RUN/epp.log" "$S2_HOME/bin/epp" --config-file="$RUN/epp-config.yaml" --pool-name=file-discovery --pool-namespace=default \
    --grpc-port="$EPP_GRPC" --grpc-health-port="$EPP_HEALTH" --metrics-port="$EPP_METRICS" --secure-serving=false --health-checking=true --tracing=false --v="${EPP_V:-2}" "${epp_extra[@]}"
}

start_router() {
  local urls=() extra=()
  for n in $(seq 0 $((NSIM - 1))); do urls+=("http://127.0.0.1:$(sim_port $n)"); done
  [[ "$stack" == vllmr-tuned || "$stack" == vllmr-affinity || "$stack" == vllmr-removal ]] && extra+=(--request-timeout-secs 10800)
  [[ "$stack" == vllmr-short ]] && extra+=(--request-timeout-secs 60)
  spawn router "$RUN/router.log" "$S2_HOME/venv/bin/python" "$S2_HOME/venv/bin/vllm-router" --host 127.0.0.1 --port "$ROUTER_PORT" \
    --worker-urls "${urls[@]}" --policy consistent_hash --prometheus-host 127.0.0.1 --prometheus-port "$VR_PROM" "${extra[@]}"
}

port_free() { ! (exec 3<> "/dev/tcp/127.0.0.1/$1") 2>/dev/null; }

cmd_start() {
  mkdir -p "$RUN"
  # A listener left over from an earlier stack would answer the health checks of this one and receive its traffic.
  local ports=("$ROUTER_PORT" "$ADMIN_PORT" "$EPP_GRPC" "$EPP_HEALTH" "$EPP_METRICS" "$VR_PROM") n p
  for n in $(seq 0 $((NSIM - 1))); do ports+=("$(sim_port $n)"); done
  for p in "${ports[@]}"; do port_free "$p" || { echo "port $p of stack $stack is already in use; refusing to start" >&2; return 1; }; done
  for n in $(seq 0 $((NSIM - 1))); do
    spawn "sim$n" "$RUN/sim$n.log" "$S2_HOME/bin/llm-d-inference-sim" --port "$(sim_port $n)" --model "$MODEL" \
      --max-model-len 16384 --max-num-seqs 64 --time-to-first-token "$SIM_TTFT" --inter-token-latency "$SIM_ITL"
  done
  for n in $(seq 0 $((NSIM - 1))); do wait_http "http://127.0.0.1:$(sim_port $n)/health" 60; done
  case ${KIND[$stack]} in
  none) ;;
  llmd)
    { echo "endpoints:"; for n in $(seq 0 $((NSIM - 1))); do
        printf '  - name: sim%d\n    address: 127.0.0.1\n    port: "%d"\n    labels:\n      model: %s\n' "$n" "$(sim_port $n)" "$MODEL"; done; } > "$RUN/endpoints.yaml"
    python3 -I "$HERE/genconf.py" "$S2_HOME/src" "$RUN" "${ENVOYV[$stack]}" "${EPPV[$stack]}" "$ROUTER_PORT" "$ADMIN_PORT" "$EPP_GRPC" "$EPP_HEALTH" "$EPP_METRICS" "$RUN/endpoints.yaml" "$RUN/envoy-access.log"
    start_epp
    sleep 3
    spawn envoy "$RUN/envoy.log" "$S2_HOME/bin/envoy-1.33.2" --service-node "envoy-$stack" --log-level warn --concurrency 2 \
      --drain-strategy immediate --drain-time-s 60 --disable-hot-restart -c "$RUN/envoy.yaml"
    wait_http "http://127.0.0.1:$ADMIN_PORT/ready" 60
    ;;
  vllmr)
    start_router
    wait_http "http://127.0.0.1:$ROUTER_PORT/health" 60
    ;;
  esac
  echo "started $stack: $(cmd_url)"
}

cmd_url() {
  if [[ ${KIND[$stack]} == none ]]; then echo "http://127.0.0.1:$(sim_port 0)/v1/chat/completions"
  else echo "http://127.0.0.1:$ROUTER_PORT/v1/chat/completions"; fi
}
cmd_metrics() { local o=(); for n in $(seq 0 $((NSIM - 1))); do o+=("http://127.0.0.1:$(sim_port $n)/metrics"); done; (IFS=,; echo "${o[*]}"); }

cmd_remove() {
  local x=${3:?index}
  case ${KIND[$stack]} in
  llmd) # atomic rewrite: the EPP reloads on rename
    python3 -I - "$RUN/endpoints.yaml" "$x" <<'PY'
import os, re, sys
path, x = sys.argv[1], sys.argv[2]
text = open(path).read()
head, *items = re.split(r"(?m)^  - name: ", text)
keep = [it for it in items if not it.startswith(f"sim{x}\n")]
assert len(keep) == len(items) - 1, "worker not found in endpoints file"
open(path + ".tmp", "w").write(head + "".join("  - name: " + it for it in keep))
os.rename(path + ".tmp", path)
PY
    ;;
  vllmr)
    curl -sf -X POST "http://127.0.0.1:$ROUTER_PORT/remove_worker?url=http://127.0.0.1:$(sim_port "$x")" ;;
  *) echo "remove: stack has no router" >&2; return 64 ;;
  esac
  echo "removed worker $x from $stack at $(date -u +%T)"
}

stop_pid() { # stop_pid <pidfile> : SIGTERM the recorded PID if it is still one of ours
  local f=$1 pid st
  [[ -f $f ]] || return 0
  read -r pid st < "$f"
  if [[ -r /proc/$pid/stat && $(awk '{print $22}' "/proc/$pid/stat") == "$st" ]]; then
    kill "$pid" 2>/dev/null || true
    for _ in $(seq 1 20); do [[ -d /proc/$pid ]] || break; sleep 0.5; done
    [[ -d /proc/$pid ]] && kill -9 "$pid" 2>/dev/null || true
  fi
  rm -f "$f"
}
cmd_stop() { for f in "$RUN"/*.pid; do [[ -e $f ]] && stop_pid "$f"; done; echo "stopped $stack"; }

cmd_status() {
  for s in ${@:-${!IDX[@]}}; do
    for f in "$S2_HOME/run/$s"/*.pid; do
      [[ -e $f ]] || continue
      read -r pid st < "$f"; alive=dead
      [[ -r /proc/$pid/stat && $(awk '{print $22}' "/proc/$pid/stat") == "$st" ]] && alive=alive
      echo "$s $(basename "$f" .pid) pid=$pid $alive"
    done
  done
}

case ${1:-} in
kill-epp) stop_pid "$RUN/epp.pid"; echo "EPP of $stack stopped at $(date -u +%T)" ;;
restart-router) stop_pid "$RUN/router.pid"; start_router; wait_http "http://127.0.0.1:$ROUTER_PORT/health" 60; echo "router of $stack restarted at $(date -u +%T)" ;;
signal-epp) read -r pid st < "$RUN/epp.pid"; [[ $(awk '{print $22}' "/proc/$pid/stat") == "$st" ]] && kill -"${3:?STOP|CONT}" "$pid"; echo "EPP of $stack: SIG${3} at $(date -u +%T)" ;;
start-epp) start_epp; echo "EPP of $stack started at $(date -u +%T)" ;;
start) cmd_start ;; stop) cmd_stop ;; remove) cmd_remove "$@" ;; url) cmd_url ;; metrics) cmd_metrics ;;
status) shift; cmd_status "$@" ;;
*) sed -n 2,12p "$0"; exit 64 ;;
esac
