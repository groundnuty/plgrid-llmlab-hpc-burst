#!/usr/bin/env bash
# One command per scenario of spike S2 phase A. Needs S2_HOME prepared by setup.sh.
#
#   run.sh long       2 h streams (7300 tokens at 1 token/s) through: no router, llm-d as shipped,
#                     llm-d tuned, vllm-router default (1,800 s) and vllm-router tuned. ~2 h 5 min.
#   run.sh controls   positive controls: vllm-router with a 60 s deadline must cut a 150 s stream;
#                     llm-d with 5 s ext_proc timers streams 600 s anyway. ~11 min.
#   run.sh placement  multi-turn sessions through each router: where do turns land. ~3 min each.
#   run.sh removal    remove a worker from discovery under an in-flight 25 min stream. ~27 min each.
#   run.sh summary    table of every stream result found under $S2_HOME/results
#
# Exit status is non-zero if any scenario's expectation is not met. Results: $S2_HOME/results/<scenario>/.
set -uo pipefail
S2_HOME=${S2_HOME:?set S2_HOME}; export S2_HOME
HERE=$(cd "$(dirname "$0")" && pwd)
SC=$HERE/stack.sh
P=$S2_HOME/bin/s2probe
LONG_TOKENS=${LONG_TOKENS:-7300}
fail=0

capacity_gate() { # the VM is shared: wait until load <= 4.0 and >= 8 GB available
  while true; do
    l=$(awk '{print $1}' /proc/loadavg); m=$(awk '/MemAvailable/{printf "%d", $2/1048576}' /proc/meminfo)
    awk -v l="$l" -v m="$m" 'BEGIN{exit !(l<=4.0 && m>=8)}' && return 0
    echo "$(date -u +%T) waiting: load=$l mem_avail_gb=$m"; sleep 60
  done
}
fresh() { "$SC" stop "$1" >/dev/null 2>&1; rm -rf "$S2_HOME/run/$1"; "$SC" start "$1"; }

# expect <label> <want: pass|cut> <cut-after-lo> <cut-after-hi> : judge a finished stream's JSON against the stated expectation
expect() {
  python3 -I - "$1" "$2" "$3" "$4" "$S2_HOME/results/$5/$1.json" <<'PY'
import json, sys
label, want, lo, hi, path = sys.argv[1:6]
r = json.load(open(path))
if want == "pass":
    ok = r["complete"]
else:
    ok = (not r["complete"]) and float(lo) <= r["elapsed_sec"] <= float(hi)
print(f"{'OK  ' if ok else 'FAIL'} {label}: expected {want}" + (f" in [{lo},{hi}] s" if want == "cut" else "") +
      f"; got complete={r['complete']} tokens={r['tokens']}/{r['expect_tokens']} elapsed={r['elapsed_sec']:.0f}s err={r.get('error','')!r}")
sys.exit(0 if ok else 1)
PY
}

scenario_long() {
  local out=$S2_HOME/results/long; mkdir -p "$out"
  capacity_gate
  local stacks=(none llmd-shipped llmd-tuned vllmr-default vllmr-tuned) pids=()
  for s in "${stacks[@]}"; do fresh "$s" || { echo "start $s failed"; return 1; }; done
  for s in "${stacks[@]}"; do
    "$P" stream -url "$("$SC" url "$s")" -tokens "$LONG_TOKENS" -session "long-$s" -label "$s" -outdir "$out" > "$out/$s.out" 2> "$out/$s.err" &
    pids+=($!)
  done
  echo "$(date -u +%T) ${#pids[@]} streams of $LONG_TOKENS tokens started"
  wait "${pids[@]}"
  for s in "${stacks[@]}"; do cp "$S2_HOME/run/$s/envoy-access.log" "$out/$s.envoy-access.log" 2>/dev/null; done
  python3 -I "$HERE/summarize.py" "$out"
  # Expectations from the specs and lessons: none and tuned pass; vllm-router's default cuts at its 1,800 s deadline.
  expect none pass 0 0 long || fail=1
  expect llmd-tuned pass 0 0 long || fail=1
  expect vllmr-tuned pass 0 0 long || fail=1
  expect vllmr-default cut 1795 1815 long || fail=1
  # llm-d as shipped: the evaluation expects a cut at 1,000 s if message_timeout bounds streams; record what happens.
  python3 -I "$HERE/summarize.py" "$out" | grep llmd-shipped
  for s in "${stacks[@]}"; do "$SC" stop "$s"; done
}

scenario_controls() {
  local out=$S2_HOME/results/controls; mkdir -p "$out"; capacity_gate
  fresh vllmr-short; fresh llmd-probe
  "$P" stream -url "$("$SC" url vllmr-short)" -tokens 150 -session c1 -label ctrl-vllmr-short -outdir "$out" > "$out/ctrl-vllmr-short.out" 2>&1 &
  local a=$!
  "$P" stream -url "$("$SC" url llmd-probe)" -tokens 600 -session c2 -label probe-llmd-5s-timers -outdir "$out" > "$out/probe-llmd-5s-timers.out" 2>&1 &
  local b=$!
  wait $a $b
  expect ctrl-vllmr-short cut 55 70 controls || fail=1
  expect probe-llmd-5s-timers pass 0 0 controls || fail=1
  "$SC" stop vllmr-short; "$SC" stop llmd-probe
}

scenario_placement() {
  local out=$S2_HOME/results/placement; mkdir -p "$out"; capacity_gate
  # name stack expect-sticky
  for spec in "llmd-affinity:true" "vllmr-affinity:true" "llmd-guide:true"; do
    local s=${spec%%:*} sticky=${spec##*:}
    fresh "$s" || { fail=1; continue; }
    "$P" placement -url "$("$SC" url "$s")" -metrics "$("$SC" metrics "$s")" -sessions 9 -turns 5 -expect-sticky="$sticky" | tee "$out/$s.txt"
    [[ ${PIPESTATUS[0]} -eq 0 ]] || fail=1
    "$SC" stop "$s"
  done
}

scenario_removal() {
  local out=$S2_HOME/results/removal; mkdir -p "$out"; capacity_gate
  for s in llmd-affinity vllmr-affinity; do
    fresh "$s" || { fail=1; continue; }
    "$P" removal -url "$("$SC" url "$s")" -metrics "$("$SC" metrics "$s")" -stream-tokens "${REMOVAL_TOKENS:-1500}" \
      -remove-cmd "S2_HOME=$S2_HOME $SC remove $s \$X_INDEX" -outdir "$out" -label "$s" | tee "$out/$s.txt"
    [[ ${PIPESTATUS[0]} -eq 0 ]] || fail=1
    "$SC" stop "$s"
  done
}

case ${1:-} in
long) scenario_long ;; controls) scenario_controls ;; placement) scenario_placement ;; removal) scenario_removal ;;
summary) python3 -I "$HERE/summarize.py" "$S2_HOME"/results/* ;;
*) sed -n 2,12p "$0"; exit 64 ;;
esac
exit $fail
