#!/usr/bin/env bash
# One command per scenario of spike S2 phase A. Needs S2_HOME prepared by setup.sh.
#
#   run.sh long       2 h streams (7300 tokens at 1 token/s) through: no router, llm-d as shipped,
#                     llm-d tuned, vllm-router default (1,800 s) and vllm-router tuned. ~2 h 5 min.
#   run.sh controls   positive controls: vllm-router with a 60 s deadline must cut a 150 s stream;
#                     llm-d with 5 s ext_proc timers streams 600 s anyway. ~11 min.
#   run.sh placement [stack...]  multi-turn sessions through each router: where do turns land. ~3 min each.
#   run.sh removal [stack...]  remove a worker from discovery under an in-flight 25 min stream. ~27 min each.
#   run.sh epp-kill   fault injection: stop the EPP under an in-flight stream. ~3 min
#   run.sh ttft       first byte after 1,100 s (past llm-d's 1,000 s message timeout), 30 tokens. ~19 min
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

scenario_placement() { # optional arguments select specs by stack name
  local out=$S2_HOME/results/placement; mkdir -p "$out"; capacity_gate
  # stack:expect-sticky:session-header:fresh-prompts[:nospread]
  #  header "-" sends none, so placement rests on the prompt prefix (llm-d) or the body hash (vllm-router);
  #  fresh-prompts 1 sends an unrelated prompt each turn, so only the header can keep a session together.
  local specs=("llmd-affinity:true:x-session-id:0" "llmd-affinity:true:x-session-id:1"
               "vllmr-affinity:true:x-session-id:0" "vllmr-affinity:true:x-session-id:1"
               "llmd-guide:true:x-session-id:0"     "llmd-guide:false:x-session-id:1" "llmd-guide:true:-:0"
               "vllmr-affinity:true:-:0:nospread")
  local spec s sticky key fr spr flag
  for spec in "${specs[@]}"; do
    IFS=: read -r s sticky key fr spr <<< "$spec"
    if [[ $# -gt 0 ]]; then case " $* " in *" $s "*) ;; *) continue ;; esac; fi
    [[ $key == - ]] && key=""
    flag=(); [[ $fr == 1 ]] && flag+=(-fresh-prompts); [[ ${spr:-} == nospread ]] && flag+=(-expect-spread=false)
    fresh "$s" || { fail=1; continue; }
    echo "== $s header=${key:-none} fresh_prompts=$fr"
    "$P" placement -url "$("$SC" url "$s")" -metrics "$("$SC" metrics "$s")" -sessions 9 -turns 5 -key "$key" -expect-sticky="$sticky" "${flag[@]}" \
      | tee "$out/$s.${key:-nohdr}.fresh$fr.txt"
    [[ ${PIPESTATUS[0]} -eq 0 ]] || fail=1
    "$SC" stop "$s"
  done
}

scenario_removal() { # optional arguments select stacks (llmd-removal, vllmr-removal)
  local out=$S2_HOME/results/removal; mkdir -p "$out"; capacity_gate
  local s
  for s in llmd-removal vllmr-removal; do
    if [[ $# -gt 0 ]]; then case " $* " in *" $s "*) ;; *) continue ;; esac; fi
    fresh "$s" || { fail=1; continue; }
    "$P" removal -url "$("$SC" url "$s")" -metrics "$("$SC" metrics "$s")" -stream-tokens "${REMOVAL_TOKENS:-1500}" \
      -remove-cmd "S2_HOME=$S2_HOME $SC remove $s \$X_INDEX" -outdir "$out" -label "$s" | tee "$out/$s.txt"
    [[ ${PIPESTATUS[0]} -eq 0 ]] || fail=1
    "$SC" stop "$s"
  done
}

# Fault injection, not an acceptance criterion: what does a dead EPP do to a stream in flight, and to new requests?
scenario_epp_kill() {
  local out=$S2_HOME/results/epp-kill; mkdir -p "$out"; capacity_gate
  fresh llmd-guide || return 1
  local url; url=$("$SC" url llmd-guide)
  "$P" stream -url "$url" -tokens 120 -session k1 -label inflight-during-epp-kill -outdir "$out" > "$out/inflight.out" 2>&1 &
  local pid=$!
  sleep 30; "$SC" kill-epp llmd-guide
  "$P" stream -url "$url" -tokens 3 -session k2 -label new-while-epp-down -outdir "$out" > "$out/new-down.out" 2>&1
  wait $pid
  "$SC" start-epp llmd-guide; sleep 20
  "$P" stream -url "$url" -tokens 3 -session k3 -label new-after-epp-restart -outdir "$out" > "$out/new-up.out" 2>&1
  python3 -I "$HERE/summarize.py" "$out"
  "$SC" stop llmd-guide
}

# A reply whose first byte takes longer than the shipped ext_proc message timeout (1,000 s): HBEP-5's
# "a reply spends many minutes in prefill before its first byte".
scenario_ttft() {
  local out=$S2_HOME/results/ttft; mkdir -p "$out"; capacity_gate
  export SIM_TTFT=${TTFT:-1100s}
  local s
  for s in llmd-ttft vllmr-ttft; do fresh "$s" || { fail=1; continue; }; done
  "$P" stream -url "$("$SC" url llmd-ttft)" -tokens 30 -session t1 -label llmd-shipped-ttft -outdir "$out" > "$out/llmd.out" 2>&1 &
  local a=$!
  "$P" stream -url "$("$SC" url vllmr-ttft)" -tokens 30 -session t2 -label vllmr-default-ttft -outdir "$out" > "$out/vllmr.out" 2>&1 &
  local b=$!
  wait $a $b
  python3 -I "$HERE/summarize.py" "$out"
  expect llmd-shipped-ttft pass 0 0 ttft || fail=1
  expect vllmr-default-ttft pass 0 0 ttft || fail=1
  for s in llmd-ttft vllmr-ttft; do "$SC" stop "$s"; done
}

case ${1:-} in
long) scenario_long ;; controls) scenario_controls ;; placement) scenario_placement "${@:2}" ;; removal) scenario_removal "${@:2}" ;; epp-kill) scenario_epp_kill ;; ttft) scenario_ttft ;;
summary) python3 -I "$HERE/summarize.py" "$S2_HOME"/results/* ;;
*) sed -n 2,12p "$0"; exit 64 ;;
esac
exit $fail
