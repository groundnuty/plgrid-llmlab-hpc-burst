#!/usr/bin/env bash
# One command per scenario of spike S2 phase A. Needs S2_HOME prepared by setup.sh.
#
#   run.sh long       2 h streams (7300 tokens at 1 token/s) through: no router, llm-d as shipped,
#                     llm-d tuned, vllm-router default (1,800 s) and vllm-router tuned. ~2 h 5 min.
#                     The four passing arms must each last at least LONG_MIN_SEC (default 7200 s).
#   run.sh controls   vllm-router with a 60 s deadline must cut a 150 s stream (the positive control: the
#                     client detects a router deadline); llm-d with 5 s ext_proc timers streams 600 s
#                     (a smoke check only: it cannot tell an armed timer from an inert one). ~11 min.
#   run.sh placement [stack...]  multi-turn sessions through each router: where do turns land. ~3 min each.
#   run.sh removal [stack...]  remove a worker from discovery under an in-flight 35 min stream. ~37 min each.
#   run.sh epp-kill   fault injection: SIGTERM, then SIGKILL after 10 s, to the EPP under an in-flight stream. ~3 min
#   run.sh epp-term   fault injection: SIGTERM alone to the EPP under an in-flight stream; records the
#                     timeline (when the EPP exits, whether the stream finishes). ~6 min
#   run.sh epp-stall  fault injection: freeze the EPP for 90 s under an in-flight stream. ~4 min
#   run.sh restart    does a removal survive a router restart? (L-4.8) ~2 min
#   run.sh ttft       first byte after 1,100 s (past llm-d's 1,000 s message timeout), 30 tokens. ~19 min
#   run.sh all        every scenario above, the 2 h streams in the background. ~2 h 15 min
#   run.sh summary    table of every stream result found under $S2_HOME/results
#
# Exit status: 0 only if every scenario that ran met its expectations. Non-zero if an expectation is not
# met, a stack or an EPP fails to start, a client crashes, or setup is missing (exit 2). `all` runs every
# scenario even after one fails, and still exits non-zero. Results: $S2_HOME/results/<scenario>/.
#
# Test hooks (scripts/test-run.sh): S2_STACK_SH and S2_PROBE replace stack.sh and the s2probe binary;
# S2_CAPACITY_GATE=0 skips the wait for a quiet VM; S2_ALL_SCENARIOS narrows what `all` runs.
# Acceptance runs use none of them. LONG_TOKENS, LONG_MIN_SEC, REMOVAL_TOKENS and REMOVAL_MIN_SEC shorten
# the runs for smoke tests; the minimum durations then have to be lowered with them, and such a run is
# not acceptance evidence.
set -uo pipefail
S2_HOME=${S2_HOME:?set S2_HOME}; export S2_HOME
HERE=$(cd "$(dirname "$0")" && pwd)
SC=${S2_STACK_SH:-$HERE/stack.sh}; export S2_STACK_SH=$SC
P=${S2_PROBE:-$S2_HOME/bin/s2probe}; export S2_PROBE=$P
LONG_TOKENS=${LONG_TOKENS:-7300}
LONG_MIN_SEC=${LONG_MIN_SEC:-7200}   # the issue's 2 h criterion; its "verifications that prove nothing" start below about 30 min
fail=0

# preflight: setup must have run. A missing binary is a setup failure, not a scenario result.
preflight() {
  local f missing=()
  for f in "$SC" "$P"; do [[ -x $f ]] || missing+=("$f"); done
  if [[ $SC == "$HERE/stack.sh" ]]; then
    for f in bin/llm-d-inference-sim bin/epp bin/envoy-1.33.2 venv/bin/vllm-router; do [[ -x $S2_HOME/$f ]] || missing+=("$S2_HOME/$f"); done
  fi
  command -v python3 > /dev/null || missing+=(python3)
  if [[ ${#missing[@]} -gt 0 ]]; then echo "run.sh: setup is incomplete, missing: ${missing[*]} (run scripts/setup.sh)" >&2; return 1; fi
}

capacity_gate() { # the VM is shared: wait until load <= 4.0 and >= 8 GB available
  [[ ${S2_CAPACITY_GATE:-1} == 0 ]] && return 0
  while true; do
    l=$(awk '{print $1}' /proc/loadavg); m=$(awk '/MemAvailable/{printf "%d", $2/1048576}' /proc/meminfo)
    awk -v l="$l" -v m="$m" 'BEGIN{exit !(l<=4.0 && m>=8)}' && return 0
    echo "$(date -u +%T) waiting: load=$l mem_avail_gb=$m"; sleep 60
  done
}
fresh() { "$SC" stop "$1" >/dev/null 2>&1; rm -rf "$S2_HOME/run/$1"; "$SC" start "$1"; }
stop_all() { local s; for s in "$@"; do "$SC" stop "$s" > /dev/null 2>&1; done; }
# step <scenario function> [args] : run it, and count a non-zero return as a failure of the whole run
step() { "$@" || { echo "FAILED: $*" >&2; fail=1; }; }

# expect <label> <want: pass|cut> <lo> <hi> <scenario> [metric] : judge a finished stream's JSON against the stated expectation.
#   pass: the stream is complete and metric (default elapsed_sec) is at least <lo> seconds; <hi> is unused.
#         The minimum is what makes a pass mean something: a stream that is too short to meet the deadlines under test proves nothing.
#   cut:  the stream is not complete and elapsed_sec lies in [<lo>, <hi>].
# A missing or unreadable result file is a FAIL.
expect() {
  python3 -I - "$1" "$2" "$3" "$4" "$S2_HOME/results/$5/$1.json" "${6:-elapsed_sec}" <<'PY'
import json, sys
label, want, lo, hi, path, metric = sys.argv[1:7]
try:
    r = json.load(open(path))
    complete, tokens, expect_tokens, elapsed = r["complete"], r["tokens"], r["expect_tokens"], r["elapsed_sec"]
    value = r[metric]
except (OSError, ValueError, KeyError) as e:
    print(f"FAIL {label}: no usable result at {path} ({type(e).__name__}: {e})")
    sys.exit(1)
if want == "pass":
    ok = complete and value >= float(lo)
    need = f" with {metric} >= {lo} s"
else:
    ok = (not complete) and float(lo) <= elapsed <= float(hi)
    need = f" in [{lo},{hi}] s"
print(f"{'OK  ' if ok else 'FAIL'} {label}: expected {want}{need}; got complete={complete} tokens={tokens}/{expect_tokens} "
      f"{metric}={value:.0f}s err={r.get('error','')!r}")
sys.exit(0 if ok else 1)
PY
}

scenario_long() {
  local out=$S2_HOME/results/long rc=0; mkdir -p "$out"
  capacity_gate
  local stacks=(none llmd-shipped llmd-tuned vllmr-default vllmr-tuned) pids=() s i
  for s in "${stacks[@]}"; do
    fresh "$s" || { echo "start $s failed"; stop_all "${stacks[@]}"; return 1; }
  done
  for s in "${stacks[@]}"; do
    "$P" stream -url "$("$SC" url "$s")" -tokens "$LONG_TOKENS" -session "long-$s" -label "$s" -outdir "$out" > "$out/$s.out" 2> "$out/$s.err" &
    pids+=($!)
  done
  echo "$(date -u +%T) ${#pids[@]} streams of $LONG_TOKENS tokens started"
  for i in "${!pids[@]}"; do # the client exits 0 (complete) or 2 (cut); anything else is a crash
    wait "${pids[$i]}"; s=$?
    [[ $s -eq 0 || $s -eq 2 ]] || { echo "FAIL ${stacks[$i]}: s2probe exited $s"; rc=1; }
  done
  for s in "${stacks[@]}"; do cp "$S2_HOME/run/$s/envoy-access.log" "$out/$s.envoy-access.log" 2>/dev/null; done
  python3 -I "$HERE/summarize.py" "$out"
  # Expectations from the issue and the lessons: every arm with its deadlines raised or absent passes, and
  # llm-d as shipped passes too (its 1,000 s message timeout is the deadline the evaluation flagged);
  # vllm-router's default cuts at its 1,800 s deadline. Each pass must last LONG_MIN_SEC.
  expect none pass "$LONG_MIN_SEC" 0 long || rc=1
  expect llmd-shipped pass "$LONG_MIN_SEC" 0 long || rc=1
  expect llmd-tuned pass "$LONG_MIN_SEC" 0 long || rc=1
  expect vllmr-tuned pass "$LONG_MIN_SEC" 0 long || rc=1
  expect vllmr-default cut 1795 1815 long || rc=1
  stop_all "${stacks[@]}"
  return $rc
}

scenario_controls() {
  local out=$S2_HOME/results/controls rc=0; mkdir -p "$out"; capacity_gate
  fresh vllmr-short && fresh llmd-probe || { stop_all vllmr-short llmd-probe; return 1; }
  "$P" stream -url "$("$SC" url vllmr-short)" -tokens 150 -session c1 -label ctrl-vllmr-short -outdir "$out" > "$out/ctrl-vllmr-short.out" 2>&1 &
  local a=$!
  "$P" stream -url "$("$SC" url llmd-probe)" -tokens 600 -session c2 -label probe-llmd-5s-timers -outdir "$out" > "$out/probe-llmd-5s-timers.out" 2>&1 &
  local b=$!
  wait $a $b
  # Positive control: the client must see a router deadline as a cut, at about 60 s.
  expect ctrl-vllmr-short cut 55 70 controls || rc=1
  # Smoke check, not a control: promptly answered ext_proc messages keep a 5 s per-message timer from firing
  # whether or not the timer is armed, so a pass here does not show that it is inert (see the README).
  expect probe-llmd-5s-timers pass 500 0 controls || rc=1
  stop_all vllmr-short llmd-probe
  return $rc
}

scenario_placement() { # optional arguments select specs by stack name
  local out=$S2_HOME/results/placement rc=0; mkdir -p "$out"; capacity_gate
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
    fresh "$s" || { rc=1; stop_all "$s"; continue; }
    echo "== $s header=${key:-none} fresh_prompts=$fr"
    "$P" placement -url "$("$SC" url "$s")" -metrics "$("$SC" metrics "$s")" -sessions 9 -turns 5 -key "$key" -expect-sticky="$sticky" "${flag[@]}" \
      | tee "$out/$s.${key:-nohdr}.fresh$fr.txt"
    [[ ${PIPESTATUS[0]} -eq 0 ]] || rc=1
    "$SC" stop "$s"
  done
  return $rc
}

scenario_removal() { # optional arguments select stacks (llmd-removal, vllmr-removal)
  local out=$S2_HOME/results/removal rc=0; mkdir -p "$out"; capacity_gate
  local s
  for s in llmd-removal vllmr-removal; do
    if [[ $# -gt 0 ]]; then case " $* " in *" $s "*) ;; *) continue ;; esac; fi
    fresh "$s" || { rc=1; stop_all "$s"; continue; }
    "$P" removal -url "$("$SC" url "$s")" -metrics "$("$SC" metrics "$s")" -stream-tokens "${REMOVAL_TOKENS:-2100}" \
      -min-stream-sec "${REMOVAL_MIN_SEC:-1800}" -min-after-removal-sec "${REMOVAL_MIN_SEC:-1800}" \
      -remove-cmd "S2_HOME=$S2_HOME $SC remove $s \$X_INDEX" -outdir "$out" -label "$s" | tee "$out/$s.txt"
    [[ ${PIPESTATUS[0]} -eq 0 ]] || rc=1
    "$SC" stop "$s"
  done
  return $rc
}

# Fault injection, not an acceptance criterion: what does a dead EPP do to a stream in flight, and to new requests?
# kill-epp is SIGTERM and then SIGKILL after 10 s (stack.sh stop_pid): a forced kill. epp-term has the graceful case.
scenario_epp_kill() {
  local out=$S2_HOME/results/epp-kill rc=0; mkdir -p "$out"; capacity_gate
  fresh llmd-guide || { stop_all llmd-guide; return 1; }
  local url; url=$("$SC" url llmd-guide)
  "$P" stream -url "$url" -tokens 120 -session k1 -label inflight-during-epp-kill -outdir "$out" > "$out/inflight.out" 2>&1 &
  local pid=$!
  sleep 30; "$SC" kill-epp llmd-guide || rc=1
  "$P" stream -url "$url" -tokens 3 -session k2 -label new-while-epp-down -outdir "$out" > "$out/new-down.out" 2>&1
  wait $pid
  "$SC" start-epp llmd-guide || rc=1
  sleep 20
  "$P" stream -url "$url" -tokens 3 -session k3 -label new-after-epp-restart -outdir "$out" > "$out/new-up.out" 2>&1
  python3 -I "$HERE/summarize.py" "$out"
  stop_all llmd-guide
  return $rc
}

# Fault injection, not an acceptance criterion: SIGTERM alone, no SIGKILL behind it. The pinned EPP starts
# GracefulStop on SIGTERM and lets open streams finish (llm-d-router runner.go runWithFileDiscovery,
# internal/runnable/grpc.go serveGRPC). What actually happens is recorded in $out/timeline.txt: seconds after the
# SIGTERM at which the EPP exits, and whether the stream, a request during the stop and a request after it finish.
scenario_epp_term() {
  local out=$S2_HOME/results/epp-term rc=0; mkdir -p "$out"; capacity_gate
  local tl=$out/timeline.txt; : > "$tl"
  fresh llmd-guide || { stop_all llmd-guide; return 1; }
  local url t0 t pid sp wait_after=${EPP_TERM_WAIT_S:-120}; url=$("$SC" url llmd-guide)
  "$P" stream -url "$url" -tokens "${EPP_TERM_TOKENS:-150}" -session e1 -label inflight-during-epp-term -outdir "$out" > "$out/inflight.out" 2>&1 &
  pid=$!
  sleep 30
  t0=$(date +%s); echo "+0s SIGTERM sent to the EPP only (stream running since 30 s)" | tee -a "$tl"
  "$SC" signal-epp llmd-guide TERM || { rc=1; echo "FAIL could not send SIGTERM" | tee -a "$tl"; }
  sleep 5
  "$SC" epp-alive llmd-guide && echo "+$(( $(date +%s) - t0 ))s EPP still alive" | tee -a "$tl" || echo "+$(( $(date +%s) - t0 ))s EPP already gone" | tee -a "$tl"
  "$P" stream -url "$url" -tokens 3 -session e2 -label new-during-epp-term -outdir "$out" > "$out/new-during.out" 2>&1
  sp=$?
  echo "+$(( $(date +%s) - t0 ))s a 3-token request sent 5 s after the SIGTERM ended with exit $sp (0 = complete)" | tee -a "$tl"
  # poll the EPP and the stream until both are gone, up to wait_after seconds after the stream ended
  local epp_gone="" stream_done="" end_at=""
  while true; do
    t=$(( $(date +%s) - t0 ))
    if [[ -z $stream_done ]] && ! kill -0 "$pid" 2>/dev/null; then stream_done=$t; end_at=$(( $(date +%s) + wait_after )); echo "+${t}s stream process ended" | tee -a "$tl"; fi
    if [[ -z $epp_gone ]] && ! "$SC" epp-alive llmd-guide; then epp_gone=$t; echo "+${t}s EPP exited" | tee -a "$tl"; fi
    [[ -n $stream_done && -n $epp_gone ]] && break
    if [[ -n $end_at && $(date +%s) -ge $end_at ]]; then echo "+${t}s EPP still alive ${wait_after} s after the stream ended: SIGKILL by PID" | tee -a "$tl"; "$SC" kill-epp llmd-guide; break; fi
    sleep 1
  done
  wait $pid; sp=$?
  echo "the in-flight stream ended with exit $sp (0 = complete)" | tee -a "$tl"
  "$P" stream -url "$url" -tokens 3 -session e3 -label new-after-epp-exit -outdir "$out" > "$out/new-after.out" 2>&1
  sp=$?
  echo "a 3-token request after the EPP exited ended with exit $sp (0 = complete)" | tee -a "$tl"
  python3 -I "$HERE/summarize.py" "$out" | tee -a "$tl"
  stop_all llmd-guide
  return $rc
}

# Fault injection: a frozen EPP. Envoy starts no ext_proc timer in FULL_DUPLEX_STREAMED mode, so nothing should notice.
scenario_epp_stall() {
  local out=$S2_HOME/results/epp-stall rc=0; mkdir -p "$out"; capacity_gate
  fresh llmd-guide || { stop_all llmd-guide; return 1; }
  "$P" stream -url "$("$SC" url llmd-guide)" -tokens 150 -session s1 -label stream-through-frozen-epp -outdir "$out" > "$out/stream.out" 2>&1 &
  local pid=$!
  sleep 20; "$SC" signal-epp llmd-guide STOP || rc=1
  sleep 90; "$SC" signal-epp llmd-guide CONT || rc=1
  wait $pid
  python3 -I "$HERE/summarize.py" "$out"
  stop_all llmd-guide
  return $rc
}

# Does a removal survive a restart of the router? (lesson L-4.8: joined workers were lost on a restart)
scenario_restart() {
  local out=$S2_HOME/results/restart rc=0; mkdir -p "$out"; capacity_gate
  local url metrics w0
  # llm-d: an EPP restart that keeps the endpoints file keeps the removal, so worker 0 must stay out.
  # (stack.sh start would write a fresh endpoints file with all three workers; start-epp does not.)
  fresh llmd-guide || { stop_all llmd-guide; return 1; }
  url=$("$SC" url llmd-guide); metrics=$("$SC" metrics llmd-guide)
  "$SC" remove llmd-guide 0 || rc=1; sleep 5
  "$P" placement -url "$url" -metrics "$metrics" -sessions 12 -turns 1 | tee "$out/llmd-before.txt" >/dev/null
  "$SC" kill-epp llmd-guide; "$SC" start-epp llmd-guide || rc=1; sleep 15
  "$P" placement -url "$url" -metrics "$metrics" -sessions 12 -turns 1 | tee "$out/llmd-after.txt" >/dev/null
  w0=$(sed -n 's/.*(by first turn): \[\([0-9]*\) .*/\1/p' "$out/llmd-after.txt")
  echo "llm-d: sessions on removed worker 0 after the EPP restart: $w0 (expected 0)"; [[ $w0 == 0 ]] || rc=1
  "$SC" stop llmd-guide
  # vllm-router: the removal is in memory, so worker 0 should be back after a restart.
  fresh vllmr-affinity || { stop_all vllmr-affinity; return 1; }
  local lw="http://127.0.0.1:$(( $("$SC" url vllmr-affinity | sed 's|.*:\([0-9]*\)/v1.*|\1|') ))/list_workers"
  "$SC" remove vllmr-affinity 0 || rc=1; echo "vllm-router workers after removal: $(curl -sf "$lw")"
  "$SC" restart-router vllmr-affinity || rc=1
  echo "vllm-router workers after restart: $(curl -sf "$lw")" | tee "$out/vllmr-after.txt"
  [[ $(curl -sf "$lw" | grep -o 'http://' | wc -l) -eq 3 ]] && echo "vllm-router: removed worker is back (expected)" || { echo "vllm-router: removal survived a restart (unexpected)"; rc=1; }
  "$SC" stop vllmr-affinity
  return $rc
}

# A reply whose first byte takes longer than the shipped ext_proc message timeout (1,000 s): HBEP-5's
# "a reply spends many minutes in prefill before its first byte".
scenario_ttft() {
  local out=$S2_HOME/results/ttft rc=0; mkdir -p "$out"; capacity_gate
  export SIM_TTFT=${TTFT:-1100s}
  local s
  for s in llmd-ttft vllmr-ttft; do fresh "$s" || { stop_all llmd-ttft vllmr-ttft; return 1; }; done
  "$P" stream -url "$("$SC" url llmd-ttft)" -tokens 30 -session t1 -label llmd-shipped-ttft -outdir "$out" > "$out/llmd.out" 2>&1 &
  local a=$!
  "$P" stream -url "$("$SC" url vllmr-ttft)" -tokens 30 -session t2 -label vllmr-default-ttft -outdir "$out" > "$out/vllmr.out" 2>&1 &
  local b=$!
  wait $a $b
  python3 -I "$HERE/summarize.py" "$out"
  # The first byte must come later than llm-d's 1,000 s message timeout, or the test says nothing about it.
  expect llmd-shipped-ttft pass 1000 0 ttft first_token_sec || rc=1
  expect vllmr-default-ttft pass 1000 0 ttft first_token_sec || rc=1
  stop_all llmd-ttft vllmr-ttft
  return $rc
}

# Everything: the 2 h streams run in the background while the other scenarios run in turn (about 2 h 15 min).
# A failing scenario does not stop the others, and any failure makes the whole run exit non-zero.
scenario_all() {
  local lp sc rc=0
  "$0" long > "$S2_HOME/results/long.log" 2>&1 &
  lp=$!
  for sc in ${S2_ALL_SCENARIOS:-controls placement removal epp-kill epp-term epp-stall restart ttft}; do
    step "scenario_${sc//-/_}"
  done
  wait $lp || { echo "FAILED: long (see $S2_HOME/results/long.log)" >&2; fail=1; }
  echo "long scenario log: $S2_HOME/results/long.log"; tail -12 "$S2_HOME/results/long.log"
  return $fail
}

case ${1:-} in
summary) python3 -I "$HERE/summarize.py" "$S2_HOME"/results/*; exit $? ;;
long|controls|placement|removal|epp-kill|epp-term|epp-stall|ttft|restart|all) ;;
*) sed -n 2,12p "$0"; exit 64 ;;
esac
preflight || exit 2
mkdir -p "$S2_HOME/results"
sc=$1; step "scenario_${sc//-/_}" "${@:2}"
exit $fail
