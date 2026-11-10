#!/usr/bin/env bash
# Run the control and every router arm, one after another, on ports 29900-29906.
# Results go to spikes/s13/results/<arm>/ (committed); the routers' logs and HOME to
# $S13_HOME/runs/<arm>/ (never committed: they name this host's paths).
#
#   S13_HOME=<scratch dir, after setup.sh> run.sh
#
# Exit 0 only if the control passed (every body reached the recorder as its exact bytes), every
# router arm completed, and nothing was left listening or running afterwards.
set -uo pipefail
S13_HOME=${S13_HOME:?set S13_HOME to the directory setup.sh filled}
HERE=$(cd "$(dirname "$0")" && pwd)
RES=$HERE/../results
BIN=$S13_HOME/bin/s13fields
[[ -x $BIN ]] || { echo "no harness at $BIN; run setup.sh" >&2; exit 2; }

busy() { ss -H -ltn | awk '{print $4}' | grep -E ':2990[0-6]$'; }
leftover() { pgrep -f "$S13_HOME/" || true; }
check_clean() {
  local b l; b=$(busy); l=$(leftover)
  [[ -z $b && -z $l ]] && return 0
  echo "left behind after $1: listeners [$b] processes [$l]" >&2; return 1
}

rc=0
check_clean "nothing (before the first arm)" || exit 2
nice -n 19 "$BIN" -out "$RES/direct-control" || { echo "the control failed" >&2; rc=1; }
check_clean "the control" || rc=1
for arm in 0.1.16:consistent_hash 0.1.16:round_robin 0.1.17:consistent_hash 0.1.17:round_robin 0.1.17:kv_aware; do
  v=${arm%%:*} p=${arm##*:}
  nice -n 19 "$BIN" -router "$S13_HOME/venv-$v/bin/vllm-router" -policy "$p" -label "vllm-router $v, $p" \
    -out "$RES/vllm-router-$v-$p" -work "$S13_HOME/runs/$v-$p" || { echo "arm $arm failed" >&2; rc=1; }
  check_clean "$arm" || rc=1
done
# Envoy as the router, ring_hash on x-session-id: the latest release, and S2's 1.33.2.
for v in 1.40.2 1.33.2; do
  envoy=$S13_HOME/bin/envoy-$v; [[ $v == 1.33.2 ]] && envoy=$S13_HOME/s2/bin/envoy-1.33.2
  nice -n 19 "$BIN" -out "$RES/envoy-$v-ring_hash" -work "$S13_HOME/runs/envoy-$v-ring_hash" \
    -label "Envoy $v, ring_hash on x-session-id" -exec -- "$HERE/stack.sh" ring-hash "$envoy" || { echo "arm envoy-$v failed" >&2; rc=1; }
  check_clean "envoy-$v" || rc=1
done
# llm-d's EPP behind Envoy 1.33.2, S2's tuned stack.
S2_HOME=$S13_HOME/s2 nice -n 19 "$BIN" -out "$RES/llm-d-epp-envoy-1.33.2" -work "$S13_HOME/runs/llm-d" \
  -label "llm-d EPP v1.0.0 behind Envoy 1.33.2 (S2's tuned stack)" -exec -- "$HERE/stack.sh" llm-d || { echo "arm llm-d failed" >&2; rc=1; }
check_clean "llm-d" || rc=1
exit $rc
