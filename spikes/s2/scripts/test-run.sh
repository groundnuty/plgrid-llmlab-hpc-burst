#!/usr/bin/env bash
# Failure-injection checks for run.sh: it must exit non-zero when a stack fails to start, when the
# shipped llm-d result is a cut, too short or missing, and when a failure is hidden inside `all`; and it
# must exit 0 when everything passes (the baseline that gives the failures their meaning).
#
# run.sh runs here against a fake stack.sh and a fake s2probe (S2_STACK_SH, S2_PROBE): no server starts,
# nothing listens, and a whole run takes seconds. The fake probe reports the results the issue lists
# (an arm through 2 h passes; vllm-router's default cuts at 1,800 s; the 60 s control cuts at 60 s).
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
T=$(mktemp -d "${TMPDIR:-/tmp}/s2-test-run.XXXXXX"); trap 'rm -rf "$T"' EXIT
fails=0
check() { local d=$1; shift; if "$@"; then echo "ok   $d"; else echo "FAIL $d"; fails=$((fails + 1)); fi; }

cat > "$T/stack.sh" <<'STUB'
#!/usr/bin/env bash
echo "$*" >> "$FAKE_LOG"
case $1 in
start) [[ " ${FAKE_START_FAIL:-} " == *" $2 "* ]] && { echo "fake: $2 refuses to start" >&2; exit 1; }; echo "started $2" ;;
url) echo "http://127.0.0.1:1/v1/chat/completions" ;;
metrics) echo "http://127.0.0.1:1/metrics,http://127.0.0.1:2/metrics,http://127.0.0.1:3/metrics" ;;
esac
exit 0
STUB
cat > "$T/probe" <<'STUB'
#!/usr/bin/env python3
import json, os, sys
a = sys.argv[1:]
if a[0] != "stream":
    sys.exit(0)  # placement and removal: succeed
def flag(n): return a[a.index(n) + 1]
label, outdir, tokens = flag("-label"), flag("-outdir"), int(flag("-tokens"))
s = json.load(open(os.environ["FAKE_SPEC"])).get(label, {})
if s.get("crash"):
    sys.exit(1)  # dies without a result file
ok = s.get("complete", True)
json.dump({"label": label, "complete": ok, "tokens": tokens if ok else 100, "expect_tokens": tokens,
           "elapsed_sec": s.get("elapsed", 7300.0), "first_token_sec": s.get("first_token", 1100.0), "error": "" if ok else "read: unexpected EOF"},
          open(f"{outdir}/{label}.json", "w"))
sys.exit(0 if ok else 2)
STUB
chmod +x "$T/stack.sh" "$T/probe"
BASE='{"vllmr-default":{"complete":false,"elapsed":1800.1},"ctrl-vllmr-short":{"complete":false,"elapsed":60.0},"probe-llmd-5s-timers":{"elapsed":600.0}}'

# runit <name> <scenario...> : fresh S2_HOME, FAKE_SPEC from $SPEC, FAKE_START_FAIL from $SF; leaves $T/out and $T/log
runit() {
  rm -rf "$T/home" "$T/log"; mkdir -p "$T/home"; : > "$T/log"
  echo "${SPEC:-$BASE}" > "$T/spec.json"
  S2_HOME="$T/home" S2_STACK_SH="$T/stack.sh" S2_PROBE="$T/probe" S2_CAPACITY_GATE=0 FAKE_SPEC="$T/spec.json" FAKE_LOG="$T/log" \
    FAKE_START_FAIL="${SF:-}" S2_ALL_SCENARIOS="${ALL:-controls}" "$HERE/run.sh" "$@" > "$T/out" 2>&1
  echo $?
}
withspec() { SPEC=$(python3 -I -c "import json,sys; d=json.loads(sys.argv[1]); d.update(json.loads(sys.argv[2])); print(json.dumps(d))" "$BASE" "$1"); }

echo "== baseline: everything as the issue expects"
SPEC= SF= ; rc=$(runit long);  check "long exits 0 when all five arms are as expected" test "$rc" = 0
check "the shipped llm-d arm is reported" grep -q '^OK   llmd-shipped' "$T/out"
rc=$(runit controls);          check "controls exits 0" test "$rc" = 0
rc=$(ALL=controls runit all);  check "all exits 0 (controls and long)" test "$rc" = 0
rc=$(runit placement);         check "placement exits 0" test "$rc" = 0
rc=$(runit ttft);              check "ttft exits 0 when the first byte comes after 1,000 s" test "$rc" = 0

echo "== llm-d as shipped must pass the 2 h criterion"
withspec '{"llmd-shipped":{"complete":false,"elapsed":1000.2}}'; rc=$(runit long)
check "a cut at 1,000 s fails run.sh" test "$rc" != 0
check "the output names the shipped arm" grep -q '^FAIL llmd-shipped' "$T/out"
withspec '{"llmd-shipped":{"elapsed":3600.0}}'; rc=$(runit long)
check "a complete but too short stream (3,600 s) fails run.sh" test "$rc" != 0
check "the output says why" grep -q 'FAIL llmd-shipped.*elapsed_sec=3600' "$T/out"
withspec '{"llmd-shipped":{"crash":true}}'; rc=$(runit long)
check "a client that dies without a result file fails run.sh" test "$rc" != 0
check "the output names the missing result" grep -q 'FAIL llmd-shipped: no usable result' "$T/out"
withspec '{"vllmr-default":{"complete":true,"elapsed":7300.0}}'; rc=$(runit long)
check "vllm-router default surviving 2 h (no cut) fails run.sh" test "$rc" != 0

echo "== stack start failures"
SPEC= SF="llmd-tuned"; rc=$(runit long)
check "a stack that fails to start fails long" test "$rc" != 0
check "the stacks already started are stopped, not left running" test "$(grep -c '^stop ' "$T/log")" -ge 5
SF="vllmr-short"; rc=$(runit controls);     check "controls fails when a stack does not start" test "$rc" != 0
SF="llmd-affinity"; rc=$(runit placement);  check "placement fails when a stack does not start" test "$rc" != 0
SF="llmd-ttft"; rc=$(runit ttft);           check "ttft fails when a stack does not start" test "$rc" != 0

echo "== failures hidden inside all"
SF="vllmr-short"; rc=$(ALL=controls runit all); check "all fails when controls cannot start a stack" test "$rc" != 0
SF="vllmr-tuned"; rc=$(ALL=controls runit all); check "all fails when only the background long scenario fails" test "$rc" != 0
SF="vllmr-short"; rc=$(ALL="controls placement" runit all)
check "all still runs the later scenarios after one failed" grep -q '^start llmd-affinity' "$T/log"
check "and still exits non-zero" test "$rc" != 0
SF=; withspec '{"llmd-shipped":{"complete":false,"elapsed":1000.2}}'; rc=$(ALL=controls runit all)
check "all fails when the shipped llm-d arm fails" test "$rc" != 0

echo "== minimum duration of a pass, ttft"
SF=; withspec '{"llmd-shipped-ttft":{"first_token":5.0}}'; rc=$(runit ttft)
check "ttft fails when the first byte came before 1,000 s" test "$rc" != 0

echo "== setup missing"
SPEC=; rc=$(S2_PROBE="$T/missing" S2_HOME="$T/home" S2_STACK_SH="$T/stack.sh" "$HERE/run.sh" long > "$T/out" 2>&1; echo $?)
check "a missing s2probe is a setup failure (exit 2)" test "$rc" = 2
check "with a message" grep -q 'setup is incomplete' "$T/out"
rc=$(S2_HOME="$T/emptyhome" "$HERE/run.sh" long > "$T/out" 2>&1; echo $?)
check "an empty S2_HOME is a setup failure (exit 2)" test "$rc" = 2
rc=$(S2_HOME="$T/emptyhome" "$HERE/run.sh" nonsense > /dev/null 2>&1; echo $?)
check "an unknown scenario exits 64" test "$rc" = 64

echo; if [[ $fails -eq 0 ]]; then echo "test-run: all passed"; else echo "test-run: $fails FAILED"; fi
exit $((fails > 0))
