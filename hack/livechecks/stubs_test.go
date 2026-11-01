package livechecks

// The stub cluster. ssh runs the remote script on this machine against fake
// Slurm commands that keep their queue in $STUB_STATE/queue, one job per line:
// id|name|qos|partition|state. Every reply is seeded with a job number, a
// host, an account and a URL with a query, the things the script must not echo.
// Every stub appends to $STUB_STATE/events, so a test can assert the order of
// the builds, the slot checks and the submit.

// stubSSH records its arguments (the account must never be among them) and
// runs the remote script, or the command string, with the fake Slurm first on PATH.
const stubSSH = `#!/bin/bash
printf '%s\n' "$*" >> "$STUB_STATE/ssh-argv"
[ -z "${STUB_SSH_FAIL:-}" ] || exit 255
shift 2
host=$1; shift
export PATH="$STUB_STATE/../bin/slurm:$PATH"
export SCRATCH="$STUB_STATE/scratch-$host"
mkdir -p "$SCRATCH"
# A login banner on standard output, as a shell start-up file can print one; not into a tar stream, which it would corrupt.
case "$1" in *"-cf - ."*) ;; *) [ -z "${STUB_BANNER:-}" ] || echo "welcome to seedhost01, account SEEDACCT" ;; esac
if [ "$1" = "bash -s" ]; then
  script=$(cat)
  case "$script" in
    *"sbatch --parsable"*)
      if [ -n "${STUB_LOSE_SUBMIT_REPLY:-}" ]; then printf '%s\n' "$script" | bash -s >/dev/null 2>&1; exit 255; fi ;;
  esac
  printf '%s\n' "$script" | exec bash -s
fi
case "$1" in
  *"-cf - ."*) [ -z "${STUB_FETCH_FAIL:-}" ] || exit 255 ;;
esac
exec bash -c "$1"
`

// stubSqueue also advances the fake clock by STUB_CLOCK_STEP seconds on every
// poll of a job, so the budgets of the real-engine run are tested without waiting.
const stubSqueue = `#!/bin/bash
Q="$STUB_STATE/queue"
[ ! -e "$STUB_STATE/squeue-broken" ] || { echo "squeue: error: unable to contact slurm controller seedhost01" >&2; exit 1; }
[ -z "${STUB_SQUEUE_FAIL:-}" ] || { echo "squeue: error: unable to contact slurm controller seedhost01" >&2; exit 1; }
fmt=""; id=""
while [ $# -gt 0 ]; do
  case $1 in -o) fmt=$2; shift ;; -j) id=$2; shift ;; esac
  shift
done
echo "squeue ${fmt}${id:+ job}" >> "$STUB_STATE/events"
if [ -n "$id" ]; then
  touch "$STUB_STATE/polled"
  if [ -n "${LIVE_FAKE_CLOCK:-}" ] && [ -n "${STUB_CLOCK_STEP:-}" ]; then echo $(( $(cat "$LIVE_FAKE_CLOCK") + STUB_CLOCK_STEP )) > "$LIVE_FAKE_CLOCK"; fi
  if [ -e "$STUB_STATE/forever-$id" ]; then cat "$STUB_STATE/forever-$id"; exit 0; fi
  f="$STUB_STATE/jobstates-$id"
  if [ -s "$f" ]; then head -1 "$f"; sed -i 1d "$f"; exit 0; fi
  sed -i "/^$id|/d" "$Q"
  echo "slurm_load_jobs error: Invalid job id specified" >&2; exit 1
fi
case $fmt in
  '%i|%j|%P') awk -F'|' '{print $1"|"$2"|"$4}' "$Q" ;;
  '%q|%P') awk -F'|' '{print $3"|"$4}' "$Q" ;;
esac
`

// stubSbatch: --test-only answers with a seeded line on stderr, as sbatch does. A
// submit queues job 7001 and writes the log and record a finished job leaves.
// STUB_JOB_STATES is the list of states squeue shows; STUB_JOB_FOREVER=<state>
// keeps the job in one state; STUB_FOREIGN=1 queues another hb- job in another partition.
const stubSbatch = `#!/bin/bash
printf '%s\n' "$*" >> "$STUB_STATE/sbatch-argv"
case " $* " in
  *" --test-only "*)
    echo "sbatch test-only" >> "$STUB_STATE/events"
    echo "sbatch: Job 424242 to start at 2030-01-01T00:00:00 using 4 processors on nodes seedhost01 in partition p [account SEEDACCT] see https://seed.example/x?token=SEEDTOKEN" >&2
    exit 0 ;;
esac
echo "sbatch submit" >> "$STUB_STATE/events"
name=""; logp=""; exp=""
while [ $# -gt 0 ]; do
  case $1 in -J) name=$2; shift ;; -o) logp=$2; shift ;; --export=*) exp=$1 ;; esac
  shift
done
h=$(printf '%s' "$exp" | sed -n 's/.*,H=\([^,]*\).*/\1/p'); tag=$(printf '%s' "$exp" | sed -n 's/.*,RUN_TAG=\([^,]*\).*/\1/p')
mkdir -p "$h/run-$tag" && echo '{}' > "$h/run-$tag/summary.json"
id=7001
echo "$id|$name|now|plgrid-gpu-gh200|PENDING" >> "$STUB_STATE/queue"
[ -z "${STUB_BREAK_SQUEUE_AFTER_SUBMIT:-}" ] || touch "$STUB_STATE/squeue-broken"
[ -z "${STUB_FOREIGN:-}" ] || echo "9002|hb-controller-xxxxxxxxxxxx|now|plgrid-gpu-gh200|RUNNING" >> "$STUB_STATE/queue"
case "${STUB_JOB_FOREVER:-}" in
  '') printf '%s\n' ${STUB_JOB_STATES:-PENDING RUNNING} > "$STUB_STATE/jobstates-$id" ;;
  1) echo RUNNING > "$STUB_STATE/forever-$id" ;;
  *) echo "$STUB_JOB_FOREVER" > "$STUB_STATE/forever-$id" ;;
esac
log=${logp//%j/$id}
mkdir -p "$(dirname "$log")"
printf '%s\n' "host seedhost01 job 424242" "--- PASS: TestRealEngine (3.00s)" "processes left in the job: ${STUB_PROCS_LEFT:-0}" > "$log"
echo "$id;seedcluster"
`

// stubSacct is empty for its first STUB_SACCT_EMPTY calls, as sacct can be
// right after a job leaves squeue.
const stubSacct = `#!/bin/bash
n=$(cat "$STUB_STATE/sacct-calls" 2>/dev/null || echo 0)
echo $((n+1)) > "$STUB_STATE/sacct-calls"
[ "$n" -ge "${STUB_SACCT_EMPTY:-0}" ] || exit 0
echo "COMPLETED|0:0"
`

const stubScancel = `#!/bin/bash
echo "$1" >> "$STUB_STATE/cancelled"
sed -i "/^$1|/d" "$STUB_STATE/queue"
`

// stubGo stands in for the go command in --run: "go test -c -o" writes the
// stub test binary. It records whether the build lock was held while it ran.
const stubGo = `#!/bin/bash
printf '%s\n' "$*" >> "$STUB_STATE/go-argv"
echo "go $1" >> "$STUB_STATE/events"
if [ -n "${STUB_GO_HOLD:-}" ]; then touch "$STUB_STATE/building"; until [ -e "$STUB_STATE/release" ]; do sleep 0.05; done; fi
if [ -n "${STUB_BUILD_LOCK:-}" ]; then
  if flock -n "$STUB_BUILD_LOCK" true; then echo free >> "$STUB_STATE/go-lock"; else echo held >> "$STUB_STATE/go-lock"; fi
fi
out=""; prev=""
for a in "$@"; do [ "$prev" = -o ] && out=$a; prev=$a; done
case "$1" in
  build) mkdir -p "$(dirname "$out")"; echo binary > "$out"; chmod +x "$out" ;;
  test) mkdir -p "$(dirname "$out")"; cp "$STUB_STATE/testbin.sh" "$out"; chmod +x "$out" ;;
esac
`

// stubTestBinary stands for a compiled live test. Like the real ones it skips
// (and exits 0) when its gate is not met. STUB_TEST_MODE decides the rest.
const stubTestBinary = `#!/bin/bash
run=""; prev=""
for a in "$@"; do [ "$prev" = -test.run ] && run=$a; prev=$a; done
t=${run#^}; t=${t%\$}
echo "testbin $t" >> "$STUB_STATE/events"
if [ -n "${STUB_BUILD_LOCK:-}" ]; then
  if flock -n "$STUB_BUILD_LOCK" true; then echo free >> "$STUB_STATE/testbin-lock"; else echo held >> "$STUB_STATE/testbin-lock"; fi
fi
env | grep -E '^(HPCBURST|HB_)' | sort > "$STUB_STATE/test-env-$t"
echo "log line: host seedhost01 job 424242 key SEEDKEY"
mode=${STUB_TEST_MODE:-pass}
case $t in
  TestConformance) [ "${HPCBURST_CONFORMANCE:-}" = 1 ] && [ -n "${HPCBURST_CONF_ACCOUNT:-}" ] || mode=skip ;;
  TestLLMLabThroughTheFront) [ -n "${HPCBURST_LLMLAB_CREDENTIALS:-}" ] || mode=skip ;;
  TestGoldenHub) [ "${HPCBURST_TEST_HUB:-}" = 1 ] || mode=skip ;;
  TestRealEnginePlan) echo '{}' > "$HB_REAL_PLAN_OUT"; echo "--- PASS: $t (0.01s)"; exit 0 ;;
esac
leave() {
  # Like the real harness: the tag is written down before anything submits it.
  [ -z "${HPCBURST_CONF_OUT:-}" ] || echo "hb-leftover-aaaaaaaaaaaa" >> "$HPCBURST_CONF_OUT/job-tags.txt"
  echo "9001|hb-leftover-aaaaaaaaaaaa|${STUB_JOB_QOS:-normal}|${STUB_JOB_PART:-plgrid}|PENDING" >> "$STUB_STATE/queue"
  # Someone else's hb- job, new in the same partition during the run: not this run's to cancel.
  [ -z "${STUB_FOREIGN:-}" ] || echo "9002|hb-controller-xxxxxxxxxxxx|${STUB_JOB_QOS:-normal}|${STUB_JOB_PART:-plgrid}|RUNNING" >> "$STUB_STATE/queue"
}
case $mode in
  pass) echo "=== RUN   $t"; echo "--- PASS: $t (0.01s)"; echo PASS ;;
  skip) echo "--- SKIP: $t (0.00s)"; echo PASS ;;
  fail) echo "--- FAIL: $t (0.01s)"; echo FAIL; exit 1 ;;
  leave-fail) leave; echo "--- FAIL: $t (0.01s)"; echo FAIL; exit 1 ;;
  leave-pass) leave; echo "--- PASS: $t (0.01s)"; echo PASS ;;
  skip-sub) echo "=== RUN   $t"; echo "    --- SKIP: $t/part (0.00s)"; echo "--- PASS: $t (0.01s)"; echo PASS ;;
  fail-sub) echo "    --- FAIL: $t/SEEDKEY-424242 (0.00s)"; echo "--- FAIL: $t (0.01s)"; echo FAIL; exit 1 ;;
  sleep) leave; touch "$STUB_STATE/started"; exec sleep 3600 ;;
esac
`
