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
  if [ -s "$f" ]; then
    s=$(head -1 "$f"); sed -i 1d "$f"
    [ "$s" != UNREADABLE ] || { echo "squeue: error: unable to contact slurm controller seedhost01" >&2; exit 1; }
    echo "$s"; exit 0
  fi
  sed -i "/^$id|/d" "$Q"
  echo "slurm_load_jobs error: Invalid job id specified" >&2; exit 1
fi
# STUB_SQUEUE_LIST_FAILS_AFTER_POLL=1: the listing (squeue --me, the cleanup's read) fails once the job has been polled by its number, so
# the poll loop reaches its bound and only the cleanup's own read of the queue breaks (#300 review, nit 1).
[ -z "${STUB_SQUEUE_LIST_FAILS_AFTER_POLL:-}" ] || [ ! -e "$STUB_STATE/polled" ] || { echo "squeue: error: unable to contact slurm controller seedhost01" >&2; exit 1; }
# A late registration, counted in reads of the listing after the test binary ran: STUB_HIDE_READS hides the
# late job from the first reads (it is in the queue but not yet shown), and STUB_LATE_JOB is added to the
# queue at read STUB_LATE_AFTER (a job that registers after the first snapshot).
late=""
if [ "$fmt" = '%i|%j|%P' ] && [ -e "$STUB_STATE/ran" ]; then
  r=$(( $(cat "$STUB_STATE/late-reads" 2>/dev/null || echo 0) + 1 )); echo $r > "$STUB_STATE/late-reads"
  [ -z "${STUB_LATE_JOB:-}" ] || [ "$r" -ne "${STUB_LATE_AFTER:-1}" ] || echo "$STUB_LATE_JOB" >> "$Q"
  [ "$r" -gt "${STUB_HIDE_READS:-0}" ] || late=hb-late-bbbbbbbbbbbb
fi
case $fmt in
  '%i|%j|%P') awk -F'|' -v hide="$late" '$2 != hide || hide == "" {print $1"|"$2"|"$4}' "$Q" ;;
  '%q|%P') awk -F'|' '{print $3"|"$4}' "$Q" ;;
esac
`

// stubSbatch: --test-only answers with a seeded line on stderr, as sbatch does. A
// submit queues job 7001 and writes the log and record a finished job leaves.
// STUB_JOB_STATES is the list of states squeue shows (UNREADABLE: that read fails); STUB_JOB_FOREVER=<state>
// keeps the job in one state; STUB_FOREIGN=1 queues another hb- job in another partition.
const stubSbatch = `#!/bin/bash
printf '%s\n' "$*" >> "$STUB_STATE/sbatch-argv"
case " $* " in
  *" --test-only "*)
    echo "sbatch test-only" >> "$STUB_STATE/events"
    if [ -n "${STUB_TESTONLY_REJECT:-}" ]; then echo "sbatch: error: Batch job submission failed: Requested node configuration is not available (account SEEDACCT, seedhost01)" >&2; exit 1; fi
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
//
// With --json it answers the history read of #297 (hack/live-checks.sh, settle_real_engine_not_run) from a file: the real
// GH200 reply of a job cancelled before it started (installStubs puts it in the state directory) unless STUB_SACCT_JSON
// names another file. Only the job number in it is the queried job's (the stub's jobs are 7001 and the like, the capture's
// 4311), which is what the real reply holds for whatever job was asked. STUB_SACCT_JSON_EMPTY=N: nothing for the first N
// of these reads; STUB_SACCT_JSON_RC=N: the command fails with that status and says so on stderr; STUB_SACCT_JSON_SHOWS=
// <state>: the first STUB_SACCT_JSON_LAG reads (default 1) show the job's state as that one, as accounting does before it has the cancel.
// Every read is an event ("sacct json", with the number of the read), so that a test can say what it came after.
const stubSacct = `#!/bin/bash
case " $* " in
  *" --json "*)
    j=""; prev=""
    for a in "$@"; do [ "$prev" = -j ] && j=$a; prev=$a; done
    k=$(( $(cat "$STUB_STATE/sacct-json-calls" 2>/dev/null || echo 0) + 1 )); echo $k > "$STUB_STATE/sacct-json-calls"
    echo "sacct json $k" >> "$STUB_STATE/events"
    printf '%s\n' "$*" >> "$STUB_STATE/sacct-json-argv"
    [ "$k" -gt "${STUB_SACCT_JSON_EMPTY:-0}" ] || exit 0
    if [ -n "${STUB_SACCT_JSON_RC:-}" ]; then echo "sacct: error: slurmdbd is not answering" >&2; exit "$STUB_SACCT_JSON_RC"; fi
    doc=${STUB_SACCT_JSON:-$STUB_STATE/sacct-never-started.json}
    if [ -n "${STUB_SACCT_JSON_SHOWS:-}" ] && [ "$k" -le "${STUB_SACCT_JSON_LAG:-1}" ]; then
      sed -e "s/\"job_id\": 4311,/\"job_id\": $j,/" -e "s/\"current\": \[\"CANCELLED\"\]/\"current\": [\"$STUB_SACCT_JSON_SHOWS\"]/" "$doc"
    else
      sed -e "s/\"job_id\": 4311,/\"job_id\": $j,/" "$doc"
    fi
    exit 0 ;;
esac
n=$(cat "$STUB_STATE/sacct-calls" 2>/dev/null || echo 0)
echo $((n+1)) > "$STUB_STATE/sacct-calls"
[ "$n" -ge "${STUB_SACCT_EMPTY:-0}" ] || exit 0
echo "COMPLETED|0:0"
`

const stubScancel = `#!/bin/bash
echo "$1" >> "$STUB_STATE/cancelled"
echo "scancel $1" >> "$STUB_STATE/events"
[ -z "${STUB_SCANCEL_KEEP:-}" ] || exit 0
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
printf '%s\n' "$*" > "$STUB_STATE/testbin-argv-$t"
touch "$STUB_STATE/ran"
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
  # The rehearsal test skips unless hack/rehearsal.sh runs it (issue #193): a run of the binary on its own is a skip.
  TestRehearsalPart3) [ "${HPCBURST_REHEARSAL:-}" = 1 ] || mode=skip ;;
  TestRealEnginePlan) echo '{}' > "$HB_REAL_PLAN_OUT"; echo "--- PASS: $t (0.01s)"; exit 0 ;;
esac
leave() {
  # Like the real harness: the tag is written down before anything submits it.
  [ -z "${HPCBURST_CONF_OUT:-}" ] || echo "hb-leftover-aaaaaaaaaaaa" >> "$HPCBURST_CONF_OUT/job-tags.txt"
  echo "9001|hb-leftover-aaaaaaaaaaaa|${STUB_JOB_QOS:-normal}|${STUB_JOB_PART:-plgrid}|PENDING" >> "$STUB_STATE/queue"
  # Someone else's hb- job, new in the same partition during the run: not this run's to cancel.
  [ -z "${STUB_FOREIGN:-}" ] || echo "9002|hb-controller-xxxxxxxxxxxx|${STUB_JOB_QOS:-normal}|${STUB_JOB_PART:-plgrid}|RUNNING" >> "$STUB_STATE/queue"
}
notrun() { [ -z "${HPCBURST_CONF_OUT:-}" ] || echo "step 1: a job did not start within the bound" > "$HPCBURST_CONF_OUT/not-run.txt"; }
case $mode in
  pass) echo "=== RUN   $t"; echo "--- PASS: $t (0.01s)"; echo PASS ;;
  skip) echo "--- SKIP: $t (0.00s)"; echo PASS ;;
  fail) echo "--- FAIL: $t (0.01s)"; echo FAIL; exit 1 ;;
  leave-fail) leave; echo "--- FAIL: $t (0.01s)"; echo FAIL; exit 1 ;;
  leave-pass) leave; echo "--- PASS: $t (0.01s)"; echo PASS ;;
  # The left job's tag and STUB_MANY_TAGS more that sort after it (hb-z...), so the owned tags are a long list
  # with the queued job's tag at its top.
  leave-many-tags) leave; seq -f "hb-$(printf 'z%.0s' $(seq 1 55))-%04g" 1 "$STUB_MANY_TAGS" >> "$HPCBURST_CONF_OUT/job-tags.txt"; echo "--- PASS: $t (0.01s)"; echo PASS ;;
  skip-sub) echo "=== RUN   $t"; echo "    --- SKIP: $t/part (0.00s)"; echo "--- PASS: $t (0.01s)"; echo PASS ;;
  fail-sub) echo "    --- FAIL: $t/SEEDKEY-424242 (0.00s)"; echo "--- FAIL: $t (0.01s)"; echo FAIL; exit 1 ;;
  sleep) leave; touch "$STUB_STATE/started"; exec sleep 3600 ;;
  # The test stopped on its start-wait bound: it skips and leaves the marker in its record directory.
  # A submission the test could not settle: its tag is written down, nothing is queued (or the late job is
  # queued, hidden from the first reads), and in the unresolved mode the test says so in unresolved.txt.
  late) echo "hb-late-bbbbbbbbbbbb" >> "$HPCBURST_CONF_OUT/job-tags.txt"; echo "9003|hb-late-bbbbbbbbbbbb|normal|plgrid|PENDING" >> "$STUB_STATE/queue"; echo "--- PASS: $t (0.01s)"; echo PASS ;;
  # A test stopped before it finished writes nothing about what it could not settle; the tag is all there is.
  timed-out) echo "hb-late-bbbbbbbbbbbb" >> "$HPCBURST_CONF_OUT/job-tags.txt"; exit 124 ;;
  sleep-late) echo "hb-late-bbbbbbbbbbbb" >> "$HPCBURST_CONF_OUT/job-tags.txt"; touch "$STUB_STATE/started"; exec sleep 3600 ;;
  late-fail) echo "hb-late-bbbbbbbbbbbb" >> "$HPCBURST_CONF_OUT/job-tags.txt"; echo "--- FAIL: $t (0.01s)"; echo FAIL; exit 1 ;;
  unresolved-symlink) echo "hb-late-bbbbbbbbbbbb" >> "$HPCBURST_CONF_OUT/job-tags.txt"; ln -s "$HPCBURST_CONF_OUT/job-tags.txt" "$HPCBURST_CONF_OUT/unresolved.txt"; echo "--- PASS: $t (0.01s)"; echo PASS ;;
  unresolved-junk) printf '%s\n' 'not a tag; touch /tmp/x' '../../etc/passwd' > "$HPCBURST_CONF_OUT/unresolved.txt"; echo "--- PASS: $t (0.01s)"; echo PASS ;;
  late-tag) echo "hb-late-bbbbbbbbbbbb" >> "$HPCBURST_CONF_OUT/job-tags.txt"; echo "--- PASS: $t (0.01s)"; echo PASS ;;
  late-unresolved) echo "hb-late-bbbbbbbbbbbb" >> "$HPCBURST_CONF_OUT/job-tags.txt"; echo "hb-late-bbbbbbbbbbbb" >> "$HPCBURST_CONF_OUT/unresolved.txt"; echo "--- PASS: $t (0.01s)"; echo PASS ;;
  not-run) notrun; echo "--- SKIP: $t (0.00s)"; echo PASS ;;
  not-run-leave) leave; notrun; echo "--- SKIP: $t (0.00s)"; echo PASS ;;
  not-run-fail) notrun; echo "--- FAIL: $t (0.01s)"; echo FAIL; exit 1 ;;
  not-run-link) echo "x" > "$HPCBURST_CONF_OUT/elsewhere.txt"; ln -s elsewhere.txt "$HPCBURST_CONF_OUT/not-run.txt"; echo "--- SKIP: $t (0.00s)"; echo PASS ;;
  not-run-rc) notrun; echo "--- SKIP: $t (0.00s)"; echo PASS; exit 1 ;;
  not-run-subfail) notrun; echo "    --- FAIL: $t/part (0.00s)"; echo "--- SKIP: $t (0.00s)"; echo FAIL; exit 1 ;;
  not-run-sub) notrun; echo "    --- SKIP: $t/part (0.00s)"; echo "--- SKIP: $t (0.00s)"; echo PASS ;;
esac
`
