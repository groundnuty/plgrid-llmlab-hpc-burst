package livechecks

// The stub cluster. ssh runs the remote script on this machine against fake
// Slurm commands that keep their queue in $STUB_STATE/queue, one job per line:
// id|name|qos|partition|state. Every reply is seeded with a job number, a
// host, an account and a URL with a query, the things the script must not echo.

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
if [ "$1" = "bash -s" ]; then exec bash -s; else exec bash -c "$1"; fi
`

const stubSqueue = `#!/bin/bash
Q="$STUB_STATE/queue"
[ -z "${STUB_SQUEUE_FAIL:-}" ] || { echo "squeue: error: unable to contact slurm controller seedhost01" >&2; exit 1; }
fmt=""; id=""
while [ $# -gt 0 ]; do
  case $1 in -o) fmt=$2; shift ;; -j) id=$2; shift ;; esac
  shift
done
if [ -n "$id" ]; then
  touch "$STUB_STATE/polled"
  if [ -e "$STUB_STATE/forever-$id" ]; then echo RUNNING; exit 0; fi
  f="$STUB_STATE/jobstates-$id"
  if [ -s "$f" ]; then head -1 "$f"; sed -i 1d "$f"; exit 0; fi
  sed -i "/^$id|/d" "$Q"
  echo "slurm_load_jobs error: Invalid job id specified" >&2; exit 1
fi
case $fmt in
  '%i|%j') awk -F'|' '{print $1"|"$2}' "$Q" ;;
  '%q|%P') awk -F'|' '{print $3"|"$4}' "$Q" ;;
esac
`

const stubSbatch = `#!/bin/bash
printf '%s\n' "$*" >> "$STUB_STATE/sbatch-argv"
case " $* " in
  *" --test-only "*)
    echo "sbatch: Job 424242 to start at 2030-01-01T00:00:00 using 4 processors on nodes seedhost01 in partition p [account SEEDACCT] see https://seed.example/x?token=SEEDTOKEN" >&2
    exit 0 ;;
esac
name=""; logp=""; exp=""
while [ $# -gt 0 ]; do
  case $1 in -J) name=$2; shift ;; -o) logp=$2; shift ;; --export=*) exp=$1 ;; esac
  shift
done
h=$(printf '%s' "$exp" | sed -n 's/.*,H=\([^,]*\).*/\1/p'); tag=$(printf '%s' "$exp" | sed -n 's/.*,RUN_TAG=\([^,]*\).*/\1/p')
mkdir -p "$h/run-$tag" && echo '{}' > "$h/run-$tag/summary.json"
id=7001
echo "$id|$name|now|plgrid-gpu-gh200|PENDING" >> "$STUB_STATE/queue"
if [ -n "${STUB_JOB_FOREVER:-}" ]; then touch "$STUB_STATE/forever-$id"; else printf 'PENDING\nRUNNING\n' > "$STUB_STATE/jobstates-$id"; fi
log=${logp//%j/$id}
mkdir -p "$(dirname "$log")"
printf '%s\n' "host seedhost01 job 424242" "--- PASS: TestRealEngine (3.00s)" "processes left in the job: ${STUB_PROCS_LEFT:-0}" > "$log"
echo "$id;seedcluster"
`

const stubSacct = `#!/bin/bash
echo "COMPLETED|0:0"
`

const stubScancel = `#!/bin/bash
echo "$1" >> "$STUB_STATE/cancelled"
sed -i "/^$1|/d" "$STUB_STATE/queue"
`

// stubGo stands in for the go command in --run: it writes the stub test
// binary where "go test -c -o" asks for one, and a plan where the plan test would.
const stubGo = `#!/bin/bash
printf '%s\n' "$*" >> "$STUB_STATE/go-argv"
out=""; prev=""
for a in "$@"; do [ "$prev" = -o ] && out=$a; prev=$a; done
case "$1" in
  build) mkdir -p "$(dirname "$out")"; echo binary > "$out"; chmod +x "$out" ;;
  test)
    case " $* " in
      *" -c "*) mkdir -p "$(dirname "$out")"; cp "$STUB_STATE/testbin.sh" "$out"; chmod +x "$out" ;;
      *) [ -z "${HB_REAL_PLAN_OUT:-}" ] || echo '{}' > "$HB_REAL_PLAN_OUT" ;;
    esac ;;
esac
`

// stubTestBinary stands for a compiled live test. Like the real ones it skips
// (and exits 0) when its gate is not met. STUB_TEST_MODE decides the rest.
const stubTestBinary = `#!/bin/bash
run=""; prev=""
for a in "$@"; do [ "$prev" = -test.run ] && run=$a; prev=$a; done
t=${run#^}; t=${t%\$}
env | grep -E '^(HPCBURST|HB_)' | sort > "$STUB_STATE/test-env-$t"
echo "log line: host seedhost01 job 424242 key SEEDKEY"
mode=${STUB_TEST_MODE:-pass}
case $t in
  TestConformance) [ "${HPCBURST_CONFORMANCE:-}" = 1 ] && [ -n "${HPCBURST_CONF_ACCOUNT:-}" ] || mode=skip ;;
  TestLLMLabThroughTheFront) [ -n "${HPCBURST_LLMLAB_CREDENTIALS:-}" ] || mode=skip ;;
  TestGoldenHub) [ "${HPCBURST_TEST_HUB:-}" = 1 ] || mode=skip ;;
esac
leave() { echo "9001|hb-leftover-aaaaaaaaaaaa|${STUB_JOB_QOS:-normal}|${STUB_JOB_PART:-plgrid}|PENDING" >> "$STUB_STATE/queue"; }
case $mode in
  pass) echo "=== RUN   $t"; echo "--- PASS: $t (0.01s)"; echo PASS ;;
  skip) echo "--- SKIP: $t (0.00s)"; echo PASS ;;
  fail) echo "--- FAIL: $t (0.01s)"; echo FAIL; exit 1 ;;
  leave-fail) leave; echo "--- FAIL: $t (0.01s)"; echo FAIL; exit 1 ;;
  leave-pass) leave; echo "--- PASS: $t (0.01s)"; echo PASS ;;
  sleep) leave; touch "$STUB_STATE/started"; exec sleep 30 ;;
esac
`
