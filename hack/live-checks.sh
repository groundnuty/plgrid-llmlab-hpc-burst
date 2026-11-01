#!/usr/bin/env bash
# One entry point for the live-only tests: the tests that run against a real
# system and never in CI (issue #163; HBEP-15, Upgrading a pinned dependency,
# "Live re-run after merge").
#
#   hack/live-checks.sh --list
#   hack/live-checks.sh --plan <base>..<head>        which live tests cover what the range changed; runs nothing
#   hack/live-checks.sh --run <name>... [--yes]      runs only the named tests
#
# --plan maps the range's changed files to the code each live test runs, read from
# the two ends of the range (copies of their trees, not the current checkout): the
# packages the test's package imports (go list -deps -test, in the host build and,
# for the real-engine run, the linux/arm64 build), the files they embed wherever
# those sit, and the inputs the table names. A test file counts only for its own
# package. A discovery that fails, or lists nothing, fails the plan (exit 2): it
# never reads as "no live test covers this range".
#
# --run gates each test by its environment, set from the settings file (below),
# and reads every credential from its own mode-600 file. A cluster test first
# prints what it would use and the queue estimate (sbatch --test-only), then
# runs only with --yes. A skipped test is a failure, not a pass.
#
# Exit codes: 0 all passed; 1 a test failed, skipped, left a job behind or
# timed out; 2 usage error or an unreadable range; 3 refused (a setting is
# missing, a file is not mode 600, the one-job slot is busy); 4 a cluster test
# was not run because --yes was not given.
#
# The settings file is named by HPCBURST_LIVE_SETTINGS: KEY=VALUE lines, mode
# 600, owned by you. Keys (none is ever printed):
#   LIVE_OUT_DIR                 absolute path of a private directory for logs and records (always)
#   LIVE_LOCK_DIR                absolute path of the directory with the hosts' lock files (cluster tests)
#   LIVE_BUILD_LOCK              optional absolute path of the shared build lock; builds, never test runs, take it
#   LIVE_LLMLAB_CREDENTIALS      absolute path of the managed credentials file, mode 600 (llmlab-smoke)
#   LIVE_LLMLAB_MODEL            optional model override (llmlab-smoke)
#   LIVE_CONF_CREDENTIALS_HELIOS, LIVE_CONF_CREDENTIALS_ATHENA
#                                the ssh credentials files of the conformance runs, mode 600
#   ACCOUNT_HELIOS_CPU, ACCOUNT_HELIOS_GH200, ACCOUNT_ATHENA_A100
#                                the grant account of each job kind
# Cluster calls go through the ssh aliases helios and athena, one at a time per
# host under <LIVE_LOCK_DIR>/<host>.lock. The aliases' ssh configuration is yours.
#
# What reaches the screen is statuses, counts and fixed labels: never a key, a URL, a job ID, a host, an
# account, a private path or the name of a failed subtest. A tool's own diagnostics, every remote reply and
# every test log go to files in LIVE_OUT_DIR. Those files are raw (a test's log can hold anything the test
# printed): the screen is clean, they are not, and they are mode 600 in a mode-700 tree.
#
# Development rule for the one-job slots (QOS now on Helios, plgrid-now on
# Athena; owner, 1 Nov 2026, issue #145): the script refuses to queue behind a
# job that already holds the slot, runs one live run per host at a time with
# the shortest time limit the test allows, builds before it takes the slot, and
# on any exit (a failure or a signal included) cancels what this run owns, and
# only that: the real-engine job by its ID (its name is written down before the
# submit, so a lost reply is found by it), a conformance run's jobs by the tags
# its test wrote down before submitting (job-tags.txt). A job that merely
# appeared in the same partition is never touched. It prints each slot use as a
# UTC start, end and duration, to post on the issue the run served.
#
# The table below is the one map from a live test to its package. Add a row
# when a test is added, and a //livecheck: directive on its Test function;
# hack/livechecks fails when a package holds a live test that no row names, when
# a Test function of a live file has no directive, or when a row's -run pattern
# and the functions that name the row disagree.

set -u -o pipefail

SELF=${BASH_SOURCE[0]}
ROOT=$(cd "$(dirname "$SELF")/.." && pwd -P)

# name | kind | package | test | gate variables | non-Go inputs | build contexts | status
# Inputs are files, directories or globs of the repository that the test reads without importing them.
# Build contexts are the ones whose dependency closure the test runs: host, and linux-arm64 for the binaries
# the real-engine job runs on the cluster. hack/livechecks checks this table against the tree: every test
# function of a live file carries a //livecheck: directive that names its rows (or says none and why), and
# every row's test pattern matches exactly the functions that name it.
live_table() {
  cat <<'TABLE'
# BEGIN LIVE TESTS
llmlab-smoke|local|internal/front|^TestLLMLabThroughTheFront$|HPCBURST_LLMLAB_CREDENTIALS|-|host|ready
hub-golden|local|pkg/hubfiles|^TestGoldenHub$|HPCBURST_TEST_HUB|spikes/s7/checkpoints.env,spikes/s7/results/run1/hub,spikes/s7/results/run1/hash-*.jsonl|host|ready
conformance-helios|cluster|internal/providerctl/conformance|^TestConformance$|HPCBURST_CONFORMANCE|profiles/helios.yaml|host|ready
conformance-athena|cluster|internal/providerctl/conformance|^TestConformance$|HPCBURST_CONFORMANCE|profiles/athena.yaml|host|ready
real-engine|cluster|cmd/hb-agent|^TestRealEngine$|HB_REAL_ENGINE_PLAN|spikes/s94/job.sh|host,linux-arm64|ready
rehearsal|cluster|-|-|-|-|-|pending
# END LIVE TESTS
TABLE
}

# What a cluster test asks of the scheduler. name | host | profile | partition |
# qos | slot | account key | GPUs | CPUs | memory | time limit | timeout (min) | extra.
# slot is the token that marks the one-job slot in squeue's QOS or partition column.
# The conformance time limit is the harness's own (TimeLimit "5m", conformance_test.go);
# the real-engine one is the documented run's (spikes/s94/README.md).
cluster_table() {
  cat <<'TABLE'
# BEGIN CLUSTER SHAPES
conformance-helios|helios|helios|plgrid|-|-|ACCOUNT_HELIOS_CPU|0|1|1Gi|00:05:00|80|-
conformance-athena|athena|athena|plgrid-now|-|plgrid-now|ACCOUNT_ATHENA_A100|1|4|32Gi|00:05:00|80|-
real-engine|helios|-|plgrid-gpu-gh200|now|now|ACCOUNT_HELIOS_GH200|1|72|120000M|00:57:00|75|-C memfs
# END CLUSTER SHAPES
TABLE
}

LOCAL_TIMEOUT_MIN=15
POLL_SECONDS=${LIVE_POLL_SECONDS:-60}
CLEAN_POLL_SECONDS=${LIVE_CLEAN_POLL_SECONDS:-3}

declare -a NAMES=()
declare -A KIND PKG TEST GATES INPUTS CONTEXTS STATUS
declare -A HOST PROFILE PARTITION QOS SLOT ACCTKEY GPUS CPUS MEM TLIMIT TMIN EXTRA
declare -A S=()

load_tables() {
  local name kind pkg test gates inputs contexts status
  while IFS='|' read -r name kind pkg test gates inputs contexts status; do
    [ -n "$name" ] || continue
    NAMES+=("$name")
    KIND[$name]=$kind PKG[$name]=$pkg TEST[$name]=$test GATES[$name]=$gates INPUTS[$name]=$inputs CONTEXTS[$name]=$contexts STATUS[$name]=$status
  done < <(live_table | grep -v -e '^#' -e '^$')
  local h pr pa q sl ak g c m tl tm ex
  while IFS='|' read -r name h pr pa q sl ak g c m tl tm ex; do
    [ -n "$name" ] || continue
    HOST[$name]=$h PROFILE[$name]=$pr PARTITION[$name]=$pa QOS[$name]=$q SLOT[$name]=$sl ACCTKEY[$name]=$ak
    GPUS[$name]=$g CPUS[$name]=$c MEM[$name]=$m TLIMIT[$name]=$tl TMIN[$name]=$tm EXTRA[$name]=$ex
  done < <(cluster_table | grep -v -e '^#' -e '^$')
}

say() { printf '%s\n' "$*"; }
usage() {
  cat <<'USAGE'
usage: hack/live-checks.sh --list
       hack/live-checks.sh --plan <base>..<head>      which live tests cover the range; runs nothing
       hack/live-checks.sh --run <name>... [--yes]    runs only the named tests (a cluster test needs --yes)
Settings: HPCBURST_LIVE_SETTINGS names a mode-600 KEY=VALUE file; the keys are listed in this script's header.
Exit: 0 passed; 1 failed, skipped or left a job; 2 usage; 3 refused; 4 cluster test not run (no --yes).
USAGE
  exit "${1:-2}"
}
refuse() { say "live-checks: refused: $*"; return 3; }

# ---------------------------------------------------------------- settings

# private_file <key> <path>: a regular file, mode 600, owned by this user. Names the key, never the path.
private_file() {
  local key=$1 path=$2
  case $path in /*) ;; *) say "live-checks: $key must be an absolute path"; return 1 ;; esac
  [ -f "$path" ] && [ ! -L "$path" ] || { say "live-checks: $key does not name a regular file"; return 1; }
  [ "$(stat -c %a "$path" 2>/dev/null)" = 600 ] && [ "$(stat -c %u "$path" 2>/dev/null)" = "$(id -u)" ] || {
    say "live-checks: $key names a file that is not mode 600 and owned by you"; return 1; }
}

load_settings() {
  local line k v
  [ -n "${HPCBURST_LIVE_SETTINGS:-}" ] || { say "live-checks: HPCBURST_LIVE_SETTINGS is not set"; return 1; }
  private_file HPCBURST_LIVE_SETTINGS "$HPCBURST_LIVE_SETTINGS" || return 1
  while IFS= read -r line || [ -n "$line" ]; do
    case $line in ''|'#'*) continue ;; esac
    line=${line#export }
    k=${line%%=*}; v=${line#*=}
    [ "$k" != "$line" ] || continue
    v=${v%\"}; v=${v#\"}; v=${v%\'}; v=${v#\'}
    case $k in
      LIVE_OUT_DIR|LIVE_LOCK_DIR|LIVE_BUILD_LOCK|LIVE_LLMLAB_CREDENTIALS|LIVE_LLMLAB_MODEL|LIVE_CONF_CREDENTIALS_HELIOS|LIVE_CONF_CREDENTIALS_ATHENA|ACCOUNT_HELIOS_CPU|ACCOUNT_HELIOS_GH200|ACCOUNT_ATHENA_A100)
        S[$k]=$v ;;
    esac
  done 2>/dev/null < "$HPCBURST_LIVE_SETTINGS"
}

# check_settings <name>...: every setting the named tests need, all problems reported at once.
check_settings() {
  local name bad=0 acct key
  need() { [ -n "${S[$1]:-}" ] || { say "live-checks: setting $1 is missing"; bad=1; return 1; }; }
  need LIVE_OUT_DIR && case ${S[LIVE_OUT_DIR]} in /*) ;; *) say "live-checks: LIVE_OUT_DIR must be an absolute path"; bad=1 ;; esac
  if [ -n "${S[LIVE_BUILD_LOCK]:-}" ]; then
    case ${S[LIVE_BUILD_LOCK]} in /*) ;; *) say "live-checks: LIVE_BUILD_LOCK must be an absolute path"; bad=1 ;; esac
  fi
  for name in "$@"; do
    case $name in
      llmlab-smoke) need LIVE_LLMLAB_CREDENTIALS && { private_file LIVE_LLMLAB_CREDENTIALS "${S[LIVE_LLMLAB_CREDENTIALS]}" || bad=1; } ;;
      hub-golden) ;;
      *)
        [ "${KIND[$name]}" = cluster ] || continue
        need LIVE_LOCK_DIR && { [ -d "${S[LIVE_LOCK_DIR]}" ] || { say "live-checks: LIVE_LOCK_DIR is not a directory"; bad=1; }; }
        key=${ACCTKEY[$name]}
        if need "$key"; then
          acct=${S[$key]}
          [[ $acct =~ ^[A-Za-z0-9_.-]+$ ]] || { say "live-checks: setting $key holds characters an account does not"; bad=1; }
        fi
        if [ "${PROFILE[$name]}" != - ]; then
          key=LIVE_CONF_CREDENTIALS_$(printf '%s' "${HOST[$name]}" | tr a-z A-Z)
          need "$key" && { private_file "$key" "${S[$key]}" || bad=1; }
        fi ;;
    esac
  done
  return $bad
}

# ---------------------------------------------------------------- the cluster

OUT=""
ACTIVE_HOST=""      # the host of the cluster test in flight, for cleanup
JOBID=""            # the real-engine job this run submitted
CHILD=""
INTERRUPTED=""
RUN_FD=""

# remote <host>: runs the script on stdin on the host's login node, one ssh call at a time per host.
# stderr goes to the private remote log: it can name the account.
remote() {
  flock -o -w 600 "${S[LIVE_LOCK_DIR]}/$1.lock" timeout 300 ssh -o BatchMode=yes "$1" 'bash -s' 2>>"$OUT/remote.log"
}

# preamble <var> <value>...: assignments for the head of a remote script, quoted.
preamble() { local k; while [ $# -ge 2 ]; do k=$1; printf '%s=%q\n' "$k" "$2"; shift 2; done; }

# queue_jobs <host>: the user's hb- jobs, one "id|name|partition" per line.
# Returns 1 when squeue could not be read, so an unreadable queue is never taken for an empty one.
queue_jobs() { # id|name|partition lines
  local reply
  reply=$(remote "$1" <<'EOS'
out=$(squeue --me -h -o '%i|%j|%P') || { echo "squeue=failed"; exit 0; }
echo "squeue=ok"
printf '%s\n' "$out" | grep '|hb-' || true
EOS
  ) || return 1
  # The marker line, wherever it sits: a login banner can come before it.
  printf '%s\n' "$reply" | grep -q -x 'squeue=ok' || return 1
  printf '%s\n' "$reply" | sed -n '/^squeue=ok$/,$p' | tail -n +2 | grep -E '^[0-9][0-9_+]*\|hb-[A-Za-z0-9._-]*\|[A-Za-z0-9._-]+$' || true
}

# slot_jobs <host> <token>: how many of the user's jobs sit in the slot, or "unknown".
slot_jobs() {
  local reply n
  reply=$({ preamble SLOT "$2"; cat <<'EOS'
out=$(squeue --me -h -o '%q|%P') || { echo "slot_jobs=unknown"; exit 0; }
printf '%s\n' "$out" | awk -F'|' -v s="$SLOT" '$1==s||$2==s{n++} END{print "slot_jobs=" n+0}'
EOS
  } | remote "$1") || { echo unknown; return; }
  n=$(printf '%s\n' "$reply" | sed -n 's/^slot_jobs=\([0-9][0-9]*\)$/\1/p' | head -1)
  echo "${n:-unknown}"
}

slurm_mem() {
  case $1 in
    *Gi) echo "$(( ${1%Gi} * 1024 ))M" ;;
    *M) echo "$1" ;;
    *) return 1 ;;
  esac
}

# shape_opts <name>: the scheduler options of the test, as the real job would pass them.
shape_opts() {
  local n=$1 o
  o="-p ${PARTITION[$n]}"
  [ "${QOS[$n]}" = - ] || o="$o -q ${QOS[$n]}"
  o="$o -N 1 --ntasks=1 -c ${CPUS[$n]} --mem=$(slurm_mem "${MEM[$n]}")"
  [ "${GPUS[$n]}" = 0 ] || o="$o --gres=gpu:${GPUS[$n]}"
  o="$o -t ${TLIMIT[$n]}"
  [ "${EXTRA[$n]}" = - ] || o="$o ${EXTRA[$n]}"
  echo "$o --no-requeue"
}

fmt_wait() {
  local s=$1
  if [ "$s" -le 0 ]; then say "now"; return; fi
  printf '%dd %dh %dm\n' $((s/86400)) $((s%86400/3600)) $((s%3600/60))
}

# estimate <name>: prints the queue estimate from sbatch --test-only. Nothing from the reply but two numbers.
estimate() {
  local n=$1 acct reply rc wait_s
  acct=${S[${ACCTKEY[$n]}]}
  reply=$({ preamble ACCOUNT "$acct" SHAPE_OPTS "$(shape_opts "$n")"; cat <<'EOS'
out=$(sbatch --test-only -A "$ACCOUNT" $SHAPE_OPTS --wrap true 2>&1); echo "rc=$?"
ts=$(printf '%s\n' "$out" | sed -n 's/.* to start at \([0-9T:-]*\) .*/\1/p' | head -1)
if [ -n "$ts" ]; then
  now=$(date +%s); at=$(date -d "$ts" +%s 2>/dev/null) && echo "wait_s=$((at-now))" || echo "wait_s=unknown"
else
  echo "wait_s=none"
fi
EOS
  } | remote "${HOST[$n]}") || { say "$n: queue estimate: the cluster could not be reached"; return 1; }
  rc=$(printf '%s\n' "$reply" | sed -n 's/^rc=\([0-9][0-9]*\)$/\1/p' | head -1)
  wait_s=$(printf '%s\n' "$reply" | sed -n 's/^wait_s=\(-\{0,1\}[0-9][0-9]*\)$/\1/p' | head -1)
  if [ "${rc:-1}" = 0 ] && [ -n "$wait_s" ]; then
    say "$n: queue estimate: a start in about $(fmt_wait "$wait_s")"
  else
    say "$n: queue estimate: none (sbatch --test-only gave no start time)"
  fi
}

# cancel_ids <host> <ids...>: scancel each id; prints how many were cancelled.
cancel_ids() {
  local host=$1 ids reply
  shift
  [ $# -gt 0 ] || { echo 0; return; }
  ids="$*"
  reply=$({ preamble IDS "$ids"; cat <<'EOS'
for i in $IDS; do
  case $i in ''|*[!0-9_+]*) continue ;; esac
  if scancel "$i" >/dev/null 2>&1; then echo cancelled=1; else echo cancelled=0; fi
done
EOS
  } | remote "$host") || { echo 0; return; }
  printf '%s\n' "$reply" | grep -c '^cancelled=1$' || true
}

# own_tags: the job names this run submitted (or may have submitted), one per line, from OWN_TAGS_FILE.
# The real-engine run writes its tag there before it submits; a conformance run's test writes each
# Allocation's tag before anything can submit it (job-tags.txt). Only a name of this shape is accepted.
OWN_TAGS_FILE=""
own_tags() {
  [ -n "$OWN_TAGS_FILE" ] && [ -f "$OWN_TAGS_FILE" ] || return 0
  grep -E '^hb-[A-Za-z0-9._-]{1,60}$' "$OWN_TAGS_FILE" 2>/dev/null | sort -u || true
}

# clean_cluster: cancels what this run owns, then checks. Ownership is this run's own: the job ID it was
# given (real-engine), and the jobs named by a tag it wrote down before submitting. A job that merely
# appeared in the same partition is never touched: the owner's controller or a manual run does not take
# this script's lock. The known ID is cancelled even when squeue cannot be read; the failed verification is
# reported apart, and the EXIT trap tries once more. Idempotent; sets CLEAN_LEFT.
CLEAN_LEFT=0
CLEAN_TRIES=0
clean_cluster() {
  [ -n "$ACTIVE_HOST" ] || return 0
  local host=$ACTIVE_HOST now="" ids="" tags line id name cancelled=0 left=0 try readable=1
  CLEAN_TRIES=$((CLEAN_TRIES+1))
  tags=$(own_tags)
  [ -z "$JOBID" ] || ids=$JOBID
  now=$(queue_jobs "$host") || readable=0
  if [ "$readable" = 1 ] && [ -n "$tags" ]; then
    while IFS='|' read -r id name _; do
      [ -n "$id" ] || continue
      printf '%s\n' "$tags" | grep -q -x -F "$name" || continue
      case " $ids " in *" $id "*) ;; *) ids="$ids $id" ;; esac
    done <<<"$now"
  fi
  # shellcheck disable=SC2086
  [ -z "$ids" ] || cancelled=$(cancel_ids "$host" $ids)
  if [ "$readable" = 1 ] && [ -n "$ids" ]; then
    # A cancelled job can show as completing for a few seconds: look again before calling it left behind.
    for try in 1 2 3 4 5 6 7 8 9 10; do
      now=$(queue_jobs "$host") || { readable=0; break; }
      left=0
      for id in $ids; do printf '%s\n' "$now" | grep -q "^${id}|" && left=$((left+1)); done
      [ "$left" -eq 0 ] && break
      sleep "$CLEAN_POLL_SECONDS"
    done
  fi
  if [ "$readable" = 0 ]; then
    say "cleanup: the queue could not be read, so the cleanup could not look for this run's jobs by tag or confirm a cancel (cancelled $cancelled known job(s)); check the queue by hand"
    CLEAN_LEFT=1
    [ "$CLEAN_TRIES" -lt 2 ] || ACTIVE_HOST=""
    return 1
  fi
  say "cleanup: left behind $left (cancelled $cancelled)"
  [ "$left" -eq 0 ] || CLEAN_LEFT=1
  ACTIVE_HOST=""
  return 0
}

on_signal() { INTERRUPTED=1; }
on_exit() {
  local rc=$?
  if [ -n "$ACTIVE_HOST" ]; then clean_cluster || rc=1; [ "$CLEAN_LEFT" -eq 0 ] || rc=1; fi
  [ -z "${CONTROL_DIR:-}" ] || rm -rf "$CONTROL_DIR"
  exit "$rc"
}

# wait_child: waits for CHILD; a signal to this script stops it. Sets CHILD_RC.
wait_child() {
  local i
  CHILD_RC=0
  wait "$CHILD" || CHILD_RC=$?
  if [ -n "$INTERRUPTED" ]; then
    kill -TERM "$CHILD" 2>/dev/null
    for i in $(seq 1 50); do kill -0 "$CHILD" 2>/dev/null || break; sleep 0.2; done
    kill -KILL "$CHILD" 2>/dev/null
    wait "$CHILD" 2>/dev/null
    CHILD_RC=130
  fi
  CHILD=""
}

# ---------------------------------------------------------------- results

# judge <name> <log> <rc>: the verdict from the test's verbose log and exit status. A skip is not a pass, at
# any level. Only counts and fixed labels reach the screen: a subtest's name can hold anything.
judge() {
  local n=$1 log=$2 rc=$3 test pass fail skip named
  test=${TEST[$n]#^}; test=${test%\$}
  pass=$(grep -c -E '^--- PASS: ' "$log" 2>/dev/null); fail=$(grep -c -E '^ *--- FAIL: ' "$log" 2>/dev/null)
  skip=$(grep -c -E '^ *--- SKIP: ' "$log" 2>/dev/null)
  named=$(sed -n -E "s/^--- (PASS|FAIL|SKIP): ${test}( .*)?\$/\\1/p" "$log" 2>/dev/null | head -1)
  say "$n: tests: ${pass:-0} passed, ${fail:-0} failed, ${skip:-0} skipped"
  if [ "$rc" = 130 ]; then say "$n: FAIL (interrupted)"; return 1; fi
  if [ "$rc" = 124 ]; then say "$n: FAIL (timed out)"; return 1; fi
  if [ "$named" = SKIP ]; then say "$n: FAIL (skipped: its gate was not met, so nothing ran)"; return 1; fi
  if [ "${skip:-0}" != 0 ]; then say "$n: FAIL (a subtest was skipped, so part of the run did not happen)"; return 1; fi
  if [ "$rc" = 0 ] && [ "$named" = PASS ] && [ "${fail:-0}" = 0 ]; then say "$n: PASS"; return 0; fi
  [ "${fail:-0}" = 0 ] || say "$n: ${fail} test(s) failed; their names are in the log in the output directory"
  say "$n: FAIL (exit status $rc)"
  return 1
}

# ---------------------------------------------------------------- running

CONTROL_DIR=""
REC_DIR=""

# with_build_lock <command>...: runs the command under the shared build lock when LIVE_BUILD_LOCK names one.
# Only builds take it; a test binary never runs while it is held.
with_build_lock() {
  if [ -n "${S[LIVE_BUILD_LOCK]:-}" ]; then flock -o -w 1800 "${S[LIVE_BUILD_LOCK]}" "$@"; else "$@"; fi
}

build_test_binary() { # <name> <bin>
  local n=$1 bin=$2
  ( cd "$ROOT" && with_build_lock go test -c -o "$bin" "./${PKG[$n]}" ) >"$OUT/$n-build.log" 2>&1 || {
    say "$n: FAIL (the test binary did not build; see the build log in the output directory)"; return 1; }
}

# run_binary <name> <bin> <timeout min> <env-setup function>: runs the test binary in its package directory.
run_binary() {
  local n=$1 bin=$2 tmin=$3 setup=$4
  (
    [ -z "$RUN_FD" ] || eval "exec ${RUN_FD}>&-"
    "$setup" "$n"
    cd "$ROOT/${PKG[$n]}" || exit 1
    exec timeout "${tmin}m" "$bin" -test.run "${TEST[$n]}" -test.v -test.count=1 -test.timeout "$((tmin+1))m"
  ) >"$OUT/$n.log" 2>&1 &
  CHILD=$!
  wait_child
}

env_llmlab() {
  export HPCBURST_LLMLAB_CREDENTIALS=${S[LIVE_LLMLAB_CREDENTIALS]}
  [ -z "${S[LIVE_LLMLAB_MODEL]:-}" ] || export HPCBURST_LLMLAB_MODEL=${S[LIVE_LLMLAB_MODEL]}
}
env_hub() { export HPCBURST_TEST_HUB=1; }
env_conformance() {
  local n=$1 host=${HOST[$1]} up
  up=$(printf '%s' "$host" | tr a-z A-Z)
  unset HPCBURST_CONF_GPU_MODEL HPCBURST_CONF_CPU_ARCH HPCBURST_CONF_DATA_PARSER
  export HPCBURST_CONFORMANCE=1 HPCBURST_CONF_PROFILE=${PROFILE[$n]} HPCBURST_CONF_PARTITION=${PARTITION[$n]}
  export HPCBURST_CONF_CREDENTIALS=${S[LIVE_CONF_CREDENTIALS_$up]} HPCBURST_CONF_ACCOUNT=${S[${ACCTKEY[$n]}]}
  export HPCBURST_CONF_GPUS=${GPUS[$n]} HPCBURST_CONF_CPUS=${CPUS[$n]} HPCBURST_CONF_MEMORY=${MEM[$n]}
  # shellcheck disable=SC2016
  export HPCBURST_CONF_STAGING='$SCRATCH/hpc-burst-conformance'
  export HPCBURST_CONF_HOST_LOCK=${S[LIVE_LOCK_DIR]}/$host.lock HPCBURST_CONF_CONTROL_DIR=$CONTROL_DIR
  export HPCBURST_CONF_OUT=$REC_DIR
}

run_local() { # <name>
  local n=$1 bin="$OUT/$1.test" setup
  case $n in llmlab-smoke) setup=env_llmlab ;; hub-golden) setup=env_hub ;; esac
  build_test_binary "$n" "$bin" || return 1
  [ -z "$INTERRUPTED" ] || { say "$n: FAIL (interrupted before the run started)"; return 1; }
  run_binary "$n" "$bin" "$LOCAL_TIMEOUT_MIN" "$setup"
  judge "$n" "$OUT/$n.log" "$CHILD_RC"
}

# slot_report <name> <start epoch> <end epoch>
slot_report() {
  local n=$1 t0=$2 t1=$3 what="cluster run (no one-job slot)"
  [ "${SLOT[$n]}" = - ] || what="slot use (${SLOT[$n]})"
  say "$n: $what: start $(date -u -d "@$t0" +%Y-%m-%dT%H:%M:%SZ) end $(date -u -d "@$t1" +%Y-%m-%dT%H:%M:%SZ) duration $(printf '%02d:%02d:%02d' $(((t1-t0)/3600)) $(((t1-t0)%3600/60)) $(((t1-t0)%60)))"
}

# check_slot <name>: refuses (3) when the one-job slot already holds a job of the user's.
check_slot() {
  local n=$1 held
  held=$(slot_jobs "${HOST[$n]}" "${SLOT[$n]}")
  case $held in
    0) return 0 ;;
    unknown) refuse "$n: the slot could not be read, so it is taken as busy"; return 3 ;;
    *) refuse "$n: the one-job slot (${SLOT[$n]}) already holds $held job(s) of yours; not queuing behind it"; return 3 ;;
  esac
}

# prepare_cluster <name>: everything that needs no slot: the builds, and for the real-engine run
# the plan and the staging. Not part of the slot's use.
prepare_cluster() {
  case $1 in
    real-engine) prepare_real_engine "$1" ;;
    *) build_test_binary "$1" "$OUT/$1.test" ;;
  esac
}

execute_cluster() {
  local n=$1
  case $n in
    real-engine) execute_real_engine "$n" ;;
    *) run_binary "$n" "$OUT/$n.test" "${TMIN[$n]}" env_conformance; judge "$n" "$OUT/$n.log" "$CHILD_RC" ;;
  esac
}

release_run_lock() { [ -z "$RUN_FD" ] || { eval "exec ${RUN_FD}>&-"; RUN_FD=""; }; }

# run_cluster <name>: preflight, estimate, the --yes gate, the builds, the run lock and a second look at
# the slot, the run, the cleanup, the report. The slot's use starts after the builds.
run_cluster() {
  local n=$1 host=${HOST[$1]} t0 t1 verdict=0
  local qos=""; [ "${QOS[$n]}" = - ] || qos=", qos ${QOS[$n]}"
  say "$n: shape: partition ${PARTITION[$n]}$qos, ${GPUS[$n]} GPU, ${CPUS[$n]} CPU, ${MEM[$n]}, time limit ${TLIMIT[$n]}, one job, no requeue"
  if [ "${SLOT[$n]}" != - ]; then check_slot "$n" || return 3; fi
  estimate "$n" || true
  if [ "$YES" != 1 ]; then say "$n: not run: pass --yes to run it"; return 4; fi
  prepare_cluster "$n" || return 1
  [ -z "$INTERRUPTED" ] || { say "$n: FAIL (interrupted before the run started)"; return 1; }
  # One live run per host at a time, slot or not: the cleanup below is only safe if no other run is here.
  exec {RUN_FD}>"${S[LIVE_LOCK_DIR]}/$host-live.lock"
  flock -n "$RUN_FD" || { release_run_lock; refuse "$n: another live run holds this host's run lock"; return 3; }
  if [ "${SLOT[$n]}" != - ]; then check_slot "$n" || { release_run_lock; return 3; }; fi
  # A queue that cannot be read now could not be read for the cleanup either: refuse before submitting.
  queue_jobs "$host" >/dev/null || { release_run_lock; say "$n: FAIL (the queue could not be read before the run)"; return 1; }
  [ -z "$INTERRUPTED" ] || { release_run_lock; say "$n: FAIL (interrupted before the run started)"; return 1; }
  # Each run records in a directory of its own, so a tag file of an earlier run is never read as this run's.
  REC_DIR="$OUT/$n-$(date -u +%Y%m%dT%H%M%SZ)-$$"
  mkdir -m 700 "$REC_DIR" || { release_run_lock; say "$n: FAIL (the record directory could not be made)"; return 1; }
  if [ "$n" = real-engine ]; then OWN_TAGS_FILE="$RE_DIR/submission-tag"; else OWN_TAGS_FILE="$REC_DIR/job-tags.txt"; fi
  JOBID=""; CLEAN_TRIES=0
  ACTIVE_HOST=$host
  t0=$(date +%s)
  execute_cluster "$n" || verdict=1
  clean_cluster || true
  t1=$(date +%s)
  slot_report "$n" "$t0" "$t1"
  [ "$CLEAN_LEFT" -eq 0 ] || verdict=1
  release_run_lock
  return $verdict
}

# job_state <host> <id>: the job's squeue state, "gone" once it has left the queue, "error" if unreadable.
job_state() {
  local reply
  reply=$({ preamble JOB "$2"; cat <<'EOS'
out=$(squeue -j "$JOB" -h -o %T 2>&1); rc=$?
if [ $rc -eq 0 ] && [ -z "$out" ]; then echo "state=gone"
elif [ $rc -eq 0 ]; then echo "state=$out"
elif printf '%s' "$out" | grep -q -i 'invalid job id'; then echo "state=gone"
else echo "state=error"; fi
EOS
  } | remote "$1") || { echo error; return; }
  printf '%s\n' "$reply" | sed -n -E 's/^state=([A-Z_]{1,24}|gone|error)$/\1/p' | head -1
}

# now_epoch: the clock for the job's budgets. LIVE_FAKE_CLOCK is a test seam: a file holding the epoch seconds.
now_epoch() { if [ -n "${LIVE_FAKE_CLOCK:-}" ]; then cat "$LIVE_FAKE_CLOCK"; else date +%s; fi; }

RE_DIR=""
RE_TAG=""

# prepare_real_engine <name>: build the arm64 agent and its test binary, write the plan, stage all of it.
# Nothing here holds the slot.
prepare_real_engine() {
  local n=$1 host=${HOST[$1]} w tag sha
  sha=$(git -C "$ROOT" rev-parse --short=9 HEAD 2>/dev/null || echo unknown)
  tag="live-$(date -u +%Y%m%d%H%M%S)-$sha"
  [[ $tag =~ ^live-[0-9]{14}-[0-9a-f]{7,12}$ ]] || { say "$n: FAIL (no commit to name the run by)"; return 1; }
  w="$OUT/real-engine-$tag"
  mkdir -m 700 "$w" && mkdir "$w/bin" || { say "$n: FAIL (the run's directory could not be made)"; return 1; }
  say "$n: building hb-agent, its test binary and the plan"
  ( cd "$ROOT" &&
    with_build_lock env CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags "-X main.version=live-$sha" -o "$w/bin/hb-agent" ./cmd/hb-agent &&
    with_build_lock env CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go test -c -trimpath -o "$w/bin/hbagent.test" ./cmd/hb-agent &&
    with_build_lock go test -c -o "$w/plan.test" ./cmd/hb-agent &&
    cd cmd/hb-agent && HB_REAL_PLAN_OUT="$w/plan.json" "$w/plan.test" -test.run '^TestRealEnginePlan$' -test.count=1 &&
    cd "$ROOT" && cp spikes/s94/job.sh "$w/job.sh" ) >"$OUT/$n-build.log" 2>&1 ||
    { say "$n: FAIL (build or plan step failed; see the build log in the output directory)"; return 1; }
  [ -s "$w/plan.json" ] || { say "$n: FAIL (the plan was not written)"; return 1; }
  say "$n: staging under \$SCRATCH on the cluster"
  # shellcheck disable=SC2016
  tar -C "$w" -cf - bin plan.json job.sh | flock -o -w 600 "${S[LIVE_LOCK_DIR]}/$host.lock" timeout 600 ssh -o BatchMode=yes "$host" \
    "mkdir -p \"\$SCRATCH/hpc-burst-live/$tag/logs\" && tar -C \"\$SCRATCH/hpc-burst-live/$tag\" -xf -" >>"$OUT/remote.log" 2>&1 ||
    { say "$n: FAIL (staging did not complete)"; return 1; }
  RE_DIR=$w; RE_TAG=$tag
}

# execute_real_engine <name>: submit, poll, fetch. The slot is held from the submit to the job's end.
# The job's budget has two clocks: the time it may wait in the queue, and, from the first state that is not
# PENDING, its time limit plus a margin. A wait of 40 minutes in QOS now must not eat the run's time.
execute_real_engine() {
  local n=$1 host=${HOST[$1]} w=$RE_DIR tag=$RE_TAG acct reply state last="" errs=0 st log finished
  local submit_t run_t="" now limit_s pend_max=${LIVE_PENDING_MAX_SECONDS:-7200} margin=${LIVE_RUN_MARGIN_SECONDS:-900}
  acct=${S[${ACCTKEY[$n]}]}
  # The job's name is written down before the submit. If the reply is lost, or a signal arrives, the
  # cleanup finds the job by this name and cancels it.
  printf 'hb-live-%s\n' "$tag" >"$RE_DIR/submission-tag" || { say "$n: FAIL (the submission tag could not be written)"; return 1; }
  reply=$({ preamble ACCOUNT "$acct" SHAPE_OPTS "$(shape_opts "$n")" TAG "$tag"; cat <<'EOS'
D="$SCRATCH/hpc-burst-live/$TAG"
sbatch --parsable -A "$ACCOUNT" $SHAPE_OPTS -J "hb-live-$TAG" -o "$D/logs/job-%j.out" --export=ALL,H="$D",RUN_TAG="$TAG" "$D/job.sh" 2>/dev/null
EOS
  } | remote "$host") || { say "$n: FAIL (the submit did not complete)"; return 1; }
  JOBID=$(printf '%s\n' "$reply" | sed -n -E 's/^([0-9][0-9_+]*)(;.*)?$/\1/p' | head -1)
  [ -n "$JOBID" ] || { say "$n: FAIL (the submit returned no job)"; return 1; }
  say "$n: submitted one job; polling every ${POLL_SECONDS}s"
  submit_t=$(now_epoch); limit_s=$(slurm_seconds "${TLIMIT[$n]}")
  while :; do
    state=$(job_state "$host" "$JOBID")
    if [ "$state" = error ]; then
      errs=$((errs+1)); [ $errs -lt 3 ] || { say "$n: FAIL (the queue could not be read three times in a row)"; return 1; }
    else
      errs=0
      [ "$state" = "$last" ] || { say "$n: job state: $state"; last=$state; }
      [ "$state" != gone ] || break
    fi
    [ -z "$INTERRUPTED" ] || { say "$n: FAIL (interrupted)"; return 1; }
    now=$(now_epoch)
    if [ "$state" != PENDING ] && [ "$state" != error ] && [ -z "$run_t" ]; then run_t=$now; fi
    if [ -z "$run_t" ] && [ $((now - submit_t)) -gt "$pend_max" ]; then
      say "$n: FAIL (the job was still pending after $((pend_max/60)) minutes; cancelling it)"; return 1
    fi
    if [ -n "$run_t" ] && [ $((now - run_t)) -gt $((limit_s + margin)) ]; then
      say "$n: FAIL (the job ran past its time limit plus $((margin/60)) minutes; cancelling it)"; return 1
    fi
    sleep "$POLL_SECONDS"
  done
  finished=$JOBID; JOBID=""
  fetch_real_engine "$n" "$tag" "$finished" "$w" || return 1
  st=$(sed -n 's/^state=//p' "$w/final.txt" | head -1)
  log="$w/job.log"
  local passed left
  passed=$(grep -c -E '^--- PASS: TestRealEngine( |$)' "$log" 2>/dev/null)
  left=$(sed -n -E 's/.*processes left in the job: ([0-9]+).*/\1/p' "$log" | tail -1)
  say "$n: job ended ${st:-unknown}; TestRealEngine passed ${passed:-0}; processes left in the job ${left:-unknown}"
  say "$n: the record is the result: read it in the output directory; staged files stay on the cluster (deleting them needs the owner's go)"
  [ "${st%% *}" = COMPLETED ] && [ "${passed:-0}" -ge 1 ] && [ "${left:-1}" = 0 ]
}

slurm_seconds() { local h m s; IFS=: read -r h m s <<<"$1"; echo $(( 10#$h * 3600 + 10#$m * 60 + 10#${s:-0} )); }

# fetch_real_engine <name> <tag> <job> <dir>: the job's log, its final state, and the run's record.
fetch_real_engine() {
  local n=$1 tag=$2 job=$3 w=$4 reply files
  # sacct can lag the job's leaving squeue: ask a few times before calling the state unknown.
  reply=$({ preamble JOB "$job" TAG "$tag" GAP "${LIVE_SACCT_GAP:-2}"; cat <<'EOS'
D="$SCRATCH/hpc-burst-live/$TAG"
final=""
for i in 1 2 3 4 5; do
  final=$(sacct -j "$JOB" -X -n -P -o State,ExitCode 2>/dev/null | head -1 | tr '|' ' ')
  [ -z "$final" ] || break
  sleep "$GAP"
done
echo "final=$final"
echo "---LOG---"
cat "$D/logs/job-$JOB.out" 2>/dev/null
EOS
  } | remote "${HOST[$n]}") || { say "$n: FAIL (the job log could not be fetched)"; return 1; }
  printf '%s\n' "$reply" | sed -n -E 's/^final=([A-Z_]+ [0-9]+:[0-9]+)$/state=\1/p' | head -1 >"$w/final.txt"
  printf '%s\n' "$reply" | sed -n '/^---LOG---$/,$p' | tail -n +2 >"$w/job.log"
  # A fresh directory (w is this run's own), and a failed or empty fetch fails the run: the record
  # reported is this run's.
  mkdir -m 700 "$w/record" || { say "$n: FAIL (the record directory could not be made)"; return 1; }
  if ! flock -o -w 600 "${S[LIVE_LOCK_DIR]}/${HOST[$n]}.lock" timeout 600 ssh -o BatchMode=yes "${HOST[$n]}" \
    "tar -C \"\$SCRATCH/hpc-burst-live/$tag/run-$tag\" -cf - ." 2>>"$OUT/remote.log" | tar -C "$w/record" -xf -; then
    say "$n: FAIL (the run's record could not be fetched)"; return 1
  fi
  files=$(find "$w/record" -type f | wc -l)
  say "$n: record: $files file(s) fetched"
  [ "$files" -gt 0 ] || { say "$n: FAIL (the fetched record is empty)"; return 1; }
}

# ---------------------------------------------------------------- plan

# The build contexts a row's dependency closure is taken in. linux-arm64 is the one the real-engine job's
# binaries are built for; a dependency that only that build imports must still be found.
declare -A CTXENV=([host]="" [linux-arm64]="CGO_ENABLED=0 GOOS=linux GOARCH=arm64")

PLAN_TMP=""

# extract_tree <rev> <dir>: the module's files at rev, without spikes/ and hbep/ (their own modules and prose).
# The plan reads the two ends of the range from these copies, never from the current checkout, which may be
# anywhere else.
extract_tree() {
  git -C "$ROOT" archive "$1" 2>/dev/null | tar -x --anchored --exclude=spikes --exclude=hbep -C "$2" 2>/dev/null
}

# pkg_deps <tree> <package> <context>: the tree's directories the package's tests run (relative), one per
# line, then "embed:<path>" lines for the files they embed (paths as go list gives them, nested ones too).
# Fails when go list fails or prints nothing: a partial listing must never read as "nothing covers this".
pkg_deps() {
  local tree root out
  tree=$(cd "$1" 2>/dev/null && pwd -P) || return 1
  # shellcheck disable=SC2086
  out=$( ( cd "$tree" && env ${CTXENV[$3]} go list -deps -test -f '{{.Dir}}|{{join .EmbedFiles ","}}' "./$2" 2>/dev/null ) |
    awk -F'|' -v root="$tree" '
      index($1, root "/") == 1 || $1 == root {
        rel = substr($1, length(root) + 2); if (rel == "") rel = "."
        print rel
        n = split($2, e, ","); for (i = 1; i <= n; i++) if (e[i] != "") print "embed:" rel "/" e[i]
      }' | sort -u ) || return 1
  [ -n "$out" ] || return 1
  printf '%s\n' "$out"
}

# input_matches <file> <inputs>: whether the file is one of the comma-separated inputs (a file, a glob or a directory).
input_matches() {
  local f=$1 inp
  for inp in ${2//,/ }; do
    # shellcheck disable=SC2053
    { [[ $f == $inp ]] || [[ $f == "$inp"/* ]]; } && return 0
  done
  return 1
}

plan() {
  local range=$1 base head bsha hsha files name pkg ctx side f d b reason key out
  local -A deps=() reasons=() hit=()
  case $range in *..*) ;; *) say "live-checks: --plan needs <base>..<head>"; return 2 ;; esac
  base=${range%%..*}; head=${range##*..}
  bsha=$(git -C "$ROOT" rev-parse --verify --quiet "${base}^{commit}" 2>/dev/null) &&
    hsha=$(git -C "$ROOT" rev-parse --verify --quiet "${head:-HEAD}^{commit}" 2>/dev/null) ||
    { say "live-checks: git cannot read that range here"; return 2; }
  case $range in *...*) bsha=$(git -C "$ROOT" merge-base "$bsha" "$hsha" 2>/dev/null) || { say "live-checks: git cannot read that range here"; return 2; } ;; esac
  files=$(git -C "$ROOT" diff --name-only --no-renames "$bsha" "$hsha" -- 2>/dev/null) || { say "live-checks: git cannot read that range here"; return 2; }
  local -a changed=()
  [ -z "$files" ] || mapfile -t changed <<<"$files"
  say "range: $range, ${#changed[@]} file(s) changed"

  PLAN_TMP=$(mktemp -d "${TMPDIR:-/tmp}/hbp.XXXXXX" 2>/dev/null) || { say "live-checks: no scratch directory for the plan"; return 2; }
  trap 'rm -rf "$PLAN_TMP"' EXIT
  mkdir "$PLAN_TMP/base" "$PLAN_TMP/head"
  extract_tree "$bsha" "$PLAN_TMP/base" && extract_tree "$hsha" "$PLAN_TMP/head" || { say "live-checks: the range's trees could not be read"; return 2; }

  # The closure at both ends of the range: a file deleted by the range was a dependency at its base.
  for name in "${NAMES[@]}"; do
    [ "${STATUS[$name]}" = ready ] || continue
    pkg=${PKG[$name]}
    for ctx in ${CONTEXTS[$name]//,/ }; do
      key="$pkg|$ctx"
      [ -z "${deps[$key]+x}" ] || continue
      deps[$key]=""
      for side in base head; do
        [ -d "$PLAN_TMP/$side/$pkg" ] || continue   # the package may be new, or deleted, in the range
        out=$(pkg_deps "$PLAN_TMP/$side" "$pkg" "$ctx") || { say "live-checks: dependency discovery failed for package $pkg ($ctx) at the $side of the range; nothing is planned"; return 2; }
        deps[$key]+="$out"$'\n'
      done
      [ -n "${deps[$key]}" ] || { say "live-checks: package $pkg exists at neither end of the range; nothing is planned"; return 2; }
      deps[$key]=$(printf '%s' "${deps[$key]}" | sort -u)
    done
  done

  for f in "${changed[@]}"; do
    d=$(dirname "$f"); b=$(basename "$f")
    for name in "${NAMES[@]}"; do
      [ "${STATUS[$name]}" = ready ] || continue
      pkg=${PKG[$name]}; reason=""
      case $f in go.mod|go.sum|go.work|go.work.sum) reason="go.mod/go.sum" ;; esac
      if [ -z "$reason" ] && [ "${INPUTS[$name]}" != - ] && input_matches "$f" "${INPUTS[$name]}"; then reason=$f; fi
      if [ -z "$reason" ]; then
        for ctx in ${CONTEXTS[$name]//,/ }; do
          # An embedded file counts wherever it sits and whatever it is called.
          if printf '%s\n' "${deps[$pkg|$ctx]}" | grep -q -x -F "embed:$f"; then reason=$f; break; fi
        done
      fi
      if [ -z "$reason" ]; then
        if [ "$d" = "$pkg" ] || [[ $f == "$pkg"/* ]]; then
          case $b in *.md) ;; *) reason=$pkg ;; esac
        else
          for ctx in ${CONTEXTS[$name]//,/ }; do
            printf '%s\n' "${deps[$pkg|$ctx]}" | grep -q -x -F "$d" || continue
            case $b in
              *_test.go) ;;
              *.go|*.s|*.S|*.c|*.h|*.cc|*.cpp) reason=$d; break ;;
            esac
          done
        fi
      fi
      [ -n "$reason" ] || continue
      hit[$name]=1
      printf '%s' "${reasons[$name]:-}" | grep -q -x -F "$reason" || reasons[$name]="${reasons[$name]:-}$reason"$'\n'
    done
  done
  local run="" none="" r shown
  for name in "${NAMES[@]}"; do
    [ "${STATUS[$name]}" = ready ] || continue
    if [ -n "${hit[$name]:-}" ]; then
      r=${reasons[$name]}
      shown=$(printf '%s' "$r" | head -6 | paste -sd, - | sed 's/,/, /g')
      [ "$(printf '%s' "$r" | wc -l)" -le 6 ] || shown="$shown, ..."
      say "covered: $name (${KIND[$name]}; gate ${GATES[$name]}): changed $shown"
      run="$run $name"
    else
      none="$none $name"
    fi
  done
  [ -z "$none" ] || say "not covered:${none}"
  for name in "${NAMES[@]}"; do
    [ "${STATUS[$name]}" = pending ] && say "pending: $name (no runnable test on main yet; run it by hand)"
  done
  if [ -n "$run" ]; then say "run:$run"; else say "run: (none) no live test covers this range"; fi
  return 0
}

# ---------------------------------------------------------------- main

MODE=""; RANGE=""; YES=0
declare -a REQUESTED=()
while [ $# -gt 0 ]; do
  case $1 in
    --plan) MODE=plan; RANGE=${2:-}; [ $# -ge 2 ] || usage 2; shift 2 ;;
    --run) MODE=run; shift; while [ $# -gt 0 ] && [[ $1 != -* ]]; do REQUESTED+=("$1"); shift; done ;;
    --list) MODE=list; shift ;;
    --yes) YES=1; shift ;;
    -h|--help) usage 0 ;;
    *) say "live-checks: unknown argument"; usage 2 ;;
  esac
done
[ -n "$MODE" ] || usage 2
load_tables

case $MODE in
  list)
    for name in "${NAMES[@]}"; do say "$name  ${KIND[$name]}  ${STATUS[$name]}  gate ${GATES[$name]}"; done
    exit 0 ;;
  plan) plan "$RANGE"; exit $? ;;
esac

# --run
[ ${#REQUESTED[@]} -gt 0 ] || { say "live-checks: --run needs at least one test name"; exit 2; }
for name in "${REQUESTED[@]}"; do
  [ -n "${KIND[$name]+x}" ] || { say "live-checks: unknown test (see --list)"; exit 2; }
  [ "${STATUS[$name]}" = ready ] || { say "live-checks: $name is pending and cannot run yet"; exit 2; }
done
load_settings || exit 3
check_settings "${REQUESTED[@]}" || exit 3
OUT=${S[LIVE_OUT_DIR]}
# Everything private is mode 600 (directories 700), and a tool's own diagnostics, which can name a path,
# go to a log in the output directory, never to the screen. The logs there are raw: the screen is clean,
# the logs are not, and they stay private.
umask 077
mkdir -p "$OUT" 2>/dev/null && chmod 700 "$OUT" 2>/dev/null || { say "live-checks: the output directory could not be made"; exit 3; }
exec 2>>"$OUT/script.log"
: >>"$OUT/remote.log"
CONTROL_DIR=$(mktemp -d "${TMPDIR:-/tmp}/hbl.XXXXXX") || { say "live-checks: no scratch directory for the ssh master"; exit 3; }
trap on_signal INT TERM HUP
trap on_exit EXIT

# worse <a> <b>: the more serious exit code: 1 (failed) over 3 (refused) over 4 (not run) over 2 over 0.
worse() {
  local c
  for c in 1 3 4 2; do { [ "$1" = "$c" ] || [ "$2" = "$c" ]; } && { echo "$c"; return; }; done
  echo 0
}

worst=0
for name in "${REQUESTED[@]}"; do
  rc=0
  if [ "${KIND[$name]}" = cluster ]; then run_cluster "$name" || rc=$?; else run_local "$name" || rc=$?; fi
  worst=$(worse "$worst" "$rc")
  [ "$CLEAN_LEFT" -eq 0 ] || { say "live-checks: a job was left behind; no further test is started"; break; }
  [ -z "$INTERRUPTED" ] || break
done
exit "$worst"
