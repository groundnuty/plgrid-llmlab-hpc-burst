#!/usr/bin/env bash
# One entry point for the live-only tests: the tests that run against a real
# system and never in CI (issue #163; HBEP-15, Upgrading a pinned dependency,
# "Live re-run after merge").
#
#   hack/live-checks.sh --list
#   hack/live-checks.sh --plan <base>..<head>        which live tests cover what the range changed; runs nothing
#   hack/live-checks.sh --run <name>... [--yes]      runs only the named tests
#
# --plan maps the range's changed files to the code each live test runs: the
# packages it imports (go list -deps -test), the files it embeds, and the
# non-Go inputs the table names. A test file counts only for its own package.
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
# Output is statuses and counts. Logs, records and every remote reply go to
# LIVE_OUT_DIR; the screen never carries a key, a URL, a job ID, a host, an
# account or a private path.
#
# Development rule for the one-job slots (QOS now on Helios, plgrid-now on
# Athena; owner, 1 Nov 2026, issue #145): the script refuses to queue behind a
# job that already holds the slot, runs one live run per host at a time with
# the shortest time limit the test allows, builds before it takes the slot, and
# on any exit (a failure or a signal included) cancels what the run left: the
# real-engine job by its ID, a conformance run's new hb- jobs in the test's own
# partition. It prints each slot use as a UTC start, end and duration, to post
# on the issue the run served.
#
# The table below is the one map from a live test to its package. Add a row
# when a test is added; hack/livechecks fails when a package holds a live test
# that no row names.

set -u -o pipefail

SELF=${BASH_SOURCE[0]}
ROOT=$(cd "$(dirname "$SELF")/.." && pwd -P)

# name | kind | package | test | gate variables | non-Go inputs | status
live_table() {
  cat <<'TABLE'
# BEGIN LIVE TESTS
llmlab-smoke|local|internal/front|^TestLLMLabThroughTheFront$|HPCBURST_LLMLAB_CREDENTIALS|-|ready
hub-golden|local|pkg/hubfiles|^TestGoldenHub$|HPCBURST_TEST_HUB|-|ready
conformance-helios|cluster|internal/providerctl/conformance|^TestConformance$|HPCBURST_CONFORMANCE|profiles/helios.yaml|ready
conformance-athena|cluster|internal/providerctl/conformance|^TestConformance$|HPCBURST_CONFORMANCE|profiles/athena.yaml|ready
real-engine|cluster|cmd/hb-agent|^TestRealEngine$|HB_REAL_ENGINE_PLAN|spikes/s94/job.sh|ready
rehearsal|cluster|-|-|-|-|pending
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
declare -A KIND PKG TEST GATES INPUTS STATUS
declare -A HOST PROFILE PARTITION QOS SLOT ACCTKEY GPUS CPUS MEM TLIMIT TMIN EXTRA
declare -A S=()

load_tables() {
  local name kind pkg test gates inputs status
  while IFS='|' read -r name kind pkg test gates inputs status; do
    [ -n "$name" ] || continue
    NAMES+=("$name")
    KIND[$name]=$kind PKG[$name]=$pkg TEST[$name]=$test GATES[$name]=$gates INPUTS[$name]=$inputs STATUS[$name]=$status
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
  done < "$HPCBURST_LIVE_SETTINGS"
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
BASELINE=""         # the hb- jobs the queue held before the test
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
queue_jobs() {
  local reply
  reply=$(remote "$1" <<'EOS'
out=$(squeue --me -h -o '%i|%j|%P') || { echo "squeue=failed"; exit 0; }
echo "squeue=ok"
printf '%s\n' "$out" | grep '|hb-' || true
EOS
  ) || return 1
  [ "$(printf '%s\n' "$reply" | head -1)" = squeue=ok ] || return 1
  printf '%s\n' "$reply" | tail -n +2 | grep -E '^[0-9][0-9_+]*\|hb-[A-Za-z0-9._-]*\|[A-Za-z0-9._-]+$' || true
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

# clean_cluster: cancels what this run left, then checks. Idempotent; sets CLEAN_LEFT. Runs on every exit path.
# The real-engine run knows its job: only that ID is cancelled. A harness-driven run does not know the IDs
# its test submitted, so only the hb- jobs that appeared during the run in the test's own partition are
# cancelled; a job queued before the run, or in another partition, is never touched.
CLEAN_LEFT=0
CLEAN_MODE=partition   # job | partition
CLEAN_PARTITION=""
clean_cluster() {
  [ -n "$ACTIVE_HOST" ] || return 0
  local host=$ACTIVE_HOST now ids line id cancelled=0 left=0 try
  if ! now=$(queue_jobs "$host"); then
    say "cleanup: the queue could not be read; check the cluster for jobs of this run by hand"
    CLEAN_LEFT=1; ACTIVE_HOST=""; return 1
  fi
  ids=""
  if [ "$CLEAN_MODE" = job ]; then
    if [ -n "$JOBID" ] && printf '%s\n' "$now" | grep -q "^${JOBID}|"; then ids=$JOBID; fi
  else
    while IFS= read -r line; do
      [ -n "$line" ] || continue
      printf '%s\n' "$BASELINE" | grep -q -x -F "$line" && continue
      [ "${line##*|}" = "$CLEAN_PARTITION" ] || continue
      ids="$ids ${line%%|*}"
    done <<<"$now"
  fi
  # shellcheck disable=SC2086
  [ -z "$ids" ] || cancelled=$(cancel_ids "$host" $ids)
  if [ -n "$ids" ]; then
    # A cancelled job can show as completing for a few seconds: look again before calling it left behind.
    for try in 1 2 3 4 5 6 7 8 9 10; do
      now=$(queue_jobs "$host") || { say "cleanup: the queue could not be read after the cancel"; CLEAN_LEFT=1; ACTIVE_HOST=""; return 1; }
      left=0
      for id in $ids; do printf '%s\n' "$now" | grep -q "^${id}|" && left=$((left+1)); done
      [ "$left" -eq 0 ] && break
      sleep "$CLEAN_POLL_SECONDS"
    done
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

# judge <name> <log> <rc>: the verdict from the test's verbose log and exit status. A skip is not a pass.
judge() {
  local n=$1 log=$2 rc=$3 test pass fail skip named failed
  test=${TEST[$n]#^}; test=${test%\$}
  pass=$(grep -c -E '^--- PASS: ' "$log" 2>/dev/null); fail=$(grep -c -E '^--- FAIL: ' "$log" 2>/dev/null)
  skip=$(grep -c -E '^--- SKIP: ' "$log" 2>/dev/null)
  named=$(sed -n -E "s/^--- (PASS|FAIL|SKIP): ${test}( .*)?\$/\\1/p" "$log" 2>/dev/null | head -1)
  say "$n: tests: ${pass:-0} passed, ${fail:-0} failed, ${skip:-0} skipped"
  if [ "$rc" = 130 ]; then say "$n: FAIL (interrupted)"; return 1; fi
  if [ "$rc" = 124 ]; then say "$n: FAIL (timed out)"; return 1; fi
  if [ "$named" = SKIP ]; then say "$n: FAIL (skipped: its gate was not met, so nothing ran)"; return 1; fi
  if [ "$rc" = 0 ] && [ "$named" = PASS ] && [ "${fail:-0}" = 0 ]; then say "$n: PASS"; return 0; fi
  failed=$(grep -E '^ *--- FAIL: ' "$log" 2>/dev/null | awk '{print $3}' | grep -E '^[A-Za-z0-9_/.-]{1,80}$' | head -8 | tr '\n' ' ')
  [ -z "$failed" ] || say "$n: failed: $failed"
  say "$n: FAIL (exit status $rc)"
  return 1
}

# ---------------------------------------------------------------- running

CONTROL_DIR=""

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
  export HPCBURST_CONF_OUT=$OUT/$n-record
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
  BASELINE=$(queue_jobs "$host") || { release_run_lock; say "$n: FAIL (the queue could not be read before the run)"; return 1; }
  [ -z "$INTERRUPTED" ] || { release_run_lock; say "$n: FAIL (interrupted before the run started)"; return 1; }
  mkdir -p -m 700 "$OUT/$n-record"
  CLEAN_PARTITION=${PARTITION[$n]}; CLEAN_MODE=partition; [ "$n" != real-engine ] || CLEAN_MODE=job
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
  local n=$1 host=${HOST[$1]} w="$OUT/real-engine" tag sha
  mkdir -p "$w/bin" && chmod 700 "$w" || return 1
  sha=$(git -C "$ROOT" rev-parse --short=9 HEAD 2>/dev/null || echo unknown)
  tag="live-$(date -u +%Y%m%d%H%M%S)-$sha"
  [[ $tag =~ ^live-[0-9]{14}-[0-9a-f]{7,12}$ ]] || { say "$n: FAIL (no commit to name the run by)"; return 1; }
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
    "mkdir -p \"\$SCRATCH/hpc-burst-live/$tag/logs\" && tar -C \"\$SCRATCH/hpc-burst-live/$tag\" -xf -" 2>>"$OUT/remote.log" ||
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
  mkdir -p "$w/record" && chmod 700 "$w/record"
  flock -o -w 600 "${S[LIVE_LOCK_DIR]}/${HOST[$n]}.lock" timeout 600 ssh -o BatchMode=yes "${HOST[$n]}" \
    "tar -C \"\$SCRATCH/hpc-burst-live/$tag/run-$tag\" -cf - . 2>/dev/null" 2>>"$OUT/remote.log" | tar -C "$w/record" -xf - 2>/dev/null || true
  files=$(find "$w/record" -type f | wc -l)
  say "$n: record: $files file(s) fetched"
}

# ---------------------------------------------------------------- plan

# pkg_deps <package>: the module's directories the package's tests run (relative), one per line, then
# "embed:<path>" lines for the files they embed.
pkg_deps() {
  ( cd "$ROOT" && go list -deps -test -f '{{.Dir}}|{{join .EmbedFiles ","}}' "./$1" 2>/dev/null ) | awk -F'|' -v root="$ROOT" '
    index($1, root "/") == 1 || $1 == root {
      rel = substr($1, length(root) + 2); if (rel == "") rel = "."
      print rel
      n = split($2, e, ","); for (i = 1; i <= n; i++) if (e[i] != "") print "embed:" rel "/" e[i]
    }' | sort -u
}

plan() {
  local range=$1 files name pkg f d b reason
  local -A deps=() reasons=() hit=()
  case $range in *..*) ;; *) say "live-checks: --plan needs <base>..<head>"; return 2 ;; esac
  files=$(git -C "$ROOT" diff --name-only --no-renames "$range" -- 2>/dev/null) || { say "live-checks: git cannot read that range here"; return 2; }
  local -a changed=()
  [ -z "$files" ] || mapfile -t changed <<<"$files"
  say "range: $range, ${#changed[@]} file(s) changed"
  for name in "${NAMES[@]}"; do
    [ "${STATUS[$name]}" = ready ] || continue
    pkg=${PKG[$name]}
    if [ -z "${deps[$pkg]+x}" ]; then
      deps[$pkg]=$(pkg_deps "$pkg")
      [ -n "${deps[$pkg]}" ] || { say "live-checks: go list found nothing for package $pkg"; return 2; }
    fi
  done
  for f in "${changed[@]}"; do
    d=$(dirname "$f"); b=$(basename "$f")
    for name in "${NAMES[@]}"; do
      [ "${STATUS[$name]}" = ready ] || continue
      pkg=${PKG[$name]}; reason=""
      case $f in go.mod|go.sum|go.work|go.work.sum) reason="go.mod/go.sum" ;; esac
      if [ -z "$reason" ] && [ "${INPUTS[$name]}" != - ]; then
        local inp
        for inp in ${INPUTS[$name]//,/ }; do
          { [ "$f" = "$inp" ] || [[ $f == "$inp"/* ]]; } && reason=$f
        done
      fi
      if [ -z "$reason" ]; then
        if [ "$d" = "$pkg" ] || [[ $f == "$pkg"/* ]]; then
          case $b in *.md) ;; *) reason=$pkg ;; esac
        elif printf '%s\n' "${deps[$pkg]}" | grep -q -x -F "$d"; then
          case $b in
            *_test.go) ;;
            *.go|*.s|*.S|*.c|*.h|*.cc|*.cpp) reason=$d ;;
            *) printf '%s\n' "${deps[$pkg]}" | grep -q -x -F "embed:$f" && reason=$d ;;
          esac
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
mkdir -p "$OUT" && chmod 700 "$OUT" || { say "live-checks: the output directory could not be made"; exit 3; }
: >>"$OUT/remote.log" && chmod 600 "$OUT/remote.log"
CONTROL_DIR=$(mktemp -d "${TMPDIR:-/tmp}/hbl.XXXXXX") || exit 3
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
