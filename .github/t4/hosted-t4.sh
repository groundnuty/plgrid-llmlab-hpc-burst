#!/usr/bin/env bash
# Hosted run for #690 (the diagnosis's T4): the fix and a mutant of it (one pollPidfd: no waitid, no
# re-poll), interleaved, on one machine. Each run prints two NUMBERS lines, one for the helper as a
# child and one as a reparented grandchild.
# Run it from the root of a checkout of the fix head, on a GitHub-hosted ubuntu-24.04 runner. It
# changes nothing in the checkout when it ends. No `set -e`: a failing run is a result, not an abort.
#
#   ROUNDS      interleaved rounds per configuration (default 2)
#   COUNT       -test.count of each invocation (default 10)
#   EXEC_COUNT  HPCBURST_TEST_EXEC_COUNT (default 3000)
#   ALLOW_OLD_KERNEL=1  run on a kernel below 6.15 anyway (local smoke test of this script only)
#   SPINNERS    busy-loop processes in the loaded configuration (default nproc)
#   FIX_BIN, MUT_BIN  prebuilt launcher test binaries (skips the builds; local use)
set -u
ROUNDS=${ROUNDS:-2}
COUNT=${COUNT:-10}
EXEC_COUNT=${EXEC_COUNT:-3000}
SPINNERS=${SPINNERS:-$(nproc)}
WORK=${WORK:-$(mktemp -d)}
mkdir -p "$WORK"
ROOT=$PWD

echo "T4 kernel: $(uname -r)  nproc: $(nproc)  ROUNDS=$ROUNDS COUNT=$COUNT EXEC_COUNT=$EXEC_COUNT"
case "$(uname -r)" in
  [0-5].* | 6.[0-9].* | 6.1[0-4].*)
    echo "T4 STOP: the kernel is below 6.15, so the race cannot show; report it, do not read any result as a fix"
    [ "${ALLOW_OLD_KERNEL:-}" = 1 ] || exit 2 ;;
esac

if [ -z "${FIX_BIN:-}" ]; then
  go test -c -o "$WORK/fix.test" ./internal/launcher || { echo "T4 STOP: the fix did not build"; exit 1; }
  FIX_BIN=$WORK/fix.test
  cp internal/launcher/handle.go "$WORK/handle.go.orig"
  # The mutant: sysPidfd.exited is one pollPidfd, with no waitid and no re-poll (today's behaviour).
  sed -i 's/^\(\s*\)return confirmExit(.*$/\1return pollPidfd(fd)/' internal/launcher/handle.go
  if [ "$(grep -c '^\s*return pollPidfd(fd)$' internal/launcher/handle.go)" = 1 ] && ! grep -q 'return confirmExit(' internal/launcher/handle.go; then
    go test -c -o "$WORK/mutant.test" ./internal/launcher || echo "T4 STOP: the mutant did not build"
  else
    echo "T4 STOP: the mutation did not apply (no 'return confirmExit(' line in sysPidfd.exited)"
  fi
  cp "$WORK/handle.go.orig" internal/launcher/handle.go
  MUT_BIN=$WORK/mutant.test
  git diff --quiet -- internal/launcher/handle.go || echo "T4 STOP: handle.go is not restored"
fi
[ -x "$MUT_BIN" ] || { echo "T4 STOP: no mutant binary"; exit 1; }

cd "$ROOT/internal/launcher" || exit 1
spinners=()
start_spinners() { for _ in $(seq "$SPINNERS"); do (while :; do :; done) & spinners+=($!); done; }
stop_spinners() { for p in "${spinners[@]}"; do kill "$p" 2>/dev/null; wait "$p" 2>/dev/null; done; spinners=(); }
trap stop_spinners EXIT

run() { # config label binary round
  local cfg=$1 label=$2 bin=$3 out="$WORK/$1-$2-$4.log"
  HPCBURST_TEST_EXEC_COUNT=$EXEC_COUNT "$bin" -test.run '^TestLivenessAcrossConcurrentExec$' \
    -test.count="$COUNT" -test.v -test.timeout 150m >"$out" 2>&1
  local rc=$?
  echo "T4 RUN config=$cfg binary=$label round=$4 exit=$rc"
  grep -h 'NUMBERS concurrent exec' "$out" | sed "s/^.*NUMBERS/T4 NUMBERS config=$cfg binary=$label round=$4:/"
  grep -h -E '^\s*(--- FAIL|exec_test.go:[0-9]+: (a live process|in [0-9]+ checks|the helper))' "$out" | sed "s/^/T4 FAIL config=$cfg binary=$label round=$4: /"
}

for cfg in alone spinners; do
  [ "$cfg" = spinners ] && start_spinners
  for round in $(seq "$ROUNDS"); do
    run "$cfg" fix "$FIX_BIN" "$round"
    run "$cfg" mutant "$MUT_BIN" "$round"
  done
  [ "$cfg" = spinners ] && stop_spinners
done
echo "T4 done; logs in $WORK"
