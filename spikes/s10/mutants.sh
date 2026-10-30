#!/usr/bin/env bash
# Planted-bug check for the simulator's tests. Each mutant breaks one cache
# rule in a scratch copy of the code and runs the simulator's tests there
# (go test -short ./sim/). Every mutant must make at least one test fail.
# Run from anywhere: spikes/s10/mutants.sh. Needs Go and perl on PATH.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../.." && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
status=0

mutate() { # name file from to
  local name=$1 file=$2 from=$3 to=$4
  rm -rf "$work/src"
  mkdir -p "$work/src/spikes" "$work/src/third_party/forked"
  cp -r "$root/spikes/s10" "$work/src/spikes/s10"
  rm -f "$work/src/spikes/s10/s10sim"
  cp -r "$root/third_party/forked/kubeai" "$work/src/third_party/forked/kubeai"
  local f="$work/src/spikes/s10/$file"
  if [ "$(grep -cF -- "$from" "$f")" != 1 ]; then
    echo "$name: the code to break is not found exactly once in $file" >&2
    exit 2
  fi
  FROM=$from TO=$to perl -0pi -e 's/\Q$ENV{FROM}\E/$ENV{TO}/' "$f"
  local out fails n
  out=$(cd "$work/src/spikes/s10" && go test -short -count=1 ./sim/ 2>&1 || true)
  if grep -q -e '\[build failed\]' -e '^# ' <<<"$out"; then
    echo "$name: the mutant does not compile" >&2
    echo "$out" >&2
    exit 2
  fi
  fails=$(grep -E -- '--- FAIL' <<<"$out" | sed -E 's/.*--- FAIL: ([A-Za-z0-9_]+).*/\1/' | sort -u | tr '\n' ' ' || true)
  n=$(wc -w <<<"$fails")
  echo "$name: $n tests fail: $fails"
  if [ "$n" -eq 0 ]; then
    echo "$name: no test failed" >&2
    status=1
  fi
}

mutate "cache never misses (every prompt hits its whole prefix)" sim/cache.go \
  'hold := &Hold{HitTokens: h * c.B, Blocks: total}' \
  'hold := &Hold{HitTokens: (promptLen - 1) / c.B * c.B, Blocks: total}'

mutate "blocks freed head first" sim/cache.go \
  'for i := len(hold.pinned) - 1; i >= 0; i-- {' \
  'for i := 0; i < len(hold.pinned); i++ {'

mutate "output tokens not cached" sim/cache.go \
  '		c.register(hold, full[n:])' \
  '		_ = full'

exit $status
