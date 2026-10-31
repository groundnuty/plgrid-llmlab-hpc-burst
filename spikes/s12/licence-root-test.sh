#!/usr/bin/env bash
# S12 follow-up: does a root LICENSE make the licence check pass for the spikes?
# (HBEP-15 open question 1; the sibling-spike problem of spikes/s10b and s10c.)
#
# Usage: licence-root-test.sh <go-licenses binary> <work dir>   (the work dir must be empty or absent;
#        it takes about 10 MB, and the spikes' module dependencies are downloaded into the Go cache)
#
# It works on a scratch copy of HEAD made with git archive and never writes into the checkout.
# The repository has had an Apache-2.0 root LICENSE since PR #73, so the copy's LICENSE is moved
# out first to reproduce the state before it (V1); V2 onwards put the same file back. If HEAD has
# none (an older commit), the Apache-2.0 text of third_party/forked/kubeai/LICENSE is the test
# input. Each variant runs licence-check.sh with CHECK_SPIKES=1 (the spikes are outside the
# HBEP-15 check; here they are the subject) and records which modules fail.
#
#   V1  no LICENSE anywhere, --local-permit-own   the state before #73       fails: spikes/s10b spikes/s10c
#   V2  LICENSE at the root, --local-permit-own   the owner's question       fails: the same two
#   V3  LICENSE at the root, strict               what CI runs               fails: every spikes/* module
#   V4  V2 + a LICENSE in spikes/s10 and s10b     the replaced siblings      fails: none
#   V5  V3 + a LICENSE in every spikes/* module   strict, all licensed       fails: none
#
# The expected sets come from how go-licenses v2.0.1 searches (licenses/find.go: the upward search
# for a licence file stops at the module directory of the package), written down before the runs.
# After the variants it compares the root module's reports of V1 and V3: the dependency rows must be
# identical, V1 must have one own row per package of the module ("go list ./..." counts them, not
# go-licenses), and V3 exactly one (the own packages collapse under one LICENSE). A missing
# report, a difference or a wrong count fails the script.
# --local-permit-own is LOCAL ONLY (see licence-check.sh). Exit status: 0 when every variant
# matches its expectation, 1 otherwise.
set -u -o pipefail
GL=$(readlink -f "${1:?usage: $0 <go-licenses binary> <work dir>}")
W=${2:?usage: $0 <go-licenses binary> <work dir>}
[ -n "$(ls -A "$W" 2>/dev/null)" ] && { echo "work dir $W is not empty" >&2; exit 2; }
mkdir -p "$W/tree" && W=$(cd "$W" && pwd)
here=$(cd "$(dirname "$0")" && pwd)
repo=$(git -C "$here" rev-parse --show-toplevel)
git -C "$repo" archive HEAD | tar -x -C "$W/tree"
tree=$W/tree
apache=$W/root-LICENSE
if [ -f "$tree/LICENSE" ]; then mv "$tree/LICENSE" "$apache"; else cp "$tree/third_party/forked/kubeai/LICENSE" "$apache"; fi
grep -q 'Apache License' "$apache" && grep -q 'Version 2.0, January 2004' "$apache" || { echo "$apache is not the Apache-2.0 text" >&2; exit 2; }
spikes=$(cd "$tree" && find spikes -name go.mod -printf '%h\n' | sort)
status=0
echo "copy of $(git -C "$repo" rev-parse --short HEAD); modules under spikes/: $(echo $spikes)"

failing() { # output file -> the modules with an unexpected failure, sorted, one line
  awk '/^##### module /{m=$3; sub(":$","",m)} /^gate: .*: FAIL$/{f[m]=1} /^UNEXPECTED:/{f[m]=1} END{for(k in f) print k}' "$1" | sort | paste -sd' ' -
}
run() { # name expected-failing-set licence-check.sh-options...
  local name=$1 want=$2 rc=0 got; shift 2
  CHECK_SPIKES=1 nice -n 10 "$here/licence-check.sh" "$@" "$GL" "$tree" "$W/out-$name" > "$W/$name.txt" 2>&1 || rc=$?
  got=$(failing "$W/$name.txt")
  echo; echo "== $name: exit $rc; failing modules: ${got:-none}"
  echo "   root module, first gate line: $(grep -m1 '^gate:' "$W/$name.txt")"
  if [ "$got" = "$want" ] && { [ -n "$want" ] || [ "$rc" = 0 ]; } && { [ -z "$want" ] || [ "$rc" != 0 ]; }; then
    echo "   as expected (${want:-none})"
  else
    echo "   UNEXPECTED: wanted '${want:-none}'"; status=1
  fi
}
addlic() { local m; for m in "$@"; do cp "$apache" "$tree/$m/LICENSE"; done; }

run V1 "spikes/s10b spikes/s10c" --local-permit-own
cp "$apache" "$tree/LICENSE"
run V2 "spikes/s10b spikes/s10c" --local-permit-own
run V3 "$(echo $spikes)"
addlic spikes/s10 spikes/s10b
run V4 "" --local-permit-own
addlic $spikes
run V5 ""

modpath=$(cd "$tree" && go list -m) || { echo "go list -m failed in the copy" >&2; exit 2; }
want_own=$(cd "$tree" && go list ./... | wc -l)
echo; echo "root report of V1 and V3 (module $modpath; $want_own packages by go list)"
own_rows() { # report file -> the number of rows of the module's own packages, or "unreadable"
  [ -s "$1" ] || { echo unreadable; return; }
  awk -F, -v m="$modpath" '$1==m || index($1, m "/")==1 {n++} END{print n+0}' "$1"
}
dep_rows() { grep -v -F "$modpath" "$1" | sort; }
for v in V1 V3; do
  [ -s "$W/out-$v/go-licenses-report.csv" ] || { echo "   UNEXPECTED: $v has no root report"; status=1; }
done
o1=$(own_rows "$W/out-V1/go-licenses-report.csv"); o3=$(own_rows "$W/out-V3/go-licenses-report.csv")
echo "   own-package rows: V1 $o1 (want $want_own), V3 $o3 (want 1)"
[ "$o1" = "$want_own" ] || { echo "   UNEXPECTED: V1 should have one row per package of the module"; status=1; }
[ "$o3" = 1 ] || { echo "   UNEXPECTED: V3 should have exactly one own row"; status=1; }
if [ -s "$W/out-V1/go-licenses-report.csv" ] && [ -s "$W/out-V3/go-licenses-report.csv" ] &&
   diff <(dep_rows "$W/out-V1/go-licenses-report.csv") <(dep_rows "$W/out-V3/go-licenses-report.csv") >/dev/null; then
  echo "   dependency rows identical with and without the root LICENSE: yes ($(dep_rows "$W/out-V1/go-licenses-report.csv" | grep -c .))"
else
  echo "   dependency rows identical with and without the root LICENSE: NO"; status=1
fi
echo; echo "licence-root-test: $([ "$status" = 0 ] && echo 'all variants as expected' || echo FAIL)"
exit "$status"
