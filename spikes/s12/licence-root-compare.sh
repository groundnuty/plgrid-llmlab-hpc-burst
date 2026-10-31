#!/usr/bin/env bash
# Compares the root module's go-licenses reports of two variants of licence-root-test.sh: V1 (no
# root LICENSE) and V3 (a root LICENSE). It exits 1 on anything unexpected, so the caller's status
# cannot stay 0 after a difference was printed.
#
# Usage: licence-root-compare.sh <module path> <packages in the module> <V1 report> <V3 report>
#
# Expected, from how go-licenses v2.0.1 groups packages by licence file (licenses/library.go):
#   - both reports exist and are not empty;
#   - V1 has one row per package of the module, because no package has a licence file; the
#     count comes from "go list ./..." (the second argument), not from go-licenses;
#   - V3 has exactly one own row, because the LICENSE at the root covers every own package;
#   - the rows of every other module are the same in both (the LICENSE changes only own rows).
# Rows are CSV lines whose first field is the library name; the module's own rows are those
# whose name is the module path or lies beneath it.
set -u -o pipefail
[ $# -eq 4 ] || { echo "usage: $0 <module path> <packages in the module> <V1 report> <V3 report>" >&2; exit 2; }
modpath=$1 want_own=$2 r1=$3 r3=$4
status=0

own_rows() { awk -F, -v m="$modpath" '$1==m || index($1, m "/")==1 {n++} END{print n+0}' "$1"; }
dep_rows() { awk -F, -v m="$modpath" '!($1==m || index($1, m "/")==1)' "$1" | sort; }

for v in "V1:$r1" "V3:$r3"; do
  grep -q . "${v#*:}" 2>/dev/null || { echo "   UNEXPECTED: ${v%%:*} has no root report (${v#*:})"; status=1; }
done
[ "$status" = 0 ] || exit 1
o1=$(own_rows "$r1"); o3=$(own_rows "$r3")
echo "   own-package rows: V1 $o1 (want $want_own), V3 $o3 (want 1)"
[ "$o1" = "$want_own" ] || { echo "   UNEXPECTED: V1 should have one row per package of the module"; status=1; }
[ "$o3" = 1 ] || { echo "   UNEXPECTED: V3 should have exactly one own row"; status=1; }
if diff <(dep_rows "$r1") <(dep_rows "$r3") >/dev/null; then
  echo "   dependency rows identical with and without the root LICENSE: yes ($(dep_rows "$r1" | grep -c .))"
else
  echo "   dependency rows identical with and without the root LICENSE: NO"; status=1
fi
exit "$status"
