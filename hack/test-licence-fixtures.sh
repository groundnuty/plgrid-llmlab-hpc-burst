#!/usr/bin/env bash
# End-to-end test of the licence check with the real go-licenses (HBEP-15, Test plan: "The licence
# check fails on a fixture module whose licence is off the allowlist, and on one with an unknown
# licence. Both fixtures' licences are written by hand."). Ported, strict, from the S12 spike's
# licence-fixtures.sh (spikes/s12). The wrapper tests (test_licence_check.py) use a fake scanner;
# this one runs hack/licence-check.sh, allowed-licenses.txt and licence_gate.py on tiny
# repositories with the go-licenses binary CI installs, so it is the test of what the scanner
# really prints and of what the gate makes of it.
#
# Usage: test-licence-fixtures.sh <go-licenses binary> [work dir]   (the work dir must be empty)
#
# Each case is a throwaway repository, a root module example.test/proj with a hand-written MIT
# LICENSE of its own, importing one local dependency module through a replace directive (no
# network). Each case runs hack/licence-check.sh, strict, as CI does, and checks the exit status
# and the line that must name the offending library. A rejection is accepted only with that line,
# so a run that fails for another reason does not pass the case.
#
#   control      a dependency with an MIT LICENSE                  exit 0, PASS, and the report lists it
#   off-list     a dependency with a BSD-4-Clause LICENSE          exit 1, "Not allowed license
#                                                                  'BSD-4-Clause' found for library"
#   unknown      a dependency with an invented LICENSE             exit 1, "Did not find license for
#                                                                  library"
#   unreadable   a dependency package whose own LICENSE is an unreadable (dangling) symlink, beside
#                a readable LICENSE at its module's root. go-licenses logs "Failed to parse" and
#                falls back to the module's LICENSE, exiting 0; the gate must fail the run on the
#                klog error line (licence_gate.py). exit 1, "gate: go-licenses exit=0 ... FAIL"
#   readable     the same dependency without the dangling file       exit 0 (shows the fixture
#                is not what fails in the case above)
set -euo pipefail
GL=$(readlink -f "${1:?usage: $0 <go-licenses binary> [workdir]}")
here=$(cd "$(dirname "$0")" && pwd)
[ -x "$GL" ] || { echo "not executable: $GL" >&2; exit 2; }
if [ -n "${2:-}" ]; then
  work=$2; mkdir -p "$work"
  [ -z "$(ls -A "$work")" ] || { echo "work dir $work is not empty" >&2; exit 2; }
else
  work=$(mktemp -d); trap 'rm -rf "$work"' EXIT
fi
work=$(cd "$work" && pwd)
# Offline and alone: the fixtures need no module download, no workspace and no other toolchain.
export GOPROXY=off GOWORK=off GOTOOLCHAIN=${GOTOOLCHAIN:-local} GOFLAGS=${GOFLAGS:-}

mkdir -p "$work/libs"
mit() {
  cat <<'EOF'
MIT License

Copyright (c) 2026 Example Author

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
EOF
}
bsd4() { # off the allowlist: the advertising clause makes it BSD-4-Clause
  cat <<'EOF'
Copyright (c) 2026 Example Author. All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
   list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.
3. All advertising materials mentioning features or use of this software must
   display the following acknowledgement: This product includes software
   developed by Example Author.
4. Neither the name of the copyright holder nor the names of its contributors
   may be used to endorse or promote products derived from this software
   without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
EOF
}
unknown() { # no known licence text at all
  cat <<'EOF'
You may use this code for any purpose, provided that you tell the author
about it afterwards and send a postcard once a year.
EOF
}

# newlib <dir> <licence-writer>: a dependency module example.test/<dir> with one package and a LICENSE
newlib() {
  local d=$work/libs/$1
  mkdir -p "$d"
  printf 'module example.test/%s\n\ngo 1.26\n' "$1" > "$d/go.mod"
  printf 'package lib\n\nfunc Name() string { return "%s" }\n' "$1" > "$d/lib.go"
  "$2" > "$d/LICENSE"
}
newlib lic-good mit
newlib lic-off bsd4
newlib lic-unknown unknown
# A module with a LICENSE at its root and a package sub/ beneath it; with $2 = 1 the package has a
# LICENSE of its own that cannot be opened (the symlink dangles, whoever runs the test).
newsub() {
  local d=$work/libs/$1
  mkdir -p "$d/sub"
  printf 'module example.test/%s\n\ngo 1.26\n' "$1" > "$d/go.mod"
  printf 'package sub\n\nfunc Name() string { return "%s" }\n' "$1" > "$d/sub/sub.go"
  mit > "$d/LICENSE"
  if [ "$2" = 1 ]; then ln -s does-not-exist "$d/sub/LICENSE"; fi
}
newsub lic-unreadable 1
newsub lic-readable 0

# newrepo <name> <dependency module> <package imported>: a root module with its own MIT LICENSE
newrepo() {
  local r=$work/$1
  mkdir -p "$r"
  mit > "$r/LICENSE"
  printf 'module example.test/proj\n\ngo 1.26\n\nrequire example.test/%s v0.0.0\nreplace example.test/%s => %s\n' \
    "$2" "$2" "$work/libs/$2" > "$r/go.mod"
  printf 'package main\n\nimport l "example.test/%s"\n\nfunc main() { _ = l.Name() }\n' "$3" > "$r/main.go"
}

failures=0
fail() { echo "FAIL: $*"; failures=$((failures+1)); }

# check <label> <repo> <want exit: 0|1> <pattern that must be in the output> [<pattern that must not>]
# Leaves the output in $out and the exit status in $rc.
check() {
  local label=$1 repo=$work/$2 want=$3 pat=$4 notpat=${5:-}
  rc=0
  out=$("$here/licence-check.sh" "$GL" "$repo" "$work/out-$2" 2>&1) || rc=$?
  echo "-- $label: exit $rc"
  printf '%s\n' "$out" | grep -E '^(UNEXPECTED|gate:|licence check)' | sed -e "s#$work#<work>#g" -e 's/\bE[0-9]\{4\} [0-9:.]* *[0-9]* /E<time> <pid> /' | sort -u | cut -c1-200 | sed 's/^/     /'
  [ "$rc" = "$want" ] || fail "$label: exit $rc, want $want"
  printf '%s\n' "$out" | grep -q -F -- "$pat" || fail "$label: the output lacks '$pat'"
  if [ -n "$notpat" ] && printf '%s\n' "$out" | grep -F -- "$notpat" | grep -q '^UNEXPECTED'; then
    fail "$label: a failure line names '$notpat'"
  fi
}

newrepo r-control lic-good lic-good
check "control: MIT dependency" r-control 0 "licence check: PASS" "UNEXPECTED"
# A check over zero packages also exits 0, so show that the control really looked at the dependency.
grep -q '^example.test/lic-good,.*,MIT$' "$work/out-r-control/go-licenses-report.csv" ||
  fail "control: the report does not list example.test/lic-good as MIT"

newrepo r-off lic-off lic-off
check "off-list licence (BSD-4-Clause)" r-off 1 \
  "UNEXPECTED: Not allowed license 'BSD-4-Clause' found for library 'example.test/lic-off'"

newrepo r-unknown lic-unknown lic-unknown
check "unknown licence" r-unknown 1 \
  "UNEXPECTED: Did not find license for library 'example.test/lic-unknown'"

# go-licenses exits 0 here: the unreadable file is only logged. The failure is the gate's.
newrepo r-unreadable lic-unreadable lic-unreadable/sub
check "unreadable package LICENSE beside a readable module LICENSE" r-unreadable 1 \
  "gate: go-licenses exit=0, unexpected=1: FAIL"
printf '%s\n' "$out" | grep -q '^UNEXPECTED: E[0-9]* .*library.go:[0-9]*\] Failed to parse .*lic-unreadable/sub/LICENSE' ||
  fail "unreadable: no 'Failed to parse' error line naming lic-unreadable/sub/LICENSE"

newrepo r-readable lic-readable lic-readable/sub
check "control: the same dependency without the unreadable file" r-readable 0 "licence check: PASS" "UNEXPECTED"

[ "$failures" = 0 ] && echo "ALL LICENCE FIXTURE CHECKS BEHAVED AS EXPECTED" || { echo "$failures licence fixture check(s) failed"; exit 1; }
