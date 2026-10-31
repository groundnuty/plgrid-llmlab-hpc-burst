#!/usr/bin/env bash
# HBEP-15 Test plan: "The licence check fails on a fixture module whose licence is off the
# allowlist, and on one with an unknown licence. Both fixtures' licences are written by hand."
#
# Builds a throwaway module that imports three local modules, each with a hand-written LICENSE,
# and runs go-licenses check with the allowlist of this directory:
#   goodlib     MIT text            expected: accepted
#   offlist     BSD-4-Clause text   expected: rejected, "Not allowed license"
#   mystery     invented text       expected: rejected, "Did not find license"
# and a control run over goodlib alone, which must pass (a failure there would mean the fixture
# setup, not the licences, is what breaks).
#
# Usage: licence-fixtures.sh <path to go-licenses> [work dir]   (the work dir must not exist)
set -euo pipefail
GL=$(readlink -f "${1:?usage: $0 <go-licenses binary> [workdir]}")
here=$(cd "$(dirname "$0")" && pwd)
work=${2:-$(mktemp -d)}
[ -n "$(ls -A "$work" 2>/dev/null)" ] && { echo "work dir $work is not empty" >&2; exit 2; }
LIST=$(grep -v '^#' "$here/allowed-licenses.txt" | paste -sd, -)
mkdir -p "$work"/{app,goodlib,offlist,mystery}

mklib() { # name licence-file-writer
  local n=$1
  printf 'module example.test/%s\n\ngo 1.26\n' "$n" > "$work/$n/go.mod"
  printf 'package %s\n\nfunc Name() string { return "%s" }\n' "$n" "$n" > "$work/$n/$n.go"
}
for n in goodlib offlist mystery; do mklib "$n"; done

cat > "$work/goodlib/LICENSE" <<'EOF'
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

cat > "$work/offlist/LICENSE" <<'EOF'
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

cat > "$work/mystery/LICENSE" <<'EOF'
You may use this code for any purpose, provided that you tell the author
about it afterwards and send a postcard once a year.
EOF

write_app() { # imports...
  printf 'module example.test/app\n\ngo 1.26\n\n' > "$work/app/go.mod"
  { printf 'package main\n\nimport (\n'; for i in "$@"; do printf '\t"example.test/%s"\n' "$i"; done
    printf ')\n\nfunc main() {\n'; for i in "$@"; do printf '\t_ = %s.Name()\n' "$i"; done; printf '}\n'; } > "$work/app/main.go"
  for i in "$@"; do printf 'require example.test/%s v0.0.0\nreplace example.test/%s => ../%s\n' "$i" "$i" "$i" >> "$work/app/go.mod"; done
}

run() { # label expected-exit-status-is-nonzero(1)/zero(0) grep-pattern
  local label=$1 want=$2 pat=$3 out rc=0
  out=$(cd "$work/app" && "$GL" check ./... --ignore example.test/app --allowed_licenses="$LIST" 2>&1) || rc=$?
  out=$(printf '%s\n' "$out" | grep -E "Not allowed|Did not find|Error" || true)
  echo "-- $label: exit $rc"; [ -n "$out" ] && printf '%s\n' "$out" | sed 's/^/     /'
  if [ "$want" = 0 ] && [ "$rc" != 0 ]; then echo "FAIL: $label should pass"; failures=$((failures+1)); fi
  if [ "$want" = 1 ]; then
    [ "$rc" = 0 ] && { echo "FAIL: $label should fail"; failures=$((failures+1)); }
    printf '%s\n' "$out" | grep -q -- "$pat" || { echo "FAIL: $label output lacks '$pat'"; failures=$((failures+1)); }
  fi
}
failures=0
export GOFLAGS=${GOFLAGS:-} GOWORK=off
write_app goodlib;            run "control: MIT only"                0 ""
# A check over zero packages also exits 0, so show that the control really looked at goodlib.
rep=$(cd "$work/app" && "$GL" report ./... --ignore example.test/app 2>/dev/null || true)
printf '%s\n' "$rep" | grep -q '^example.test/goodlib,.*,MIT$' \
  && echo "-- control: report lists example.test/goodlib as MIT" \
  || { echo "FAIL: control report does not list goodlib as MIT: $rep"; failures=$((failures+1)); }
write_app goodlib offlist;    run "off-list licence (BSD-4-Clause)"  1 "Not allowed license 'BSD-4-Clause' found for library 'example.test/offlist'"
write_app goodlib mystery;    run "unknown licence"                  1 "Did not find license for library 'example.test/mystery'"
[ "$failures" = 0 ] && echo "ALL FIXTURE CHECKS BEHAVED AS EXPECTED" || { echo "$failures fixture check(s) failed"; exit 1; }
