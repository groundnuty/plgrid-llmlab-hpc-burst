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
# Part 2 runs licence-check.sh (the gate that reproduce.sh and the CI jobs use) on fixture
# repositories and checks its exit status:
#   control      a root module without a LICENSE, a clean dependency, a clean nested module,
#                with --local-permit-own (local only)                                          exit 0
#   strict       the same without that option (the default, and what CI runs)                  exit 1
#   licensed     the same with a LICENSE at the root, strict                                   exit 0
#   spike        a root with a LICENSE and a separate module spikes/x without one, strict,
#                CHECK_SPIKES=1: the root's LICENSE does not reach it; with its own LICENSE
#                it passes; a second spike importing the first needs the first's LICENSE too
#   siblings     an off-list, and an unknown, dependency at example.test/proj-extra and
#                example.test/proj-unknown: the root is example.test/proj, so a prefix match
#                on the root's path would hide them                                           exit 1
#   beneath      the same two at example.test/proj/sub and example.test/proj/unk, separate
#                modules whose paths lie beneath the root's                                   exit 1
#   nested       a nested module (third_party/forked/nested) with an off-list dependency      exit 1
#   test-only    an off-list dependency imported only by a test of the root module           exit 1
# The cases below that probe the dependency checks run with --local-permit-own, so that the root's
# own missing LICENSE does not decide the result: they show a dependency is never exempt.
# Every "exit 1" case must also name the offending library, so it fails for the right reason.
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

# ---- part 2: licence-check.sh on fixture repositories ---------------------------------------
libs=$work/libs
declare -A libdir
newlib() { # dir module-path licence(goodlib|offlist|mystery: the directory whose LICENSE to copy)
  libdir[$2]=$libs/$1
  mkdir -p "$libs/$1"
  printf 'module %s\n\ngo 1.26\n' "$2" > "$libs/$1/go.mod"
  printf 'package lib\n\nfunc Name() string { return "%s" }\n' "$2" > "$libs/$1/lib.go"
  cp "$work/$3/LICENSE" "$libs/$1/LICENSE"
}
newlib good       example.test/lic-good        goodlib
newlib off        example.test/lic-off         offlist
newlib sibling    example.test/proj-extra      offlist
newlib siblingunk example.test/proj-unknown    mystery
newlib beneath    example.test/proj/sub        offlist
newlib beneathunk example.test/proj/unk        mystery

gomod_with() { # module-path imports...
  local mod=$1; shift
  printf 'module %s\n\ngo 1.26\n\n' "$mod"
  local m; for m in "$@"; do printf 'require %s v0.0.0\nreplace %s => %s\n' "$m" "$m" "${libdir[$m]}"; done
}
imports_of() { # pkg imports... -> a Go file body that uses each import
  local pkg=$1 i=0 m; shift
  printf 'package %s\n\nimport (\n' "$pkg"
  for m in "$@"; do printf '\tl%d "%s"\n' "$i" "$m"; i=$((i+1)); done
  printf ')\n\nfunc use() {\n'
  i=0; for m in "$@"; do printf '\t_ = l%d.Name()\n' "$i"; i=$((i+1)); done
  printf '}\n'
}
mkrepo() { # dir root-imports(space separated) [nested-imports(space separated)] [test-only-imports]
  local d=$1 ri=$2 ni=${3:-} ti=${4:-}
  mkdir -p "$d"
  gomod_with example.test/proj $ri $ti > "$d/go.mod"
  { imports_of main $ri; printf '\nfunc main() { use() }\n'; } > "$d/main.go"
  if [ -n "$ti" ]; then
    local i=0 m
    { printf 'package main\n\nimport (\n\t"testing"\n'
      for m in $ti; do printf '\tt%d "%s"\n' "$i" "$m"; i=$((i+1)); done
      printf ')\n\nfunc TestUse(t *testing.T) {\n'
      i=0; for m in $ti; do printf '\t_ = t%d.Name()\n' "$i"; i=$((i+1)); done
      printf '}\n'; } > "$d/main_test.go"
  fi
  if [ -n "$ni" ]; then
    local n=$d/third_party/forked/nested
    mkdir -p "$n"
    gomod_with example.test/proj/third_party/forked/nested $ni > "$n/go.mod"
    imports_of nested $ni > "$n/nested.go"
    cp "$work/goodlib/LICENSE" "$n/LICENSE"
  fi
}

gate_case() { # label want(0|1) pattern repo [licence-check.sh options...]
  local label=$1 want=$2 pat=$3 repo=$4 out rc=0; shift 4
  out=$("$here/licence-check.sh" "$@" "$GL" "$repo" "$work/out-$(basename "$repo")" 2>&1) || rc=$?
  echo "-- $label: exit $rc"
  printf '%s\n' "$out" | grep -E '^(UNEXPECTED|licence check|WARNING)|^permitted' | sort -u | sed 's/^/     /'
  if [ "$want" = 0 ] && [ "$rc" != 0 ]; then echo "FAIL: $label should pass"; failures=$((failures+1)); fi
  if [ "$want" = 1 ]; then
    [ "$rc" = 0 ] && { echo "FAIL: $label should fail"; failures=$((failures+1)); }
  fi
  printf '%s\n' "$out" | grep -q -F -- "$pat" || { echo "FAIL: $label output lacks '$pat'"; failures=$((failures+1)); }
}
BSD4="Not allowed license 'BSD-4-Clause' found for library"
NOLIC="Did not find license for library"
echo
echo "== licence-check.sh on fixture repositories"
mkrepo "$work/r-control" "example.test/lic-good" "example.test/lic-good"
LOCAL=--local-permit-own   # LOCAL ONLY: the CI job never passes it
gate_case "control: no root LICENSE (permitted with $LOCAL), clean dependency, clean nested module" 0 \
  "permitted (a package of this module, which has no LICENSE): example.test/proj" "$work/r-control" $LOCAL
gate_case "same repository, strict (the default, as CI runs it)" 1 "UNEXPECTED: $NOLIC 'example.test/proj'" "$work/r-control"
mkrepo "$work/r-licensed" "example.test/lic-good" "example.test/lic-good"
cp "$work/goodlib/LICENSE" "$work/r-licensed/LICENSE"
gate_case "same repository with a LICENSE at the root, strict" 0 "licence check: PASS" "$work/r-licensed"
# A separate module under spikes/ is not reached by the root's LICENSE: go-licenses stops its
# search for a licence file at the module's own directory (licenses/find.go, FindCandidates).
mkspike() { # repo name has-own-licence(0|1) imports...
  local repo=$1 name=$2 lic=$3 m d; shift 3
  d=$repo/spikes/$name
  mkdir -p "$d"
  { printf 'module example.test/proj/spikes/%s\n\ngo 1.26\n\n' "$name"
    for m in "$@"; do printf 'require %s v0.0.0\nreplace %s => %s\n' "$m" "$m" "${libdir[$m]}"; done; } > "$d/go.mod"
  { imports_of "$name" "$@"; printf '\nfunc Name() string { use(); return "%s" }\n' "$name"; } > "$d/$name.go"
  if [ "$lic" = 1 ]; then cp "$work/goodlib/LICENSE" "$d/LICENSE"; fi
}
mkrepo "$work/r-spike" "example.test/lic-good"
cp "$work/goodlib/LICENSE" "$work/r-spike/LICENSE"
mkspike "$work/r-spike" x 0 example.test/lic-good
CHECK_SPIKES=1 gate_case "LICENSE at the root, a separate module spikes/x without one, strict, CHECK_SPIKES=1" 1 \
  "UNEXPECTED: $NOLIC 'example.test/proj/spikes/x'" "$work/r-spike"
mkrepo "$work/r-spike-lic" "example.test/lic-good"
cp "$work/goodlib/LICENSE" "$work/r-spike-lic/LICENSE"
mkspike "$work/r-spike-lic" x 1 example.test/lic-good
CHECK_SPIKES=1 gate_case "the same with a LICENSE in spikes/x itself, strict, CHECK_SPIKES=1" 0 "licence check: PASS" "$work/r-spike-lic"
mkrepo "$work/r-sibling" "example.test/lic-good example.test/proj-extra"
gate_case "off-list dependency at a sibling path sharing the root's prefix (proj-extra)" 1 \
  "UNEXPECTED: $BSD4 'example.test/proj-extra'" "$work/r-sibling" $LOCAL
mkrepo "$work/r-siblingunk" "example.test/lic-good example.test/proj-unknown"
gate_case "unknown-licence dependency at a sibling path sharing the prefix (proj-unknown)" 1 \
  "UNEXPECTED: $NOLIC 'example.test/proj-unknown'" "$work/r-siblingunk" $LOCAL
mkrepo "$work/r-beneath" "example.test/lic-good example.test/proj/sub"
gate_case "off-list dependency in a separate module beneath the root's path (proj/sub)" 1 \
  "UNEXPECTED: $BSD4 'example.test/proj/sub'" "$work/r-beneath" $LOCAL
mkrepo "$work/r-beneathunk" "example.test/lic-good example.test/proj/unk"
gate_case "unknown-licence dependency in a separate module beneath the root's path (proj/unk)" 1 \
  "UNEXPECTED: $NOLIC 'example.test/proj/unk'" "$work/r-beneathunk" $LOCAL
mkrepo "$work/r-nested" "example.test/lic-good" "example.test/lic-good example.test/lic-off"
gate_case "off-list dependency of a nested module (the root itself is fine)" 1 \
  "UNEXPECTED: $BSD4 'example.test/lic-off'" "$work/r-nested" $LOCAL
mkrepo "$work/r-testonly" "example.test/lic-good" "" "example.test/lic-off"
gate_case "off-list dependency imported only by a test of the root module" 1 \
  "UNEXPECTED: $BSD4 'example.test/lic-off'" "$work/r-testonly" $LOCAL
echo "-- for comparison, go-licenses' own --ignore example.test/proj, a path prefix, on the same fixtures (exit 0 = the dependency is hidden):"
for r in sibling siblingunk beneath beneathunk; do
  rc=0; (cd "$work/r-$r" && "$GL" check ./... --ignore example.test/proj --allowed_licenses="$LIST" >/dev/null 2>&1) || rc=$?
  echo "     r-$r: exit $rc"
done

[ "$failures" = 0 ] && echo "ALL FIXTURE CHECKS BEHAVED AS EXPECTED" || { echo "$failures fixture check(s) failed"; exit 1; }
