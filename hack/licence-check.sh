#!/usr/bin/env bash
# Licence check of the repository's Go modules with go-licenses and HBEP-15's allowlist. Run by
# the licences job of .github/workflows/ci.yml; moved here from spikes/s12, strict.
#
# Usage: licence-check.sh [--negative-control] <go-licenses binary> <repo root> <output dir> \
#                         [module dir ...]
#
# With no module dir it checks every module that licence-modules.sh finds: every go.mod outside
# spikes/, found by a search of the whole tree (the root module "." is one of them). The spikes
# are not distributed and are outside the check (HBEP-15). Each module runs with its own go.mod,
# so the Go version it names must be the one installed (GOTOOLCHAIN=local is fine). The allowlist
# is allowed-licenses.txt next to this script, and the known zeros are known-zero-modules.txt
# next to it. Neither can be changed from the environment or the command line.
#
# Strict, no exemption: every module's own packages need a LICENSE (the root's Apache-2.0 LICENSE
# for the root module, the copy's own LICENSE for a module under third_party/). A "Did not find
# license" line fails whatever package it names. There is no option that excuses one; an unknown
# option exits 2.
#
# No empty run: a go-licenses run over a module with no buildable package exits 0 and checks
# nothing. So each module's "go-licenses report" must show at least one package, else the run
# fails, unless the module is a known zero: listed in known-zero-modules.txt with the reason,
# which is printed. A module on that list that does have packages is only noted.
#
# Exit status: 0 when no module has a failure; 1 otherwise; 2 on bad usage. A failure is anything
# go-licenses reports, an error diagnostic go-licenses logs even when it exits 0 (licence_gate.py
# decides, on go-licenses' raw output), a go-licenses run that itself fails, and a run that
# checked no package and is not a known zero. The root module is also checked with
# --include_tests, so a licence reached only through a test import fails too.
#
# --negative-control also takes BSD-3-Clause off the allowlist for the root module; the run
# must then fail and name BSD-3-Clause packages, else the check is not looking at the list.
#
# Writes to <output dir>: go-licenses-report.csv (root report), go-licenses-check.txt (root
# runs), go-licenses-modules.txt (the other modules). Everything is also printed.
set -u -o pipefail

usage="usage: $0 [--negative-control] <go-licenses binary> <repo root> <output dir> [module dir ...]"
neg=0
while [ $# -gt 0 ]; do
  case $1 in
    --negative-control) neg=1; shift ;;
    -*) echo "$usage" >&2; exit 2 ;;
    *) break ;;
  esac
done
[ $# -ge 3 ] || { echo "$usage" >&2; exit 2; }
GL=$(readlink -f "$1")
root=$(cd "$2" && pwd) || exit 2
mkdir -p "$3" && OUT=$(cd "$3" && pwd) || exit 2
shift 3
[ -x "$GL" ] || { echo "not executable: $GL" >&2; exit 2; }
here=$(cd "$(dirname "$0")" && pwd)
ALLOWED_FILE=$here/allowed-licenses.txt
LIST=$(grep -v '^#' "$ALLOWED_FILE" | paste -sd, -)
[ -n "$LIST" ] || { echo "empty allowlist: $ALLOWED_FILE" >&2; exit 2; }
# The negative control's list: the same one without BSD-3-Clause (a whole line, not a substring).
LIST_NEG=$(grep -v '^#' "$ALLOWED_FILE" | grep -v -x 'BSD-3-Clause' | paste -sd, -)
[ "$LIST_NEG" != "$LIST" ] || { echo "BSD-3-Clause is not on the allowlist $ALLOWED_FILE, so the negative control cannot run" >&2; exit 2; }

modules=()
if [ $# -gt 0 ]; then
  for m in "$@"; do m=${m#./}; m=${m%/}; modules+=("${m:-.}"); done
else
  found=$("$here/licence-modules.sh" "$root") || { echo "licence-modules.sh failed" >&2; exit 2; }
  while IFS= read -r m; do modules+=("$m"); done <<< "$found"
  echo "spikes/: outside the check (HBEP-15)"
fi

KNOWN_ZERO=$here/known-zero-modules.txt
declare -A zero_reason=()
if [ -f "$KNOWN_ZERO" ]; then
  while IFS= read -r line; do
    case $line in ''|'#'*) continue ;; esac
    zm=${line%%:*}; zr=${line#*:}; zr=${zr# }
    if [ "$zm" = "$line" ] || [ -z "$zm" ] || [ -z "${zr//[[:space:]]/}" ]; then
      echo "known-zero list $KNOWN_ZERO: an entry needs '<module dir>: <reason>', got: $line" >&2; exit 2
    fi
    zero_reason[$zm]=$zr
  done < "$KNOWN_ZERO"
fi

tmpdir=$(mktemp -d)
trap 'rm -rf "$tmpdir"' EXIT
cat > "$tmpdir/report.tmpl" <<'EOF'
{{ range . }}{{ .Name }},{{ .Version }},{{ .LicenseURL }},{{ .LicenseName }}
{{ end }}
EOF
status=0
grc=0
gate_out=

# For display only: hide the module cache path. The gate reads go-licenses' raw output, so
# nothing here can hide a diagnostic from it.
modcache=${GOMODCACHE:-$(go env GOMODCACHE 2>/dev/null || true)}
show() { if [ -n "$modcache" ]; then sed "s#$modcache#<modcache>#g"; else cat; fi; }

# The negative control is meant to fail. Its failure lines are shown as "expected", so that
# nobody reading the log (or the job summary) takes them for real failures; gate_out keeps the
# gate's own text, which the control is judged on.
expect_fail=0

gate_run() { # label allowlist dir go-licenses-check-args...  (sets grc: 0 when the gate accepts the run)
  local label=$1 list=$2 dir=$3; shift 3
  local out rc=0
  echo; echo "== $label"
  out=$(cd "$dir" && "$GL" check "$@" --allowed_licenses="$list" 2>&1) || rc=$?
  grc=0
  gate_out=$(printf '%s\n' "$out" | python3 -I "$here/licence_gate.py" --rc "$rc") || grc=$?
  if [ "$expect_fail" = 1 ]; then
    printf '%s\n' "$gate_out" | cut -c1-200 | sed -e 's/^UNEXPECTED:/expected failure:/' -e 's/: FAIL$/: FAIL (expected: negative control)/' | show
  else
    printf '%s\n' "$gate_out" | cut -c1-200 | show
  fi
  return 0
}

check_module() { # module dir relative to the root
  local m=$1 dir rep rows
  dir=$root/$m
  echo; echo "##### module $m"
  if [ ! -f "$dir/go.mod" ]; then echo "UNEXPECTED: no go.mod in $m"; status=1; return 0; fi
  if ! rep=$(cd "$dir" && "$GL" report ./... --template "$tmpdir/report.tmpl" 2>"$tmpdir/report.err"); then
    echo "UNEXPECTED: go-licenses report failed: $(head -3 "$tmpdir/report.err" | show)"
    status=1; return 0
  fi
  rep=$(printf '%s\n' "$rep" | sed '/^$/d')
  rows=$(printf '%s' "$rep" | grep -c . || true)
  echo "allowlist: $LIST"
  echo "report rows: $rows"
  if [ "$m" = . ]; then printf '%s\n' "$rep" > "$OUT/go-licenses-report.csv"; fi
  # A check over zero packages also exits 0, so a run that saw nothing fails unless it is a known zero.
  if [ "$rows" -eq 0 ]; then
    if [ -n "${zero_reason[$m]:-}" ]; then
      echo "known zero: $m has no package to check; reason: ${zero_reason[$m]}"
    else
      echo "UNEXPECTED: go-licenses saw no package in $m, so a check over it would pass and check nothing (list it in known-zero-modules.txt, with the reason, only if that is expected)"
      status=1
    fi
  elif [ -n "${zero_reason[$m]:-}" ]; then
    echo "NOTE: $m is listed as a known zero, but go-licenses saw $rows packages; remove the entry"
  fi
  gate_run "check ./..." "$LIST" "$dir" ./...
  [ "$grc" = 0 ] || status=1
  if [ "$m" = . ]; then
    gate_run "check ./... --include_tests (licences only tests reach)" "$LIST" "$dir" ./... --include_tests
    [ "$grc" = 0 ] || status=1
    if [ "$neg" = 1 ]; then
      expect_fail=1
      gate_run "negative control: BSD-3-Clause taken off the list (must fail)" "$LIST_NEG" "$dir" ./...
      expect_fail=0
      if [ "$grc" != 0 ] && printf '%s\n' "$gate_out" | grep -q "Not allowed license 'BSD-3-Clause'"; then
        echo "negative control: failed and named BSD-3-Clause packages, as it should"
      else
        echo "NEGATIVE CONTROL FAILED"; status=1
      fi
    fi
  fi
  return 0
}

: > "$OUT/go-licenses-check.txt"
: > "$OUT/go-licenses-modules.txt"
for m in "${modules[@]}"; do
  if [ "$m" = . ]; then f=$OUT/go-licenses-check.txt; else f=$OUT/go-licenses-modules.txt; fi
  check_module "$m" > "$tmpdir/section.txt" 2>&1
  cat "$tmpdir/section.txt"; cat "$tmpdir/section.txt" >> "$f"
done
echo; echo "licence check: $([ "$status" = 0 ] && echo PASS || echo FAIL)"
exit "$status"
