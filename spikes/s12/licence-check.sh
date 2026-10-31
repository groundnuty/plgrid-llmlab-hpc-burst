#!/usr/bin/env bash
# Licence check of a repository with go-licenses and HBEP-15's allowlist. Used by
# reproduce.sh, by licence-fixtures.sh, and (moved to hack/) by the proposed CI jobs.
#
# Usage: licence-check.sh [--local-permit-own] [--negative-control] <go-licenses binary> \
#                         <repo root> <output dir> [module dir ...]
#
# Checks the root module (".") and, unless module dirs are given, every go.mod under
# third_party/ (and under spikes/ when CHECK_SPIKES=1). Each module runs with its own go.mod,
# so the Go version it names must be the one installed (GOTOOLCHAIN=local is fine).
# The allowlist is allowed-licenses.txt next to this script, or $ALLOWED_LICENSES.
#
# Exit status: 0 when no module has an unexpected failure; 1 otherwise; 2 on bad usage.
# An unexpected failure is anything go-licenses reports (except the one case that
# --local-permit-own permits, below), and also a go-licenses or go list run that itself fails. The root module is also checked
# with --include_tests, so a licence reached only through a test import fails too.
#
# Strict by default: a package of the module under check that has no LICENSE ("Did not find
# license") is a failure, for every module. That is how the proposed CI job calls this script,
# and it is the state once the repository has a root LICENSE (HBEP-15 open question 1, the
# owner's decision). Without one, the root module's own packages cannot pass.
#
# --local-permit-own (LOCAL ONLY, never for CI): permit that one failure for the root module
# and for spikes/*, never for third_party/*, whose shipped copies carry their own LICENSE. It
# exists so reproduce.sh and licence-fixtures.sh can run on a repository that has no LICENSE
# yet. The permission is decided by licence_gate.py against the packages that "go list"
# assigns to the module under check (Module.Main), so it cannot cover a dependency, a module
# nested beneath the root's path, or a repository whose name merely starts with the same text.
# That is why this script does not use go-licenses' --ignore, which matches a path prefix.
# The copy that moves to hack/ has no such option.
#
# --negative-control also takes BSD-3-Clause off the allowlist for the root module; the run
# must then fail and name BSD-3-Clause packages, else the check is not looking at the list.
#
# Writes to <output dir>: go-licenses-report.csv (root report), go-licenses-check.txt (root
# runs), go-licenses-modules.txt (the other modules). Everything is also printed.
set -u -o pipefail

usage="usage: $0 [--local-permit-own] [--negative-control] <go-licenses binary> <repo root> <output dir> [module dir ...]"
permit_own=0
neg=0
while [ $# -gt 0 ]; do
  case $1 in
    --local-permit-own) permit_own=1; shift ;;
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
LIST=$(grep -v '^#' "${ALLOWED_LICENSES:-$here/allowed-licenses.txt}" | paste -sd, -)
[ -n "$LIST" ] || { echo "empty allowlist" >&2; exit 2; }

modules=()
skipped=()
if [ $# -gt 0 ]; then
  modules=("$@")
else
  modules=(.)
  while IFS= read -r m; do
    case $m in
      spikes/*) [ "${CHECK_SPIKES:-0}" = 1 ] || { skipped+=("$m: skipped (CHECK_SPIKES=1 runs it)"); continue; } ;;
    esac
    modules+=("$m")
  done < <(cd "$root" && find third_party spikes -name go.mod -printf '%h\n' 2>/dev/null | sort)
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

# Drop go-licenses' warnings about assembly files (they carry module cache paths) and hide the cache path.
clean() { { grep -v -e "non-Go code" -e '\.s$' || true; } | sed "s#$(go env GOMODCACHE)#<modcache>#g"; }

permit_for() { # module dir -> 1 when an own package without a LICENSE is permitted
  case $1 in
    .|spikes/*) echo "$permit_own" ;;
    *) echo 0 ;;
  esac
}

gate_run() { # label permit dir go-licenses-check-args...  (sets grc: 0 when the gate accepts the run)
  local label=$1 permit=$2 dir=$3; shift 3
  local out rc=0 own gout
  echo; echo "== $label"
  out=$(cd "$dir" && "$GL" check "$@" --allowed_licenses="$LIST" 2>&1) || rc=$?
  if ! own=$(cd "$dir" && go list -buildvcs=false -deps -test -f '{{if .Module}}{{if .Module.Main}}{{.ImportPath}}{{end}}{{end}}' ./... 2>"$tmpdir/golist.err"); then
    echo "UNEXPECTED: go list failed: $(clean < "$tmpdir/golist.err" | head -3)"
    grc=1; gate_out=; return 0
  fi
  grc=0
  gate_out=$(printf '%s\n' "$out" | clean | python3 -I "$here/licence_gate.py" --rc "$rc" --permit-own "$permit" \
               --own-packages <(printf '%s\n' "$own")) || grc=$?
  printf '%s\n' "$gate_out" | cut -c1-200
  return 0
}

check_module() { # module dir relative to the root
  local m=$1 dir permit rep rows
  dir=$root/$m
  permit=$(permit_for "$m")
  echo; echo "##### module $m: an own package without a LICENSE is $([ "$permit" = 1 ] && echo permitted || echo a failure)"
  if [ ! -f "$dir/go.mod" ]; then echo "UNEXPECTED: no go.mod in $m"; status=1; return 0; fi
  if ! rep=$(cd "$dir" && "$GL" report ./... --template "$tmpdir/report.tmpl" 2>"$tmpdir/report.err"); then
    echo "UNEXPECTED: go-licenses report failed: $(clean < "$tmpdir/report.err" | head -3)"
    status=1; return 0
  fi
  rep=$(printf '%s\n' "$rep" | sed '/^$/d')
  rows=$(printf '%s' "$rep" | grep -c . || true)
  echo "allowlist: $LIST"
  echo "report rows: $rows"
  if [ "$m" = . ]; then
    printf '%s\n' "$rep" > "$OUT/go-licenses-report.csv"
    # A check over zero packages also exits 0, so a root run that saw nothing is a failure.
    [ "$rows" -gt 0 ] || { echo "UNEXPECTED: go-licenses report is empty at the root: a check over zero packages would also pass"; status=1; }
  elif [ "$rows" -eq 0 ]; then
    echo "WARNING: go-licenses saw no package in $m, so nothing is checked there"
  fi
  gate_run "check ./..." "$permit" "$dir" ./...
  [ "$grc" = 0 ] || status=1
  if [ "$m" = . ]; then
    gate_run "check ./... --include_tests (licences only tests reach)" "$permit" "$dir" ./... --include_tests
    [ "$grc" = 0 ] || status=1
    if [ "$neg" = 1 ]; then
      local saved=$LIST
      LIST=${LIST/BSD-3-Clause,/}
      gate_run "negative control: BSD-3-Clause taken off the list (must fail)" "$permit" "$dir" ./...
      LIST=$saved
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
if [ "${#skipped[@]}" -gt 0 ]; then
  printf '%s\n' "${skipped[@]}" | tee -a "$OUT/go-licenses-modules.txt"
fi
echo; echo "licence check: $([ "$status" = 0 ] && echo PASS || echo FAIL)"
exit "$status"
