#!/usr/bin/env bash
# The modules the licence check runs on (HBEP-15, "The check"): every go.mod outside spikes/,
# found by a search of the whole tree, never by a fixed list or a search of one folder. A shipped
# module added anywhere (a cmd/ tool, a third_party folder, a new top-level folder) is therefore
# checked without anyone editing a list.
#
# Usage: licence-modules.sh [--json] <repo root>
#
# Prints one module directory per line, relative to the repo root, sorted ("." is the root
# module). With --json it prints one line for a GitHub Actions matrix:
#   {"include":[{"module":"."},{"module":"third_party/forked/kubeai"}]}
#
# Only the top-level spikes/ folder is outside the check (the spikes are not distributed; the
# HBEP says so). A folder named spikes anywhere else is searched like any other, and so is a
# folder such as testdata or vendor: nothing is excluded quietly. .git is not searched.
#
# It fails (exit 1, nothing on stdout) when it finds no go.mod at all, or a module directory with
# a character outside A-Z a-z 0-9 . _ - / (the name goes into a shell command and a JSON
# document in CI, so an unusual one is refused, not quoted). Exit 2 on bad usage.
set -u -o pipefail
usage="usage: $0 [--json] <repo root>"
json=0
if [ "${1:-}" = "--json" ]; then json=1; shift; fi
[ $# -eq 1 ] || { echo "$usage" >&2; exit 2; }
root=$(cd "$1" 2>/dev/null && pwd) || { echo "$usage" >&2; exit 2; }

found=$(cd "$root" && find . \( -path ./.git -o -path ./spikes \) -prune -o -name go.mod ! -type d -printf '%h\n' |
        sed -e 's#^\./##' | LC_ALL=C sort) || { echo "licence-modules.sh: find failed in $root" >&2; exit 1; }
[ -n "$found" ] || { echo "licence-modules.sh: no go.mod found outside spikes/ in $root" >&2; exit 1; }
bad=$(printf '%s\n' "$found" | grep -v -E '^[A-Za-z0-9._/-]+$' || true)
[ -z "$bad" ] || { echo "licence-modules.sh: refused, unusual characters in a module directory: $bad" >&2; exit 1; }

if [ "$json" = 1 ]; then
  printf '%s\n' "$found" | awk 'BEGIN{printf "{\"include\":["} {printf "%s{\"module\":\"%s\"}", (NR>1?",":""), $0} END{print "]}"}'
else
  printf '%s\n' "$found"
fi
