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
# It fails (exit 1, nothing on stdout) when
#   - it finds no go.mod at all;
#   - a directory symlink lies anywhere in the searched tree: find does not follow one, so a
#     module behind it (tools/cli -> ../spikes/cli) would be left out of the check without a word.
#     Such a link is refused, and named, instead of followed (a followed link needs boundary and
#     cycle handling that this check does not want). A symlink in spikes/ is outside the check;
#   - a module directory has a character outside A-Z a-z 0-9 . _ - / (the name goes into a shell
#     command and a JSON document in CI, so an unusual one is refused, not quoted). A newline or
#     another control character gets its own message.
# The search output stays NUL-delimited until every complete path has been checked: a newline in
# a directory name must not split it into two accepted paths. Exit 2 on bad usage.
set -u -o pipefail
usage="usage: $0 [--json] <repo root>"
json=0
if [ "${1:-}" = "--json" ]; then json=1; shift; fi
[ $# -eq 1 ] || { echo "$usage" >&2; exit 2; }
root=$(cd "$1" 2>/dev/null && pwd) || { echo "$usage" >&2; exit 2; }

# The search of the checked tree: everything outside .git and the top-level spikes/. Its output is
# NUL-delimited; a pipeline reads it (with pipefail, a failing find fails the pipeline), because a
# process substitution would hide find's exit status.
search() { (cd "$root" && find . \( -path ./.git -o -path ./spikes \) -prune -o "$@"); }

# A directory symlink is refused (see above). -xtype d is a symlink whose target is a directory.
links=$(search -type l -xtype d -print0 | while IFS= read -r -d '' p; do printf '  %q\n' "$p"; done) ||
  { echo "licence-modules.sh: find failed in $root" >&2; exit 1; }
if [ -n "$links" ]; then
  echo "licence-modules.sh: refused, a directory symlink would hide any module behind it from the search (find does not follow it):" >&2
  printf '%s\n' "$links" >&2
  echo "Replace the link with the directory itself, or move what it points to out of the checked tree." >&2
  exit 1
fi

# Each complete NUL-delimited path is checked before it becomes a line; only then is the list sorted.
found=$(search -name go.mod ! -type d -printf '%h\0' | {
  while IFS= read -r -d '' p; do
    p=${p#./}
    if [[ $p == *[[:cntrl:]]* ]]; then
      printf 'licence-modules.sh: refused, a module directory with a newline or another control character in its name: %q\n' "$p" >&2
      exit 1
    fi
    if [[ ! $p =~ ^[A-Za-z0-9._/-]+$ ]]; then
      echo "licence-modules.sh: refused, unusual characters in a module directory: $p" >&2
      exit 1
    fi
    printf '%s\n' "$p"
  done
} | LC_ALL=C sort) || { echo "licence-modules.sh: the search of $root failed, or refused a path (see above)" >&2; exit 1; }
[ -n "$found" ] || { echo "licence-modules.sh: no go.mod found outside spikes/ in $root" >&2; exit 1; }

if [ "$json" = 1 ]; then
  printf '%s\n' "$found" | awk 'BEGIN{printf "{\"include\":["} {printf "%s{\"module\":\"%s\"}", (NR>1?",":""), $0} END{print "]}"}'
else
  printf '%s\n' "$found"
fi
