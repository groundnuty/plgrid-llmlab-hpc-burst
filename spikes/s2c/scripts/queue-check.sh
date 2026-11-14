#!/usr/bin/env bash
# queue-check.sh: when would the S2c job start? One `sbatch --test-only` on Athena for the job's own shape
# (shape.sh). Nothing is submitted. Mirrors the daily Athena A100 check of #145, with the S2c shape.
#
#   S2C_PRIVATE_SETTINGS=<file with ACCOUNT_ATHENA_A100=...> S2C_ATHENA_LOCK=<lock file> spikes/s2c/scripts/queue-check.sh
#
# Standard output is ONE line: `to start at <time>` (Athena's local time), or `no estimate` when sbatch
# answered with no start time, or `no estimate` when sbatch refused the shape (then also exit 3, and a line
# on standard error that says so without quoting sbatch). A login banner, a job ID, a node, the account or a
# URL never reaches either stream: the remote script prints one of three fixed words, and only a line equal
# to one of them is read back from ssh. Exit 1: ssh or the lock failed. Exit 2: the settings are unusable.
#
# Both S2C_PRIVATE_SETTINGS and S2C_ATHENA_LOCK are required and have no default, because the real paths of
# this VM must not be in a committed file; every missing one is named in one error (exit 2). The lock is the
# one-ssh-call-at-a-time lock of Athena shared by every agent (SPIKE-COMMON), taken with `flock -o`
# (IMPL-COMMON: no child keeps it).
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=shape.sh
. "$HERE/shape.sh"

missing=()
for v in S2C_PRIVATE_SETTINGS S2C_ATHENA_LOCK; do [ -n "${!v:-}" ] || missing+=("$v"); done
if [ ${#missing[@]} -gt 0 ]; then echo "queue-check.sh: required variables not set: ${missing[*]}" >&2; exit 2; fi
priv=$S2C_PRIVATE_SETTINGS
lock=$S2C_ATHENA_LOCK
[ -r "$priv" ] || { echo "queue-check.sh: the private settings file is not readable" >&2; exit 2; }
acct=$(sed -n -E 's/^(export )?ACCOUNT_ATHENA_A100=//p' "$priv" | tr -d "\"'")
[ -n "$acct" ] || { echo "queue-check.sh: ACCOUNT_ATHENA_A100 is not set in the private settings" >&2; exit 2; }
# The account goes inside single quotes of the remote script: refuse anything that could end them.
[[ $acct =~ ^[A-Za-z0-9._-]+$ ]] || { echo "queue-check.sh: ACCOUNT_ATHENA_A100 holds a character outside A-Za-z0-9._-" >&2; exit 2; }

remote=$(cat <<EOF
out=\$(sbatch --test-only ${S2C_SHAPE[*]} --account='$acct' --wrap true 2>&1); rc=\$?
est=\$(printf '%s\n' "\$out" | grep -o 'to start at [0-9T:-]*' | head -1)
if [ "\$rc" -ne 0 ]; then echo S2C-REFUSED
elif [ -n "\$est" ]; then echo "\$est"
else echo "no estimate"
fi
EOF
)

rc=0
raw=$(printf '%s\n' "$remote" | flock -o -w 600 "$lock" timeout 120 ssh athena 'bash -s' 2>/dev/null) || rc=$?
# Only a line that is exactly one of the three answers is kept (the last one): a banner cannot pass.
answer=$(printf '%s\n' "$raw" | grep -E -x 'to start at [0-9T:-]+|no estimate|S2C-REFUSED' | tail -1 || true)
if [ -z "$answer" ]; then
  echo "queue-check.sh: no answer from Athena (ssh, lock or remote script exit $rc)" >&2
  exit 1
fi
if [ "$answer" = S2C-REFUSED ]; then
  echo "no estimate"
  echo "queue-check.sh: sbatch refused the S2c shape (its message is not shown here)" >&2
  exit 3
fi
echo "$answer"
