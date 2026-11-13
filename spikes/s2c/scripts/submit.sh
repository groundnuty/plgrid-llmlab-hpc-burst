#!/usr/bin/env bash
# submit.sh: the LIVE submit of the S2c job on Athena. It spends an allocation: no test runs it (test_job_status.sh
# and test_queue_check.sh use stubs of their own), and it refuses to run unless S2C_CONFIRM=1 is set.
#
#   S2C_CONFIRM=1 S2C_PRIVATE_SETTINGS=<file> S2C_ATHENA_LOCK=<lock file> S2C_HOME=<dir> IMAGE=<path> \
#     CHECKPOINT_DIR=<dir> VR_BIN=<path> [S2C_SUBMIT_RECORD=<file>] spikes/s2c/scripts/submit.sh
#
# The line comes from submit-line.sh, which adds the account from the private settings (like queue-check.sh),
# and travels to Athena on ssh's standard input, so the account is in no argument list and in no output. The same lock and ssh call as queue-check.sh. S2C_PRIVATE_SETTINGS and S2C_ATHENA_LOCK are
# required with no default (the VM's real paths stay out of committed files); every missing one is named at once.
#
# The sbatch reply (job ID;cluster) is the owner's and the parent's, not a result: it goes to the file
# S2C_SUBMIT_RECORD (default $TMPDIR/s2c-submit-record.txt, mode 600) and is never printed. Standard output
# says only that a job was submitted and where the record is. Exit 1: no job ID came back (the raw answer is
# kept in the record file's sibling .raw, mode 600, and is not printed either). Exit 2: refused before any call.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)

[ "${S2C_CONFIRM:-}" = 1 ] || { echo "submit.sh: this submits a real job; set S2C_CONFIRM=1 to go ahead" >&2; exit 2; }
missing=()
for v in S2C_PRIVATE_SETTINGS S2C_ATHENA_LOCK; do [ -n "${!v:-}" ] || missing+=("$v"); done
if [ ${#missing[@]} -gt 0 ]; then echo "submit.sh: required variables not set: ${missing[*]}" >&2; exit 2; fi
priv=$S2C_PRIVATE_SETTINGS
lock=$S2C_ATHENA_LOCK
[ -r "$priv" ] || { echo "submit.sh: the private settings file is not readable" >&2; exit 2; }
record=${S2C_SUBMIT_RECORD:-${TMPDIR:-/tmp}/s2c-submit-record.txt}

line=$("$HERE/submit-line.sh") || exit 2   # S2C_PRIVATE_SETTINGS reaches it through the environment
case $line in *" --account="*) ;; *) echo "submit.sh: the submit line has no account; nothing was sent" >&2; exit 2 ;; esac
umask 077
rc=0
raw=$(printf '%s\n' "$line" | flock -o -w 600 "$lock" timeout 120 ssh athena 'bash -s' 2>&1) || rc=$?
printf '%s\n' "$raw" > "$record.raw"
# The reply of --parsable is "<id>" or "<id>;<cluster>" on a line of its own; a banner or a warning is not one.
reply=$(printf '%s\n' "$raw" | grep -E -x '[0-9]+(;[A-Za-z0-9_.-]+)?' | tail -1 || true)
if [ -z "$reply" ]; then
  echo "submit.sh: no job ID came back (ssh, lock or sbatch exit $rc); the raw answer is in the record file's .raw sibling" >&2
  exit 1
fi
printf '%s\t%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$reply" > "$record"
echo "submit.sh: submitted; the reply is recorded in $record (mode 600)"
