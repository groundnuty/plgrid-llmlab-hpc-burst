#!/usr/bin/env bash
# submit-line.sh: prints the sbatch line of the S2c job (one line, to standard output). It contacts nothing.
#
#   S2C_HOME=<dir> IMAGE=<path> CHECKPOINT_DIR=<dir> VR_BIN=<path> [MODE=double PORT_BASE=30420 ...] \
#     spikes/s2c/scripts/submit-line.sh
#
# The line is "sbatch --parsable <the shape of shape.sh> --wrap='<env> bash <STAGE>/scripts/job.sh'", in the
# dialect internal/slurm/fakeslurm's sbatch reads: long options with "=", words of plain characters, the
# --wrap word in single quotes (so it holds no single quote). The job's environment travels inside the
# --wrap word, as assignments before `bash`, because the fake runs the word with /bin/sh and sbatch's own
# --export would be one more option to keep in step. Each value is double-quoted inside the word.
#
# Variables passed on, when set: the four required ones (S2C_HOME IMAGE CHECKPOINT_DIR VR_BIN, all must be
# set: every missing one is named in one error, exit 2) and the optional ones in OPTIONAL below. A value is
# refused (exit 2) unless it uses only letters, digits, space and ._:=,/@%+- , so no value can end the word,
# open a substitution or reach the account. A path variable must be absolute and hold no space.
#
# The account: when S2C_PRIVATE_SETTINGS names a settings file, the line carries --account=<ACCOUNT_ATHENA_A100 of
# that file> (unreadable, or no such key: exit 2, never a line that silently has none). THEN THE LINE IS PRIVATE:
# submit.sh sends it to ssh on standard input; nobody prints it, logs it or pastes it anywhere. Without
# S2C_PRIVATE_SETTINGS the line has no account (the dry run's fake Slurm takes a job without one).
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
# shellcheck source=shape.sh
. "$HERE/shape.sh"

REQUIRED=(S2C_HOME IMAGE CHECKPOINT_DIR VR_BIN)
OPTIONAL=(MODE STAGE RESULT_TAG PLAN LOAD_ARGS LOAD_ARGS_H NWORK BOUND KV_BLOCKS_B KV_BLOCKS_A GPU_UTIL MAX_LEN MAX_NUM_SEQS
  PROBE_EST_S PROBE_MAX_S PROBE_HOLD PROBE_STREAM_TOKENS
  SERVED BUDGET_S ARM_EST_S ARM_MAX_S WORKER_WAIT_S PORT_BASE S2C_FAULT APPTAINER PYTHON
  POLL_S SAMPLE_S DRAIN_WAIT_S ENVOY_WAIT_S VR_WAIT_S RELAY_WAIT_S)
PATHS=" S2C_HOME STAGE IMAGE CHECKPOINT_DIR VR_BIN "

missing=()
for v in "${REQUIRED[@]}"; do [[ -n ${!v:-} ]] || missing+=("$v"); done
if ((${#missing[@]} > 0)); then
  echo "submit-line.sh: required variables not set: ${missing[*]}" >&2
  exit 2
fi

bad=()
wrap=""
for v in "${REQUIRED[@]}" "${OPTIONAL[@]}"; do
  [[ -n ${!v:-} ]] || continue
  val=${!v}
  if [[ ! $val =~ ^[A-Za-z0-9\ ._:=,/@%+-]+$ ]]; then bad+=("$v: a character outside the allowed set"); continue; fi
  if [[ $PATHS == *" $v "* && ( $val != /* || $val == *" "* ) ]]; then bad+=("$v: not an absolute path without spaces"); continue; fi
  wrap+="$v=\"$val\" "
done
if ((${#bad[@]} > 0)); then
  printf 'submit-line.sh: refused value(s): %s\n' "${bad[*]}" >&2
  exit 2
fi

stage=${STAGE:-$S2C_HOME/stage}
wrap+="bash $stage/scripts/job.sh"

line="sbatch --parsable ${S2C_SHAPE[*]}"
if [[ -n ${S2C_PRIVATE_SETTINGS:-} ]]; then
  [[ -r $S2C_PRIVATE_SETTINGS ]] || { echo "submit-line.sh: the private settings file is not readable" >&2; exit 2; }
  acct=$(sed -n -E 's/^(export )?ACCOUNT_ATHENA_A100=//p' "$S2C_PRIVATE_SETTINGS" | tr -d "\"'")
  [[ -n $acct ]] || { echo "submit-line.sh: ACCOUNT_ATHENA_A100 is not set in the private settings" >&2; exit 2; }
  [[ $acct =~ ^[A-Za-z0-9._-]+$ ]] || { echo "submit-line.sh: ACCOUNT_ATHENA_A100 holds a character outside A-Za-z0-9._-" >&2; exit 2; }
  line+=" --account=$acct"
fi
printf "%s --wrap='%s'\n" "$line" "$wrap"
