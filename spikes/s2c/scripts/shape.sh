# shape.sh: the one shape of the S2c job (CONTRACT "Job shape"). Sourced by submit-line.sh and queue-check.sh,
# so the queue estimate is taken for exactly the job that is submitted. Never run on its own.
#
# One Athena A100 node, all four GPUs (one vLLM worker per GPU), 75 minutes (the science agent on #62: 01:15:00, the
# budget guard at about 70), no requeue. Every option is
# in long form with "=", the dialect internal/slurm/fakeslurm's sbatch accepts, and every word uses only
# the plain characters of its lexer, so the words need no quoting on a command line.
# The account is not part of the shape: it is private, and the callers add it themselves.
S2C_PARTITION=plgrid-gpu-a100
S2C_SHAPE=(
  --job-name=hb-s2c
  --no-requeue
  --partition="$S2C_PARTITION"
  --time=01:15:00
  --nodes=1
  --gres=gpu:4
  --cpus-per-task=64
  --mem=500G
)
