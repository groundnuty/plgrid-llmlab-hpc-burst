// Package fakeslurm implements a fake Slurm command line for tests.
//
// One multi-call binary behaves as sbatch, squeue, sacct, scancel and the
// control command fakeslurmctl, chosen by the base name of argv[0]. All state
// lives in one JSON file; every command takes an exclusive flock on
// <state>.lock, reads the state, modifies it and rewrites it atomically
// (temp file + rename) before releasing the lock.
//
// The fake exists to test a provider that must never resubmit a job. Slurm
// forgets finished jobs from squeue after MinJobAge while sacct keeps them;
// the log of sbatch calls is the evidence the tests count.
//
// # Environment
//
//	FAKE_SLURM_STATE         required: path of the JSON state file
//	FAKE_SLURM_LOG           required: JSON-lines event log, appended under the lock
//	FAKE_SLURM_CALLER        free text copied into every log line
//	FAKE_SLURM_MIN_JOB_AGE   seconds a terminal job stays in squeue (default 10)
//	FAKE_SLURM_ACCT_DELAY    seconds sacct lags behind (default 0): each job is
//	                         shown as it was at now - delay
//	FAKE_SLURM_DATA_PARSER   v0.0.42 (default, Slurm 24.11) or v0.0.40 (Slurm 23.11);
//	                         v0.0.40 sacct jobs carry no restart_cnt
//	FAKE_SLURM_USER          user name of submitted jobs and of --me (default spike)
//	FAKE_SLURM_STORE_COMMENT 1: sacct returns the job comment (default: empty,
//	                         as slurmdbd without AccountingStoreFlags=job_comment)
//	TZ                       zone for sacct -S/-E and the default -S (UTC if unset)
//
// sbatch hooks (the sleeps and the block run outside the lock):
//
//	FAKE_SLURM_SBATCH_DELAY_BEFORE  seconds to sleep before registering the job
//	FAKE_SLURM_SBATCH_DELAY_AFTER   seconds to sleep after registering, before printing
//	FAKE_SLURM_SBATCH_BLOCK_MARKER  path: after registering, write
//	                                {"pid":<pid>,"job_id":<id>} there (atomically),
//	                                then block until killed; nothing is printed
//	FAKE_SLURM_SBATCH_FAIL          1: print a submission failure, exit 1, register nothing
//
// # Log
//
// Each event is one JSON line: ts (RFC3339Nano UTC), unix_ms, event, cmd,
// argv, caller, pid, job_id (when known) and exit (null for sbatch_start).
// Events: sbatch_start (on invocation, before any delay), sbatch_submitted,
// sbatch_failed, squeue, sacct, scancel (one per id), ctl.
//
// # Commands
//
// sbatch registers a PENDING job with ids from 1001. squeue and sacct print
// Slurm data_parser JSON (--json only). scancel cancels PENDING and RUNNING
// jobs. fakeslurmctl drives job lifecycles: start, end, requeue, dump,
// count-sbatch.
//
// Fake jobs never progress on their own; tests move them with fakeslurmctl.
package fakeslurm
