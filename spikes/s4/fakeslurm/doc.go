// Package fakeslurm implements a fake Slurm command line for tests.
//
// One multi-call binary behaves as sbatch, squeue, sacct, scancel, scontrol
// and the control command fakeslurmctl, chosen by the base name of argv[0]. All state
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
//	FAKE_SLURM_SQUEUE_ORDER  asc (default) or desc: squeue lists jobs by ascending
//	                         or descending job ID
//	TZ                       zone for sacct -S/-E and the default -S (UTC if unset)
//
// sbatch hooks (the sleeps and the block run outside the lock):
//
//	FAKE_SLURM_SBATCH_DELAY_BEFORE  seconds to sleep before registering the job
//	FAKE_SLURM_SBATCH_DELAY_AFTER   seconds to sleep after registering, before printing
//	FAKE_SLURM_SBATCH_BLOCK_MARKER  path: after registering, write
//	                                {"pid":<pid>,"job_id":<id>} there (atomically),
//	                                then block until killed; nothing is printed
//	FAKE_SLURM_SBATCH_FAIL          1: print a definite submission failure, exit 1,
//	                                register nothing. The reason is "Invalid account or
//	                                account/partition combination specified", which a
//	                                retry cannot cure
//	FAKE_SLURM_SBATCH_FAIL_ONCE     marker path: the first call that finds no marker
//	                                creates it (O_EXCL) and fails exactly like
//	                                FAKE_SLURM_SBATCH_FAIL; later calls go on. This check
//	                                comes right after sbatch_start, before the gate
//	FAKE_SLURM_SBATCH_FAIL_ALIGN    1: the call that fails once first sleeps until 30 ms
//	                                past the next whole second (UTC), so the failure
//	                                lands early in a second
//	FAKE_SLURM_SBATCH_GATE          path: after sbatch_start (and the fail-once check)
//	                                log sbatch_gate_wait, then poll every 50 ms, outside
//	                                the lock, until the file exists; only then the
//	                                delay and the registration. A process killed while
//	                                it waits has registered nothing
//	FAKE_SLURM_SBATCH_LOST_REPLY_ONCE  marker path: the first call that finds no marker
//	                                at reply time creates it. The job is registered as
//	                                usual (hold honoured, sbatch_submitted logged), then
//	                                the call prints "Socket timed out on send/recv
//	                                operation" to stderr, nothing to stdout, and exits
//	                                1: the reply was lost after slurmctld accepted the
//	                                job. With BLOCK_MARKER set the block comes first
//	                                and the reply is never printed
//
// Order of an sbatch call: sbatch_start, fail-once (and its align sleep), gate,
// delay_before, FAIL, registration, block marker, delay_after, block, lost reply.
//
// # Log
//
// Each event is one JSON line: ts (RFC3339Nano UTC), unix_ms, event, cmd,
// argv, caller, pid, job_id (when known), exit (null for sbatch_start and
// sbatch_gate_wait) and an optional note.
// Events: sbatch_start (on invocation, before any delay), sbatch_gate_wait,
// sbatch_submitted (note "held" for sbatch --hold), sbatch_failed, squeue,
// sacct, scancel (one per id), scontrol_release (one per id; note "already
// released" when the job was not held), scontrol (any other subcommand), ctl.
//
// # Commands
//
// sbatch registers a PENDING job with ids from 1001; --hold or -H registers
// it on a user hold. squeue and sacct print Slurm data_parser JSON (--json
// only). squeue reports hold, priority and state_reason: a held job has hold
// true, priority {set:true,infinite:false,number:0} and state_reason
// JobHeldUser or JobHeldAdmin; any other job has hold false and priority 1000.
// A requeued job is plain PENDING in squeue (restart_cnt tells), and sacct -D
// shows its earlier executions as PENDING plus REQUEUED. sacct lists the
// invoking user's jobs (FAKE_SLURM_USER) unless -a/--allusers or -u/--user is
// given; -n is --noheader (no effect on JSON) and --name has no short form.
// scancel cancels PENDING and RUNNING jobs. scontrol supports only
// "scontrol release <id>[,<id>...]": a user hold is cleared, an admin hold is
// refused (Access/permission denied), a job that is not held is left alone,
// an ended job gives "Job has already finished" and an unknown or purged one
// "Invalid job id specified" (exit 1 for each failed id).
//
// fakeslurmctl drives job lifecycles:
//
//	start <id>            PENDING to RUNNING; refused (exit 1) for a held job
//	end <id> [STATE]      close the job (default COMPLETED)
//	requeue <id>          close the execution as REQUEUED, open a PENDING one
//	requeue-hold <id>     requeue, then hold the new execution (JobHeldUser)
//	hold-admin <id>       put a PENDING job on an admin hold (JobHeldAdmin)
//	acct-hide <id>        sacct omits every record of the job (slurmdbd does not
//	                      have it yet); squeue is unaffected
//	acct-show <id>        undo acct-hide
//	forget-requeue <id>   model an accounting purge: drop every execution but the
//	                      latest and set the restart count to 0
//	dump                  print the state file
//	count-sbatch [name]   number of sbatch_submitted events (optionally by name)
//
// Fake jobs never progress on their own; tests move them with fakeslurmctl.
package fakeslurm
