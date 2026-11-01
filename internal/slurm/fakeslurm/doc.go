// Package fakeslurm is an in-process fake Slurm login node for tests. It
// implements slurm.Runner: a test hands a Session to slurm.NewBatch and the
// plugin's command lines run against a fake cluster held in memory.
//
// Provenance: new package, ported from the multi-process fake of spike S4
// (spikes/s4/fakeslurm). Each file says which S4 file it comes from, or "new".
// The JSON shapes follow S7's real Helios captures (spikes/s7/
// slurm-json-real.md) and, for v0.0.40, Slurm's source as S4 modelled it.
// Differences from S4: state lives in memory under one mutex (no state file,
// no flock, no binary); a manual clock replaces the real one; and the fake adds
// per-user views, a call log, faults, scripted sbatch outcomes, gates, an
// in-memory file system, both "not listed" shapes and freed-slot phantoms.
//
// # Model
//
// A Platform is one Slurm cluster. Login returns a Session, the view of one
// user over one connection to the login node; a Session is a slurm.Runner.
// Jobs, files and the call log belong to the Platform, so two Sessions share
// one cluster and one file system, but squeue --me and the default sacct show
// only the calling user's jobs, and each Session has its own environment
// (HOME, SCRATCH, ...). Outside run mode jobs never progress on their own: a
// test moves them with Start, End, Requeue and the other job controls. The
// only thing the clock does by itself is the purge of an ended job after
// MinJobAge.
//
// # Command grammar
//
// The fake reads only what internal/slurm/dialect.go builds, and refuses
// everything else loudly so that a change in the dialect fails a test:
//
//   - a line is simple commands joined by " && ". Each runs only when the one
//     before exited 0; the line's exit code is the last one run;
//   - words are separated by single spaces. A word is plain (letters, digits
//     and ._:=,/@%+-) or wrapped in single quotes, which are stripped. A quoted
//     word may hold any character but a single quote; the --wrap word holds
//     " && ", "|" and "$(" and is one word. Any other character outside quotes
//     (a "$", "~", ";", "|", a double quote, a newline...) is a syntax error,
//     exit 2, nothing runs;
//   - "cat > <file>" is the only redirection. It writes standard input to the
//     file with mode 0666 &^ umask and needs the parent directory to exist;
//   - commands: sbatch, squeue, sacct, scancel, scontrol release, printenv,
//     sha256sum, umask, mkdir -p, chmod, mv -f, rm -f, rm -rf and test -f.
//     Anything else is exit 127, "command not found". Options the plugin never
//     sends are refused (exit 1), so an unexpected flag cannot pass silently.
//
// # Call log
//
// One rule: a simple command is logged when it actually begins to run on the
// login node, after any gate has released it. A command that a fault refused
// (FailNext), that failed to parse, or whose context ended while it waited at a
// gate is not in Calls; refused ones are in Faulted. Count and CountFor count
// calls by their first word. Standard input is never stored: a Call keeps only
// its length. Tests count sbatch from this log, never from return values.
//
// # Concurrency
//
// Run is safe for concurrent use. One mutex guards all state; gate waits and
// context waits happen outside it. MaxConcurrent reports the most Run calls in
// progress at once, counted from entry to exit, including time blocked at a
// gate.
//
// # Modelling choices the brief left open
//
//   - Only a running job's end hands its slot to a phantom job. Requeueing a
//     running job ends that execution, so it frees its slot to a phantom too.
//   - scancel and scontrol release on another user's job give "Access/
//     permission denied", exit 1.
//   - A scripted sbatch step is consumed by every sbatch that gets past option
//     parsing, including one that a partition policy then refuses; the policy
//     check comes first.
//   - squeue's text table has the columns JOBID PARTITION NAME USER ST TIME
//     NODES NODELIST(REASON) SUBMIT_TIME START_TIME, the last two as local
//     time in Config.TimeZone ("N/A" when unset). Real squeue has no time
//     columns by default; these exist so that tests can show a local-time
//     reading that no plugin code may use.
//   - sacct state.reason is "None" for a pending job, even a held one, as in
//     S4; the hold shows in hold and priority, which both squeue and sacct
//     carry.
//   - Command.Timeout is ignored. A test that needs a timed-out command uses
//     FailNext with slurm.ErrTimeout, or LostReply for one that registered the
//     job first.
//
// # Run mode
//
// With Config.Run set, the fake runs each job (issue #146). The command
// grammar, the JSON replies and the job controls are those above; what
// changes:
//
//   - The file system is the real directory RunMode.Root, at the same paths,
//     as a cluster's compute nodes share the login node's file system. The
//     logins' HOME and SCRATCH must be under it. Every operation of the
//     fake's own (the commands, the job's output files, Files and Dirs) goes
//     through an os.Root opened on the directory. A path outside it reads as
//     missing, a write, mkdir, chmod, mv or rm there is refused (exit 1), and
//     so is a path that is not cleaned ("root/../x") and one that goes
//     through a symbolic link that resolves outside the root, which a job's
//     processes can make. A link that stays inside works if it is relative
//     (as a Hugging Face cache's are); os.Root refuses an absolute one even
//     when its target is inside. So no command of the fake touches the host
//     outside the test's directory. That is the fake's own reach: the jobs'
//     processes run as the test does and are not confined.
//   - Reads are bounded, because the root can hold what jobs wrote and the
//     VM has no swap. sha256sum streams, for at most 256 MiB, and exits 1
//     for a bigger file. Files reads every regular file under the root, with
//     no Owner, but a file over 16 MiB, files over 64 MiB in all or more than
//     50,000 files are a test bug: Files panics, naming the file, rather than
//     return a partial map (its signature is the in-memory one's). It holds
//     no lock while it reads. Do not call it on a root that holds staged
//     models or a cache; stat the file instead. A FIFO, a device or a
//     directory as --output or --error fails the job, and no open of the fake
//     waits for a FIFO's other end.
//   - A job starts as soon as it is eligible (not held, a free slot), within
//     the command that made it so: sbatch, scontrol release, FreePhantoms, or
//     the end of another job's execution. Start is refused. Starting at once
//     is a modelling choice: real slurmctld schedules a little later.
//   - The batch script is "#!/bin/sh" and the --wrap command, as sbatch writes
//     it, run by /bin/sh in a session of its own, so it leads its own process
//     group. Standard input is /dev/null; standard output and error go to
//     --output and --error (default slurm-<id>.out in the submit directory),
//     truncated, or appended to after a requeue. The job starts in the login's
//     HOME, the submit directory of a non-interactive ssh command.
//   - Its environment is PATH (/usr/local/bin:/usr/bin:/bin unless set), the
//     login's environment (sbatch exports it by default), RunMode.Env, the
//     Slurm variables (SLURM_JOB_ID, SLURM_JOBID, SLURM_JOB_NAME,
//     SLURM_JOB_USER, SLURM_JOB_PARTITION, SLURM_JOB_ACCOUNT,
//     SLURM_SUBMIT_DIR, SLURM_CPUS_PER_TASK, SLURM_MEM_PER_NODE,
//     SLURM_JOB_NUM_NODES, SLURM_NNODES, and SLURM_RESTART_COUNT after a
//     requeue), TMPDIR, and the job variables (RunMode.JobVars): each a fresh
//     directory of the execution under RunMode.JobDir, a featured one only
//     for a job whose --constraint names the feature. The execution's
//     directory goes when its processes are gone.
//   - The job is RUNNING while its batch shell runs. The moment the shell
//     exits the job is COMPLETED for exit 0 and FAILED otherwise, a signal
//     included, unless something ended it first; a scancel that comes later
//     finds a finished job. Then whatever is left of the job is killed, as
//     Slurm's cgroup does: the session's processes and their descendants, so
//     a child in a process group of its own goes too. A child that calls
//     setsid and whose parent has died escapes this; a cgroup would catch it.
//   - scancel ends the job CANCELLED at once, as slurmctld does, then sends
//     CONT and TERM to its processes and, after RunMode.KillWait, KILL to
//     what is left. The time limit (RunMode.Minute per minute of --time, from
//     the start) ends it TIMEOUT the same way; SetTimeLimit moves the limit,
//     counted from the start, and one already passed ends the job TIMEOUT
//     before it returns. End and Requeue send KILL at once, as a failed node
//     does; a requeued job's new execution starts once the old one's
//     processes are gone.
//   - The fake signals only processes it started. The batch shell stays a
//     zombie until the last scan of its session, because a zombie keeps its
//     PID, the session ID the scans select by; no scan of /proc runs after
//     the shell is reaped. A member is signalled through a pidfd, opened
//     before its start time is checked again, so a recycled PID is left
//     alone. What is left is a window inside one listing of /proc: a
//     descendant is a process whose parent's PID is a member's, and a stranger
//     would be signalled only if a member exited, its PID went to an
//     unrelated process and that process forked, all within the few
//     milliseconds of one listing; that takes a wrap of the PID space and
//     cannot be tested. A /proc that cannot be read is an error, never "none".
//   - Job carries the execution's PID, Finished, its exit code or signal, when
//     TERM and KILL were sent, and CleanupErr. Finished is true once the
//     execution is terminal and none of its processes is left (the shell is
//     reaped and nothing is in its session), at once for an execution that
//     never had a process (cancelled while held, ended by a control), and
//     never while a process may live: if the processes survive KILL for 30 s
//     or /proc cannot be read, Finished stays false and CleanupErr says why.
//     Processes lists the live ones, and returns an error when it cannot tell.
//     Tests wait on these, never on a sleep. Finished is not "every
//     descendant reaped": an orphan that init has not reaped yet is a zombie
//     in the job's process group, which the kernel still counts as a member.
//   - Close sends KILL to every job's processes and waits for them, and no
//     job starts after it; a job still RUNNING then ends FAILED, killed by a
//     signal, like any job whose shell is killed. It returns an error for a
//     job whose processes survived or could not be listed. A Platform in run
//     mode must be closed. Timers and the grace are wall time; job states and
//     times use the Platform's clock, so a full-stack run passes a real clock.
//   - The fake has no Pdeathsig: in Go it fires when the forking thread
//     exits, not the process. A process that ends without Close leaves its
//     jobs running, as a Slurm job outlives the login session.
package fakeslurm
