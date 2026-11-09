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
// MinJobAge (in run mode, not of one that is still completing).
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
//   - commands: sbatch, srun, squeue, sacct, scancel, scontrol release, printenv,
//     sha256sum, umask, mkdir -p, chmod, mv -f and mv -fT, rm -f, rm -rf and test -f.
//     Anything else is exit 127, "command not found". Options the plugin never
//     sends are refused (exit 1), so an unexpected flag cannot pass silently;
//   - srun is "srun --overlap --jobid=<digits> [--ntasks=1] [--time=<t>]
//     <program> [args...]", the options in any order, each in the one form
//     shown (--jobid=<id>, never "--jobid <id>"). The program and its words
//     follow the first word that is not an option and are not read, so
//     "bash -s" is the program's own. --overlap and --jobid are required,
//     --ntasks takes only 1, and any other option is refused with exit 1. The
//     program's standard input is the Command's Stdin (a script for "bash
//     -s"), and the call log keeps its length. See "srun" under Run mode: it
//     runs a host process, so it needs run mode.
//
// Job IDs count up from 1001, or from Config.FirstJobID when it is set. Fakes
// of different tests that run at once on one machine set it apart, so that a
// job found by its ID (SLURM_JOB_ID, in run mode) is never another Platform's.
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
// # QOS
//
// Config.QOS lists the cluster's QOSes with two per-user limits (qos.go). A
// job runs under the QOS its --qos names, else Config.DefaultQOS, and counts
// against that QOS and its partition's own QOS (Partition.QOS). For each limit
// the first QOS in the order partition QOS, job QOS that sets it applies (no
// OverPartQOS). A limit is a pointer: nil is not set, and Cap(0) is set to 0
// and allows nothing, so a partition QOS with a GPU cap of 0 is not skipped
// for the job QOS's cap, and a submit limit of 0 refuses every submission. The submit limit (MaxSubmitPerUser) counts the user's pending
// and running jobs of any name, a held job included: a submission over it is
// refused with the two lines Slurm prints, the reason line
// "sbatch: error: QOSMaxSubmitJobPerUserLimit" first, then the accounting-policy
// line, exit 1, nothing registered. A one-job-per-user Partition refuses the
// same way. The GPU cap (MaxGPUsPerUser) lets a job that fits alone be accepted
// and pend: Start refuses it while the user's running jobs under the QOS hold
// the GPUs (canStart). The pending reason QOSMaxGRESPerUser is not shown in
// squeue. RejectPolicy scripts any other reason name with both lines, Reject
// prints the one line of a refusal with no reason line, LostReplyLate loses a
// reply and registers its job, held, when the next sbatch arrives, and
// OutsideJob adds a job of the user that is no call, as the owner's other
// projects would submit through the same login.
//
// # Modelling choices the brief left open
//
//   - Only a running job's end hands its slot to a phantom job, once its node
//     is free: at once, or, for a job that is completing, when its flag clears.
//     Requeueing a running job ends that execution, so it frees its slot to a
//     phantom too.
//   - scancel and scontrol release on another user's job give "Access/
//     permission denied", exit 1.
//   - scancel of an ended or unknown job prints nothing and exits 0, as real
//     Slurm 24.11's does (Config.NoisyScancel brings back the error text).
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
//   - Command.Timeout is ignored, except by srun (which runs a process; see
//     Run mode). A test that needs a timed-out command uses FailNext with
//     slurm.ErrTimeout, or LostReply for one that registered the job first.
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
//   - A job whose end state is set while its processes still live is
//     completing, as in Slurm: squeue lists ["<end state>", "COMPLETING"] (ST
//     CG) until Finished, in either NotListed mode, by --me and by
//     --jobs=<id>, and it is not purged, however long ago it ended. squeue
//     reports pending, running and completing jobs by default, and the purge
//     (_list_find_job_old, Slurm 24.11, src/slurmctld/job_mgr.c) returns for a
//     completing job before it looks at its age. scancel prints nothing and
//     exits 0, as for any ended job (since #207; Config.NoisyScancel prints
//     "Job/step already completing or completed" and exits 1), and sends
//     nothing more to the processes (job_signal returns ESLURM_ALREADY_DONE
//     when IS_JOB_FINISHED, which ignores the COMPLETING flag); scontrol release
//     gives "Job has already finished" (update_job, ESLURM_JOB_FINISHED).
//     MinJobAge counts from the end (the job's end_time), not from when the
//     processes were gone, as in Slurm: clearing COMPLETING leaves end_time
//     alone. A job whose processes were gone before end + MinJobAge then
//     follows the NotListed mode (Helios: an empty list, then the error from
//     end + MinJobAge; Athena: listed in its end state until then), and one
//     whose processes outlive it is purged the moment they are gone. Slurm
//     purges in periodic passes, the fake on the next query. A job whose
//     processes cannot be cleared (Finished stays false) completes for ever
//     and is never purged.
//   - The flag outlasts the processes by RunMode.Epilog (default none), on the
//     platform's clock, counted from the moment they were gone: Slurm's epilog
//     and the nodes' reports run after the job's processes end, and
//     COMPLETING stays until they are done. Job.Completing says so; Finished
//     does not (it is true once the processes are gone). The job is listed
//     with the flag in both NotListed modes, and not purged, until the epilog
//     ends; after that the mode's rule applies, so the Athena shape (listed in
//     its end state until MinJobAge, counted from the end) shows only for an
//     epilog that ends before end + MinJobAge, and the Helios shape drops the
//     job at once. Slurm sets the flag when it sets the end state, and frees
//     the job's submit slot and its GPUs then: a completing job is neither
//     pending nor running, so the submit limit and the GPU cap do not count
//     it, and a pending job that was waiting on them starts while it
//     completes. A partition's slots (Partition.Slots) still count it, because
//     Slurm frees a node only once its epilog has completed and takes no
//     COMPLETING node for a new job (make_node_comp and make_node_idle in
//     src/slurmctld/node_mgr.c, Slurm 24.11.5, lines 4599-4646 and 4752-4845;
//     _pick_best_nodes in src/slurmctld/node_scheduler.c, lines 1832-1846):
//     the slot is the completing job's until its flag clears, and then goes to
//     a phantom job, as any ended job's slot does.
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
//   - srun --overlap --jobid=<id> runs a step in a job of the calling Session's
//     user that is RUNNING with a live process: the program, looked up on the
//     PATH of the job's environment (not the test's), runs as a host process
//     with the job's own environment (the one its batch shell got, SLURM_JOB_ID
//     among it) and the submit directory as its working directory. Standard
//     input is the Command's, and its standard output and error come back as
//     the Result's, each capped at 16 MiB (a step that writes more is killed,
//     exit 1). The exit status is the program's, or 128 plus the signal that
//     ended it; a program that is not found is exit 127 and one that is not
//     executable 126, as a shell says. Nothing runs in a refusal, exit 1 and
//     "srun: error: Unable to confirm allocation for job <id>: <reason>", the
//     reason in the words of Slurm's error table: "Invalid job id specified"
//     (unknown or purged), "Access/permission denied" (another user's job; an
//     unknown job is checked first, as in scancel), "Job/step already
//     completing or completed" (ended, completing included), "Job is pending
//     execution" (pending or held), and "job is not running" for a RUNNING
//     job with no process to join. Outside run mode srun is refused, exit 1.
//     The step is a process in a session of its own, as a job is, so it is
//     not among the job's Processes, and it changes nothing of the job: not
//     its state, not its accounting. What the program does to the job's
//     processes (a kill by PID) is its own doing. mu is not held while the
//     step runs, so every other command is answered meanwhile. Command.Timeout
//     (zero is provider.CommandTimeout, 30 s, as in the transport) and the
//     Run call's context bound it: the step and everything left in its
//     session, its children included, get KILL, and Run returns ErrTimeout
//     for the timeout and the error of the context for a context that ended.
//     --time bounds it too, in RunMode.Minute (exit 1, "STEP CANCELLED DUE TO
//     TIME LIMIT"); the job's batch shell exiting ends it (exit 1, "Job step
//     aborted"), as a Slurm step does not outlive its job. When the program
//     exits, whatever it left in its session is killed, and the process is
//     reaped before Run returns, so no process of a step is left, with the
//     limit of a job's: a process that calls setsid and whose parent has died
//     escapes, and one that holds the step's output open is waited for two
//     seconds, then left. A sweep that cannot tell that the processes are gone
//     makes Run return an error that wraps slurm.ErrTransport.
//   - A host that dies without Close (SIGKILL, SIGTERM, a -test.timeout panic,
//     an out-of-memory kill) takes each job's batch shell and each step's
//     program with it: they are started with a parent-death signal of KILL,
//     from one goroutine that keeps its thread for good, because in Go the
//     signal fires when the forking thread exits, not the process (issue
//     #274; before it, such a shell looped for days). What the shell started
//     is not reached: a descendant it left running outlives a host that died
//     without Close, and only Close's sweep ends it. Real Slurm would keep the
//     job, which outlives the login session; the fake does not, since its
//     host is a test.
package fakeslurm
