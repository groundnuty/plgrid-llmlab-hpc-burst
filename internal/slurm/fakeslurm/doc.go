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
// (HOME, SCRATCH, ...). Jobs never progress on their own: a test moves them
// with Start, End, Requeue and the other job controls. The only thing the clock
// does by itself is the purge of an ended job after MinJobAge.
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
// # QOS
//
// Config.QOS lists the cluster's QOSes with two per-user limits (qos.go). A
// job runs under the QOS its --qos names, else Config.DefaultQOS, and counts
// against that QOS and its partition's own QOS (Partition.QOS). For each limit
// the first QOS in the order partition QOS, job QOS that sets it applies (no
// OverPartQOS). The submit limit (MaxSubmitPerUser) counts the user's pending
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
package fakeslurm
