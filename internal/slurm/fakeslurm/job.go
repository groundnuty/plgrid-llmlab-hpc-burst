package fakeslurm

// Ported from spikes/s4/fakeslurm/state.go. S4 kept every state transition with
// its time so that sacct could lag behind; this fake has no lag, so an
// execution keeps only its current state.

import (
	"fmt"
	"syscall"
	"time"
)

// Job states used by the fake.
const (
	Pending   = "PENDING"
	Running   = "RUNNING"
	Completed = "COMPLETED"
	Failed    = "FAILED"
	Cancelled = "CANCELLED"
	Timeout   = "TIMEOUT"
	NodeFail  = "NODE_FAIL"
	Requeued  = "REQUEUED"
)

const firstJobID = 1001

// Hold reasons, as squeue prints them in state_reason.
const (
	HeldUser  = "JobHeldUser"
	HeldAdmin = "JobHeldAdmin"
)

// IsTerminal reports whether s is a final state.
func IsTerminal(s string) bool {
	switch s {
	case Completed, Failed, Cancelled, Timeout, NodeFail:
		return true
	}
	return false
}

// execution is one attempt of a job. A requeue closes the current one as
// REQUEUED and opens a new one.
type execution struct {
	submit, start, end time.Time
	state              string
	reason             string
	// run is the execution's process in run mode; nil before it starts and
	// outside run mode.
	run *jobRun
}

// job is one submitted job, guarded by Platform.mu.
type job struct {
	id         int
	name, user string
	partition  string
	account    string
	timeLimit  int // minutes; 0 means none was given
	cpus       int
	nodes      int
	gpus       int
	memMiB     int64
	requeue    bool
	output     string
	errPath    string
	wrap       string
	constraint string
	restartCnt int
	execs      []*execution
	// held is true while the job waits on a hold; the reason (HeldUser or
	// HeldAdmin) is the latest execution's reason.
	held bool
	// estimate is a pending job's scheduler start estimate; nil for none.
	estimate *time.Time
	// acctHidden hides every accounting record of the job from sacct.
	acctHidden bool
	// env is the submitting login's environment, which sbatch exports to
	// the job by default (run mode).
	env map[string]string
}

func newExecution(at time.Time, reason string) *execution {
	return &execution{submit: at, reason: reason, state: Pending}
}

// cur is the latest execution.
func (j *job) cur() *execution { return j.execs[len(j.execs)-1] }

// state is the latest execution's state.
func (j *job) state() string { return j.cur().state }

// live reports whether the job is pending or running.
func (j *job) live() bool { return j.state() == Pending || j.state() == Running }

// purged reports whether slurmctld has forgotten the job: terminal for at
// least MinJobAge. The boundary is inclusive, as in S4.
func (j *job) purged(now time.Time, minAge time.Duration) bool {
	e := j.cur()
	if !IsTerminal(e.state) {
		return false
	}
	return !e.end.Add(minAge).After(now)
}

func (j *job) markStart(now time.Time) error {
	e := j.cur()
	if e.state != Pending {
		return fmt.Errorf("job %d is %s, not PENDING", j.id, e.state)
	}
	if j.held {
		return fmt.Errorf("job %d is held (%s); a held job never starts, release it first", j.id, e.reason)
	}
	e.start = now
	e.state = Running
	j.estimate = nil
	return nil
}

func (j *job) markEnd(now time.Time, state string) error {
	if !IsTerminal(state) {
		return fmt.Errorf("%q is not a terminal state (COMPLETED, FAILED, CANCELLED, TIMEOUT, NODE_FAIL)", state)
	}
	e := j.cur()
	if e.state != Pending && e.state != Running {
		return fmt.Errorf("job %d is already %s", j.id, e.state)
	}
	e.end = now
	e.state = state
	return nil
}

// markRequeue closes the current execution as REQUEUED and opens a new PENDING
// one, whatever the job's requeue flag says. The new execution is not held.
func (j *job) markRequeue(now time.Time) {
	e := j.cur()
	if e.end.IsZero() {
		e.end = now
	}
	e.state = Requeued
	j.restartCnt++
	j.held = false
	j.estimate = nil
	j.execs = append(j.execs, newExecution(now, "BeginTime"))
}

// hold puts the job on a hold with the given reason (HeldUser or HeldAdmin).
func (j *job) hold(reason string) {
	j.held = true
	j.cur().reason = reason
}

// release clears the hold; the pending reason goes back to "None".
func (j *job) release() {
	j.held = false
	j.cur().reason = "None"
}

// forgetRequeues models an accounting purge of the earlier records: only the
// latest execution stays and the restart count is 0.
func (j *job) forgetRequeues() {
	j.execs = []*execution{j.cur()}
	j.restartCnt = 0
}

// stateReason is squeue's state_reason: the pending reason while pending, the
// end reason of a failed job, "None" otherwise.
func (j *job) stateReason() string {
	e := j.cur()
	switch e.state {
	case Pending:
		if e.reason == "" {
			return "None"
		}
		return e.reason
	case Failed:
		return "NonZeroExitCode"
	case Timeout:
		return "TimeLimit"
	case NodeFail:
		return "NodeDown"
	}
	return "None"
}

// Job is a snapshot of one job, for tests.
type Job struct {
	ID                 int
	Name, User         string
	Partition          string
	Account            string
	TimeLimit          int // minutes
	CPUs, GPUs         int
	MemMiB             int64
	State              string
	Held               bool
	HoldReason         string // "" when not held
	RestartCount       int
	Executions         int
	Submit, Start, End time.Time
	Estimate           *time.Time
	AcctHidden         bool
	Output, Error      string
	// Wrap is the --wrap command, the batch script.
	Wrap string
	// Constraint is the --constraint value, "" when none was given. The fake
	// accepts any feature; it does not model node features, except that a
	// run-mode JobVar can depend on one.
	Constraint string

	// Run mode only, for the current execution; zero otherwise.
	//
	// PID is the batch shell's PID, which is also its process group and
	// session ID; 0 until the execution starts.
	PID int
	// Finished is true once the execution is terminal and none of its
	// processes is left: the batch shell was reaped and nothing is in its
	// session. An execution that ended without ever having a process
	// (cancelled while pending or held, ended by a control, one whose
	// start failed) is Finished as soon as it is terminal. It is never true
	// while a process of the execution may live: if the fake cannot list or
	// kill them (processes that survive KILL, an unreadable /proc), it stays
	// false and CleanupErr says why. A test that waits for the job's
	// processes to end waits for Finished.
	Finished bool
	// ExitCode is the batch shell's exit status once it has exited, -1 when
	// a signal ended it; ExitSignal is that signal. The job's state follows
	// at once (unless it had ended already), before its other processes are
	// cleared.
	ExitCode   int
	ExitSignal syscall.Signal
	// TermAt and KillAt are the wall-clock times the fake sent the job TERM
	// (scancel or the time limit) and, after the grace, KILL (or KILL at once
	// for a control or Close); zero when it did not. The KILL that clears
	// what is left after the batch shell exits is not counted.
	TermAt, KillAt time.Time
	// StartErr is why the execution could not start (its directory or an
	// output file could not be made); the job then ended FAILED.
	StartErr error
	// CleanupErr is why the execution's processes or directory could not be
	// cleared, or a signal could not be sent; nil when all went well.
	CleanupErr error
}

func (j *job) snapshot() Job {
	e := j.cur()
	s := Job{
		ID: j.id, Name: j.name, User: j.user, Partition: j.partition, Account: j.account,
		TimeLimit: j.timeLimit, CPUs: j.cpus, GPUs: j.gpus, MemMiB: j.memMiB,
		State: e.state, Held: j.held, RestartCount: j.restartCnt, Executions: len(j.execs),
		Submit: e.submit, Start: e.start, End: e.end, AcctHidden: j.acctHidden,
		Output: j.output, Error: j.errPath, Wrap: j.wrap, Constraint: j.constraint,
	}
	if j.held {
		s.HoldReason = e.reason
	}
	if r := e.run; r != nil {
		s.PID, s.Finished, s.TermAt, s.KillAt = r.pid, r.done, r.termAt, r.killAt
		s.StartErr, s.CleanupErr = r.startErr, r.cleanupErr
		if r.exitSeen {
			s.ExitCode, s.ExitSignal = r.exitCode, r.exitSignal
		}
	}
	if j.estimate != nil {
		t := *j.estimate
		s.Estimate = &t
	}
	return s
}
