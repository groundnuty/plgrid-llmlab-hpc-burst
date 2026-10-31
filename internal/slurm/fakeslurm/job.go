package fakeslurm

// Ported from spikes/s4/fakeslurm/state.go. S4 kept every state transition with
// its time so that sacct could lag behind; this fake has no lag, so an
// execution keeps only its current state.

import (
	"fmt"
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
	restartCnt int
	execs      []*execution
	// held is true while the job waits on a hold; the reason (HeldUser or
	// HeldAdmin) is the latest execution's reason.
	held bool
	// estimate is a pending job's scheduler start estimate; nil for none.
	estimate *time.Time
	// acctHidden hides every accounting record of the job from sacct.
	acctHidden bool
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
}

func (j *job) snapshot() Job {
	e := j.cur()
	s := Job{
		ID: j.id, Name: j.name, User: j.user, Partition: j.partition, Account: j.account,
		TimeLimit: j.timeLimit, CPUs: j.cpus, GPUs: j.gpus, MemMiB: j.memMiB,
		State: e.state, Held: j.held, RestartCount: j.restartCnt, Executions: len(j.execs),
		Submit: e.submit, Start: e.start, End: e.end, AcctHidden: j.acctHidden,
		Output: j.output, Error: j.errPath, Wrap: j.wrap,
	}
	if j.held {
		s.HoldReason = e.reason
	}
	if j.estimate != nil {
		t := *j.estimate
		s.Estimate = &t
	}
	return s
}
