package fakeslurm

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

// Transition is one state change of an execution.
type Transition struct {
	At    time.Time `json:"at"`
	State string    `json:"state"`
}

// Exec is one execution (attempt) of a job. A requeue closes the current one
// as REQUEUED and opens a new one.
type Exec struct {
	Submit      time.Time    `json:"submit"`
	Start       time.Time    `json:"start,omitzero"`
	End         time.Time    `json:"end,omitzero"`
	Reason      string       `json:"reason"`
	Transitions []Transition `json:"transitions"`
}

// Job is one submitted job.
type Job struct {
	ID         int      `json:"id"`
	Name       string   `json:"name"`
	Comment    string   `json:"comment"`
	User       string   `json:"user"`
	Partition  string   `json:"partition"`
	Account    string   `json:"account"`
	TimeLimit  int      `json:"time_limit_minutes"`
	Requeue    bool     `json:"requeue"`
	RestartCnt int      `json:"restart_cnt"`
	Argv       []string `json:"argv"`
	Script     string   `json:"script,omitempty"`
	Execs      []*Exec  `json:"execs"`
	// Held is true while the job waits on a hold; the reason (HeldUser or
	// HeldAdmin) is the latest execution's Reason.
	Held bool `json:"held"`
	// AcctHidden hides every accounting record of the job from sacct.
	AcctHidden bool `json:"acct_hidden,omitempty"`
}

// State is the whole fake cluster.
type State struct {
	NextID int    `json:"next_id"`
	Jobs   []*Job `json:"jobs"`
}

func newState() *State { return &State{NextID: firstJobID, Jobs: []*Job{}} }

// Find returns the job with the id, or nil.
func (s *State) Find(id int) *Job {
	for _, j := range s.Jobs {
		if j.ID == id {
			return j
		}
	}
	return nil
}

// Cur is the latest execution.
func (j *Job) Cur() *Exec { return j.Execs[len(j.Execs)-1] }

// StateAt is the state the execution had at t; "" before it was submitted.
func (e *Exec) StateAt(t time.Time) string {
	st := ""
	for _, tr := range e.Transitions {
		if !tr.At.After(t) {
			st = tr.State
		}
	}
	return st
}

// StartAt is the start time as known at t (zero when not started yet).
func (e *Exec) StartAt(t time.Time) time.Time {
	if !e.Start.IsZero() && !e.Start.After(t) {
		return e.Start
	}
	return time.Time{}
}

// EndAt is the end time as known at t (zero when still open).
func (e *Exec) EndAt(t time.Time) time.Time {
	if !e.End.IsZero() && !e.End.After(t) {
		return e.End
	}
	return time.Time{}
}

func (e *Exec) setState(at time.Time, s string) {
	e.Transitions = append(e.Transitions, Transition{At: at, State: s})
}

func newExec(at time.Time, reason string) *Exec {
	e := &Exec{Submit: at, Reason: reason}
	e.setState(at, Pending)
	return e
}

// current returns the state of the latest execution as of the live clock now.
func (j *Job) current(now time.Time) string { return j.Cur().StateAt(now) }

// purged reports whether slurmctld has forgotten the job: terminal for at
// least MinJobAge.
func (j *Job) purged(now time.Time, minAge time.Duration) bool {
	e := j.Cur()
	if !IsTerminal(e.StateAt(now)) {
		return false
	}
	return !e.End.Add(minAge).After(now)
}

func (j *Job) markStart(now time.Time) error {
	e := j.Cur()
	if e.StateAt(now) != Pending {
		return fmt.Errorf("job %d is %s, not PENDING", j.ID, e.StateAt(now))
	}
	if j.Held {
		return fmt.Errorf("job %d is held (%s); a held job never starts, release it first", j.ID, e.Reason)
	}
	e.Start = now
	e.setState(now, Running)
	return nil
}

func (j *Job) markEnd(now time.Time, state string) error {
	if !IsTerminal(state) {
		return fmt.Errorf("%q is not a terminal state (COMPLETED, FAILED, CANCELLED, TIMEOUT, NODE_FAIL)", state)
	}
	e := j.Cur()
	if cur := e.StateAt(now); cur != Pending && cur != Running {
		return fmt.Errorf("job %d is already %s", j.ID, cur)
	}
	e.End = now
	e.setState(now, state)
	return nil
}

// markRequeue closes the current execution as REQUEUED and opens a new
// PENDING one, whatever the job's requeue flag says. The new execution is not
// held.
func (j *Job) markRequeue(now time.Time) {
	e := j.Cur()
	if e.End.IsZero() {
		e.End = now
	}
	e.setState(now, Requeued)
	j.RestartCnt++
	j.Held = false
	j.Execs = append(j.Execs, newExec(now, "BeginTime"))
}

// hold puts the job on a hold with the given reason (HeldUser or HeldAdmin).
func (j *Job) hold(reason string) {
	j.Held = true
	j.Cur().Reason = reason
}

// release clears the hold; the pending reason goes back to "None".
func (j *Job) release() {
	j.Held = false
	j.Cur().Reason = "None"
}

// forgetRequeues models an accounting purge of the earlier records: only the
// latest execution stays and the restart count is 0.
func (j *Job) forgetRequeues() {
	j.Execs = []*Exec{j.Cur()}
	j.RestartCnt = 0
}
