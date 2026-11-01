package fakeslurm

// New. B5 and the time-limit gaps of the PR 153 review: a stale limit timer
// must not end a job whose limit was raised, SetTimeLimit counts from the
// job's start, and a limit already passed ends the job before the call
// returns. Time is waited for as a lower bound only (a job has run at least
// so long), never as a window the test hopes to land in.

import (
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// The timer of the original limit fires and its callback is held before it
// takes the lock. SetTimeLimit raises the limit meanwhile, which cannot stop a
// callback that has started. When the callback runs it must find the limit
// raised and leave the job RUNNING; a later, lower limit still ends it.
func TestRunModeRaisedLimitSurvivesAStaleTimer(t *testing.T) {
	const minute = 20 * time.Millisecond
	r := newRunRig(t, RunMode{Minute: minute, KillWait: time.Hour})
	fired, proceed := make(chan struct{}), make(chan struct{})
	decided := make(chan bool, 4)
	var first atomic.Bool
	r.p.knobs.limitFired = func(*jobRun) {
		if first.CompareAndSwap(false, true) {
			close(fired)
			<-proceed
		}
	}
	r.p.knobs.limitDecided = func(_ *jobRun, ended bool) { decided <- ended }
	id := r.submit(waitLoop(r.home+"/never"), "--time=1") // one minute: 20 ms
	waitFor(t, "the limit timer's callback to start", func() bool {
		select {
		case <-fired:
			return true
		default:
			return false
		}
	})
	// The callback has started and waits; a limit of 100,000 scaled minutes
	// (over half an hour) is far enough away that no load can reach it.
	if err := r.p.SetTimeLimit(id, 100000); err != nil {
		t.Fatal(err)
	}
	close(proceed)
	var ended bool
	waitFor(t, "the stale callback to decide", func() bool {
		select {
		case ended = <-decided:
			return true
		default:
			return false
		}
	})
	if j := r.job(id); ended || j.State != Running || !j.TermAt.IsZero() {
		t.Errorf("the stale callback ended the job: ended %v, state %s, TermAt %v; want RUNNING with no TERM after the limit was raised", ended, j.State, j.TermAt)
	}
	// The job has run for more than the first limit; a lower limit than its
	// time so far ends it before SetTimeLimit returns.
	if err := r.p.SetTimeLimit(id, 1); err != nil {
		t.Fatal(err)
	}
	if j := r.job(id); j.State != Timeout {
		t.Errorf("after the limit was lowered below the time run: state %s, want TIMEOUT before SetTimeLimit returned", j.State)
	}
	if j := r.waitFinished(id); j.TermAt.IsZero() || j.ExitSignal != syscall.SIGTERM {
		t.Errorf("TermAt %v, signal %v; want the job sent TERM and ended by it", j.TermAt, j.ExitSignal)
	}
}

// A limit is counted from the job's start, not from the call that sets it: a
// job that has run for three scaled minutes and gets a limit of one ends
// TIMEOUT before SetTimeLimit returns. The wait is for the three minutes to
// have passed, a lower bound; nothing is asserted about how soon after.
func TestRunModeSetTimeLimitCountsFromTheStart(t *testing.T) {
	const minute = 100 * time.Millisecond
	r := newRunRig(t, RunMode{Minute: minute, KillWait: time.Hour})
	up := r.home + "/up"
	id := r.submit("echo up > "+up+"; "+waitLoop(r.home+"/never"), "--time=6000") // 10 minutes of 100 ms
	r.waitFile(up)
	end := r.squeueJob(id)["end_time"].(map[string]any)
	if want := float64(t0.Add(600 * time.Second).Unix()); end["set"] != true || end["number"] != want {
		t.Errorf("projected end_time %v, want set at %v (start plus 6000 minutes of 100 ms)", end, want)
	}
	seen := time.Now() // the job started before this
	waitFor(t, "three scaled minutes of the job's time to pass", func() bool { return time.Since(seen) >= 3*minute })
	if j := r.job(id); j.State != Running {
		t.Fatalf("state %s before the limit is lowered, want RUNNING", j.State)
	}
	if err := r.p.SetTimeLimit(id, 1); err != nil {
		t.Fatal(err)
	}
	if j := r.job(id); j.State != Timeout {
		t.Errorf("right after SetTimeLimit(1) on a job that ran for 3 minutes: state %s, want TIMEOUT", j.State)
	}
	if j := r.waitFinished(id); j.TermAt.IsZero() {
		t.Errorf("the job ended TIMEOUT without a TERM")
	}
}
