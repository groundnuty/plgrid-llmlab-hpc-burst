package alpha

// The two-job guard's plumbing in the live steps (review of PR 255, B6): any
// failure of the guard, a failed write of the end included, fails the step's
// next wait at once, so that Close runs; and a run of two jobs must end with
// the successor's drain start seen as the deployment's end in the API. The
// looks and the writes are the test's own (liveRun.lookFn, setEndFn), the
// guard's goroutine and the steps' waits are the live code. Every wait here is
// on observable state (a channel, a counter) with a generous deadline.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// guardAt is the guards' fixed start: a whole second, in UTC.
var guardAt = time.Date(2026, 11, 2, 9, 0, 0, 0, time.UTC)

// guardJob is a job of the run that started at start, with the rows' 1 h 46 min time limit.
func guardJob(id string, start time.Time) provider.Job {
	he := start.Add(106 * time.Minute)
	return provider.Job{ID: id, Name: "hb-alpha-0a1b2c3d4e-" + id, State: []string{"RUNNING"}, StartTime: &start, HardEnd: &he}
}

// guardedRun is a liveRun with the guard and nothing else: the looks come from looks (called with the look's
// number, from 1), the writes go to write, and a fatal is recorded, not raised.
type guardedRun struct {
	*liveRun
	mu     sync.Mutex
	fatal  string
	writes atomic.Int32
}

func newGuardedRun(t *testing.T, looks func(n int) (Look, error), write func(time.Time) (time.Time, error)) *guardedRun {
	g := &guardedRun{}
	var n atomic.Int32
	g.liveRun = &liveRun{t: t, guardFail: make(chan struct{}), guardEvery: 5 * time.Millisecond,
		guard:  &TwoJobGuard{TimeLimit: 106 * time.Minute, DrainWindow: 15 * time.Minute},
		lookFn: func(context.Context) (Look, error) { return looks(int(n.Add(1))) },
		setEndFn: func(e time.Time) (time.Time, error) {
			g.writes.Add(1)
			return write(e)
		},
		fatalf: func(format string, args ...any) {
			g.mu.Lock()
			if g.fatal == "" {
				g.fatal = fmt.Sprintf(format, args...)
			}
			g.mu.Unlock()
		},
	}
	t.Cleanup(g.stopGuard)
	return g
}

// waitFatal runs the steps' wait, whose condition never holds, until the guard's failure fails it: the fatal
// is recorded by the test's fatalf, which does not stop the wait, so the wait's own condition ends it then.
func (g *guardedRun) waitFatal(t *testing.T) string {
	t.Helper()
	g.waitFor(30*time.Second, "a state that never comes", func() bool {
		g.mu.Lock()
		defer g.mu.Unlock()
		return g.fatal != ""
	})
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.fatal
}

func written(e time.Time) (time.Time, error) { return e, nil }

// A guard that stops on an error fails the step's next wait: a third job of the run that starts, a write of the
// end that fails, and three looks in a row that fail.
func TestTheGuardsFailureFailsTheStepsNextWait(t *testing.T) {
	for _, c := range []struct {
		name  string
		looks func(n int) (Look, error)
		write func(time.Time) (time.Time, error)
		want  string
	}{
		{"a third job starts", func(n int) (Look, error) {
			return Look{Clock: guardAt.Add(time.Hour), Jobs: []provider.Job{guardJob("p", guardAt), guardJob("s", guardAt.Add(time.Minute)), guardJob("x", guardAt.Add(2*time.Minute))}}, nil
		}, written, "a third job of the run has started"},
		{"the end cannot be written", func(n int) (Look, error) { return Look{Clock: guardAt}, nil },
			func(time.Time) (time.Time, error) { return time.Time{}, errors.New("the API refused the patch") }, "the API refused the patch"},
		{"three looks fail in a row", func(n int) (Look, error) { return Look{}, errors.New("ssh: connection lost") }, written, "3 looks at the cluster in a row failed"},
	} {
		t.Run(c.name, func(t *testing.T) {
			g := newGuardedRun(t, c.looks, c.write)
			g.startGuard()
			if got := g.waitFatal(t); !strings.Contains(got, "the job-count guard stopped the run") || !strings.Contains(got, c.want) {
				t.Errorf("the step failed with %q, want the guard's %q", got, c.want)
			}
			if err := g.guardResult(); err == nil || !strings.Contains(err.Error(), c.want) {
				t.Errorf("guardResult = %v, want the guard's error", err)
			}
		})
	}
}

// Two looks that fail, then one that works: the guard goes on, and moves the end.
func TestTwoFailedLooksDoNotStopTheGuard(t *testing.T) {
	g := newGuardedRun(t, func(n int) (Look, error) {
		if n%3 != 0 {
			return Look{}, errors.New("ssh: connection lost")
		}
		return Look{Clock: guardAt.Add(time.Duration(n) * time.Second)}, nil
	}, written)
	g.startGuard()
	deadline := time.Now().Add(30 * time.Second)
	for g.writes.Load() < 5 {
		if time.Now().After(deadline) {
			t.Fatalf("the guard wrote the end %d times in 30 s, want 5", g.writes.Load())
		}
		time.Sleep(5 * time.Millisecond)
	}
	select {
	case <-g.guardFail:
		t.Fatal("two failed looks in a row stopped the guard")
	default:
	}
}

// A run of two jobs must end with the successor's drain start as the deployment's end in the API: the guard's
// final end read back, and an error when the API holds another, or when the successor never started.
func TestTheRunMustEndWithTheSuccessorsDrainStartObserved(t *testing.T) {
	succStart := guardAt.Add(80 * time.Minute)
	dsSucc := succStart.Add(91 * time.Minute)
	looks := func(n int) (Look, error) {
		return Look{Clock: succStart.Add(time.Minute), Jobs: []provider.Job{guardJob("p", guardAt), guardJob("s", succStart)}}, nil
	}
	sch := k8sruntime.NewScheme()
	if err := v1alpha1.AddToScheme(sch); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name   string
		inAPI  time.Time
		wantOK bool
	}{
		{"the API holds the successor's drain start", dsSucc, true},
		{"the API holds another end", dsSucc.Add(-time.Minute), false},
	} {
		t.Run(c.name, func(t *testing.T) {
			md := &v1alpha1.ModelDeployment{ObjectMeta: metav1.ObjectMeta{Name: "alpha-0a1b2c3d4e", Namespace: "default"}}
			e := metav1.NewTime(c.inAPI)
			md.Spec.EndTime = &e
			g := newGuardedRun(t, looks, written)
			g.c = crfake.NewClientBuilder().WithScheme(sch).WithObjects(md).Build()
			g.w.md = md
			// Returns as soon as the guard is final: the bound only stops a run that never gets there.
			g.resultWait = 30 * time.Second
			g.startGuard()
			err := g.guardResult()
			if (err == nil) != c.wantOK {
				t.Errorf("guardResult = %v, want ok %v", err, c.wantOK)
			}
		})
	}
}

// waitingJob is a job of the run the scheduler shows waiting.
func waitingJob(id string, phase v1alpha1.AllocationPhase) provider.Job {
	return provider.Job{ID: id, Name: "hb-alpha-0a1b2c3d4e-" + id, State: []string{"PENDING"}, Phase: phase}
}

// B3 of the review of PR 284: the first job ran and the second never started is NOT RUN (the ruling of 8 Nov 2026,
// #272), and only when a read made at the bound says so. A second job that started, one that went away, one that
// never was, and a read that fails are failures that name what they found. The guard's goroutine never looks here
// (an interval of an hour): the one look is the waiter's own, so the outcome depends on no goroutine's timing.
func TestASuccessorThatNeverStartedIsNotRunOnlyOnARead(t *testing.T) {
	first := guardJob("p", guardAt)
	second := guardJob("s", guardAt.Add(80*time.Minute))
	ended := second
	end := guardAt.Add(81 * time.Minute)
	ended.StartTime, ended.HardEnd, ended.EndTime, ended.Phase = nil, nil, &end, v1alpha1.AllocationEnded
	requeued := waitingJob("s", v1alpha1.AllocationFailed)
	requeued.Requeued = true
	for _, c := range []struct {
		name string
		look func() (Look, error)
		// want is the failure's words; empty is NOT RUN.
		want string
	}{
		{"the second job is pending", func() (Look, error) {
			return Look{Clock: guardAt, Jobs: []provider.Job{first, waitingJob("s", v1alpha1.AllocationPending)}}, nil
		}, ""},
		{"the second job is held", func() (Look, error) {
			return Look{Clock: guardAt, Jobs: []provider.Job{first, waitingJob("s", v1alpha1.AllocationHeld)}}, nil
		}, ""},
		{"the queue cannot be read", func() (Look, error) { return Look{}, errors.New("ssh: connection lost") }, "the successor's state could not be read: ssh: connection lost"},
		{"no second job was ever accepted", func() (Look, error) { return Look{Clock: guardAt, Jobs: []provider.Job{first}}, nil }, "the queue lists no second job of the run"},
		{"the second job has started, at this very look", func() (Look, error) { return Look{Clock: guardAt, Jobs: []provider.Job{first, second}}, nil }, "the successor has started"},
		{"the second job ended without starting", func() (Look, error) { return Look{Clock: guardAt, Jobs: []provider.Job{first, ended}}, nil }, "a second job of the run is Ended"},
		{"the second job was requeued", func() (Look, error) { return Look{Clock: guardAt, Jobs: []provider.Job{first, requeued}}, nil }, "a second job of the run is Failed"},
		{"no first job is known to have started", func() (Look, error) {
			return Look{Clock: guardAt, Jobs: []provider.Job{waitingJob("s", v1alpha1.AllocationPending)}}, nil
		}, "no first job of the run is known to have started"},
		{"a third job has started", func() (Look, error) {
			return Look{Clock: guardAt, Jobs: []provider.Job{first, guardJob("s", guardAt.Add(time.Minute)), guardJob("x", guardAt.Add(2*time.Minute))}}, nil
		}, "a third job of the run has started"},
	} {
		t.Run(c.name, func(t *testing.T) {
			g := newGuardedRun(t, func(int) (Look, error) { return c.look() }, written)
			g.guardEvery, g.resultWait = time.Hour, time.Nanosecond
			g.startGuard()
			err := g.guardResult()
			if c.want == "" {
				if !errors.Is(err, ErrSecondNotStarted) || !NotRun(rowOf("alpha-athena-handover"), err) {
					t.Fatalf("guardResult = %v, want the NOT RUN of a second job that never started", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Errorf("guardResult = %v, want a failure that says %q", err, c.want)
			}
			if NotRun(rowOf("alpha-athena-handover"), err) {
				t.Errorf("guardResult = %v, which ends the run NOT RUN", err)
			}
		})
	}
}

// allocVersion is one version of an Allocation as the watch saw it.
func allocVersion(phase v1alpha1.AllocationPhase, reason string, started *metav1.Time) stack.AllocObservation {
	a := &v1alpha1.Allocation{Status: v1alpha1.AllocationStatus{AtProvider: v1alpha1.AllocationObservation{Phase: phase, Reason: reason, StartTime: started}}}
	return stack.AllocObservation{Phase: phase, Allocation: a}
}

// B2 of the review of PR 284: at the first clock's end the run is NOT RUN only on a read that shows its job waiting.
// Every case goes through the real waiter, its deployment created a bound ago, so the bound has passed at its first
// look and no window decides anything. A job first seen failed or requeued with no start time, and a job Running
// with an unreadable start time, end the wait at once, as failures; so does whatever the read at the bound cannot call
// waiting.
func TestAFirstJobIsNotRunAtTheBoundOnlyWhenARead(t *testing.T) {
	waiting := waitingJob("p", v1alpha1.AllocationPending)
	started := guardJob("p", guardAt)
	listed := func(j ...provider.Job) func(context.Context) (Look, error) {
		return func(context.Context) (Look, error) { return Look{Clock: guardAt, Jobs: j}, nil }
	}
	pendingVersions := []stack.AllocObservation{allocVersion(v1alpha1.AllocationSubmitting, "", nil), allocVersion(v1alpha1.AllocationPending, "", nil)}
	ended := waitingJob("p", v1alpha1.AllocationEnded)
	endedAt := guardAt
	ended.EndTime = &endedAt
	requeued := waitingJob("p", v1alpha1.AllocationPending)
	requeued.Requeued = true
	pendingStarted, pendingEnded := waitingJob("p", v1alpha1.AllocationPending), waitingJob("p", v1alpha1.AllocationPending)
	pendingStarted.StartTime, pendingEnded.EndTime = &endedAt, &endedAt
	now := metav1.NewTime(guardAt)
	for _, c := range []struct {
		name     string
		versions []stack.AllocObservation
		look     func(context.Context) (Look, error)
		// wantNotRun, or wantNil for a started job, or else the failure's words.
		wantNotRun, wantNil bool
		want                string
	}{
		{name: "the watch and the read show it pending", versions: pendingVersions, look: listed(waiting), wantNotRun: true},
		{name: "the watch and the read show it held", versions: []stack.AllocObservation{allocVersion(v1alpha1.AllocationHeld, "", nil)}, look: listed(waitingJob("p", v1alpha1.AllocationHeld)), wantNotRun: true},
		{name: "a start time was seen", versions: []stack.AllocObservation{allocVersion(v1alpha1.AllocationRunning, "", &now)}, look: listed(started), wantNil: true},
		{name: "first seen failed with no start time", versions: []stack.AllocObservation{allocVersion(v1alpha1.AllocationFailed, "NodeFail", nil)}, look: listed(waiting), want: "is Failed (reason \"NodeFail\")"},
		{name: "first seen requeued with no start time", versions: []stack.AllocObservation{allocVersion(v1alpha1.AllocationFailed, v1alpha1.ReasonRequeued, nil)}, look: listed(waiting), want: "is Failed (reason \"" + v1alpha1.ReasonRequeued + "\")"},
		{name: "refused", versions: []stack.AllocObservation{allocVersion(v1alpha1.AllocationRefused, "the reason", nil)}, look: listed(waiting), want: "is Refused"},
		{name: "lost", versions: []stack.AllocObservation{allocVersion(v1alpha1.AllocationLost, "the reason", nil)}, look: listed(waiting), want: "is Lost"},
		{name: "running with an unreadable start time", versions: []stack.AllocObservation{allocVersion(v1alpha1.AllocationRunning, "", nil)}, look: listed(waiting), want: "is Running"},
		{name: "ended with no start time", versions: []stack.AllocObservation{allocVersion(v1alpha1.AllocationEnded, "", nil)}, look: listed(waiting), want: "is Ended"},
		{name: "the queue cannot be read at the bound", versions: pendingVersions, look: func(context.Context) (Look, error) { return Look{}, errors.New("ssh: connection lost") }, want: "the queue could not be read: ssh: connection lost"},
		{name: "the watch is stale and the read shows it started", versions: pendingVersions, look: listed(started), want: "a job of the run is"},
		{name: "the read shows it ended", versions: pendingVersions, look: listed(ended), want: "a job of the run is Ended"},
		{name: "the read shows it running, with no start time to read", versions: pendingVersions, look: listed(waitingJob("p", v1alpha1.AllocationRunning)), want: "a job of the run is Running"},
		{name: "the read shows it pending with a start time", versions: pendingVersions, look: listed(pendingStarted), want: "a job of the run is Pending"},
		{name: "the read shows it pending with an end time", versions: pendingVersions, look: listed(pendingEnded), want: "a job of the run is Pending"},
		{name: "the read shows it requeued, pending again after it ran", versions: pendingVersions, look: listed(requeued), want: "a job of the run is Pending"},
		{name: "the queue lists no job of the run: none was accepted", versions: pendingVersions, look: listed(), want: "the queue lists no job of the run"},
		{name: "no Allocation at all, and no job", look: listed(), want: "the queue lists no job of the run"},
	} {
		t.Run(c.name, func(t *testing.T) {
			row := rowOf("tunnel-hour-athena")
			bound := 10 * time.Minute
			r := &liveRun{t: t, guardFail: make(chan struct{}), limit: row.Lease, dw: 15 * time.Minute, created: time.Now().Add(-bound),
				allocsFn: func() []stack.AllocObservation { return c.versions }, lookFn: c.look}
			err := r.firstJobStarted(Plan{Row: row, PendingMax: bound, Logf: t.Logf})
			switch {
			case c.wantNil:
				if err != nil {
					t.Errorf("firstJobStarted = %v, want a job that started", err)
				}
			case c.wantNotRun:
				var ns *NotStartedError
				if !errors.As(err, &ns) || !NotRun(row, err) {
					t.Errorf("firstJobStarted = %v, want a NOT RUN", err)
				}
			default:
				if err == nil || !strings.Contains(err.Error(), c.want) {
					t.Errorf("firstJobStarted = %v, want a failure that says %q", err, c.want)
				}
				if NotRun(row, err) {
					t.Errorf("firstJobStarted = %v, which ends the run NOT RUN", err)
				}
			}
		})
	}
}

// The second job's own bound (review of PR 284, B3): past the time it may start by, a successor that has not started
// ends the wait there, and the run is NOT RUN; it does not stand in the queue for the readiness budget that only a
// job that started needs. A successor that has started keeps its whole wait. The client holds no Instance, so
// neither can become ready, and only the wait's own bounds can end it.
func TestTheSuccessorsWaitEndsAtItsStartBoundUnlessItStarted(t *testing.T) {
	sch := k8sruntime.NewScheme()
	if err := v1alpha1.AddToScheme(sch); err != nil {
		t.Fatal(err)
	}
	wait := func(t *testing.T, final bool, d time.Duration, startBy time.Time) (time.Duration, bool) {
		t.Helper()
		g := newGuardedRun(t, func(int) (Look, error) { return Look{Clock: guardAt}, nil }, written)
		g.c = crfake.NewClientBuilder().WithScheme(sch).Build()
		g.w.md = &v1alpha1.ModelDeployment{ObjectMeta: metav1.ObjectMeta{Name: "alpha-0a1b2c3d4e", Namespace: "default"}}
		g.guardFinal = final
		// The test binary's own -test.timeout is not this wait's to see.
		g.boundedFn = func(d time.Duration) (time.Time, bool) { return time.Now().Add(d), false }
		began := time.Now()
		_, ok := g.successorOf("pre", d, startBy)
		return time.Since(began), ok
	}
	t.Run("not started past its bound: the wait ends there", func(t *testing.T) {
		const whole = 20 * time.Second
		if took, ok := wait(t, false, whole, time.Now().Add(-time.Hour)); ok || took >= whole {
			t.Errorf("ok %t after %s, want the wait ended at its bound, before the whole %s", ok, took, whole)
		}
	})
	// The rest wait the whole of d: a lower bound only, which no load can break.
	const d = 200 * time.Millisecond
	t.Run("started: the whole wait", func(t *testing.T) {
		if took, ok := wait(t, true, d, time.Now().Add(-time.Hour)); ok || took < d {
			t.Errorf("ok %t after %s, want the whole %s", ok, took, d)
		}
	})
	t.Run("not started, before its bound: the whole wait", func(t *testing.T) {
		if took, ok := wait(t, false, d, time.Now().Add(time.Hour)); ok || took < d {
			t.Errorf("ok %t after %s, want the whole %s", ok, took, d)
		}
	})
	t.Run("no bound of its own: the whole wait", func(t *testing.T) {
		if took, ok := wait(t, false, d, time.Time{}); ok || took < d {
			t.Errorf("ok %t after %s, want the whole %s", ok, took, d)
		}
	})
}
