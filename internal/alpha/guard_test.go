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
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
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
			g.c = crfake.NewClientBuilder().WithScheme(eventScheme(t)).Build()
			g.w.md = &v1alpha1.ModelDeployment{ObjectMeta: metav1.ObjectMeta{Name: "alpha-0a1b2c3d4e", Namespace: "default"}}
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

// eventScheme is a scheme with the events of the API, and the Allocations.
func eventScheme(t *testing.T) *k8sruntime.Scheme {
	t.Helper()
	sch := k8sruntime.NewScheme()
	if err := v1alpha1.AddToScheme(sch); err != nil {
		t.Fatal(err)
	}
	if err := eventsv1.AddToScheme(sch); err != nil {
		t.Fatal(err)
	}
	return sch
}

// The second job's NOT RUN says, as information, whether the controller raised ReplacementLate for the run's deployment
// (the science agent, on PR 284). It is no check, so it cannot change how the run ends: every reading, a read that
// fails included, ends the same NOT RUN through the real waiter, and only the marker's word differs. Another
// deployment's event, and another reason regarding this one, are not it.
func TestWhatTheRunReadOfReplacementLateNeverChangesItsEnd(t *testing.T) {
	const name = "alpha-0a1b2c3d4e"
	event := func(evName, reason, deployment string) *eventsv1.Event {
		return &eventsv1.Event{ObjectMeta: metav1.ObjectMeta{Name: evName, Namespace: "default"}, Reason: reason, Regarding: corev1.ObjectReference{Name: deployment}}
	}
	withEvents := func(t *testing.T, evs ...client.Object) client.Client {
		return crfake.NewClientBuilder().WithScheme(eventScheme(t)).WithObjects(evs...).Build()
	}
	for _, c := range []struct {
		name   string
		client func(t *testing.T) client.Client
		want   string
	}{
		{"raised", func(t *testing.T) client.Client {
			return withEvents(t, event("e1", v1alpha1.ReasonReplacementLate, name))
		}, LateRaised},
		{"not raised", func(t *testing.T) client.Client { return withEvents(t) }, LateNotRaised},
		{"raised for another deployment", func(t *testing.T) client.Client {
			return withEvents(t, event("e1", v1alpha1.ReasonReplacementLate, "alpha-ffffffffff"))
		}, LateNotRaised},
		{"another reason for this deployment", func(t *testing.T) client.Client { return withEvents(t, event("e1", "HandedOver", name)) }, LateNotRaised},
		// A scheme with no events: the list fails, as a read of the API can.
		{"the events cannot be read", func(t *testing.T) client.Client {
			sch := k8sruntime.NewScheme()
			if err := v1alpha1.AddToScheme(sch); err != nil {
				t.Fatal(err)
			}
			return crfake.NewClientBuilder().WithScheme(sch).Build()
		}, LateUnknown},
	} {
		t.Run(c.name, func(t *testing.T) {
			first := guardJob("p", guardAt)
			g := newGuardedRun(t, func(int) (Look, error) {
				return Look{Clock: guardAt, Jobs: []provider.Job{first, waitingJob("s", v1alpha1.AllocationPending)}}, nil
			}, written)
			g.guardEvery, g.resultWait = time.Hour, time.Nanosecond
			g.c = c.client(t)
			g.w.md = &v1alpha1.ModelDeployment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"}}
			g.startGuard()
			err := g.guardResult()
			row := rowOf("alpha-athena-handover")
			if !errors.Is(err, ErrSecondNotStarted) || !NotRun(row, err) {
				t.Fatalf("guardResult = %v, want the NOT RUN of a second job that never started, whatever was read", err)
			}
			note := NotRunNote(row, 10*time.Minute, err)
			if want := "alpha-athena-handover: the first job ran and the second never started; ReplacementLate for its deployment: " + c.want + "; this run's jobs were cancelled by tag\n"; note != want {
				t.Errorf("note %q, want %q", note, want)
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

// The second job's own bound (review of PR 284, B3, and its delta review, L2): past the time it may start by, a successor that has
// not started ends the wait there, and the run is NOT RUN; it does not stand in the queue for the readiness budget that only a job
// that started needs. A look made then decides, on the scheduler's clock and record, not the guard's last look or this host's clock.
// The client holds no Instance, so none can become ready, and only the wait's own bounds can end it.
func TestTheSuccessorsWaitEndsAtItsStartBoundOnAFreshRead(t *testing.T) {
	sch := k8sruntime.NewScheme()
	if err := v1alpha1.AddToScheme(sch); err != nil {
		t.Fatal(err)
	}
	const predTag = "hb-alpha-0a1b2c3d4e-p"
	first := guardJob("p", guardAt)
	first.Name = predTag
	second := guardJob("s", guardAt.Add(time.Minute))
	waiting := waitingJob("s", v1alpha1.AllocationPending)
	// The bound, on the scheduler's clock. This host's clock is an hour past it in every case but one.
	startBy := time.Now().Add(-time.Hour)
	onTheScheduler := func(at time.Time, jobs ...provider.Job) func(int) (Look, error) {
		return func(int) (Look, error) { return Look{Clock: at, Jobs: jobs}, nil }
	}
	wait := func(t *testing.T, look func(int) (Look, error), d time.Duration, stop func(g *guardedRun) func() bool) (time.Duration, bool) {
		t.Helper()
		g := newGuardedRun(t, look, written)
		g.c = crfake.NewClientBuilder().WithScheme(sch).Build()
		g.w.md = &v1alpha1.ModelDeployment{ObjectMeta: metav1.ObjectMeta{Name: "alpha-0a1b2c3d4e", Namespace: "default"}}
		// The test binary's own -test.timeout is not this wait's to see.
		g.boundedFn = func(d time.Duration) (time.Time, bool) { return time.Now().Add(d), false }
		began := time.Now()
		_, ok := g.successorOf("pre", d, stop(g))
		return time.Since(began), ok
	}
	past := func(g *guardedRun) func() bool { return g.secondPastItsBound(predTag, startBy) }
	t.Run("not started on the scheduler's clock past its bound: the wait ends there", func(t *testing.T) {
		const whole = 20 * time.Second
		look := onTheScheduler(startBy.Add(time.Second), first, waiting)
		if took, ok := wait(t, look, whole, past); ok || took >= whole {
			t.Errorf("ok %t after %s, want the wait ended at its bound, before the whole %s", ok, took, whole)
		}
	})
	// The rest wait the whole of d: a lower bound only, which no load can break.
	const d = 200 * time.Millisecond
	for _, c := range []struct {
		name string
		look func(int) (Look, error)
		stop func(g *guardedRun) func() bool
	}{
		{"the scheduler's clock is behind this host's and short of the bound", onTheScheduler(startBy.Add(-time.Second), first, waiting), past},
		{"it started, whatever the guard last saw: readiness goes on", onTheScheduler(startBy.Add(time.Hour), first, second), past},
		{"the look fails, which decides nothing", func(int) (Look, error) { return Look{}, errors.New("ssh: connection lost") }, past},
		// The scheduler's clock is past the bound here, and only this host's is short, so that it is this host's that holds the look back.
		{"this host's clock is short of the bound, so nothing is looked at", onTheScheduler(time.Now().Add(2*time.Hour), first, waiting), func(g *guardedRun) func() bool {
			return g.secondPastItsBound(predTag, time.Now().Add(time.Hour))
		}},
		{"no bound of its own", onTheScheduler(startBy.Add(time.Hour), first, waiting), func(*guardedRun) func() bool { return nil }},
	} {
		t.Run(c.name, func(t *testing.T) {
			if took, ok := wait(t, c.look, d, c.stop); ok || took < d {
				t.Errorf("ok %t after %s, want the whole %s", ok, took, d)
			}
		})
	}
	// Once a second job has started, the wait never looks again: it can last an hour.
	t.Run("once it started it is not looked at again", func(t *testing.T) {
		looks := 0
		g := newGuardedRun(t, func(int) (Look, error) {
			looks++
			return Look{Clock: startBy.Add(time.Hour), Jobs: []provider.Job{first, second}}, nil
		}, written)
		g.guardEvery = time.Nanosecond
		stop := g.secondPastItsBound(predTag, startBy)
		for i := 0; i < 3; i++ {
			if stop() {
				t.Fatal("the wait stopped on a job that started")
			}
		}
		if looks != 1 {
			t.Errorf("%d looks, want 1", looks)
		}
	})
}

// failingFor is a Runner whose commands starting with a word fail once armed.
type failingFor struct {
	slurm.Runner
	word  string
	armed *bool
}

func (f failingFor) Run(ctx context.Context, cmd slurm.Command) (slurm.Result, error) {
	if *f.armed && strings.HasPrefix(cmd.Line, f.word) {
		return slurm.Result{}, errors.New(f.word + ": connection reset")
	}
	return f.Runner.Run(ctx, cmd)
}

// closeWorld is a liveRun over a fake Slurm with its real batch client and its real Close, and two of the run's jobs in it: one that
// has started, which a two-job run knows of, and one pending.
type closeWorld struct {
	*liveRun
	plat            *fakeslurm.Platform
	clock           *pfake.Clock
	first, waiting  int
	accountingFails *bool
}

// startAfterTheRead starts the pending job, as one can between the read that showed it waiting and its cancel. The clock moves on, since a
// job that ends at the very time it started is what a job cancelled while pending looks like (JobStarted).
func (w *closeWorld) startAfterTheRead(t *testing.T) {
	t.Helper()
	if err := w.plat.Start(w.waiting); err != nil {
		t.Fatal(err)
	}
	w.clock.Advance(time.Minute)
}

func newCloseWorld(t *testing.T) *closeWorld {
	t.Helper()
	clock := pfake.NewClock(guardAt)
	plat := fakeslurm.New(clock, fakeslurm.Config{Partitions: []fakeslurm.Partition{{Name: standInPartition}}})
	sess := plat.Login(fakeslurm.Login{User: standInUser, Env: map[string]string{"HOME": "/home/example-user", "SCRATCH": "/scratch/example-user"}})
	prof := provider.Profile{Name: "example", Spec: v1alpha1.PlatformProfileSpec{JSONDataParser: "v0.0.42", StagingDir: "/scratch/example-user"},
		Partition: v1alpha1.PartitionProfile{Name: standInPartition}}
	fails := new(bool)
	b := slurm.NewBatch(failingFor{sess, "sacct", fails}, prof, provider.Settings{Provider: "example", Account: "example-account", Partition: standInPartition},
		slurm.Options{Clock: clock, PollInterval: time.Nanosecond, BusyInterval: time.Nanosecond})
	const prefix = "hb-alpha-0a1b2c3d4e-"
	rec, err := NewRecorder(filepath.Join(t.TempDir(), "out"))
	if err != nil {
		t.Fatal(err)
	}
	first := plat.OutsideJob(standInUser, prefix+"first-000000000001", standInPartition, "", 1)
	if err := plat.SetTimeLimit(first, 5); err != nil {
		t.Fatal(err)
	}
	if err := plat.Start(first); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Minute) // so that its cancel comes after its start, and it reads as a job that started (JobStarted)
	waiting := plat.OutsideJob(standInUser, prefix+"secnd-000000000002", standInPartition, "", 1)
	for _, tag := range []string{prefix + "first-000000000001", prefix + "secnd-000000000002"} {
		if err := rec.Tag(tag); err != nil {
			t.Fatal(err)
		}
	}
	started, listed, err := b.QueueJob(context.Background(), fmt.Sprint(first))
	if err != nil || !listed || started.HardEnd == nil || started.StartTime == nil {
		t.Fatalf("the first job's read: listed %t, err %v", listed, err)
	}
	r := &liveRun{t: t, guardFail: make(chan struct{}), batch: b, tags: rec, prefix: prefix, since: guardAt, endEvery: time.Millisecond,
		guard: &TwoJobGuard{TimeLimit: started.HardEnd.Sub(*started.StartTime), DrainWindow: time.Minute}, c: crfake.NewClientBuilder().WithScheme(eventScheme(t)).Build()}
	r.w.md = &v1alpha1.ModelDeployment{ObjectMeta: metav1.ObjectMeta{Name: "alpha-0a1b2c3d4e", Namespace: "default"}}
	r.lookFn = func(ctx context.Context) (Look, error) {
		js, err := b.Queue(ctx)
		return Look{Clock: clock.Now(), Jobs: js}, err
	}
	return &closeWorld{liveRun: r, plat: plat, clock: clock, first: first, waiting: waiting, accountingFails: fails}
}

// The delta review of PR 284, L3: a job that starts after the read that showed it waiting is no NOT RUN. Close reads the history of the run's
// jobs through their cancel, on a real batch client over the fake Slurm: a second job that started by then is a failure, and so
// is a history that cannot be read. The first job, which a two-job run knows has started, is not counted. A run that read nothing
// as waiting does not look at all, or a read that fails would fail a run that passed.
func TestCloseChecksThatAJobReadAsWaitingNeverStartedThroughItsCancel(t *testing.T) {
	for _, c := range []struct {
		name  string
		after func(t *testing.T, w *closeWorld)
		want  string
	}{
		{"it never started", func(*testing.T, *closeWorld) {}, ""},
		{"it started after the read", func(t *testing.T, w *closeWorld) { w.startAfterTheRead(t) }, "it had started by its cancel"},
		{"its history cannot be read", func(_ *testing.T, w *closeWorld) { *w.accountingFails = true }, "its history could not be read after its cancel"},
	} {
		t.Run(c.name, func(t *testing.T) {
			w := newCloseWorld(t)
			w.guardEvery = time.Hour
			err := w.secondNeverFinal()
			if !errors.Is(err, ErrSecondNotStarted) {
				t.Fatalf("secondNeverFinal = %v, want a second job that never started", err)
			}
			c.after(t, w)
			cerr := w.Close(context.Background())
			if c.want == "" {
				if cerr != nil {
					t.Errorf("Close = %v, want nothing: the first job's start is known, and the second never started", cerr)
				}
				return
			}
			if cerr == nil || !strings.Contains(cerr.Error(), c.want) {
				t.Errorf("Close = %v, want a failure that says %q", cerr, c.want)
			}
		})
	}
	t.Run("a run that read nothing as waiting does not look", func(t *testing.T) {
		w := newCloseWorld(t)
		*w.accountingFails = true
		if cerr := w.Close(context.Background()); cerr != nil {
			t.Errorf("Close = %v, want nothing", cerr)
		}
	})
}

// firstOnly is a tunnel run of one step: the real first-job waiter, then whatever happens after its read, on the real Close, through Run.
type firstOnly struct {
	*closeWorld
	after func()
}

func (f firstOnly) TunnelHour(_ context.Context, p Plan) (TunnelHourObs, error) {
	err := f.firstJobStarted(p)
	f.after()
	return TunnelHourObs{}, err
}

func (f firstOnly) NowServe(_ context.Context, p Plan) (NowServeObs, error) {
	err := f.firstJobStarted(p)
	f.after()
	return NowServeObs{}, err
}

// The first job's side of it, through Run: its deferred Close turns the NOT RUN into a failure that names the start, before any marker.
func TestRunEndsAFailureWhenAFirstJobReadAsWaitingHadStartedByItsCancel(t *testing.T) {
	row := rowOf("tunnel-hour-athena")
	for _, c := range []struct {
		name       string
		start      bool
		wantNotRun bool
	}{
		{"it never started", false, true},
		{"it started after the read", true, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			w := newCloseWorld(t)
			// Only the pending job is this run's first job: the fixture's started one is ended and forgotten.
			if err := w.plat.End(w.first, string(fakeslurm.Cancelled)); err != nil {
				t.Fatal(err)
			}
			var listed []provider.Job
			w.lookFn = func(ctx context.Context) (Look, error) {
				js, err := w.batch.Queue(ctx)
				listed = listed[:0]
				for _, j := range js {
					if j.EndTime == nil {
						listed = append(listed, j)
					}
				}
				return Look{Clock: guardAt, Jobs: listed}, err
			}
			w.limit, w.dw, w.created = row.Lease, 15*time.Minute, time.Now().Add(-time.Hour)
			w.claimedPred = fmt.Sprint(w.first)
			cfg := Config{Row: row, PendingMax: 10 * time.Minute, Out: filepath.Join(t.TempDir(), "out"), FrontLabel: testFrontLabel}
			rec, err := NewRecorder(cfg.Out)
			if err != nil {
				t.Fatal(err)
			}
			st := firstOnly{w, func() {
				if c.start {
					w.startAfterTheRead(t)
				}
			}}
			w.allocsFn = func() []stack.AllocObservation { return nil }
			_, err = Run(context.Background(), cfg, st, rec, t.Logf)
			if got := NotRun(row, err); got != c.wantNotRun {
				t.Errorf("NotRun(%v) = %t, want %t", err, got, c.wantNotRun)
			}
			if !c.wantNotRun && (err == nil || !strings.Contains(err.Error(), "it had started by its cancel")) {
				t.Errorf("err = %v, want a failure that names the start", err)
			}
		})
	}
}

// The delta review of PR 284, L4: on a run whose second job never started, the handovers are not judged, so no read of them can fail the
// run. The client here has no events to list: stack.Events would fail the test.
func TestAHandoverWhoseSecondJobNeverStartedReadsNoEvents(t *testing.T) {
	sch := k8sruntime.NewScheme()
	if err := v1alpha1.AddToScheme(sch); err != nil {
		t.Fatal(err)
	}
	r := &liveRun{t: t, c: crfake.NewClientBuilder().WithScheme(sch).Build()}
	if n := r.handoversOf(&SecondNotStartedError{ReplacementLate: LateRaised}, v1alpha1.Instance{}); n != 0 {
		t.Errorf("%d handovers, want 0 and no read", n)
	}
	withEvents := &liveRun{t: t, c: crfake.NewClientBuilder().WithScheme(eventScheme(t)).Build()}
	if n := withEvents.handoversOf(nil, v1alpha1.Instance{}); n != 0 {
		t.Errorf("%d handovers on a run with none, want 0", n)
	}
}

// A run whose second job never started still judges its first job's part from the watch, so a catch-up that cannot tell is a failure naming that
// (the science agent, on PR 284), with what was read of the second job beside it. It is no NOT RUN: as text, the second job's read cannot
// make it one. The other direction, a run whose first job never started, makes no catch-up and judges nothing from the watch, so a reset
// there decides nothing (TestTheLiveStepsWaitOnWhatTheSumsCount pins the call; stack, TestWhatAResetAtTheEndOfTheTestCountsFor).
func TestACatchUpThatCannotTellFailsARunWhoseSecondJobNeverStarted(t *testing.T) {
	row := rowOf("alpha-athena-handover")
	notKnown := errors.New("the handover's result: whether the watch has caught up with the API is not known")
	err := notKnownBeside(notKnown, &SecondNotStartedError{ReplacementLate: LateRaised})
	if errors.Is(err, ErrSecondNotStarted) || NotRun(row, err) || !errors.Is(err, notKnown) {
		t.Errorf("err = %v; want the catch-up's failure, which is no NOT RUN", err)
	}
	for _, want := range []string{"is not known", "the second never started", "ReplacementLate for its deployment: raised"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("err = %v, which lacks %q", err, want)
		}
	}
	if err := notKnownBeside(notKnown, nil); err != notKnown {
		t.Errorf("err = %v with no second job to speak of, want the catch-up's own", err)
	}
}
