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
	t.Run("the successor never starts", func(t *testing.T) {
		g := newGuardedRun(t, func(n int) (Look, error) {
			return Look{Clock: guardAt.Add(time.Duration(n) * time.Second), Jobs: []provider.Job{guardJob("p", guardAt)}}, nil
		}, written)
		// Whatever the bound, a successor that never starts never makes the guard final.
		g.resultWait = 50 * time.Millisecond
		g.startGuard()
		if err := g.guardResult(); err == nil || !strings.Contains(err.Error(), "the successor never started") {
			t.Errorf("guardResult = %v, want the successor's never starting", err)
		}
	})
}
