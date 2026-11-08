package alpha

// The two-job guard of the live steps (review of PR 255, B5 and B6): its
// goroutine looks at the cluster and moves the run's deployment's end
// (TwoJobGuard), any failure of it fails the step's next wait, and a run of
// two jobs must end with the successor's drain start in the API.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
)

// guardEveryLive is how often the two-job guard looks at the cluster, and
// guardLookFails how many looks in a row may fail before it stops the run: an
// end it cannot move on is still a bound, but one that stops moving is reached
// in the time limit less the drain window.
const (
	guardEveryLive = 10 * time.Second
	guardLookFails = 3
)

// every is the guard's interval.
func (r *liveRun) every() time.Duration {
	if r.guardEvery > 0 {
		return r.guardEvery
	}
	return guardEveryLive
}

// look is one look at the cluster for the guard: the cluster's clock, then a
// fresh queue listing, and the run's jobs in it (by its prefix and tags).
func (r *liveRun) look(ctx context.Context) (Look, error) {
	now, err := r.clusterNow(ctx)
	if err != nil {
		return Look{}, err
	}
	js, err := r.batch.Queue(ctx)
	if err != nil {
		return Look{}, fmt.Errorf("the queue: %w", err)
	}
	owned := RunOwned(r.prefix, r.tags)
	l := Look{Clock: now}
	for _, j := range js {
		if owned(j.Name) {
			l.Jobs = append(l.Jobs, j)
		}
	}
	return l, nil
}

// startGuard runs the two-job guard from its own goroutine until stopGuard:
// every guardEvery it looks at the cluster and sets the deployment's end the
// guard gives. Any error of the guard, a failed write of the end included,
// stops it and fails the run's next wait (checkGuard; review of PR 255, B6).
func (r *liveRun) startGuard() {
	r.guardQuit, r.guardDone = make(chan struct{}), make(chan struct{})
	look, setEnd := r.lookFn, r.setEndFn
	if look == nil {
		look = r.look
	}
	if setEnd == nil {
		setEnd = r.endAt
	}
	go func() {
		defer close(r.guardDone)
		fails := 0
		for {
			select {
			case <-r.guardQuit:
				return
			case <-time.After(r.every()):
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			l, err := look(ctx)
			cancel()
			if err != nil {
				if fails++; fails >= guardLookFails {
					r.failGuard(fmt.Errorf("%d looks at the cluster in a row failed, the last: %w", fails, err))
					return
				}
				continue
			}
			fails = 0
			end, final, err := r.guard.Next(l)
			if err == nil {
				_, err = setEnd(end)
			}
			if err != nil {
				r.failGuard(err)
				return
			}
			r.mu.Lock()
			r.guardEnd, r.guardFinal = end, final
			r.mu.Unlock()
		}
	}()
}

// failGuard records the guard's error, once.
func (r *liveRun) failGuard(err error) {
	r.guardOnce.Do(func() {
		r.mu.Lock()
		r.guardErr = err
		r.mu.Unlock()
		close(r.guardFail)
	})
}

// stopGuard stops the guard's goroutine and waits for it.
func (r *liveRun) stopGuard() {
	if r.guardQuit == nil {
		return
	}
	r.guardStop.Do(func() { close(r.guardQuit) })
	<-r.guardDone
}

// checkGuard fails the step at once when the guard has stopped on an error:
// the deployment's end may no longer keep the row to its job count, and Close
// must cancel the run's jobs.
func (r *liveRun) checkGuard() {
	r.t.Helper()
	select {
	case <-r.guardFail:
		r.mu.Lock()
		err := r.guardErr
		r.mu.Unlock()
		fatalf := r.fatalf
		if fatalf == nil {
			fatalf = r.t.Fatalf
		}
		fatalf("the job-count guard stopped the run: %v", err)
	default:
	}
}

// guardResult waits, a few looks at most, for the guard to set the
// successor's drain start as the deployment's end, stops it, and reads the
// end back from the API: a run of two jobs must end with that bound observed.
func (r *liveRun) guardResult() error {
	wait := r.resultWait
	if wait == 0 {
		wait = 6 * r.every()
	}
	deadline := time.Now().Add(wait)
	for {
		select {
		case <-r.guardFail:
			r.mu.Lock()
			err := r.guardErr
			r.mu.Unlock()
			return fmt.Errorf("the job-count guard stopped the run: %w", err)
		default:
		}
		r.mu.Lock()
		final, end := r.guardFinal, r.guardEnd
		r.mu.Unlock()
		if final {
			r.stopGuard()
			var md v1alpha1.ModelDeployment
			ctx, cancel := context.WithTimeout(context.Background(), stack.APICallTimeout)
			defer cancel()
			if err := r.c.Get(ctx, client.ObjectKey{Name: r.w.md.Name, Namespace: r.w.md.Namespace}, &md); err != nil {
				return fmt.Errorf("the run's deployment: %w", err)
			}
			if md.Spec.EndTime == nil || !md.Spec.EndTime.Time.Equal(end) {
				return errors.New("the run's deployment does not end at the successor's drain start the guard set")
			}
			r.t.Logf("the deployment ends at the successor's drain start, %s after the predecessor's", end.Sub(r.guard.predDrainStart))
			return nil
		}
		if time.Now().After(deadline) {
			r.stopGuard()
			return errors.New("the successor never started, so the run's deployment never ended at its drain start")
		}
		time.Sleep(time.Second)
	}
}

// waitFor is stack.WaitFor that fails the step at once when the guard has
// stopped on an error.
func (r *liveRun) waitFor(d time.Duration, what string, cond func() bool) {
	r.t.Helper()
	guarded := func() bool {
		r.checkGuard()
		return cond()
	}
	stack.WaitFor(r.t, d, what, guarded)
}
