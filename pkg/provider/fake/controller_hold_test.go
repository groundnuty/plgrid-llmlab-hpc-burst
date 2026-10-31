package fake

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// The hold-mode tests (HBEP-7, open question 11: Hold mode and Settle). Every
// count of a submission, a lift or a cancel comes from the Batch's call log,
// never from what Sync returns. Times are written out by hand.

// liftCalls counts the scontrol-release calls in the log, and those of them
// that took effect.
func liftCalls(b *Batch) (all, applied int) {
	for _, c := range b.Calls() {
		if c.Op != "scontrol-release" {
			continue
		}
		all++
		if len(c.Args) == 2 && c.Args[1] == LiftApplied {
			applied++
		}
	}
	return all, applied
}

func (e *env) wantLifts(all, applied int) {
	e.t.Helper()
	if gotAll, gotApplied := liftCalls(e.batch); gotAll != all || gotApplied != applied {
		e.t.Fatalf("scontrol-release: %d calls, %d took effect; want %d and %d (log %+v)", gotAll, gotApplied, all, applied, e.batch.Calls())
	}
}

func (e *env) annotate(name, key, value string) {
	e.t.Helper()
	a := e.alloc(name)
	if a.Annotations == nil {
		a.Annotations = map[string]string{}
	}
	a.Annotations[key] = value
	if err := e.c.Update(context.Background(), a); err != nil {
		e.t.Fatal(err)
	}
}

// setRecordJSON writes a submission record by hand, as a controller that died
// at some step would have left it.
func (e *env) setRecordJSON(name, record string) {
	e.t.Helper()
	e.annotate(name, v1alpha1.AnnotationSubmission, record)
}

// submitFor places a job with the Allocation's tag in the scheduler, as another
// controller or a late registration would. Its sbatch is in the call log.
func (e *env) submitFor(name string, hold bool) string {
	e.t.Helper()
	a := e.alloc(name)
	id, outcome, err := e.batch.Submit(context.Background(), provider.SubmitRequest{
		Tag: v1alpha1.JobTag(testNS, name, a.UID), Hold: hold, Account: accountName, Params: *a.Spec.ForProvider.Batch,
	})
	if outcome != provider.Accepted || err != nil {
		e.t.Fatalf("setup Submit = %s %v", outcome, err)
	}
	return id
}

// recordJob places a job for the Allocation and records it as the Allocation's
// job, with its attempt Done and no hold-lifted annotation.
func (e *env) recordJob(name string, hold bool) string {
	e.t.Helper()
	id := e.submitFor(name, hold)
	e.annotate(name, v1alpha1.AnnotationJobID, id)
	e.setRecordJSON(name, `{"attempt":1,"state":"Done","writer":"fake","time":"2026-10-30T12:00:00Z"}`)
	return id
}

func (e *env) wantJobState(id, state string) provider.Job {
	e.t.Helper()
	j, ok := e.batch.Job(id)
	if !ok || j.State[0] != state {
		e.t.Fatalf("job %s = %v (found %v), want state %s", id, j.State, ok, state)
	}
	return j
}

func TestFailedLiftIsRetriedAndExactlyOneLiftTakesEffect(t *testing.T) {
	e := newEnv(t)
	e.ctl.HoldRelease = true
	e.batch.Enqueue(Script{QueueWait: 5 * time.Minute})
	boom := errors.New("example: scontrol failed")
	e.batch.FailNextLiftHold(boom)
	e.newAlloc("a1", testProvider)

	err := e.ctl.Sync(context.Background()) // 12:00:00: the lift fails
	if !errors.Is(err, boom) || !strings.Contains(err.Error(), "a1") {
		t.Fatalf("Sync = %v, want the lift's error, naming a1", err)
	}
	a := e.alloc("a1")
	if got, ok := a.Annotations[v1alpha1.AnnotationHoldLifted]; ok {
		t.Fatalf("hold-lifted annotation = %q after a failed lift, want none", got)
	}
	if a.Annotations[v1alpha1.AnnotationJobID] != "998" {
		t.Fatalf("job-id = %q: the job is recorded even though its lift failed", a.Annotations[v1alpha1.AnnotationJobID])
	}
	o := e.wantPhase("a1", v1alpha1.AllocationHeld) // the status shows the job as it is
	if !o.Held || o.Reason != provider.ReasonHeldUser {
		t.Fatalf("observation = held %v reason %q, want held by JobHeldUser", o.Held, o.Reason)
	}
	if j, _ := e.batch.Job("998"); j.Hold != provider.HoldUser {
		t.Fatalf("job 998 hold = %q, want it still held by its user", j.Hold)
	}
	e.wantLifts(1, 0)

	e.syncAt(at(12, 0, 30)) // the next pass lifts it
	if got := e.alloc("a1").Annotations[v1alpha1.AnnotationHoldLifted]; got != "998" {
		t.Fatalf("hold-lifted annotation = %q, want 998", got)
	}
	o = e.wantPhase("a1", v1alpha1.AllocationPending)
	if o.Held {
		t.Fatal("the observation still shows a hold")
	}
	e.wantLifts(2, 1)

	// The job's own state says one lift took effect, at 12:00:30: it starts 5m
	// after that, not after its submission.
	e.syncAt(at(12, 5, 29))
	e.wantPhase("a1", v1alpha1.AllocationPending)
	e.syncAt(at(12, 5, 30))
	o = e.wantPhase("a1", v1alpha1.AllocationRunning)
	wantMT(t, "startTime", o.StartTime, at(12, 5, 30))
	e.syncAt(at(12, 10, 0))
	e.syncAt(at(12, 20, 0))
	e.wantLifts(2, 1)
	if n := e.batch.Count("sbatch"); n != 1 {
		t.Fatalf("sbatch count = %d, want 1: a failed lift is retried, never resubmitted", n)
	}
}

func TestRecordedHeldJobIsLiftedOnObserve(t *testing.T) {
	// The job was recorded by an earlier step that did not lift it: the lift
	// belongs to every observe, not only to the establish and the adoption.
	e := newEnv(t)
	e.ctl.HoldRelease = true
	e.newAlloc("a1", testProvider)
	id := e.recordJob("a1", true)
	e.wantLifts(0, 0)

	e.sync()
	if got := e.alloc("a1").Annotations[v1alpha1.AnnotationHoldLifted]; got != id {
		t.Fatalf("hold-lifted annotation = %q, want %s", got, id)
	}
	o := e.wantPhase("a1", v1alpha1.AllocationRunning)
	if o.Held {
		t.Fatal("the observation still shows a hold")
	}
	e.syncAt(at(12, 1, 0))
	e.wantLifts(1, 1)
	if n := e.batch.Count("sbatch"); n != 1 { // the setup's own
		t.Fatalf("sbatch count = %d, want 1 (the setup's): a recorded job is never resubmitted", n)
	}
}

func TestLiftIsGuardedByTheAnnotationAndTheJobsState(t *testing.T) {
	for _, tc := range []struct {
		name      string
		setup     func(e *env, id string)
		script    Script
		wantPhase v1alpha1.AllocationPhase
		wantAll   int
		wantLift  int
	}{
		{
			name:      "the annotation names the job",
			setup:     func(e *env, id string) { e.annotate("a1", v1alpha1.AnnotationHoldLifted, id) },
			wantPhase: v1alpha1.AllocationHeld,
		},
		{
			name:      "the annotation names another job",
			setup:     func(e *env, _ string) { e.annotate("a1", v1alpha1.AnnotationHoldLifted, "111") },
			wantPhase: v1alpha1.AllocationRunning, wantAll: 1, wantLift: 1,
		},
		{
			name:      "the restart count is above 0",
			setup:     func(e *env, id string) { e.batch.Requeue(id) },
			wantPhase: v1alpha1.AllocationFailed, // a requeue is Failed by phase
		},
		{
			name:      "an administrator holds the job",
			script:    Script{AdminHold: true},
			wantPhase: v1alpha1.AllocationHeld,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t)
			e.ctl.HoldRelease = true
			e.batch.Enqueue(tc.script)
			e.newAlloc("a1", testProvider)
			id := e.recordJob("a1", true)
			if tc.setup != nil {
				tc.setup(e, id)
			}
			e.sync()
			e.syncAt(at(12, 1, 0))
			e.wantPhase("a1", tc.wantPhase)
			e.wantLifts(tc.wantAll, tc.wantLift)
		})
	}
}

func TestSettleCancelsHeldDuplicatesButNotTheRecordedJob(t *testing.T) {
	e := newEnv(t)
	e.ctl.HoldRelease = true
	e.batch.Enqueue(Script{QueueWait: time.Hour}) // the recorded job stays pending
	e.newAlloc("a1", testProvider)
	e.newAlloc("a2", testProvider)
	e.sync() // 12:00:00: jobs 998 (a1) and 999 (a2), both lifted
	if e.alloc("a1").Annotations[v1alpha1.AnnotationJobID] != "998" {
		t.Fatal("setup: a1's job is not 998")
	}
	heldDup := e.submitFor("a1", true)     // 1000: a late registration, held
	runningDup := e.submitFor("a1", false) // 1001: not held
	otherHeld := e.submitFor("a2", true)   // 1002: another Allocation's held job
	_ = e.submitFor("a2", true)            // 1003

	e.syncAt(at(12, 1, 0))
	e.wantJobState(heldDup, "CANCELLED")
	e.wantJobState(runningDup, "RUNNING") // settle cancels held jobs only
	e.wantJobState("998", "PENDING")      // the recorded job is never cancelled
	// a2's held duplicates are cancelled by a2's own pass, after a1's.
	e.wantJobState(otherHeld, "CANCELLED")
	var cancelled []string
	for _, c := range e.batch.Calls() {
		if c.Op == "scancel" {
			cancelled = append(cancelled, c.Args[0])
		}
	}
	if len(cancelled) != 3 || cancelled[0] != heldDup {
		t.Fatalf("scancel calls = %v, want a1's duplicate first, then a2's two", cancelled)
	}
	e.wantPhase("a1", v1alpha1.AllocationPending)

	// Settled: the next passes cancel nothing more.
	e.syncAt(at(12, 2, 0))
	if n := e.batch.Count("scancel"); n != 3 {
		t.Fatalf("scancel logged %d times after the next pass, want 3", n)
	}
}

func TestFallbackModeDoesNotSettle(t *testing.T) {
	e := newEnv(t) // HoldRelease false
	e.batch.Enqueue(Script{QueueWait: time.Hour})
	e.newAlloc("a1", testProvider)
	e.sync()
	dup := e.submitFor("a1", true)
	e.syncAt(at(12, 1, 0))
	e.wantJobState(dup, "PENDING")
	if n := e.batch.Count("scancel"); n != 0 {
		t.Fatalf("scancel logged %d times, want 0: only hold mode settles", n)
	}
	e.wantLifts(0, 0)
}
