package fake

import (
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// Requeues seen by the controller. A job submitted at 12:00:00 with QueueWait 5m
// starts at 12:05:00.

// A job lifted once, started, then requeued with a hold by its user must not be
// lifted again: HBEP-7 (Settle) lifts a hold once, so a requeue hold stays
// until an operator acts. The test would be vacuous if the requeue left the job
// running and unheld, so it first asserts the state that makes it a test: the
// job is held by its user with no start, and only the annotation and the
// restart count stand between it and a second lift.
func TestARequeueHeldJobIsNotLiftedAgain(t *testing.T) {
	e := newEnv(t)
	e.ctl.HoldRelease = true
	e.batch.Enqueue(Script{QueueWait: 5 * time.Minute})
	e.newAlloc("a1", testProvider)
	e.sync() // 12:00:00: submitted held, lifted at once
	e.wantLifts(1, 1)
	if got := e.alloc("a1").Annotations[v1alpha1.AnnotationHoldLifted]; got != "998" {
		t.Fatalf("hold-lifted annotation = %q, want 998", got)
	}
	e.syncAt(at(12, 6, 0))
	o := e.wantPhase("a1", v1alpha1.AllocationRunning)
	wantMT(t, "startTime", o.StartTime, at(12, 5, 0))

	e.batch.RequeueHold("998")
	j := e.wantJobState("998", "PENDING")
	if j.Hold != provider.HoldUser || j.StartTime != nil || j.RestartCount != 1 {
		t.Fatalf("setup: job 998 = hold %q start %v restarts %d, want held by its user, no start, 1 restart", j.Hold, j.StartTime, j.RestartCount)
	}

	for _, when := range []time.Time{at(12, 7, 0), at(12, 8, 0), at(12, 20, 0), at(13, 0, 0)} {
		e.syncAt(when)
	}
	e.wantLifts(1, 1) // the one lift of the setup; no further call, applied or not
	e.wantJobState("998", "PENDING")
	if j, _ = e.batch.Job("998"); j.Hold != provider.HoldUser || j.StartTime != nil {
		t.Fatalf("job 998 = hold %q start %v at the end, want it still held and not started", j.Hold, j.StartTime)
	}
	o = e.wantPhase("a1", v1alpha1.AllocationFailed)
	if !o.Held || o.Reason != v1alpha1.ReasonRequeued || o.RestartCount != 1 {
		t.Fatalf("observation = held %v reason %q restarts %d, want held, Requeued, 1", o.Held, o.Reason, o.RestartCount)
	}
	wantNilMT(t, "startTime", o.StartTime)
}

// A requeued job that starts again is the same job: it adds no second queue
// wait, and the Allocation shows the new start.
func TestARequeuedJobThatStartsAgainAddsNoSecondQueueWait(t *testing.T) {
	e := newEnv(t)
	e.batch.Enqueue(Script{QueueWait: 5 * time.Minute})
	e.newAlloc("a1", testProvider)
	e.sync()
	e.syncAt(at(12, 6, 0))
	e.wantOneWait("998", at(12, 0, 0), at(12, 5, 0))

	e.clock.Set(at(12, 10, 0))
	e.batch.Requeue("998") // eligible again from 12:10:00, starts 12:15:00
	e.sync()
	o := e.wantPhase("a1", v1alpha1.AllocationFailed)
	wantNilMT(t, "startTime after the requeue", o.StartTime)
	e.syncAt(at(12, 16, 0))
	o = e.wantPhase("a1", v1alpha1.AllocationFailed)
	wantMT(t, "startTime after the restart", o.StartTime, at(12, 15, 0))
	e.wantOneWait("998", at(12, 0, 0), at(12, 5, 0))
}
