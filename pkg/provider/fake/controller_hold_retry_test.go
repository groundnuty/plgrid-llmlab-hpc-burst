package fake

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// More hold-mode tests (HBEP-7, open question 11): the retry of an attempt that
// is dropped after the in-flight window, failing reads, and deletion. They use
// the helpers of controller_hold_test.go.

func TestLateRegistrationOfADroppedAttemptIsCancelledBySettle(t *testing.T) {
	e := newEnv(t)
	e.ctl.HoldRelease = true
	e.batch.Enqueue(
		Script{Outcome: provider.Unknown, Registered: true, QueueWait: 5 * time.Minute}, // the reply is lost
		Script{QueueWait: 5 * time.Minute},
	)
	e.newAlloc("a1", testProvider)
	e.sync() // 12:00:00: attempt 1 Unknown, and the scheduler holds job 998
	e.wantPhase("a1", v1alpha1.AllocationSubmissionUnknown)
	e.batch.Vanish("998") // neither squeue nor sacct show it yet

	e.syncAt(at(12, 5, 0)) // exactly the 5m window: still waiting
	e.wantPhase("a1", v1alpha1.AllocationSubmissionUnknown)
	if n := e.batch.Count("sbatch"); n != 1 {
		t.Fatalf("sbatch count = %d at the window, want 1", n)
	}

	e.syncAt(at(12, 5, 1)) // one second past it: attempt 1 is dropped, attempt 2 runs
	if n := e.batch.Count("sbatch"); n != 2 {
		t.Fatalf("sbatch count = %d after the window, want 2", n)
	}
	a := e.alloc("a1")
	if rec := submission(t, a); rec.Attempt != 2 || rec.State != v1alpha1.SubmissionDone {
		t.Fatalf("record = %+v, want attempt 2 Done", rec)
	}
	if a.Annotations[v1alpha1.AnnotationJobID] != "999" || a.Annotations[v1alpha1.AnnotationHoldLifted] != "999" {
		t.Fatalf("annotations = %v, want job 999 recorded and lifted", a.Annotations)
	}
	e.wantPhase("a1", v1alpha1.AllocationPending)

	// The late job of attempt 1 appears, held. Settle cancels it and never lifts it.
	e.batch.Reappear("998")
	e.syncAt(at(12, 6, 0))
	j := e.wantJobState("998", "CANCELLED")
	wantTime(t, "late job EndTime", j.EndTime, at(12, 6, 0))
	e.wantJobState("999", "PENDING")
	e.wantPhase("a1", v1alpha1.AllocationPending)
	for _, c := range e.batch.Calls() {
		if c.Op == "scontrol-release" && c.Args[0] == "998" {
			t.Fatalf("the late job's hold was lifted: %+v", c)
		}
	}
	e.wantLifts(1, 1)
	if n := e.batch.Count("scancel"); n != 1 {
		t.Fatalf("scancel logged %d times, want 1", n)
	}
	e.syncAt(at(12, 10, 1)) // 999 was lifted at 12:05:01 and starts at 12:10:01
	e.wantPhase("a1", v1alpha1.AllocationRunning)
}

func TestHoldModeDropsAnInFlightClaimOlderThanTheWindow(t *testing.T) {
	for _, tc := range []struct {
		name   string
		window time.Duration
		before time.Time // the last pass that waits
		after  time.Time // the first pass that submits
	}{
		{"the default window is 5m", 0, at(12, 5, 0), at(12, 5, 1)},
		{"a window of 90s", 90 * time.Second, at(12, 1, 30), at(12, 1, 31)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t)
			e.ctl.HoldRelease = true
			e.ctl.InflightWindow = tc.window
			e.newAlloc("a1", testProvider)
			// A controller died after its claim at 12:00:00, before its sbatch.
			e.setRecordJSON("a1", `{"attempt":1,"state":"InFlight","writer":"fake","time":"2026-10-30T12:00:00Z"}`)
			e.syncAt(at(12, 0, 0))
			e.wantPhase("a1", v1alpha1.AllocationSubmitting) // an attempt in flight
			e.syncAt(tc.before)
			e.wantPhase("a1", v1alpha1.AllocationSubmitting)
			if n := e.batch.Count("sbatch"); n != 0 {
				t.Fatalf("sbatch count = %d inside the window, want 0", n)
			}
			e.syncAt(tc.after)
			if n := e.batch.Count("sbatch"); n != 1 {
				t.Fatalf("sbatch count = %d past the window, want 1", n)
			}
			a := e.alloc("a1")
			if rec := submission(t, a); rec.Attempt != 2 || rec.State != v1alpha1.SubmissionDone {
				t.Fatalf("record = %+v, want attempt 2 Done", rec)
			}
			e.wantPhase("a1", v1alpha1.AllocationRunning)
		})
	}
}

func TestHoldModeFailedReadsNeverLoseTheAllocationAndSettleNothing(t *testing.T) {
	e := newEnv(t)
	e.ctl.HoldRelease = true
	e.newAlloc("a1", testProvider)
	e.recordJob("a1", true)        // 998, held, recorded
	dup := e.submitFor("a1", true) // 999, held, a duplicate
	boom := errors.New("example transport failure")
	e.batch.FailReads(boom)

	// Far longer than LostGrace of failing reads, each pass without an error.
	for _, when := range []time.Time{at(12, 0, 0), at(12, 1, 0), at(12, 5, 0), at(12, 20, 0)} {
		e.syncAt(when)
		o := e.obs("a1")
		if o.Phase == v1alpha1.AllocationLost || o.AbsentSince != nil {
			t.Fatalf("at %s the Allocation is %q, absent since %v: a failed read is not absence", when.Format("15:04:05"), o.Phase, o.AbsentSince)
		}
		if e.provider().Status.LastSuccessfulStatusRead != nil {
			t.Fatal("lastSuccessfulStatusRead advanced on a failing platform")
		}
	}
	if n := e.batch.Count("scancel"); n != 0 {
		t.Fatalf("scancel logged %d times on failing reads, want 0", n)
	}
	e.wantLifts(0, 0)

	e.batch.FailReads(nil)
	e.syncAt(at(12, 21, 0))
	e.wantJobState(dup, "CANCELLED")
	e.wantLifts(1, 1)
	e.wantPhase("a1", v1alpha1.AllocationRunning)
}

func TestDeleteCancelsAHeldJob(t *testing.T) {
	e := newEnv(t)
	e.ctl.HoldRelease = true
	e.batch.Enqueue(Script{QueueWait: 10 * time.Minute})
	e.batch.FailNextLiftHold(errors.New("example: scontrol failed"))
	e.newAlloc("a1", testProvider)
	_ = e.ctl.Sync(context.Background()) // the lift fails: the job stays held
	e.wantPhase("a1", v1alpha1.AllocationHeld)
	e.wantLifts(1, 0)

	e.del("a1")
	e.syncAt(at(12, 1, 0))
	if !e.gone("a1") {
		t.Fatalf("Allocation survived its deletion: %+v", e.alloc("a1").ObjectMeta)
	}
	j := e.wantJobState("998", "CANCELLED")
	wantTime(t, "EndTime", j.EndTime, at(12, 1, 0))
	e.wantLifts(1, 0) // a deleting Allocation lifts nothing
}

func TestDeleteInHoldModeReleasesAnUnknownAttemptAfterTheWindow(t *testing.T) {
	e := newEnv(t)
	e.ctl.HoldRelease = true
	e.batch.Enqueue(Script{Outcome: provider.Unknown})
	e.newAlloc("a1", testProvider)
	e.sync() // 12:00:00: Unknown, no job
	e.del("a1")
	e.syncAt(at(12, 5, 0)) // exactly the window: the attempt may still register
	if e.gone("a1") {
		t.Fatal("the finalizer went inside the in-flight window")
	}
	e.syncAt(at(12, 5, 1))
	if !e.gone("a1") {
		t.Fatal("the finalizer stayed after the window: a late registration is a held orphan, not a reason to wait")
	}
	if n := e.batch.Count("sbatch"); n != 1 {
		t.Fatalf("sbatch count = %d, want 1: a deleting Allocation submits nothing", n)
	}
}
