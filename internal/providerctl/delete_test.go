package providerctl

// Deletion with a submission in flight or a dead claimant (HBEP-7 open
// question 11, Deletion; S4 Astra B3 and the fallback deletion rows of
// spikes/s4/README.md; the doc comment of pkg/provider/fake/controller.go).
// Counts come from the fake's call log.

import (
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// ptDelete deletes the Allocation; its finalizer keeps the object.
func ptDelete(e *env, name string) {
	e.t.Helper()
	if err := e.c.Delete(e.ctx, e.get(name)); err != nil {
		e.t.Fatal(err)
	}
}

// S4 Astra B3 (README table: "finalizer kept; late job cancelled before the
// object goes; 0 live"; HBEP-7 Deletion: "a submission that returns during
// deletion records its job ID, never lifts its hold, and is cancelled"): the
// Allocation is deleted while a reconciler's sbatch is in flight. The
// finalizer stays while the sbatch is unresolved and while the late job is
// live; the late job is cancelled before the object disappears; at the end no
// live job carries the tag; the late job's hold is never lifted (no lift call
// at all). Hold mode and fallback mode (where the late job would run unheld).
func TestDeleteWhileSbatchIsInFlight(t *testing.T) {
	for _, m := range ptModes {
		t.Run(m.name, func(t *testing.T) {
			e := newEnv(t, m.mods...)
			gate := ptViaGate(e)
			second := ptControllerOver(e, e.platform)
			a := e.newAlloc("a")
			tag := tagOf(a)

			gate.arm()
			submitter := ptReconcileAsync(e, e.ctl, "a")
			ptWaitEntered(t, gate)
			ptDelete(e, "a")

			// The other reconciler handles the deletion while the sbatch is in
			// flight: the finalizer stays.
			for i := 0; i < 3; i++ {
				_ = ptReconcileWith(e, second, "a")
				if ptGone(e, "a") {
					t.Fatal("the finalizer was released while an sbatch was in flight")
				}
			}
			if n := e.count("sbatch"); n != 0 {
				t.Fatalf("%d sbatch calls while the first is held at the gate, want 0", n)
			}

			close(gate.release)
			ptWaitDone(t, submitter)
			if _, ok := e.batch.Job(ptFirstJob); !ok {
				t.Fatal("precondition: the late sbatch registered no job")
			}
			if got := ptJobID(e, "a"); got != ptFirstJob {
				t.Errorf("job ID %q after the late sbatch returned, want %s recorded", got, ptFirstJob)
			}

			// Step through the deletion: while the late job is live the object
			// stays; it goes only after the job is cancelled.
			for i := 0; i < 8 && !ptGone(e, "a"); i++ {
				_ = ptReconcileWith(e, second, "a")
				j, _ := e.batch.Job(ptFirstJob)
				switch {
				case ptGone(e, "a") && j.EndTime == nil:
					t.Fatal("the Allocation disappeared while the late job was still live")
				case !ptGone(e, "a") && j.EndTime != nil && i >= 7:
					t.Fatal("the late job is cancelled but the Allocation is still there after 8 reconciles")
				}
			}
			if !ptGone(e, "a") {
				t.Fatal("the Allocation is still there")
			}
			if !ptEnded(t, e, ptFirstJob, "CANCELLED") {
				t.Error("the late job was not cancelled")
			}
			if live := e.live(tag); len(live) != 0 {
				t.Errorf("live jobs with the tag after deletion: %v", live)
			}
			if n := ptLiftAttempts(e); n != 0 {
				t.Errorf("%d lift calls for a job that landed during deletion, want 0", n)
			}
			if n := e.count("sbatch"); n != 1 {
				t.Errorf("%d sbatch calls, want 1", n)
			}
			if n := e.count("remove-allocation-files"); n != 1 {
				t.Errorf("%d file removals, want 1", n)
			}
		})
	}
}

// Deletion with a dead claimant (S4 table, TestFallbackDeleteWaitsForUnresolvedAttempt:
// "finalizer kept past the window until the operator abandons; 0 sbatch";
// fake.Controller doc, Deletion): in fallback mode an unresolved attempt with
// no job keeps the finalizer past the in-flight window, and past unknownBound,
// until an operator sets abandon-attempt to its number; nothing is submitted.
func TestFallbackDeleteWithADeadClaimantWaitsForTheOperator(t *testing.T) {
	e := newEnv(t, fallback)
	e.newAlloc("a")
	ptPlantRecord(e, "a", ptInFlight(e, 1))
	e.run("a", 1) // the reconciler adds its finalizer while it waits on the attempt
	if !ptHasFinalizer(e.get("a")) {
		t.Fatal("precondition: no finalizer")
	}
	ptDelete(e, "a")

	stays := func(step string) {
		t.Helper()
		e.run("a", 2)
		if ptGone(e, "a") {
			t.Fatalf("%s: the Allocation went with an unresolved attempt in fallback mode", step)
		}
	}
	stays("inside the window")
	e.clock.Advance(ptWindow + time.Second)
	stays("past the window")
	e.clock.Advance(time.Hour)
	stays("an hour on")
	if c := e.get("a").GetCondition(v1alpha1.ConditionSubmissionResolved); c.Reason != v1alpha1.ReasonNeedsOperator {
		t.Errorf("SubmissionResolved = %s/%s past unknownBound, want NeedsOperator", c.Status, c.Reason)
	}
	ptSetAnnotation(e, "a", v1alpha1.AnnotationAbandonAttempt, "2") // not the attempt's number
	stays("a wrong abandon-attempt")
	if n := e.count("remove-allocation-files"); n != 0 {
		t.Errorf("%d file removals while the attempt is unresolved, want 0", n)
	}

	ptSetAnnotation(e, "a", v1alpha1.AnnotationAbandonAttempt, "1")
	e.run("a", 3)
	if !ptGone(e, "a") {
		t.Error("the Allocation is still there after the operator abandoned the attempt")
	}
	if n := e.count("sbatch"); n != 0 {
		t.Errorf("%d sbatch calls, want 0: a deleting Allocation submits nothing", n)
	}
	if n := e.count("remove-allocation-files"); n != 1 {
		t.Errorf("%d file removals after the abandon, want 1", n)
	}
}

// The contrast in hold mode (fake.Controller doc, Deletion: "in hold mode
// InflightWindow has passed"; HBEP-7: a registration that arrives later is a
// held orphan, which never runs): the same dead claimant is let go once the
// window has passed, with nothing submitted.
func TestHoldDeleteWithADeadClaimantGoesAfterTheWindow(t *testing.T) {
	e := newEnv(t)
	e.newAlloc("a")
	ptPlantRecord(e, "a", ptInFlight(e, 1))
	e.run("a", 1)
	if !ptHasFinalizer(e.get("a")) {
		t.Fatal("precondition: no finalizer")
	}
	ptDelete(e, "a")
	e.run("a", 2)
	if ptGone(e, "a") {
		t.Fatal("the Allocation went inside the window")
	}
	e.clock.Advance(ptWindow) // exactly the window: still held
	e.run("a", 2)
	if ptGone(e, "a") {
		t.Fatal("the Allocation went at exactly the window")
	}
	e.clock.Advance(time.Second)
	e.run("a", 3)
	if !ptGone(e, "a") {
		t.Error("the Allocation is still there past the window")
	}
	if n := e.count("sbatch"); n != 0 {
		t.Errorf("%d sbatch calls, want 0", n)
	}
}
