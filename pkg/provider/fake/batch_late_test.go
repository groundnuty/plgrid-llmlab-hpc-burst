package fake

import (
	"context"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// Tests of Script.RegisteredLate (HBEP-4, QOS limits and the Test plan's "A
// lost reply under a one-job limit"): the reply of an sbatch is lost and the
// scheduler registers the job only when the next sbatch arrives, before that
// sbatch is checked against the submit limit. Expected IDs and times are
// literals worked out from the script, as elsewhere in this package: the fake
// numbers jobs from 998.

// TestRegisteredLateJobTakesTheSlotOfTheNextSubmit: with a one-job limit, a
// lost reply whose job registers late leaves FindByTag with nothing, and the
// next Submit registers it first, held, so that Submit is refused for the full
// slot.
func TestRegisteredLateJobTakesTheSlotOfTheNextSubmit(t *testing.T) {
	b, _ := newQOSBatch(Config{MaxSubmitPerUser: 1})
	b.Enqueue(Script{Outcome: provider.Unknown, RegisteredLate: true})
	id, outcome, err := b.Submit(context.Background(), gpuReq("example-lost", true))
	if id != "" || outcome != provider.Unknown || err == nil {
		t.Fatalf("Submit = (%q, %s, %v), want no ID, Unknown and an error", id, outcome, err)
	}
	if jobs, err := b.FindByTag(context.Background(), "example-lost", at(11, 0, 0)); err != nil || len(jobs) != 0 {
		t.Fatalf("FindByTag before the late registration = %v, %v; want no job", jobs, err)
	}
	if n := b.LateJobs(); n != 1 {
		t.Fatalf("LateJobs = %d, want 1", n)
	}
	wantSubmitLimit(t, b, "example-next") // the late job is registered before this Submit is checked
	if n := b.LateJobs(); n != 0 {
		t.Fatalf("LateJobs = %d after the next Submit, want 0", n)
	}
	jobs, err := b.FindByTag(context.Background(), "example-lost", at(11, 0, 0))
	if err != nil || len(jobs) != 1 {
		t.Fatalf("FindByTag after the late registration = %v, %v; want the one job", jobs, err)
	}
	if jobs[0].ID != "998" || jobs[0].Hold != provider.HoldUser || jobs[0].Phase != v1alpha1.AllocationHeld {
		t.Fatalf("late job = ID %s hold %v phase %s, want ID 998 held by its user", jobs[0].ID, jobs[0].Hold, jobs[0].Phase)
	}
	if n := b.Count("sbatch"); n != 2 {
		t.Fatalf("sbatch count = %d, want 2 (the lost one and the refused one)", n)
	}
}

// TestRegisteredLateWithoutALimit: with no submit limit the next Submit is
// accepted, and the late job registered before it has the lower ID.
func TestRegisteredLateWithoutALimit(t *testing.T) {
	b, _ := newQOSBatch(Config{})
	b.Enqueue(Script{Outcome: provider.Unknown, RegisteredLate: true})
	if _, outcome, _ := b.Submit(context.Background(), gpuReq("example-lost", false)); outcome != provider.Unknown {
		t.Fatalf("outcome = %s, want Unknown", outcome)
	}
	if id := mustSubmit(t, b, gpuReq("example-next", false)); id != "999" {
		t.Fatalf("next job ID = %s, want 999: the late job took 998", id)
	}
	if jobs, _ := b.FindByTag(context.Background(), "example-lost", at(11, 0, 0)); len(jobs) != 1 || jobs[0].ID != "998" {
		t.Fatalf("late job = %v, want one with ID 998", jobs)
	}
}

// TestRegisteredLateOutcomeIsChecked: RegisteredLate means a lost reply, so it
// panics with any other outcome, and with Registered.
func TestRegisteredLateOutcomeIsChecked(t *testing.T) {
	for name, sc := range map[string]Script{
		"accepted":   {RegisteredLate: true},
		"rejected":   {Outcome: provider.Rejected, RegisteredLate: true},
		"registered": {Outcome: provider.Unknown, Registered: true, RegisteredLate: true},
	} {
		t.Run(name, func(t *testing.T) {
			b, _ := newQOSBatch(Config{})
			defer func() {
				if r := recover(); r == nil || !strings.Contains(r.(string), "RegisteredLate") {
					t.Fatalf("Enqueue panicked with %v, want a message naming RegisteredLate", r)
				}
			}()
			b.Enqueue(sc)
		})
	}
}

// TestRegisteredLateThroughTheController: the lost-reply case under a one-job
// limit, through the stand-in Provider controller in hold mode. The in-flight
// window is 5 minutes, so attempt 1 (12:00:00) is dropped by the pass at
// 12:05:01, which claims attempt 2; the late job registers at that sbatch and
// the attempt is refused for the full slot. The next pass looks the tag up
// before the backoff, adopts the job, and lifts its hold once. No attempt 3 is
// made, then or after the backoff.
func TestRegisteredLateThroughTheController(t *testing.T) {
	e := newEnv(t)
	e.batch = NewBatch(e.clock, Config{Partitions: qosPartitions(), MaxSubmitPerUser: 1})
	e.ctl.Batch = e.batch
	e.ctl.HoldRelease = true
	e.batch.Enqueue(Script{Outcome: provider.Unknown, RegisteredLate: true})
	e.newAlloc("a1", testProvider)

	e.syncAt(at(12, 0, 0))
	e.wantPhase("a1", v1alpha1.AllocationSubmissionUnknown)
	e.syncAt(at(12, 5, 0)) // the age is the window, not past it: nothing is dropped
	e.wantPhase("a1", v1alpha1.AllocationSubmissionUnknown)
	if n := e.batch.Count("sbatch"); n != 1 {
		t.Fatalf("sbatch count = %d at 12:05:00, want 1", n)
	}

	e.syncAt(at(12, 5, 1))
	o := e.wantPhase("a1", v1alpha1.AllocationSubmitting)
	rec := submission(t, e.alloc("a1"))
	if rec.Attempt != 2 || rec.State != v1alpha1.SubmissionRejected || rec.Code != v1alpha1.ReasonSubmitLimitReached {
		t.Fatalf("record = %+v, want attempt 2 Rejected with code SubmitLimitReached: the late job holds the slot", rec)
	}
	if o.Reason != v1alpha1.ReasonSubmitLimitReached {
		t.Fatalf("reason = %q, want SubmitLimitReached", o.Reason)
	}
	if _, ok := e.alloc("a1").Annotations[v1alpha1.AnnotationJobID]; ok {
		t.Fatal("a job ID was recorded for a refused attempt")
	}

	e.syncAt(at(12, 5, 2)) // inside the backoff: the tag is looked up all the same
	o = e.alloc("a1").Status.AtProvider
	if id := e.alloc("a1").Annotations[v1alpha1.AnnotationJobID]; id != "998" || o.JobID != "998" {
		t.Fatalf("job ID annotation %q and status %q, want the late job 998 adopted", id, o.JobID)
	}
	if o.Phase != v1alpha1.AllocationRunning {
		t.Fatalf("phase = %q (reason %q), want Running: the hold is lifted and the job has no queue wait", o.Phase, o.Reason)
	}
	if o.Held {
		t.Fatal("the adopted job is still held")
	}

	e.syncAt(at(12, 8, 0)) // well past the backoff
	if n := e.batch.Count("sbatch"); n != 2 {
		t.Fatalf("sbatch count = %d, want 2: no attempt 3", n)
	}
	if n := e.batch.Count("scontrol-release"); n != 1 {
		t.Fatalf("scontrol-release count = %d, want 1", n)
	}
}

// TestRejectedAttemptWithoutAJobWaitsOutItsBackoff: the lookup before the
// backoff finds nothing for an ordinary refusal, so the backoff and attempt 3
// are as before.
func TestRejectedAttemptWithoutAJobWaitsOutItsBackoff(t *testing.T) {
	e := newEnv(t)
	e.batch.Enqueue(Script{Outcome: provider.Rejected, Reason: "example refusal"})
	e.newAlloc("a1", testProvider)
	e.syncAt(at(12, 0, 0))
	e.wantPhase("a1", v1alpha1.AllocationSubmitting)
	e.syncAt(at(12, 0, 59))
	if n := e.batch.Count("sbatch"); n != 1 {
		t.Fatalf("sbatch count = %d at 12:00:59, want 1: the backoff is 1 minute", n)
	}
	e.syncAt(at(12, 1, 0)) // the age is the backoff: attempt 2 runs
	if n := e.batch.Count("sbatch"); n != 2 {
		t.Fatalf("sbatch count = %d after the backoff, want 2", n)
	}
	e.wantPhase("a1", v1alpha1.AllocationRunning)
}
