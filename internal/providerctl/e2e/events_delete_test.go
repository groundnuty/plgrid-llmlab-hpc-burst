package e2e

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventcount"
)

// Deleting an Allocation (HBEP-8, Allocation events, Planned or not; the erratum
// for #625, R3): a deleting pass writes no phase, so JobEnded is raised in the
// deleting pass that first sees the recorded job's end time, unless the stored
// phase already announced an end or a failure.

// setCause writes the release cause the lifecycle controller writes before it
// deletes an Allocation.
func (r *evRig) setCause(name, cause string) {
	r.t.Helper()
	a, gone := r.get(name)
	if gone {
		r.t.Fatalf("Allocation %s is gone", name)
	}
	if a.Annotations == nil {
		a.Annotations = map[string]string{}
	}
	a.Annotations[v1alpha1.AnnotationReleaseCause] = cause
	if err := c.Update(r.ctx, a); err != nil {
		r.t.Fatal(err)
	}
}

// deleteAndWait deletes the Allocation and runs passes until its finalizer is
// gone.
func (r *evRig) deleteAndWait(name string) {
	r.t.Helper()
	a, _ := r.get(name)
	if err := c.Delete(r.ctx, a); err != nil {
		r.t.Fatal(err)
	}
	for i := 0; i < 30; i++ {
		_ = r.pass(name)
		if _, gone := r.get(name); gone {
			return
		}
	}
	a, _ = r.get(name)
	r.t.Fatalf("the Allocation is not gone after 30 passes of its deletion: finalizers %v phase %s", a.Finalizers, a.Status.AtProvider.Phase)
}

// Deleting a running Allocation gives ended JobEnded, with the release cause in
// its note. The cause is the lifecycle controller's own words, free of the job
// ID: a person's cause that names the job ID is redacted, never copied.
func TestDeletingARunningAllocationRaisesJobEndedWithTheCause(t *testing.T) {
	r, a, id := running(t)
	cause := "Instance example-i failed: example reason, then released"
	r.setCause("a", cause)
	r.deleteAndWait("a")
	evs := r.waitEvents(a, want{"ended", "JobEnded"})
	notes := notesOf(evs, "JobEnded")
	if len(notes) == 0 || !strings.Contains(notes[0], cause) {
		t.Errorf("JobEnded notes %q, want one carrying the release cause %q", notes, cause)
	}
	r.checkNotes(evs, id)
	if n := r.count(r.key("ended", "JobEnded", "Normal")); n != 1 {
		t.Errorf("JobEnded handed over %v times for one deletion, want 1 (once per process)", n)
	}
	for _, reason := range []string{"AllocationEndedEarly", "AllocationFailed"} {
		if n := r.count(r.key("failing", reason, "Warning")); n != 0 {
			t.Errorf("%s raised %v times for a deletion", reason, n)
		}
	}
}

// A hand-written release cause in the form an Operation's executor writes
// (operation <namespace>/<name> <uid>, the whole cause) puts the Operation in
// related and in the note; a cause in any other form, one with text around the
// form included, puts none in related, and the note still carries it.
func TestAnOperationsReleaseCauseIsInRelated(t *testing.T) {
	const opUID = "0a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d"
	r, a, id := running(t)
	cause := "operation example-ns/stop-example " + opUID
	r.setCause("a", cause)
	r.deleteAndWait("a")
	evs := r.waitEvents(a, want{"ended", "JobEnded"})
	r.checkNotes(evs, id)
	var found bool
	for _, e := range evs {
		if e.Reason != "JobEnded" {
			continue
		}
		found = true
		if !strings.Contains(e.Note, cause) {
			t.Errorf("JobEnded note %q, want the release cause %q", e.Note, cause)
		}
		rel := e.Related
		if rel == nil || rel.Kind != "Operation" || rel.Namespace != "example-ns" || rel.Name != "stop-example" || string(rel.UID) != opUID {
			t.Errorf("JobEnded related %+v, want the Operation example-ns/stop-example with UID %s", rel, opUID)
		}
	}
	if !found {
		t.Fatal("no JobEnded event")
	}
}

// A cause that is not the Operation form exactly gives no related object.
func TestAReleaseCauseThatIsNotTheOperationFormGivesNoRelated(t *testing.T) {
	const opUID = "0a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d"
	r, a, id := running(t)
	r.setCause("a", "by hand: operation example-ns/stop-example "+opUID)
	r.deleteAndWait("a")
	evs := r.waitEvents(a, want{"ended", "JobEnded"})
	r.checkNotes(evs, id)
	for _, e := range evs {
		if e.Reason == "JobEnded" && e.Related != nil {
			t.Errorf("JobEnded related %+v for a cause that is not the Operation form, want none", e.Related)
		}
	}
}

// A release cause a person wrote that names the job ID is copied into the note
// with the ID taken out.
func TestAReleaseCauseThatNamesTheJobIDIsRedacted(t *testing.T) {
	r, a, id := running(t)
	r.setCause("a", "stopped by hand, job "+fmt.Sprint(id)+" is not needed")
	r.deleteAndWait("a")
	evs := r.waitEvents(a, want{"ended", "JobEnded"})
	r.checkNotes(evs, id)
	if notes := notesOf(evs, "JobEnded"); len(notes) == 0 || !strings.Contains(notes[0], "stopped by hand") {
		t.Errorf("JobEnded notes %q, want the cause with the job ID redacted", notes)
	}
}

// Deleting an Allocation that carries SubmittedAsOther gives that failing event
// and no end event: the failure was announced, and its deletion follows it. The
// absence of JobEnded is shown by the counter with HBEP-8's four conditions.
func TestDeletingAnAllocationThatCarriesSubmittedAsOtherRaisesNoEndEvent(t *testing.T) {
	r := newEvRig(t, rigOpts{})
	r.newAlloc("a", "3h")
	a := r.until("a", "submitted", 6, func(a *v1alpha1.Allocation) bool { return a.Annotations[v1alpha1.AnnotationJobID] != "" })
	id := r.jobID(a)
	// The scheduler established the job with 2 h, not the requested 3 h: the read-back
	// of the first observe finds it.
	if err := r.sl.SetTimeLimit(id, 120); err != nil {
		t.Fatal(err)
	}
	a = r.until("a", "SubmittedAsOther", 6, func(a *v1alpha1.Allocation) bool {
		return a.GetCondition(v1alpha1.ConditionSubmittedAsOther).Status == "True"
	})
	evs := r.waitEvents(a, want{"failing", "SubmittedAsOther"})
	r.checkNotes(evs, id)
	if notes := notesOf(evs, "SubmittedAsOther"); len(notes) == 0 || !strings.Contains(notes[0], "timeLimit: requested 3h0m0s, got 2h0m0s") {
		t.Errorf("SubmittedAsOther notes %q, want the differing field with both values", notes)
	}
	// The deletion: the first pass cancels the job; a later one reads it ended
	// and would raise JobEnded if the stored phase had not announced a failure.
	// The barrier is a deleting pass of this Allocation that began after the job
	// had ended (the fake Slurm shows it CANCELLED when the pass starts) and has
	// finished emitting when Reconcile returns.
	key := r.key("ended", "JobEnded", "Normal")
	first := r.read(key)
	change := time.Now()
	if err := c.Delete(r.ctx, a); err != nil {
		t.Fatal(err)
	}
	var barrier *eventcount.Barrier
	for i := 0; i < 30; i++ {
		started := time.Now()
		endedBefore := false
		if j, ok := r.sl.Job(id); ok && j.State == "CANCELLED" {
			endedBefore = true
		}
		_ = r.pass("a")
		if endedBefore && barrier == nil {
			barrier = &eventcount.Barrier{What: "a deleting pass of the Allocation that began after the job ended", PassStart: started, Observed: time.Now(), Due: change}
		}
		if _, gone := r.get("a"); gone {
			break
		}
	}
	if _, gone := r.get("a"); !gone {
		t.Fatal("the Allocation is not gone after 30 passes of its deletion")
	}
	if barrier == nil {
		t.Fatal("no deleting pass began after the job ended, so the absence of JobEnded would prove nothing")
	}
	second := r.read(key)
	abs := eventcount.Absence{Key: key, First: first, Second: second, Change: change, Barriers: []eventcount.Barrier{*barrier},
		Quiet: "the only emitter of provider-" + r.prov + "'s JobEnded is this controller's pass over this Allocation, the only Allocation of the Provider, and the controller is not restarted"}
	if err := abs.Require(); err != nil {
		t.Fatalf("JobEnded is not shown absent: %v", err)
	}
	if got := r.apiEvents(a); len(notesOf(got, "JobEnded")) != 0 {
		t.Errorf("the API holds a JobEnded event for an Allocation that carried SubmittedAsOther: %q", notesOf(got, "JobEnded"))
	}
}
