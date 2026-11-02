package fake

// The fake Controller's deleting pass over completing jobs (Astra B2, and both
// reviewers' nit, on PR 217). It writes what the real provider writes: the
// blocking job's flag while any job with the tag completes, naming it when it is
// not the recorded one, and jobCompleting false once nothing does, even when an
// extra finalizer keeps the object. Job 998 is the fake's first, 999 the second.

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The deleting pass that finds the flag cleared writes jobCompleting false and
// the condition False, whichever way the flag cleared, so a finalizer that
// outlives the clearance (an extra one keeps the object) never leaves
// "jobCompleting: true" beside a False condition (a job listed nowhere keeps
// the fake's own finalizer as well, waiting for its final read). The job runs from 12:00:00,
// the delete cancels it at 12:03:00 and its epilog ends at 12:13:00.
func TestDeletingPassClearsJobCompletingWhenAnExtraFinalizerKeepsTheObject(t *testing.T) {
	for _, tc := range []struct {
		name   string
		vanish bool // the job is listed nowhere at clearance, else read from accounting
	}{{"read from accounting", false}, {"listed nowhere", true}} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t)
			e.batch.Enqueue(Script{Epilog: 10 * time.Minute})
			e.newAlloc("a1", testProvider)
			e.sync()
			a := e.alloc("a1")
			controllerutil.AddFinalizer(a, "example.org/keep")
			if err := e.c.Update(context.Background(), a); err != nil {
				t.Fatal(err)
			}
			e.clock.Set(at(12, 3, 0))
			e.del("a1")
			e.sync() // cancels the job at 12:03:00: completing
			if !e.obs("a1").JobCompleting {
				t.Fatal("precondition: the deleted Allocation does not show the job completing")
			}
			if tc.vanish {
				e.batch.Vanish("998")
			}
			e.syncAt(at(12, 14, 0)) // the epilog has ended
			if e.gone("a1") {
				t.Fatal("precondition: the object is gone although an extra finalizer keeps it")
			}
			if n := e.batch.Count("remove-allocation-files"); !tc.vanish && n != 1 {
				t.Fatalf("precondition: %d file removals, want 1: the fake's own finalizer is done", n)
			}
			e.wantCleared("after the epilog", e.obs("a1").Phase)
		})
	}
}

// A job with the tag, not the recorded one, completes after the recorded job has
// ended without the flag: the deleted Allocation keeps its finalizer, its files
// and its record, shows jobCompleting true (the slot count reads it) with a
// condition that names the duplicate and the recorded job, and goes once the
// duplicate's flag clears. Job 998 is recorded and fails at 12:05:00, job 999
// fails then too and completes for 10 minutes, until 12:15:00.
func TestACompletingDuplicateHoldsTheDeletedAllocationAndItsSlot(t *testing.T) {
	e := newEnv(t)
	five := 5 * time.Minute
	e.batch.Enqueue(Script{FailAfter: &five})
	e.batch.Enqueue(Script{FailAfter: &five, Epilog: 10 * time.Minute})
	e.newAlloc("a1", testProvider)
	e.sync()
	if dup := e.submitFor("a1", false); dup != "999" {
		t.Fatalf("precondition: the duplicate is job %s, want 999", dup)
	}
	e.syncAt(at(12, 6, 0))
	if o := e.obs("a1"); o.JobID != "998" || o.Phase != v1alpha1.AllocationFailed || o.JobCompleting {
		t.Fatalf("precondition: job %s, phase %s, jobCompleting %v; want 998, Failed, false: the recorded job is not completing", o.JobID, o.Phase, o.JobCompleting)
	}
	e.stageFile("alloc/"+v1alpha1.JobTag(testNS, "a1", "uid-a1")+"/credential", "example-credential")
	e.del("a1")
	const want = "job 999 is completing in state FAILED since 2026-10-30T12:05:00Z; it carries the tag but is not the recorded job 998"
	for _, when := range []time.Time{at(12, 6, 30), at(12, 8, 0), at(12, 14, 59)} {
		e.syncAt(when)
		if e.gone("a1") {
			t.Fatalf("%s: the Allocation is gone while a job with its tag is completing", when.Format("15:04:05"))
		}
		if n := e.batch.Count("remove-allocation-files"); n != 0 {
			t.Fatalf("%s: %d file removals while a job with the tag is completing, want none", when.Format("15:04:05"), n)
		}
		a := e.alloc("a1")
		c := a.GetCondition(v1alpha1.ConditionJobCompleting)
		if !a.Status.AtProvider.JobCompleting || c.Status != corev1.ConditionTrue || c.Message != want {
			t.Fatalf("%s: jobCompleting %v, condition %s %q; want true, True %q", when.Format("15:04:05"), a.Status.AtProvider.JobCompleting, c.Status, c.Message, want)
		}
	}
	e.syncAt(at(12, 15, 0))
	if !e.gone("a1") {
		t.Fatal("the Allocation is still there after the duplicate's flag cleared")
	}
	if n := e.batch.Count("remove-allocation-files"); n != 1 {
		t.Errorf("%d file removals after the flag cleared, want 1", n)
	}
}
