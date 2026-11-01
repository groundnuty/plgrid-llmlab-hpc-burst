package fake

// The Controller over a COMPLETING job (HBEP-4, Phases, A completing job;
// science-agent ruling for #198, 1 Nov 2026): it writes what the real
// provider-slurm writes, so the lifecycle controller's tests see jobCompleting.
// Job 998 is the fake's first; the job starts at 12:00:00 and, with FailAfter
// 5m, fails at 12:05:00; its epilog is 10 minutes. Expected values are literals.

import (
	"context"
	"errors"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

func (e *env) wantCompleting(what string, phase v1alpha1.AllocationPhase) {
	e.t.Helper()
	a := e.alloc("a1")
	o := a.Status.AtProvider
	if o.Phase != phase || !o.JobCompleting {
		e.t.Errorf("%s: phase %s, jobCompleting %v; want %s, true", what, o.Phase, o.JobCompleting, phase)
	}
	c := a.GetCondition(v1alpha1.ConditionJobCompleting)
	const want = "job 998 is completing in state FAILED since 2026-10-30T12:05:00Z"
	if c.Status != corev1.ConditionTrue || c.Reason != v1alpha1.ReasonJobCompleting || c.Message != want {
		e.t.Errorf("%s: condition JobCompleting %s %q %q, want True Completing %q", what, c.Status, c.Reason, c.Message, want)
	}
}

func (e *env) wantCleared(what string, phase v1alpha1.AllocationPhase) {
	e.t.Helper()
	a := e.alloc("a1")
	o := a.Status.AtProvider
	if o.Phase != phase || o.JobCompleting {
		e.t.Errorf("%s: phase %s, jobCompleting %v; want %s, false", what, o.Phase, o.JobCompleting, phase)
	}
	c := a.GetCondition(v1alpha1.ConditionJobCompleting)
	if c.Status != corev1.ConditionFalse || c.Reason != v1alpha1.ReasonJobNotCompleting || c.Message != "" {
		e.t.Errorf("%s: condition JobCompleting %s %q %q, want False NotCompleting with no message", what, c.Status, c.Reason, c.Message)
	}
}

// completingEnv has one Allocation whose job failed at 12:05:00 and is
// completing at 12:06:00.
func completingEnv(t *testing.T) *env {
	t.Helper()
	e := newEnv(t)
	five := 5 * time.Minute
	e.batch.Enqueue(Script{FailAfter: &five, Epilog: 10 * time.Minute})
	e.newAlloc("a1", testProvider)
	e.sync()
	e.syncAt(at(12, 6, 0))
	return e
}

func TestControllerMirrorsAndClearsTheCompletingFlag(t *testing.T) {
	e := completingEnv(t)
	e.wantCompleting("12:06:00", v1alpha1.AllocationFailed)
	if o := e.obs("a1"); o.EndTime == nil || !o.EndTime.Time.Equal(at(12, 5, 0)) {
		t.Errorf("endTime %v, want 12:05:00", o.EndTime)
	}
	e.syncAt(at(12, 14, 59)) // 1 s short of the epilog's end, far past MinJobAge
	e.wantCompleting("12:14:59", v1alpha1.AllocationFailed)
	e.syncAt(at(12, 15, 0)) // the epilog ended: the job reads from accounting, which never has the flag
	e.wantCleared("12:15:00", v1alpha1.AllocationFailed)
}

func TestControllerClearsTheFlagWhenNoReadListsTheJob(t *testing.T) {
	e := completingEnv(t)
	e.batch.Vanish("998")
	e.syncAt(at(12, 7, 0))
	e.wantCleared("listed by neither read", v1alpha1.AllocationFailed)
	if e.obs("a1").AbsentSince == nil {
		t.Error("absentSince is unset after a read that showed nothing")
	}
}

func TestControllerKeepsTheFlagOnAFailedRead(t *testing.T) {
	e := completingEnv(t)
	e.batch.FailReads(errors.New("scripted: the platform is unreachable"))
	e.clock.Set(at(12, 20, 0)) // the epilog is over: a successful read would clear the flag
	e.sync()
	e.wantCompleting("read failing", v1alpha1.AllocationFailed)
	e.batch.FailReads(nil)
	e.sync()
	e.wantCleared("read back", v1alpha1.AllocationFailed)
}

// While the job is completing, a deleted Allocation keeps its finalizer, its
// files and its accounting record are not touched, the job is not cancelled
// again, and all three follow once the flag has cleared, once.
func TestDeletedAllocationWaitsForACompletingJob(t *testing.T) {
	e := completingEnv(t)
	e.stageFile("alloc/"+v1alpha1.JobTag(testNS, "a1", "uid-a1")+"/credential", "example-credential")
	scancels := e.batch.Count("scancel")
	e.del("a1")
	revisions := func() int {
		var l appsv1.ControllerRevisionList
		if err := e.c.List(context.Background(), &l); err != nil {
			t.Fatal(err)
		}
		return len(l.Items)
	}
	for _, at := range []time.Time{at(12, 6, 30), at(12, 8, 0), at(12, 14, 59)} {
		e.syncAt(at)
		if e.gone("a1") {
			t.Fatalf("%s: the Allocation is gone while its job is completing", at.Format("15:04:05"))
		}
		if n := revisions(); n != 0 {
			t.Fatalf("%s: %d accounting records while the job is completing, want none", at.Format("15:04:05"), n)
		}
		if n := e.batch.Count("remove-allocation-files"); n != 0 {
			t.Fatalf("%s: %d file removals while the job is completing, want none", at.Format("15:04:05"), n)
		}
	}
	if n := e.batch.Count("scancel") - scancels; n != 0 {
		t.Errorf("%d scancel calls for a completing job, want none", n)
	}
	e.syncAt(at(12, 15, 0))
	if !e.gone("a1") {
		t.Fatal("the Allocation is still there after the flag cleared")
	}
	if n := revisions(); n != 1 {
		t.Errorf("%d accounting records after the flag cleared, want 1", n)
	}
	if n := e.batch.Count("remove-allocation-files"); n != 1 {
		t.Errorf("%d file removals after the flag cleared, want 1", n)
	}
}

// A deleted Allocation whose job the delete cancelled shows the job completing
// in its status: the deleting pass writes jobCompleting and the JobCompleting
// condition (HBEP-4, Risks; it is, with absentSince, the only status a deleting
// pass writes), and the next one after the epilog clears them with the record,
// the files and the finalizer. Job 998 runs from 12:00:00; the delete cancels it
// at 12:03:00 and its epilog is 10 minutes.
func TestDeletedAllocationShowsTheJobCompleting(t *testing.T) {
	e := newEnv(t)
	e.batch.Enqueue(Script{Epilog: 10 * time.Minute})
	e.newAlloc("a1", testProvider)
	e.sync()
	if o := e.obs("a1"); o.Phase != v1alpha1.AllocationRunning || o.JobCompleting {
		t.Fatalf("precondition: phase %s, jobCompleting %v; want Running, false", o.Phase, o.JobCompleting)
	}
	e.clock.Set(at(12, 3, 0))
	e.del("a1")
	e.sync() // cancels the job at 12:03:00
	a := e.alloc("a1")
	c := a.GetCondition(v1alpha1.ConditionJobCompleting)
	const want = "job 998 is completing in state CANCELLED since 2026-10-30T12:03:00Z"
	if !a.Status.AtProvider.JobCompleting || c.Status != corev1.ConditionTrue || c.Message != want {
		t.Fatalf("the deleted Allocation shows jobCompleting %v and condition %s %q, want true and True %q", a.Status.AtProvider.JobCompleting, c.Status, c.Message, want)
	}
	if e.gone("a1") {
		t.Fatal("the Allocation is gone while its job is completing")
	}
	e.syncAt(at(12, 13, 1)) // the epilog ended at 12:13:00
	if !e.gone("a1") {
		t.Fatal("the Allocation is still there after the epilog")
	}
}
