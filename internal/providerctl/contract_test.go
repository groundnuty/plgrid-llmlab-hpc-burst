package providerctl

// The rules the contracts fix round pinned (api/v1alpha1 conditions.go and
// allocation_types.go; docs/development.md, Provider readiness and Queue waits;
// pkg/provider/fake/controller.go's doc comment). Counts come from the fakes'
// call logs.

import (
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// The settings are compared on the first observe only: an administrator's
// later extension of the time limit is a later hard end, never a mismatch.
func TestReadBackOnlyOnTheFirstObserve(t *testing.T) {
	e := newEnv(t)
	e.newAlloc("a")
	e.run("a", 4)
	if p := ptPhase(e, "a"); p != v1alpha1.AllocationRunning {
		t.Fatalf("precondition: phase %s, want Running", p)
	}
	before := e.get("a").Status.AtProvider.HardEnd
	e.batch.SetTimeLimit(ptFirstJob, 90*time.Minute)
	a := ptStep(e, "a", time.Minute)
	if s := ptCondStatus(a, v1alpha1.ConditionSubmittedAsOther); s == corev1.ConditionTrue {
		t.Errorf("SubmittedAsOther is True after a later extension: %s", a.GetCondition(v1alpha1.ConditionSubmittedAsOther).Message)
	}
	if h := a.Status.AtProvider.HardEnd; h == nil || before == nil || !h.Time.Equal(before.Add(60*time.Minute)) {
		t.Errorf("hard end %v, want the earlier %v plus 60 min", h, before)
	}
}

// The name is checked on every observe; the renamed job is cancelled once,
// however many observes follow (one scancel in the call log).
func TestRenamedJobIsCancelledOnce(t *testing.T) {
	e := newEnv(t)
	e.batch.Enqueue(fake.Script{Rename: "hb-short"})
	e.newAlloc("a")
	for i := 0; i < 6; i++ {
		ptStep(e, "a", time.Minute)
	}
	if n := e.count("scancel"); n != 1 {
		t.Errorf("%d scancel calls, want 1", n)
	}
	if o := e.get("a").Status.AtProvider; o.Phase != v1alpha1.AllocationRefused || o.Reason != v1alpha1.ReasonJobRenamed {
		t.Errorf("phase %s reason %q, want Refused, %s", o.Phase, o.Reason, v1alpha1.ReasonJobRenamed)
	}
}

// Refused, once set, is kept on every path, also when set by hand; the rest of
// the observation follows the job, and a Refused Allocation with no job ID
// submits nothing.
func TestRefusedIsKept(t *testing.T) {
	e := newEnv(t)
	e.newAlloc("a")
	e.run("a", 4)
	a := e.get("a")
	a.Status.AtProvider.Phase, a.Status.AtProvider.Reason = v1alpha1.AllocationRefused, "set by an operator"
	if err := e.c.Status().Update(e.ctx, a); err != nil {
		t.Fatal(err)
	}
	e.batch.CancelExternally(ptFirstJob)
	a = ptStep(e, "a", time.Minute)
	o := a.Status.AtProvider
	if o.Phase != v1alpha1.AllocationRefused || o.Reason != "set by an operator" {
		t.Errorf("phase %s reason %q, want the operator's Refused kept", o.Phase, o.Reason)
	}
	if o.EndTime == nil {
		t.Error("the rest of the observation did not follow the job: no end time after it was cancelled")
	}

	b := e.newAlloc("b")
	b.Status.AtProvider.Phase, b.Status.AtProvider.Reason = v1alpha1.AllocationRefused, "set by an operator"
	if err := e.c.Status().Update(e.ctx, b); err != nil {
		t.Fatal(err)
	}
	n := e.count("sbatch")
	e.run("b", 4)
	if m := e.count("sbatch"); m != n {
		t.Errorf("%d sbatch calls for a Refused Allocation with no job, want 0", m-n)
	}
}

// A requeue returns the job to pending: the Allocation is Failed, reason
// Requeued, with no start time, then shows the new start when the job runs
// again, and adds no second queue wait.
func TestRequeueToPendingAddsNoSecondWait(t *testing.T) {
	e := newEnv(t)
	e.batch.Enqueue(fake.Script{QueueWait: 3 * time.Minute})
	e.newAlloc("a")
	e.run("a", 4)
	if p := ptStep(e, "a", 4*time.Minute).Status.AtProvider.Phase; p != v1alpha1.AllocationRunning {
		t.Fatalf("precondition: phase %s, want Running", p)
	}
	e.batch.Requeue(ptFirstJob)
	a := ptStep(e, "a", time.Minute)
	o := a.Status.AtProvider
	if o.Phase != v1alpha1.AllocationFailed || o.Reason != v1alpha1.ReasonRequeued || o.StartTime != nil {
		t.Errorf("phase %s reason %q start %v, want Failed, Requeued, no start", o.Phase, o.Reason, o.StartTime)
	}
	a = ptStep(e, "a", 3*time.Minute)
	if a.Status.AtProvider.StartTime == nil {
		t.Error("no start time after the requeued job ran again")
	}
	if got := ptWaitIDs(ptProvider(e)); len(got) != 1 {
		t.Errorf("waits %v, want one: a requeued job adds none", got)
	}
}

// Memory 0 is refused before any call (science agent's ruling, 31 Oct 2026):
// the CRD admits it, and Slurm reads --mem=0 as the whole node.
func TestMemoryZeroIsRefusedBeforeAnyCall(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := newSlurm(clock, fakeslurm.Config{})
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a")
	a := e.newAlloc("alloc-a", 1)
	a.Spec.ForProvider.Batch.Memory = resource.MustParse("0")
	if err := e.c.Update(e.ctx, a); err != nil {
		t.Fatal(err)
	}
	if calls, err := e.createDirect("alloc-a"); calls != 0 || err == nil {
		t.Errorf("Create made %d calls for memory 0 and returned %v, want no call at all and a refusal", calls, err)
	}
	e.run("alloc-a", 3)
	if n := sl.Count("sbatch"); n != 0 {
		t.Errorf("%d sbatch calls for memory 0, want 0", n)
	}
	rec, err := readRecord(e.get("alloc-a"))
	if err != nil || rec.sub == nil || rec.sub.State != v1alpha1.SubmissionRejected || !strings.Contains(rec.sub.Reason, "memory") {
		t.Errorf("record %+v (%v), want Rejected naming the memory", rec.sub, err)
	}
}
