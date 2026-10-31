package lifecycle

// Failed and lost Allocations, a requeued job, and the lifecycle half of the
// submission protocol (HBEP-7, Test plan; issue #61). The provider half of the
// protocol (the Unknown outcome, the resolution by tag, the hold lift, the
// fallback mode's NeedsOperator) is issue #59's: the tests here check what the
// lifecycle controller does around it, with the provider fake as the other side.

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	afake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// allocJobID returns the job ID recorded on an Allocation, "" before it has one.
func allocJobID(a *v1alpha1.Allocation) string { return a.Annotations[v1alpha1.AnnotationJobID] }

// allocLive returns a deployment's Allocations that are not being deleted.
func allocLive(rg *rig, md string) []v1alpha1.Allocation {
	var out []v1alpha1.Allocation
	for _, a := range rg.allocations(md) {
		if a.DeletionTimestamp.IsZero() {
			out = append(out, a)
		}
	}
	return out
}

// allocReleased reports whether an Allocation is gone or being deleted: the
// provider's finalizer goes at its next pass, so released is not yet gone.
func allocReleased(rg *rig, name string) bool {
	a, ok := rg.allocation(name)
	return !ok || !a.DeletionTimestamp.IsZero()
}

// allocCount counts the fake Slurm's call-log entries of op whose first
// argument is arg (a job ID or a tag).
func allocCount(b *pfake.Batch, op, arg string) int {
	n := 0
	for _, c := range b.Calls() {
		if c.Op == op && len(c.Args) > 0 && c.Args[0] == arg {
			n++
		}
	}
	return n
}

// allocLifts counts the hold lifts that took effect on a job.
func allocLifts(b *pfake.Batch, jobID string) int {
	n := 0
	for _, c := range b.Calls() {
		if c.Op == "scontrol-release" && len(c.Args) == 2 && c.Args[0] == jobID && c.Args[1] == pfake.LiftApplied {
			n++
		}
	}
	return n
}

// allocUntilEnded steps until the Instance is ended and returns it as it then
// was: an ended Instance is deleted once its Allocation is gone.
func allocUntilEnded(rg *rig, name string, limit time.Duration) v1alpha1.Instance {
	rg.t.Helper()
	var got v1alpha1.Instance
	rg.until(limit, 10*time.Second, "Instance "+name+" ended", func() bool {
		var in v1alpha1.Instance
		if err := rg.c.Get(rg.ctx, client.ObjectKey{Namespace: rigNS, Name: name}, &in); err != nil || in.Status.Phase != v1alpha1.InstanceEnded {
			return false
		}
		got = in
		return true
	})
	return got
}

// allocRunTo steps in ticks, the last one shorter, until the clock is at to.
func allocRunTo(rg *rig, to time.Time, tick time.Duration) {
	rg.t.Helper()
	for rg.clock.Now().Before(to) {
		rg.clock.Advance(min(tick, to.Sub(rg.clock.Now())))
		rg.Step()
	}
}

// allocServe creates the deployment "md" and steps until one Instance is ready.
func allocServe(rg *rig) (v1alpha1.Allocation, v1alpha1.Instance) {
	rg.t.Helper()
	rg.create(mdSpec("md"))
	rg.until(10*time.Minute, 10*time.Second, "one ready Instance", func() bool { return rg.readyCount("md") == 1 })
	return rg.allocations("md")[0], rg.liveInstances("md")[0]
}

// allocReplaced checks that one replacement Allocation, with a job of its own,
// serves in place of old, and that nothing else was submitted: 2 sbatch calls.
func allocReplaced(t *testing.T, rg *rig, old *v1alpha1.Allocation) {
	t.Helper()
	rg.until(10*time.Minute, 10*time.Second, "old Allocation gone, replacement ready", func() bool {
		_, there := rg.allocation(old.Name)
		return !there && rg.readyCount("md") == 1
	})
	rg.run(30*time.Minute, time.Minute)
	live := allocLive(rg, "md")
	if len(live) != 1 || live[0].Name == old.Name || allocJobID(&live[0]) == allocJobID(old) || allocJobID(&live[0]) == "" {
		t.Fatalf("want one new Allocation with a new job, got %d: %+v", len(live), live)
	}
	// sbatch calls, not Allocations: a second submission for one Allocation
	// would not show in a count of Allocations.
	if got := rg.submissions(); got != 2 {
		t.Fatalf("sbatch calls = %d, want 2 (the first job and its one replacement)", got)
	}
}

// TestAllocationEndedEarlyIsReplaced covers the Test plan item "An Allocation
// ends early (a cancel outside the framework) while its Instance serves": the
// Instance ends with AllocationEndedEarly, one failing event fires, the
// Allocation is deleted and a replacement is provisioned.
func TestAllocationEndedEarlyIsReplaced(t *testing.T) {
	rg := newRig(t)
	old, served := allocServe(rg)
	rg.batch.CancelExternally(allocJobID(&old))
	ended := allocUntilEnded(rg, served.Name, 2*time.Minute)
	if ended.Status.EndReason != v1alpha1.ReasonAllocationEndedEarly {
		t.Errorf("EndReason = %q, want %q", ended.Status.EndReason, v1alpha1.ReasonAllocationEndedEarly)
	}
	ev := rg.rec.Find("failing", v1alpha1.ReasonAllocationEndedEarly)
	if len(ev) != 1 || ev[0].Name != served.Name {
		t.Errorf("failing/AllocationEndedEarly events = %+v, want exactly one, for %s", ev, served.Name)
	}
	if !allocReleased(rg, old.Name) {
		t.Error("the Allocation whose job was cancelled is not deleted")
	}
	allocReplaced(t, rg, &old)
}

// TestAllocationHardEndWhileDrainingRaisesNoEvent covers the second half of the
// same item: an Allocation that reaches its hard end with its Instance draining
// raises no such event. The engine keeps requests running, so the Instance is
// still draining when the job times out; the previous test is the control that
// the event does fire for an early end.
func TestAllocationHardEndWhileDrainingRaisesNoEvent(t *testing.T) {
	rg := newRig(t, withMaxTime(4*time.Hour))
	rg.engine = func(string) afake.Engine { return afake.Engine{Health: afake.HealthPlainLoad()} }
	alloc, primary := allocServe(rg)
	// The job starts at 08:00:20 (the smoke test's hand calculation), so its hard end is 12:00:20.
	hardEnd := time.Date(2026, 11, 2, 12, 0, 20, 0, time.UTC)
	if got := alloc.Status.AtProvider.HardEnd; got == nil || !got.Time.Equal(hardEnd) {
		t.Fatalf("hard end = %v, want %s", got, hardEnd)
	}
	allocRunTo(rg, hardEnd.Add(-time.Minute), time.Minute)
	allocRunTo(rg, hardEnd.Add(-10*time.Second), 10*time.Second)
	if p := rg.instance(primary.Name).Status.Phase; p != v1alpha1.InstanceDraining {
		t.Fatalf("10 s before the hard end the Instance is %q, want draining: the case is not exercised", p)
	}
	ended := allocUntilEnded(rg, primary.Name, 2*time.Minute)
	if ended.Status.EndReason != v1alpha1.ReasonDrained {
		t.Errorf("EndReason = %q, want %q", ended.Status.EndReason, v1alpha1.ReasonDrained)
	}
	for _, e := range rg.rec.Events() {
		if e.Name == primary.Name && e.Type == "failing" {
			t.Errorf("failing event for the drained Instance: %+v", e)
		}
	}
}

// TestAllocationVanishedJobIsLostAfterGrace covers "A recorded job absent from
// the queue listing and accounting is not Lost before lostGrace (2 min), is Lost
// after it, and the lifecycle controller then replaces it and deletes the
// Allocation". The grace runs from absentSince, which the provider fake writes
// at its first pass after the job vanished; the fake's rule is "more than 2 min".
func TestAllocationVanishedJobIsLostAfterGrace(t *testing.T) {
	rg := newRig(t)
	old, served := allocServe(rg)
	rg.batch.Vanish(allocJobID(&old))
	rg.clock.Advance(10 * time.Second)
	rg.Step()
	since := rg.allocations("md")[0].Status.AtProvider.AbsentSince
	if since == nil {
		t.Fatal("the provider recorded no absentSince for a vanished job")
	}
	phase := func() v1alpha1.AllocationPhase {
		if a, ok := rg.allocation(old.Name); ok {
			return a.Status.AtProvider.Phase
		}
		return ""
	}
	for rg.clock.Now().Before(since.Add(2 * time.Minute)) {
		rg.clock.Advance(10 * time.Second)
		rg.Step()
		if p := rg.instance(served.Name).Status.Phase; phase() == v1alpha1.AllocationLost || allocReleased(rg, old.Name) || p != v1alpha1.InstanceReady || rg.submissions() != 1 {
			t.Fatalf("%s after the job was first absent: phase %q, Instance %q, %d submissions; nothing may happen within the grace",
				rg.clock.Now().Sub(since.Time), phase(), p, rg.submissions())
		}
	}
	rg.clock.Advance(10 * time.Second) // 2 min 10 s after absentSince
	rg.Step()
	if p := phase(); p != v1alpha1.AllocationLost {
		t.Fatalf("phase %q after the grace, want Lost", p)
	}
	if in := rg.instance(served.Name); in.Status.EndReason != v1alpha1.ReasonAllocationLost {
		t.Errorf("EndReason = %q, want %q", in.Status.EndReason, v1alpha1.ReasonAllocationLost)
	}
	if ev := rg.rec.Find("failing", v1alpha1.ReasonAllocationLost); len(ev) != 1 {
		t.Errorf("failing/AllocationLost events = %+v, want one", ev)
	}
	if !allocReleased(rg, old.Name) {
		t.Error("the Lost Allocation is not deleted")
	}
	allocReplaced(t, rg, &old)
}

// TestAllocationRequeuedJobFailsInstanceAndIsReplaced covers "The scheduler
// requeues a serving allocation under the same job ID". The first case shows the
// requeue in both the scheduler's record (Failed with Requeued) and a second
// registration of the agent; the second shows the second registration alone,
// before the provider has read the requeue.
func TestAllocationRequeuedJobFailsInstanceAndIsReplaced(t *testing.T) {
	for _, tc := range []struct {
		name      string
		scheduler bool
	}{{"the scheduler and the agent show it", true}, {"only the second registration shows it", false}} {
		t.Run(tc.name, func(t *testing.T) {
			rg := newRig(t)
			old, served := allocServe(rg)
			job, first := allocJobID(&old), served.Status.Agent.AgentInstanceID
			if n := allocLifts(rg.batch, job); n != 1 {
				t.Fatalf("hold lifts before the requeue = %d, want 1", n)
			}
			if tc.scheduler {
				rg.batch.Requeue(job)
			}
			rg.agentOf(old.Name).agent.Requeue()
			ended := allocUntilEnded(rg, served.Name, 3*time.Minute)
			if ended.Status.EndReason != v1alpha1.ReasonRequeued {
				t.Errorf("EndReason = %q, want %q", ended.Status.EndReason, v1alpha1.ReasonRequeued)
			}
			if ended.Status.Agent == nil || ended.Status.Agent.AgentInstanceID != first {
				t.Errorf("status.agent = %+v, want the first execution's agent %s kept", ended.Status.Agent, first)
			}
			allocReplaced(t, rg, &old)
			if n := allocLifts(rg.batch, job); n != 1 {
				t.Errorf("hold lifts of the requeued job = %d, want 1: its hold is never lifted again", n)
			}
			if n := allocCount(rg.batch, "scancel", job); n < 1 {
				t.Errorf("scancel calls for the requeued job = %d, want at least 1", n)
			}
			if ev := rg.rec.Find("failing", v1alpha1.ReasonRequeued); len(ev) != 1 {
				t.Errorf("failing/Requeued events = %+v, want exactly one", ev)
			}
		})
	}
}

// TestAllocationLostSubmissionReplyIsOneJobWithOneLift covers the lifecycle half
// of "The reply to a submission is lost": the Allocation shows SubmissionUnknown,
// the controller treats it as existing, and after the provider resolves it by
// tag exactly one job carries the tag and has had its hold lifted. The
// resolution itself is issue #59's.
func TestAllocationLostSubmissionReplyIsOneJobWithOneLift(t *testing.T) {
	rg := newRig(t)
	rg.batch.Enqueue(pfake.Script{Outcome: provider.Unknown, Registered: true})
	rg.create(mdSpec("md"))
	rg.until(2*time.Minute, 10*time.Second, "attempt 1 unknown", func() bool {
		al := rg.allocations("md")
		return len(al) == 1 && al[0].Status.AtProvider.Phase == v1alpha1.AllocationSubmissionUnknown
	})
	for range 5 { // the provider is silent for 2.5 min, inside its 5 min window
		rg.clock.Advance(30 * time.Second)
		rg.reconcileAll()
	}
	if n := len(rg.allocations("md")); n != 1 {
		t.Errorf("%d Allocations while the submission is unknown, want 1", n)
	}
	if got := rg.submissions(); got != 1 {
		t.Errorf("sbatch calls = %d, want 1", got)
	}
	if live := rg.liveInstances("md"); len(live) != 1 || live[0].Status.Phase != v1alpha1.InstancePending {
		t.Errorf("Instances while unknown: %+v, want one pending", live)
	}
	rg.until(10*time.Minute, 10*time.Second, "one ready Instance", func() bool { return rg.readyCount("md") == 1 })
	al := rg.allocations("md")
	if len(al) != 1 || rg.submissions() != 1 {
		t.Fatalf("%d Allocations and %d sbatch calls after resolution, want 1 and 1", len(al), rg.submissions())
	}
	jobs, err := rg.batch.FindByTag(rg.ctx, al[0].Annotations[v1alpha1.AnnotationExternalName], time.Time{})
	if err != nil || len(jobs) != 1 {
		t.Fatalf("jobs with the tag: %v, %v; want exactly one", jobs, err)
	}
	if n := allocLifts(rg.batch, jobs[0].ID); n != 1 || allocJobID(&al[0]) != jobs[0].ID {
		t.Errorf("hold lifts of job %s = %d (Allocation records job %q), want 1 and the same job", jobs[0].ID, n, allocJobID(&al[0]))
	}
}

// TestAllocationFallbackUnknownNeedsOperatorUntilAbandoned covers "In fallback
// mode, an Unknown attempt with no job found raises the operator condition after
// the Unknown bound, and no second job is submitted until an operator abandons
// the attempt", as the lifecycle controller sees it: no second Allocation, and
// the one Allocation goes on once the annotation names the attempt. The bound
// is the fake Controller's 10 min default, counted from the attempt's time.
func TestAllocationFallbackUnknownNeedsOperatorUntilAbandoned(t *testing.T) {
	rg := newRig(t, withFallbackMode())
	rg.batch.Enqueue(pfake.Script{Outcome: provider.Unknown})
	rg.create(mdSpec("md"))
	rg.until(2*time.Minute, 10*time.Second, "attempt 1 unknown", func() bool {
		al := rg.allocations("md")
		return len(al) == 1 && al[0].Status.AtProvider.Phase == v1alpha1.AllocationSubmissionUnknown
	})
	name := rg.allocations("md")[0].Name
	resolved := func() corev1.ConditionStatus { // Unknown while the condition is absent
		a, _ := rg.allocation(name)
		return a.GetCondition(v1alpha1.ConditionSubmissionResolved).Status
	}
	attempt := rg.allocations("md")[0].Status.AtProvider.Submission.Time.Time
	allocRunTo(rg, attempt.Add(10*time.Minute), 10*time.Second)
	if s := resolved(); s == corev1.ConditionFalse {
		t.Fatal("SubmissionResolved is False exactly at the bound; it is raised once more than 10 min have passed")
	}
	rg.clock.Advance(10 * time.Second)
	rg.Step()
	if a, _ := rg.allocation(name); a.GetCondition(v1alpha1.ConditionSubmissionResolved).Status != corev1.ConditionFalse ||
		a.GetCondition(v1alpha1.ConditionSubmissionResolved).Reason != v1alpha1.ReasonNeedsOperator {
		t.Fatalf("condition after the bound: %+v, want False NeedsOperator", a.GetCondition(v1alpha1.ConditionSubmissionResolved))
	}
	rg.run(time.Hour, time.Minute)
	if n, subs := len(allocLive(rg, "md")), rg.submissions(); n != 1 || subs != 1 {
		t.Fatalf("%d Allocations and %d sbatch calls while waiting for the operator, want 1 and 1", n, subs)
	}
	var a v1alpha1.Allocation
	if err := rg.c.Get(rg.ctx, client.ObjectKey{Namespace: rigNS, Name: name}, &a); err != nil {
		t.Fatal(err)
	}
	a.Annotations[v1alpha1.AnnotationAbandonAttempt] = "1"
	if err := rg.c.Update(rg.ctx, &a); err != nil {
		t.Fatal(err)
	}
	rg.until(10*time.Minute, 10*time.Second, "one ready Instance after the abandon", func() bool { return rg.readyCount("md") == 1 })
	if n, subs := len(allocLive(rg, "md")), rg.submissions(); n != 1 || subs != 2 {
		t.Errorf("%d Allocations and %d sbatch calls after the abandon, want 1 and 2", n, subs)
	}
}

// TestAllocationTwoControllersLeaveOnlyDesiredAllocations covers "Two controllers
// reconcile one Allocation at once: no Allocation has more than one job whose
// hold is lifted, and no more allocations than desired remain", for two
// lifecycle controllers on one ModelDeployment. Strict alternation of whole
// passes could never double-provision, so the first controller's create of its
// Instance runs a whole pass of the second, which has read the same empty store
// and creates its own Instance and Allocation. The second controller shares the
// first's Backend: one of its own has no registrations (agents dial one
// endpoint) and would fail the Instances with RegistrationTimeout. Conflicts
// between the two are expected and tolerated.
func TestAllocationTwoControllersLeaveOnlyDesiredAllocations(t *testing.T) {
	var second *Reconciler
	key := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: rigNS, Name: "md"}}
	fired := false
	var nested error
	rg := newRig(t, withInterceptors(interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if _, ok := obj.(*v1alpha1.Instance); ok && second != nil && !fired {
				fired = true
				if _, err := second.Reconcile(ctx, key); err != nil && !apierrors.IsConflict(err) {
					nested = err
				}
			}
			return c.Create(ctx, obj, opts...)
		},
	}))
	second = &Reconciler{Client: rg.c, Reader: rg.c, Clock: rg.clock, Backend: rg.backend, Checkpoints: rg.source, Recorder: rg.rec, Config: rg.r.Config}
	rg.create(mdSpec("md"))
	// The provider acts between the two controllers' passes, so the duplicate
	// is submitted before the surplus is seen.
	for end := rg.clock.Now().Add(20 * time.Minute); rg.clock.Now().Before(end); {
		for _, r := range []*Reconciler{rg.r, second} {
			rg.clock.Advance(5 * time.Second)
			if err := rg.prov.Sync(rg.ctx); err != nil {
				t.Fatal(err)
			}
			rg.syncAgents()
			if _, err := r.Reconcile(rg.ctx, key); err != nil && !apierrors.IsConflict(err) {
				t.Fatalf("reconcile at %s: %v", rg.clock.Now().Format(time.TimeOnly), err)
			}
		}
	}
	if nested != nil {
		t.Fatalf("nested pass of the second controller: %v", nested)
	}
	if !fired || rg.submissions() < 2 {
		t.Fatalf("the race produced %d sbatch calls (fired=%v): no duplicate was created, so the test proves nothing", rg.submissions(), fired)
	}
	// No more Allocations than desired host an Instance. A surplus that had
	// started when it was seen is scaled down: its Instance drains and ends,
	// and its Allocation, hosting nothing, follows its retention policy
	// (HBEP-7, Release: scale-down; "In every case, an Allocation left with no
	// Instance follows its retention policy"); under the default, keep until
	// the end time, which this deployment does not set, it stays as warm
	// capacity until its hard end. A surplus still pending is cancelled.
	hosting := map[string]bool{}
	for _, in := range rg.liveInstances("md") {
		hosting[in.Spec.AllocationRef.Name] = true
	}
	var desc []string
	for _, a := range allocLive(rg, "md") {
		desc = append(desc, fmt.Sprintf("%s (job %s, %s, hosting %v)", a.Name, allocJobID(&a), a.Status.AtProvider.Phase, hosting[a.Name]))
		if !hosting[a.Name] && a.Status.AtProvider.StartTime == nil {
			t.Errorf("surplus Allocation %s never started and hosts nothing, yet it was not cancelled", a.Name)
		}
	}
	if len(hosting) != 1 {
		t.Errorf("%d Allocations host a live Instance for 1 desired: %s", len(hosting), strings.Join(desc, ", "))
	}
	if n := rg.readyCount("md"); n != 1 || len(rg.liveInstances("md")) != 1 {
		t.Errorf("%d ready and %d live Instances, want 1 and 1", n, len(rg.liveInstances("md")))
	}
	for _, c := range rg.batch.Calls() {
		if c.Op != "sbatch" {
			continue
		}
		jobs, err := rg.batch.FindByTag(rg.ctx, c.Args[0], time.Time{})
		if err != nil {
			t.Fatal(err)
		}
		lifts := 0
		for _, j := range jobs {
			lifts += allocLifts(rg.batch, j.ID)
		}
		if lifts > 1 {
			t.Errorf("tag %s has %d jobs with a lifted hold, want at most 1", c.Args[0], lifts)
		}
	}
}
