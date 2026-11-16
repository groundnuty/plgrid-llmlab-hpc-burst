package lifecycle

// Partial failure and the report (#589, PR 5, Part B): a Release of two kept Allocations on two Providers, one of which
// cannot cancel its job, reports one target Done and one Failed, never a single refusal, with the phase PartlyFailed and
// the OperationPartlyFailed event; once the Provider can cancel again the target is retried to Done and the Operation is
// Done. The fake Slurm of Provider B fails its cancels (Batch.FailCancels) and the rig plays crossplane's reconciler
// (playCrossplane), which writes the Allocation's Synced=False with the error's text.

import (
	"errors"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

func (w *execWorld) partlyFailedEvents(op string) []RecordedEvent {
	var out []RecordedEvent
	for _, e := range w.rec.Events() {
		if e.Code == v1alpha1.ReasonOperationPartlyFailed && e.Name == op {
			out = append(out, e)
		}
	}
	return out
}

func outcomeOf(op *v1alpha1.Operation, name string) (v1alpha1.TargetOutcome, bool) {
	for _, o := range op.Status.Targets {
		if o.Name == name {
			return o, true
		}
	}
	return v1alpha1.TargetOutcome{}, false
}

func TestAReleaseOnTwoProvidersWhereOneCannotCancelReportsOneDoneAndOneFailedThenRecovers(t *testing.T) {
	w, kA, kB := newKeptTwoProvidersWorld(t)
	w.playCrossplane()
	const slurmError = "scancel: slurm_kill_job: Socket timed out on send/recv operation"
	w.b.batch.FailCancels(errors.New(slurmError))
	kAUID, kBUID := w.allocOrNil(kA).UID, w.allocOrNil(kB).UID

	w.plan("release", releaseSpec(kA, kB))
	w.confirm("release")
	w.untilPhase("release", v1alpha1.OperationPhaseAwaitingApproval)
	if got := w.getOp("release").Status.Phase; got != v1alpha1.OperationPhaseAwaitingApproval {
		t.Fatalf("phase %q: releasing kept Allocations needs an approval", got)
	}
	w.approve("release", "person", userPerson)
	w.untilPhase("release", v1alpha1.OperationPhasePartlyFailed)
	op := w.getOp("release")

	// One Done and one Failed, never a single refusal.
	done, okA := outcomeOf(op, kA)
	failed, okB := outcomeOf(op, kB)
	if !okA || !okB || len(op.Status.Targets) != 2 {
		t.Fatalf("outcomes %+v, want one for each of the two committed Allocations", op.Status.Targets)
	}
	if done.State != v1alpha1.TargetStateDone || done.UID != kAUID {
		t.Errorf("A's Allocation is %+v, want Done with its UID %s: A's cancel works", done, kAUID)
	}
	if failed.State != v1alpha1.TargetStateFailed || failed.UID != kBUID || failed.Reason != "ReconcileError" ||
		!strings.Contains(failed.Message, slurmError) || len(failed.Message) > 1024 {
		t.Errorf("B's Allocation is %+v, want Failed with crossplane's reason and the cancel's own text", failed)
	}
	if op.Status.Phase == v1alpha1.OperationPhaseRefused || op.Status.FinishedAt != nil {
		t.Errorf("phase %q, finishedAt %v: a failed target is retried, never a refusal and never finished", op.Status.Phase, op.Status.FinishedAt)
	}
	if _, ok := w.allocation(kA); ok {
		t.Error("A's Allocation still exists")
	}
	if a, ok := w.allocation(kB); !ok || a.DeletionTimestamp.IsZero() {
		t.Errorf("B's Allocation is %+v (found %v): its delete is asked for and held by the failing cancel", a, ok)
	}
	w.wantCause(kA, op)
	w.wantCause(kB, op)
	if c := condOf(op, v1alpha1.ConditionExecutable); c == nil || c.Status != "True" || c.Reason != v1alpha1.ReasonExecuting {
		t.Errorf("Executable %+v, want True/Executing: it is still being carried out", c)
	}

	// OperationPartlyFailed: raised once when it starts, regarding the Operation, and not again within the hour.
	evs := w.partlyFailedEvents("release")
	if len(evs) != 1 {
		t.Fatalf("%d OperationPartlyFailed events, want one when it starts", len(evs))
	}
	if e := evs[0]; e.Type != "failing" || !e.Warning || e.Kind != "*v1alpha1.Operation" || !strings.Contains(e.Message, kB) || !strings.Contains(e.Message, slurmError) {
		t.Errorf("the event is %+v, want a failing Warning regarding the Operation, naming B's Allocation and the error", e)
	}
	w.run(30*time.Minute, 30*time.Second)
	if got := len(w.partlyFailedEvents("release")); got != 1 {
		t.Errorf("%d OperationPartlyFailed events after 30 minutes of the same failure, want still one", got)
	}
	if got := w.getOp("release").Status.Phase; got != v1alpha1.OperationPhasePartlyFailed {
		t.Errorf("phase %q while the cancel keeps failing, want PartlyFailed", got)
	}
	w.run(40*time.Minute, 30*time.Second)
	if got := len(w.partlyFailedEvents("release")); got != 2 {
		t.Errorf("%d OperationPartlyFailed events after more than an hour, want two: at most once an hour while it lasts", got)
	}
	if got := w.md(hoMD).Spec.Instances; got != 1 {
		t.Errorf("spec.instances is %d: a Release writes no spec", got)
	}

	// Recovery: the Provider can cancel again, the target is retried and the Operation is Done.
	w.b.batch.FailCancels(nil)
	w.untilPhase("release", v1alpha1.OperationPhaseDone)
	op = w.getOp("release")
	for _, o := range op.Status.Targets {
		if o.State != v1alpha1.TargetStateDone || o.Reason != "" || o.Message != "" {
			t.Errorf("after the recovery %s is %+v, want Done with no failure left", o.Name, o)
		}
	}
	if _, ok := w.allocation(kB); ok {
		t.Error("B's Allocation still exists after the recovery")
	}
	if op.Status.FinishedAt == nil {
		t.Error("the Operation is Done with no finish time")
	}
	if got := len(w.partlyFailedEvents("release")); got != 2 {
		t.Errorf("%d OperationPartlyFailed events at the end, want the two raised while it failed", got)
	}
}

// Both Providers' cancels working, the same Release is Done with no failure and raises no OperationPartlyFailed: the
// control of the test above, on the same world, so that its Failed is the failing cancel's and nothing else's.
func TestAReleaseOnTwoProvidersWhoseCancelsWorkIsDoneAndRaisesNoFailure(t *testing.T) {
	w, kA, kB := newKeptTwoProvidersWorld(t)
	w.playCrossplane()
	w.plan("release", releaseSpec(kA, kB))
	w.confirm("release")
	w.untilPhase("release", v1alpha1.OperationPhaseAwaitingApproval)
	w.approve("release", "person", userPerson)
	w.untilPhase("release", v1alpha1.OperationPhaseDone)
	for _, o := range w.getOp("release").Status.Targets {
		if o.State != v1alpha1.TargetStateDone {
			t.Errorf("%s is %s, want Done", o.Name, o.State)
		}
	}
	if got := len(w.partlyFailedEvents("release")); got != 0 {
		t.Errorf("%d OperationPartlyFailed events, want none", got)
	}
}

// The Keep link, through the plan (the science agent's ruling on #589, 16 Nov 2026; team-lead's decision, option (c)): an
// approved End without delete, on a deployment whose one Instance is draining, with its started Allocation under
// ReleaseWhenIdle 30m. The plan lists the Allocation as Keep with needsApproval: true (shutdownKeepNeeds), the Operation is
// Done at the commit, and later, past the idle limit by the injected clock, retention releases the Allocation with its OWN
// cause, which is no Operation's. The Done Operation's plan still holds the Allocation's UID as Keep, with needsApproval
// and its cost: the UID is the key a later audit joins through. The control: a Keep that needs no approval.
//
// The Allocation's UID is read from the Allocation object the controller made, never from the plan.
func TestAKeepThatTheOperationApprovedIsLinkedThroughItsPlanAndReleasedByRetentionsOwnCause(t *testing.T) {
	rg := newRig(t)
	md := mdSpec("md")
	md.Spec.RetentionPolicy = &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle, IdleLimit: dur(30 * time.Minute)}
	rg.create(md)
	pol := globalPolicy(person)
	rg.create(&pol)
	rg.until(10*time.Minute, 10*time.Second, "one ready Instance", func() bool { return rg.readyCount("md") == 1 })
	in := rg.liveInstances("md")[0]
	allocName := in.Spec.AllocationRef.Name
	alloc, _ := rg.allocation(allocName)
	allocUID := alloc.UID // from the Allocation object, never from a plan

	// The person lowers the count to zero: the Instance drains. The plan is made while it is draining.
	rg.mutateMD("md", func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 0 })
	rg.until(2*time.Minute, 10*time.Second, "the Instance draining", func() bool { return isDraining(rg.instance(in.Name)) })
	if ended(rg.instance(in.Name)) {
		t.Fatal("the Instance has ended: the plan must be made while it drains")
	}
	w := &execWorld{hedgeRig: &hedgeRig{rig: rg}, log: newCauseLog()}
	w.planOn("md", "end", endSpec(false))
	planned := w.getOp("end")
	var keep *v1alpha1.PlanTarget
	for i, tg := range planned.Status.Plan.Targets {
		if tg.Kind == v1alpha1.TargetKindAllocation && tg.Name == allocName {
			keep = &planned.Status.Plan.Targets[i]
		}
	}
	if keep == nil || keep.Effect != v1alpha1.PlanEffectKeep || !keep.NeedsApproval || keep.UID != allocUID || keep.Cost == nil || keep.Cost.LeaseLeft == nil {
		t.Fatalf("the plan's target for the Allocation is %+v, want Keep with needsApproval, its UID %s and its cost with the lease left", keep, allocUID)
	}
	if got := planTargets(planned.Status.Plan); len(got) != 2 || got["ModelDeployment md"] != v1alpha1.PlanEffectSetEndTime {
		t.Fatalf("the plan is %v, want the Allocation's Keep and the deployment's SetEndTime: the draining Instance is not taken", got)
	}

	w.confirm("end")
	w.approve("end", "person", userPerson)
	w.untilPhase("end", v1alpha1.OperationPhaseDone)
	op := w.getOp("end")
	if _, exists := rg.allocation(allocName); !exists {
		t.Fatal("the Allocation was released by the Operation: a Keep has no act")
	}
	if got := rg.allocOrNil(allocName).Annotations[v1alpha1.AnnotationReleaseCause]; got != "" {
		t.Fatalf("the Allocation carries the release cause %q before anything released it", got)
	}

	// Later, past the idle limit: retention releases it, with its own cause.
	var cause string
	rg.until(2*time.Hour, 30*time.Second, "retention releases the Allocation", func() bool {
		a, ok := rg.allocation(allocName)
		if ok && a.Annotations[v1alpha1.AnnotationReleaseCause] != "" {
			cause = a.Annotations[v1alpha1.AnnotationReleaseCause]
		}
		return !ok || !a.DeletionTimestamp.IsZero()
	})
	if cause != "released when idle, after 30m0s" {
		t.Errorf("the release cause is %q, want retention's own, \"released when idle, after 30m0s\"", cause)
	}
	if ns, name, uid, ok := v1alpha1.ParseReleaseCauseOperation(cause); ok {
		t.Errorf("retention's cause parsed as Operation %s/%s %s: a Keep's later release carries no Operation's cause", ns, name, uid)
	}
	// The Done Operation's plan still holds the Allocation's UID as Keep with needsApproval and its cost.
	done := w.getOp("end")
	if done.Status.Phase != v1alpha1.OperationPhaseDone || done.UID != op.UID {
		t.Fatalf("the Operation is %q", done.Status.Phase)
	}
	var joined *v1alpha1.PlanTarget
	for i, tg := range done.Status.Plan.Targets {
		if tg.UID == allocUID {
			joined = &done.Status.Plan.Targets[i]
		}
	}
	if joined == nil || joined.Effect != v1alpha1.PlanEffectKeep || !joined.NeedsApproval || joined.Cost == nil || joined.Cost.LeaseLeft == nil {
		t.Errorf("the Done Operation's plan target for the Allocation's UID %s is %+v, want Keep, needsApproval and its cost with the lease left", allocUID, joined)
	}
}

// The control: a Keep that needs no approval (kept until its hard end) is no different at the release: retention's cause,
// and no Operation's.
func TestAKeepThatNeededNoApprovalIsReleasedByRetentionsOwnCause(t *testing.T) {
	rg := newRig(t)
	md := mdSpec("md")
	until := rg.clock.Now().Add(45 * time.Minute)
	md.Spec.RetentionPolicy = &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionKeep, Until: ptrTo(metav1.NewTime(until))}
	rg.create(md)
	rg.until(10*time.Minute, 10*time.Second, "one ready Instance", func() bool { return rg.readyCount("md") == 1 })
	allocName := rg.liveInstances("md")[0].Spec.AllocationRef.Name
	rg.mutateMD("md", func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 0 })
	rg.until(10*time.Minute, 10*time.Second, "the Instance ended", func() bool { return len(rg.liveInstances("md")) == 0 })
	var cause string
	rg.until(2*time.Hour, 30*time.Second, "retention releases the Allocation", func() bool {
		a, ok := rg.allocation(allocName)
		if ok && a.Annotations[v1alpha1.AnnotationReleaseCause] != "" {
			cause = a.Annotations[v1alpha1.AnnotationReleaseCause]
		}
		return !ok || !a.DeletionTimestamp.IsZero()
	})
	if !strings.HasPrefix(cause, "kept until ") {
		t.Errorf("the release cause is %q, want retention's own, \"kept until <date>\"", cause)
	}
	if _, _, _, ok := v1alpha1.ParseReleaseCauseOperation(cause); ok {
		t.Errorf("the cause %q parsed as an Operation's", cause)
	}
}
