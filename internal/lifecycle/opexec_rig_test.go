package lifecycle

// Execution of Operations on the scenario rig (#589, PR 5, the release paths): each action's acts, the spec written
// first, and the release cause of every release. Every expectation is written by hand from HBEP-7's and HBEP-10's rules,
// never read from the planner's or the executor's own output, and each release's cause is spelled out from the Operation
// object the API server holds, not through the code that writes it (operationCauseOf).

import (
	"regexp"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// independentCause is p625's parser of the cause, as its plan states it, written out here rather than read from
// api/v1alpha1: the cause of a release made for an Operation is the whole text, with nothing before or after it.
var independentCause = regexp.MustCompile(`^operation ([a-z0-9-]+)/(\S+) ([0-9a-f-]{36})$`)

// wantCause fails unless the Allocation was deleted with the cause operationCauseOf spells out for the Operation, and
// the independent parser reads that same Operation from it.
func (w *execWorld) wantCause(alloc string, op *v1alpha1.Operation) {
	w.t.Helper()
	got, ok := w.log.deletedWith(alloc)
	if !ok {
		w.t.Errorf("Allocation %s was never deleted", alloc)
		return
	}
	if want := operationCauseOf(op); got != want {
		w.t.Errorf("Allocation %s was deleted with the cause %q, want %q", alloc, got, want)
	}
	m := independentCause.FindStringSubmatch(got)
	if m == nil || m[1] != op.Namespace || m[2] != op.Name || m[3] != string(op.UID) {
		w.t.Errorf("Allocation %s: the cause %q does not parse as Operation %s/%s %s: %v", alloc, got, op.Namespace, op.Name, op.UID, m)
	}
	// The Provider's controller reads the cause with this parser to put the Operation in its JobEnded event's related object
	// (internal/providerctl, operationOf; HBEP-8, Allocation events). JobEnded is raised by that controller, which the rig's
	// Provider (pkg/provider/fake) is not, so the row asserts what the event reads: the cause names this Operation to the
	// parser the event uses. providerctl/e2e (events_delete_test.go) shows the event naming the Operation from such a cause.
	if ns, name, uid, ok := v1alpha1.ParseReleaseCauseOperation(got); !ok || ns != op.Namespace || name != op.Name || uid != op.UID {
		w.t.Errorf("Allocation %s: ParseReleaseCauseOperation(%q) = %q/%q %q %v, want Operation %s/%s %s: JobEnded's related would not name it", alloc, got, ns, name, uid, ok, op.Namespace, op.Name, op.UID)
	}
}

// targetSet is a plan's or a commit's targets as "<Kind> <name>" with the effect.
func targetSet(kinds []v1alpha1.TargetKind, names []string, effects []v1alpha1.PlanEffect) map[string]v1alpha1.PlanEffect {
	out := map[string]v1alpha1.PlanEffect{}
	for i := range kinds {
		out[string(kinds[i])+" "+names[i]] = effects[i]
	}
	return out
}

func planTargets(p *v1alpha1.OperationPlan) map[string]v1alpha1.PlanEffect {
	var k []v1alpha1.TargetKind
	var n []string
	var e []v1alpha1.PlanEffect
	for _, t := range p.Targets {
		k, n, e = append(k, t.Kind), append(n, t.Name), append(e, t.Effect)
	}
	return targetSet(k, n, e)
}

func committedTargets(c []v1alpha1.CommittedTarget) map[string]v1alpha1.PlanEffect {
	var k []v1alpha1.TargetKind
	var n []string
	var e []v1alpha1.PlanEffect
	for _, t := range c {
		k, n, e = append(k, t.Kind), append(n, t.Name), append(e, t.Effect)
	}
	return targetSet(k, n, e)
}

func sameTargets(a, b map[string]v1alpha1.PlanEffect) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// A Scale that is approved cancels exactly the targets it committed, writes spec.instances = N, and releases the
// Allocation it cancelled with the Operation's cause: P and its Allocation go, R and its Allocation are untouched, and
// the fake Slurm sees no new submission.
func TestAnApprovedScaleActsOnItsCommittedTargetsAndWritesTheSpec(t *testing.T) {
	w := newExecWorld(t, 10*time.Hour)
	sbatches := w.submissions()
	// The identities of what exists, taken from the objects the rig made, before any Operation.
	pendingUID, pendingAllocUID := w.mustInstance(w.pending).UID, w.allocOrNil(w.pendingAlloc).UID
	readyUID, readyAllocUID := w.mustInstance(w.ready).UID, w.allocOrNil(w.readyAlloc).UID

	planned := w.plan("scale", scaleTo(1))
	want := map[string]v1alpha1.PlanEffect{
		"Instance " + w.pending:        v1alpha1.PlanEffectCancelPending,
		"Allocation " + w.pendingAlloc: v1alpha1.PlanEffectCancelPending,
	}
	if got := planTargets(planned.Status.Plan); !sameTargets(got, want) {
		t.Fatalf("the plan is %v, want %v: P and its pending Allocation are what a Scale to one takes", got, want)
	}
	w.confirm("scale")
	w.untilPhase("scale", v1alpha1.OperationPhaseAwaitingApproval)
	if op := w.getOp("scale"); op.Status.Phase != v1alpha1.OperationPhaseAwaitingApproval {
		t.Fatalf("phase %q: cancelling a pending Allocation needs an approval, which nobody gave", op.Status.Phase)
	}
	if md := w.md(hoMD); md.Spec.Instances != 2 {
		t.Fatalf("spec.instances is %d before the approval, want 2", md.Spec.Instances)
	}

	w.approve("scale", "person", userPerson)
	w.untilPhase("scale", v1alpha1.OperationPhaseDone)
	op := w.getOp("scale")

	// What was committed is the plan's targets, by their UIDs.
	if got := committedTargets(op.Status.Committed); !sameTargets(got, want) {
		t.Errorf("committed %v, want %v", got, want)
	}
	for _, c := range op.Status.Committed {
		wantUID := map[string]string{"Instance " + w.pending: string(pendingUID), "Allocation " + w.pendingAlloc: string(pendingAllocUID)}[string(c.Kind)+" "+c.Name]
		if string(c.UID) != wantUID {
			t.Errorf("committed %s %s has UID %s, want the UID of the object that was there, %s", c.Kind, c.Name, c.UID, wantUID)
		}
	}
	// The spec was written, and what it did not take stays.
	if md := w.md(hoMD); md.Spec.Instances != 1 {
		t.Errorf("spec.instances is %d, want 1: the Operation writes the count it commits to", md.Spec.Instances)
	}
	if in := w.instanceOrNil(w.pending); in != nil && (!ended(in) || in.UID != pendingUID) {
		t.Errorf("P's Instance is %q, want ended or gone", in.Status.Phase)
	}
	if a, ok := w.allocation(w.pendingAlloc); ok {
		t.Errorf("P's Allocation still exists, deleting %v", !a.DeletionTimestamp.IsZero())
	}
	if in := w.instance(w.ready); in.Status.Phase != v1alpha1.InstanceReady || in.UID != readyUID {
		t.Errorf("R's Instance is %q: a Scale to one that took P leaves R serving", in.Status.Phase)
	}
	if a, ok := w.allocation(w.readyAlloc); !ok || !a.DeletionTimestamp.IsZero() || a.UID != readyAllocUID || a.Annotations[v1alpha1.AnnotationReleaseCause] != "" {
		t.Errorf("R's Allocation was touched: %+v", a)
	}
	// Its release carries the Operation's cause, and the other Allocation was never deleted.
	w.wantCause(w.pendingAlloc, op)
	if _, ok := w.log.deletedWith(w.readyAlloc); ok {
		t.Error("R's Allocation was deleted")
	}
	if w.submissions() != sbatches {
		t.Errorf("the fake Slurm saw %d submissions, %d before: a Scale that lowers the count makes none", w.submissions(), sbatches)
	}
	// The report: one outcome per committed target, each done; the Operation is finished, and the spec write is recorded.
	if len(op.Status.Targets) != len(op.Status.Committed) {
		t.Fatalf("%d outcomes for %d committed targets", len(op.Status.Targets), len(op.Status.Committed))
	}
	for _, o := range op.Status.Targets {
		if o.State != v1alpha1.TargetStateDone {
			t.Errorf("%s %s is %s, want Done", o.Kind, o.Name, o.State)
		}
	}
	if op.Status.FinishedAt == nil || !isTrue(op.Status.Conditions, v1alpha1.ConditionSpecWritten) {
		t.Errorf("finishedAt %v, SpecWritten %v", op.Status.FinishedAt, isTrue(op.Status.Conditions, v1alpha1.ConditionSpecWritten))
	}
	if c := condOf(op, v1alpha1.ConditionExecutable); c == nil || c.Status != "False" || c.Reason != v1alpha1.ReasonDone {
		t.Errorf("Executable %+v, want False/Done", c)
	}
}

// An approved End shuts the deployment down, and neither the pass that commits it nor the next one provisions anything.
// The deployment asks for three Instances and has two: with the front not configured the third could not be made, and it
// is configured in the very pass that commits. A pass that went on from the old spec would make it, an sbatch in the
// call log; the End's spec write must set the pass to shut down. The call log of the fake Slurm is read, not the API
// alone.
func TestAnApprovedEndShutsDownAndItsCommitPassProvisionsNothing(t *testing.T) {
	w := newExecWorld(t, 10*time.Hour)
	w.restart(false)
	w.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 3 })
	// The deployment asks for a third Instance, and with no front configured the controller says why it cannot make it.
	w.until(time.Minute, 10*time.Second, "the controller to see the third Instance it cannot make", func() bool {
		c := w.condition(hoMD, v1alpha1.ConditionDegraded)
		return c.Status == "True" && strings.Contains(c.Message, v1alpha1.ReasonFrontNotConfigured)
	})

	planned := w.plan("end", endSpec(false))
	want := map[string]v1alpha1.PlanEffect{
		"Instance " + w.ready:          v1alpha1.PlanEffectDrainLastReady,
		"Instance " + w.pending:        v1alpha1.PlanEffectCancelPending,
		"Allocation " + w.readyAlloc:   v1alpha1.PlanEffectReleaseStarted,
		"Allocation " + w.pendingAlloc: v1alpha1.PlanEffectCancelPending,
		"ModelDeployment " + hoMD:      v1alpha1.PlanEffectSetEndTime,
	}
	if got := planTargets(planned.Status.Plan); !sameTargets(got, want) {
		t.Fatalf("the plan is %v, want %v", got, want)
	}
	w.confirm("end")
	w.untilPhase("end", v1alpha1.OperationPhaseAwaitingApproval)
	if got := w.getOp("end").Status.Phase; got != v1alpha1.OperationPhaseAwaitingApproval {
		t.Fatalf("phase %q, want AwaitingApproval", got)
	}

	// The front is configured in the very pass that commits.
	before, sbatches, writes := w.objectNames(), w.submissions(), w.slurmWrites()["sbatch"]
	if writes != sbatches {
		t.Fatalf("the call log holds %d sbatch calls, the counter %d", writes, sbatches)
	}
	w.restart(true)
	w.approve("end", "person", userPerson)
	w.tick() // the pass that commits
	op := w.getOp("end")
	if op.Status.Phase != v1alpha1.OperationPhaseExecuting || len(op.Status.Committed) != len(want) {
		t.Fatalf("after the approval: phase %q with %d committed targets, want Executing with %d", op.Status.Phase, len(op.Status.Committed), len(want))
	}
	md := w.md(hoMD)
	if md.Spec.EndTime == nil || md.Spec.EndTime.Time.After(w.clock.Now()) {
		t.Errorf("spec.endTime is %v, want now (%v)", md.Spec.EndTime, w.clock.Now())
	}
	w.tick() // the next pass
	if got := w.slurmWrites()["sbatch"]; got != writes {
		t.Errorf("the fake Slurm's call log holds %d sbatch calls, %d before the approval: the commit's pass or the next one provisioned", got, writes)
	}
	for n := range w.objectNames() {
		if !before[n] {
			t.Errorf("%s was created", n)
		}
	}

	w.untilPhase("end", v1alpha1.OperationPhaseDone)
	op = w.getOp("end")
	if got := committedTargets(op.Status.Committed); !sameTargets(got, want) {
		t.Errorf("committed %v, want %v", got, want)
	}
	if in := w.instanceOrNil(w.ready); in != nil && (!ended(in) || in.Status.EndReason != v1alpha1.ReasonDrained) {
		t.Errorf("R's Instance is %q (%s), want drained and ended, or gone", in.Status.Phase, in.Status.EndReason)
	}
	for _, a := range []string{w.readyAlloc, w.pendingAlloc} {
		if _, ok := w.allocation(a); ok {
			t.Errorf("Allocation %s still exists", a)
		}
		w.wantCause(a, op)
	}
	if got := w.slurmWrites()["sbatch"]; got != writes {
		t.Errorf("the call log holds %d sbatch calls at the end, %d before: an End provisions nothing", got, writes)
	}
}

// An approved End with delete removes the ModelDeployment with a UID precondition, shuts its Instances down and releases
// its Allocations, each with the Operation's cause; the Operation's final report is written after the deployment is gone.
func TestAnApprovedEndWithDeleteDeletesTheDeploymentAndWritesItsFinalReport(t *testing.T) {
	w := newExecWorld(t, 10*time.Hour)
	var deleted []string
	w.fc.set(nil, func(method string, obj client.Object) {
		if _, ok := obj.(*v1alpha1.ModelDeployment); ok && method == "Delete" {
			deleted = append(deleted, obj.GetName())
		}
	})
	mdUID := w.md(hoMD).UID
	planned := w.plan("end", endSpec(true))
	if got := planTargets(planned.Status.Plan)["ModelDeployment "+hoMD]; got != v1alpha1.PlanEffectDeleteDeployment {
		t.Fatalf("the plan's deployment target is %q, want DeleteDeployment", got)
	}
	w.confirm("end")
	w.untilPhase("end", v1alpha1.OperationPhaseAwaitingApproval)
	w.approve("end", "person", userPerson)
	w.untilPhase("end", v1alpha1.OperationPhaseDone)
	op := w.getOp("end")
	var md v1alpha1.ModelDeployment
	if err := w.c.Get(w.ctx, client.ObjectKey{Namespace: rigNS, Name: hoMD}, &md); !apierrors.IsNotFound(err) {
		t.Errorf("the ModelDeployment: %v, want it gone", err)
	}
	if len(deleted) != 1 {
		t.Errorf("the controller deleted the ModelDeployment %d times, want once", len(deleted))
	}
	for _, c := range op.Status.Committed {
		if c.Kind == v1alpha1.TargetKindModelDeployment && c.UID != mdUID {
			t.Errorf("the committed deployment target has UID %s, want %s", c.UID, mdUID)
		}
	}
	for _, a := range []string{w.readyAlloc, w.pendingAlloc} {
		w.wantCause(a, op)
	}
	for _, o := range op.Status.Targets {
		if o.State != v1alpha1.TargetStateDone {
			t.Errorf("%s %s is %s, want Done", o.Kind, o.Name, o.State)
		}
	}
}

// keptWorld is two ready Instances on A whose count a person lowers to one: the drained Instance's started Allocation
// stays, hosting nothing, under the default retention. Release's candidate.
func newKeptWorld(t *testing.T) (w *execWorld, kept string) {
	t.Helper()
	w = newExecWorld(t, 0)
	live := w.liveInstances(hoMD)
	w.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 1 })
	w.until(10*time.Minute, 10*time.Second, "one Instance drained", func() bool { return len(w.liveInstances(hoMD)) == 1 })
	for _, in := range live {
		if w.endedOrGone(in.Name) {
			kept = in.Spec.AllocationRef.Name
		}
	}
	a, ok := w.allocation(kept)
	if !ok || a.Status.AtProvider.StartTime == nil || !a.DeletionTimestamp.IsZero() {
		t.Fatalf("the drained Instance's Allocation %q should be started and kept: %+v %v", kept, a, ok)
	}
	return w, kept
}

// An approved Release deletes its Allocations, each with the Operation's cause, and nothing else.
func TestAnApprovedReleaseDeletesItsAllocation(t *testing.T) {
	w, kept := newKeptWorld(t)
	var serving string
	for _, in := range w.liveInstances(hoMD) {
		serving = in.Spec.AllocationRef.Name
	}
	keptUID := w.allocOrNil(kept).UID
	w.plan("release", releaseSpec(kept))
	w.confirm("release")
	w.untilPhase("release", v1alpha1.OperationPhaseAwaitingApproval)
	if got := w.getOp("release").Status.Phase; got != v1alpha1.OperationPhaseAwaitingApproval {
		t.Fatalf("phase %q: releasing an Allocation its retention keeps needs an approval", got)
	}
	w.approve("release", "person", userPerson)
	w.untilPhase("release", v1alpha1.OperationPhaseDone)
	op := w.getOp("release")
	if len(op.Status.Committed) != 1 || op.Status.Committed[0].UID != keptUID || op.Status.Committed[0].Effect != v1alpha1.PlanEffectReleaseKept {
		t.Errorf("committed %+v, want the kept Allocation %s, ReleaseKept", op.Status.Committed, keptUID)
	}
	if _, ok := w.allocation(kept); ok {
		t.Error("the Allocation still exists")
	}
	w.wantCause(kept, op)
	if _, ok := w.log.deletedWith(serving); ok {
		t.Error("the serving Allocation was deleted")
	}
	if got := w.md(hoMD).Spec.Instances; got != 1 {
		t.Errorf("spec.instances is %d: a Release writes no spec", got)
	}
}
