package lifecycle

// What an Executing Operation holds back (HBEP-7, Reconcile loop, step 7: "While an Operation is Executing ..., the scale-down
// takes that Operation's committed targets instead of choosing its own"; #589, PR 5, the review's fix 3). Beside an Operation of
// ANY action step 7 makes no selection of its own; nothing is placed on an Allocation an Operation committed to release; and a
// committed Instance target gets no replacement. Provisioning a missing primary goes on.

import (
	"errors"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// newKeptThreeWorld is a deployment that serves two ready Instances and keeps one idle, started Allocation, kept (kA): three
// ready Instances on A, started five minutes apart, and a person lowers the count to two, so the one with the nearest lease end
// drains and ends and its Allocation stays.
func newKeptThreeWorld(t *testing.T) (w *execWorld, serving [2]string, kept string) {
	t.Helper()
	w, insts, allocs := newThreeWorld(t)
	w.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 2 })
	w.until(10*time.Minute, 10*time.Second, "one Instance drained and ended", func() bool { return len(w.liveInstances(hoMD)) == 2 })
	n := 0
	for i, name := range insts {
		if w.endedOrGone(name) {
			kept = allocs[i]
		} else {
			serving[n] = name
			n++
		}
	}
	if a, ok := w.allocation(kept); !ok || a.Status.AtProvider.StartTime == nil || !a.DeletionTimestamp.IsZero() {
		t.Fatalf("the drained Instance's Allocation %q should be started and kept: %+v %v", kept, a, ok)
	}
	return w, serving, kept
}

// failAllocationWrites makes every update of the Allocation fail with an error that says "injected" while on is set; the
// release cause's write is the first of a release, so a failing one leaves the Allocation in place.
func (w *execWorld) failAllocationWrites(name string, on *bool) {
	w.fc.set(func(method string, obj client.Object) error {
		if a, ok := obj.(*v1alpha1.Allocation); ok && a.Name == name && method == "Update" && *on {
			return errors.New("injected: the write of the Allocation fails")
		}
		return nil
	}, nil)
}

// Beside an Executing Release whose delete cannot be made, a person lowers the count: step 7 takes no victim of its own. When
// the Release is done the selection runs again: the control, which shows the lowered count really asks for a drain.
func TestAnExecutingReleaseHoldsBackTheControllersOwnScaleDown(t *testing.T) {
	w, serving, kept := newKeptThreeWorld(t)
	failing := true
	w.failAllocationWrites(kept, &failing)
	w.plan("release", releaseSpec(kept))
	w.confirm("release")
	w.untilPhase("release", v1alpha1.OperationPhaseAwaitingApproval)
	w.approve("release", "person", userPerson)
	w.stepsUntil(time.Minute, 10*time.Second, "the commit", func() bool { return len(w.getOp("release").Status.Committed) == 1 })

	w.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 1 })
	// A pass of the deployment's key runs after the lowered count exists: the sentinel reads Planned only then. The passes
	// fail on the Release's injected error, which the sentinel's wait tolerates by stepping through stepKeepingErrors.
	w.t.Helper()
	w.planOnTolerant(hoMD, "sentinel", endSpec(false))
	for _, n := range serving {
		if in := w.instance(n); isDraining(in) || in.Status.Phase != v1alpha1.InstanceReady {
			t.Errorf("Instance %s is %q beside an Executing Release: step 7's own selection must take no victim", n, in.Status.Phase)
		}
	}
	if got := w.batch.Count("scancel"); got != 0 {
		t.Errorf("%d scancel calls beside the stuck Release", got)
	}
	// The Release's delete can be made now; once it is done, the selection resumes and takes one of the two.
	failing = false
	w.untilPhase("release", v1alpha1.OperationPhaseDone)
	w.stepsUntil(10*time.Minute, 10*time.Second, "the lowered count taking a victim", func() bool {
		return len(w.liveInstances(hoMD)) == 1
	})
}

// planOnTolerant is planOn for a world whose controller's passes fail with an injected error: it steps through
// stepKeepingErrors, which tolerates it, until the Operation reads Planned.
func (w *execWorld) planOnTolerant(md, name string, spec v1alpha1.OperationSpec) {
	w.t.Helper()
	op := opRigOp(name, spec)
	op.Spec.ModelDeploymentRef.Name = md
	w.create(op)
	w.stepsUntil(time.Minute, 10*time.Second, name+" planned", func() bool { return w.getOp(name).Status.Phase == v1alpha1.OperationPhasePlanned })
}

// An Executing Release's Allocation, whose delete fails in the very pass in which a primary is missing, is not warm capacity:
// nothing is placed on it. A new Allocation is made for the missing primary instead.
func TestNothingIsPlacedOnAnAllocationAnExecutingReleaseCommittedToRelease(t *testing.T) {
	w, serving, kept := newKeptThreeWorld(t)
	keptUID := w.allocOrNil(kept).UID
	failing := true
	w.failAllocationWrites(kept, &failing)
	w.plan("release", releaseSpec(kept))
	w.confirm("release")
	w.untilPhase("release", v1alpha1.OperationPhaseAwaitingApproval)
	// A serving Instance's job is cancelled from outside, so a primary is missing in the pass that commits the Release.
	lost := w.allocOrNil(w.instance(serving[1]).Spec.AllocationRef.Name)
	w.batch.CancelExternally(lost.Annotations[v1alpha1.AnnotationJobID])
	sbatches := w.submissions()
	w.approve("release", "person", userPerson)
	w.stepsUntil(5*time.Minute, 10*time.Second, "a replacement primary placed", func() bool { return w.submissions() > sbatches })
	for _, in := range w.liveInstances(hoMD) {
		if a := w.allocOrNil(in.Spec.AllocationRef.Name); a != nil && a.UID == keptUID {
			t.Errorf("Instance %s was placed on Allocation %s, which the Executing Release committed to release", in.Name, kept)
		}
	}
	failing = false
	w.untilPhase("release", v1alpha1.OperationPhaseDone)
	if _, ok := w.allocation(kept); ok {
		t.Error("the committed Allocation still exists after the Release is Done")
	}
}

// A Scale whose drain write failed: the committed Instance is a target of the Operation, and step 8 makes it no replacement,
// even when the selection has since chosen it as a primary and its replacement is due.
func TestACommittedInstanceTargetGetsNoReplacement(t *testing.T) {
	w, insts, allocs := newTwoWorld(t)
	r1 := insts[0]
	// A Scale to one takes R1, the nearest lease end, as a drain of an Instance while another stays ready.
	planned := w.plan("scale", scaleTo(1))
	if got := planTargets(planned.Status.Plan)["Instance "+r1]; got != v1alpha1.PlanEffectDrainInstance {
		t.Fatalf("the plan takes R1 as %q: %v", got, planTargets(planned.Status.Plan))
	}
	w.fc.set(func(method string, obj client.Object) error {
		if in, ok := obj.(*v1alpha1.Instance); ok && in.Name == r1 && isDraining(in) {
			return errors.New("injected: the write that drains R1 fails")
		}
		return nil
	}, nil)
	w.confirm("scale") // no target needs an approval: it commits at once, with R1's drain failing from now on
	w.stepsUntil(time.Minute, 10*time.Second, "the commit", func() bool { return len(w.getOp("scale").Status.Committed) > 0 })
	// R2's limit is cut to three hours and R1's is as it was, four from its start: R1 is the better primary now, and its
	// replacement is due as its lease nears its end.
	w.batch.SetTimeLimit(w.allocOrNil(allocs[1]).Annotations[v1alpha1.AnnotationJobID], 3*time.Hour)
	known := map[string]bool{}
	for _, in := range w.instances(hoMD) {
		known[in.Name] = true
	}
	w.stepsUntil(4*time.Hour, 2*time.Minute, "R1's replacement time passed", func() bool {
		in := w.instance(r1)
		return in.Status.LeaseEnd != nil && w.clock.Now().After(in.Status.LeaseEnd.Add(-20*time.Minute))
	})
	if w.instance(r1).Status.Phase != v1alpha1.InstanceReady || isDraining(w.instance(r1)) {
		t.Fatalf("R1 is %q: its drain write fails, so it is Ready and not draining", w.instance(r1).Status.Phase)
	}
	// R2 ended at its own hard end, three hours in; the deployment has one primary, R1, whose replacement would be due, and an
	// Operation that takes R1: no replacement is made for it.
	for _, in := range w.instances(hoMD) {
		if in.Annotations[v1alpha1.AnnotationPredecessor] == r1 {
			t.Errorf("Instance %s was made as the replacement of R1, a committed target of the Executing Scale", in.Name)
		}
	}
	for _, in := range w.instances(hoMD) {
		if !known[in.Name] {
			t.Errorf("Instance %s was made beside the Executing Scale: nothing is made for a committed target", in.Name)
		}
	}
}
