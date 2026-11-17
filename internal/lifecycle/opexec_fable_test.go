package lifecycle

// The delta review of #664 at 1a650eb6 by Fable (17 Nov 2026) added a split-pass trigger for the kept-warm finalizer's window, the
// requirement that the report agree with the cluster, and a ruling on the mark under a Scale above zero:
//   1. the finalizer, the delete and the mark do not always land in one pass: a pending Instance's end write can conflict with
//      the tunnel-state writer in the delete's pass, and the job starts before the next pass reaches step 6;
//   2. for every target the reported outcome is what happened to its Allocation;
//   3. under a Scale above zero the pair stays and serves, and no mark outlives the Operation on a serving Allocation.

import (
	"errors"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// reportAgreesWithTheCluster fails for each Allocation target whose reported outcome contradicts what happened to the Allocation:
// kept warm in the report means the Operation did not release it, so it still exists and is not being deleted; whether its mark is
// still there is not asked, since the report is historical and a later clearing of the mark does not change it (HBEP-10, The report,
// Kept warm). Done means it is gone or going, for a target the Operation committed to release.
func (w *execWorld) reportAgreesWithTheCluster(opName string) {
	w.t.Helper()
	for _, o := range w.getOp(opName).Status.Targets {
		if o.Kind != v1alpha1.TargetKindAllocation {
			continue
		}
		a, ok := w.allocation(o.Name)
		switch {
		case o.State == v1alpha1.TargetStateFailed && o.Reason == reasonJobStartedSinceCommit && strings.Contains(o.Message, "kept warm"):
			if !ok || !a.DeletionTimestamp.IsZero() {
				cause, deleted := w.log.deletedWith(o.Name)
				w.t.Errorf("the report says Allocation %s is kept warm, but it is %+v (found %v; released %v with the cause %q): the report and the act contradict each other", o.Name, a, ok, deleted, cause)
			}
		case o.State == v1alpha1.TargetStateDone && w.releasesAllocation(opName, o.UID):
			if ok && a.DeletionTimestamp.IsZero() {
				w.t.Errorf("the report says Allocation %s is Done, but it still exists and is not being deleted", o.Name)
			}
		}
	}
}

// releasesAllocation reports whether the Operation committed the Allocation target for a release: a target with another effect (Keep)
// is Done with its Allocation still there, since nothing was to be done to it.
func (w *execWorld) releasesAllocation(opName string, uid types.UID) bool {
	for _, t := range w.getOp(opName).Status.Committed {
		if t.UID == uid {
			return releasingEffect(t.Effect)
		}
	}
	return false
}

// 1 and 2. An End with delete. In the delete's pass the pending Instance's END write conflicts (injected, as the tunnel-state
// writer's write of the same status would), while the finalizer, the delete and the SpecWritten record land: the spec writes are
// NOT failed, so the finalizer and the delete are made in that one pass. P's job starts before the next pass, whose step 6 then
// ends the Instance and whose Operations step marks the Allocation. The Allocation is kept and the deployment is still
// deleting and held, and the report says what the cluster did.
func TestAnEndWithDeleteWhoseInstanceEndConflictsInTheDeletesPassKeepsAJobThatStartsLaterAndTheReportAgrees(t *testing.T) {
	w := newExecWorld(t, time.Hour)
	pAlloc := w.pendingAlloc
	endFails, conflicts := true, 0
	w.fc.set(func(method string, obj client.Object) error {
		if in, ok := obj.(*v1alpha1.Instance); ok && method == "Status.Update" && endFails && in.Name == w.pending && in.Status.Phase == v1alpha1.InstanceEnded {
			conflicts++
			return apierrors.NewConflict(schema.GroupResource{Group: v1alpha1.Group, Resource: "instances"}, in.Name, errors.New("injected: the tunnel-state writer wrote the status first"))
		}
		return nil
	}, nil)
	w.plan("end", endSpec(true))
	w.confirm("end")
	w.untilPhase("end", v1alpha1.OperationPhaseAwaitingApproval)
	w.approve("end", "person", userPerson)
	w.stepsUntil(time.Minute, 10*time.Second, "the commit", func() bool { return len(w.getOp("end").Status.Committed) > 0 })
	if a := w.allocOrNil(pAlloc); a == nil || a.Status.AtProvider.StartTime != nil {
		t.Fatalf("P's Allocation is %+v at the commit, want it queued", a)
	}

	// The delete's pass: P is still queued when it reads it.
	w.clock.Advance(10 * time.Second)
	if err := w.stepKeepingErrors(); err == nil && conflicts == 0 {
		t.Fatal("no Instance end was attempted in the delete's pass: the scenario did not arise")
	}
	if !apimeta.IsStatusConditionTrue(w.getOp("end").Status.Conditions, v1alpha1.ConditionSpecWritten) {
		t.Fatal("SpecWritten was not recorded in the delete's pass: the scenario did not arise")
	}

	// P's job starts before the next pass, which no longer conflicts.
	endFails = false
	w.clock.Advance(90 * time.Minute)
	w.passes(8, nil)
	if a := w.allocOrNil(pAlloc); a != nil && a.Status.AtProvider.StartTime == nil {
		t.Fatalf("P's Allocation is still queued: the scenario did not arise")
	}

	a, ok := w.allocation(pAlloc)
	if !ok || !a.DeletionTimestamp.IsZero() || !keptWarm(a) {
		cause, deleted := w.log.deletedWith(pAlloc)
		t.Errorf("P's started Allocation is %+v (found %v; released %v with the cause %q), want it marked and kept: the approval was to cancel a queued job", a, ok, deleted, cause)
	}
	if md, ok := w.deployment(); !ok || md.DeletionTimestamp.IsZero() || !controllerutil.ContainsFinalizer(md, v1alpha1.FinalizerKeptWarm) {
		t.Errorf("the deployment is %+v (found %v), want it deleting and held by the finalizer while P is kept warm", md, ok)
	}
	w.reportAgreesWithTheCluster("end")
}

// 3. Under a Scale above zero the pair is not taken and carries no mark: the Instance stays and serves, and when it later ends with
// no Operation (here the deployment's end time), its Allocation is released by retention as normal, with retention's cause, not an
// Operation's and not held for good by a mark.
func TestAnInstanceKeptUnderAScaleAboveZeroLaterEndsByItsEndTimeAndItsAllocationIsReleasedByRetention(t *testing.T) {
	w := newExecWorld(t, time.Hour)
	stop := w.failSpecWrites()
	w.approveScale("scale", 1, 2)
	w.stepsUntil(90*time.Minute, 2*time.Minute, "P ready", func() bool { return w.instance(w.pending).Status.Phase == v1alpha1.InstanceReady })
	stop()
	w.stepsUntil(5*time.Minute, 10*time.Second, "the Operation finished", func() bool { return w.getOp("scale").Status.FinishedAt != nil })
	if a, ok := w.allocation(w.pendingAlloc); !ok {
		t.Fatalf("P's Allocation is gone after the Scale: the case is not exercised")
	} else if keptWarm(a) {
		t.Errorf("P's Allocation carries the kept-warm mark %q after the Scale, want none: it serves, and the mark would outlive the Operation", a.Annotations[v1alpha1.AnnotationKeptWarm])
	}

	// No Operation is left. A person ends the deployment: both Instances drain, and retention releases their Allocations.
	w.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) {
		now := metav1.NewTime(w.clock.Now())
		m.Spec.EndTime = &now
	})
	released := false
	w.passes(240, func(int) bool { _, released = w.log.deletedWith(w.pendingAlloc); return released }) // up to two hours of 10 s passes
	cause, _ := w.log.deletedWith(w.pendingAlloc)
	if !released {
		t.Fatalf("P's Allocation was not released within two hours of passes after the deployment's end time: its Instance is %s and the Allocation is marked %v: a mark that outlived the Operation keeps retention from applying the end time", w.instanceState(w.pending), w.markedNow(w.pendingAlloc))
	}
	if ns, name, uid, ok := v1alpha1.ParseReleaseCauseOperation(cause); ok {
		t.Errorf("the cause is the Operation's, %q (%s/%s %s): retention releases it, with its own cause", cause, ns, name, uid)
	}
	if cause == "" {
		t.Error("the release carries no cause")
	}
}

// 3b. The ruled row. The pair kept under a Scale above zero serves; a PERSON later lowers the count, with no Operation involved,
// and the controller drains the pair's Instance (its lease end is the nearest). Its Allocation, which carries no mark, is released
// by its idle limit with retention's own cause, not held to its hard end by a mark that outlived the Operation.
func TestAnInstanceKeptUnderAScaleAboveZeroEndsWhenAPersonLowersTheCountAndItsAllocationIsReleasedByItsIdleLimit(t *testing.T) {
	w := newExecWorldWith(t, time.Hour, func(m *v1alpha1.ModelDeployment) {
		m.Spec.RetentionPolicy = &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle, IdleLimit: dur(10 * time.Minute)}
	})
	stop := w.failSpecWrites()
	w.approveScale("scale", 1, 2)
	w.stepsUntil(90*time.Minute, 2*time.Minute, "P ready", func() bool { return w.instance(w.pending).Status.Phase == v1alpha1.InstanceReady })
	stop()
	w.stepsUntil(5*time.Minute, 10*time.Second, "the Operation finished", func() bool { return w.getOp("scale").Status.FinishedAt != nil })
	if a, ok := w.allocation(w.pendingAlloc); !ok {
		t.Fatalf("P's Allocation is gone after the Scale: the case is not exercised")
	} else if keptWarm(a) {
		t.Errorf("P's Allocation carries the kept-warm mark %q after the Scale, want none: it serves, and the mark would outlive the Operation", a.Annotations[v1alpha1.AnnotationKeptWarm])
	}

	// R's lease end moves later, so that P's is the nearest; then a person lowers the count, and no Operation is involved.
	w.moveLeaseEnd(w.readyAlloc)
	w.until(time.Minute, 10*time.Second, "the controller to read R's moved lease end", func() bool {
		lr, lp := w.instance(w.ready).Status.LeaseEnd, w.instance(w.pending).Status.LeaseEnd
		return lr != nil && lp != nil && lr.After(lp.Time)
	})
	w.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 1 })
	released := false
	w.passes(360, func(int) bool { _, released = w.log.deletedWith(w.pendingAlloc); return released }) // up to an hour of 10 s passes
	cause, _ := w.log.deletedWith(w.pendingAlloc)
	if !released {
		t.Fatalf("P's Allocation was not released within an hour of passes after a person lowered the count: its Instance is %s and the Allocation is marked %v: a mark that outlived the Operation keeps retention from applying its idle limit", w.instanceState(w.pending), w.markedNow(w.pendingAlloc))
	}
	if ns, name, uid, ok := v1alpha1.ParseReleaseCauseOperation(cause); ok {
		t.Errorf("the cause is the Operation's, %q (%s/%s %s): retention's idle limit releases it, with its own cause", cause, ns, name, uid)
	}
	if !strings.Contains(cause, "idle") {
		t.Errorf("the cause is %q, want retention's idle limit", cause)
	}
}

// instanceState names the Instance's phase, or says it is gone.
func (w *execWorld) instanceState(name string) string {
	in := w.instanceOrNil(name)
	if in == nil {
		return "gone"
	}
	return string(in.Status.Phase)
}

// markedNow reports whether the Allocation exists and carries the kept-warm mark.
func (w *execWorld) markedNow(name string) bool {
	a, ok := w.allocation(name)
	return ok && keptWarm(a)
}
