package lifecycle

// An Allocation whose job started after an Operation committed to cancel it as queued is KEPT WARM (HBEP-10, The report, as
// amended by #698): the controller marks it (AnnotationKeptWarm) before it records the target's final failure, and no rule
// but the profile's maxIdle, its hard end, HBEP-7's unconditional releases, a person's direct delete of its deployment and a new
// Operation's approved Release releases it. An End with delete holds the deployment with a finalizer until no Allocation of it
// carries the mark. Each row makes an End of a world in which P's job starts after the commit and before the spec write lands.

import (
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// keptWarmWorld is newExecWorld (R ready, P pending behind a 1-hour queue), an End planned, confirmed and approved while the spec
// write fails, with P's job started before it lands. stop lets the write through.
func keptWarmWorld(t *testing.T, del bool, opts ...rigOption) (w *execWorld, pAlloc string, stop func()) {
	t.Helper()
	w = newExecWorld(t, time.Hour, opts...)
	pAlloc = w.pendingAlloc
	stop = w.failSpecWrites()
	planned := w.plan("end", endSpec(del))
	if pt := planTargets(planned.Status.Plan); pt["Allocation "+pAlloc] != v1alpha1.PlanEffectCancelPending {
		t.Fatalf("the plan takes P's Allocation as %q: %v", pt["Allocation "+pAlloc], pt)
	}
	w.confirm("end")
	w.untilPhase("end", v1alpha1.OperationPhaseAwaitingApproval)
	w.approve("end", "person", userPerson)
	w.stepsUntil(time.Minute, 10*time.Second, "the commit", func() bool { return len(w.getOp("end").Status.Committed) > 0 })
	w.stepsUntil(90*time.Minute, 2*time.Minute, "P's job started", func() bool {
		a := w.allocOrNil(pAlloc)
		return a != nil && a.Status.AtProvider.StartTime != nil
	})
	return w, pAlloc, stop
}

// releaseDone runs passes until the Operation is Done, and fails with its status when it is not within five minutes.
func (w *execWorld) releaseDone(name string) {
	w.t.Helper()
	for i := 0; i < 30; i++ {
		if w.getOp(name).Status.Phase == v1alpha1.OperationPhaseDone {
			return
		}
		w.clock.Advance(10 * time.Second)
		if err := w.stepKeepingErrors(); err != nil {
			w.t.Logf("pass %d: %v", i, err)
		}
	}
	op := w.getOp(name)
	w.t.Fatalf("Operation %s is %q after five minutes: conditions %+v, committed %d, targets %+v", name, op.Status.Phase, op.Status.Conditions, len(op.Status.Committed), op.Status.Targets)
}

func (w *execWorld) deployment() (*v1alpha1.ModelDeployment, bool) {
	var md v1alpha1.ModelDeployment
	err := w.c.Get(w.ctx, client.ObjectKey{Namespace: rigNS, Name: hoMD}, &md)
	if apierrors.IsNotFound(err) {
		return nil, false
	}
	if err != nil {
		w.t.Fatal(err)
	}
	return &md, true
}

// A started-since-commit Allocation of an End is marked, with the End in the mark, and is never released by any step of the
// End's passes: not by the shutdown, not by retention's keep rules (the deployment's end time the End wrote).
func TestAnEndLeavesAStartedSinceCommitAllocationKeptWarm(t *testing.T) {
	w, pAlloc, stop := keptWarmWorld(t, false)
	stop()
	for i := 0; i < 90; i++ { // 45 minutes of passes
		w.clock.Advance(30 * time.Second)
		if err := w.stepKeepingErrors(); err != nil {
			t.Fatalf("pass %d: %v", i, err)
		}
	}
	a, ok := w.allocation(pAlloc)
	if !ok || !a.DeletionTimestamp.IsZero() {
		t.Fatalf("P's Allocation is %+v (found %v): kept warm, nothing releases it", a, ok)
	}
	op := w.getOp("end")
	mark := a.Annotations[v1alpha1.AnnotationKeptWarm]
	if ns, name, uid, ok := v1alpha1.ParseReleaseCauseOperation(mark); !ok || ns != op.Namespace || name != op.Name || uid != op.UID {
		t.Errorf("the mark is %q, want the End %s/%s %s in the release cause's form", mark, op.Namespace, op.Name, op.UID)
	}
	if o, ok := outcomeOf(op, pAlloc); !ok || o.State != v1alpha1.TargetStateFailed || o.Reason != reasonJobStartedSinceCommit || !strings.Contains(o.Message, "kept warm") {
		t.Errorf("P's Allocation outcome is %+v (%v), want Failed with %s and the words kept warm", o, ok, reasonJobStartedSinceCommit)
	}
	if in := w.instanceOrNil(w.pending); in != nil && !ended(in) && !isDraining(in) {
		t.Errorf("P's Instance is %q: under an End its Instance drains, though its Allocation is kept", in.Status.Phase)
	}
}

// 1 (ruled). An End with delete over a started-since-commit Allocation: the deployment stays deleting, with the finalizer held, until
// the profile's maxIdle releases the Allocation, and, in a second row, until a person's approved Release completes it.
func TestAnEndWithDeleteHoldsTheDeploymentWhileAnAllocationIsKeptWarm(t *testing.T) {
	held := func(t *testing.T, w *execWorld, pAlloc string) {
		t.Helper()
		md, ok := w.deployment()
		if !ok || md.DeletionTimestamp.IsZero() || !controllerutil.ContainsFinalizer(md, v1alpha1.FinalizerKeptWarm) {
			t.Fatalf("the deployment is %+v (found %v), want it deleting and holding the kept-warm finalizer", md, ok)
		}
		if a, ok := w.allocation(pAlloc); !ok || !a.DeletionTimestamp.IsZero() || a.Annotations[v1alpha1.AnnotationKeptWarm] == "" {
			t.Fatalf("P's Allocation is %+v (found %v), want it kept warm", a, ok)
		}
	}
	t.Run("until maxIdle releases it", func(t *testing.T) {
		w, pAlloc, stop := keptWarmWorld(t, true, withProfile(func(p *v1alpha1.PlatformProfileSpec) { p.MaxIdle = dur(30 * time.Minute) }))
		stop()
		for i := 0; i < 20; i++ { // 10 minutes: P's drain ends, the Allocation is idle, and maxIdle has not run
			w.clock.Advance(30 * time.Second)
			if err := w.stepKeepingErrors(); err != nil {
				t.Fatalf("pass %d: %v", i, err)
			}
		}
		held(t, w, pAlloc)
		if o, ok := outcomeOf(w.getOp("end"), pAlloc); !ok || !strings.Contains(o.Message, "kept warm") {
			t.Errorf("the report's reason for the hold is %+v (%v), want the kept-warm words", o, ok)
		}
		w.stepsUntil(2*time.Hour, 30*time.Second, "maxIdle releases the Allocation", func() bool { _, ok := w.allocation(pAlloc); return !ok })
		cause, _ := w.log.deletedWith(pAlloc)
		if !strings.Contains(cause, "maxIdle") {
			t.Errorf("the Allocation was released with the cause %q, want maxIdle's", cause)
		}
		w.stepsUntil(5*time.Minute, 30*time.Second, "the deployment gone", func() bool { _, ok := w.deployment(); return !ok })
	})
	t.Run("until a person's approved Release completes it", func(t *testing.T) {
		w, pAlloc, stop := keptWarmWorld(t, true)
		stop()
		for i := 0; i < 90; i++ {
			w.clock.Advance(30 * time.Second)
			if err := w.stepKeepingErrors(); err != nil {
				t.Fatalf("pass %d: %v", i, err)
			}
		}
		held(t, w, pAlloc)
		w.plan("rel", releaseSpec(pAlloc)) // a Release is planned on a deployment that is being deleted, which holds the finalizer
		w.confirm("rel")
		w.untilPhase("rel", v1alpha1.OperationPhaseAwaitingApproval)
		w.approve("rel", "person", userPerson)
		w.releaseDone("rel")
		cause, deleted := w.log.deletedWith(pAlloc)
		if rel := w.getOp("rel"); !deleted || cause != operationCauseOf(rel) {
			t.Errorf("the Allocation was deleted (%v) with the cause %q, want the Release's %q", deleted, cause, operationCauseOf(rel))
		}
		w.stepsUntil(5*time.Minute, 10*time.Second, "the deployment gone", func() bool { _, ok := w.deployment(); return !ok })
	})
}

// 2. An End with delete with NO started target completes promptly: the finalizer is added, then removed, and the deployment is
// gone, so the unconditional add never strands a deployment.
func TestAnEndWithDeleteWithNoStartedTargetCompletesPromptly(t *testing.T) {
	w := newExecWorld(t, 10*time.Hour) // P stays pending: nothing is kept warm
	w.plan("end", endSpec(true))
	w.confirm("end")
	w.untilPhase("end", v1alpha1.OperationPhaseAwaitingApproval)
	w.approve("end", "person", userPerson)
	sawFinalizer := false
	w.fc.set(nil, func(method string, obj client.Object) {
		if md, ok := obj.(*v1alpha1.ModelDeployment); ok && method == "Update" && controllerutil.ContainsFinalizer(md, v1alpha1.FinalizerKeptWarm) {
			sawFinalizer = true
		}
	})
	for i := 0; i < 6; i++ {
		w.clock.Advance(10 * time.Second)
		if err := w.stepKeepingErrors(); err != nil {
			t.Fatalf("pass %d: %v", i, err)
		}
		if _, ok := w.deployment(); !ok {
			break
		}
	}
	if !sawFinalizer {
		t.Error("the finalizer was never added: it is added at the End's commit, before its delete")
	}
	if md, ok := w.deployment(); ok {
		t.Fatalf("the deployment is still there after six passes: deleting %v, finalizers %v", !md.DeletionTimestamp.IsZero(), md.Finalizers)
	}
}

// 3. A controller restart while the finalizer is held: the finalizer is still removed once the last mark clears.
func TestAControllerRestartWhileTheKeptWarmFinalizerIsHeldStillRemovesIt(t *testing.T) {
	w, pAlloc, stop := keptWarmWorld(t, true)
	stop()
	for i := 0; i < 90; i++ {
		w.clock.Advance(30 * time.Second)
		if err := w.stepKeepingErrors(); err != nil {
			t.Fatalf("pass %d: %v", i, err)
		}
	}
	if md, ok := w.deployment(); !ok || !controllerutil.ContainsFinalizer(md, v1alpha1.FinalizerKeptWarm) {
		t.Fatalf("the deployment is %+v (found %v), want it held", md, ok)
	}
	w.restart(false)
	w.plan("rel", releaseSpec(pAlloc))
	w.confirm("rel")
	w.untilPhase("rel", v1alpha1.OperationPhaseAwaitingApproval)
	w.approve("rel", "person", userPerson)
	w.releaseDone("rel")
	w.stepsUntil(5*time.Minute, 10*time.Second, "the deployment gone", func() bool { _, ok := w.deployment(); return !ok })
}

// 4. A person's direct delete of a deployment (it has no finalizer) releases a kept-warm Allocation at once, with retention's cause,
// which is not an Operation's: a person's delete is a person's act, outside the guard by design.
func TestAPersonsDirectDeleteReleasesAKeptWarmAllocationWithRetentionsCause(t *testing.T) {
	w, pAlloc, stop := keptWarmWorld(t, false) // an End without delete: the mark, and no finalizer
	stop()
	for i := 0; i < 6; i++ {
		w.clock.Advance(30 * time.Second)
		if err := w.stepKeepingErrors(); err != nil {
			t.Fatalf("pass %d: %v", i, err)
		}
	}
	if a, ok := w.allocation(pAlloc); !ok || a.Annotations[v1alpha1.AnnotationKeptWarm] == "" || !a.DeletionTimestamp.IsZero() {
		t.Fatalf("P's Allocation is %+v (found %v), want it marked and not released", a, ok)
	}
	md, ok := w.deployment()
	if !ok || controllerutil.ContainsFinalizer(md, v1alpha1.FinalizerKeptWarm) {
		t.Fatalf("the deployment is %+v (found %v), want it present with no kept-warm finalizer: an End without delete adds none", md, ok)
	}
	if err := w.c.Delete(w.ctx, md); err != nil { // a person's delete
		t.Fatal(err)
	}
	w.stepsUntil(10*time.Minute, 10*time.Second, "P's Allocation released", func() bool { _, ok := w.allocation(pAlloc); return !ok })
	cause, _ := w.log.deletedWith(pAlloc)
	if ns, name, uid, ok := v1alpha1.ParseReleaseCauseOperation(cause); ok {
		t.Errorf("the cause is the Operation's, %q (%s/%s %s): a person's delete releases it with retention's cause", cause, ns, name, uid)
	}
	if !strings.Contains(cause, "deleted") {
		t.Errorf("the cause is %q, want retention's: its deployment was deleted", cause)
	}
}
