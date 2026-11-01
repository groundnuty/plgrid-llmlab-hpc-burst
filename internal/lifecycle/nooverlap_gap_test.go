package lifecycle

// The one rule for NoOverlapSlot (HBEP-7, Handover, No-overlap mode; Events and
// conditions): it shows from the predecessor's drain start until a successor
// is ready, and at no other time. These tests use the timeline of
// nooverlap_test.go: the primary's drain start is 09:50:20; on the queued path
// its successor is created at 09:19:30 and pends on the GPU cap until the
// primary's job ends.
//   - Not before the drain start: a primary that fails first is a failure.
//   - Through a successor's failure and its retries.
//   - Through a controller that stops between the predecessor's clean-up and
//     the successor's creation.

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// successorReady reports whether a successor of a primary that drained in the
// mode (marked as one) has been ready, which ends the gap.
func successorReady(rg *rig) bool {
	for _, in := range rg.instances(hoMD) {
		if in.Annotations[annotationNoOverlapSuccessor] != "" && in.Status.ReadyTime != nil {
			return true
		}
	}
	return false
}

// TestNoOverlapPredecessorFailsBeforeDrainStart: on the queued path, the
// primary's job is cancelled from outside at 09:30:00, before its drain start.
// The queued successor already exists, but there is no planned gap: the
// deployment has CapacityDropped, never NoOverlapSlot, from the failure until
// the successor is ready. Without the rule, the successor's mark alone raised
// NoOverlapSlot from the failure, though the primary never drained.
func TestNoOverlapPredecessorFailsBeforeDrainStart(t *testing.T) {
	rg := noOverlapQueuedRig(t)
	p := noOverlapStart(rg)
	hoStepTo(rg, hoT(9, 19, 20), 5*time.Minute, nil)
	hoStepTo(rg, hoT(9, 30, 0), 10*time.Second, nil)
	if len(hoRepl(rg, p.Name)) != 1 {
		t.Fatal("no queued successor at 09:30:00")
	}
	if c := rg.condition(hoMD, v1alpha1.ConditionDegraded); c.Reason != v1alpha1.ReasonAsDesired {
		t.Fatalf("at 09:30:00 Degraded = %s/%s %q, want AsDesired", c.Status, c.Reason, c.Message)
	}
	pa, _ := rg.allocation(p.Spec.AllocationRef.Name)
	rg.batch.CancelExternally(pa.Annotations[v1alpha1.AnnotationJobID])

	sawCapacityDropped := false
	hoStepUntil(rg, hoT(10, 0, 0), 10*time.Second, func() bool { return successorReady(rg) }, func() {
		c := rg.condition(hoMD, v1alpha1.ConditionDegraded)
		if c.Reason == v1alpha1.ReasonNoOverlapSlot {
			t.Fatalf("at %s Degraded = %s/%s %q: the primary failed before its drain start, which is no planned gap", rg.clock.Now().Format(time.TimeOnly), c.Status, c.Reason, c.Message)
		}
		if c.Reason == v1alpha1.ReasonCapacityDropped {
			sawCapacityDropped = true
		}
	})
	if !sawCapacityDropped {
		t.Fatal("the failed primary never showed CapacityDropped")
	}
	if c := rg.condition(hoMD, v1alpha1.ConditionDegraded); c.Status == "True" {
		t.Fatalf("Degraded = %s/%s %q once the successor is ready, want False", c.Status, c.Reason, c.Message)
	}
}

// TestNoOverlapGapSurvivesASuccessorFailure: on the queued path, the successor
// starts when the primary's job has ended and fails 10 s later, before it is
// ready. The gap lasts until a successor is ready, so the condition stays
// NoOverlapSlot through the failure and the retry, which is a new Instance
// carrying both marks and the failed one as its predecessor. Without the
// marks on the retry, NoOverlapSlot dropped to CapacityDropped for the retry's
// whole start.
func TestNoOverlapGapSurvivesASuccessorFailure(t *testing.T) {
	rg := noOverlapQueuedRig(t)
	failAfter := 10 * time.Second
	// The primary's job, the queued successor (fails 10 s into its run), the retry.
	rg.batch.Enqueue(pfake.Script{}, pfake.Script{FailAfter: &failAfter})
	p := noOverlapStart(rg)
	hoStepTo(rg, hoT(9, 19, 20), 5*time.Minute, nil)
	hoStepTo(rg, hoT(9, 19, 40), 10*time.Second, nil)
	reps := hoRepl(rg, p.Name)
	if len(reps) != 1 {
		t.Fatalf("%d successors at 09:19:40, want 1", len(reps))
	}
	failed := reps[0]

	hoStepTo(rg, hoT(9, 50, 20), 5*time.Minute, nil)
	var retry *v1alpha1.Instance
	failedWithoutReady := false
	gapSteps := 0
	hoStepUntil(rg, hoT(10, 30, 0), 10*time.Second, func() bool { return successorReady(rg) }, func() {
		now := rg.clock.Now()
		if rg.readyCount(hoMD) == 0 {
			gapSteps++
			c := rg.condition(hoMD, v1alpha1.ConditionDegraded)
			if c.Reason != v1alpha1.ReasonNoOverlapSlot {
				t.Fatalf("at %s in the gap Degraded = %s/%s %q, want NoOverlapSlot until a successor is ready", now.Format(time.TimeOnly), c.Status, c.Reason, c.Message)
			}
		}
		for _, in := range rg.instances(hoMD) {
			if in.Name == failed.Name && in.Status.Phase == v1alpha1.InstanceEnded && in.Status.ReadyTime == nil {
				failedWithoutReady = true
			}
			if in.Annotations[v1alpha1.AnnotationPredecessor] == failed.Name {
				retry = &in
			}
		}
	})
	if gapSteps == 0 {
		t.Fatal("no step in the gap was checked")
	}
	if !failedWithoutReady {
		t.Fatalf("the first successor %s was never seen ended without having been ready; the test proves nothing", failed.Name)
	}
	if retry == nil {
		t.Fatal("no retry named the failed successor as its predecessor")
	}
	if retry.Annotations[annotationNoOverlapSuccessor] == "" || retry.Annotations[annotationNoOverlapGap] != p.Name {
		t.Fatalf("the retry carries successor mark %q and gap %q, want both, the gap naming %s", retry.Annotations[annotationNoOverlapSuccessor], retry.Annotations[annotationNoOverlapGap], p.Name)
	}
	if retry.Status.Phase != v1alpha1.InstanceReady {
		t.Fatalf("the retry is %s, want ready", retry.Status.Phase)
	}
	if c := rg.condition(hoMD, v1alpha1.ConditionDegraded); c.Status == "True" {
		t.Fatalf("Degraded = %s/%s %q once the retry is ready, want False", c.Status, c.Reason, c.Message)
	}
}

// createKiller models a controller that stops right before it creates an
// Instance that names a predecessor (a successor or a retry): the create does
// not happen and the pass returns an error. It fires once.
type createKiller struct {
	mu    sync.Mutex
	armed bool
	fired int
}

func (k *createKiller) funcs() interceptor.Funcs {
	return interceptor.Funcs{Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
		k.mu.Lock()
		defer k.mu.Unlock()
		if in, ok := obj.(*v1alpha1.Instance); ok && k.armed && in.Annotations[v1alpha1.AnnotationPredecessor] != "" {
			k.armed = false
			k.fired++
			return errHOKilled
		}
		return c.Create(ctx, obj, opts...)
	}}
}

// stepTolerant is rg.Step for the deployment alone, with the pass's error
// returned to the test instead of failing it.
func stepTolerant(rg *rig) error {
	rg.t.Helper()
	if err := rg.prov.Sync(rg.ctx); err != nil {
		rg.t.Fatalf("provider sync: %v", err)
	}
	rg.syncAgents()
	_, err := rg.r.Reconcile(rg.ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: rigNS, Name: hoMD}})
	return err
}

// diesBeforeTheSuccessor runs the one-slot path until the controller stops in
// the pass in which the primary's Allocation is gone, right before it creates
// the successor. It returns the rig, with the controller not yet restarted,
// and the primary.
func diesBeforeTheSuccessor(t *testing.T) (*rig, *v1alpha1.Instance) {
	k := &createKiller{}
	rg := noOverlapRig(t, oneSlotOpts(withInterceptors(k.funcs()), withBatchConfig(func(c *pfake.Config) { c.MaxSubmitPerUser, c.MaxGPUsPerUser = 1, 1 }))...)
	p := noOverlapStart(rg)
	pAlloc := p.Spec.AllocationRef.Name
	hoStepTo(rg, hoT(9, 50, 30), 5*time.Minute, nil)
	if c := rg.condition(hoMD, v1alpha1.ConditionDegraded); c.Reason != v1alpha1.ReasonNoOverlapSlot {
		t.Fatalf("at 09:50:30 Degraded = %s/%s %q, want NoOverlapSlot", c.Status, c.Reason, c.Message)
	}
	k.mu.Lock()
	k.armed = true
	k.mu.Unlock()
	// Step by hand, so the error of the pass that dies is the test's to see.
	died := false
	for !died {
		if !rg.clock.Now().Before(hoT(10, 30, 0)) {
			t.Fatal("the controller never reached the successor's creation by 10:30:00")
		}
		rg.clock.Advance(10 * time.Second)
		switch err := stepTolerant(rg); {
		case err == nil:
		case errors.Is(err, errHOKilled):
			died = true
		default:
			t.Fatalf("reconcile at %s: %v", rg.clock.Now().Format(time.TimeOnly), err)
		}
	}
	if k.fired != 1 {
		t.Fatalf("the controller died %d times, want once", k.fired)
	}
	if _, alive := rg.allocation(pAlloc); alive {
		t.Fatalf("the controller died while the primary's Allocation %s still existed; the test needs the pass after its clean-up", pAlloc)
	}
	if n := len(hoRepl(rg, p.Name)); n != 0 {
		t.Fatalf("%d successors exist when the controller died before creating one", n)
	}
	return rg, p
}

// TestNoOverlapGapSurvivesARestartBeforeTheSuccessor: on the one-slot path the
// controller stops in the pass in which the predecessor's Allocation is gone,
// right before it creates the successor, and starts again. The successor is
// created marked, and NoOverlapSlot holds on every step until it is ready.
// Without the hold on the predecessor's Instance, it was deleted in that
// pass, and the restarted controller created an unmarked successor under
// CapacityDropped.
func TestNoOverlapGapSurvivesARestartBeforeTheSuccessor(t *testing.T) {
	rg, p := diesBeforeTheSuccessor(t)
	rg.restartController()

	steps := 0
	hoStepUntil(rg, hoT(10, 30, 0), 10*time.Second, func() bool { return successorReady(rg) }, func() {
		steps++
		if rg.readyCount(hoMD) == 0 {
			if c := rg.condition(hoMD, v1alpha1.ConditionDegraded); c.Reason != v1alpha1.ReasonNoOverlapSlot {
				t.Fatalf("at %s after the restart Degraded = %s/%s %q, want NoOverlapSlot until the successor is ready", rg.clock.Now().Format(time.TimeOnly), c.Status, c.Reason, c.Message)
			}
		}
	})
	if steps == 0 {
		t.Fatal("no step after the restart was checked")
	}
	reps := hoRepl(rg, p.Name)
	if len(reps) != 1 || reps[0].Annotations[annotationNoOverlapSuccessor] == "" || reps[0].Annotations[annotationNoOverlapGap] != p.Name {
		t.Fatalf("the primary's successors %+v, want one, marked as a no-overlap successor with the gap begun", reps)
	}
}

// TestNoOverlapGapHoldEndsWithTheDeployment: the hold that keeps a gap's
// predecessor until its successor exists is for a successor that will be
// created. When the deployment is scaled to zero instead, the predecessor's
// Instance is deleted and no successor is made; otherwise it would stay for
// ever and the controller would never stop passes over it.
func TestNoOverlapGapHoldEndsWithTheDeployment(t *testing.T) {
	rg, p := diesBeforeTheSuccessor(t)
	rg.restartController()
	if rg.instancesByName()[p.Name] == nil {
		t.Fatal("the primary's Instance is already gone; the test needs it held")
	}
	rg.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 0 })
	hoStepUntil(rg, hoT(10, 30, 0), 10*time.Second, func() bool { return len(rg.instances(hoMD)) == 0 }, nil)
	if n := len(rg.allAllocations()); n != 0 {
		t.Fatalf("%d Allocations after the deployment was scaled to zero, want none", n)
	}
}
