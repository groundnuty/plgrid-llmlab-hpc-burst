package lifecycle

// A hedge among an Executing Operation's committed targets gets no member, through the pass's own Operations (HBEP-7,
// Hedging (beta), Interactions, Operations; frozenByOperation wired as hedgeFrozen's default, hedge_grow.go), with no
// Reconciler.hedgeFrozen seam set. Once the Operation is finished the hedge grows as the hedge rule says.

import (
	"errors"
	"strings"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// newHedgeOpWorld is a deployment of two Instances on Providers A and B, A first: R, ready on A, and M1, pending on A behind
// 10 h, the newest, which a Scale to one takes as CancelPending. A's queue waits are 20 min, so M1's hedge is due its member
// on B 20 min after M1's submission (the hedge time, after #627's 10-minute floor). Every job on B pends behind 10 h.
func newHedgeOpWorld(t *testing.T) (w *execWorld, r, m1 string) {
	t.Helper()
	log := newCauseLog()
	h := newHedgeRig(t, withInterceptors(log.funcs()))
	h.batch.Enqueue(pfake.Script{}, pfake.Script{QueueWait: 10 * time.Hour}, pfake.Script{QueueWait: 10 * time.Hour})
	h.b.batch.Enqueue(pfake.Script{QueueWait: 10 * time.Hour}, pfake.Script{QueueWait: 10 * time.Hour})
	h.Step() // the Providers' first status: Ready
	setWaits(h.rig, rigProvider, 20, 20*time.Minute)
	md := noOverAllocation(mdSpec(hoMD))
	onBoth(md)
	md.Spec.Instances = 2
	h.create(md)
	pol := globalPolicy(person)
	h.create(&pol)
	h.until(5*time.Minute, 10*time.Second, "R ready and M1 pending", func() bool {
		return h.readyCount(hoMD) == 1 && len(h.liveInstances(hoMD)) == 2
	})
	for _, in := range h.liveInstances(hoMD) {
		if in.Status.Phase == v1alpha1.InstanceReady {
			r = in.Name
		} else {
			m1 = in.Name
		}
	}
	w = &execWorld{hedgeRig: h, log: log, fc: &faultClient{Client: h.r.Client}}
	h.r.Client = w.fc
	return w, r, m1
}

// A Scale to one commits to cancel M1, a hedge's only member so far, and M1's end does not reach the API server, so the
// Operation stays Executing (its target failed, retried) with M1 live. The hedge's time passes on the injected clock, minute
// by minute and well beyond #627's 10-minute floor, and at no minute does B get a member of M1's hedge. A person then raises the
// count, the committed effect no longer holds, the Operation is finished with M1 untouched, and the hedge, due its member
// since 20 min after M1's submission, gets it in the next pass. No Reconciler.hedgeFrozen is set: the pass's own Operations
// say it.
func TestAnExecutingOperationFreezesTheHedgeOfItsCommittedMemberAndTheFinishedOneReleasesIt(t *testing.T) {
	w, _, m1 := newHedgeOpWorld(t)
	if w.r.hedgeFrozen != nil {
		t.Fatal("the seam is set: this test is of the pass's own default")
	}
	m1Alloc := w.mustInstance(m1).Spec.AllocationRef.Name
	// M1's end is refused, as a write that does not reach the API server: only the write that ends it.
	w.fc.set(func(method string, obj client.Object) error {
		if in, ok := obj.(*v1alpha1.Instance); ok && in.Name == m1 && strings.HasPrefix(method, "Status.") && in.Status.Phase == v1alpha1.InstanceEnded {
			return errors.New("injected: M1's end does not reach the API server")
		}
		return nil
	}, nil)
	planned := w.plan("scale", scaleTo(1))
	if got := planTargets(planned.Status.Plan)["Instance "+m1]; got != v1alpha1.PlanEffectCancelPending {
		t.Fatalf("the plan takes M1 as %q: %v", got, planTargets(planned.Status.Plan))
	}
	w.confirm("scale")
	w.untilPhase("scale", v1alpha1.OperationPhaseAwaitingApproval)
	w.approve("scale", "person", userPerson)
	w.stepsUntil(2*time.Minute, 10*time.Second, "the commit", func() bool { return len(w.getOp("scale").Status.Committed) == 2 })
	w.stepsUntil(2*time.Minute, 10*time.Second, "M1's end refused and reported", func() bool {
		o, ok := outcomeOf(w.getOp("scale"), m1)
		return ok && o.State == v1alpha1.TargetStateFailed
	})
	a, ok := w.allocation(m1Alloc)
	if !ok || a.Status.AtProvider.SubmitTime == nil {
		t.Fatalf("M1's Allocation %+v (found %v) has no submission time", a, ok)
	}
	due := a.Status.AtProvider.SubmitTime.Add(20 * time.Minute) // the hedge time: A's waits
	if op := w.getOp("scale"); !isExecuting(op) {
		t.Fatalf("the Operation is %q finished %v, want it Executing or PartlyFailed and open", op.Status.Phase, op.Status.FinishedAt)
	}

	// The clock passes the hedge time by 15 min; at no minute does B have a member of M1's hedge.
	w.stepsUntil(due.Sub(w.clock.Now())+15*time.Minute, time.Minute, "the clock past the hedge time", func() bool {
		if n := len(w.membersOn(hoMD, m1, hedgeB)); n != 0 {
			t.Fatalf("at %s B has %d members of M1's hedge while the Operation holds it", w.clock.Now().Format(time.TimeOnly), n)
		}
		return w.clock.Now().After(due.Add(15 * time.Minute))
	})
	if in := w.instance(m1); ended(in) {
		t.Fatal("M1 has ended: its end does not reach the API server, the hedge must still hold it")
	}

	// A person raises the count: the effect the Operation committed to no longer holds, and it is finished, with M1 untouched.
	w.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 3 })
	w.stepsUntil(2*time.Minute, 10*time.Second, "the Operation finished", func() bool { return w.getOp("scale").Status.FinishedAt != nil })
	if in := w.instance(m1); ended(in) {
		t.Error("M1 has ended: a finished Operation acted on nothing more")
	}
	w.stepsUntil(2*time.Minute, 10*time.Second, "B's member of M1's hedge", func() bool { return len(w.membersOn(hoMD, m1, hedgeB)) == 1 })
}
