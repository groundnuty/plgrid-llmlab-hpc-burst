package lifecycle

// The three notes of #611's review, carried into #604 PR 2 (f604-plan.md §14): no replacement for a member of a hedge
// with no winner; a lost successor carries no gap (the parity table in reserved_slot_holes_test.go holds its shape);
// and a winner whose Allocation goes in the resolving pass, driven through the rig.

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// TestNoReplacementForAMemberOfAHedgeWithNoWinner: a missing primary's hedge has M1 on A, whose job starts at
// 08:00:10, and M2 on B, pending 10 h (seeded, hedgeWidth 1, so growth adds nothing). A's waits are then all 10 h, so
// M1's replacement is planned at its job's start (clamped) and due at once. While M1 starts and the hedge has no
// winner, no replacement is made; once M1 is ready and wins, its replacement is made in that pass or the next.
func TestNoReplacementForAMemberOfAHedgeWithNoWinner(t *testing.T) {
	h := newHedgeRig(t)
	h.batch.Enqueue(pfake.Script{})
	h.b.batch.Enqueue(pfake.Script{QueueWait: 10 * time.Hour})
	h.Step()
	md := mdSpec(hoMD)
	seededOnly(md)
	h.create(md)
	h.Step() // 08:00:00: M1 on A
	m1 := h.onProvider(hoMD, rigProvider)[0].Name
	h.seedHedgeMember(hoMD, m1, "", hedgeB)
	h.clock.Advance(10 * time.Second)
	h.Step() // 08:00:10: M1's job starts
	if a, ok := h.allocation(h.mustInstance(m1).Spec.AllocationRef.Name); !ok || a.Status.AtProvider.StartTime == nil {
		t.Fatalf("M1's job has not started at 08:00:10: %+v", a)
	}
	setWaits(h.rig, rigProvider, 20, 10*time.Hour)
	sawStarting := false
	for range 30 {
		h.clock.Advance(10 * time.Second)
		h.Step()
		in := h.mustInstance(m1)
		var repl []v1alpha1.Instance
		for _, o := range h.liveInstances(hoMD) {
			if o.Annotations[v1alpha1.AnnotationPredecessor] == m1 {
				repl = append(repl, o)
			}
		}
		if in.Status.Phase != v1alpha1.InstanceReady {
			sawStarting = true
			if len(repl) != 0 {
				t.Fatalf("at %s M1 is %s in a hedge with no winner, and has %d replacements, want none", h.clock.Now().Format(time.TimeOnly), in.Status.Phase, len(repl))
			}
			continue
		}
		if !sawStarting {
			t.Fatal("M1 was ready before any pass saw it starting: the test proves nothing")
		}
		if len(repl) == 0 {
			h.clock.Advance(10 * time.Second)
			h.Step()
			for _, o := range h.liveInstances(hoMD) {
				if o.Annotations[v1alpha1.AnnotationPredecessor] == m1 {
					return
				}
			}
			t.Fatalf("M1 won at %s and no replacement was made by the next pass", h.clock.Now().Format(time.TimeOnly))
		}
		return
	}
	t.Fatal("M1 never became ready")
}

// TestAWinnerWhoseAllocationGoesInTheRig: M1 on A pends 10 h and M2 on B starts at once (seeded, hedgeWidth 1). The
// moment M2's ready status is written, in the pass that resolves the hedge, its Allocation is deleted from outside,
// after that pass's own read of the Allocations. Resolution reads the winner's Allocation again, so no loser ends: M1
// is not HedgeLost, and stays pending.
func TestAWinnerWhoseAllocationGoesInTheRig(t *testing.T) {
	var winner string
	var deleted atomic.Bool
	gate := interceptor.Funcs{SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
		if err := cl.SubResource(sub).Update(ctx, obj, opts...); err != nil {
			return err
		}
		if in, ok := obj.(*v1alpha1.Instance); ok && in.Name == winner && in.Status.Phase == v1alpha1.InstanceReady && deleted.CompareAndSwap(false, true) {
			var a v1alpha1.Allocation
			if err := cl.Get(ctx, client.ObjectKey{Namespace: in.Namespace, Name: in.Spec.AllocationRef.Name}, &a); err != nil {
				return err
			}
			return cl.Delete(ctx, &a)
		}
		return nil
	}}
	h := newHedgeRig(t, withInterceptors(gate))
	h.batch.Enqueue(pfake.Script{QueueWait: 10 * time.Hour})
	h.Step()
	md := mdSpec(hoMD)
	seededOnly(md)
	h.create(md)
	h.Step()
	m1 := h.onProvider(hoMD, rigProvider)[0].Name
	winner = h.seedHedgeMember(hoMD, m1, "", hedgeB)
	h.until(5*time.Minute, 10*time.Second, "M2's Allocation deleted as it became ready", deleted.Load)
	for range 6 {
		l := h.mustInstance(m1)
		if l.Status.EndReason == v1alpha1.ReasonHedgeLost || ended(l) {
			t.Fatalf("M1 ended (%s) beside a winner whose Allocation was deleted in the resolving pass", l.Status.EndReason)
		}
		h.clock.Advance(10 * time.Second)
		h.Step()
	}
}

// TestAMemberJoinsANoOverlapGap: on the queued path, P's successor S is made marked at 09:19:40 and pends behind A's
// GPU cap; P drains in no-overlap mode at 09:50:20, so the gap begins and Degraded is NoOverlapSlot. At 09:50:30 a
// second Provider with room joins the deployment. S's start deadline has passed, so in the next pass its hedge gets a
// member there, at once: it names P and S's hedge, and carries no no-overlap stamp, nor its Allocation the queued
// mark. Once that member is ready it wins, and the gap ends: S ends HedgeLost and NoOverlapSlot clears.
func TestAMemberJoinsANoOverlapGap(t *testing.T) {
	rg := noOverlapQueuedRig(t)
	p := noOverlapStart(rg)
	hoStepTo(rg, hoT(9, 19, 20), 5*time.Minute, nil)
	hoStepTo(rg, hoT(9, 19, 40), 10*time.Second, nil)
	reps := hoRepl(rg, p.Name)
	if len(reps) != 1 || reps[0].Annotations[annotationNoOverlapSuccessor] == "" {
		t.Fatalf("P's replacements %+v, want one queued successor, marked", reps)
	}
	s := reps[0].Name
	hoStepTo(rg, hoT(9, 50, 30), time.Minute, nil)
	if c := rg.condition(hoMD, v1alpha1.ConditionDegraded); c.Reason != v1alpha1.ReasonNoOverlapSlot {
		t.Fatalf("at 09:50:30 Degraded = %s %q, want NoOverlapSlot: the fixture has no gap, so the test proves nothing", c.Reason, c.Message)
	}
	rg.addProvider(stuckSecond, "gpu-long", 4*time.Hour, nil, nil)
	rg.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Providers.Names = []string{rigProvider, stuckSecond} })
	rg.clock.Advance(10 * time.Second)
	rg.Step() // 09:50:40
	if a, ok := rg.allocation(rg.instance(s).Spec.AllocationRef.Name); !ok || a.Status.AtProvider.StartTime != nil {
		t.Fatalf("S's job has started by 09:50:40 (%+v): no member is due, so the test proves nothing", a)
	}
	var m *v1alpha1.Instance
	for _, in := range rg.liveInstances(hoMD) {
		if in.Labels[v1alpha1.LabelProvider] == stuckSecond {
			m = &in
		}
	}
	if m == nil {
		t.Fatal("no member of S's hedge on the second Provider at 09:50:40")
	}
	if m.Annotations[v1alpha1.AnnotationPredecessor] != p.Name || m.Annotations[v1alpha1.AnnotationHedgeGroup] != s ||
		m.Annotations[annotationNoOverlapSuccessor] != "" || m.Annotations[annotationNoOverlapGap] != "" {
		t.Fatalf("the member's annotations %v: want predecessor %s, hedge-group %s, and no no-overlap stamp", m.Annotations, p.Name, s)
	}
	if a, ok := rg.allocation(m.Spec.AllocationRef.Name); !ok || a.Annotations[v1alpha1.AnnotationQueuedSuccessor] != "" {
		t.Fatalf("the member's Allocation %+v: want it made, without the queued mark", a)
	}
	member := m.Name
	hoStepUntil(rg, hoT(10, 30, 0), 10*time.Second, func() bool { return rg.instance(member).Status.Phase == v1alpha1.InstanceReady }, nil)
	rg.clock.Advance(10 * time.Second)
	rg.Step()
	if c := rg.condition(hoMD, v1alpha1.ConditionDegraded); c.Reason == v1alpha1.ReasonNoOverlapSlot || strings.Contains(c.Message, "drained with no replacement") {
		t.Fatalf("after the member is ready Degraded = %s %q: the gap should have ended", c.Reason, c.Message)
	}
	// S, pending, lost: it ended HedgeLost and its Allocation was cancelled, so its Instance may be gone already.
	if in := rg.instancesByName()[s]; in != nil && in.Status.EndReason != v1alpha1.ReasonHedgeLost {
		t.Fatalf("S is %s, ended for %q, want HedgeLost", in.Status.Phase, in.Status.EndReason)
	}
	if n := len(rg.rec.Find(eventReplaced, v1alpha1.ReasonHedgeLost)); n < 1 {
		t.Fatal("no replaced HedgeLost event for S")
	}
	if g := rg.passOf(hoMD).gapCarriers(); len(g) != 0 {
		t.Fatalf("%d gap carriers after the member won, want none", len(g))
	}
}
