package lifecycle

// The three notes of #611's review, carried into #604 PR 2 (f604-plan.md §14): no replacement for a member of a hedge
// with no winner; a lost successor carries no gap (the parity table in reserved_slot_holes_test.go holds its shape);
// and a winner whose Allocation goes in the resolving pass, driven through the rig.

import (
	"context"
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
