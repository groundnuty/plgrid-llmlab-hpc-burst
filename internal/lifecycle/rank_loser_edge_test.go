package lifecycle

// Two edges of a hedge's started loser kept as a candidate (#616, PR 5): D1's guard that only a loser that has never
// been ready is kept, and what a candidate does when it restarts after it outranked its winner and before the winner's
// drain starts, which is an OPEN POINT: the rows pin what the code does today, for the design to rule on.

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// TestAStartedLoserThatHasBeenReadyIsNotKept (D1): M1 on A and M2 on B, instances 1, k = 1, both jobs started at
// 08:00:20 and both engines slowed, so the win is forged on the pass's objects at 08:00:40: M2 reads ready, and the
// pass resolves the hedge. M1, whose Instance never was ready, is kept (marked, not ended). The control row gives M1 a
// readyTime of a minute before, as an Instance that was ready and restarted has: it is ended at the win with HedgeLost,
// unmarked, as on main.
func TestAStartedLoserThatHasBeenReadyIsNotKept(t *testing.T) {
	for _, c := range []struct {
		name     string
		wasReady bool
	}{{"never ready: kept", false}, {"was ready once: ended at the win", true}} {
		t.Run(c.name, func(t *testing.T) {
			h := newKlRig(t)
			h.batch.Enqueue(pfake.Script{})
			h.b.batch.Enqueue(pfake.Script{})
			md := mdSpec(hoMD)
			klMD(1, 0)(md)
			h.create(md)
			h.clock.Advance(10 * time.Second)
			h.Step() // 08:00:10: M1 on A
			m1 := h.instances(hoMD)[0].Name
			h.delay[h.mustInstance(m1).Spec.AllocationRef.Name] = klDelay
			m2 := h.seedHedgeMember(hoMD, m1, "", hedgeB)
			h.delay[h.mustInstance(m2).Spec.AllocationRef.Name] = klDelay
			h.setK(1)
			hoStepTo(h.rig, hoT(8, 0, 40), 10*time.Second, nil)
			p := h.passOf(hoMD)
			l, w := p.instanceNamed(m1), p.instanceNamed(m2)
			now := metav1.NewTime(p.now)
			w.Status.Phase, w.Status.ReadyTime = v1alpha1.InstanceReady, &now
			if c.wasReady {
				before := metav1.NewTime(p.now.Add(-time.Minute))
				l.Status.ReadyTime = &before
			}
			if err := p.resolveHedges(); err != nil {
				t.Fatal(err)
			}
			got := h.mustInstance(m1)
			if c.wasReady {
				if !ended(got) || got.Status.EndReason != v1alpha1.ReasonHedgeLost || got.Annotations[annotationCandidateOf] != "" {
					t.Fatalf("M1 is %s (end %q, candidate-of %q), want ended for HedgeLost and unmarked: it was ready once", got.Status.Phase, got.Status.EndReason, got.Annotations[annotationCandidateOf])
				}
				return
			}
			if ended(got) || got.Annotations[annotationCandidateOf] != m2 {
				t.Fatalf("M1 is %s (end %q, candidate-of %q), want kept as M2's candidate", got.Status.Phase, got.Status.EndReason, got.Annotations[annotationCandidateOf])
			}
		})
	}
}

// TestAnOutrankingCandidateThatRestartsBeforeItsWinnerDrains pins an OPEN POINT, not a ruling. A candidate that
// outranked its winner and restarts inside the front's 10 s window, before the winner's drain starts, is verifying
// again with its readyTime set, so it is no longer a waiting candidate (candidateWinnerOf needs a never-ready Instance)
// and takes PR 3's general path on re-passing. Today that has two consequences, found by tracing and pinned here:
//   - in P's replacement hedge, readyUnmarked clears its mark while the winner still serves, so it rejoins the winner's
//     hedge and the next resolveHedges drains it, ending HedgeLost: the winner serves on, and capacity holds;
//   - in a missing-primary hedge with another primary on its Provider (instances 2), its peer is that primary, so it
//     outranks it: the comparison HBEP-7 forbids, though only inside the window.
//
// The restart is forged on the pass's objects (phase verifying, checks passed), as TestAKeptLoserRankingPointCases does.
func TestAnOutrankingCandidateThatRestartsBeforeItsWinnerDrains(t *testing.T) {
	t.Run("a replacement hedge: the candidate is drained as the hedge's loser", func(t *testing.T) {
		h, p0 := klPrimary(t)
		w := klWin(t, h, p0, rk179, ptrTo(int32(1)), false)
		w.waitsToItsPoint()
		h.clock.Advance(10 * time.Second)
		h.Step() // 09:30:20: R1 outranks R2
		h.at(hoT(9, 30, 20))
		if in := h.mustInstance(w.r1); in.Status.Phase != v1alpha1.InstanceReady || h.mustInstance(w.r2).Annotations[annotationRankedOut] != w.r1 {
			t.Fatalf("R1 is %s and R2 carries %q at 09:30:20: the fixture did not outrank", in.Status.Phase, h.mustInstance(w.r2).Annotations[annotationRankedOut])
		}
		p := h.passOf(hoMD)
		l := p.instanceNamed(w.r1)
		l.Status.Phase = v1alpha1.InstanceVerifying
		klPass(l, rk179)
		called := false
		if err := p.enterReady(l, p.allocOf(l), func() error { called = true; l.Status.Phase = v1alpha1.InstanceReady; return nil }); err != nil {
			t.Fatal(err)
		}
		if !called || candidateOf(l) != "" {
			t.Fatalf("ready called %v, candidate-of %q: today a re-passing outranker is made ready with its mark cleared", called, candidateOf(l))
		}
		if err := p.resolveHedges(); err != nil {
			t.Fatal(err)
		}
		got := h.mustInstance(w.r1)
		if got.Annotations[annotationHedgeLost] != w.r2 || (got.Status.Phase != v1alpha1.InstanceDraining && got.Status.Phase != v1alpha1.InstanceEnded) {
			t.Fatalf("R1 is %s with hedge-lost %q: today it is drained as the hedge's loser", got.Status.Phase, got.Annotations[annotationHedgeLost])
		}
		if win := h.mustInstance(w.r2); win.Status.Phase != v1alpha1.InstanceReady {
			t.Fatalf("R2 is %s, want it serving on: capacity holds", win.Status.Phase)
		}
	})
	t.Run("instances 2: the candidate outranks the other primary on its Provider", func(t *testing.T) {
		h, pp, m1, m2 := klMissing(t, 2, rk179)
		hoStepTo(h.rig, hoT(8, 10, 50), 10*time.Second, nil)
		h.clock.Advance(10 * time.Second)
		h.Step() // 08:11:00: M1 outranks M2
		h.at(hoT(8, 11, 0))
		if h.mustInstance(m1).Status.Phase != v1alpha1.InstanceReady || h.mustInstance(m2).Annotations[annotationRankedOut] != m1 {
			t.Fatal("the fixture did not outrank")
		}
		p := h.passOf(hoMD)
		l := p.instanceNamed(m1)
		l.Status.Phase = v1alpha1.InstanceVerifying
		klPass(l, rk179)
		called := false
		if err := p.enterReady(l, p.allocOf(l), func() error { called = true; return nil }); err != nil {
			t.Fatal(err)
		}
		if !called || h.mustInstance(pp).Annotations[annotationRankedOut] != m1 {
			t.Fatalf("ready called %v, the other primary's ranked-out mark %q: today the re-passing candidate outranks it", called, h.mustInstance(pp).Annotations[annotationRankedOut])
		}
	})
}
