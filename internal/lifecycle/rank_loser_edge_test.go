package lifecycle

// Two edges of a hedge's started loser kept as a candidate (#616, PR 5): D1's guard that only a loser that has never
// been ready is kept, and what a candidate does when it restarts after it outranked its winner and before the winner's
// drain starts: it keeps its mark and is ranked against that winner alone again (HBEP-7, Hedging's started losers; issue
// #730, whose stated limit #725's batch lifts).

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

// TestAnOutrankingCandidateThatRestartsBeforeItsWinnerDrains (#730): a candidate that outranked its winner and restarts
// inside the front's 10 s window, before the winner's drain starts, is verifying again with its readyTime set. It is not
// ready now, so it is a waiting candidate again (waitingCandidate), keeps its candidate-of mark and, passing, is ranked
// against its winner alone, as it was the first time (rankCandidate); it never takes PR 3's general path, which
// compared it with the other primaries of its Provider. Worked by hand for each row (17.9 ms against 20 ms:
// 179 < 180, so it outranks, as the fixture's first ranking did):
//   - in P's replacement hedge, R1 (candidate of R2) passes again: R2 is ready, has no live replacement, and carries the
//     ranked-out mark of R1 itself, which is its own and not another's, so R1 is ranked against R2: it outranks, ready
//     is called, R2's mark is not rewritten (it names R1 already), R1's candidate-of mark is still R2, and resolveHedges
//     drains nothing: R1 is a hedge of its own (its key is its own), no loser of R2's, so it is not ended with HedgeLost
//     and R2 serves on (the old behaviour cleared the mark, rejoined the hedge and drained R1);
//   - with instances 2, M1 (candidate of M2, on A) passes again with P, the other primary on A, ready at 40 ms: it is
//     ranked against M2 alone, outranks it, ready is called, and P's ranked-out mark stays empty (the old behaviour
//     outranked P: 17.9 ms against 40 ms).
//
// The restart is forged on the pass's objects (phase verifying, checks passed), as TestAKeptLoserRankingPointCases does.
func TestAnOutrankingCandidateThatRestartsBeforeItsWinnerDrains(t *testing.T) {
	t.Run("a replacement hedge: the candidate is ranked against its winner again and not drained as the hedge's loser", func(t *testing.T) {
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
		if !called || candidateOf(l) != w.r2 {
			t.Fatalf("ready called %v, candidate-of %q: want the candidate ranked against R2, outranking it again, with its mark kept", called, candidateOf(l))
		}
		if err := p.resolveHedges(); err != nil {
			t.Fatal(err)
		}
		got := h.mustInstance(w.r1)
		if got.Annotations[annotationHedgeLost] != "" || got.Status.Phase != v1alpha1.InstanceReady {
			t.Fatalf("R1 is %s with hedge-lost %q: it is its own hedge, no loser of R2's, so it is not drained", got.Status.Phase, got.Annotations[annotationHedgeLost])
		}
		if win := h.mustInstance(w.r2); win.Status.Phase != v1alpha1.InstanceReady || win.Annotations[annotationRankedOut] != w.r1 {
			t.Fatalf("R2 is %s with ranked-out %q, want it serving on with its mark naming R1", win.Status.Phase, win.Annotations[annotationRankedOut])
		}
	})
	t.Run("instances 2: the candidate is ranked against its winner alone and never against the other primary on its Provider", func(t *testing.T) {
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
		if !called || candidateOf(l) != m2 || h.mustInstance(pp).Annotations[annotationRankedOut] != "" {
			t.Fatalf("ready called %v, candidate-of %q, the other primary's ranked-out mark %q: want M1 ranked against M2 alone, outranking it, and P never compared", called, candidateOf(l), h.mustInstance(pp).Annotations[annotationRankedOut])
		}
	})
}

// TestARestartedOutrankingCandidateWithNoPeerIsRankedAgainstItsWinner (#725's R4; #721's note: "a restarted outranking
// candidate with no peer (instances 1) ends RankedOut, as read from the code"). Instances 1: M1 on A is the candidate
// of M2, on B, which wins at 08:01:10 measuring 20 ms; M1 passes at 08:11:00, measures 17.9 ms and outranks it (179 <
// 180). M1 restarts before M2's drain starts and passes again. Its Provider A holds no other primary, so on the general
// path it had no peer and ended RankedOut with no ranking at all, whatever it measured (the old behaviour). With its
// mark kept it is ranked against M2 alone, whatever their Providers:
//   - measuring 17.9 ms again, 179 < 180: it outranks, ready is called, it is not ended, no RankedOut event regards it,
//     and its mark is still M2;
//   - measuring 18.0 ms now, 180 is not below 180: it does not outrank, and ends HedgeLost, not RankedOut (it has a
//     winner, not a peer), kept as warm capacity.
func TestARestartedOutrankingCandidateWithNoPeerIsRankedAgainstItsWinner(t *testing.T) {
	for _, c := range []struct {
		name         string
		again        time.Duration
		wantReady    bool
		wantEndsWith string
	}{
		{"17.9 ms again: it outranks and is ready", rk179, true, ""},
		{"18.0 ms now: it ends HedgeLost, not RankedOut", rk180, false, v1alpha1.ReasonHedgeLost},
	} {
		t.Run(c.name, func(t *testing.T) {
			h, _, m1, m2 := klMissing(t, 1, rk179)
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
			klPass(l, c.again)
			called := false
			if err := p.enterReady(l, p.allocOf(l), func() error { called = true; return nil }); err != nil {
				t.Fatal(err)
			}
			got := h.mustInstance(m1)
			if called != c.wantReady {
				t.Fatalf("ready called %v, want %v", called, c.wantReady)
			}
			if c.wantReady {
				if ended(got) || candidateOf(l) != m2 {
					t.Fatalf("M1 is %s with candidate-of %q, want it kept as M2's candidate", got.Status.Phase, candidateOf(l))
				}
			} else if !ended(got) || got.Status.EndReason != c.wantEndsWith {
				t.Fatalf("M1 is %s (end %q), want it ended with %s", got.Status.Phase, got.Status.EndReason, c.wantEndsWith)
			}
			for _, e := range h.rec.Find(eventReplaced, v1alpha1.ReasonRankedOut) {
				if e.Name == m1 {
					t.Fatalf("a RankedOut event regards M1: %+v: a candidate with a winner is never ranked out against no peer", e)
				}
			}
		})
	}
}
