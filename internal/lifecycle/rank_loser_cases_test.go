package lifecycle

// HBEP-7's Test plan row "A hedge's started loser" and its edges (#616, PR 5), through the pass: the missing-primary
// hedge, the winner's live replacement and failure, a controller restart, a failed check, and the ranking point's cases
// on the pass's own objects. The fixture and its timeline are rank_loser_rig_test.go's.

import (
	"testing"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// klMissing starts a deployment of n instances at k = 0 whose last missing primary has a hedge, seeded: M1 on A (the
// loser, its engine measuring tpotL and starting its worker after klDelay: its ranking point is 08:11:00) and M2 on
// B (the winner, measuring 20 ms, ready at 08:01:10). With n = 2 the other primary P is on A, measuring 40 ms, ready
// at 08:01:10 too. The deployment's overAllocation is set to 1 before the win. It steps to 08:01:10 and returns the
// names (p is "" for n = 1).
func klMissing(t *testing.T, n int32, tpotL time.Duration, bScripts ...pfake.Script) (h *klRig, p, m1, m2 string) {
	t.Helper()
	a := []pfake.Script{{}}
	if n == 2 {
		a = []pfake.Script{{}, {}}
	}
	return klMissingA(t, a, n, tpotL, bScripts...)
}

// klMissingA is klMissing with A's queue waits given: with n = 2 the first script is P's and the second M1's (the
// order the fake submits in, the Allocations' names).
func klMissingA(t *testing.T, aScripts []pfake.Script, n int32, tpotL time.Duration, bScripts ...pfake.Script) (h *klRig, p, m1, m2 string) {
	t.Helper()
	h = newKlRig(t)
	h.batch.Enqueue(aScripts...)
	if len(bScripts) == 0 {
		bScripts = []pfake.Script{{}}
	}
	h.b.batch.Enqueue(bScripts...)
	md := mdSpec(hoMD)
	klMD(n, 0)(md)
	h.create(md)
	h.clock.Advance(10 * time.Second)
	h.Step() // 08:00:10: the missing primaries are made on A
	ins := rkRoles(h.instances(hoMD))
	if len(ins) != int(n) {
		t.Fatalf("%d Instances at 08:00:10, want the %d missing primaries", len(ins), n)
	}
	if n == 2 {
		p, m1 = ins[0], ins[1] // the order the fake submits in, the Allocations' names
	} else {
		m1 = ins[0]
	}
	m1A := h.mustInstance(m1).Spec.AllocationRef.Name
	h.delay[m1A], h.tpot[m1A] = klDelay, tpotL
	m2 = h.seedHedgeMember(hoMD, m1, "", hedgeB)
	h.tpot[h.mustInstance(m2).Spec.AllocationRef.Name] = 20 * time.Millisecond
	h.setK(1)
	h.until(5*time.Minute, 10*time.Second, "M2 ready", func() bool { return h.mustInstance(m2).Status.Phase == v1alpha1.InstanceReady })
	h.at(hoT(8, 1, 10))
	return h, p, m1, m2
}

// TestAStartedLoserIsNeverComparedWithAnotherPrimary: instances 2. P, ready on A at 08:01:10 with no replacement,
// measures 40 ms, so it is the peer PR 3's rule would compare a candidate on A with. The second primary's hedge has M1
// on A (started at 08:00:20) and M2 on B, which wins at 08:01:10 measuring 20 ms. M1 passes at 08:11:00. Measuring 18.0
// ms it ends HedgeLost, though it is faster than P by more than 10%: it is ranked against M2 alone, and P is left
// alone. Measuring 17.9 ms it outranks M2, which is marked, drains and ends RankedOut, and P, which M1 would outrank
// by far, is never marked or drained. Members are seeded.
func TestAStartedLoserIsNeverComparedWithAnotherPrimary(t *testing.T) {
	for _, c := range []struct {
		name     string
		tpot     time.Duration
		outranks bool
	}{{"18.0 ms against 20 ms", rk180, false}, {"17.9 ms against 20 ms", rk179, true}} {
		t.Run(c.name, func(t *testing.T) {
			h, p, m1, m2 := klMissing(t, 2, c.tpot)
			if in := h.mustInstance(m1); ended(in) || in.Annotations[annotationCandidateOf] != m2 {
				t.Fatalf("M1 is %s with candidate-of %q at the win, want it kept as M2's candidate", in.Status.Phase, in.Annotations[annotationCandidateOf])
			}
			hoStepTo(h.rig, hoT(8, 10, 50), 10*time.Second, func() {
				if in := h.mustInstance(m1); ended(in) || in.Status.Phase == v1alpha1.InstanceReady {
					t.Fatalf("M1 is %s (end %q) at %s, want it checking until 08:11:00", in.Status.Phase, in.Status.EndReason, h.clock.Now().Format(time.TimeOnly))
				}
			})
			h.clock.Advance(10 * time.Second)
			h.Step()
			h.at(hoT(8, 11, 0))
			l, w, pp := h.mustInstance(m1), h.mustInstance(m2), h.mustInstance(p)
			if pp.Annotations[annotationRankedOut] != "" || pp.Status.Phase != v1alpha1.InstanceReady {
				t.Fatalf("P is %s with ranked-out %q: it was compared with the candidate", pp.Status.Phase, pp.Annotations[annotationRankedOut])
			}
			if !c.outranks {
				if !ended(l) || l.Status.EndReason != v1alpha1.ReasonHedgeLost {
					t.Fatalf("M1 is %s (end %q) at 08:11:00, want ended for HedgeLost: it is not faster than M2, whatever P measures", l.Status.Phase, l.Status.EndReason)
				}
				if w.Status.Phase != v1alpha1.InstanceReady || w.Annotations[annotationRankedOut] != "" {
					t.Fatalf("M2 is %s with ranked-out %q, want ready and unmarked", w.Status.Phase, w.Annotations[annotationRankedOut])
				}
				return
			}
			if l.Status.Phase != v1alpha1.InstanceReady || w.Annotations[annotationRankedOut] != m1 {
				t.Fatalf("M1 is %s and M2 carries ranked-out %q at 08:11:00, want M1 ready and M2 marked as outranked by it", l.Status.Phase, w.Annotations[annotationRankedOut])
			}
			h.until(3*time.Minute, 10*time.Second, "M2 ended", func() bool { return ended(h.mustInstance(m2)) })
			if in := h.mustInstance(m2); in.Status.EndReason != v1alpha1.ReasonRankedOut {
				t.Fatalf("M2 ended for %q, want RankedOut", in.Status.EndReason)
			}
			if pp := h.mustInstance(p); pp.Status.Phase != v1alpha1.InstanceReady || pp.Annotations[annotationRankedOut] != "" {
				t.Fatalf("P is %s with ranked-out %q after the outranking: it must be left alone", pp.Status.Phase, pp.Annotations[annotationRankedOut])
			}
		})
	}
}

// TestAWinnerWithALiveReplacementKeepsServing: instances 1, the missing primary's hedge M1 on A (the loser) and M2 on
// B, which wins at 08:01:10 and is then given a live replacement R3 at 08:01:20 (seeded on B, pending behind 10 h).
// M2 is in its handover, which ranking leaves alone: M1 passes at 08:11:00 measuring 17.9 ms, which would outrank
// M2, and ends HedgeLost instead, kept; M2 keeps serving, unmarked and not draining. Members are seeded.
func TestAWinnerWithALiveReplacementKeepsServing(t *testing.T) {
	h, _, m1, m2 := klMissing(t, 1, rk179, pfake.Script{}, pfake.Script{QueueWait: 10 * time.Hour})
	if in := h.mustInstance(m1); ended(in) || in.Annotations[annotationCandidateOf] != m2 {
		t.Fatalf("M1 is %s with candidate-of %q at the win, want it kept", in.Status.Phase, in.Annotations[annotationCandidateOf])
	}
	h.clock.Advance(10 * time.Second)
	h.Step() // 08:01:20
	p := h.passOf(hoMD)
	var pred *v1alpha1.Instance
	for _, in := range p.insts {
		if in.Name == m2 {
			pred = in
		}
	}
	if ok, err := p.provisionWith(pred, placeOpts{only: hedgeB}); err != nil || !ok {
		t.Fatalf("seeding M2's replacement: placed %v, %v", ok, err)
	}
	repl := 0
	for _, in := range h.liveInstances(hoMD) {
		if in.Annotations[v1alpha1.AnnotationPredecessor] == m2 {
			repl++
		}
	}
	if repl != 1 {
		t.Fatalf("%d live replacements of M2, want the one just seeded", repl)
	}
	hoStepTo(h.rig, hoT(8, 11, 0), 10*time.Second, nil)
	l, w := h.mustInstance(m1), h.mustInstance(m2)
	if !ended(l) || l.Status.EndReason != v1alpha1.ReasonHedgeLost {
		t.Fatalf("M1 is %s (end %q) at 08:11:00, want ended for HedgeLost: M2 has a live replacement", l.Status.Phase, l.Status.EndReason)
	}
	if w.Status.Phase != v1alpha1.InstanceReady || w.Annotations[annotationRankedOut] != "" {
		t.Fatalf("M2 is %s with ranked-out %q, want it serving and unmarked", w.Status.Phase, w.Annotations[annotationRankedOut])
	}
	if h.released(h.mustInstance(m1).Spec.AllocationRef.Name) {
		t.Fatal("M1's Allocation was released, want it kept as warm capacity")
	}
}

// TestAWinnerThatFailsLeavesTheLoserChosen: instances 1. M2 on B wins at 08:01:10 and its job fails at 08:04:20
// (FailAfter 4 min from its start at 08:00:20), while M1, kept as its candidate, still verifies. With the winner gone
// M1 is no longer forced surplus: it is the deployment's only Instance, so it is chosen, and no Allocation is made in
// that pass or after (HBEP-7, How it counts: "When the round's primary fails, its extra is chosen in its place, and no
// Allocation is made"). M1 passes at 08:11:00, measuring 17.9 ms, and becomes ready as a chosen Instance with no
// ranking and no peer, its mark cleared. The two jobs submitted at the start are the only ones, to the end.
func TestAWinnerThatFailsLeavesTheLoserChosen(t *testing.T) {
	fail := 4 * time.Minute
	h, _, m1, m2 := klMissing(t, 1, rk179, pfake.Script{FailAfter: &fail})
	if subs := h.sbatches(); subs != 2 {
		t.Fatalf("%d jobs at the win, want the two members'", subs)
	}
	hoStepTo(h.rig, hoT(8, 4, 40), 10*time.Second, nil)
	if in := h.mustInstance(m2); !ended(in) || in.Status.EndReason == "" || cleanEnd(in.Status.EndReason) {
		t.Fatalf("M2 is %s (end %q) at 08:04:40, want ended for a failure: the fixture's winner did not fail", in.Status.Phase, in.Status.EndReason)
	}
	if in := h.mustInstance(m1); ended(in) {
		t.Fatalf("M1 ended (%q) when its winner failed", in.Status.EndReason)
	}
	hoStepTo(h.rig, hoT(8, 11, 0), 10*time.Second, nil)
	l := h.mustInstance(m1)
	if l.Status.Phase != v1alpha1.InstanceReady || l.Status.ReadyTime == nil || !l.Status.ReadyTime.Time.Equal(hoT(8, 11, 0)) {
		t.Fatalf("M1 is %s (ready %v) at 08:11:00, want ready at 08:11:00 as a chosen Instance", l.Status.Phase, l.Status.ReadyTime)
	}
	if l.Annotations[annotationCandidateOf] != "" {
		t.Fatalf("M1 still carries candidate-of %q once chosen, want it cleared", l.Annotations[annotationCandidateOf])
	}
	h.run(30*time.Minute, time.Minute)
	if subs := h.sbatches(); subs != 2 {
		t.Fatalf("%d jobs submitted by 08:41, want the two members' only: no Allocation is made for the failed winner", subs)
	}
	if h.readyCount(hoMD) != 1 {
		t.Fatalf("%d ready Instances, want M1 alone", h.readyCount(hoMD))
	}
}

// TestAKeptLosersMarkIsReadAfterARestart: the controller restarts at 09:25:00, between the win and the ranking
// point. Its memory is lost; the mark is read from R1's annotation, and the result is the one of the same row with no
// restart, in both cases: 18.0 ms ends HedgeLost, kept, with a success; 17.9 ms becomes ready and R2 ends RankedOut.
// No time is asserted for it: the restart changes how long a pass takes to see the agents again.
func TestAKeptLosersMarkIsReadAfterARestart(t *testing.T) {
	for _, c := range []struct {
		name     string
		tpot     time.Duration
		outranks bool
	}{{"18.0 ms against 20 ms", rk180, false}, {"17.9 ms against 20 ms", rk179, true}} {
		t.Run(c.name, func(t *testing.T) {
			h, p := klPrimary(t)
			w := klWin(t, h, p, c.tpot, ptrTo(int32(1)), false)
			w.keptAtTheWin()
			hoStepTo(h.rig, hoT(9, 25, 0), 30*time.Second, nil)
			h.restartController()
			if got := h.mustInstance(w.r1).Annotations[annotationCandidateOf]; got != w.r2 {
				t.Fatalf("R1's candidate-of %q after the restart, want %q read from the object", got, w.r2)
			}
			h.until(15*time.Minute, 10*time.Second, "R1 ranked", func() bool {
				in := h.mustInstance(w.r1)
				return ended(in) || in.Status.Phase == v1alpha1.InstanceReady
			})
			l := h.mustInstance(w.r1)
			if c.outranks {
				if l.Status.Phase != v1alpha1.InstanceReady || h.mustInstance(w.r2).Annotations[annotationRankedOut] != w.r1 {
					t.Fatalf("R1 is %s and R2 carries %q after the restart, want R1 ready and R2 marked by it", l.Status.Phase, h.mustInstance(w.r2).Annotations[annotationRankedOut])
				}
				h.until(3*time.Minute, 10*time.Second, "R2 ended", func() bool { return ended(h.mustInstance(w.r2)) })
				if r := h.mustInstance(w.r2).Status.EndReason; r != v1alpha1.ReasonRankedOut {
					t.Fatalf("R2 ended for %q after the restart, want RankedOut", r)
				}
				return
			}
			if !ended(l) || l.Status.EndReason != v1alpha1.ReasonHedgeLost || h.released(w.r1A) {
				t.Fatalf("R1 is %s (end %q), Allocation released %v after the restart: want ended for HedgeLost, kept", l.Status.Phase, l.Status.EndReason, h.released(w.r1A))
			}
			if o, ok := h.outcomeOf(w.r1UID); !ok || o.Result != v1alpha1.StartOutcomeReady {
				t.Fatalf("R1's outcome %+v (%v) after the restart, want a success", o, ok)
			}
			if h.mustInstance(w.r2).Status.Phase != v1alpha1.InstanceReady {
				t.Fatal("R2 stopped serving")
			}
		})
	}
}

// TestAKeptLoserThatFailsACheckIsReleased: R1's engine's three greedy outputs differ within their first 32 tokens, so
// R1 fails its corruption check, in qualification, before its warm-up and before its ranking point. It fails as any
// Instance does: it ends QualificationFailed, never HedgeLost or RankedOut, its Allocation is released (a broken node
// is no spare), and A's class records one failure for it. R2 serves on, unmarked.
func TestAKeptLoserThatFailsACheckIsReleased(t *testing.T) {
	h, p := klPrimary(t)
	w := klWin(t, h, p, rk179, ptrTo(int32(1)), true)
	w.keptAtTheWin()
	h.until(15*time.Minute, 10*time.Second, "R1 ended", func() bool { return ended(h.mustInstance(w.r1)) })
	l := h.mustInstance(w.r1)
	if l.Status.EndReason != v1alpha1.ReasonQualificationFailed {
		t.Fatalf("R1 ended for %q, want QualificationFailed", l.Status.EndReason)
	}
	// The release is made in the pass that ends it, not left to the next pass's sweep of a failed Instance's
	// Allocation (tombstone), which would release it all the same and hide an end that kept it.
	if !h.released(w.r1A) {
		t.Fatal("R1's Allocation is not released in the pass that ended it: a broken node is no spare")
	}
	if f := h.classAFailures(); f != 1 {
		t.Fatalf("A's class records %d failures, want 1 (R1's)", f)
	}
	if o, ok := h.outcomeOf(w.r1UID); !ok || o.Result != v1alpha1.StartOutcomeFailed || o.Reason != v1alpha1.ReasonQualificationFailed {
		t.Fatalf("R1's outcome %+v (%v), want Failed with QualificationFailed", o, ok)
	}
	if n := len(h.hedgeLostEvents(w.r1)); n != 0 {
		t.Fatalf("%d HedgeLost events regarding R1, want none: it failed", n)
	}
	if win := h.mustInstance(w.r2); win.Status.Phase != v1alpha1.InstanceReady || win.Annotations[annotationRankedOut] != "" {
		t.Fatalf("R2 is %s with ranked-out %q, want it serving, unmarked", win.Status.Phase, win.Annotations[annotationRankedOut])
	}
}

// TestAKeptLoserIsSurplusWhileTheDeploymentIsShort: instances 2. The first primary P is still pending on A behind a 10 h
// wait, and the second's hedge has M1 on A (started 08:00:20) and M2 on B, which wins at 08:01:10 measuring 20 ms. The
// deployment is one primary short, so by its rank alone (started, above pending) M1 would be chosen; it is the surplus
// of A all the same (HBEP-7, How it counts: a hedge's started loser kept as a candidate is surplus on its Provider
// too), so it is ranked: at 08:11:00, measuring 18.0 ms against M2's 20, it ends HedgeLost, where a chosen M1 would
// have become ready unranked. P stays pending and chosen, and no Allocation is made: three jobs in all, P's, M1's and
// M2's. Members are seeded.
func TestAKeptLoserIsSurplusWhileTheDeploymentIsShort(t *testing.T) {
	h, p, m1, m2 := klMissingA(t, []pfake.Script{{QueueWait: 10 * time.Hour}, {}}, 2, rk180)
	if in := h.mustInstance(m1); ended(in) || in.Annotations[annotationCandidateOf] != m2 {
		t.Fatalf("M1 is %s with candidate-of %q at the win, want it kept as M2's candidate", in.Status.Phase, in.Annotations[annotationCandidateOf])
	}
	if a, _ := h.allocation(h.mustInstance(p).Spec.AllocationRef.Name); a == nil || jobStarted(a) {
		t.Fatalf("P's job started, want it pending behind its 10 h wait: %+v", a)
	}
	hoStepTo(h.rig, hoT(8, 11, 0), 10*time.Second, nil)
	l := h.mustInstance(m1)
	if !ended(l) || l.Status.EndReason != v1alpha1.ReasonHedgeLost || l.Status.EndTime == nil || !l.Status.EndTime.Time.Equal(hoT(8, 11, 0)) {
		t.Fatalf("M1 is %s (end %q at %v) at 08:11:00, want ended for HedgeLost at 08:11:00: it is surplus, so it is ranked and 18.0 ms is not faster than 20 ms by more than 10%%", l.Status.Phase, l.Status.EndReason, l.Status.EndTime)
	}
	if in := h.mustInstance(p); ended(in) || jobStarted(h.allocOrNil(in.Spec.AllocationRef.Name)) {
		t.Fatalf("P is %s at 08:11:00, want it pending still", in.Status.Phase)
	}
	if subs := h.sbatches(); subs != 3 {
		t.Fatalf("%d jobs submitted, want P's, M1's and M2's only", subs)
	}
}

// klPass reads the Instance as having passed its checks, measuring tpot, in the pass's memory.
func klPass(in *v1alpha1.Instance, tpot time.Duration) {
	now := metav1.Now()
	for _, c := range []string{v1alpha1.ConditionVerified, v1alpha1.ConditionQualified, v1alpha1.ConditionWarmedUp} {
		apimeta.SetStatusCondition(&in.Status.Conditions, metav1.Condition{Type: c, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonPassed, LastTransitionTime: now})
	}
	in.Status.Throughput = &v1alpha1.Throughput{TimePerOutputToken: v1alpha1.Duration(tpot.String()), TargetSessions: 8}
}

// TestAKeptLoserRankingPointCases pins what a candidate does at its ranking point when its winner is not what the
// win left, on the pass's own objects (the winner's state is set in the pass's memory, as
// TestAWinnerWhoseAllocationGoesResolvesNothing does). R1 has passed its checks, tunnel up.
//   - The winner is verifying again (restarting): R1 waits, ready is not called, the mark stays, nothing is written.
//   - The winner has ended: R1 is chosen like any Instance: ready is called, with the mark cleared.
//   - The winner has a live replacement: R1 ends with HedgeLost, ready is not called.
//   - The winner is ready, with none: R1 is ranked (17.9 ms against 20: ready is called, R2 marked).
func TestAKeptLoserRankingPointCases(t *testing.T) {
	for _, c := range []struct {
		name      string
		prep      func(p *pass, l, w *v1alpha1.Instance)
		ready     bool
		ended     bool
		marked    bool // R1 keeps candidate-of
		wantMarkW bool // R2 carries the ranked-out mark
	}{
		{"the winner is verifying again", func(p *pass, l, w *v1alpha1.Instance) { w.Status.Phase = v1alpha1.InstanceVerifying }, false, false, true, false},
		{"the winner has ended", func(p *pass, l, w *v1alpha1.Instance) { w.Status.Phase = v1alpha1.InstanceEnded }, true, false, false, false},
		{"the winner has a live replacement", func(p *pass, l, w *v1alpha1.Instance) {
			r := &v1alpha1.Instance{ObjectMeta: metav1.ObjectMeta{Namespace: rigNS, Name: "w-next", UID: "uid-w-next", Annotations: map[string]string{v1alpha1.AnnotationPredecessor: w.Name}}}
			r.Status.Phase = v1alpha1.InstancePending
			p.insts = append(p.insts, r)
		}, false, true, true, false},
		{"the winner is ready and has none", func(p *pass, l, w *v1alpha1.Instance) {}, true, false, true, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			h, p0 := klPrimary(t)
			w := klWin(t, h, p0, rk179, ptrTo(int32(1)), false)
			w.keptAtTheWin()
			p := h.passOf(hoMD)
			l, win := p.instanceNamed(w.r1), p.instanceNamed(w.r2)
			klPass(l, rk179)
			c.prep(p, l, win)
			called := false
			if err := p.enterReady(l, p.allocOf(l), func() error { called = true; return nil }); err != nil {
				t.Fatal(err)
			}
			if called != c.ready {
				t.Fatalf("ready called %v, want %v", called, c.ready)
			}
			got := h.mustInstance(w.r1)
			if ended(got) != c.ended || (c.ended && got.Status.EndReason != v1alpha1.ReasonHedgeLost) {
				t.Fatalf("R1 is %s (end %q), want ended %v with HedgeLost", got.Status.Phase, got.Status.EndReason, c.ended)
			}
			if (got.Annotations[annotationCandidateOf] != "") != c.marked {
				t.Fatalf("R1's candidate-of is %q, want kept %v", got.Annotations[annotationCandidateOf], c.marked)
			}
			if (h.mustInstance(w.r2).Annotations[annotationRankedOut] != "") != c.wantMarkW {
				t.Fatalf("R2's ranked-out mark is %q, want set %v", h.mustInstance(w.r2).Annotations[annotationRankedOut], c.wantMarkW)
			}
		})
	}
}

// TestAWaitingCandidateIsExemptFromStartTimeoutOnlyWithItsTunnelUp (D7): a kept started loser that has passed its checks
// waits at its ranking point as any surplus Instance does, so health.startTimeout no longer judges it; one whose
// tunnel is not connected is judged, and fails at its deadline with TunnelNotConnected (HBEP-6, Key restriction). One
// whose winner has gone is chosen, so the exemption (keyed on the selection) does not hold for it.
func TestAWaitingCandidateIsExemptFromStartTimeoutOnlyWithItsTunnelUp(t *testing.T) {
	h, p0 := klPrimary(t)
	w := klWin(t, h, p0, rk179, ptrTo(int32(1)), false)
	p := h.passOf(hoMD)
	l, win := p.instanceNamed(w.r1), p.instanceNamed(w.r2)
	if p.waitingAtRankingPoint(l) {
		t.Fatal("R1 waits at its ranking point before it passed its checks")
	}
	klPass(l, rk179)
	setTunnel := func(s metav1.ConditionStatus) {
		for i := range l.Status.Conditions {
			if l.Status.Conditions[i].Type == v1alpha1.ConditionTunnelConnected {
				l.Status.Conditions = append(l.Status.Conditions[:i], l.Status.Conditions[i+1:]...)
				break
			}
		}
		l.Status.Conditions = append(l.Status.Conditions, metav1.Condition{Type: v1alpha1.ConditionTunnelConnected, Status: s})
	}
	setTunnel(metav1.ConditionTrue)
	if !p.waitingAtRankingPoint(l) {
		t.Fatal("R1, passed with its tunnel up and its winner alive, does not wait at its ranking point: startTimeout would end it")
	}
	setTunnel(metav1.ConditionFalse)
	if p.waitingAtRankingPoint(l) {
		t.Fatal("R1 with its tunnel down waits at its ranking point: it must fail at its deadline with TunnelNotConnected")
	}
	setTunnel(metav1.ConditionTrue)
	win.Status.Phase = v1alpha1.InstanceEnded
	if p.waitingAtRankingPoint(l) {
		t.Fatal("R1 waits at a ranking point whose winner has ended: it is chosen, and a chosen Instance is judged by startTimeout")
	}
}
