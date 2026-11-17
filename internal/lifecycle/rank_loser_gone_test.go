package lifecycle

// A kept started loser never enters the general ranking (HBEP-7, Hedging's started losers; The ranking point, Its
// peers: "A hedge's started loser is the one exception: its one peer is its hedge's winner ... and never the
// deployment's other primaries on its Provider"; #616, PR 5, the first review's point 1).
//
// The fixture is two slots: instances 2, P on A measuring 40 ms (the default), and the second primary's hedge, seeded,
// with the candidate L on A (started at 08:00:20, measuring 18.0 ms, its ranking point 08:11:00) and the winner W on B
// (measuring 20 ms, ready at 08:01:10). 18.0 ms is not faster than W's 20 by more than 10%, but it is faster than P's
// 40 by far: a comparison with P would make L ready and mark P, which is what the rows forbid. W's replacement R is
// seeded on B (a hedge of one for W's need). In each row the winner is gone before L reaches its ranking point, by a
// different road, and the deployment's need is met without L, so the selection leaves L surplus: L ends HedgeLost at
// 08:11:00, kept warm, and P is neither compared nor marked nor drained.
//
// The old structure ranked L only against a winner that was still countable (candidateWinnerOf); a winner that had
// started to drain, or had failed, sent L to the general path, whose peer is the chosen primaries on its Provider.

import (
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// seedReplacementOf makes a replacement of the named Instance on B, a hedge of one for its need (provisionWith with
// the predecessor, as the planned time would), and returns its name.
func (h *klRig) seedReplacementOf(pred string) string {
	h.t.Helper()
	p := h.passOf(hoMD)
	in := p.instanceNamed(pred)
	if in == nil {
		h.t.Fatalf("no Instance %s to seed a replacement for", pred)
	}
	if ok, err := p.provisionWith(in, placeOpts{only: hedgeB}); err != nil || !ok {
		h.t.Fatalf("seeding the replacement of %s: placed %v, %v", pred, ok, err)
	}
	for _, o := range h.liveInstances(hoMD) {
		if o.Annotations[v1alpha1.AnnotationPredecessor] == pred {
			return o.Name
		}
	}
	h.t.Fatalf("the seeded replacement of %s is not among the live Instances", pred)
	return ""
}

// klSlots is the fixture's names: P, the candidate L, the winner W and, in two rows, W's replacement R.
type klSlots struct {
	h          *klRig
	p, l, w, r string
}

// klTwoSlots starts the fixture: P's job starts after pWait, and bScripts are W's job and then any replacement's.
func klTwoSlots(t *testing.T, pWait time.Duration, bScripts ...pfake.Script) *klSlots {
	t.Helper()
	h, p, l, w := klMissingA(t, []pfake.Script{{QueueWait: pWait}, {}}, 2, rk180, bScripts...)
	if in := h.mustInstance(l); ended(in) || in.Annotations[annotationCandidateOf] != w {
		t.Fatalf("L is %s with candidate-of %q at the win, want it kept as W's candidate", in.Status.Phase, in.Annotations[annotationCandidateOf])
	}
	return &klSlots{h: h, p: p, l: l, w: w}
}

// waitsStillKept asserts that L is still a waiting candidate (kept, never ready) at the moment the winner has gone, so
// the row exercises the pass at L's ranking point and not an earlier end.
func (s *klSlots) waitsStillKept() {
	h := s.h
	h.t.Helper()
	in := h.mustInstance(s.l)
	if ended(in) || in.Status.ReadyTime != nil || in.Annotations[annotationCandidateOf] != s.w {
		h.t.Fatalf("L is %s (ready %v, candidate-of %q) when W has gone, want it kept and waiting for its checks", in.Status.Phase, in.Status.ReadyTime, in.Annotations[annotationCandidateOf])
	}
}

// toItsPoint steps from where the clock is to 08:11:00, L's ranking point: in 30 s ticks to 08:09:50, then in 10 s ones.
// The checks are answered a round at a time, so the minute after the worker starts (08:10:20) takes 10 s ticks.
func (s *klSlots) toItsPoint() {
	h := s.h
	h.t.Helper()
	hoStepTo(h.rig, hoT(8, 9, 50), 30*time.Second, nil)
	hoStepTo(h.rig, hoT(8, 11, 0), 10*time.Second, nil)
}

// lostAndPUntouched asserts the outcome: L ended HedgeLost at 08:11:00 with its Allocation kept and a success, P is ready,
// unmarked and not draining, no RankedOut event was raised, and no job was submitted beyond jobs.
func (s *klSlots) lostAndPUntouched(jobs int) {
	h := s.h
	t := h.t
	t.Helper()
	l := h.mustInstance(s.l)
	if !ended(l) || l.Status.EndReason != v1alpha1.ReasonHedgeLost || l.Status.EndTime == nil || !l.Status.EndTime.Time.Equal(hoT(8, 11, 0)) {
		t.Fatalf("L is %s (end %q at %v) at 08:11:00, want ended for HedgeLost at 08:11:00: its winner is gone and the need is met without it, so it is never ranked against P", l.Status.Phase, l.Status.EndReason, l.Status.EndTime)
	}
	if h.released(l.Spec.AllocationRef.Name) {
		t.Fatal("L's Allocation was released, want it kept as warm capacity")
	}
	p := h.mustInstance(s.p)
	if p.Status.Phase != v1alpha1.InstanceReady || p.Annotations[annotationRankedOut] != "" {
		t.Fatalf("P is %s with ranked-out %q, want it ready and unmarked: it was compared with the candidate", p.Status.Phase, p.Annotations[annotationRankedOut])
	}
	if n := len(h.rec.Find(eventReplaced, v1alpha1.ReasonRankedOut)); n != 0 {
		t.Fatalf("%d RankedOut events, want none: nothing was outranked", n)
	}
	if subs := h.sbatches(); subs != jobs {
		t.Fatalf("%d jobs submitted, want %d: no Allocation is made", subs, jobs)
	}
	if o, ok := h.outcomeOf(h.allocUID(l.Spec.AllocationRef.Name)); !ok || o.Result != v1alpha1.StartOutcomeReady {
		t.Fatalf("L's outcome %+v (%v), want a success: it passed every check", o, ok)
	}
}

// TestAKeptLoserIsNeverRankedAgainstAnotherPrimaryWhenItsWinnerLeaves has three rows, one road each for the winner's
// going. Members and W's replacement are seeded.
//   - W drains after its replacement R is ready (the handover's drain start, forced at 08:03 with the pass's own
//     startDrain, since the real one, 09:50:20, is long after L's ranking point): P and R fill the two slots.
//   - W fails after R is ready (its job fails at 08:05:20): P and R fill the slots.
//   - The deployment is scaled down to 1 at 08:03, with L waiting: P's job started later than W's, so P has the later
//     hard end and W, the nearest, is the surplus the scale-down drains; P fills the one slot.
func TestAKeptLoserIsNeverRankedAgainstAnotherPrimaryWhenItsWinnerLeaves(t *testing.T) {
	t.Run("its winner drains after its replacement is ready", func(t *testing.T) {
		s := klTwoSlots(t, 0, pfake.Script{}, pfake.Script{})
		h := s.h
		s.r = h.seedReplacementOf(s.w)
		h.until(5*time.Minute, 10*time.Second, "R ready", func() bool { return h.mustInstance(s.r).Status.Phase == v1alpha1.InstanceReady })
		p := h.passOf(hoMD)
		if err := p.startDrain(p.instanceNamed(s.w)); err != nil {
			t.Fatal(err)
		}
		h.until(5*time.Minute, 10*time.Second, "W ended", func() bool { return ended(h.mustInstance(s.w)) })
		s.waitsStillKept()
		s.toItsPoint()
		s.lostAndPUntouched(4) // P's, L's, W's and R's jobs
		if in := h.mustInstance(s.r); in.Status.Phase != v1alpha1.InstanceReady {
			t.Fatalf("R is %s, want it serving in W's place", in.Status.Phase)
		}
	})
	t.Run("its winner fails after its replacement is ready", func(t *testing.T) {
		fail := 5 * time.Minute
		s := klTwoSlots(t, 0, pfake.Script{FailAfter: &fail}, pfake.Script{})
		h := s.h
		s.r = h.seedReplacementOf(s.w)
		h.until(5*time.Minute, 10*time.Second, "R ready", func() bool { return h.mustInstance(s.r).Status.Phase == v1alpha1.InstanceReady })
		hoStepTo(h.rig, hoT(8, 5, 40), 10*time.Second, nil)
		if in := h.mustInstance(s.w); !ended(in) || in.Status.EndReason == "" || cleanEnd(in.Status.EndReason) {
			t.Fatalf("W is %s (end %q) at 08:05:40, want ended for a failure: the fixture's winner did not fail", in.Status.Phase, in.Status.EndReason)
		}
		s.waitsStillKept()
		s.toItsPoint()
		s.lostAndPUntouched(4)
		if in := h.mustInstance(s.r); in.Status.Phase != v1alpha1.InstanceReady {
			t.Fatalf("R is %s, want it serving in W's place", in.Status.Phase)
		}
	})
	t.Run("the deployment is scaled down and its winner drains", func(t *testing.T) {
		s := klTwoSlots(t, 30*time.Second, pfake.Script{})
		h := s.h
		h.until(5*time.Minute, 10*time.Second, "P and W ready", func() bool {
			return h.mustInstance(s.p).Status.Phase == v1alpha1.InstanceReady && h.mustInstance(s.w).Status.Phase == v1alpha1.InstanceReady
		})
		h.mutateMD(hoMD, func(md *v1alpha1.ModelDeployment) { md.Spec.Instances = 1 })
		h.until(5*time.Minute, 10*time.Second, "W ended", func() bool { return ended(h.mustInstance(s.w)) })
		if in := h.mustInstance(s.w); in.Status.EndReason != v1alpha1.ReasonDrained {
			t.Fatalf("W ended for %q, want Drained: the scale-down took the wrong Instance, so the fixture proves nothing", in.Status.EndReason)
		}
		if in := h.mustInstance(s.p); in.Status.Phase != v1alpha1.InstanceReady {
			t.Fatalf("P is %s after the scale-down, want it serving: the fixture's hard ends are off", in.Status.Phase)
		}
		s.waitsStillKept()
		s.toItsPoint()
		s.lostAndPUntouched(3) // P's, L's and W's jobs
	})
}

// klRound is the fixture of the two rows that follow Opus's review: a missing primary's round of the first member F and
// the extra S on one Provider, a hedge member L on another, kept as F's candidate. With P (instances 2) P is ready on
// L's Provider, A, with no replacement and a time of 40 ms, and the round goes to B; without P (instances 1) the round
// is on A and L on B. F measures 20 ms and has no start delay, so it is ready first and wins; S and L measure 17.9 ms.
type klRound struct {
	h          *klRig
	p, f, s, l string
}

// klExtraRound starts it. The round's jobs start together, and S's engine starts its worker sDelay after, L's lDelay
// after, so S reaches its ranking point 40 s after sDelay and L 40 s after lDelay.
func klExtraRound(t *testing.T, withP bool, sDelay, lDelay time.Duration) *klRound {
	t.Helper()
	h := newKlRig(t)
	rd := &klRound{h: h}
	roundOn, candOn := rigProvider, hedgeB
	if withP {
		// P is ready on A at 08:01:10; at 08:01:20 the deployment becomes 2 Instances on B then A, and its pass makes F
		// and S on B, the first allowed Provider with room.
		h.batch.Enqueue(pfake.Script{}, pfake.Script{})   // P's job, then L's
		h.b.batch.Enqueue(pfake.Script{}, pfake.Script{}) // F's and S's
		reservedStart(h.rig, hoMD, klMD(1, 0))
		rd.p = h.instances(hoMD)[0].Name
		h.mutateMD(hoMD, func(md *v1alpha1.ModelDeployment) {
			md.Spec.Providers.Names = []string{hedgeB, rigProvider}
			md.Spec.Instances, md.Spec.OverAllocation = 2, ptrTo(int32(1))
		})
		roundOn, candOn = hedgeB, rigProvider
	} else {
		h.batch.Enqueue(pfake.Script{}, pfake.Script{}) // F's and S's
		h.b.batch.Enqueue(pfake.Script{})               // L's
		md := mdSpec(hoMD)
		klMD(1, 1)(md)
		h.create(md)
	}
	h.clock.Advance(10 * time.Second)
	h.Step() // the pass that makes the missing primary and, beside it, the extra, both on the round's Provider
	ins := rkRoles(h.onProvider(hoMD, roundOn))
	if len(ins) != 2 {
		t.Fatalf("%d Instances on %s after the pass, want the missing primary and the extra", len(ins), roundOn)
	}
	rd.f, rd.s = ins[0], ins[1]
	rd.l = h.seedHedgeMember(hoMD, rd.f, "", candOn)
	set := func(name string, tpot, delay time.Duration) {
		a := h.mustInstance(name).Spec.AllocationRef.Name
		h.tpot[a], h.delay[a] = tpot, delay
	}
	set(rd.f, rk200, 0)
	set(rd.s, rk179, sDelay)
	set(rd.l, rk179, lDelay)
	return rd
}

// keptAtTheWin runs to F's win and asserts L is kept as F's candidate.
func (rd *klRound) keptAtTheWin() {
	h := rd.h
	h.t.Helper()
	h.until(15*time.Minute, 10*time.Second, "F ready", func() bool { return h.mustInstance(rd.f).Status.Phase == v1alpha1.InstanceReady })
	if in := h.mustInstance(rd.l); ended(in) || in.Status.ReadyTime != nil || in.Annotations[annotationCandidateOf] != rd.f {
		h.t.Fatalf("L is %s (ready %v, candidate-of %q) at F's win, want it kept as F's candidate", in.Status.Phase, in.Status.ReadyTime, in.Annotations[annotationCandidateOf])
	}
}

// outranked runs to the pass in which S becomes ready, and asserts S outranked F: F carries the mark naming S and is
// still ready (the front's window), and L is still waiting.
func (rd *klRound) outranked() {
	h := rd.h
	h.t.Helper()
	h.until(15*time.Minute, 10*time.Second, "S ready", func() bool { return h.mustInstance(rd.s).Status.Phase == v1alpha1.InstanceReady })
	f := h.mustInstance(rd.f)
	if f.Status.Phase != v1alpha1.InstanceReady || f.Annotations[annotationRankedOut] != rd.s {
		h.t.Fatalf("F is %s with ranked-out %q after S became ready, want it ready and marked by %s", f.Status.Phase, f.Annotations[annotationRankedOut], rd.s)
	}
	if in := h.mustInstance(rd.l); ended(in) || in.Status.ReadyTime != nil {
		h.t.Fatalf("L is %s (ready %v) before its ranking point, want it waiting", in.Status.Phase, in.Status.ReadyTime)
	}
}

// settled runs until L has ended or become ready and returns it.
func (rd *klRound) settled() *v1alpha1.Instance {
	h := rd.h
	h.t.Helper()
	h.until(10*time.Minute, 10*time.Second, "L ended or ready", func() bool {
		in := h.mustInstance(rd.l)
		return ended(in) || in.Status.ReadyTime != nil
	})
	return h.mustInstance(rd.l)
}

// TestAKeptLoserDoesNotRewriteTheMarkOfAWinnerAnotherCandidateOutranked (Opus, point 1a; instances 1, no P): F is the
// round's winner and S, the extra, passes and outranks it, so F carries the ranked-out mark naming S and stays ready
// through the front's 10 s window. L passes in that window, 10 s after S: its winner is F, which is already outranked,
// so L is not ranked against it. L ends HedgeLost, kept warm; F's mark still names S, and only S serves in the end: one
// ready Instance for the one slot, where ranking L against F would rewrite the mark to L and leave S and L both ready
// for one slot, with step 7 draining whichever has the nearer hard end. Members are seeded.
func TestAKeptLoserDoesNotRewriteTheMarkOfAWinnerAnotherCandidateOutranked(t *testing.T) {
	rd := klExtraRound(t, false, 4*time.Minute, 4*time.Minute+10*time.Second)
	h := rd.h
	rd.keptAtTheWin()
	rd.outranked()
	h.clock.Advance(10 * time.Second)
	h.Step() // L passes, in F's window
	l := h.mustInstance(rd.l)
	if l.Status.ReadyTime != nil || !ended(l) || l.Status.EndReason != v1alpha1.ReasonHedgeLost {
		t.Fatalf("L is %s (end %q, ready %v) in F's window, want ended for HedgeLost: F was outranked, so L is not ranked against it", l.Status.Phase, l.Status.EndReason, l.Status.ReadyTime)
	}
	if got := h.mustInstance(rd.f).Annotations[annotationRankedOut]; got != rd.s {
		t.Fatalf("F's ranked-out mark names %q, want %q: a mark is never rewritten", got, rd.s)
	}
	h.until(3*time.Minute, 10*time.Second, "F ended", func() bool { return ended(h.mustInstance(rd.f)) })
	if r := h.mustInstance(rd.f).Status.EndReason; r != v1alpha1.ReasonRankedOut {
		t.Fatalf("F ended for %q, want RankedOut", r)
	}
	if n := h.readyCount(hoMD); n != 1 || h.mustInstance(rd.s).Status.Phase != v1alpha1.InstanceReady {
		t.Fatalf("%d ready Instances, S %s, want S alone: one Instance for the one slot", n, h.mustInstance(rd.s).Status.Phase)
	}
	if h.released(h.mustInstance(rd.l).Spec.AllocationRef.Name) {
		t.Fatal("L's Allocation was released, want it kept as warm capacity")
	}
	if subs := h.sbatches(); subs != 3 {
		t.Fatalf("%d jobs submitted, want F's, S's and L's", subs)
	}
}

// TestAKeptLoserIsNeverRankedAgainstAPrimaryOnceTheWinnerSIsOutranked (Opus, point 1b; instances 2): P, ready on A with
// no replacement and measuring 40 ms, is on L's Provider. The round, F and the extra S, is on B. S outranks F, which
// drains and ends RankedOut; S and P fill the two slots. L, F's candidate, passes a minute after S, when F has ended:
// its winner is gone and the selection leaves it surplus, so it ends HedgeLost. It is not compared with P, whom its
// 17.9 ms would outrank by far. Members are seeded.
func TestAKeptLoserIsNeverRankedAgainstAPrimaryOnceTheWinnerSIsOutranked(t *testing.T) {
	rd := klExtraRound(t, true, 4*time.Minute, 5*time.Minute)
	h := rd.h
	rd.keptAtTheWin()
	rd.outranked()
	h.until(3*time.Minute, 10*time.Second, "F ended", func() bool { return ended(h.mustInstance(rd.f)) })
	if in := h.mustInstance(rd.l); ended(in) || in.Status.ReadyTime != nil {
		t.Fatalf("L is %s (ready %v) when F has ended, want it still waiting for its checks: the fixture's timeline is off", in.Status.Phase, in.Status.ReadyTime)
	}
	l := rd.settled()
	if !ended(l) || l.Status.EndReason != v1alpha1.ReasonHedgeLost {
		t.Fatalf("L is %s (end %q, ready %v), want ended for HedgeLost: it must not be ranked against P", l.Status.Phase, l.Status.EndReason, l.Status.ReadyTime)
	}
	p := h.mustInstance(rd.p)
	if p.Status.Phase != v1alpha1.InstanceReady || p.Annotations[annotationRankedOut] != "" {
		t.Fatalf("P is %s with ranked-out %q, want it ready and unmarked: it was compared with the candidate", p.Status.Phase, p.Annotations[annotationRankedOut])
	}
	for _, e := range h.rec.Find(eventReplaced, v1alpha1.ReasonRankedOut) {
		if e.Name == rd.p {
			t.Fatalf("a RankedOut event regards P: %+v", e)
		}
	}
	if n := h.readyCount(hoMD); n != 2 {
		t.Fatalf("%d ready Instances, want P and S", n)
	}
	if subs := h.sbatches(); subs != 4 {
		t.Fatalf("%d jobs submitted, want P's, L's, F's and S's", subs)
	}
}
