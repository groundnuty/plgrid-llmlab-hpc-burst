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

// toItsPoint steps from where the clock is to 08:11:00, L's ranking point.
func (s *klSlots) toItsPoint() {
	h := s.h
	h.t.Helper()
	hoStepTo(h.rig, hoT(8, 10, 50), 30*time.Second, nil)
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
