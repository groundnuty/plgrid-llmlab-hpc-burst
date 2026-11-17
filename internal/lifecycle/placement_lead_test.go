package lifecycle

// A warm Allocation takes a replacement only when its drain start is later than its predecessor's by AT LEAST the
// lead an Instance needs: the drain window plus handoverMargin plus health.startTimeout, 2 h 10 min + 30 min + 1 h =
// 3 h 40 min in the rig (HBEP-7, Provisioning, Where; Hedging (beta), Started losers are kept; amendment #711; #724's
// reviews). The rows put a spare's drain start EXACTLY the lead after the primary's, where it is taken, and one second
// short, where the placement goes on to the next step, through the three paths that place on warm capacity: the
// replacement's cross-Provider sweep, a hedge member, and the no-overlap decision. The primary starts at 09:00:20 with
// a 4 h limit (hard end 13:00:20, drain start 10:50:20); B's spare started at 08:00:20 on an 8 h 40 min partition,
// hard end 16:40:20, drain start 14:30:20, which is 3 h 40 min after the primary's; one second short is the primary
// starting a second later (ordOpts.primaryOffset), its drain start 10:50:21.

import (
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	afake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// ordLeadBMax is B's partition limit for the lead rows: its spare, started at 08:00:20, has its hard end at 16:40:20.
const ordLeadBMax = 8*time.Hour + 40*time.Minute

// leadCases are the two sides of the boundary: the primary's start offset, and whether the spare is taken.
var leadCases = []struct {
	name   string
	offset time.Duration
	taken  bool
}{
	{"a spare draining exactly the lead after the primary", 0, true},
	{"a spare draining one second short of the lead", time.Second, false},
}

// TestTheSweepTakesAWarmAllocationOnlyByTheLead: P is on A, whose one submit slot P fills, so its replacement cannot go
// to its own Provider; B, of A's class and with no limit, holds the warm spare. At the replacement's planned time,
// 10:19:30 plus the offset, the cross-Provider warm sweep takes the spare when it drains exactly the lead after P, and
// goes on to the next step, a new Allocation on B, when it drains one second short of it.
func TestTheSweepTakesAWarmAllocationOnlyByTheLead(t *testing.T) {
	for _, c := range leadCases {
		t.Run(c.name, func(t *testing.T) {
			o := newOrdRig(t, ordOpts{aLimit: 1, bMaxTime: ordLeadBMax, primaryOffset: c.offset})
			o.create(ordMD(hoMD))
			wb := o.warmUp(hoMD, ordB)
			if he := o.mustAllocation(wb).Status.AtProvider.HardEnd; he == nil || !he.Time.Equal(hoT(16, 40, 20)) {
				t.Fatalf("the spare's hard end is %v, want 16:40:20", he)
			}
			p := o.primaryOnA(hoMD, rigProvider, ordB)
			_, subsB, _ := o.subs()
			r := o.replacementAt(p)
			prov, alloc := where(r)
			onB := o.allocsOn(hoMD, ordB)
			if c.taken {
				if prov != ordB || alloc != wb || !sameAllocs(onB, []string{wb}) {
					t.Fatalf("the replacement is on %s, Allocation %s; B has %v; want B's spare %s, which drains exactly the lead after P", prov, alloc, onB, wb)
				}
				if _, b, _ := o.subs(); b != subsB {
					t.Fatalf("B received %d submissions, want none", b-subsB)
				}
				return
			}
			if prov != ordB || alloc == wb || len(onB) != 2 || o.hostsLive(hoMD, wb) {
				t.Fatalf("the replacement is on %s, Allocation %s; B has %v; want a new Allocation on B beside the spare %s, which drains one second short of the lead and hosts nothing", prov, alloc, onB, wb)
			}
		})
	}
}

// TestAHedgeMemberTakesAWarmAllocationOnlyByTheLead (Fable's scenario): P is on A with no limit, so its replacement R1
// is a new Allocation on A, pending behind 10 h; the deployment allows A, B, C with hedgeWidth 2. B's only submit slot
// is held by its spare (B has a limit of 1), and C, of another class, is free. At R1's hedge time, 10:29:40 plus the
// offset, the hedge's second member R2 goes to the spare when it drains exactly the lead after P; one second short of
// it, R2 does not take it, and goes to the next step after B's warm and submit steps, a new Allocation on C, since
// B's submission is refused for its limit. So a member that wins never lands on a spare that drains with P, and R1 is
// cancelled only through the hedge's ordinary resolution.
func TestAHedgeMemberTakesAWarmAllocationOnlyByTheLead(t *testing.T) {
	for _, c := range leadCases {
		t.Run(c.name, func(t *testing.T) {
			o := newOrdRig(t, ordOpts{bLimit: 1, bMaxTime: ordLeadBMax, withC: true, primaryOffset: c.offset})
			o.batch.Enqueue(pfake.Script{}, pfake.Script{QueueWait: 10 * time.Hour})
			md := ordMD(hoMD)
			md.Spec.HedgeWidth = nil // the default, 2
			o.create(md)
			wb := o.warmUp(hoMD, ordB)
			p := o.primaryOnA(hoMD, rigProvider, ordB, ordC)
			r1 := o.replacementAt(p)
			if prov, _ := where(r1); prov != rigProvider {
				t.Fatalf("R1 is on %s, want A, its predecessor's Provider", prov)
			}
			_, subsB, subsC := o.subs()
			var r2 v1alpha1.Instance
			o.until(30*time.Minute, 10*time.Second, "R1's hedge gets its second member", func() bool {
				for _, in := range hoRepl(o.rig, p.Name) {
					if in.Name != r1.Name && !ended(&in) {
						r2 = in
					}
				}
				return r2.Name != ""
			})
			if !o.clock.Now().Equal(hoT(10, 29, 40).Add(c.offset)) {
				t.Fatalf("R2 was made at %s, want R1's hedge time, 10:29:40 plus the offset", o.clock.Now().Format(time.TimeOnly))
			}
			prov, alloc := where(r2)
			if c.taken {
				if prov != ordB || alloc != wb || !sameAllocs(o.allocsOn(hoMD, ordB), []string{wb}) || len(o.allocsOn(hoMD, ordC)) != 0 {
					t.Fatalf("R2 is on %s, Allocation %s; B has %v, C has %v; want B's spare %s, which drains exactly the lead after P", prov, alloc, o.allocsOn(hoMD, ordB), o.allocsOn(hoMD, ordC), wb)
				}
				if _, b, cc := o.subs(); b != subsB || cc != subsC {
					t.Fatalf("B received %d and C %d submissions, want none", b-subsB, cc-subsC)
				}
				return
			}
			onC := o.allocsOn(hoMD, ordC)
			if prov != ordC || len(onC) != 1 || alloc != onC[0] || !sameAllocs(o.allocsOn(hoMD, ordB), []string{wb}) || o.hostsLive(hoMD, wb) {
				t.Fatalf("R2 is on %s, Allocation %s; B has %v, C has %v; want a new Allocation on C, and B's spare %s, one second short of the lead, hosting nothing",
					prov, alloc, o.allocsOn(hoMD, ordB), onC, wb)
			}
		})
	}
}

// TestTheNoOverlapDecisionTakesAWarmAllocationOnlyByTheLead: both Providers have a submit limit of 1, which P fills on A
// and B's spare fills on B, so the replacement has no slot anywhere and only a spare could take it. The spare that
// drains exactly the lead after P is room beside (reusableWarm reads warmFor): the replacement goes there at its
// planned time with no gap. One second short of the lead it is no room, and the mode is entered as with no warm
// Allocation at all: no replacement at its planned time, no submission on B, NoOverlapSlot shortly after the drain
// start, 10:50:21 plus the offset.
func TestTheNoOverlapDecisionTakesAWarmAllocationOnlyByTheLead(t *testing.T) {
	for _, c := range leadCases {
		t.Run(c.name, func(t *testing.T) {
			o := newOrdRig(t, ordOpts{aLimit: 1, bLimit: 1, bMaxTime: ordLeadBMax, primaryOffset: c.offset})
			o.create(ordMD(hoMD))
			wb := o.warmUp(hoMD, ordB)
			p := o.primaryOnA(hoMD, rigProvider, ordB)
			_, subsB, _ := o.subs()
			planned := hoT(10, 19, 30).Add(c.offset)
			if c.taken {
				r := o.replacementAt(p)
				prov, alloc := where(r)
				if prov != ordB || alloc != wb || r.Annotations[annotationNoOverlapSuccessor] != "" {
					t.Fatalf("the replacement is on %s, Allocation %s, marked %q; want B's spare %s, unmarked", prov, alloc, r.Annotations[annotationNoOverlapSuccessor], wb)
				}
				if cd := o.condition(hoMD, v1alpha1.ConditionDegraded); cd.Reason == v1alpha1.ReasonNoOverlapSlot {
					t.Fatalf("Degraded = %s/%s %q: the spare takes the replacement, so there is no gap", cd.Status, cd.Reason, cd.Message)
				}
				if _, b, _ := o.subs(); b != subsB {
					t.Fatalf("B received %d submissions, want none", b-subsB)
				}
				return
			}
			hoStepTo(o.rig, planned.Add(-10*time.Second), 5*time.Minute, nil)
			hoStepTo(o.rig, planned.Add(10*time.Second), 10*time.Second, nil)
			if reps := hoRepl(o.rig, p.Name); len(reps) != 0 {
				t.Fatalf("%d replacements at %s, want none: the spare drains one second short of the lead, so it is no room beside and both slots are taken", len(reps), o.clock.Now().Format(time.TimeOnly))
			}
			hoStepTo(o.rig, hoT(10, 50, 50).Add(c.offset), 10*time.Second, nil)
			if cd := o.condition(hoMD, v1alpha1.ConditionDegraded); cd.Reason != v1alpha1.ReasonNoOverlapSlot {
				t.Fatalf("at %s Degraded = %s/%s %q, want NoOverlapSlot", o.clock.Now().Format(time.TimeOnly), cd.Status, cd.Reason, cd.Message)
			}
			if _, b, _ := o.subs(); b != subsB {
				t.Fatalf("B received %d submissions, want none", b-subsB)
			}
		})
	}
}

// TestAStartedLoserThirtyMinutesAfterItsPredecessorIsNotPlacedOn is HBEP-7's "A started loser is kept" row as #711
// revises it, on its own timeline (P starts at 00:00, here 08:00:20, with a 12 h limit: hard end 20:00:20, drain start
// 17:50:20). P's replacement hedge has R1 on A, whose job starts 30 min after P's, 08:30:20, and R2 on B, whose job
// starts at 08:20:30 and which is ready at about 08:55, when R1, started and not ready, ends HedgeLost and its Allocation
// is kept (hard end 20:30:20, drain start 18:20:20, 30 min after P's). B's member fails at 09:30 (the HBEP row has it at
// 03:00 of its timeline; the rig's B has a 4 h partition, whose drain would end R2 at 10:10:30, so it fails before that):
// P's new replacement is NOT placed on A's kept Allocation, its drain start being only 30 min after P's, less than the lead, 3 h 40 min. It is
// submitted anew, on A, P's own Provider, and the kept Allocation stays, with no Instance (HBEP-7, Test plan). The
// row of TestAStartedLosersAllocationTakesTheWinnersReplacement is the other side, on a timeline 12 h later.
func TestAStartedLoserThirtyMinutesAfterItsPredecessorIsNotPlacedOn(t *testing.T) {
	h := newHedgeRig(t, withMaxTime(12*time.Hour))
	var r1Alloc string
	h.rig.engine = func(alloc string) afake.Engine {
		if a, ok := h.allocation(alloc); ok && a.Labels[v1alpha1.LabelProvider] == hedgeB {
			return afake.Engine{StartDelay: 34 * time.Minute}
		}
		if alloc != "" && alloc == r1Alloc {
			return afake.Engine{StartDelay: 6 * time.Hour} // started, never ready
		}
		return afake.Engine{}
	}
	p := replacementStart(t, h, 29*time.Minute+50*time.Second) // R1 submitted at 08:00:30, started at 08:30:20
	h.b.batch.Enqueue(pfake.Script{QueueWait: 20 * time.Minute})
	setWaits(h.rig, rigProvider, 20, 10*time.Hour)
	h.clock.Advance(10 * time.Second)
	h.Step() // 08:00:20: P's job starts, R1 is made, and B's member R2 with it
	var r1, r2 string
	for _, r := range hoRepl(h.rig, p) {
		if r.Labels[v1alpha1.LabelProvider] == rigProvider {
			r1 = r.Name
		} else {
			r2 = r.Name
		}
	}
	if r1 == "" || r2 == "" {
		t.Fatalf("want R1 on A and R2 on B at 08:00:20, got %+v", hoRepl(h.rig, p))
	}
	r1Alloc = h.mustInstance(r1).Spec.AllocationRef.Name
	h.until(time.Hour, 10*time.Second, "R2 ready", func() bool { return h.mustInstance(r2).Status.Phase == v1alpha1.InstanceReady })
	if a := h.allocOrNil(r1Alloc); a == nil || a.Status.AtProvider.StartTime == nil || !a.Status.AtProvider.StartTime.Time.Equal(hoT(8, 30, 20)) ||
		a.Status.AtProvider.HardEnd == nil || !a.Status.AtProvider.HardEnd.Time.Equal(hoT(20, 30, 20)) {
		t.Fatalf("R1's Allocation at R2's win: %+v, want started at 08:30:20 with hard end 20:30:20", a)
	}
	if lost := h.mustInstance(r1); !ended(lost) || lost.Status.EndReason != v1alpha1.ReasonHedgeLost || h.released(r1Alloc) {
		t.Fatalf("R1 is %s, ended for %q, its Allocation released %v: want HedgeLost with the Allocation kept", lost.Status.Phase, lost.Status.EndReason, h.released(r1Alloc))
	}
	h.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.HedgeWidth = ptrTo(int32(1)) }) // the row is about placement, not growth
	hoStepTo(h.rig, hoT(9, 30, 0), 5*time.Minute, nil)
	subsA := h.batch.Count("sbatch")
	r2Alloc := h.mustAllocation(h.mustInstance(r2).Spec.AllocationRef.Name)
	h.b.batch.CancelExternally(r2Alloc.Annotations[v1alpha1.AnnotationJobID])
	var next v1alpha1.Instance
	h.until(10*time.Minute, 10*time.Second, "P's new replacement", func() bool {
		for _, in := range hoRepl(h.rig, p) {
			if in.Name != r1 && in.Name != r2 && !ended(&in) {
				next = in
			}
		}
		return next.Name != ""
	})
	prov, alloc := where(next)
	h.clock.Advance(10 * time.Second)
	h.Step() // the provider submits the new Allocation's job in the step after its creation
	if prov != rigProvider || alloc == r1Alloc || h.batch.Count("sbatch") != subsA+1 {
		t.Fatalf("P's new replacement is on %s, Allocation %s, with %d new submissions on A; want a new Allocation on A (1 submission), not R1's kept %s: it drains only 30 min after P", prov, alloc, h.batch.Count("sbatch")-subsA, r1Alloc)
	}
	if h.released(r1Alloc) || len(h.liveOnAllocation(r1Alloc)) != 0 {
		t.Fatalf("R1's kept Allocation %s is released %v or hosts an Instance: want it kept, empty, until its hard end", r1Alloc, h.released(r1Alloc))
	}
}
