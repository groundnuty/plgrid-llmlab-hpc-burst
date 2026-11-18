package lifecycle

// A Scale Operation's plan over a hedge's members (HBEP-7, Hedging (beta), Interactions, Operations: "scale-down and
// Operations: a hedge goes whole, each member its own target"; HBEP-10, The plan and What needs approval; #604, #589
// PR 4; #725's comment). Every target below is written out by hand from HBEP-10's table and the fixtures' times
// (opfixture_test.go: the hard end is T0+4h, 13:00; the plan is made at T0+1h, 10:00), never read from
// scaleDownTargets: the plan's own selection is the code under test, so a row that took its targets from that
// function would agree with it whatever it chose.

import (
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// TestAScalePlanListsEachMemberOfAHedgeAsItsOwnTarget: instances 2, k = 0. R is ready on sano and stays. The second
// primary is a hedge of two: m1, pending on sano, created at 09:10, and m2, on athena, whose job has started and whose Instance is still starting.
// Scale(1) drops the hedge whole, so BOTH members are targets, each with the effect and the approval row its own state
// gives:
//   - m1 (pending): CancelPending, which needs approval whatever its age (the table's row for a pending Allocation,
//     "other than a hedge loser": a member of a hedge with no winner is no loser); its Allocation, emptied and not
//     started, is cancelled too, with the same row;
//   - m2 (started, not ready): EndStarting, the drain's rule by what its Allocation's retention does next: under Keep
//     the Allocation stays until its hard end, 13:00, and the end needs no approval; under ReleaseWhenIdle with a 30
//     minute limit retention releases it at 10:30, before its hard end, and the end needs approval.
//
// Targets come in kind-then-name order. The Instances' cost is present for each (an Instance target always shows
// it), and an Allocation that is only kept shows none.
func TestAScalePlanListsEachMemberOfAHedgeAsItsOwnTarget(t *testing.T) {
	for _, c := range []struct {
		name      string
		retention *v1alpha1.RetentionPolicy
		want      []wantTarget
	}{
		{
			name: "the started member's Allocation is kept to its hard end: its end needs no approval",
			want: []wantTarget{
				{kind: tAlloc, name: "alloc-m1", effect: v1alpha1.PlanEffectCancelPending, needs: true, rule: litCancelPending, cost: true},
				{kind: tAlloc, name: "alloc-m2", effect: v1alpha1.PlanEffectKeep, needs: false, rule: litKeep + ", until 2026-11-16T13:00:00Z"},
				{kind: tInst, name: "m1", effect: v1alpha1.PlanEffectCancelPending, needs: true, rule: litCancelPending, cost: true},
				{kind: tInst, name: "m2", effect: v1alpha1.PlanEffectEndStarting, needs: false, rule: litEndKept, cost: true},
			},
		},
		{
			name:      "the started member's Allocation is released at 10:30 by its retention: its end needs approval",
			retention: &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle, IdleLimit: durPtr("30m")},
			want: []wantTarget{
				{kind: tAlloc, name: "alloc-m1", effect: v1alpha1.PlanEffectCancelPending, needs: true, rule: litCancelPending, cost: true},
				{kind: tAlloc, name: "alloc-m2", effect: v1alpha1.PlanEffectKeep, needs: false, rule: litKeep + ", until 2026-11-16T10:30:00Z"},
				{kind: tInst, name: "m1", effect: v1alpha1.PlanEffectCancelPending, needs: true, rule: litCancelPending, cost: true},
				{kind: tInst, name: "m2", effect: v1alpha1.PlanEffectEndStarting, needs: true, rule: litEndReleased, cost: true},
			},
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := newOpFixture(t, 2)
			f.addProvider("athena")
			f.withGlobalPolicy()
			f.addInstance(instOpts{name: "r", phase: v1alpha1.InstanceReady, started: true, hardEnd: tp(opHardEnd), created: 0})
			f.addInstance(instOpts{name: "m1", phase: v1alpha1.InstancePending, created: 10 * time.Minute, submit: tp(opT0.Add(10 * time.Minute))})
			f.addInstance(instOpts{name: "m2", phase: v1alpha1.InstanceStarting, started: true, hardEnd: tp(opHardEnd), created: 11 * time.Minute,
				provider: "athena", hedge: "m1", retention: c.retention})
			plan, refusal := f.plan(scaleOp(1))
			checkPlan(t, plan, refusal, c.want)
			for _, tg := range plan.Targets {
				if tg.Kind == tInst && tg.Name == "m2" && tg.Provider != "athena" {
					t.Errorf("m2's target is on Provider %q, want athena: each member is a target of its own Provider", tg.Provider)
				}
			}
		})
	}
}

// TestAScalePlanOverASurplusHedgeListsOnlyTheMembersStep7Trims: instances 2, k = 1, so each Provider leaves one non-ready
// surplus Instance alone. R1 (hard end 13:00) and R2 (14:00) are ready on sano. The non-ready surplus, in the
// selection's fixed order (the older first): S, pending on athena, created 09:02; H, pending on sano, created 09:05; H's member
// hb, pending on athena, created 09:06. At the current count, 2, R1 and R2 are chosen:
//   - S takes athena's room, H takes sano's, so hb finds athena with none and is the one trimmed, as over-allocation's own
//     trim (cause overAllocationTrim, which needs no approval); H and S stay pending with their queue ages.
//
// Scale(1) keeps R2 (the later hard end) and takes R1, ready surplus, which drains (R2 stays ready) and is no trim
// (lowered: the current count does not take it), kept under Keep until 13:00. The surplus hedge's room is the same, so
// the plan lists R1 and hb, and neither H nor S, and not the hedge whole.
func TestAScalePlanOverASurplusHedgeListsOnlyTheMembersStep7Trims(t *testing.T) {
	f := newOpFixture(t, 2)
	f.overAllocation(1)
	f.addProvider("athena")
	f.withGlobalPolicy()
	f.addInstance(instOpts{name: "r1", phase: v1alpha1.InstanceReady, started: true, hardEnd: tp(opHardEnd), created: 0})
	f.addInstance(instOpts{name: "r2", phase: v1alpha1.InstanceReady, started: true, hardEnd: tp(opHardEnd.Add(time.Hour)), created: time.Minute})
	f.addInstance(instOpts{name: "s", phase: v1alpha1.InstancePending, created: 2 * time.Minute, provider: "athena", submit: tp(opT0.Add(2 * time.Minute))})
	f.addInstance(instOpts{name: "h", phase: v1alpha1.InstancePending, created: 5 * time.Minute, submit: tp(opT0.Add(5 * time.Minute))})
	f.addInstance(instOpts{name: "hb", phase: v1alpha1.InstancePending, created: 6 * time.Minute, provider: "athena", hedge: "h", submit: tp(opT0.Add(6 * time.Minute))})
	plan, refusal := f.plan(scaleOp(1))
	checkPlan(t, plan, refusal, []wantTarget{
		{kind: tAlloc, name: "alloc-hb", effect: v1alpha1.PlanEffectCancelPending, needs: false, rule: litOverAllocation, cost: true},
		{kind: tAlloc, name: "alloc-r1", effect: v1alpha1.PlanEffectKeep, needs: false, rule: litKeep + ", until 2026-11-16T13:00:00Z"},
		{kind: tInst, name: "hb", effect: v1alpha1.PlanEffectCancelPending, needs: false, rule: litOverAllocation, cost: true},
		{kind: tInst, name: "r1", effect: v1alpha1.PlanEffectDrainInstance, needs: false, rule: litDrainKept, cost: true},
	})
}
