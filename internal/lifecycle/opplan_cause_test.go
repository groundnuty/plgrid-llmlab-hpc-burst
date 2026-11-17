package lifecycle

// The cause of a scale-down target, and the managed-source End (#589, PR 4). Every expectation below is
// worked out by hand from the rulings (HBEP-7, How it counts, What step 7 takes; HBEP-10, What needs
// approval; #616's definition of the cause, agreed 16 Nov), never read from scaleDownTargets' own output:
//
//	overAllocationTrim: the target is in the selection at the CURRENT count AND is non-ready surplus beyond k
//	                    on its Provider, or the ranked-out-marked peer.
//	lowered:            everything else, a ready surplus Instance that is not marked included.
//
// The cause is decided by membership in the current count's selection, not by state. The times are those of
// opfixture_test.go: hard end T0+4h (13:00), plan at T0+1h (10:00).

import (
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

const (
	litOverAllocation = "over-allocation's own trim under the agreed spec"
	litManagedEnd     = "End a managed-source deployment: it holds no Allocation, so nothing is released"
	litNoCapacityHeld = "no HPC capacity held"
)

// An absent cause reads as lowered; the two typed causes read as their words.
func TestPlanCauseReadsTheTargetsCause(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   scaleTarget
		want string
	}{
		{"absent reads as lowered", scaleTarget{}, "lowered"},
		{"lowered", scaleTarget{cause: scaleLowered}, "lowered"},
		{"over-allocation's trim", scaleTarget{cause: scaleOverAllocationTrim}, "overAllocationTrim"},
	} {
		if got := planCause(tc.in); got != tc.want {
			t.Errorf("%s: %q, want %q", tc.name, got, tc.want)
		}
	}
}

// k = 1, two desired. R1 and R2 are ready (hard ends 13:00 and 14:00), E1 and E2 pending extras. At the
// current count, 2: the primaries are R2 then R1 (the later hard end ranks first), the surplus is E1, E2 in
// the selection's order (the older first). k = 1 leaves E1 alone and takes E2: E2 is non-ready surplus beyond
// k, over-allocation's own trim. Scale(1): the primaries are R2 alone, the surplus R1, E1, E2: R1 is ready
// and taken (lowered, it is no trim); E1 is within k and left alone; E2 is taken, and it is a trim at the
// current count too, so it needs no approval, where a lowered pending cancel would (HBEP-10's table). It is
// still listed, with its cost, and so is its Allocation.
func TestANonReadyExtraBeyondKIsOverAllocationsTrimAndNeedsNoApproval(t *testing.T) {
	f := newOpFixture(t, 2)
	f.overAllocation(1)
	f.withGlobalPolicy()
	f.addInstance(instOpts{name: "r1", phase: v1alpha1.InstanceReady, started: true, hardEnd: tp(opHardEnd), created: 0})
	f.addInstance(instOpts{name: "r2", phase: v1alpha1.InstanceReady, started: true, hardEnd: tp(opHardEnd.Add(time.Hour)), created: time.Minute})
	f.addInstance(instOpts{name: "e1", phase: v1alpha1.InstancePending, created: 2 * time.Minute, submit: tp(opT0.Add(10 * time.Minute))})
	f.addInstance(instOpts{name: "e2", phase: v1alpha1.InstancePending, created: 3 * time.Minute, submit: tp(opT0.Add(20 * time.Minute))})
	plan, refusal := f.plan(scaleOp(1))
	checkPlan(t, plan, refusal, []wantTarget{
		{kind: tAlloc, name: "alloc-e2", effect: v1alpha1.PlanEffectCancelPending, needs: false, rule: litOverAllocation, cost: true},
		{kind: tAlloc, name: "alloc-r1", effect: v1alpha1.PlanEffectKeep, needs: false, rule: litKeep + ", until 2026-11-16T13:00:00Z"},
		{kind: tInst, name: "e2", effect: v1alpha1.PlanEffectCancelPending, needs: false, rule: litOverAllocation, cost: true},
		{kind: tInst, name: "r1", effect: v1alpha1.PlanEffectDrainInstance, needs: false, rule: litDrainKept, cost: true},
	})
}

// k = 1, two desired. C is the candidate that outranked R2: ready, healthy since 09:50, so the front's probe
// interval (10 s) has long passed at 10:00 and R2's drain is due. R2 carries the ranked-out mark. Hard ends:
// C 15:00, R2 14:00, R1 13:00. At the current count, 2: C chosen, R2 skipped (marked: surplus whatever its
// rank), R1 chosen; the surplus is R2 alone, taken as the marked peer, a trim. Scale(1): C chosen; the surplus
// is R2 and R1, both ready and so both taken; R2 is a trim (marked, in the current selection), R1 is lowered
// (it is not in the current surplus). C stays ready, so both drains are DrainInstance. R2 and R1 are both
// under ReleaseWhenIdle with a 30 minute limit, so BOTH drains would need approval by the table; R1 does, and
// R2, the marked peer, needs none. Each Allocation is emptied at 10:00 and kept until 10:30.
func TestARankedOutPeerIsOverAllocationsTrimAndNeedsNoApproval(t *testing.T) {
	idle := &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle, IdleLimit: durPtr("30m")}
	f := newOpFixture(t, 2)
	f.overAllocation(1)
	f.withGlobalPolicy()
	f.addInstance(instOpts{name: "r1", phase: v1alpha1.InstanceReady, started: true, hardEnd: tp(opHardEnd), created: 0, retention: idle})
	f.addInstance(instOpts{name: "r2", phase: v1alpha1.InstanceReady, started: true, hardEnd: tp(opHardEnd.Add(time.Hour)), created: time.Minute, retention: idle, rankedOut: "c"})
	f.addInstance(instOpts{name: "c", phase: v1alpha1.InstanceReady, started: true, hardEnd: tp(opHardEnd.Add(2 * time.Hour)), created: 2 * time.Minute, healthySince: tp(opT0.Add(50 * time.Minute))})
	plan, refusal := f.plan(scaleOp(1))
	checkPlan(t, plan, refusal, []wantTarget{
		{kind: tAlloc, name: "alloc-r1", effect: v1alpha1.PlanEffectKeep, needs: false, rule: litKeep + ", until 2026-11-16T10:30:00Z"},
		{kind: tAlloc, name: "alloc-r2", effect: v1alpha1.PlanEffectKeep, needs: false, rule: litOverAllocation},
		{kind: tInst, name: "r1", effect: v1alpha1.PlanEffectDrainInstance, needs: true, rule: litDrainReleased, cost: true},
		{kind: tInst, name: "r2", effect: v1alpha1.PlanEffectDrainInstance, needs: false, rule: litOverAllocation, cost: true},
	})
}

// A marked peer whose drain waits for the front's probe interval is not listed yet, and the planner asks for
// no pass of its own on its account (p.at): the plan is pure. The candidate turned healthy at 09:59:55, so the
// peer is due at 10:00:05, after the pass's 10:00:00.
func TestAPlanAsksForNoPassWhileAMarkedPeerWaits(t *testing.T) {
	f := newOpFixture(t, 2)
	f.overAllocation(1)
	f.withGlobalPolicy()
	f.addInstance(instOpts{name: "r1", phase: v1alpha1.InstanceReady, started: true, hardEnd: tp(opHardEnd), created: 0})
	f.addInstance(instOpts{name: "r2", phase: v1alpha1.InstanceReady, started: true, hardEnd: tp(opHardEnd.Add(time.Hour)), created: time.Minute, rankedOut: "c"})
	f.addInstance(instOpts{name: "c", phase: v1alpha1.InstanceReady, started: true, hardEnd: tp(opHardEnd.Add(2 * time.Hour)), created: 2 * time.Minute, healthySince: tp(opAt.Add(-5 * time.Second))})
	p := f.pass()
	if plan, refusal := p.planOperation(f.request(scaleOp(1), opAt)); plan == nil {
		t.Fatalf("refused: %s", refusal)
	}
	if !p.next.IsZero() {
		t.Errorf("the plan asked for a pass at %v: the planner asks for none", p.next)
	}
}

// Three ready Instances for a count of 2: R1 (13:00) is a ready surplus, which only a lowered count makes, so
// it is NOT a trim, though step 7 would drain it anyway: its cause is lowered and it goes through the
// approval table (HBEP-10:92 exempts only what over-allocation does not keep; the science agent's correction,
// 16 Nov). At the current count, 2: R3 (15:00) and R2 (14:00) are chosen, R1 is the surplus. Scale(1): R3 is
// chosen; R2 and R1 are taken, and R3 stays ready so both are DrainInstance. R1 is under ReleaseWhenIdle,
// kept until 10:30 after its drain, so it needs approval; R2 is under Keep until its hard end, 14:00, and needs
// none. Neither is exempt.
func TestAReadySurplusThatIsNotMarkedIsLoweredAndGoesThroughTheTable(t *testing.T) {
	idle := &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle, IdleLimit: durPtr("30m")}
	f := newOpFixture(t, 2)
	f.overAllocation(1)
	f.withGlobalPolicy()
	f.addInstance(instOpts{name: "r1", phase: v1alpha1.InstanceReady, started: true, hardEnd: tp(opHardEnd), created: 0, retention: idle})
	f.addInstance(instOpts{name: "r2", phase: v1alpha1.InstanceReady, started: true, hardEnd: tp(opHardEnd.Add(time.Hour)), created: time.Minute})
	f.addInstance(instOpts{name: "r3", phase: v1alpha1.InstanceReady, started: true, hardEnd: tp(opHardEnd.Add(2 * time.Hour)), created: 2 * time.Minute})
	plan, refusal := f.plan(scaleOp(1))
	checkPlan(t, plan, refusal, []wantTarget{
		{kind: tAlloc, name: "alloc-r1", effect: v1alpha1.PlanEffectKeep, needs: false, rule: litKeep + ", until 2026-11-16T10:30:00Z"},
		{kind: tAlloc, name: "alloc-r2", effect: v1alpha1.PlanEffectKeep, needs: false, rule: litKeep + ", until 2026-11-16T14:00:00Z"},
		{kind: tInst, name: "r1", effect: v1alpha1.PlanEffectDrainInstance, needs: true, rule: litDrainReleased, cost: true},
		{kind: tInst, name: "r2", effect: v1alpha1.PlanEffectDrainInstance, needs: false, rule: litDrainKept, cost: true},
	})
}

// k616's worked case (#616, comment 6905650131). k = 1, two desired. R is ready, S a pending primary, E a
// pending extra (S is the older). At the current count, 2: R and S are chosen, E is the surplus and k = 1 leaves
// it alone. Scale(1): R is chosen, the surplus is S, E; k = 1 leaves S alone (the older) and E is taken. E was
// kept within k at the current count, so it is NOT a trim: its cause is lowered, and a pending cancel needs
// approval by the table. The cause is decided by membership, not by state.
func TestAPendingExtraKeptWithinKThatAScaleTakesIsLoweredAndNeedsApproval(t *testing.T) {
	f := newOpFixture(t, 2)
	f.overAllocation(1)
	f.withGlobalPolicy()
	f.addInstance(instOpts{name: "r", phase: v1alpha1.InstanceReady, started: true, hardEnd: tp(opHardEnd), created: 0})
	f.addInstance(instOpts{name: "s", phase: v1alpha1.InstancePending, created: time.Minute, submit: tp(opT0.Add(10 * time.Minute))})
	f.addInstance(instOpts{name: "e", phase: v1alpha1.InstancePending, created: 2 * time.Minute, submit: tp(opT0.Add(20 * time.Minute))})
	plan, refusal := f.plan(scaleOp(1))
	checkPlan(t, plan, refusal, []wantTarget{
		{kind: tAlloc, name: "alloc-e", effect: v1alpha1.PlanEffectCancelPending, needs: true, rule: litCancelPending, cost: true},
		{kind: tInst, name: "e", effect: v1alpha1.PlanEffectCancelPending, needs: true, rule: litCancelPending, cost: true},
	})
}

// A trim moves no approval onto a plan that holds nothing else: the plan ID covers needsApproval, so the same
// Instances with and without the mark are two plans.
func TestATrimChangesThePlanID(t *testing.T) {
	build := func(marked bool) string {
		f := newOpFixture(t, 2)
		f.overAllocation(1)
		f.withGlobalPolicy()
		var mark string
		if marked {
			mark = "c"
		}
		f.addInstance(instOpts{name: "r1", phase: v1alpha1.InstanceReady, started: true, hardEnd: tp(opHardEnd), created: 0})
		f.addInstance(instOpts{name: "r2", phase: v1alpha1.InstanceReady, started: true, hardEnd: tp(opHardEnd.Add(time.Hour)), created: time.Minute, rankedOut: mark,
			retention: &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle, IdleLimit: durPtr("30m")}})
		f.addInstance(instOpts{name: "c", phase: v1alpha1.InstanceReady, started: true, hardEnd: tp(opHardEnd.Add(2 * time.Hour)), created: 2 * time.Minute, healthySince: tp(opT0.Add(50 * time.Minute))})
		plan, refusal := f.plan(scaleOp(1))
		if plan == nil {
			t.Fatalf("refused: %s", refusal)
		}
		return plan.ID
	}
	if build(true) == build(false) {
		t.Error("the same Instances, with and without the ranked-out mark, planned to one ID")
	}
}

// End of a managed-source ModelDeployment: ONE target, the deployment itself, with no approval (the science
// agent's ruling on #589, 16 Nov: a managed-source deployment holds no Allocation; SetEndTime or
// DeleteDeployment needs none by HBEP-10's table), and a cost that says what ending it does. Every value is
// written here as a literal, not read from managedEndNeedsApproval or the rule's constant: if the ruling flips,
// this row's literal changes with that one value.
func TestEndingAManagedDeploymentIsPlannedWithOneTarget(t *testing.T) {
	for _, tc := range []struct {
		name   string
		op     *v1alpha1.Operation
		effect v1alpha1.PlanEffect
	}{
		{"without delete", endOp(false), v1alpha1.PlanEffectSetEndTime},
		{"with delete", endOp(true), v1alpha1.PlanEffectDeleteDeployment},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newOpFixture(t, 1)
			f.withGlobalPolicy()
			f.md.Spec.Source = v1alpha1.ModelSource{Managed: &v1alpha1.ManagedCatalogEntry{}}
			plan, refusal := f.plan(tc.op)
			checkPlan(t, plan, refusal, []wantTarget{
				{kind: tMD, name: "md", effect: tc.effect, needs: false, rule: litManagedEnd, cost: true},
			})
			tg := plan.Targets[0]
			c := tg.Cost
			// The note is ReleaseCost's own field, and no figure is unknown: unknown stays for a figure that
			// applies and cannot be observed (the science agent's ruling on #653, 16 Nov).
			if c.Note != "no HPC capacity held" {
				t.Errorf("the cost's note is %q, want exactly %q", c.Note, "no HPC capacity held")
			}
			if c.GPUHoursLeft != nil || c.LeaseLeft != nil || c.QueueAge != nil || c.StartEstimate != nil || c.LastReady || c.QueueWaits != nil || c.EstimateNow != nil || c.StartToReady != nil {
				t.Errorf("the cost holds a figure, known or unknown, beside the note: %+v", c)
			}
			if tg.UID != "uid-md" || tg.Generation != 3 || tg.Provider != "" || len(tg.Policies) != 0 {
				t.Errorf("target %+v: want the deployment's own identity (uid-md, generation 3), no Provider and no policies", tg)
			}
		})
	}
}

// A person switched a serving deployment to a managed source, and it still holds a started Allocation under
// keep (#653's review, finding 1). Its End is the shutdown, not the one-target managed plan. Worked by hand
// from HBEP-7's retention: with delete, a started Allocation of a deleted deployment is released at once;
// without it, the end time becomes now (10:00), which is a keep policy's date when it names none, so the
// Allocation is released at once too. Either way it is a release of a started Allocation before its 13:00
// hard end: approval (HBEP-10's table), with its cost. The deployment's own target needs none.
func TestEndingAManagedDeploymentThatStillHoldsAnAllocationNeedsApproval(t *testing.T) {
	for _, tc := range []struct {
		name string
		op   *v1alpha1.Operation
		md   wantTarget
	}{
		{"with delete", endOp(true), wantTarget{kind: tMD, name: "md", effect: v1alpha1.PlanEffectDeleteDeployment, needs: false, rule: litDeleteMD}},
		{"without delete", endOp(false), wantTarget{kind: tMD, name: "md", effect: v1alpha1.PlanEffectSetEndTime, needs: false, rule: litSetEndTime}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newOpFixture(t, 1)
			f.withGlobalPolicy()
			f.emptyAllocation("kept", &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionKeep})
			f.md.Spec.Source = v1alpha1.ModelSource{Managed: &v1alpha1.ManagedCatalogEntry{}}
			plan, refusal := f.plan(tc.op)
			checkPlan(t, plan, refusal, []wantTarget{
				{kind: tAlloc, name: "kept", effect: v1alpha1.PlanEffectReleaseStarted, needs: true, rule: litReleaseStarted, cost: true},
				tc.md,
			})
		})
	}
}

// A Scale or a Release of a managed-source deployment stays refused, with the message that says so.
func TestScalingOrReleasingAManagedDeploymentIsRefused(t *testing.T) {
	for _, op := range []*v1alpha1.Operation{scaleOp(0), releaseOp(v1alpha1.ReleaseParameters{Allocations: []string{"a"}})} {
		f := newOpFixture(t, 2)
		f.md.Spec.Source = v1alpha1.ModelSource{Managed: &v1alpha1.ManagedCatalogEntry{}}
		plan, refusal := f.plan(op)
		if plan != nil || !strings.Contains(refusal, "has a managed source") || !strings.Contains(refusal, "nothing to scale or release") {
			t.Errorf("%s: plan %v, refusal %q, want a refusal naming the managed source", op.Spec.Action, plan, refusal)
		}
	}
}

// Through the Operations step: the End of a managed-source deployment is planned with its one target, and once
// confirmed it waits with Executable=False/ExecutionNotBuilt, since no target needs approval.
func TestAManagedEndIsPlannedByTheStepAndWaitsAsNotBuiltOnceConfirmed(t *testing.T) {
	f := newOpFixture(t, 1)
	f.withGlobalPolicy()
	f.md.Spec.Source = v1alpha1.ModelSource{Managed: &v1alpha1.ManagedCatalogEntry{}}
	g := newOpRig(t, f, opNamed("end", endOp(false)))
	g.step(opAt)
	planned := g.op("end")
	if planned.Status.Phase != v1alpha1.OperationPhasePlanned || planned.Status.Plan == nil || len(planned.Status.Plan.Targets) != 1 {
		t.Fatalf("phase %q, plan %+v: want Planned with one target", planned.Status.Phase, planned.Status.Plan)
	}
	g.confirm("end", planned.Status.Plan.ID)
	g.step(opAt.Add(time.Minute))
	op := g.op("end")
	if c := condOf(op, v1alpha1.ConditionExecutable); c == nil || c.Status != "False" || c.Reason != v1alpha1.ReasonExecutionNotBuilt {
		t.Errorf("Executable %+v, want False/ExecutionNotBuilt", c)
	}
}
