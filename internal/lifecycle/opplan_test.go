package lifecycle

// The approval table, one fixture per row (HBEP-10, What needs approval; #589, PR 4, AC 1). Every
// expected effect, needsApproval and approvalRule below is written out by hand from HBEP-7's rules and
// HBEP-10's table, with the times of opfixture_test.go: the hard end is T0+4h (13:00), the plan is made at
// T0+1h (10:00). The planner's code computes none of them. The rule strings are spelled out in full, so a
// reworded rule fails here and is noticed.

import (
	"fmt"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

const (
	tAlloc = v1alpha1.TargetKindAllocation
	tInst  = v1alpha1.TargetKindInstance
	tMD    = v1alpha1.TargetKindModelDeployment

	litDrainKept      = "Drain an Instance while another stays ready, its Allocation kept until its hard end"
	litDrainReleased  = "Drain an Instance whose Allocation its retention then releases before its hard end: under ReleaseWhenIdle, a keep date before the hard end, or the profile's maxIdle"
	litEndKept        = "End an Instance still starting, as a drain: its Allocation is kept until its hard end"
	litEndReleased    = "End an Instance still starting, as a drain: its Allocation's retention then releases it before its hard end (ReleaseWhenIdle, a keep date before the hard end, or the profile's maxIdle)"
	litLastReady      = "Drain a deployment's last ready Instance before its end time"
	litCancelPending  = "Cancel a pending Allocation, other than a hedge loser"
	litReleaseStarted = "Release a started Allocation before its hard end, outside the spec"
	litReleaseKept    = "Release an Allocation that its retention policy keeps"
	litSetEndTime     = "Set the end time to now: the consequences are separate targets"
	litDeleteMD       = "Delete the ModelDeployment: the consequences are separate targets"
	litKeep           = "Keep an Allocation under its retention policy"
	litKeepReleased   = "Release a started Allocation before its hard end: the shutdown keeps it for now, and its retention then releases it early (ReleaseWhenIdle, a keep date before the hard end, or the profile's maxIdle)"
)

// checkPlan compares a plan's targets, in kind-then-name order, with the hand-written ones.
func checkPlan(t *testing.T, plan *v1alpha1.OperationPlan, refusal string, want []wantTarget) {
	t.Helper()
	if refusal != "" {
		t.Fatalf("refused: %s", refusal)
	}
	if len(plan.Targets) != len(want) {
		t.Fatalf("%d targets, want %d: %v", len(plan.Targets), len(want), targetNames(plan))
	}
	for i, w := range want {
		g := plan.Targets[i]
		if g.Kind != w.kind || g.Name != w.name {
			t.Fatalf("target %d is %s %s, want %s %s (all: %v)", i, g.Kind, g.Name, w.kind, w.name, targetNames(plan))
		}
		if g.Effect != w.effect || g.NeedsApproval != w.needs || g.ApprovalRule != w.rule {
			t.Errorf("%s %s: effect %s, needsApproval %v, rule %q; want %s, %v, %q", g.Kind, g.Name, g.Effect, g.NeedsApproval, g.ApprovalRule, w.effect, w.needs, w.rule)
		}
		if (g.Cost != nil) != w.cost {
			t.Errorf("%s %s: cost present = %v, want %v: every target that lowers capacity shows its cost, approval or not, and one that does not shows none", g.Kind, g.Name, g.Cost != nil, w.cost)
		}
		if w.needs != (len(g.Policies) > 0) {
			t.Errorf("%s %s: %d policies recorded with needsApproval %v: only a target that needs approval records its policies (one policy exists in these fixtures)", g.Kind, g.Name, len(g.Policies), w.needs)
		}
	}
}

func targetNames(plan *v1alpha1.OperationPlan) []string {
	var out []string
	for _, tg := range plan.Targets {
		out = append(out, string(tg.Kind)+" "+tg.Name+" "+string(tg.Effect))
	}
	return out
}

// withGlobalPolicy gives the fixture the global policy, so a target that needs approval records one.
func (f *opFixture) withGlobalPolicy() {
	g := true
	f.policies = append(f.policies, v1alpha1.ApprovalPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "global", UID: "uid-global", Generation: 1},
		Spec: v1alpha1.ApprovalPolicySpec{
			Scope:     v1alpha1.ApprovalScope{Global: &g},
			Approvers: []v1alpha1.Approver{{User: "person"}},
		},
	})
}

// retentionVariant is one way an Allocation's retention can treat a drained Instance's Allocation, with
// what the table says about it.
type retentionVariant struct {
	name string
	// apply changes the fixture and the Allocation's retention.
	retention *v1alpha1.RetentionPolicy
	maxIdle   *time.Duration
	noHardEnd bool
	mdEnd     *time.Time
	// needs: the drain's rule asks for approval; keep is the date the Allocation target is kept until.
	needs bool
	keep  string
}

func variants() []retentionVariant {
	d20 := 20 * time.Minute
	return []retentionVariant{
		// Keep, no date, no maxIdle: kept until the hard end, 13:00. No approval.
		{name: "keep", needs: false, keep: "2026-11-16T13:00:00Z"},
		// ReleaseWhenIdle with a 30 minute limit, emptied at 10:00: released at 10:30. Approval.
		{name: "releaseWhenIdle", retention: &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle, IdleLimit: durPtr("30m")}, needs: true, keep: "2026-11-16T10:30:00Z"},
		// A keep date, 12:00, before the hard end, 13:00. Approval.
		{name: "keepDateBeforeHardEnd", retention: &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionKeep, Until: opTime(opT0.Add(3 * time.Hour))}, needs: true, keep: "2026-11-16T12:00:00Z"},
		// The profile's maxIdle, 20 minutes: released at 10:20 even under keep. Approval.
		{name: "maxIdle", maxIdle: &d20, needs: true, keep: "2026-11-16T10:20:00Z"},
		// The hard end is not observed: approval. Kept until a hard end nobody knows.
		{name: "unknownHardEnd", noHardEnd: true, needs: true, keep: "its hard end"},
		// The deployment's end time, 12:00, before the hard end, 13:00, with no policy date. Approval.
		{name: "deploymentEndsBeforeHardEnd", mdEnd: tp(opT0.Add(3 * time.Hour)), needs: true, keep: "2026-11-16T12:00:00Z"},
		// Control: a keep date, 14:00, AFTER the hard end. The hard end comes first: no approval.
		{name: "keepDateAfterHardEnd", retention: &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionKeep, Until: opTime(opT0.Add(5 * time.Hour))}, needs: false, keep: "2026-11-16T13:00:00Z"},
	}
}

func durPtr(s string) *v1alpha1.Duration { d := v1alpha1.Duration(s); return &d }

func (v retentionVariant) opts(o instOpts) instOpts {
	o.retention = v.retention
	if v.noHardEnd {
		o.hardEnd = nil
	}
	return o
}

func (f *opFixture) setVariant(v retentionVariant) {
	if v.maxIdle != nil {
		f.pvs["sano"].settings.maxIdle = v.maxIdle
	}
	if v.mdEnd != nil {
		f.md.Spec.EndTime = opTime(*v.mdEnd)
	}
}

// A drain while another Instance stays ready: the rule of the drain's row, under each way retention can
// release the Allocation. r1 has the nearest hard end, so the scale-down drains it; r2 stays.
func TestADrainWhileAnotherStaysReadyNeedsApprovalOnlyWhenRetentionReleasesEarly(t *testing.T) {
	for _, v := range variants() {
		t.Run(v.name, func(t *testing.T) {
			f := newOpFixture(t, 2)
			f.withGlobalPolicy()
			f.setVariant(v)
			f.addInstance(v.opts(instOpts{name: "r1", phase: v1alpha1.InstanceReady, started: true, hardEnd: tp(opHardEnd), created: 0}))
			f.addInstance(v.opts(instOpts{name: "r2", phase: v1alpha1.InstanceReady, started: true, hardEnd: tp(opHardEnd.Add(time.Hour)), created: time.Minute}))
			plan, refusal := f.plan(scaleOp(1))
			rule := litDrainKept
			if v.needs {
				rule = litDrainReleased
			}
			// r1 has the nearest hard end, so it drains. With no hard end known in either, nothing separates them but
			// age, and HBEP-7's order keeps the older one (How it counts: among equals, the older first): r2 drains.
			drained := "r1"
			if v.noHardEnd {
				drained = "r2"
			}
			checkPlan(t, plan, refusal, []wantTarget{
				{kind: tAlloc, name: "alloc-" + drained, effect: v1alpha1.PlanEffectKeep, needs: false, rule: litKeep + ", until " + v.keep},
				{kind: tInst, name: drained, effect: v1alpha1.PlanEffectDrainInstance, needs: v.needs, rule: rule, cost: true},
			})
		})
	}
}

// An Instance still starting on a started Allocation ends at once, and its Allocation follows retention:
// the erratum gives EndStarting the drain's rule. s1 starts later than the ready r, so the scale-down takes
// it first.
func TestEndingAStartingInstanceHasTheDrainsRule(t *testing.T) {
	for _, v := range variants() {
		t.Run(v.name, func(t *testing.T) {
			f := newOpFixture(t, 2)
			f.withGlobalPolicy()
			f.setVariant(v)
			f.addInstance(instOpts{name: "r", phase: v1alpha1.InstanceReady, started: true, hardEnd: tp(opHardEnd), created: 0})
			f.addInstance(v.opts(instOpts{name: "s1", phase: v1alpha1.InstanceStarting, started: true, hardEnd: tp(opHardEnd), created: time.Minute}))
			plan, refusal := f.plan(scaleOp(1))
			rule := litEndKept
			if v.needs {
				rule = litEndReleased
			}
			checkPlan(t, plan, refusal, []wantTarget{
				{kind: tAlloc, name: "alloc-s1", effect: v1alpha1.PlanEffectKeep, needs: false, rule: litKeep + ", until " + v.keep},
				{kind: tInst, name: "s1", effect: v1alpha1.PlanEffectEndStarting, needs: v.needs, rule: rule, cost: true},
			})
		})
	}
}

// A pending Allocation's cancel needs approval, and so does the Instance that goes with it; the ready one
// stays.
func TestCancellingAPendingAllocationNeedsApproval(t *testing.T) {
	f := newOpFixture(t, 2)
	f.withGlobalPolicy()
	f.addInstance(instOpts{name: "r", phase: v1alpha1.InstanceReady, started: true, hardEnd: tp(opHardEnd)})
	f.addInstance(instOpts{name: "p", phase: v1alpha1.InstancePending, created: time.Minute, submit: tp(opT0.Add(10 * time.Minute))})
	plan, refusal := f.plan(scaleOp(1))
	checkPlan(t, plan, refusal, []wantTarget{
		{kind: tAlloc, name: "alloc-p", effect: v1alpha1.PlanEffectCancelPending, needs: true, rule: litCancelPending, cost: true},
		{kind: tInst, name: "p", effect: v1alpha1.PlanEffectCancelPending, needs: true, rule: litCancelPending, cost: true},
	})
}

// A primary's replacement goes with it, never promoted: the pending replacement is cancelled, and the ready
// primary that has the nearest hard end drains while the other stays ready. Both primaries have a replacement
// pending: HBEP-7's order puts a ready Instance with a live replacement before one without, so with only r1
// replaced the scale-down would keep r1 and drain r2.
func TestAScaleDownTakesTheReplacementWithItsPrimary(t *testing.T) {
	f := newOpFixture(t, 2)
	f.withGlobalPolicy()
	f.addInstance(instOpts{name: "r1", phase: v1alpha1.InstanceReady, started: true, hardEnd: tp(opHardEnd), created: 0})
	f.addInstance(instOpts{name: "r2", phase: v1alpha1.InstanceReady, started: true, hardEnd: tp(opHardEnd.Add(time.Hour)), created: time.Minute})
	f.addInstance(instOpts{name: "r1-repl", phase: v1alpha1.InstancePending, pred: "r1", created: 2 * time.Minute, submit: tp(opT0.Add(20 * time.Minute))})
	f.addInstance(instOpts{name: "r2-repl", phase: v1alpha1.InstancePending, pred: "r2", created: 3 * time.Minute, submit: tp(opT0.Add(25 * time.Minute))})
	plan, refusal := f.plan(scaleOp(1))
	checkPlan(t, plan, refusal, []wantTarget{
		{kind: tAlloc, name: "alloc-r1", effect: v1alpha1.PlanEffectKeep, needs: false, rule: litKeep + ", until 2026-11-16T13:00:00Z"},
		{kind: tAlloc, name: "alloc-r1-repl", effect: v1alpha1.PlanEffectCancelPending, needs: true, rule: litCancelPending, cost: true},
		{kind: tInst, name: "r1", effect: v1alpha1.PlanEffectDrainInstance, needs: false, rule: litDrainKept, cost: true},
		{kind: tInst, name: "r1-repl", effect: v1alpha1.PlanEffectCancelPending, needs: true, rule: litCancelPending, cost: true},
	})
}

// A Scale to zero is the shutdown case, as End without its end-time write: every ready Instance is the last
// one, and there is no target for the deployment.
func TestScaleToZeroDrainsTheLastReadyInstance(t *testing.T) {
	f := newOpFixture(t, 1)
	f.withGlobalPolicy()
	f.addInstance(instOpts{name: "r1", phase: v1alpha1.InstanceReady, started: true, hardEnd: tp(opHardEnd)})
	plan, refusal := f.plan(scaleOp(0))
	checkPlan(t, plan, refusal, []wantTarget{
		{kind: tAlloc, name: "alloc-r1", effect: v1alpha1.PlanEffectKeep, needs: false, rule: litKeep + ", until 2026-11-16T13:00:00Z"},
		{kind: tInst, name: "r1", effect: v1alpha1.PlanEffectDrainLastReady, needs: true, rule: litLastReady, cost: true},
	})
	if !plan.Targets[1].Cost.LastReady {
		t.Error("the last ready Instance's cost carries lastReady: true")
	}
}

// End of a serving deployment needs approval with and without delete: it drains the last ready Instance.
// Without delete the keep date becomes now (10:00) unless the policy's until is later; with it a started
// Allocation is released at once.
func TestEndingAServingDeploymentNeedsApproval(t *testing.T) {
	serving := func(retention *v1alpha1.RetentionPolicy) *opFixture {
		f := newOpFixture(t, 1)
		f.withGlobalPolicy()
		f.addInstance(instOpts{name: "r1", phase: v1alpha1.InstanceReady, started: true, hardEnd: tp(opHardEnd), retention: retention})
		return f
	}
	t.Run("without delete", func(t *testing.T) {
		plan, refusal := serving(nil).plan(endOp(false))
		checkPlan(t, plan, refusal, []wantTarget{
			{kind: tAlloc, name: "alloc-r1", effect: v1alpha1.PlanEffectReleaseStarted, needs: true, rule: litReleaseStarted, cost: true},
			{kind: tInst, name: "r1", effect: v1alpha1.PlanEffectDrainLastReady, needs: true, rule: litLastReady, cost: true},
			{kind: tMD, name: "md", effect: v1alpha1.PlanEffectSetEndTime, needs: false, rule: litSetEndTime},
		})
	})
	t.Run("without delete, a policy date later than now", func(t *testing.T) {
		// until 11:30, after the plan's 10:00: the Allocation is kept until then. The plan takes its Instance,
		// r1, so the approval is asked through r1's drain; the Allocation's own target needs none (the
		// science agent's ruling on #653: the kept Allocation's own approval is for one with no plannable
		// Instance).
		plan, refusal := serving(&v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionKeep, Until: opTime(opT0.Add(150 * time.Minute))}).plan(endOp(false))
		checkPlan(t, plan, refusal, []wantTarget{
			{kind: tAlloc, name: "alloc-r1", effect: v1alpha1.PlanEffectKeep, needs: false, rule: litKeep + ", until 2026-11-16T11:30:00Z"},
			{kind: tInst, name: "r1", effect: v1alpha1.PlanEffectDrainLastReady, needs: true, rule: litLastReady, cost: true},
			{kind: tMD, name: "md", effect: v1alpha1.PlanEffectSetEndTime, needs: false, rule: litSetEndTime},
		})
	})
	t.Run("with delete", func(t *testing.T) {
		plan, refusal := serving(nil).plan(endOp(true))
		checkPlan(t, plan, refusal, []wantTarget{
			{kind: tAlloc, name: "alloc-r1", effect: v1alpha1.PlanEffectReleaseStarted, needs: true, rule: litReleaseStarted, cost: true},
			{kind: tInst, name: "r1", effect: v1alpha1.PlanEffectDrainLastReady, needs: true, rule: litLastReady, cost: true},
			{kind: tMD, name: "md", effect: v1alpha1.PlanEffectDeleteDeployment, needs: false, rule: litDeleteMD},
		})
	})
}

// End of a deployment that waits in a queue cancels its pending Allocation, and ends an Instance that is
// starting; the end sets the keep date to now, so the starting one's Allocation is released at once.
func TestEndingAQueuedDeploymentCancelsAndEnds(t *testing.T) {
	f := newOpFixture(t, 2)
	f.withGlobalPolicy()
	f.addInstance(instOpts{name: "p", phase: v1alpha1.InstancePending, submit: tp(opT0.Add(10 * time.Minute))})
	f.addInstance(instOpts{name: "s", phase: v1alpha1.InstanceStarting, started: true, hardEnd: tp(opHardEnd), created: time.Minute})
	plan, refusal := f.plan(endOp(false))
	checkPlan(t, plan, refusal, []wantTarget{
		{kind: tAlloc, name: "alloc-p", effect: v1alpha1.PlanEffectCancelPending, needs: true, rule: litCancelPending, cost: true},
		{kind: tAlloc, name: "alloc-s", effect: v1alpha1.PlanEffectReleaseStarted, needs: true, rule: litReleaseStarted, cost: true},
		{kind: tInst, name: "p", effect: v1alpha1.PlanEffectCancelPending, needs: true, rule: litCancelPending, cost: true},
		{kind: tInst, name: "s", effect: v1alpha1.PlanEffectEndStarting, needs: true, rule: litEndReleased, cost: true},
		{kind: tMD, name: "md", effect: v1alpha1.PlanEffectSetEndTime, needs: false, rule: litSetEndTime},
	})
}

// End and Scale(0) of a deployment whose one Instance the plan does not take: it is draining already (a
// no-overlap handover), or it has ended in a failure. The plan has no Instance target to ask for approval,
// so the started Allocation's own target must (#653's review, finding 2; the science agent's ruling on
// #653, 16 Nov 2026: toward approval, whenever releasesBeforeHardEnd holds). Worked by hand: ReleaseWhenIdle
// with a 30 minute limit, emptied at the plan's 10:00, is kept until 10:30 and then released, two and a half
// hours before its 13:00 hard end, with nothing placed on it again: a release of a started Allocation before
// its hard end, so approval, and its cost. The End's end time does not move a ReleaseWhenIdle date, so End
// and Scale(0) keep it to the same 10:30. The control: under keep with no date, Scale(0) keeps it until its
// hard end, which needs no approval and shows no cost.
func TestAKeptAllocationWithNoInstanceTargetNeedsApprovalWhenReleasedEarly(t *testing.T) {
	idle := &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle, IdleLimit: durPtr("30m")}
	draining := func(f *opFixture, r *v1alpha1.RetentionPolicy) {
		f.addInstance(instOpts{name: "x", phase: v1alpha1.InstanceDraining, started: true, hardEnd: tp(opHardEnd), retention: r})
	}
	failed := func(f *opFixture, r *v1alpha1.RetentionPolicy) {
		in, _ := f.addInstance(instOpts{name: "x", phase: v1alpha1.InstanceEnded, started: true, hardEnd: tp(opHardEnd), retention: r})
		in.Status.EndReason = v1alpha1.ReasonVerificationFailed
	}
	kept := wantTarget{kind: tAlloc, name: "alloc-x", effect: v1alpha1.PlanEffectKeep, needs: true, rule: litKeepReleased + ", until 2026-11-16T10:30:00Z", cost: true}
	setEnd := wantTarget{kind: tMD, name: "md", effect: v1alpha1.PlanEffectSetEndTime, needs: false, rule: litSetEndTime}
	for _, tc := range []struct {
		name  string
		setup func(*opFixture, *v1alpha1.RetentionPolicy)
		r     *v1alpha1.RetentionPolicy
		op    *v1alpha1.Operation
		want  []wantTarget
	}{
		{"End, a draining Instance", draining, idle, endOp(false), []wantTarget{kept, setEnd}},
		{"End, a failed Instance", failed, idle, endOp(false), []wantTarget{kept, setEnd}},
		{"Scale(0), a draining Instance", draining, idle, scaleOp(0), []wantTarget{kept}},
		{"Scale(0), a failed Instance", failed, idle, scaleOp(0), []wantTarget{kept}},
		{"Scale(0), a draining Instance under keep, kept until its hard end", draining, &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionKeep}, scaleOp(0),
			[]wantTarget{{kind: tAlloc, name: "alloc-x", effect: v1alpha1.PlanEffectKeep, needs: false, rule: litKeep + ", until 2026-11-16T13:00:00Z"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newOpFixture(t, 1)
			f.withGlobalPolicy()
			tc.setup(f, tc.r)
			plan, refusal := f.plan(tc.op)
			checkPlan(t, plan, refusal, tc.want)
			if c := plan.Targets[0].Cost; tc.want[0].needs && (c == nil || c.LeaseLeft == nil || c.LeaseLeft.Duration == nil || c.LeaseLeft.Duration.Duration != 3*time.Hour) {
				t.Errorf("the kept Allocation's cost %+v: want 3h of lease left, its 13:00 hard end minus the plan's 10:00", c)
			}
		})
	}
}

// A Release of a kept empty Allocation needs approval; it names the Allocation, or a selector matches it,
// and only this deployment's Allocations are found, by spec.modelDeploymentRef.name.
func TestReleasingAKeptAllocationNeedsApproval(t *testing.T) {
	build := func() *opFixture {
		f := newOpFixture(t, 1)
		f.withGlobalPolicy()
		f.addInstance(instOpts{name: "r1", phase: v1alpha1.InstanceReady, started: true, hardEnd: tp(opHardEnd)})
		k1 := f.emptyAllocation("kept-1", nil)
		k1.Labels["tier"] = "warm"
		k2 := f.emptyAllocation("kept-2", nil)
		k2.Labels["tier"] = "warm"
		other := f.emptyAllocation("others", nil)
		other.Labels["tier"] = "warm"
		other.Spec.ModelDeploymentRef = &v1alpha1.ObjectReference{Name: "another-md"}
		return f
	}
	t.Run("by name", func(t *testing.T) {
		plan, refusal := build().plan(releaseOp(v1alpha1.ReleaseParameters{Allocations: []string{"kept-1"}}))
		checkPlan(t, plan, refusal, []wantTarget{
			{kind: tAlloc, name: "kept-1", effect: v1alpha1.PlanEffectReleaseKept, needs: true, rule: litReleaseKept, cost: true},
		})
	})
	t.Run("by selector, only this deployment's Allocations", func(t *testing.T) {
		// The selector matches kept-1, kept-2 and the other deployment's Allocation, which has the label too.
		plan, refusal := build().plan(releaseOp(v1alpha1.ReleaseParameters{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"tier": "warm"}}}))
		checkPlan(t, plan, refusal, []wantTarget{
			{kind: tAlloc, name: "kept-1", effect: v1alpha1.PlanEffectReleaseKept, needs: true, rule: litReleaseKept, cost: true},
			{kind: tAlloc, name: "kept-2", effect: v1alpha1.PlanEffectReleaseKept, needs: true, rule: litReleaseKept, cost: true},
		})
	})
}

// A request the controller cannot plan is Refused, with a message that names what is wrong.
func TestRefusals(t *testing.T) {
	base := func() *opFixture {
		f := newOpFixture(t, 2)
		f.withGlobalPolicy()
		f.addInstance(instOpts{name: "r1", phase: v1alpha1.InstanceReady, started: true, hardEnd: tp(opHardEnd)})
		f.addInstance(instOpts{name: "r2", phase: v1alpha1.InstanceReady, started: true, hardEnd: tp(opHardEnd), created: time.Minute})
		f.emptyAllocation("kept-1", nil).Labels["tier"] = "warm"
		other := f.emptyAllocation("others", nil)
		other.Spec.ModelDeploymentRef = &v1alpha1.ObjectReference{Name: "another-md"}
		other.Labels["tier"] = "other"
		return f
	}
	for _, tc := range []struct {
		name   string
		mutate func(*opFixture)
		op     *v1alpha1.Operation
		want   string // a substring of the refusal
	}{
		{"a Scale that does not lower the count", nil, scaleOp(2), "a Scale lowers the count"},
		{"a Scale above the count", nil, scaleOp(5), "a Scale lowers the count"},
		{"a missing ModelDeployment", func(f *opFixture) { f.md = nil }, scaleOp(1), "does not exist"},
		{"a ModelDeployment being deleted", func(f *opFixture) { f.md.DeletionTimestamp = opTime(opT0) }, scaleOp(1), "is being deleted"},
		{"a managed source", func(f *opFixture) { f.md.Spec.Source = v1alpha1.ModelSource{Managed: &v1alpha1.ManagedCatalogEntry{}} }, scaleOp(1), "managed source"},
		{"a named Allocation that does not exist", nil, releaseOp(v1alpha1.ReleaseParameters{Allocations: []string{"nope"}}), "Allocation nope does not exist"},
		{"a named Allocation of another deployment", nil, releaseOp(v1alpha1.ReleaseParameters{Allocations: []string{"others"}}), "Allocation others is not an Allocation of ModelDeployment md"},
		{"a named Allocation that hosts a live Instance", nil, releaseOp(v1alpha1.ReleaseParameters{Allocations: []string{"alloc-r1"}}), "Allocation alloc-r1 hosts a live Instance (r1)"},
		{"a selector that matches nothing", nil, releaseOp(v1alpha1.ReleaseParameters{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"tier": "none"}}}), "matches no Allocation"},
		{"a selector that matches only another deployment's Allocation", nil, releaseOp(v1alpha1.ReleaseParameters{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"tier": "other"}}}), "matches no Allocation"},
		{"an empty selector", nil, releaseOp(v1alpha1.ReleaseParameters{Selector: &metav1.LabelSelector{}}), "an empty release selector"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := base()
			if tc.mutate != nil {
				tc.mutate(f)
			}
			var plan *v1alpha1.OperationPlan
			var refusal string
			if f.md == nil {
				p := f.pass()
				p.md = nil
				plan, refusal = p.planOperation(f.request(tc.op, opAt))
			} else {
				plan, refusal = f.plan(tc.op)
			}
			if plan != nil {
				t.Fatalf("planned %v, want a refusal containing %q", targetNames(plan), tc.want)
			}
			if !strings.Contains(refusal, tc.want) {
				t.Errorf("refusal %q does not contain %q", refusal, tc.want)
			}
		})
	}
}

// The mapping of the controller's own effect to the plan's, by hand: a drain is the last ready one when no
// Instance stays ready.
func TestTheScaleEffectMapping(t *testing.T) {
	for _, tc := range []struct {
		name       string
		effect     scaleEffect
		staysReady bool
		want       v1alpha1.PlanEffect
	}{
		{"a pending Allocation is cancelled", effectCancelPending, true, v1alpha1.PlanEffectCancelPending},
		{"a starting Instance ends, whoever stays", effectEndKeepAllocation, false, v1alpha1.PlanEffectEndStarting},
		{"a drain while another stays ready", effectDrain, true, v1alpha1.PlanEffectDrainInstance},
		{"a drain with none staying ready is the last ready one", effectDrain, false, v1alpha1.PlanEffectDrainLastReady},
	} {
		if got := scaleEffectOf(scaleTarget{effect: tc.effect}, tc.staysReady); got != tc.want {
			t.Errorf("%s: %s, want %s", tc.name, got, tc.want)
		}
	}
}

// A plan larger than the 256 targets an Operation's status holds is refused, with the count, and not written
// to fail on every pass: 129 pending Instances are 258 targets, an Instance and its Allocation each.
func TestAPlanTooLargeForTheStatusIsRefused(t *testing.T) {
	f := newOpFixture(t, 131)
	for i := 0; i < 130; i++ {
		f.addInstance(instOpts{name: fmt.Sprintf("p%03d", i), phase: v1alpha1.InstancePending, created: time.Duration(i) * time.Second, submit: tp(opT0.Add(10 * time.Minute))})
	}
	plan, refusal := f.plan(scaleOp(1))
	if plan != nil || !strings.Contains(refusal, "the plan has 258 targets, more than the 256") {
		t.Errorf("plan %v, refusal %q: want a refusal naming 258 targets", plan, refusal)
	}
}
