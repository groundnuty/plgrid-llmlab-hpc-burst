package lifecycle

// The warm steps of the placement order (HBEP-7, Provisioning, Where; #604 PR 3): a hedge's member, two needs in one
// pass, a ranked-out Allocation, the `only` filter, and the deferral for an Allocation whose agent has not registered
// again after a controller restart. Fixtures are placement_order_rig_test.go's.

import (
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// TestAHedgeMemberGoesToAWarmAllocationBeforeANewSubmission: P is on A; B, of A's class, holds a warm Allocation, and
// C, of another class, has room and no warm Allocation. The deployment allows A, then C, then B, with hedgeWidth 2. P's
// replacement R1 is made on A at 10:19:30 and pends behind 10 h. R1's job is submitted at 10:19:40, so its hedge time
// is 10 min later, 10:29:40 (the expected start is its submission plus the planning wait, 0 s, before the minimum wait;
// the deadline, 10:49:30, is later): the hedge's second member R2 is placed on B's warm Allocation, by the warm sweep,
// and not submitted on C, which comes first in the list and has room. At 10:30:00 R1 still pends and R2 is starting;
// R2 starts at once, with no slot and no caps check, and wins.
func TestAHedgeMemberGoesToAWarmAllocationBeforeANewSubmission(t *testing.T) {
	o := newOrdRig(t, ordOpts{withC: true})
	o.batch.Enqueue(pfake.Script{}, pfake.Script{QueueWait: 10 * time.Hour})
	md := ordMD(hoMD)
	md.Spec.HedgeWidth = nil // the default, 2
	o.create(md)
	wb := o.warmUp(hoMD, ordB)
	p := o.primaryOnA(hoMD, rigProvider, ordC, ordB)
	r1 := o.replacementAt(p)
	if prov, _ := where(r1); prov != rigProvider {
		t.Fatalf("the replacement R1 is on %s, want A, its predecessor's Provider", prov)
	}
	hoStepTo(o.rig, hoT(10, 29, 30), 10*time.Second, nil)
	if n := len(hoRepl(o.rig, p.Name)); n != 1 {
		t.Fatalf("%d members of the hedge at 10:29:30, before its hedge time, want R1 alone", n)
	}
	hoStepTo(o.rig, hoT(10, 30, 0), 10*time.Second, nil)
	var members []v1alpha1.Instance
	for _, in := range hoRepl(o.rig, p.Name) {
		if !ended(&in) {
			members = append(members, in)
		}
	}
	if len(members) != 2 {
		t.Fatalf("%d live members of the hedge at 10:30:00, want R1 and R2", len(members))
	}
	var r2 v1alpha1.Instance
	for _, m := range members {
		if m.Name != r1.Name {
			r2 = m
		}
	}
	prov, alloc := where(r2)
	if prov != ordB || alloc != wb || len(o.allocsOn(hoMD, ordC)) != 0 || len(o.allocsOn(hoMD, rigProvider)) != 2 || !sameAllocs(o.allocsOn(hoMD, ordB), []string{wb}) {
		t.Fatalf("R2 is on %s, Allocation %s; A has %v, B has %v, C has %v; want R2 on B's warm Allocation %s and no Allocation on C", prov, alloc,
			o.allocsOn(hoMD, rigProvider), o.allocsOn(hoMD, ordB), o.allocsOn(hoMD, ordC), wb)
	}
	o.until(10*time.Minute, 10*time.Second, "R2 ready", func() bool { return o.mustInstance(r2.Name).Status.Phase == v1alpha1.InstanceReady })
}

// TestTwoNeedsInOnePassNeverTakeOneWarmAllocation: the deployment wants two Instances and allows A, then B, whose warm
// Allocation is the only one. In one pass, the first missing primary is placed on it and the second is submitted on A:
// the Instance the first made is listed on that Allocation when the second looks.
func TestTwoNeedsInOnePassNeverTakeOneWarmAllocation(t *testing.T) {
	o := newOrdRig(t, ordOpts{})
	o.create(ordMD(hoMD))
	wb := o.warmUp(hoMD, ordB)
	hoStepTo(o.rig, hoT(9, 0, 0), 5*time.Minute, nil)
	o.allowOn(hoMD, 2, rigProvider, ordB)
	o.clock.Advance(10 * time.Second)
	o.Step()
	live := o.liveInstances(hoMD)
	if len(live) != 2 {
		t.Fatalf("%d live Instances after the pass, want the two missing primaries", len(live))
	}
	onWarm := 0
	for _, in := range live {
		if _, a := where(in); a == wb {
			onWarm++
		}
	}
	onA := o.allocsOn(hoMD, rigProvider)
	if onWarm != 1 || len(onA) != 1 || !sameAllocs(o.allocsOn(hoMD, ordB), []string{wb}) || !o.hostsLive(hoMD, onA[0]) {
		t.Fatalf("%d Instances on B's warm Allocation %s, A has %v: want exactly one on it and the other on a new Allocation on A", onWarm, wb, onA)
	}
}

// TestARankedOutAllocationTakesTheNextMissingPrimary: HBEP-7 over-allocation's fixture (rank_rig_test.go): the primary
// P and the extra X are made together on A, the extra reaches its ranking point after P, is outranked, and ends
// RankedOut with its Allocation kept. Provider B, of A's class, is then added with room, and the deployment allows B
// first, then A. P's job is cancelled outside the controller, and the missing primary is placed on X's kept Allocation
// on A, not submitted on B, which comes first: a ranked-out Allocation is warm capacity (#635).
func TestARankedOutAllocationTakesTheNextMissingPrimary(t *testing.T) {
	h := newRkRig(t, false)
	p, x := h.start(func(md *v1alpha1.ModelDeployment) {
		endsAt(rigStart.Add(20 * time.Hour))(md)
		md.Spec.HedgeWidth = ptrTo(int32(1)) // the row is about placement, not hedging: growth adds no member
	},
		pfake.Script{QueueWait: 10 * time.Minute}, pfake.Script{QueueWait: rkXWait})
	h.engineOf(p, rk200, 0)
	h.engineOf(x, rk180, 0)
	h.stepTo(rkAt(0, 45, 0), 10*time.Second, nil)
	rkAssertRankedOut(t, h, x, p, rkAt(0, 45, 0))
	xAlloc, pAlloc := h.alloc(x), h.alloc(p)

	b := h.addProvider(ordB, "gpu-b", 12*time.Hour, nil, nil)
	for range 2 {
		if err := b.prov.Sync(h.ctx); err != nil {
			t.Fatal(err)
		}
	}
	h.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Providers.Names = []string{ordB, rigProvider} })
	if pv := h.passOf(hoMD).pvByName[ordB]; pv == nil || !pv.usable() {
		t.Fatalf("Provider B is not usable (%+v): the row would pass with the order unchanged", pv)
	}
	pa, ok := h.allocation(pAlloc)
	if !ok {
		t.Fatal("P's Allocation is gone")
	}
	h.batch.CancelExternally(pa.Annotations[v1alpha1.AnnotationJobID])
	h.until(10*time.Minute, 10*time.Second, "the missing primary placed", func() bool {
		for _, in := range h.liveInstances(hoMD) {
			if in.Name != p {
				return true
			}
		}
		return false
	})
	live := h.liveInstances(hoMD)
	if len(live) != 1 {
		t.Fatalf("%d live Instances, want the one missing primary", len(live))
	}
	if prov, alloc := where(live[0]); prov != rigProvider || alloc != xAlloc {
		t.Fatalf("the missing primary is on %s, Allocation %s; want X's kept Allocation %s on A", prov, alloc, xAlloc)
	}
	for _, a := range h.allAllocations() {
		if a.Labels[v1alpha1.LabelProvider] == ordB {
			t.Fatalf("Allocation %s was made on B: a submission came before the kept Allocation", a.Name)
		}
	}
}

// TestOnlyNamesTheOneProviderTried: placeOpts.only restricts both sweeps, warm and submit (the queued no-overlap
// successor and the over-allocation extra): with a warm Allocation on B and room on A, `only: A` submits on A and
// never uses B's warm Allocation, the extra's options too; `only: B` takes B's warm Allocation; no filter, the control,
// takes it too. The calls are made on a pass directly, with no missing primary for the controller to place.
func TestOnlyNamesTheOneProviderTried(t *testing.T) {
	for _, c := range []struct {
		name     string
		opts     placeOpts
		wantProv string
		wantWarm bool
	}{
		{"only A", placeOpts{only: rigProvider}, rigProvider, false},
		{"only A, with the extra's options", placeOpts{only: rigProvider, extra: true, caps: true}, rigProvider, false},
		{"only B", placeOpts{only: ordB}, ordB, true},
		{"no filter (the control)", placeOpts{}, ordB, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			o := newOrdRig(t, ordOpts{})
			o.create(ordMD(hoMD))
			wb := o.warmUp(hoMD, ordB)
			hoStepTo(o.rig, hoT(9, 0, 0), 5*time.Minute, nil)
			o.allowOn(hoMD, 0, rigProvider, ordB)
			p := o.passOf(hoMD)
			ok, err := p.provisionWith(nil, c.opts)
			if err != nil || !ok {
				t.Fatalf("provisionWith placed %v, %v", ok, err)
			}
			live := o.liveInstances(hoMD)
			if len(live) != 1 {
				t.Fatalf("%d live Instances, want the one placed", len(live))
			}
			prov, alloc := where(live[0])
			if prov != c.wantProv || (alloc == wb) != c.wantWarm {
				t.Fatalf("the Instance is on %s, Allocation %s; want it on %s, on B's warm Allocation %s: %v", prov, alloc, c.wantProv, wb, c.wantWarm)
			}
			if c.wantWarm == (len(o.allocsOn(hoMD, rigProvider)) != 0) || len(o.allocsOn(hoMD, ordB)) != 1 {
				t.Fatalf("A has %v, B has %v: a warm placement makes no Allocation and a submission on A makes one", o.allocsOn(hoMD, rigProvider), o.allocsOn(hoMD, ordB))
			}
		})
	}
}

// syncProviders runs each Provider's controller once, as a round of the rig does before the agents.
func (o *ordRig) syncProviders() {
	o.t.Helper()
	if err := o.prov.Sync(o.ctx); err != nil {
		o.t.Fatalf("provider sync: %v", err)
	}
	for _, x := range o.extra {
		if err := x.prov.Sync(o.ctx); err != nil {
			o.t.Fatalf("provider %s sync: %v", x.name, err)
		}
	}
}

// restartedWith restarts the controller and holds the agents of the named Allocations back: they do not register
// again, as agents the restarted controller has not heard from yet. The restart is at the rig's clock now.
func (o *ordRig) restartedWith(unregistered ...string) {
	o.t.Helper()
	o.restartController()
	for _, a := range unregistered {
		o.agentOf(a).stopped = true
	}
}

// TestARestartDefersAMissingPrimaryForAnUnregisteredWarmAllocation: the deployment wants one Instance and allows A, then
// B, whose warm Allocation's agent has not registered again since the controller restarted at 09:00:00. Within
// agentGrace, 45 s, the placement waits: no Instance, no Allocation on A, and the pass asks to be run again at the end
// of the grace. At 09:00:45 the Allocation is passed over, and the missing primary is submitted on A. The agent never
// registers in the row, so the grace's end is all that releases the placement, and a deferral that was dropped would
// have submitted at 09:00:05.
func TestARestartDefersAMissingPrimaryForAnUnregisteredWarmAllocation(t *testing.T) {
	o := newOrdRig(t, ordOpts{})
	o.create(ordMD(hoMD))
	wb := o.warmUp(hoMD, ordB)
	hoStepTo(o.rig, hoT(9, 0, 0), 5*time.Minute, nil)
	o.allowOn(hoMD, 1, rigProvider, ordB)
	o.restartedWith(wb)
	start := o.clock.Now()
	p := o.passOf(hoMD)
	if ok, err := p.provisionWith(nil, placeOpts{}); err != nil || ok || !p.deferred || !p.next.Equal(start.Add(agentGrace)) {
		t.Fatalf("a pass at the restart: placed %v (%v), deferred %v, next %v; want it deferred, to run again at %s", ok, err, p.deferred, p.next, start.Add(agentGrace).Format(time.TimeOnly))
	}
	for i := 1; i <= 9; i++ {
		o.clock.Advance(5 * time.Second)
		o.Step()
		at := o.clock.Now().Sub(start)
		if at < agentGrace {
			if n := len(o.liveInstances(hoMD)) + len(o.allocsOn(hoMD, rigProvider)); n != 0 {
				t.Fatalf("at %s after the restart, inside the grace, %d Instances or Allocations on A: the placement was not deferred", at, n)
			}
			continue
		}
		live := o.liveInstances(hoMD)
		onA := o.allocsOn(hoMD, rigProvider)
		if len(live) != 1 || len(onA) != 1 || live[0].Spec.AllocationRef.Name != onA[0] || !sameAllocs(o.allocsOn(hoMD, ordB), []string{wb}) {
			t.Fatalf("at %s after the restart, the grace's end: %d live Instances, A has %v, B has %v; want the missing primary on a new Allocation on A", at, len(live), onA, o.allocsOn(hoMD, ordB))
		}
	}
}

// TestARestartWaitsForAnEarlierProvidersUnregisteredWarmAllocation: A's and B's warm Allocations, the deployment allows A,
// then B, and the controller restarts at 09:00:00. B's agent registers again at once, A's does not. The placement waits
// for A's, the earlier one in the order, though B's could take it (HBEP-7's order and the plan's Q3: a deferral
// defers the whole placement, so a registered Allocation later in the order does not jump it). At 09:00:45 A's is
// passed over, and the missing primary is placed on B's, with no submission.
func TestARestartWaitsForAnEarlierProvidersUnregisteredWarmAllocation(t *testing.T) {
	o := newOrdRig(t, ordOpts{})
	o.create(ordMD(hoMD))
	wa := o.warmUp(hoMD, rigProvider)
	wb := o.warmUp(hoMD, ordB)
	hoStepTo(o.rig, hoT(9, 0, 0), 5*time.Minute, nil)
	o.allowOn(hoMD, 1, rigProvider, ordB)
	o.restartedWith(wa)
	start := o.clock.Now()
	o.Step() // the first pass, at the restart: the grace counts from it
	for i := 1; i <= 9; i++ {
		o.clock.Advance(5 * time.Second)
		o.Step()
		at := o.clock.Now().Sub(start)
		live := o.liveInstances(hoMD)
		if at < agentGrace {
			if len(live) != 0 {
				t.Fatalf("at %s after the restart, inside the grace, %d live Instances: B's registered Allocation took the placement ahead of A's", at, len(live))
			}
			continue
		}
		if len(live) != 1 || live[0].Spec.AllocationRef.Name != wb || !sameAllocs(o.allocsOn(hoMD, rigProvider), []string{wa}) || !sameAllocs(o.allocsOn(hoMD, ordB), []string{wb}) {
			t.Fatalf("at %s after the restart, the grace's end: %d live Instances (%v); want the missing primary on B's warm Allocation %s, with no new Allocation", at, len(live), live, wb)
		}
	}
}

// TestAReplacementsDeferralLapsAtItsStartDeadline (the plan's Q3, ruled 16 Nov 2026): P is on A, whose submit limit of
// 1 P fills, and B holds a warm Allocation. The controller is down from 10:19:20 and restarts at 10:49:00, when P's
// replacement, planned for 10:19:30, has not been made. B's agent has not registered again, so the warm step defers
// the placement; its grace would end at 10:49:45, but P's replacement is late after 10:49:30 (P's drain start 10:50:20
// minus the measured start-to-ready time, 50 s), so the deferral lapses there: the pass asks to run again at 10:49:30,
// and at 10:49:30, not 10:49:45, the replacement is submitted on B, a new Allocation. The pass runs every second.
func TestAReplacementsDeferralLapsAtItsStartDeadline(t *testing.T) {
	o := newOrdRig(t, ordOpts{aLimit: 1})
	o.create(ordMD(hoMD))
	wb := o.warmUp(hoMD, ordB)
	p := o.primaryOnA(hoMD, rigProvider, ordB)
	hoStepTo(o.rig, hoT(10, 19, 20), 5*time.Minute, nil)
	if n := len(hoRepl(o.rig, p.Name)); n != 0 {
		t.Fatalf("%d replacements at 10:19:20, before its planned time", n)
	}
	o.clock.Advance(hoT(10, 49, 0).Sub(o.clock.Now()))
	o.syncProviders() // the Providers' controllers kept running while the lifecycle controller was down
	o.restartedWith(wb)
	pass := o.passOf(hoMD)
	var pred *v1alpha1.Instance
	for _, in := range pass.insts {
		if in.Name == p.Name {
			pred = in
		}
	}
	if ok, err := pass.provisionWith(pred, placeOpts{skip: rigProvider}); err != nil || ok || !pass.deferred || !pass.next.Equal(hoT(10, 49, 30)) {
		bp := pass.pvByName[ordB]
		t.Fatalf("a pass at 10:49:00: placed %v (%v), deferred %v, next %v; want it deferred, to run again at its deadline, 10:49:30, not at the grace's end 10:49:45 (B usable: %v; B's warm Allocations: %d)",
			ok, err, pass.deferred, pass.next, bp != nil && bp.usable(), len(pass.warmOf(bp)))
	}
	for o.clock.Now().Before(hoT(10, 49, 30)) {
		o.Step()
		if n := len(hoRepl(o.rig, p.Name)); n != 0 {
			t.Fatalf("a replacement at %s, before the deadline: the placement was not deferred", o.clock.Now().Format(time.TimeOnly))
		}
		o.clock.Advance(time.Second)
	}
	o.Step()
	reps := hoRepl(o.rig, p.Name)
	if len(reps) != 1 {
		t.Fatalf("%d replacements at 10:49:30, want one: the deferral lapses at the deadline", len(reps))
	}
	prov, alloc := where(reps[0])
	if !reps[0].CreationTimestamp.Time.Equal(hoT(10, 49, 30)) || prov != ordB || alloc == wb || len(o.allocsOn(hoMD, ordB)) != 2 {
		t.Fatalf("the replacement was made at %s on %s, Allocation %s; B has %v; want 10:49:30, a new Allocation on B beside the warm %s",
			reps[0].CreationTimestamp.Time.Format(time.TimeOnly), prov, alloc, o.allocsOn(hoMD, ordB), wb)
	}
}

// TestAReplacementWithASlotSubmitsOnItsOwnProviderWithoutWaiting: P is on A, which has no submit limit, and B's warm
// Allocation has not registered again since the controller restarted at 10:19:40, just after P's replacement time
// (10:19:30). HBEP-7 sends the replacement to its predecessor's Provider first, when it has a slot, so it is a new
// submission on A at 10:19:40, and B's Allocation is neither used nor waited for: amendment #719 holds only the new
// submissions that come after the warm Allocation's step in the order, and a replacement's own-Provider submission
// comes first. That is pass.waitsBeforeOwnSubmit's default, false, the first row. The second row sets the predicate,
// the literal reading #719 dropped: the replacement waits for the grace's end, 10:20:25, which is before its
// deadline, 10:49:30, and is then submitted on A all the same: the wait ends, and B's Allocation was never going to
// take it.
func TestAReplacementWithASlotSubmitsOnItsOwnProviderWithoutWaiting(t *testing.T) {
	for _, c := range []struct {
		name    string
		literal bool
		at      time.Time
	}{
		{"the predicate's default: at once", false, hoT(10, 19, 40)},
		{"waitsBeforeOwnSubmit: at the grace's end", true, hoT(10, 20, 25)},
	} {
		t.Run(c.name, func(t *testing.T) {
			o := newOrdRig(t, ordOpts{})
			o.create(ordMD(hoMD))
			wb := o.warmUp(hoMD, ordB)
			p := o.primaryOnA(hoMD, rigProvider, ordB)
			hoStepTo(o.rig, hoT(10, 19, 20), 5*time.Minute, nil)
			o.clock.Advance(20 * time.Second)
			o.restartedWith(wb)
			if c.literal {
				o.r.waitsBeforeOwnSubmit = func() bool { return true }
			}
			for o.clock.Now().Before(c.at) {
				o.Step()
				if n := len(hoRepl(o.rig, p.Name)); n != 0 {
					t.Fatalf("a replacement at %s, before %s", o.clock.Now().Format(time.TimeOnly), c.at.Format(time.TimeOnly))
				}
				o.clock.Advance(time.Second)
			}
			o.Step()
			reps := hoRepl(o.rig, p.Name)
			if len(reps) != 1 {
				t.Fatalf("%d replacements at %s, want one", len(reps), c.at.Format(time.TimeOnly))
			}
			prov, alloc := where(reps[0])
			if !reps[0].CreationTimestamp.Time.Equal(c.at) || prov != rigProvider || alloc == wb || len(o.allocsOn(hoMD, rigProvider)) != 2 || !sameAllocs(o.allocsOn(hoMD, ordB), []string{wb}) {
				t.Fatalf("the replacement was made at %s on %s, Allocation %s; A has %v, B has %v; want %s, a new Allocation on A, and B's warm %s untouched",
					reps[0].CreationTimestamp.Time.Format(time.TimeOnly), prov, alloc, o.allocsOn(hoMD, rigProvider), o.allocsOn(hoMD, ordB), c.at.Format(time.TimeOnly), wb)
			}
		})
	}
}

// TestAStartedLosersAllocationTakesTheWinnersReplacement is the last clause of HBEP-7's "A started loser is kept" row:
// the loser's kept Allocation takes the next replacement with no new Allocation. The fixture is that row's, with A's
// 12 h partition: P on A is ready at 08:01:10 with lease end 20:00:20, its drain start is 17:50:20 and its replacement
// R1 is planned at 17:50:20 - (0 + 50 s + 30 min) = 17:19:30. R1's job on A starts at 17:19:40 with an engine that
// never gets ready, and R2, seeded on B, wins: R1 ends HedgeLost and its Allocation is kept (hard end 05:19:40 the
// next day, so a drain start 03:09:40, more than 9 hours after P's, later by any lead). R2's job is then cancelled
// outside the controller, and P's new replacement is placed on R1's kept Allocation on A in that pass or the next:
// A keeps its two Allocations, P's and R1's, and no job is submitted anywhere.
func TestAStartedLosersAllocationTakesTheWinnersReplacement(t *testing.T) {
	h := newHedgeRig(t, withMaxTime(12*time.Hour))
	md := mdSpec(hoMD)
	seededOnly(md)
	h.create(md)
	for range 7 {
		h.clock.Advance(10 * time.Second)
		h.Step()
	}
	ins := h.instances(hoMD)
	if len(ins) != 1 || ins[0].Status.Phase != v1alpha1.InstanceReady || ins[0].Status.LeaseEnd == nil || !ins[0].Status.LeaseEnd.Time.Equal(hoT(20, 0, 20)) {
		t.Fatalf("want one primary, ready, with lease end 20:00:20; got %+v", ins)
	}
	p := ins[0]
	hoStepTo(h.rig, hoT(17, 19, 20), 5*time.Minute, nil)
	hoStepTo(h.rig, hoT(17, 19, 30), 10*time.Second, nil)
	reps := hoRepl(h.rig, p.Name)
	if len(reps) != 1 || reps[0].Labels[v1alpha1.LabelProvider] != rigProvider {
		t.Fatalf("P's replacements at 17:19:30: %+v, want one, on A", reps)
	}
	r1, r1Alloc := reps[0].Name, reps[0].Spec.AllocationRef.Name
	h.slow[r1Alloc] = true
	h.clock.Advance(10 * time.Second)
	h.Step()
	if a, ok := h.allocation(r1Alloc); !ok || a.Status.AtProvider.StartTime == nil {
		t.Fatalf("R1's job has not started at 17:19:40: %+v", a)
	}
	r2 := h.seedHedgeMember(hoMD, r1, p.Name, hedgeB)
	h.until(10*time.Minute, 10*time.Second, "R2 ready", func() bool { return h.mustInstance(r2).Status.Phase == v1alpha1.InstanceReady })
	if lost := h.mustInstance(r1); !ended(lost) || lost.Status.EndReason != v1alpha1.ReasonHedgeLost || h.released(r1Alloc) {
		t.Fatalf("R1 is %s, ended for %q, its Allocation released %v: want HedgeLost with the Allocation kept", lost.Status.Phase, lost.Status.EndReason, h.released(r1Alloc))
	}
	subsA, subsB := h.batch.Count("sbatch"), h.b.batch.Count("sbatch")
	r2Alloc, _ := h.allocation(h.mustInstance(r2).Spec.AllocationRef.Name)
	h.b.batch.CancelExternally(r2Alloc.Annotations[v1alpha1.AnnotationJobID])
	var next v1alpha1.Instance
	h.until(10*time.Minute, 10*time.Second, "P's new replacement", func() bool {
		for _, in := range hoRepl(h.rig, p.Name) {
			if in.Name != r1 && in.Name != r2 && !ended(&in) {
				next = in
			}
		}
		return next.Name != ""
	})
	if prov, alloc := where(next); prov != rigProvider || alloc != r1Alloc {
		t.Fatalf("P's new replacement is on %s, Allocation %s; want R1's kept Allocation %s on A", prov, alloc, r1Alloc)
	}
	if a, b := h.batch.Count("sbatch"), h.b.batch.Count("sbatch"); a != subsA || b != subsB {
		t.Fatalf("A received %d submissions and B %d, want %d and %d: no job is submitted for a replacement placed on a kept Allocation", a, b, subsA, subsB)
	}
}
