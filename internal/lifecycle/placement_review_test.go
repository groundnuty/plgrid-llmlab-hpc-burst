package lifecycle

// The rows of #724's first review (Astra's points 2 to 5): a deferral never holds a submission past a replacement's
// start deadline, through the no-overlap decision (reusableWarm) or through a hedge whose predecessor is gone; a
// deferral holds only new SUBMISSIONS, never a later warm step (amendment #719 as reworded); and `only` restricts the
// submit sweep as it does the warm one. Fixtures are placement_order_rig_test.go's; the timeline is the Q3 row's
// (placement_warm_test.go): P on A with a 4 h limit, its drain start 10:50:20, its replacement planned at 10:19:30 and
// late after 10:49:30 (the drain start minus the measured start-to-ready time of 50 s).

import (
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// TestAnUnregisteredSpareNeverHoldsAQueuedSuccessorPastTheDeadline (Astra's point 2): P is on A, whose per-user GPU cap
// of 1 P fills, so the replacement cannot run beside P there, and A has no submit limit, so a queued no-overlap
// successor could be made on it. B's only submit slot is held by B's own warm spare, whose agent has not registered
// again since the controller restarted at 10:49:00, so B has no free slot and the spare is no room until it
// registers. Until the deadline, 10:49:30 (P's drain start 10:50:20 minus 50 s), the unregistered spare counts as room
// beside (reusableWarm), so the placement waits for it. At the deadline it does not: the mode is entered, and A's
// queued successor is submitted at 10:49:30, not at the grace's end, 10:49:45. The row's B has NO free slot: with
// one, the deadline row of TestAReplacementsDeferralLapsAtItsStartDeadline submits on B at the deadline either way.
func TestAnUnregisteredSpareNeverHoldsAQueuedSuccessorPastTheDeadline(t *testing.T) {
	o := newOrdRig(t, ordOpts{aGPUCap: 1, bLimit: 1})
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
	_, subsB, _ := o.subs()
	for o.clock.Now().Before(hoT(10, 49, 30)) {
		o.Step()
		if n := len(hoRepl(o.rig, p.Name)); n != 0 {
			t.Fatalf("a replacement at %s, before the deadline: the spare counts as room beside until then", o.clock.Now().Format(time.TimeOnly))
		}
		o.clock.Advance(time.Second)
	}
	o.Step() // 10:49:30
	reps := hoRepl(o.rig, p.Name)
	if len(reps) != 1 {
		t.Fatalf("%d replacements at 10:49:30, want A's queued successor, made at the deadline", len(reps))
	}
	prov, alloc := where(reps[0])
	if !reps[0].CreationTimestamp.Time.Equal(hoT(10, 49, 30)) || prov != rigProvider || reps[0].Annotations[annotationNoOverlapSuccessor] == "" ||
		len(o.allocsOn(hoMD, rigProvider)) != 2 || !sameAllocs(o.allocsOn(hoMD, ordB), []string{wb}) {
		t.Fatalf("the replacement was made at %s on %s, Allocation %s, marked %q; A has %v, B has %v; want 10:49:30, a marked no-overlap successor on a new Allocation on A, and B's spare %s alone on B",
			reps[0].CreationTimestamp.Time.Format(time.TimeOnly), prov, alloc, reps[0].Annotations[annotationNoOverlapSuccessor], o.allocsOn(hoMD, rigProvider), o.allocsOn(hoMD, ordB), wb)
	}
	if _, b, _ := o.subs(); b != subsB {
		t.Fatalf("B received %d submissions, want none: its only slot is the spare's", b-subsB)
	}
}

// TestAReplacementHedgesDeferralLapsAtOnceWhenItsPredecessorIsGone (Astra's point 3): P's replacement R1 is a hedge of
// one member, pending behind 10 h on A, and the controller restarts at 10:35:00 with P gone (its Instance ended and
// deleted: the pass's list is read without it, as TestAReplacementWhosePredecessorIsGoneWaitsTheMinimumWait does).
// R1's hedge time is its submission, 10:19:40, plus the 10 min minimum, 10:29:40, so the hedge is due its second
// member, and B's warm spare, the only warm Allocation it could take, has not registered again. The hedge's start
// deadline is passed (a gone predecessor reads as "now"), so no deferral holds it: its member is made at once, on a
// new Allocation on B, and not after the grace's end, 10:35:45. The spare is untouched.
func TestAReplacementHedgesDeferralLapsAtOnceWhenItsPredecessorIsGone(t *testing.T) {
	o := newOrdRig(t, ordOpts{})
	o.batch.Enqueue(pfake.Script{}, pfake.Script{QueueWait: 10 * time.Hour})
	md := ordMD(hoMD)
	md.Spec.HedgeWidth = nil // the default, 2
	o.create(md)
	wb := o.warmUp(hoMD, ordB)
	p := o.primaryOnA(hoMD, rigProvider, ordB)
	o.r.hedgeFrozen = func(hedgeGroup) bool { return true } // no growth until the restart: the row makes the second member
	r1 := o.replacementAt(p)
	if prov, _ := where(r1); prov != rigProvider {
		t.Fatalf("R1 is on %s, want A", prov)
	}
	hoStepTo(o.rig, hoT(10, 35, 0), 10*time.Second, nil)
	if n := len(hoRepl(o.rig, p.Name)); n != 1 {
		t.Fatalf("%d members of the hedge at 10:35:00 before the restart, want R1 alone", n)
	}
	o.restartedWith(wb) // a new Reconciler: no frozen hook, no registrations
	ps := o.passOf(hoMD)
	var kept []*v1alpha1.Instance
	for _, in := range ps.insts {
		if in.Name != p.Name {
			kept = append(kept, in)
		}
	}
	if len(kept) != len(ps.insts)-1 {
		t.Fatalf("P is not among the pass's Instances before it is taken out")
	}
	ps.insts = kept // P gone
	if err := ps.growHedges(); err != nil {
		t.Fatal(err)
	}
	members := o.membersOn(hoMD, r1.Name, ordB)
	if ps.deferred || len(members) != 1 {
		t.Fatalf("after the restart, growth deferred %v and made %d members on B at 10:35:00, want none deferred and one: the hedge's deadline is passed, so no grace holds it", ps.deferred, len(members))
	}
	prov, alloc := where(members[0])
	if prov != ordB || alloc == wb || len(o.allocsOn(hoMD, ordB)) != 2 || o.released(wb) || o.hostsLive(hoMD, wb) {
		t.Fatalf("the member is on %s, Allocation %s; B has %v; want a new Allocation on B beside the spare %s, which is untouched", prov, alloc, o.allocsOn(hoMD, ordB), wb)
	}
}

// TestADeferralHoldsOnlySubmissionsNotALaterWarmStep (Astra's point 4, with amendment #719 as reworded): the deployment
// wants one Instance and allows A, then B; A's warm spare WA has not registered again since the controller restarted
// at 09:00:00, and B's spare WB registers at once. The placement holds only the new submissions after WA's step: the
// missing primary is placed on WB within 10 s of the restart, while WA is waited for no longer than it takes WB's
// agent to register. Before this rule the whole placement waited for WA until 09:00:45, and WB, whose maxIdle falls
// at 09:00:20 (the profile's maxIdle is set so that it does), was released by retention during the wait, so the primary
// found neither. WB is warmed up first, so it is idle the longer: its maxIdle falls first, and WA's, about 80 s later,
// falls after the grace ends. Nothing is submitted; WA is untouched; WB is kept, hosting the primary.
func TestADeferralHoldsOnlySubmissionsNotALaterWarmStep(t *testing.T) {
	o := newOrdRig(t, ordOpts{})
	o.create(ordMD(hoMD))
	wb := o.warmUp(hoMD, ordB)
	wa := o.warmUp(hoMD, rigProvider)
	tb, ta := o.idleSinceOf(wb), o.idleSinceOf(wa)
	if ta.Sub(tb) < time.Minute {
		t.Fatalf("WA has been idle since %s and WB since %s: the fixture needs WB at least a minute older", ta.Format(time.TimeOnly), tb.Format(time.TimeOnly))
	}
	restart := hoT(9, 0, 0)
	maxIdle := restart.Add(20 * time.Second).Sub(tb) // WB's idle limit is reached at 09:00:20
	hoStepTo(o.rig, restart.Add(-time.Minute), 5*time.Minute, nil)
	o.setMaxIdle(maxIdle)
	hoStepTo(o.rig, restart, 10*time.Second, nil)
	if o.released(wb) || o.released(wa) {
		t.Fatalf("a spare was released before the restart, at %s", o.clock.Now().Format(time.TimeOnly))
	}
	o.allowOn(hoMD, 1, rigProvider, ordB)
	o.restartedWith(wa)
	subsA, subsB, _ := o.subs()
	for i := 0; i <= 12; i++ { // 09:00:00 to 09:01:00, every 5 s
		o.Step()
		at := o.clock.Now().Sub(restart)
		if at == 10*time.Second {
			live := o.liveInstances(hoMD)
			if len(live) != 1 || live[0].Spec.AllocationRef.Name != wb {
				t.Fatalf("at 09:00:10 the live Instances are %v, want the missing primary on B's registered spare %s: only new submissions wait for A's spare", live, wb)
			}
		}
		o.clock.Advance(5 * time.Second)
	}
	live := o.liveInstances(hoMD)
	if len(live) != 1 || live[0].Spec.AllocationRef.Name != wb || o.released(wb) {
		t.Fatalf("at 09:01:00 the live Instances are %v and B's spare %s is released %v: retention released the spare the primary was placed on", live, wb, o.released(wb))
	}
	if o.released(wa) || o.hostsLive(hoMD, wa) || !sameAllocs(o.allocsOn(hoMD, rigProvider), []string{wa}) {
		t.Fatalf("A's spare %s is released %v or hosts an Instance (A has %v): want it untouched", wa, o.released(wa), o.allocsOn(hoMD, rigProvider))
	}
	if a, b, _ := o.subs(); a != subsA || b != subsB {
		t.Fatalf("%d submissions on A and %d on B after the restart, want none", a-subsA, b-subsB)
	}
}

// TestOnlyAGivesNoSubmissionOnBWhenAIsFull (Astra's point 5): `only: A` with A full and B free creates no Allocation at
// all, warm or submitted: the extra, which names its Provider, never lands on B. A's one submit slot is held by another
// deployment's pending job; B has no limit and no warm Allocation, so a submit sweep that ignored `only` would make the
// Allocation on B. The control, with no filter, does: the row is not vacuous.
func TestOnlyAGivesNoSubmissionOnBWhenAIsFull(t *testing.T) {
	for _, c := range []struct {
		name   string
		opts   placeOpts
		placed bool
	}{
		{"only A", placeOpts{only: rigProvider}, false},
		{"only A, with the extra's options", placeOpts{only: rigProvider, extra: true, caps: true}, false},
		{"no filter (the control)", placeOpts{}, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			o := newOrdRig(t, ordOpts{aLimit: 1})
			o.batch.Enqueue(pfake.Script{QueueWait: 10 * time.Hour})
			o.Step()
			x := mdSpec("md-x") // fills A's one slot, pending behind 10 h
			o.create(x)
			for range 3 {
				o.clock.Advance(10 * time.Second)
				o.Step()
			}
			if n := len(o.allocations("md-x")); n != 1 {
				t.Fatalf("md-x has %d Allocations, want the one that fills A's limit of 1", n)
			}
			o.create(ordMD(hoMD))
			o.allowOn(hoMD, 0, rigProvider, ordB)
			ps := o.passOf(hoMD)
			placed, err := ps.provisionWith(nil, c.opts)
			if err != nil || placed != c.placed {
				t.Fatalf("provisionWith placed %v (%v), want %v", placed, err, c.placed)
			}
			onB := o.allocsOn(hoMD, ordB)
			if c.placed != (len(onB) == 1) || len(o.allocsOn(hoMD, rigProvider)) != 0 {
				t.Fatalf("A has %v and B has %v for the deployment; want %v", o.allocsOn(hoMD, rigProvider), onB, map[bool]string{true: "one new Allocation on B", false: "none anywhere"}[c.placed])
			}
			if _, b, _ := o.subs(); !c.placed && b != 0 {
				t.Fatalf("B received %d submissions, want none", b)
			}
		})
	}
}
