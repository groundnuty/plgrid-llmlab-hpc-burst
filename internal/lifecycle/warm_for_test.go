package lifecycle

// A replacement goes on a warm Allocation only when the warm Allocation's drain start is later than its predecessor's BY AT
// LEAST the lead an Instance needs (HBEP-7, Hedging (beta), Started losers are kept; Over-allocation and qualification
// (beta), The lowest-ranked is kept; #604 PR 3's plan, Q4; amendment #711 of the science agent's ruling on #708's Fable
// review): warmFor, which provisioning places by and the no-overlap decision reads (reusableWarm), so placement and the mode
// agree. The lead (leadOf) is the drain window, handoverMargin and health.startTimeout: 2 h 10 min + 30 min + 1 h = 3 h 40 min
// in this fixture. Without the rule the ranked-out extra's Allocation, which started with the primary's and drains with it,
// took the primary's replacement, which then ended at the very moment it was to take over (CapacityDropped, no
// ReplacementLate, no new job), and one that drained moments after it left the deployment without a ready Instance.
//
// The fixture is handover_test.go's: a 4 h job limit, drain window 2 h 10 min, handoverMargin 30 min, no wait but the
// jobs' own 0 s, start-to-ready 50 s. A job that starts at s ends at s + 4 h and drains from s + 1 h 50 min, so the
// warm Allocation's drain start is its job's start plus 1 h 50 min and the predecessor's is its own. In 4 h jobs the
// two never differ by 3 h 40 min, so every row here is "not used"; the rows where it is used, and the boundary, are in
// warm_lead_test.go on a 12 h partition. Every expected time and count is worked by hand in the row.

import (
	"slices"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	afake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// TestAReplacementGoesOnAWarmAllocationOnlyIfItDrainsLater: instances 2, then 1, with a warm Allocation left, and the
// primary's replacement at its planned time. The first row was "used" before the lead: a warm Allocation draining 10 min
// after the predecessor took the replacement; under the lead (3 h 40 min) 10 min is not enough, so it is not used now. The first job starts at 08:00:20 (hard end 12:00:20, drain start
// 09:50:20), so its replacement is planned at 09:50:20 - (0 + 50 s + 30 min) = 09:19:30.
func TestAReplacementGoesOnAWarmAllocationOnlyIfItDrainsLater(t *testing.T) {
	for _, c := range []struct {
		name string
		// raise is when instances goes to 2 (the zero time: at the start); the second job then starts 20 s later.
		raise time.Time
		// slow keeps the second Instance's worker from running, so it is the one a lowering ends, and its Allocation,
		// kept, is the warm one; without it both serve and the lowering drains the nearer hard end.
		slow     bool
		lower    time.Time
		planned  time.Time // the survivor's replacement's planned time
		warmUsed bool
		// warmDrain and predDrain are the two drain starts, for the failure message.
		warmDrain, predDrain string
	}{
		{name: "the warm Allocation drains 10 min AFTER the predecessor, less than the lead of 3 h 40 min: not used", raise: hoT(8, 10, 0), slow: true, lower: hoT(8, 11, 0), planned: hoT(9, 19, 30), warmUsed: false,
			warmDrain: "10:00:20", predDrain: "09:50:20"},
		{name: "the warm Allocation drains exactly AT the predecessor's drain start: not used", slow: true, lower: hoT(8, 2, 0), planned: hoT(9, 19, 30), warmUsed: false,
			warmDrain: "09:50:20", predDrain: "09:50:20"},
		{name: "the warm Allocation drains 10 min BEFORE the predecessor: not used", raise: hoT(8, 10, 0), lower: hoT(8, 20, 0), planned: hoT(9, 29, 30), warmUsed: false,
			warmDrain: "09:50:20", predDrain: "10:00:20"},
	} {
		t.Run(c.name, func(t *testing.T) {
			rg := newRig(t, withMaxTime(4*time.Hour))
			calls := 0
			rg.engine = func(string) afake.Engine {
				calls++
				if c.slow && calls == 2 {
					return afake.Engine{StartDelay: 6 * time.Hour}
				}
				return afake.Engine{}
			}
			md := mdSpec(hoMD)
			if c.raise.IsZero() {
				md.Spec.Instances = 2
			}
			rg.create(md)
			if !c.raise.IsZero() {
				hoStepTo(rg, c.raise, 10*time.Second, nil)
				rg.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 2 })
			}
			hoStepTo(rg, c.lower, 10*time.Second, nil)
			rg.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 1 })
			rg.until(2*time.Minute, 10*time.Second, "one live Instance", func() bool { return len(rg.liveInstances(hoMD)) == 1 })
			survivor := rg.liveInstances(hoMD)[0]
			warm := ""
			for _, a := range rg.allocations(hoMD) {
				if a.Name != survivor.Spec.AllocationRef.Name {
					warm = a.Name
				}
			}
			if warm == "" || allocReleased(rg, warm) {
				t.Fatalf("no warm Allocation kept beside the survivor's: the row proves nothing (survivor on %s)", survivor.Spec.AllocationRef.Name)
			}
			if n := rg.submissions(); n != 2 {
				t.Fatalf("%d submissions before the replacement, want the two jobs", n)
			}
			hoStepTo(rg, c.planned.Add(-10*time.Second), time.Minute, nil)
			hoStepTo(rg, c.planned.Add(10*time.Second), 10*time.Second, nil)
			reps := hoRepl(rg, survivor.Name)
			if len(reps) != 1 {
				t.Fatalf("%d replacements of %s at %s, want one (planned %s)", len(reps), survivor.Name, rg.clock.Now().Format(time.TimeOnly), c.planned.Format(time.TimeOnly))
			}
			onWarm := reps[0].Spec.AllocationRef.Name == warm
			wantSubmissions := 3
			if c.warmUsed {
				wantSubmissions = 2
			}
			if onWarm != c.warmUsed || rg.submissions() != wantSubmissions {
				t.Fatalf("the replacement is on the warm Allocation: %v, %d submissions, want %v and %d (warm drains %s, predecessor %s)",
					onWarm, rg.submissions(), c.warmUsed, wantSubmissions, c.warmDrain, c.predDrain)
			}
		})
	}
}

// TestNoOverlapAWarmAllocationDrainingFirstGivesNoRoomBeside: maxSubmitPerUser 2, the primary's Allocation and a
// retained warm one that drains BEFORE it take both slots. Before warmFor the decision counted that warm Allocation as
// room beside (reusableWarm), and the replacement was placed on it to end first; now it is no room, so the mode is
// entered, as with no warm Allocation at all: no replacement at its planned time, and NoOverlapSlot once the primary
// drains. The run: instances 1, ready 08:01:10 (hard end 12:00:20); raised to 2 at 08:10, the second job starts
// 08:10:20 (hard end 12:10:20) and is ready 08:11:10; lowered to 1 at 08:20, which drains the nearer hard end, the
// first, leaving its Allocation warm (drain start 09:50:20) beside the survivor's (drain start 10:00:20). The QOS
// allows 3 until then and 2 from then on, as an operator's edit would (nooverlap_reuse_test.go).
func TestNoOverlapAWarmAllocationDrainingFirstGivesNoRoomBeside(t *testing.T) {
	qos := []v1alpha1.QOSProfile{{Name: "example-s2", MaxSubmitPerUser: i32(3)}}
	rg := newRig(t, append(qosOpts(qos, "", "example-s2"), withMaxTime(4*time.Hour), withBatchConfig(func(c *pfake.Config) { c.MaxSubmitPerUser = 2 }))...)
	rg.create(mdSpec(hoMD))
	hoStepTo(rg, hoT(8, 10, 0), 10*time.Second, nil)
	rg.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 2 })
	rg.until(5*time.Minute, 10*time.Second, "both ready", func() bool { return rg.readyCount(hoMD) == 2 })
	hoStepTo(rg, hoT(8, 20, 0), 10*time.Second, nil)
	rg.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 1 })
	rg.until(2*time.Minute, 10*time.Second, "one live Instance", func() bool { return len(rg.liveInstances(hoMD)) == 1 })
	rg.setSubmitLimit("example-s2", 2)
	survivor := rg.liveInstances(hoMD)[0]
	if got := survivor.Status.LeaseEnd; got == nil || !got.Time.Equal(hoT(12, 10, 20)) {
		t.Fatalf("the survivor's hard end is %v, want 12:10:20: the later-started Instance must be the survivor", got)
	}
	// 09:29:30 is the survivor's replacement's planned time: 10:00:20 - (0 + 50 s + 30 min).
	hoStepTo(rg, hoT(9, 29, 20), time.Minute, nil)
	hoStepTo(rg, hoT(9, 30, 0), 10*time.Second, nil)
	if reps := hoRepl(rg, survivor.Name); len(reps) != 0 {
		t.Fatalf("%d replacements at 09:30:00, want none: the one-slot path holds it back, and the warm Allocation drains first, so it is no room", len(reps))
	}
	hoStepTo(rg, hoT(10, 0, 50), 10*time.Second, nil)
	if c := rg.condition(hoMD, v1alpha1.ConditionDegraded); c.Reason != v1alpha1.ReasonNoOverlapSlot {
		t.Fatalf("at 10:00:50 Degraded = %s/%s %q, want NoOverlapSlot", c.Status, c.Reason, c.Message)
	}
}

// TestNoOverlapTheExtrasKeptAllocationIsNoRoomBeside is TestNoOverlapWarmAllocationTakesTheReplacement at k = 1, where
// the warm Allocation the row is about is the ranked-out extra's, which started with the primary's job and so drains
// with it: not later, so not room beside. The QOS allows 3 until 08:30 and 2 from then on, as an operator's edit would
// (the original row's own device). At 08:01:10 the primary P is ready and the extra X, ranked out against it, is kept
// (listed: P and X, two slots). At 09:19:30 P's replacement is planned: no free slot (2 listed, limit 2), and X's
// Allocation, with the same hard end 12:00:20, drain start 09:50:20, is not later than P's, so it takes nothing: the mode
// is entered as it would be with no warm Allocation. No replacement at 09:19:40, and NoOverlapSlot at 09:50:30. The
// original row's own k = 1 run is not possible: its scale-up at 09:00 would place the second primary on X's kept
// Allocation, which is no longer the one with a later hard end that row needs; that case is the first row of
// TestAReplacementGoesOnAWarmAllocationOnlyIfItDrainsLater.
func TestNoOverlapTheExtrasKeptAllocationIsNoRoomBeside(t *testing.T) {
	qos := []v1alpha1.QOSProfile{{Name: "example-s2", MaxSubmitPerUser: i32(3)}}
	rg := newRig(t, append(qosOpts(qos, "", "example-s2"), withMaxTime(4*time.Hour), withBatchConfig(func(c *pfake.Config) { c.MaxSubmitPerUser = 2 }))...)
	p := hoStartK(rg, nil, true)
	hoStepTo(rg, hoT(8, 30, 0), time.Minute, nil)
	rg.setSubmitLimit("example-s2", 2)
	if n := len(rg.allAllocations()); n != 2 {
		t.Fatalf("%d Allocations at 08:30, want the primary's and the extra's kept one", n)
	}
	hoStepTo(rg, hoT(9, 19, 20), time.Minute, nil)
	hoStepTo(rg, hoT(9, 19, 40), 10*time.Second, nil)
	if reps := hoRepl(rg, p.Name); len(reps) != 0 {
		t.Fatalf("%d replacements at 09:19:40, want none: the extra's kept Allocation drains with the primary and is no room beside", len(reps))
	}
	hoStepTo(rg, hoT(9, 50, 30), 10*time.Second, nil)
	if c := rg.condition(hoMD, v1alpha1.ConditionDegraded); c.Reason != v1alpha1.ReasonNoOverlapSlot {
		t.Fatalf("at 09:50:30 Degraded = %s/%s %q, want NoOverlapSlot", c.Status, c.Reason, c.Message)
	}
}

// TestWarmOfIsTheWarmAllocationsLatestHardEndFirst: warmOf's contract (#604 PR 3's seam): the deployment's Allocations on
// the Provider that are Running, not deleting, and host no live Instance, the latest hard end first, then by name, one
// with no hard end last. By hand: a 12:00, b 14:00, c 14:00, i no hard end are warm and come as b, c, a, i; d is Ended, e
// hosts a live Instance, g is being deleted, and h is on another Provider, so none of them is listed.
func TestWarmOfIsTheWarmAllocationsLatestHardEndFirst(t *testing.T) {
	alloc := func(name, prov string, phase v1alpha1.AllocationPhase, end *time.Time) *v1alpha1.Allocation {
		a := &v1alpha1.Allocation{}
		a.Name = name
		a.Labels = map[string]string{v1alpha1.LabelProvider: prov}
		a.Status.AtProvider.Phase = phase
		if end != nil {
			e := metav1.NewTime(*end)
			a.Status.AtProvider.HardEnd = &e
		}
		return a
	}
	noon, two := hoT(12, 0, 0), hoT(14, 0, 0)
	g := alloc("g", "prov-a", v1alpha1.AllocationRunning, &two)
	del := metav1.NewTime(hoT(9, 0, 0))
	g.DeletionTimestamp = &del
	allocs := map[string]*v1alpha1.Allocation{}
	for _, a := range []*v1alpha1.Allocation{
		alloc("a", "prov-a", v1alpha1.AllocationRunning, &noon), alloc("b", "prov-a", v1alpha1.AllocationRunning, &two),
		alloc("c", "prov-a", v1alpha1.AllocationRunning, &two), alloc("i", "prov-a", v1alpha1.AllocationRunning, nil),
		alloc("d", "prov-a", v1alpha1.AllocationEnded, &two), alloc("e", "prov-a", v1alpha1.AllocationRunning, &two), g,
		alloc("h", "prov-b", v1alpha1.AllocationRunning, &two),
	} {
		allocs[a.Name] = a
	}
	host := member("host", "", hoT(9, 0, 0), v1alpha1.InstanceReady, nil)
	host.Spec.AllocationRef.Name = "e"
	p := &pass{allocs: allocs, insts: []*v1alpha1.Instance{host}, now: hoT(10, 0, 0)}
	var got []string
	for _, a := range p.warmOf(&providerView{name: "prov-a"}) {
		got = append(got, a.Name)
	}
	if want := []string{"b", "c", "a", "i"}; !slices.Equal(got, want) {
		t.Fatalf("warmOf = %v, want %v", got, want)
	}
}
