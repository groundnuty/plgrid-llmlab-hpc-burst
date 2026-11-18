package lifecycle

// The ranked-out Allocation is kept, and step 7 with a surplus (HBEP-7, Test plan, "The ranked-out Allocation is
// kept", "Scale-down with a surplus" and "The excluded scale-down case"; #616, PR 3). Each expected time, reason and
// target is written out in the row.

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// rkNoDelay is an engine with no start delay, which its worker starts at once (a delay of 0 means the default).
const rkNoDelay = time.Nanosecond

// TestARankedOutAllocationIsKeptUnderItsPolicy: X is ranked out at 00:45 (18.0 ms against 20 ms) and its Allocation
// carries idle-since 00:45, with the deployment's end time at 20:00 on the HBEP clock (04:00 the next day on the
// rig's). Under Keep with no maxIdle it stays, to 15:50 (HBEP-7's row says to its hard end; P's replacement is planned
// at 16:00:20 and is placed on this warm Allocation before any new submission, which ends its idleness), with no approval. With a profile maxIdle of 30 min it is
// released at 01:15 (09:15:00), and under ReleaseWhenIdle with a 15 min idle limit at 01:00 (09:00:00), each with no
// approval and a release cause that names the rule.
func TestARankedOutAllocationIsKeptUnderItsPolicy(t *testing.T) {
	for _, c := range []struct {
		name    string
		profile func(*v1alpha1.PlatformProfileSpec)
		edit    func(*v1alpha1.ModelDeployment)
		release time.Time // zero: kept
		cause   string
	}{
		{"Keep with no maxIdle", nil, nil, time.Time{}, ""},
		{"a profile maxIdle of 30 min", func(s *v1alpha1.PlatformProfileSpec) { s.MaxIdle = dur(30 * time.Minute) }, nil, rkAt(1, 15, 0),
			"idle for the profile's maxIdle 30m0s"},
		{"ReleaseWhenIdle after 15 min", nil, func(md *v1alpha1.ModelDeployment) {
			md.Spec.RetentionPolicy = &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle, IdleLimit: dur(15 * time.Minute)}
		}, rkAt(1, 0, 0), "released when idle, after 15m0s"},
	} {
		t.Run(c.name, func(t *testing.T) {
			log := newCauseLog()
			opts := []rigOption{withInterceptors(log.funcs())}
			if c.profile != nil {
				opts = append(opts, withProfile(c.profile))
			}
			h := newRkRig(t, false, opts...)
			p, x := h.start(func(md *v1alpha1.ModelDeployment) {
				endsAt(rigStart.Add(20 * time.Hour))(md)
				if c.edit != nil {
					c.edit(md)
				}
			}, pfake.Script{QueueWait: 10 * time.Minute}, pfake.Script{QueueWait: rkXWait})
			h.engineOf(p, rk200, 0)
			h.engineOf(x, rk180, 0)
			h.stepTo(rkAt(0, 45, 0), 10*time.Second, nil)
			rkAssertRankedOut(t, h, x, p, rkAt(0, 45, 0))
			noApprovals := func() {
				t.Helper()
				var as v1alpha1.ApprovalList
				if err := h.c.List(h.ctx, &as); err != nil || len(as.Items) != 0 {
					t.Fatalf("%d Approvals (%v), want none: the controller releases an Allocation its retention policy lets go with no approval", len(as.Items), err)
				}
			}
			if c.release.IsZero() {
				h.stepTo(rkAt(7, 50, 0), 5*time.Minute, func() {
					if !h.kept(x) {
						t.Fatalf("X's Allocation was released at %s under Keep", h.clock.Now().Format(time.TimeOnly))
					}
				})
				noApprovals()
				return
			}
			h.stepTo(c.release.Add(-10*time.Second), 10*time.Second, func() {
				if !h.kept(x) {
					t.Fatalf("X's Allocation was released at %s, before %s", h.clock.Now().Format(time.TimeOnly), c.release.Format(time.TimeOnly))
				}
			})
			h.tick(20 * time.Second)
			if h.kept(x) {
				t.Fatalf("X's Allocation is still kept at %s, want released at %s", h.clock.Now().Format(time.TimeOnly), c.release.Format(time.TimeOnly))
			}
			noApprovals()
			if cause, deleted := log.deletedWith(h.alloc(x)); !deleted || !strings.Contains(cause, c.cause) {
				t.Fatalf("X's Allocation deleted %v with the release cause %q, want one naming %q", deleted, cause, c.cause)
			}
		})
	}
}

// TestThreeRankedOutEndsSetNoRestartLoop: three RankedOut ends in one class within restart.window add nothing to its
// failure streak and pause nothing; the same three ends with a failure's reason are a streak of three and a pause,
// so the row is not vacuous. X's real end at 00:45 is the first; the others are made the same way, ended later.
func TestThreeRankedOutEndsSetNoRestartLoop(t *testing.T) {
	h := newRkRig(t, false)
	p, x := h.start(nil, pfake.Script{QueueWait: 10 * time.Minute}, pfake.Script{QueueWait: rkXWait})
	h.engineOf(p, rk200, 0)
	h.engineOf(x, rk180, 0)
	h.stepTo(rkAt(0, 45, 0), 10*time.Second, nil)
	rkAssertRankedOut(t, h, x, p, rkAt(0, 45, 0))
	class := v1alpha1.AcceleratorClass{GPUModel: rigGPU, CPUArch: rigArch}
	for _, c := range []struct {
		reason string
		streak int
		paused bool
	}{
		{v1alpha1.ReasonRankedOut, 0, false},
		{v1alpha1.ReasonAllocationFailed, 3, true},
	} {
		pass := h.passOf(hoMD)
		xi := h.mustInstance(x)
		for i, at := range []time.Time{rkAt(0, 46, 0), rkAt(0, 47, 0)} {
			clone := xi.DeepCopy()
			clone.Name = "ended-" + string(rune('a'+i))
			clone.UID = types.UID("uid-" + clone.Name)
			clone.Status.EndReason = c.reason
			end := metav1.NewTime(at)
			clone.Status.EndTime = &end
			pass.insts = append(pass.insts, clone)
		}
		for _, in := range pass.insts {
			if in.Name == x {
				in.Status.EndReason = c.reason
			}
		}
		if got := len(pass.classStreak(class)); got != c.streak {
			t.Errorf("three %s ends: a streak of %d, want %d", c.reason, got, c.streak)
		}
		if paused, _ := pass.loopPaused(class, pass.pvByName[rigProvider].settings); paused != c.paused {
			t.Errorf("three %s ends: paused %v, want %v", c.reason, paused, c.paused)
		}
	}
}

// rkScaleStart makes the scale-down rows' deployment: instances 2, so the pass at 08:00:10 makes P1, P2 and PR 4's
// extra X together, and the provider submits their jobs at 08:00:20 in the order of their Allocations' names, which
// is the order they take the scripts (rkRoles): P1's wait is 10 min and P2's the one given, so P1 starts at 00:10 and
// P2 at 00:20:20, hard ends 20:10:20 and 20:20:20 on the rig's clock (12:10 and 12:20 on the HBEP's), and X's is the
// script given. The roles are the jobs' order and not the selection's choice, which the UID decides while all three pend
// from one second and the job starts then fix. It steps to 09:00:00 (01:00) in the caller.
func rkScaleStart(h *rkRig, xScript pfake.Script, p2 pfake.Script, edit func(*v1alpha1.ModelDeployment)) (p1, p2n, x string) {
	h.t.Helper()
	roles := h.startN(edit, 3, pfake.Script{QueueWait: 10 * time.Minute}, p2, xScript)
	p1, p2n, x = roles[0], roles[1], roles[2]
	for _, n := range roles {
		h.alloc(n) // remembered, for the one a cancellation deletes
	}
	return p1, p2n, x
}

// TestAWarmAllocationDoesNotCancelAPendingSurplus: instances 3, k 1, so the pass at 08:00:10 makes P1, P2, P3 and PR 4's
// extra X together, and the provider submits their jobs at 08:00:20 in the order of their Allocations' names, which is
// the order they take the scripts (rkRoles): P1's wait is 20 min, P2's 10 min, and P3's and X's 3 h. P1 starts at
// 08:20:20 and P2 at 08:10:20, so P2 has the nearer hard end, 20:10:20 against 20:20:20; P3 and X pend. Which of the
// last two the selection chose as the third primary while both pend is left to their UIDs, so the row calls them S and
// S' and asserts nothing that depends on it. At 09:00 instances is lowered to 1: P1, the later hard end, is chosen, P2
// drains (lowered), and of the two non-ready surplus Instances k = 1 leaves ONE alone, in the selection's fixed order,
// and the other is cancelled with the trim cause. Once P2 has ended and its Allocation carries idle-since it is the
// deployment's warm Allocation, and for the next 10 minutes, 60 passes, none cancels the one left alone: a warm
// Allocation counts toward k only in the extra's creation bound (PR 4), never in the trim of surplus that exists (the
// science agent's ruling on #635, option B; HBEP-7 amendment #636).
func TestAWarmAllocationDoesNotCancelAPendingSurplus(t *testing.T) {
	h := newRkRig(t, false)
	h.defaultDelay = rkNoDelay
	roles := h.startN(func(md *v1alpha1.ModelDeployment) { md.Spec.Instances = 3 }, 4,
		pfake.Script{QueueWait: 20 * time.Minute}, pfake.Script{QueueWait: 10 * time.Minute}, pfake.Script{QueueWait: 3 * time.Hour}, pfake.Script{QueueWait: 3 * time.Hour})
	p1, p2, s1, s2 := roles[0], roles[1], roles[2], roles[3]
	for _, n := range roles {
		h.alloc(n)
	}
	h.stepTo(rkAt(1, 0, 0), 10*time.Second, nil)
	for _, n := range []string{p1, p2} {
		if h.mustInstance(n).Status.Phase != v1alpha1.InstanceReady {
			t.Fatalf("%s is %s at 01:00, want ready", n, h.mustInstance(n).Status.Phase)
		}
	}
	if a, b := h.mustInstance(p1).Status.LeaseEnd, h.mustInstance(p2).Status.LeaseEnd; a == nil || b == nil || !b.Before(a) {
		t.Fatalf("hard ends %v and %v, want P2's the nearer", a, b)
	}
	for _, n := range []string{s1, s2} {
		if h.mustInstance(n).Status.Phase == v1alpha1.InstanceReady || ended(h.mustInstance(n)) {
			t.Fatalf("%s is %s at 01:00, want pending", n, h.mustInstance(n).Status.Phase)
		}
	}
	h.mutateMD(hoMD, func(md *v1alpha1.ModelDeployment) { md.Spec.Instances = 1 })
	pass := h.passOf(hoMD)
	var got []string
	for _, tg := range pass.scaleDownTargets(pass.primaries(), 0) {
		got = append(got, tg.instance.Name+":"+string(tg.effect)+":"+string(tg.cause))
	}
	// P2 drains (lowered), and exactly one of S and S' is cancelled beyond k (the trim cause); written for each choice.
	want1 := []string{s2 + ":cancelPending:overAllocationTrim", p2 + ":drain:lowered"}
	want2 := []string{s1 + ":cancelPending:overAllocationTrim", p2 + ":drain:lowered"}
	if g := strings.Join(got, ","); g != strings.Join(want1, ",") && g != strings.Join(want2, ",") {
		t.Fatalf("targets %v, want P2 drained (lowered) and one of S, S' cancelled (overAllocationTrim)", got)
	}
	left := s1
	if strings.HasPrefix(got[0], s1+":") {
		left = s2
	}
	h.until(10*time.Minute, 10*time.Second, "P2 ended and its Allocation warm", func() bool {
		_, anchored := h.idleSince(p2)
		return h.mustInstance(p2).Status.Phase == v1alpha1.InstanceEnded && anchored
	})
	if a, _ := h.allocation(h.alloc(p2)); a == nil || a.Status.AtProvider.Phase != v1alpha1.AllocationRunning || len(h.passOf(hoMD).liveOn(a.Name)) != 0 {
		t.Fatalf("P2's Allocation %+v, want it running with no live Instance: the warm Allocation the row is about", a)
	}
	for i := range 60 {
		h.tick(10 * time.Second)
		if in := h.mustInstance(left); ended(in) || !h.kept(left) {
			t.Fatalf("pass %d after P2's Allocation turned warm: %s is %s (%s), Allocation kept %v, want it still pending", i+1, left, in.Status.Phase, in.Status.EndReason, h.kept(left))
		}
	}
	if got := h.submissions(); got != 4 {
		t.Fatalf("%d submissions, want the four from before the lowering", got)
	}
	if h.mustInstance(p1).Status.Phase != v1alpha1.InstanceReady {
		t.Fatal("P1 stopped serving")
	}
}

// TestScaleDownWithASurplus: instances 2, P1 and P2 ready with hard ends 12:10 and 12:20 (HBEP clock), and the extra
// X pending, at 01:00. Targets come from the pass as it reads the spec after the edit; each is "Instance:effect:cause".
//
//	instances lowered to 1: P1, the nearest hard end, drains with the lowered cause; X is left alone, pending. Once P1
//	  has drained, its Allocation is the deployment's warm Allocation; it counts toward k only in the extra's creation
//	  bound, so X stays pending (the science agent's ruling on #635, option B; the row runs 3 min past the lowering, and
//	  TestAWarmAllocationDoesNotCancelAPendingSurplus runs it with a third primary).
//	overAllocation lowered to 0: X is cancelled with the trim cause (it is surplus beyond k = 0 at the current count),
//	  and nothing drains.
//	X started and not ready, overAllocation lowered to 0: X's Instance ends and its Allocation is kept, the trim cause.
func TestScaleDownWithASurplus(t *testing.T) {
	long := pfake.Script{QueueWait: 3 * time.Hour}
	for _, c := range []struct {
		name    string
		x       pfake.Script
		xDelay  time.Duration
		settle  time.Duration // how long the rig runs after the edit
		edit    func(*v1alpha1.ModelDeployment)
		targets func(p1, p2, x string) []string
		after   func(t *testing.T, h *rkRig, p1, p2, x string)
	}{
		{"instances lowered to 1", long, 0, 3 * time.Minute, func(md *v1alpha1.ModelDeployment) { md.Spec.Instances = 1 },
			func(p1, _, _ string) []string { return []string{p1 + ":drain:lowered"} },
			func(t *testing.T, h *rkRig, p1, p2, x string) {
				if in := h.mustInstance(p1); in.Status.Phase != v1alpha1.InstanceDraining && in.Status.Phase != v1alpha1.InstanceEnded {
					t.Fatalf("P1 is %s, want draining or ended", in.Status.Phase)
				}
				if h.mustInstance(p2).Status.Phase != v1alpha1.InstanceReady {
					t.Fatal("P2, the later hard end, stopped serving")
				}
				if xi := h.mustInstance(x); ended(xi) || !h.kept(x) {
					t.Fatalf("X is %s: the extra is left alone, pending", xi.Status.Phase)
				}
			}},
		{"overAllocation lowered to 0", long, 0, time.Minute, func(md *v1alpha1.ModelDeployment) { md.Spec.OverAllocation = ptrTo(int32(0)) },
			func(_, _, x string) []string { return []string{x + ":cancelPending:overAllocationTrim"} },
			func(t *testing.T, h *rkRig, p1, p2, x string) {
				if ph := h.phase(x); (ph != v1alpha1.InstanceEnded && ph != "gone") || h.kept(x) {
					t.Fatalf("X is %s, Allocation kept %v, want ended and cancelled", ph, h.kept(x))
				}
				for _, n := range []string{p1, p2} {
					if h.mustInstance(n).Status.Phase != v1alpha1.InstanceReady {
						t.Fatalf("%s is %s, want ready: nothing drains", n, h.mustInstance(n).Status.Phase)
					}
				}
			}},
		{"X started, overAllocation lowered to 0", pfake.Script{QueueWait: 5 * time.Minute}, 6 * time.Hour, time.Minute,
			func(md *v1alpha1.ModelDeployment) { md.Spec.OverAllocation = ptrTo(int32(0)) },
			func(_, _, x string) []string { return []string{x + ":endKeepAllocation:overAllocationTrim"} },
			func(t *testing.T, h *rkRig, p1, p2, x string) {
				if xi := h.mustInstance(x); xi.Status.Phase != v1alpha1.InstanceEnded || !h.kept(x) {
					t.Fatalf("X is %s, Allocation kept %v, want its Instance ended and its Allocation kept", xi.Status.Phase, h.kept(x))
				}
				if since, ok := h.idleSince(x); !ok || !since.Equal(h.mustInstance(x).Status.EndTime.Time) {
					t.Fatalf("X's Allocation idle-since %v (%v), want its Instance's end", since, ok)
				}
				for _, n := range []string{p1, p2} {
					if h.mustInstance(n).Status.Phase != v1alpha1.InstanceReady {
						t.Fatalf("%s is %s, want ready", n, h.mustInstance(n).Status.Phase)
					}
				}
			}},
	} {
		t.Run(c.name, func(t *testing.T) {
			h := newRkRig(t, false)
			p1, p2, x := rkScaleStart(h, c.x, pfake.Script{QueueWait: 20 * time.Minute}, nil)
			h.engineOf(p1, rk200, rkNoDelay)
			h.engineOf(p2, rk200, rkNoDelay)
			h.engineOf(x, rk200, max(c.xDelay, rkNoDelay))
			h.stepTo(rkAt(1, 0, 0), 10*time.Second, nil)
			for _, n := range []string{p1, p2} {
				if h.mustInstance(n).Status.Phase != v1alpha1.InstanceReady {
					t.Fatalf("%s is %s at 01:00, want ready", n, h.mustInstance(n).Status.Phase)
				}
			}
			if a, b := h.mustInstance(p1).Status.LeaseEnd, h.mustInstance(p2).Status.LeaseEnd; a == nil || b == nil || !a.Before(b) {
				t.Fatalf("hard ends %v and %v, want P1's the nearer", a, b)
			}
			h.mutateMD(hoMD, c.edit)
			pass := h.passOf(hoMD)
			var got []string
			for _, tg := range pass.scaleDownTargets(pass.primaries(), 0) {
				got = append(got, tg.instance.Name+":"+string(tg.effect)+":"+string(tg.cause))
			}
			if want := c.targets(p1, p2, x); strings.Join(got, ",") != strings.Join(want, ",") {
				t.Fatalf("targets %v, want %v", got, want)
			}
			h.run(c.settle, 10*time.Second)
			c.after(t, h, p1, p2, x)
		})
	}
}

// TestLoweringInstancesLeavesANonReadyPrimaryAsTheExtra: with overAllocation 1, the default, lowering instances from
// 2 to 1 while the second primary P2 pends leaves it alone, as the surplus k allows: it keeps its queue age, and is
// ranked when it starts. With overAllocation 0 it is cancelled, as before. (The visible change PR 3 makes to step 7:
// HBEP-7, Left alone.)
func TestLoweringInstancesLeavesANonReadyPrimaryAsTheExtra(t *testing.T) {
	for _, c := range []struct {
		name      string
		k         int32
		cancelled bool
	}{{"k 1", 1, false}, {"k 0", 0, true}} {
		t.Run(c.name, func(t *testing.T) {
			h := newRkRig(t, false)
			h.defaultDelay = rkNoDelay
			h.batch.Enqueue(pfake.Script{QueueWait: 10 * time.Minute}, pfake.Script{QueueWait: 3 * time.Hour})
			h.create(rkMD(func(md *v1alpha1.ModelDeployment) { md.Spec.Instances = 2; md.Spec.OverAllocation = ptrTo(c.k) }))
			h.run(30*time.Minute, 10*time.Second)
			var p2 string
			for _, in := range h.instances(hoMD) {
				if in.Status.Phase != v1alpha1.InstanceReady {
					p2 = in.Name
				}
			}
			if p2 == "" {
				t.Fatal("no pending second primary")
			}
			h.alloc(p2)
			h.mutateMD(hoMD, func(md *v1alpha1.ModelDeployment) { md.Spec.Instances = 1 })
			h.run(time.Minute, 10*time.Second)
			ph := h.phase(p2)
			if c.cancelled != ((ph == v1alpha1.InstanceEnded || ph == "gone") && h.released(h.alloc(p2))) {
				t.Fatalf("P2 is %s, its Allocation released %v: cancelled should be %v with overAllocation %d", ph, h.released(h.alloc(p2)), c.cancelled, c.k)
			}
		})
	}
}

// TestTheExcludedScaleDownCase: instances 2, no wait observed. P1's job starts at 00:10 (08:10:20) and its
// replacement R1 is created then; P1 is still starting at 00:30. P2 is ready since 00:21 and has R2. Lowering to 1 at
// 00:30 keeps P2 ready and chosen (a ready Instance ranks above a starting one whose replacement is live: HBEP-7's
// earlier order would drain P2 and leave nothing ready). P1's Instance ends with its Allocation kept, and R1 is
// cancelled; the targets are R1 (cancelPending) and P1 (endKeepAllocation), both with the trim cause, since at the
// lowered count both are non-ready surplus beyond k. A population with every primary ready cannot show this.
func TestTheExcludedScaleDownCase(t *testing.T) {
	h := newRkRig(t, true)
	h.batch.Enqueue(pfake.Script{QueueWait: 10 * time.Minute}, pfake.Script{QueueWait: 19*time.Minute + 40*time.Second},
		pfake.Script{QueueWait: 5 * time.Hour}, pfake.Script{QueueWait: 5 * time.Hour})
	h.create(rkMD(func(md *v1alpha1.ModelDeployment) { md.Spec.OverAllocation = ptrTo(int32(0)) }))
	h.tick(10 * time.Second)
	h.tick(10 * time.Second)
	h.mutateMD(hoMD, func(md *v1alpha1.ModelDeployment) { md.Spec.Instances = 2 })
	h.tick(10 * time.Second)
	h.tick(10 * time.Second) // 08:00:40: both primaries' jobs are submitted
	ins := h.instances(hoMD)
	p1, p2 := ins[0].Name, ins[1].Name
	h.engineOf(p1, rk200, 6*time.Hour) // P1's worker stays starting
	h.engineOf(p2, rk200, rkNoDelay)
	h.stepTo(rkAt(0, 30, 0), 10*time.Second, nil)
	r1, r2 := hoRepl(h.rig, p1), hoRepl(h.rig, p2)
	if len(r1) != 1 || len(r2) != 1 || !r1[0].CreationTimestamp.Time.Equal(rkAt(0, 10, 20)) {
		t.Fatalf("replacements of P1 %+v and P2 %+v, want one each, R1 created at 08:10:20", r1, r2)
	}
	if h.mustInstance(p2).Status.Phase != v1alpha1.InstanceReady || h.mustInstance(p1).Status.Phase == v1alpha1.InstanceReady {
		t.Fatalf("P1 is %s and P2 %s at 00:30, want P1 starting and P2 ready", h.mustInstance(p1).Status.Phase, h.mustInstance(p2).Status.Phase)
	}
	h.alloc(r1[0].Name)
	h.mutateMD(hoMD, func(md *v1alpha1.ModelDeployment) { md.Spec.Instances = 1 })
	pass := h.passOf(hoMD)
	var got []string
	for _, tg := range pass.scaleDownTargets(pass.primaries(), 0) {
		got = append(got, tg.instance.Name+":"+string(tg.effect)+":"+string(tg.cause))
	}
	want := []string{r1[0].Name + ":cancelPending:overAllocationTrim", p1 + ":endKeepAllocation:overAllocationTrim"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("targets %v, want %v", got, want)
	}
	h.run(time.Minute, 10*time.Second)
	if in := h.mustInstance(p2); in.Status.Phase != v1alpha1.InstanceReady || ended(h.mustInstance(r2[0].Name)) {
		t.Fatalf("P2 is %s: want it ready, chosen, with its replacement alive", in.Status.Phase)
	}
	if in := h.mustInstance(p1); in.Status.Phase != v1alpha1.InstanceEnded || !h.kept(p1) {
		t.Fatalf("P1 is %s, Allocation kept %v, want its Instance ended and its Allocation kept", in.Status.Phase, h.kept(p1))
	}
	if ph := h.phase(r1[0].Name); (ph != v1alpha1.InstanceEnded && ph != "gone") || !h.released(h.alloc(r1[0].Name)) {
		t.Fatalf("R1 is %s, want it cancelled", ph)
	}
}
