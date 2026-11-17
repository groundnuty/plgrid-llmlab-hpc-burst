package lifecycle

// The over-allocation extra (beta; HBEP-7, Over-allocation and qualification (beta), k, and when the extra is
// submitted; Its checks; In host-dialed mode; #616, PR 4). The fixture is handover_test.go's unless a row says: a 12 h
// or 4 h job limit, drain window 2 h 10 min, handoverMargin 30 min. Every count is worked by hand in the row, never
// read from slotAsk, reservedOn, selectPrimaries or warmOf.
//
// The pass at 08:00:10 makes a deployment's missing primaries and the extra together (the rig creates at 08:00:00 and
// its first pass is the first step), and the provider submits their jobs at 08:00:20.

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// withK is the deployment with overAllocation k (nil: unset, which is 1).
func withK(md *v1alpha1.ModelDeployment, k *int32) *v1alpha1.ModelDeployment {
	md.Spec.OverAllocation = k
	return md
}

// TestAMissingPrimaryBringsItsExtra: one extra with the missing primary, in the pass that makes it, on its Provider; k
// is 1 when overAllocation is unset, and with k = 0 there is none. The extra carries no mark: no predecessor, no
// hedge group, no no-overlap mark; the selection decides what it is.
func TestAMissingPrimaryBringsItsExtra(t *testing.T) {
	for _, c := range []struct {
		name string
		k    *int32
		want int
	}{
		{"overAllocation unset: k is 1", nil, 2},
		{"overAllocation 1", ptrTo(int32(1)), 2},
		{"overAllocation 0: no extra", ptrTo(int32(0)), 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			rg := newRig(t)
			rg.create(withK(mdSpec(hoMD), c.k))
			rg.clock.Advance(10 * time.Second)
			rg.Step() // 08:00:10
			ins := rg.instances(hoMD)
			if len(ins) != c.want || len(rg.allocations(hoMD)) != c.want {
				t.Fatalf("%d Instances and %d Allocations at 08:00:10, want %d of each", len(ins), len(rg.allocations(hoMD)), c.want)
			}
			for _, in := range ins {
				if len(in.Annotations) != 0 || in.Labels[v1alpha1.LabelProvider] != rigProvider {
					t.Fatalf("Instance %s has annotations %v on Provider %q: neither the primary nor the extra carries a mark, and both are on %s", in.Name, in.Annotations, in.Labels[v1alpha1.LabelProvider], rigProvider)
				}
			}
			rg.clock.Advance(10 * time.Second)
			rg.Step() // 08:00:20: the jobs are submitted
			if n := rg.submissions(); n != c.want {
				t.Fatalf("%d jobs submitted by 08:00:20, want %d", n, c.want)
			}
		})
	}
}

// TestTheExtraAndTheSlotBeyondItsOwnHandovers: the extra's slot and caps checks, each row a Provider and a deployment. The
// effective submit limit counts the listed Allocations, those reserved and not listed yet, and the slots reserved for
// handovers; the extra needs listed + expected + reserved + alsoLeave + 1 within the limit, where alsoLeave is one for
// each chosen primary there that holds no reservation yet but will once Running: the deployment has no end time, or one
// after its expected drain start, creation + the time limit - the drain window. Every deployment is fully served at
// 08:01:10 (its primaries ready), so a refusal that sets a Degraded reason would show.
func TestTheExtraAndTheSlotBeyondItsOwnHandovers(t *testing.T) {
	// 08:00:10 + 12 h - 2 h 10 min = 17:50:10 is the primary's expected drain start in the 12 h rows.
	endBefore := metav1.NewTime(hoT(14, 0, 0))
	for _, c := range []struct {
		name      string
		opts      []rigOption
		instances int32
		end       *metav1.Time
		want      int // Allocations, the extra's included
	}{
		{"host-dialed (2), no end time: 1 + 1 + 1 = 3 > 2, no extra", []rigOption{withHostDialed()}, 1, nil, 1},
		{"host-dialed (2), end time 14:00, before the drain start 17:50:10: the handover reserves no slot, 1 + 0 + 1 = 2, the extra", []rigOption{withHostDialed()}, 1, &endBefore, 2},
		{"limit 5, instances 2: 2 + 2 + 1 = 5, the extra", qosOpts([]v1alpha1.QOSProfile{{Name: "example-s5", MaxSubmitPerUser: i32(5)}}, "", "example-s5"), 2, nil, 3},
		{"limit 4, instances 2: 2 + 2 + 1 = 5 > 4, no extra", qosOpts([]v1alpha1.QOSProfile{{Name: "example-s4", MaxSubmitPerUser: i32(4)}}, "", "example-s4"), 2, nil, 2},
		{"a QOS of one GPU per user: 1 + 1 = 2 GPUs > 1, no extra", qosOpts([]v1alpha1.QOSProfile{{Name: "example-g1", MaxGPUsPerUser: i32(1)}}, "", "example-g1"), 1, nil, 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			rg := newRig(t, append(c.opts, withMaxTime(12*time.Hour))...)
			md := withK(mdSpec(hoMD), ptrTo(int32(1)))
			md.Spec.Instances = c.instances
			md.Spec.EndTime = c.end
			rg.create(md)
			for i := range 7 {
				rg.clock.Advance(10 * time.Second)
				rg.Step()
				if i == 0 {
					// The pass at 08:00:10 is the only one that tries an extra, so a refusal that set a Degraded reason
					// would show in this status and be overwritten by the next pass: it is read here.
					if d := rg.condition(hoMD, v1alpha1.ConditionDegraded); d.Status != metav1.ConditionFalse || d.Reason != v1alpha1.ReasonAsDesired {
						t.Fatalf("Degraded = %s/%s %q right after the pass that makes the primaries and tries the extra, want False/AsDesired: a refused extra is silent", d.Status, d.Reason, d.Message)
					}
				}
			}
			rg.run(10*time.Minute, 10*time.Second)
			if n := len(rg.allAllocations()); n != c.want {
				t.Fatalf("%d Allocations after 10 minutes of passes, want %d", n, c.want)
			}
			if n := rg.readyCount(hoMD); n != int(c.instances) {
				t.Fatalf("%d ready, want the %d primaries: a refusal that shows no Degraded reason on a deployment that is not served proves nothing", n, c.instances)
			}
			d := rg.condition(hoMD, v1alpha1.ConditionDegraded)
			if d.Status != metav1.ConditionFalse || d.Reason != v1alpha1.ReasonAsDesired || strings.Contains(d.Message, v1alpha1.ReasonSubmitLimitReached) {
				t.Fatalf("Degraded = %s/%s %q on a deployment that is fully served, want False/AsDesired and no SubmitLimitReached", d.Status, d.Reason, d.Message)
			}
		})
	}
}

// TestNoExtraWhereThePassMadeNoNewAllocationForAPrimary: the extra goes only with a missing primary's NEW Allocation,
// on its Provider, and only while the bound allows it.
func TestNoExtraWhereThePassMadeNoNewAllocationForAPrimary(t *testing.T) {
	t.Run("a missing primary placed on a warm Allocation brings no extra", func(t *testing.T) {
		// k 0 to 08:40: the primary is ready at 08:01:10 (hard end 12:00:20) and the deployment is scaled to zero at
		// 08:30, which keeps its Allocation warm (Keep). At 08:40 instances 1 and k 1: the missing primary goes on the warm
		// Allocation, which has 3 h 20 min of lease left, above 2 * 50 s + 2 h 10 min = 2 h 11 min 40 s. No Allocation is
		// made, so no extra: one Allocation, one job.
		rg := newRig(t, withMaxTime(4*time.Hour))
		hoStart(rg, nil)
		hoStepTo(rg, hoT(8, 30, 0), time.Minute, nil)
		rg.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 0 })
		rg.until(5*time.Minute, 10*time.Second, "no live Instance", func() bool { return len(rg.liveInstances(hoMD)) == 0 })
		warm := rg.allocations(hoMD)[0].Name
		hoStepTo(rg, hoT(8, 40, 0), time.Minute, nil)
		rg.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 1; m.Spec.OverAllocation = ptrTo(int32(1)) })
		rg.run(5*time.Minute, 10*time.Second)
		live := rg.liveInstances(hoMD)
		if len(live) != 1 || live[0].Spec.AllocationRef.Name != warm || len(rg.allAllocations()) != 1 || rg.submissions() != 1 {
			t.Fatalf("live %d on %v, %d Allocations, %d jobs, want one Instance on the warm Allocation %s, one Allocation and one job", len(live), live, len(rg.allAllocations()), rg.submissions(), warm)
		}
	})
	t.Run("a replacement brings no extra", func(t *testing.T) {
		// k 1 from 08:30. At 09:19:30 the primary's replacement is made (hard end 12:00:20, drain start 09:50:20): two
		// Allocations, two jobs, and none for an extra after the replacement's pass.
		rg := newRig(t, withMaxTime(4*time.Hour))
		hoStart(rg, nil)
		hoStepTo(rg, hoT(8, 30, 0), time.Minute, nil)
		rg.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.OverAllocation = ptrTo(int32(1)) })
		hoStepTo(rg, hoT(9, 19, 20), time.Minute, nil)
		hoStepTo(rg, hoT(9, 25, 0), 10*time.Second, nil)
		if n, a, s := len(rg.instances(hoMD)), len(rg.allAllocations()), rg.submissions(); n != 2 || a != 2 || s != 2 {
			t.Fatalf("%d Instances, %d Allocations and %d jobs at 09:25:00, want the primary and its replacement only: 2 of each", n, a, s)
		}
	})
	t.Run("a hedge member brings no extra", func(t *testing.T) {
		// A and B, k 1. The pass at 08:00:00 makes the missing primary and the extra on A, both pending 10 h. No wait
		// is observed on A, so the hedge time is the deadline, 08:00:00 + the 30 min margin, 08:30:00: the hedge of the
		// chosen one gets its member on B then. B holds that member alone: three Instances in all, no extra on B.
		h := newHedgeRig(t)
		h.batch.Enqueue(pfake.Script{QueueWait: 10 * time.Hour}, pfake.Script{QueueWait: 10 * time.Hour})
		h.Step()
		md := withK(mdSpec(hoMD), ptrTo(int32(1)))
		onBoth(md)
		h.create(md)
		h.Step() // 08:00:00
		stepUntilClock(h.rig, hoT(8, 30, 10), 10*time.Second, nil)
		if a, b := len(h.onProvider(hoMD, rigProvider)), len(h.onProvider(hoMD, hedgeB)); a != 2 || b != 1 {
			t.Fatalf("A holds %d and B %d Instances at 08:30:10, want the primary and the extra on A and the hedge member alone on B", a, b)
		}
	})
	t.Run("a warm Allocation the primary cannot use keeps the extra out: the bound reads warmOf", func(t *testing.T) {
		// k 0, then k 1. The primary is ready at 08:01:10 (hard end 12:00:20) and the deployment is scaled to zero at
		// 08:30, keeping its Allocation warm. At 10:00 instances 1 and k 1: the warm Allocation has 2 h 0 min 20 s of lease
		// left, below the 2 h 11 min 40 s a new Instance needs, so the primary cannot go on it and a new Allocation is
		// made. A warm Allocation of the deployment is on the Provider, so the extra's bound refuses it: two Allocations,
		// the warm one and the primary's, not three.
		rg := newRig(t, withMaxTime(4*time.Hour))
		hoStart(rg, nil)
		hoStepTo(rg, hoT(8, 30, 0), time.Minute, nil)
		rg.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 0 })
		rg.until(5*time.Minute, 10*time.Second, "no live Instance", func() bool { return len(rg.liveInstances(hoMD)) == 0 })
		hoStepTo(rg, hoT(10, 0, 0), time.Minute, nil)
		rg.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 1; m.Spec.OverAllocation = ptrTo(int32(1)) })
		rg.run(5*time.Minute, 10*time.Second)
		if a, l := len(rg.allAllocations()), len(rg.liveInstances(hoMD)); a != 2 || l != 1 || rg.submissions() != 2 {
			t.Fatalf("%d Allocations, %d live Instances and %d jobs, want 2, 1 and 2: the warm one and the primary's, no extra", a, l, rg.submissions())
		}
	})
}

// TestTheExtrasBoundIsPerProvider: the bound's two halves, read through extraBound on hand-built populations (a missing
// primary co-occurs with a surplus only when a ranked-out peer waits for its drain: the selection skips a marked peer, so
// the deployment lacks a primary while the peer is surplus; the pass would then create the missing primary and ask the
// bound for an extra). instances 2, prov-a the Provider asked:
//   - R ready on prov-a, ranked out by C, ready on prov-b: C is chosen, the peer R is surplus on prov-a: no extra there,
//     and the missing second primary leaves prov-b with no surplus: an extra is allowed there;
//   - with no marked peer there is no surplus at all: both Providers allow it;
//   - a warm Allocation of the deployment on prov-a (Running, no Instance) bounds prov-a alone: no extra there, one
//     allowed on prov-b.
func TestTheExtrasBoundIsPerProvider(t *testing.T) {
	bound := func(f *selFx, desired int32, prov string) bool {
		return f.pass(desired, 1).extraBound(&providerView{name: prov})
	}
	t.Run("a surplus ranked-out peer bounds its own Provider", func(t *testing.T) {
		f := newSelFx()
		f.add("R", v1alpha1.InstanceReady, hardEnd(hoT(12, 10, 0)), rankedOutBy("C"))
		f.add("C", v1alpha1.InstanceReady, hardEnd(hoT(12, 20, 0)), onProv("prov-b"))
		if bound(f, 2, "prov-a") || !bound(f, 2, "prov-b") {
			t.Fatalf("bound on prov-a %v and on prov-b %v, want false and true: the surplus peer is on prov-a", bound(f, 2, "prov-a"), bound(f, 2, "prov-b"))
		}
	})
	t.Run("no surplus: both Providers allow an extra", func(t *testing.T) {
		f := newSelFx()
		f.add("R", v1alpha1.InstanceReady, hardEnd(hoT(12, 10, 0)))
		if !bound(f, 2, "prov-a") || !bound(f, 2, "prov-b") {
			t.Fatal("the bound refused with no surplus and no warm Allocation")
		}
	})
	t.Run("a warm Allocation bounds its own Provider", func(t *testing.T) {
		f := newSelFx()
		f.add("R", v1alpha1.InstanceReady, hardEnd(hoT(12, 10, 0)))
		f.warmAlloc("warm", "prov-a")
		if bound(f, 2, "prov-a") || !bound(f, 2, "prov-b") {
			t.Fatalf("bound on prov-a %v and on prov-b %v, want false and true: the warm Allocation is on prov-a", bound(f, 2, "prov-a"), bound(f, 2, "prov-b"))
		}
	})
}

// TestTheSameSecondTieIsStable: the primary and PR 4's extra are made in one pass, so in one second, both pending on one
// Provider. The selection orders equals by the earlier job start, then the older creation time, then the UID (select.go),
// so the chosen one is the one with the LOWER UID, and stays so pass after pass; when the HIGHER-UID one's job starts first
// it is the more advanced and becomes the chosen one. The expected side is read off the two Instances' UIDs, never from
// the selection. The roles of the scripts are the Allocations' names (the provider submits in that order), so the script
// for the higher-UID Instance is placed by its Allocation's place in that order. The chosen Instance is read through the
// pass's own selection (primaries), the one step 8 would replace.
func TestTheSameSecondTieIsStable(t *testing.T) {
	chosenUID := func(h *rkRig) string {
		h.t.Helper()
		ch := h.passOf(hoMD).primaries()
		if len(ch) != 1 {
			h.t.Fatalf("%d chosen primaries, want 1", len(ch))
		}
		return string(ch[0].UID)
	}
	setup := func(t *testing.T, hiWait time.Duration) (h *rkRig, lo, hi string) {
		t.Helper()
		h = newRkRig(t, false)
		h.create(rkMD(nil))
		h.tick(10 * time.Second) // 08:00:10: the primary and the extra are made, in one second
		ins := h.instances(hoMD)
		if len(ins) != 2 || !ins[0].CreationTimestamp.Equal(&ins[1].CreationTimestamp) {
			t.Fatalf("%d Instances made in the pass at 08:00:10 (created %v), want two in one second", len(ins), ins[0].CreationTimestamp)
		}
		loI, hiI := ins[0], ins[1]
		if loI.UID > hiI.UID {
			loI, hiI = hiI, loI
		}
		// the provider submits the Allocations in the order of their names, so the first by name takes the first script
		long := pfake.Script{QueueWait: 3 * time.Hour}
		hiScript := pfake.Script{QueueWait: hiWait}
		if hiI.Spec.AllocationRef.Name < loI.Spec.AllocationRef.Name {
			h.batch.Enqueue(hiScript, long)
		} else {
			h.batch.Enqueue(long, hiScript)
		}
		h.tick(10 * time.Second) // 08:00:20: both jobs are submitted
		return h, string(loI.UID), string(hiI.UID)
	}
	t.Run("both pend: the lower UID is chosen at every pass", func(t *testing.T) {
		h, lo, _ := setup(t, 3*time.Hour)
		for i := range 10 {
			h.tick(10 * time.Second)
			if got := chosenUID(h); got != lo {
				t.Fatalf("pass %d at %s: the chosen primary has UID %s, want the lower one, %s", i+1, h.clock.Now().Format(time.TimeOnly), got, lo)
			}
		}
	})
	t.Run("the higher UID's job starts first: it becomes the chosen one", func(t *testing.T) {
		// The higher-UID Instance waits 5 min from its submission at 08:00:20, so its job starts at 08:05:20; the other
		// pends 3 h. Before 08:05:20 the lower UID is chosen; once the job has started, the higher.
		h, lo, hi := setup(t, 5*time.Minute)
		h.stepTo(rkAt(0, 5, 10), 10*time.Second, func() {
			if got := chosenUID(h); got != lo {
				t.Fatalf("at %s the chosen primary has UID %s, want the lower one, %s, while both pend", h.clock.Now().Format(time.TimeOnly), got, lo)
			}
		})
		h.stepTo(rkAt(0, 6, 0), 10*time.Second, nil)
		if got := chosenUID(h); got != hi {
			t.Fatalf("at 00:06 the chosen primary has UID %s, want the higher one, %s: its job started first", got, hi)
		}
	})
}
