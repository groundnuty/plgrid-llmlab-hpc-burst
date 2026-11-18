package lifecycle

// Two races of the per-user caps that were in alpha too (review of #756, Astra 3 and Astra 4), closed here:
//   - a primary's reservation records its size like any other, so a hedge member or an extra that asks before the primary's
//     Allocation is listed counts it against the caps;
//   - a hedge member's retry reserves with the caps, as its first creation did, so another job that filled the QOS in the
//     meantime keeps the retry from creating the Allocation.
// Every count is worked by hand.

import (
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// TestAPrimarysReservationCountsAgainstALaterMembersCaps, alpha (Helios- or Athena-shaped: one QOS, no qosByGpus): the QOS caps
// a user at 4 GPUs. A primary of 3 GPUs reserves its slot, and its Allocation is not listed yet. A hedge member of 2 GPUs asks
// with the caps check: 3 + 2 = 5 GPUs, above 4, so it is refused; a member of 1 GPU fits, 3 + 1 = 4.
func TestAPrimarysReservationCountsAgainstALaterMembersCaps(t *testing.T) {
	caps := v1alpha1.QOSLimits{MaxGPUsPerUser: ptrTo(int32(4)), From: map[string]string{v1alpha1.LimitMaxGPUsPerUser: "now"}}
	r := allocsReader{}
	tab := newSlotTable()
	now := fixedAt(hoT(9, 0, 0))
	primary := batch(3, 48, "360Gi")
	if a, err := tab.reserveAsk(t.Context(), r, slotAsk{provider: provB, key: rigNS + "/primary", size: &primary}, now); err != nil || !a.ok {
		t.Fatalf("the primary's reservation: %+v, %v", a, err)
	}
	member := func(key string, gpus int32) slotAnswer {
		size := batch(gpus, gpus*16, "120Gi")
		size.Memory.Mul(int64(gpus))
		a, err := tab.reserveAsk(t.Context(), r, slotAsk{provider: provB, key: rigNS + "/" + key, params: &size, caps: caps}, now)
		if err != nil {
			t.Fatal(err)
		}
		return a
	}
	if a := member("member-2", 2); a.ok || !strings.Contains(a.capsWhy, "5 GPUs, above the maxGpusPerUser 4") {
		t.Fatalf("a 2-GPU member beside the primary's 3: ok %v, %q; want refused with 5 GPUs above 4", a.ok, a.capsWhy)
	}
	if a := member("member-1", 1); !a.ok {
		t.Fatalf("a 1-GPU member beside the primary's 3 (4 in all) was refused: %q", a.capsWhy)
	}
}

// TestAPrimarysReservationCountsAgainstASanoMembersCaps is the same race on Sano's partition: gpu1 allows 1 GPU per user.
func TestAPrimarysReservationCountsAgainstASanoMembersCaps(t *testing.T) {
	q := sanoQOS()
	q[0].MaxGPUsPerUser = i32(1)
	pv := sanoView(q, false)
	r := allocsReader{}
	tab := newSlotTable()
	if a := askOn(t, tab, r, pv, "primary", 1, false); !a.ok {
		t.Fatalf("the primary's reservation was refused: %+v", a)
	}
	if a := askOn(t, tab, r, pv, "member", 1, true); a.ok || !strings.Contains(a.capsWhy, "2 GPUs, above the maxGpusPerUser 1 of QOS gpu1") {
		t.Fatalf("a 1-GPU member beside the primary's 1 GPU: ok %v, %q; want refused: 2 GPUs under QOS gpu1 above its 1", a.ok, a.capsWhy)
	}
}

// TestEveryReservationRecordsItsSize: a primary placed through the pass leaves its size (1 GPU, 16 CPUs, 120Gi, the rig
// partition's per-GPU shares) among the Provider's expectations until its Allocation is listed, and so does a primary whose
// Allocation create was cut short and is retried.
func TestEveryReservationRecordsItsSize(t *testing.T) {
	size := func(rg *rig) []v1alpha1.BatchParameters {
		var out []v1alpha1.BatchParameters
		for _, b := range rg.r.slots().params[rigProvider] {
			out = append(out, b)
		}
		return out
	}
	want := func(t *testing.T, got []v1alpha1.BatchParameters) {
		t.Helper()
		if len(got) != 1 || got[0].GPUs != 1 || got[0].CPUsPerTask != 16 || got[0].Memory.Cmp(resource.MustParse("120Gi")) != 0 {
			t.Fatalf("the recorded sizes are %+v, want the one primary's 1 GPU, 16 CPUs, 120Gi", got)
		}
	}
	t.Run("a primary placed through the pass", func(t *testing.T) {
		rg := newRig(t)
		rg.create(mdSpec("md"))
		rg.clock.Advance(10 * time.Second)
		rg.Step() // the primary is placed: its reservation is the last the pass makes, so its expectation is still held
		if len(rg.allocations("md")) != 1 {
			t.Fatalf("%d Allocations after the pass, want the primary's", len(rg.allocations("md")))
		}
		want(t, size(rg))
	})
	t.Run("a cut-short primary's retry", func(t *testing.T) {
		cut := &allocCutter{}
		rg := newRig(t, withInterceptors(cut.funcs(interceptor.Funcs{})))
		cut.arm("md", 0)
		rg.create(mdSpec("md"))
		rg.clock.Advance(10 * time.Second)
		stepTolerantAll(rg)
		ins := rg.instances("md")
		if len(ins) != 1 || len(rg.allocations("md")) != 0 {
			t.Fatalf("%d Instances and %d Allocations after the cut create, want 1 and 0", len(ins), len(rg.allocations("md")))
		}
		cut.disarm()
		rg.r.slots().release(rigProvider, rigNS+"/"+ins[0].Spec.AllocationRef.Name) // the cut create gave its slot back; start clean
		p := rg.passOf("md")
		if err := p.ensureAllocation(&ins[0]); err != nil {
			t.Fatal(err)
		}
		want(t, size(rg))
	})
}

// hedgeRetryRig is a hedge rig whose Provider B is under a cap of 1 GPU per user: alpha-shaped (one QOS), or Sano-shaped
// (qosByGpus, gpu1 capping 1 GPU). The deployment's primary is on A.
func hedgeRetryRig(t *testing.T, sano bool, cut *allocCutter) *hedgeRig {
	t.Helper()
	opts := []rigOption{withMaxTime(4 * time.Hour), withInterceptors(cut.funcs(interceptor.Funcs{}))}
	var fix func(*v1alpha1.PartitionProfile)
	if sano {
		opts = append(opts, withProfile(func(s *v1alpha1.PlatformProfileSpec) {
			q := sanoQOS()
			q[0].MaxGPUsPerUser = i32(1)
			s.QOS = q
		}))
		fix = func(pp *v1alpha1.PartitionProfile) { pp.AcceleratorClass.GPUModel = hedgeBGPU; sanoPartition(pp) }
	} else {
		opts = append(opts, withProfile(func(s *v1alpha1.PlatformProfileSpec) {
			s.QOS = append(s.QOS, v1alpha1.QOSProfile{Name: "example-b1", MaxSubmitPerUser: ptrTo(int32(4)), MaxGPUsPerUser: ptrTo(int32(1))})
		}))
		fix = func(pp *v1alpha1.PartitionProfile) { pp.AcceleratorClass.GPUModel = hedgeBGPU; pp.QOS = "example-b1" }
	}
	rg := newRig(t, opts...)
	b := rg.addProvider(hedgeB, hedgeBSlurm, 4*time.Hour, fix, func(c *pfake.Config) { c.Partitions[0].GPUModel = hedgeBGPU })
	return &hedgeRig{rig: rg, b: b, slow: map[string]bool{}}
}

// TestAHedgeMembersRetryReservesWithTheCaps: the deployment hedges onto B, whose QOS caps a user at 1 GPU. The member's first
// creation passes the caps (B is empty) and its Allocation's create is cut short. Then another deployment's primary takes B's
// one GPU. The member's retry asks the caps again: 1 + 1 = 2 GPUs above 1, so it makes no Allocation. Both shapes.
func TestAHedgeMembersRetryReservesWithTheCaps(t *testing.T) {
	for _, sano := range []bool{false, true} {
		name := "alpha, one QOS"
		if sano {
			name = "Sano, qosByGpus"
		}
		t.Run(name, func(t *testing.T) {
			cut := &allocCutter{}
			h := hedgeRetryRig(t, sano, cut)
			md := mdSpec(hoMD)
			seededOnly(md)
			h.create(md)
			h.clock.Advance(10 * time.Second)
			h.Step()
			primary := h.onProvider(hoMD, rigProvider)
			if len(primary) != 1 {
				t.Fatalf("%d Instances of %s on A, want the primary", len(primary), hoMD)
			}
			// The member's first creation, with the caps check a hedge's growth makes; its Allocation's create is cut short.
			cut.arm(hoMD, 0)
			p := h.passOf(hoMD)
			if _, err := p.provisionWith(nil, placeOpts{only: hedgeB, hedgeGroup: primary[0].Name, caps: true, quiet: true}); err == nil {
				t.Fatal("the member's Allocation create was not cut short")
			}
			cut.disarm()
			members := h.onProvider(hoMD, hedgeB)
			if len(members) != 1 || len(h.allocationsOnB(hoMD)) != 0 {
				t.Fatalf("%d members on B and %d Allocations there, want one member and none", len(members), len(h.allocationsOnB(hoMD)))
			}
			// Another deployment's primary now takes B's one GPU.
			other := mdSpec("md-x")
			other.Spec.Providers.Names = []string{hedgeB}
			h.create(other)
			h.clock.Advance(10 * time.Second)
			stepTolerantAll(h.rig, hoMD)
			if n := len(h.allocations("md-x")); n != 1 {
				t.Fatalf("md-x has %d Allocations, want its primary on B", n)
			}
			if err := h.passOf(hoMD).ensureAllocation(&members[0]); err != nil {
				t.Fatal(err)
			}
			if got := h.allocationsOnB(hoMD); len(got) != 0 {
				t.Fatalf("the member's retry made %d Allocation(s) on B: 2 GPUs under a cap of 1", len(got))
			}
		})
	}
}

// allocationsOnB returns the deployment's Allocations on Provider B.
func (h *hedgeRig) allocationsOnB(md string) []v1alpha1.Allocation {
	var out []v1alpha1.Allocation
	for _, a := range h.allocations(md) {
		if a.Labels[v1alpha1.LabelProvider] == hedgeB {
			out = append(out, a)
		}
	}
	return out
}
