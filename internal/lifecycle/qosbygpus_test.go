package lifecycle

// A partition's QOS by GPU count (HBEP-4, The Slurm dialect and QOS limits; HBEP-7, Provisioning and Per-user limits;
// #745, #735): the plan resolves the job's QOS from the GPU count BEFORE it sizes the Allocation, and each QOS's per-user
// limits count only the Allocations under that QOS. Every expected value is worked out by hand in the row's comment from
// Sano's probes (11 CPUs and 135,904 MiB per GPU, a 3-day wall, the caps of probe 3), never read back through
// QOSForGPUs, EffectiveQOSLimits or the sizing code.

import (
	"context"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// sanoQOS is Sano's four QOS classes with their per-job caps from #735's probe 3 and a 3-day wall.
func sanoQOS() []v1alpha1.QOSProfile {
	return []v1alpha1.QOSProfile{
		{Name: "gpu1", MaxWall: dur(72 * time.Hour), MaxGPUsPerJob: i32(1), MaxCPUsPerJob: i32(12), MaxMemPerJob: memQ("143360Mi")},
		{Name: "gpu2", MaxWall: dur(72 * time.Hour), MaxGPUsPerJob: i32(2), MaxCPUsPerJob: i32(24), MaxMemPerJob: memQ("286720Mi")},
		{Name: "gpu4", MaxWall: dur(72 * time.Hour), MaxGPUsPerJob: i32(4), MaxCPUsPerJob: i32(48), MaxMemPerJob: memQ("573440Mi")},
		{Name: "gpu8", MaxWall: dur(72 * time.Hour), MaxGPUsPerJob: i32(8), MaxCPUsPerJob: i32(92), MaxMemPerJob: memQ("1087232Mi")},
	}
}

// sanoPartition makes the rig's partition Sano's gpu partition: 8 GPUs a node, 11 CPUs and 135,904 MiB a GPU, 7 days.
func sanoPartition(pp *v1alpha1.PartitionProfile) {
	pp.GPUsPerNode, pp.CPUsPerGPU, pp.MemPerGPU = i32(8), i32(11), memQ("135904Mi")
	pp.MaxTime = dur(168 * time.Hour)
	pp.QOSByGPUs = []v1alpha1.QOSByGPUs{{GPUs: 1, QOS: "gpu1"}, {GPUs: 2, QOS: "gpu2"}, {GPUs: 4, QOS: "gpu4"}, {GPUs: 8, QOS: "gpu8"}}
}

// sanoRigOpts is a rig on Sano's partition with the QOS list edited by edit (nil: as is), and no Provider qos.
func sanoRigOpts(edit func([]v1alpha1.QOSProfile)) []rigOption {
	q := sanoQOS()
	if edit != nil {
		edit(q)
	}
	return []rigOption{
		withProfile(func(s *v1alpha1.PlatformProfileSpec) { s.QOS = q }),
		withPartition(sanoPartition), withMaxTime(168 * time.Hour),
	}
}

func tpFlag(n string) v1alpha1.EngineFlag {
	return v1alpha1.EngineFlag{Name: "tensor-parallel-size", Value: n}
}

// planOf runs one pass for a deployment of the given tensor-parallel size and returns its one Allocation's parameters.
func planOf(t *testing.T, rg *rig, tpSize string) v1alpha1.BatchParameters {
	t.Helper()
	rg.create(withFlags(mdSpec("md"), tpFlag(tpSize)))
	rg.clock.Advance(10 * time.Second)
	rg.Step()
	as := rg.allocations("md")
	if len(as) != 1 {
		c := rg.condition("md", v1alpha1.ConditionDegraded)
		t.Fatalf("%d Allocations, want 1 (Degraded %s: %s)", len(as), c.Reason, c.Message)
	}
	return *as[0].Spec.ForProvider.Batch
}

// TestSanoPlanSizesAnAllocationUnderTheQOSItsGPUCountChooses: the size follows the GPUs, and the time limit is the
// chosen QOS's 3-day wall (72 h), never the partition's 7 days (168 h): each QOS has maxWall 72h, so min(168 h, 72 h) = 72 h.
//   - 1 GPU: gpu1; 11 CPUs and 135,904 MiB, under gpu1's 12 and 140 GiB (143,360 MiB);
//   - 3 GPUs: gpu4; 33 CPUs and 3 x 135,904 = 407,712 MiB, under gpu4's 48 and 560 GiB;
//   - 8 GPUs: gpu8; 88 CPUs and 8 x 135,904 = 1,087,232 MiB, exactly gpu8's cap, under its 92 CPUs.
func TestSanoPlanSizesAnAllocationUnderTheQOSItsGPUCountChooses(t *testing.T) {
	for _, tc := range []struct {
		tp    string
		cpus  int32
		memMi int64
	}{{"1", 11, 135904}, {"3", 33, 407712}, {"8", 88, 1087232}} {
		t.Run("tensor-parallel-size "+tc.tp, func(t *testing.T) {
			got := planOf(t, newRig(t, sanoRigOpts(nil)...), tc.tp)
			if got.CPUsPerTask != tc.cpus || got.Memory.Cmp(*resource.NewQuantity(tc.memMi<<20, resource.BinarySI)) != 0 {
				t.Errorf("size = %d CPUs and %s, want %d CPUs and %d MiB", got.CPUsPerTask, got.Memory.String(), tc.cpus, tc.memMi)
			}
			if got.TimeLimit != "72h0m0s" {
				t.Errorf("timeLimit = %q, want 72h0m0s: the chosen QOS's 3-day wall bounds it, not the partition's 7 days", got.TimeLimit)
			}
		})
	}
}

// TestSanoPlanRefusesWhatNoQOSOrCapAdmits: a count above the list's last entry has no QOS, and a count above the chosen
// QOS's maxGpusPerJob is above a cap; each is refused, never capped, with SpecUnsupported and no Allocation.
//   - 9 GPUs: above gpu8, the last entry: refused naming the partition's last entry;
//   - a copy whose gpu4 allows 2 GPUs per job: 3 GPUs, which gpu4 would take by count, is refused naming maxGpusPerJob.
func TestSanoPlanRefusesWhatNoQOSOrCapAdmits(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func([]v1alpha1.QOSProfile)
		tp   string
		want []string
	}{
		{"9 GPUs, above the last entry", nil, "9", []string{"needs 9 GPUs", "above the 8 of its last qosByGpus entry"}},
		{"3 GPUs under gpu4 with maxGpusPerJob 2", func(q []v1alpha1.QOSProfile) { q[2].MaxGPUsPerJob = i32(2) }, "3", []string{"needs 3 GPUs", "maxGpusPerJob 2 of QOS gpu4"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rg := newRig(t, sanoRigOpts(tc.edit)...)
			rg.create(withFlags(mdSpec("md"), tpFlag(tc.tp)))
			rg.clock.Advance(10 * time.Second)
			rg.Step()
			if as := rg.allocations("md"); len(as) != 0 {
				t.Fatalf("%d Allocations, want none: it is refused, never capped (first: %+v)", len(as), as[0].Spec.ForProvider.Batch)
			}
			c := rg.condition("md", v1alpha1.ConditionDegraded)
			if c.Status != metav1.ConditionTrue || c.Reason != v1alpha1.ReasonSpecUnsupported {
				t.Fatalf("Degraded = %s/%s %q, want True/SpecUnsupported", c.Status, c.Reason, c.Message)
			}
			for _, w := range tc.want {
				if !strings.Contains(c.Message, w) {
					t.Errorf("the message %q does not say %q", c.Message, w)
				}
			}
		})
	}
}

// TestSanoPlanCapsCPUsAndMemoryAtTheJobQOSsCaps: with gpu1's caps below one GPU's share (11 CPUs, 135,904 MiB), a 1-GPU
// Allocation is created with the cap, and keeps its 1 GPU.
//   - maxCpusPerJob 8: 8 CPUs, not 11;
//   - maxMemPerJob 100Gi: 102,400 MiB, not 135,904;
//   - maxCpusPerUser 6 beside maxCpusPerJob 8: the smaller, 6;
//   - maxMemPerUser 90Gi beside maxMemPerJob 100Gi: the smaller, 92,160 MiB.
func TestSanoPlanCapsCPUsAndMemoryAtTheJobQOSsCaps(t *testing.T) {
	for _, tc := range []struct {
		name  string
		edit  func(*v1alpha1.QOSProfile)
		cpus  int32
		memMi int64
	}{
		{"maxCpusPerJob 8", func(q *v1alpha1.QOSProfile) { q.MaxCPUsPerJob = i32(8) }, 8, 135904},
		{"maxMemPerJob 100Gi", func(q *v1alpha1.QOSProfile) { q.MaxMemPerJob = memQ("100Gi") }, 11, 102400},
		{"maxCpusPerUser 6 beside maxCpusPerJob 8", func(q *v1alpha1.QOSProfile) { q.MaxCPUsPerJob, q.MaxCPUsPerUser = i32(8), i32(6) }, 6, 135904},
		{"maxMemPerUser 90Gi beside maxMemPerJob 100Gi", func(q *v1alpha1.QOSProfile) { q.MaxMemPerJob, q.MaxMemPerUser = memQ("100Gi"), memQ("90Gi") }, 11, 92160},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := planOf(t, newRig(t, sanoRigOpts(func(q []v1alpha1.QOSProfile) { tc.edit(&q[0]) })...), "1")
			if got.GPUs != 1 || got.CPUsPerTask != tc.cpus || got.Memory.Cmp(*resource.NewQuantity(tc.memMi<<20, resource.BinarySI)) != 0 {
				t.Errorf("size = %d GPUs, %d CPUs, %s; want 1, %d, %d MiB: capped, never refused", got.GPUs, got.CPUsPerTask, got.Memory.String(), tc.cpus, tc.memMi)
			}
		})
	}
}

// TestAPartitionQOSsPerJobCapsCountInSlurmsOrder (HBEP-4, Which limit applies; #745): the rig's partition has 16 CPUs per GPU,
// and its own QOS part-qos caps a job at 8 CPUs.
//   - the Provider sets no qos: only the partition QOS's typed limits apply, so 1 GPU gets 8 CPUs, not 16;
//   - the Provider's qos fast sets maxCpusPerJob 12 and no OverPartQOS: the partition QOS's 8 comes first, 8 CPUs;
//   - fast with OverPartQOS: its own 12 comes first, 12 CPUs.
func TestAPartitionQOSsPerJobCapsCountInSlurmsOrder(t *testing.T) {
	for _, tc := range []struct {
		name     string
		provider string
		over     bool
		cpus     int32
	}{
		{"no Provider qos: the partition QOS's cap", "", false, 8},
		{"the job QOS after the partition QOS", "fast", false, 8},
		{"overPartQos reverses the order", "fast", true, 12},
	} {
		t.Run(tc.name, func(t *testing.T) {
			qos := []v1alpha1.QOSProfile{{Name: "part-qos", MaxCPUsPerJob: i32(8)}, {Name: "fast", MaxCPUsPerJob: i32(12), OverPartQOS: tc.over}}
			got := planOf(t, newRig(t, qosOpts(qos, "part-qos", tc.provider)...), "1")
			if got.CPUsPerTask != tc.cpus {
				t.Errorf("CPUs = %d, want %d", got.CPUsPerTask, tc.cpus)
			}
		})
	}
}

// sanoView is a Provider view on Sano's partition whose profile lists qos, for the counting rows.
func sanoView(qos []v1alpha1.QOSProfile, hostDialed bool) *providerView {
	var part v1alpha1.PartitionProfile
	part.Name = "gpu"
	sanoPartition(&part)
	prof := &v1alpha1.PlatformProfile{Spec: v1alpha1.PlatformProfileSpec{QOS: qos, Partitions: []v1alpha1.PartitionProfile{part}}}
	return &providerView{name: provB, prov: &v1alpha1.Provider{}, profile: prof, partition: part, hostDialed: hostDialed}
}

// listedAlloc is a Running Allocation of ours on Provider B of the given size.
func listedAlloc(name string, gpus int32) v1alpha1.Allocation {
	a := v1alpha1.Allocation{ObjectMeta: metav1.ObjectMeta{Namespace: rigNS, Name: name}}
	a.Spec.ProviderConfigReference = providerRef(provB)
	b := batch(gpus, gpus*11, "135904Mi")
	b.Memory.Mul(int64(gpus))
	a.Spec.ForProvider.Batch = &b
	a.Status.AtProvider.Phase = v1alpha1.AllocationRunning
	return a
}

// askOn makes the reservation pass.reserveSlotAs makes for an Allocation of gpus GPUs on the view, at the table.
func askOn(t *testing.T, tab *slotTable, r allocsReader, pv *providerView, key string, gpus int32, withCaps bool) slotAnswer {
	t.Helper()
	rv, err := pv.forGPUs(gpus)
	if err != nil {
		t.Fatal(err)
	}
	q := slotAsk{provider: pv.name, key: rigNS + "/" + key, limit: rv.limits.MaxSubmitPerUser, caps: rv.limits, scope: rv.scope, gpus: gpus}
	if withCaps {
		size := batch(gpus, gpus*11, "135904Mi")
		size.Memory.Mul(int64(gpus))
		q.params = &size
	}
	a, err := tab.reserveAsk(context.Background(), r, q, fixedAt(hoT(9, 0, 0)))
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// TestSubmitLimitsCountPerQOS (HBEP-4, Limits counted per QOS; HBEP-7, Per-user limits): on one Provider, gpu1 allows 1 job
// and gpu4 allows 1 job (profile copies; Sano's own QOS has none typed).
//   - a 1-GPU Allocation is listed. A second 1-GPU one is refused: 1 listed under gpu1 + 1 >= its limit 1, and the
//     refusal is the QOS's own (scoped);
//   - a 4-GPU one is not: nothing is listed under gpu4. Reserved, it is an expectation under gpu4, so a second 4-GPU ask
//     is refused (1 under gpu4 + 1 >= 1) while a third 1-GPU ask is still refused only by gpu1, and 2-GPU asks, under gpu2
//     with no limit, are never refused.
func TestSubmitLimitsCountPerQOS(t *testing.T) {
	q := sanoQOS()
	q[0].MaxSubmitPerUser, q[2].MaxSubmitPerUser = i32(1), i32(1)
	pv := sanoView(q, false)
	r := allocsReader{allocs: []v1alpha1.Allocation{listedAlloc("one-gpu", 1)}}
	tab := newSlotTable()

	if a := askOn(t, tab, r, pv, "second-gpu1", 1, false); a.ok || !a.scoped || a.count != 1 {
		t.Fatalf("a second 1-GPU ask: ok %v, scoped %v, count %d; want refused by gpu1's own limit with 1 counted", a.ok, a.scoped, a.count)
	}
	if a := askOn(t, tab, r, pv, "first-gpu4", 4, false); !a.ok {
		t.Fatalf("a 4-GPU ask beside a 1-GPU Allocation was refused (count %d, scoped %v): gpu1's Allocation is not under gpu4", a.count, a.scoped)
	}
	if a := askOn(t, tab, r, pv, "second-gpu4", 4, false); a.ok || !a.scoped || a.count != 1 {
		t.Fatalf("a second 4-GPU ask: ok %v, scoped %v, count %d; want refused: the first, expected, is under gpu4", a.ok, a.scoped, a.count)
	}
	for i, key := range []string{"gpu2-a", "gpu2-b"} {
		if a := askOn(t, tab, r, pv, key, 2, false); !a.ok {
			t.Fatalf("2-GPU ask %d was refused (count %d): gpu2 has no limit, and neither the 1-GPU nor the expected 4-GPU one is under it", i, a.count)
		}
	}
	// Released, the 4-GPU slot is free again.
	tab.release(provB, rigNS+"/first-gpu4")
	if a := askOn(t, tab, r, pv, "third-gpu4", 4, false); !a.ok {
		t.Fatalf("a 4-GPU ask after the first was released was refused: %+v", a)
	}
}

// TestTheHostDialedCeilingStillCountsEveryAllocation: in host-dialed mode a Provider has at most 2 Allocations (HBEP-6), whatever
// their QOS, and a QOS's own limit counts its own. gpu1 allows 1 job.
//   - one 1-GPU Allocation is listed: a 4-GPU ask is admitted (1 counted of the ceiling's 2), a second 1-GPU one is refused
//     by gpu1's own limit;
//   - a 1-GPU and a 4-GPU Allocation are listed, 2 in all: a 4-GPU ask is refused by the ceiling, which counts both, not by a
//     QOS (gpu4 has no limit and 1 is under it): the refusal is no scoped one, and it counts 2.
func TestTheHostDialedCeilingStillCountsEveryAllocation(t *testing.T) {
	q := sanoQOS()
	q[0].MaxSubmitPerUser = i32(1)
	pv := sanoView(q, true)
	tab := newSlotTable()
	one := allocsReader{allocs: []v1alpha1.Allocation{listedAlloc("one-gpu", 1)}}
	if a := askOn(t, tab, one, pv, "gpu4-ask", 4, false); !a.ok {
		t.Fatalf("a 4-GPU ask with 1 of the ceiling's 2 counted was refused: %+v", a)
	}
	tab.release(provB, rigNS+"/gpu4-ask")
	if a := askOn(t, tab, one, pv, "gpu1-ask", 1, false); a.ok || !a.scoped {
		t.Fatalf("a second 1-GPU ask: ok %v, scoped %v; want refused by gpu1's own limit", a.ok, a.scoped)
	}
	both := allocsReader{allocs: []v1alpha1.Allocation{listedAlloc("one-gpu", 1), listedAlloc("four-gpu", 4)}}
	if a := askOn(t, tab, both, pv, "gpu4-again", 4, false); a.ok || a.scoped || a.count != 2 {
		t.Fatalf("a 4-GPU ask with 2 listed: ok %v, scoped %v, count %d; want refused by the Provider-wide ceiling with 2 counted", a.ok, a.scoped, a.count)
	}
}

// TestHedgeMembersAndExtrasAreChargedToTheirOwnQOSsCaps (HBEP-7, Hedging and Over-allocation): a member or an extra asks with
// its size (params), and the per-user caps of ITS QOS count that QOS's Allocations alone. gpu1 allows 2 GPUs and 22 CPUs per
// user, gpu4 4 GPUs. One 1-GPU Allocation (11 CPUs) is listed.
//   - a 1-GPU member: 1 + 1 = 2 GPUs and 22 CPUs, which fills gpu1's caps exactly: admitted;
//   - a second 1-GPU member: 3 GPUs above gpu1's 2: refused, naming QOS gpu1 (the first member is expected, not listed);
//   - a 4-GPU member: under gpu4 it counts 4, at its cap of 4, though 1 + 1 + 4 = 6 GPUs in all would be above it, and
//     a second 4-GPU member is then refused, 4 + 4 = 8 above 4.
func TestHedgeMembersAndExtrasAreChargedToTheirOwnQOSsCaps(t *testing.T) {
	q := sanoQOS()
	q[0].MaxGPUsPerUser, q[0].MaxCPUsPerUser = i32(2), i32(22)
	q[2].MaxGPUsPerUser = i32(4)
	pv := sanoView(q, false)
	r := allocsReader{allocs: []v1alpha1.Allocation{listedAlloc("one-gpu", 1)}}
	tab := newSlotTable()
	if a := askOn(t, tab, r, pv, "member-1", 1, true); !a.ok {
		t.Fatalf("a 1-GPU member filling gpu1's caps exactly was refused: %s", a.capsWhy)
	}
	a := askOn(t, tab, r, pv, "member-2", 1, true)
	if a.ok || !strings.Contains(a.capsWhy, "under QOS gpu1") || !strings.Contains(a.capsWhy, "3 GPUs, above the maxGpusPerUser 2 of QOS gpu1") {
		t.Fatalf("a second 1-GPU member: ok %v, %q; want refused: 3 GPUs under QOS gpu1 above its 2", a.ok, a.capsWhy)
	}
	if a := askOn(t, tab, r, pv, "member-gpu4", 4, true); !a.ok {
		t.Fatalf("a 4-GPU member was refused (%s): it runs under gpu4, which no 1-GPU Allocation is charged to", a.capsWhy)
	}
	a = askOn(t, tab, r, pv, "member-gpu4-again", 4, true)
	if a.ok || !strings.Contains(a.capsWhy, "under QOS gpu4") || !strings.Contains(a.capsWhy, "8 GPUs, above the maxGpusPerUser 4 of QOS gpu4") {
		t.Fatalf("a second 4-GPU member: ok %v, %q; want refused: 8 GPUs under QOS gpu4 above its 4", a.ok, a.capsWhy)
	}
}

// TestReservedSlotsAreChargedToTheQOSOfTheirGPUCount (HBEP-7, Per-user limits): a handover's reserved slot belongs to the
// QOS its Instance's GPU count chooses, so a 1-GPU primary's slot counts against gpu1 and not gpu4. An Instance that
// launches 4 GPUs holds 4 GPUs' slot; one that launches 2 GPUs on 2 workers holds 4 as well.
func TestReservedSlotsAreChargedToTheQOSOfTheirGPUCount(t *testing.T) {
	two := v1alpha1.Instance{}
	two.Spec.Launch.GPUs = 2
	two.Spec.Launch.Workers = i32(2)
	four := v1alpha1.Instance{}
	four.Spec.Launch.GPUs = 4
	if a, b := launchGPUs(&two), launchGPUs(&four); a != 4 || b != 4 {
		t.Fatalf("launchGPUs = %d and %d, want 4 and 4", a, b)
	}
	pv := sanoView(sanoQOS(), false)
	rv, err := pv.forGPUs(1)
	if err != nil {
		t.Fatal(err)
	}
	var got []int32
	for _, s := range rv.scope.reservedUnder([]reservedSlot{{key: "a", gpus: 1}, {key: "b", gpus: 4}, {key: "c", gpus: 1}, {key: "d"}}) {
		got = append(got, s.gpus)
	}
	// gpu1: the two 1-GPU slots, and the one whose size is not known, which counts under every QOS so that no count falls short.
	if len(got) != 3 || got[0] != 1 || got[1] != 1 || got[2] != 0 {
		t.Fatalf("slots under gpu1 have %v GPUs, want [1 1 0]", got)
	}
}

// TestAnExtrasHeadroomIsCountedOverItsOwnQOSAlone (HBEP-7, Per-user limits; Over-allocation): H is the missing primaries of the
// other deployments that the Provider allows. Under qosByGpus, an extra of 1 GPU (gpu1) counts those that would run under
// gpu1 only. Two others are missing a primary: o1 wants 1 GPU (gpu1) and o4 wants 4 (gpu4).
//   - Provider-wide: H = 2 (both);
//   - over gpu1: H = 1 (o1); over gpu4: H = 1 (o4); over gpu2, where neither runs: H = 0.
func TestAnExtrasHeadroomIsCountedOverItsOwnQOSAlone(t *testing.T) {
	f := &headroomFixture{}
	f.md("this", 1, []string{"prov-x"}, nil)
	f.objs = append(f.objs, &v1alpha1.Provider{ObjectMeta: metav1.ObjectMeta{Name: "prov-x"}})
	f.md("o1", 1, []string{"prov-x"}, nil)
	f.md("o4", 1, []string{"prov-x"}, func(md *v1alpha1.ModelDeployment) { withFlags(md, tpFlag("4")) })
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c := clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(f.objs...).Build()
	p := &pass{r: &Reconciler{Client: c, Reader: c, Clock: pfake.NewClock(hoT(8, 0, 0))}, key: types.NamespacedName{Namespace: "ns", Name: "this"}, now: hoT(8, 0, 0)}
	pv := sanoView(sanoQOS(), false)
	pv.name = "prov-x"
	h := func(gpus int32) int {
		t.Helper()
		var fn func(context.Context) (int, error)
		if gpus == 0 {
			fn = p.headroomOf(pv)
		} else {
			rv, err := pv.forGPUs(gpus)
			if err != nil {
				t.Fatal(err)
			}
			fn = p.headroomWhere(pv, rv.scope)
		}
		n, err := fn(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	for _, tc := range []struct {
		name string
		gpus int32 // 0: Provider-wide
		want int
	}{{"Provider-wide", 0, 2}, {"under gpu1", 1, 1}, {"under gpu4", 4, 1}, {"under gpu2", 2, 0}} {
		if got := h(tc.gpus); got != tc.want {
			t.Errorf("headroom %s = %d, want %d", tc.name, got, tc.want)
		}
	}
}
