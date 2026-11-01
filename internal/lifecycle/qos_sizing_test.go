package lifecycle

// HBEP-7's Test plan items on QOS sizing, through the controller: the QOS time
// limit, the GPU bound, the CPU and memory caps, and which submit limit
// applies. Every expected value is worked out by hand in the comment beside it
// (HBEP-4, QOS limits: for each limit the partition QOS's value if set, unless
// the job QOS has overPartQos, else the job QOS's; the time limit is the
// smallest of maxTime, maxWall and leaseLimit), never read back through
// EffectiveQOSLimits or the sizing code.

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// qosOpts configures the rig's profile with qos, the rig partition's qos and
// the rig Provider's qos.
func qosOpts(qos []v1alpha1.QOSProfile, partitionQOS, providerQOS string) []rigOption {
	return []rigOption{
		withProfile(func(s *v1alpha1.PlatformProfileSpec) { s.QOS = qos }),
		withPartition(func(pp *v1alpha1.PartitionProfile) { pp.QOS = partitionQOS }),
		withProviderQOS(providerQOS),
	}
}

func memQ(s string) *resource.Quantity { q := resource.MustParse(s); return &q }

// withFlags returns a deployment spec with engine flags added.
func withFlags(md *v1alpha1.ModelDeployment, flags ...v1alpha1.EngineFlag) *v1alpha1.ModelDeployment {
	md.Spec.Source.Served.Flags = append(md.Spec.Source.Served.Flags, flags...)
	return md
}

// TestQOSTimeLimit covers the Test plan item "QOS time limit". The partition's
// maxTime is 48 h (the rig's default):
//   - a Provider QOS with maxWall 12 h: min(48 h, 12 h) = 12 h;
//   - maxWall 72 h: min(48 h, 72 h) = 48 h, a wall limit never lifts maxTime;
//   - no qos: 48 h;
//   - a leaseLimit of 4 h under the 12 h QOS: min(48 h, 12 h, 4 h) = 4 h.
func TestQOSTimeLimit(t *testing.T) {
	for _, tc := range []struct {
		name    string
		maxWall *v1alpha1.Duration
		qos     bool
		lease   *v1alpha1.Duration
		want    v1alpha1.Duration
	}{
		{"maxWall 12h", dur(12 * time.Hour), true, nil, "12h0m0s"},
		{"maxWall 72h", dur(72 * time.Hour), true, nil, "48h0m0s"},
		{"no qos", nil, false, nil, "48h0m0s"},
		{"leaseLimit 4h under the 12h QOS", dur(12 * time.Hour), true, dur(4 * time.Hour), "4h0m0s"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var opts []rigOption
			if tc.qos {
				opts = qosOpts([]v1alpha1.QOSProfile{{Name: "example-now", MaxWall: tc.maxWall}}, "", "example-now")
			}
			rg := newRig(t, opts...)
			md := mdSpec("md")
			md.Spec.LeaseLimit = tc.lease
			rg.create(md)
			rg.clock.Advance(10 * time.Second)
			rg.Step()
			as := rg.allocations("md")
			if len(as) != 1 {
				t.Fatalf("%d Allocations, want 1", len(as))
			}
			if got := as[0].Spec.ForProvider.Batch.TimeLimit; got != tc.want {
				t.Fatalf("timeLimit = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestQOSGPUBound covers the Test plan item "GPU bound". The partition has
// gpusPerNode 8, cpusPerGpu 16 and memPerGpu 120Gi.
//   - A Provider QOS with maxGpusPerUser 1: tensor-parallel-size 2 needs 2
//     GPUs, above min(8, 1) = 1, so it is refused, not capped, and with no
//     other allowed Provider the deployment has SpecUnsupported and no
//     Allocation. The partition's own QOS sets no GPU cap, so the Provider
//     QOS's cap applies ("The GPU cap comes from the Provider's QOS when the
//     partition QOS sets none").
//   - A spec of one GPU gets one GPU, 16 CPUs and 120Gi.
//   - A QOS whose maxMemPerUser, 100Gi, is below one GPU's memPerGpu caps the
//     memory at 100Gi, and the GPU count stays 1; a maxCpusPerUser of 8 caps
//     the CPUs at 8.
func TestQOSGPUBound(t *testing.T) {
	gpuNode := func(pp *v1alpha1.PartitionProfile) { pp.GPUsPerNode = i32(8) }
	qos := func(now v1alpha1.QOSProfile) []rigOption {
		return []rigOption{
			withProfile(func(s *v1alpha1.PlatformProfileSpec) {
				s.QOS = []v1alpha1.QOSProfile{{Name: "example-part", MaxSubmitPerUser: i32(2000)}, now}
			}),
			withPartition(func(pp *v1alpha1.PartitionProfile) { gpuNode(pp); pp.QOS = "example-part" }),
			withProviderQOS(now.Name),
		}
	}
	t.Run("two GPUs refused, not capped", func(t *testing.T) {
		rg := newRig(t, qos(v1alpha1.QOSProfile{Name: "example-now", MaxGPUsPerUser: i32(1)})...)
		rg.create(withFlags(mdSpec("md"), v1alpha1.EngineFlag{Name: "tensor-parallel-size", Value: "2"}))
		rg.clock.Advance(10 * time.Second)
		rg.Step()
		if as := rg.allocations("md"); len(as) != 0 {
			t.Fatalf("%d Allocations, want none: a parallel size is refused, never capped (first: %+v)", len(as), as[0].Spec.ForProvider.Batch)
		}
		if ins := rg.instances("md"); len(ins) != 0 {
			t.Fatalf("%d Instances, want none", len(ins))
		}
		c := rg.condition("md", v1alpha1.ConditionDegraded)
		if c.Status != metav1.ConditionTrue || c.Reason != v1alpha1.ReasonSpecUnsupported {
			t.Fatalf("Degraded = %s/%s, want True/SpecUnsupported", c.Status, c.Reason)
		}
		for _, want := range []string{"2 GPUs", "maxGpusPerUser 1", "QOS example-now"} {
			if !strings.Contains(c.Message, want) {
				t.Errorf("message %q does not name %q", c.Message, want)
			}
		}
	})
	t.Run("one GPU gets one", func(t *testing.T) {
		rg := newRig(t, qos(v1alpha1.QOSProfile{Name: "example-now", MaxGPUsPerUser: i32(1)})...)
		rg.create(mdSpec("md"))
		rg.clock.Advance(10 * time.Second)
		rg.Step()
		as := rg.allocations("md")
		if len(as) != 1 {
			t.Fatalf("%d Allocations, want 1", len(as))
		}
		b := as[0].Spec.ForProvider.Batch
		if b.GPUs != 1 || b.CPUsPerTask != 16 || b.Memory.Cmp(resource.MustParse("120Gi")) != 0 {
			t.Fatalf("sized %d GPUs, %d CPUs, %s; want 1, 16, 120Gi", b.GPUs, b.CPUsPerTask, b.Memory.String())
		}
	})
	t.Run("memory and CPUs capped, GPUs unchanged", func(t *testing.T) {
		rg := newRig(t, qos(v1alpha1.QOSProfile{Name: "example-now", MaxGPUsPerUser: i32(1), MaxMemPerUser: memQ("100Gi"), MaxCPUsPerUser: i32(8)})...)
		rg.create(mdSpec("md"))
		rg.clock.Advance(10 * time.Second)
		rg.Step()
		as := rg.allocations("md")
		if len(as) != 1 {
			t.Fatalf("%d Allocations, want 1", len(as))
		}
		b := as[0].Spec.ForProvider.Batch
		if b.GPUs != 1 || b.CPUsPerTask != 8 || b.Memory.Cmp(resource.MustParse("100Gi")) != 0 {
			t.Fatalf("sized %d GPUs, %d CPUs, %s; want 1, 8, 100Gi", b.GPUs, b.CPUsPerTask, b.Memory.String())
		}
	})
}

// TestQOSWhichSubmitLimitApplies covers the Test plan item "Which limit
// applies", through the controller. The limit is observed where it binds:
// once the count reaches it, the next deployment gets no Allocation and
// SubmitLimitReached names the limit and the QOS that set it.
//   - Partition QOS maxSubmitPerUser 2000, Provider QOS 1: the partition's
//     comes first, so 2000. With 1999 Allocations of other deployments not
//     ended on the Provider, a first deployment gets the 2000th slot and a
//     second none.
//   - overPartQos on the Provider's QOS: its 1 comes first. One deployment
//     gets the slot; a second none.
//   - No partition QOS: the Provider QOS's 1.
func TestQOSWhichSubmitLimitApplies(t *testing.T) {
	for _, tc := range []struct {
		name      string
		partQOS   string
		overPart  bool
		preload   int
		wantLimit string
	}{
		{"partition 2000 before Provider 1", "example-part", false, 1999, "maxSubmitPerUser 2000 of QOS example-part"},
		{"overPartQos puts the Provider's 1 first", "example-part", true, 0, "maxSubmitPerUser 1 of QOS example-now"},
		{"no partition QOS gives the Provider's 1", "", false, 0, "maxSubmitPerUser 1 of QOS example-now"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			qos := []v1alpha1.QOSProfile{
				{Name: "example-part", MaxSubmitPerUser: i32(2000)},
				{Name: "example-now", MaxSubmitPerUser: i32(1), MaxGPUsPerUser: i32(1), OverPartQOS: tc.overPart},
			}
			rg := newRig(t, qosOpts(qos, tc.partQOS, "example-now")...)
			rg.Step() // the Provider's first status: Ready
			for i := range tc.preload {
				rg.create(otherAllocation(fmt.Sprintf("other-%04d", i)))
			}
			// Lifecycle passes only from here: the Provider's controller would
			// submit the preloaded Allocations, which stand for jobs already
			// held.
			rg.create(mdSpec("md-a"))
			rg.reconcileAll()
			rg.create(mdSpec("md-b"))
			rg.reconcileAll()
			if n := len(rg.allocations("md-a")); n != 1 {
				t.Fatalf("md-a has %d Allocations, want the last slot", n)
			}
			if n := len(rg.allocations("md-b")); n != 0 {
				t.Fatalf("md-b has %d Allocations, want none past the limit", n)
			}
			c := rg.condition("md-b", v1alpha1.ConditionDegraded)
			if c.Status != metav1.ConditionTrue || c.Reason != v1alpha1.ReasonSubmitLimitReached || !strings.Contains(c.Message, tc.wantLimit) || !strings.Contains(c.Message, "Provider "+rigProvider) {
				t.Fatalf("md-b Degraded = %s/%s %q, want SubmitLimitReached naming Provider %s and %q", c.Status, c.Reason, c.Message, rigProvider, tc.wantLimit)
			}
		})
	}
}

// otherAllocation is an Allocation of another deployment, in another
// namespace, on the rig's Provider. Its phase is not yet written, which is not
// Ended, so the count holds it.
func otherAllocation(name string) *v1alpha1.Allocation {
	a := &v1alpha1.Allocation{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "example-other",
			Labels: map[string]string{v1alpha1.LabelProvider: rigProvider, v1alpha1.LabelModelDeployment: "example-other-md"}},
		Spec: v1alpha1.AllocationSpec{
			ForProvider: v1alpha1.AllocationParameters{Batch: &v1alpha1.BatchParameters{Partition: rigPartition, TimeLimit: "1h0m0s", GPUs: 1, CPUsPerTask: 16, Memory: resource.MustParse("120Gi"), Nodes: 1}},
		},
	}
	a.Spec.ProviderConfigReference = providerRef(rigProvider)
	return a
}
