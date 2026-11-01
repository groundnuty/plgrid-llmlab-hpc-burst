package lifecycle

// HBEP-7's Test plan item "PartitionTooShort at the boundary", on fixtures
// (the shipped Athena plgrid-now follows when its sizing is restored). The
// profile sets drainWindow 2 h 10 min, drainMargin 10 min, handoverMargin
// 30 min and health.startTimeout 1 h, each written out so the lead is worked
// out by hand:
//   - no routing.maxRequestDuration: drain window 2h10m, lead 2h10m + 30m + 1h
//     = 3h40m;
//   - routing.maxRequestDuration 20 min: drain window min(2h10m, 20m + 10m) =
//     30m, lead 30m + 30m + 1h = 2h.
//
// A time limit not longer than the lead gives PartitionTooShort with no
// Allocation; one minute more gets an Allocation. Cases well inside or outside
// the boundary prove nothing, so every case sits on it.

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// shortProfile sets the profile values the lead is made of.
func shortProfile(s *v1alpha1.PlatformProfileSpec) {
	s.DrainWindow = dur(2*time.Hour + 10*time.Minute)
	s.DrainMargin = dur(10 * time.Minute)
	s.HandoverMargin = dur(30 * time.Minute)
	s.Health = &v1alpha1.HealthThresholds{StartTimeout: dur(time.Hour)}
}

// shortStep creates the deployment and runs one round.
func shortStep(rg *rig, md *v1alpha1.ModelDeployment) {
	rg.t.Helper()
	rg.create(md)
	rg.clock.Advance(10 * time.Second)
	rg.Step()
}

func wantTooShort(t *testing.T, rg *rig, md string, names ...string) {
	t.Helper()
	if as := rg.allocations(md); len(as) != 0 {
		t.Fatalf("%d Allocations on a partition too short, want none", len(as))
	}
	if ins := rg.instances(md); len(ins) != 0 {
		t.Fatalf("%d Instances, want none", len(ins))
	}
	c := rg.condition(md, v1alpha1.ConditionDegraded)
	if c.Status != metav1.ConditionTrue || c.Reason != v1alpha1.ReasonPartitionTooShort {
		t.Fatalf("Degraded = %s/%s %q, want True/PartitionTooShort", c.Status, c.Reason, c.Message)
	}
	for _, n := range names {
		if !strings.Contains(c.Message, n) {
			t.Errorf("message %q does not name %q", c.Message, n)
		}
	}
}

func wantAllocated(t *testing.T, rg *rig, md string, limit v1alpha1.Duration) {
	t.Helper()
	as := rg.allocations(md)
	if len(as) != 1 {
		t.Fatalf("%d Allocations, want 1 just past the lead", len(as))
	}
	if got := as[0].Spec.ForProvider.Batch.TimeLimit; got != limit {
		t.Fatalf("timeLimit %q, want %q", got, limit)
	}
	if c := rg.condition(md, v1alpha1.ConditionDegraded); c.Reason == v1alpha1.ReasonPartitionTooShort || strings.Contains(c.Message, v1alpha1.ReasonPartitionTooShort) {
		t.Fatalf("Degraded = %s/%s %q just past the lead", c.Status, c.Reason, c.Message)
	}
}

// TestPartitionTooShortAtTheBoundary: the bound is the partition's maxTime.
func TestPartitionTooShortAtTheBoundary(t *testing.T) {
	m := func(h, min int) time.Duration { return time.Duration(h)*time.Hour + time.Duration(min)*time.Minute }
	for _, tc := range []struct {
		name     string
		maxTime  time.Duration
		maxReq   *v1alpha1.Duration
		short    bool
		limit    string // as the message and the Allocation write it
		wantLead string
	}{
		{"3h39m against 3h40m", m(3, 39), nil, true, "3h39m0s", "3h40m0s"},
		{"exactly 3h40m", m(3, 40), nil, true, "3h40m0s", "3h40m0s"},
		{"3h41m", m(3, 41), nil, false, "3h41m0s", ""},
		{"1h59m against 2h", m(1, 59), dur(20 * time.Minute), true, "1h59m0s", "2h0m0s"},
		{"exactly 2h", m(2, 0), dur(20 * time.Minute), true, "2h0m0s", "2h0m0s"},
		{"2h1m", m(2, 1), dur(20 * time.Minute), false, "2h1m0s", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rg := newRig(t, withMaxTime(tc.maxTime), withProfile(shortProfile))
			md := mdSpec("md")
			if tc.maxReq != nil {
				md.Spec.Routing = &v1alpha1.Routing{MaxRequestDuration: tc.maxReq}
			}
			shortStep(rg, md)
			if !tc.short {
				wantAllocated(t, rg, "md", v1alpha1.Duration(tc.limit))
				return
			}
			wantTooShort(t, rg, "md", "partition "+rigPartition, "time limit "+tc.limit, "set by maxTime", "lead "+tc.wantLead)
		})
	}
}

// TestPartitionTooShortNamesTheBound: the message names the bound that set the
// limit. With maxTime 48 h, a leaseLimit of 3h40m sets it (leaseLimit); a QOS
// maxWall of 3h40m sets it (maxWall). The lead is 3h40m, so both are too short.
func TestPartitionTooShortNamesTheBound(t *testing.T) {
	t.Run("leaseLimit", func(t *testing.T) {
		rg := newRig(t, withProfile(shortProfile))
		md := mdSpec("md")
		md.Spec.LeaseLimit = dur(3*time.Hour + 40*time.Minute)
		shortStep(rg, md)
		wantTooShort(t, rg, "md", "time limit 3h40m0s", "set by leaseLimit", "lead 3h40m0s")
	})
	t.Run("maxWall", func(t *testing.T) {
		opts := append(qosOpts([]v1alpha1.QOSProfile{{Name: "example-now", MaxWall: dur(3*time.Hour + 40*time.Minute)}}, "", "example-now"), withProfile(func(s *v1alpha1.PlatformProfileSpec) {
			shortProfile(s)
			s.QOS = []v1alpha1.QOSProfile{{Name: "example-now", MaxWall: dur(3*time.Hour + 40*time.Minute)}}
		}))
		rg := newRig(t, opts...)
		shortStep(rg, mdSpec("md"))
		wantTooShort(t, rg, "md", "time limit 3h40m0s", "set by maxWall", "lead 3h40m0s")
	})
}

// TestPartitionTooShortSecondProviderFits: the rig's Provider is too short
// (maxTime 1 h against a lead of 3h40m); a second allowed Provider with 48 h
// fits, so the Allocation goes there, and the condition still names the first.
func TestPartitionTooShortSecondProviderFits(t *testing.T) {
	rg := newRig(t, withMaxTime(time.Hour), withProfile(shortProfile))
	rg.addProvider("example-second", "gpu-long", 48*time.Hour, nil, nil)
	md := mdSpec("md")
	md.Spec.Providers.Names = []string{rigProvider, "example-second"}
	shortStep(rg, md)
	as := rg.allocations("md")
	if len(as) != 1 || as[0].Labels[v1alpha1.LabelProvider] != "example-second" || as[0].Spec.ForProvider.Batch.Partition != "gpu-long" {
		t.Fatalf("Allocations %+v, want one on example-second's gpu-long", as)
	}
	c := rg.condition("md", v1alpha1.ConditionDegraded)
	if c.Reason != v1alpha1.ReasonPartitionTooShort || !strings.Contains(c.Message, "partition "+rigPartition+" of Provider "+rigProvider) || !strings.Contains(c.Message, "time limit 1h0m0s") {
		t.Fatalf("Degraded = %s/%s %q, want PartitionTooShort naming %s of %s", c.Status, c.Reason, c.Message, rigPartition, rigProvider)
	}
	if strings.Contains(c.Message, "gpu-long") {
		t.Fatalf("the condition names the Provider that fits: %q", c.Message)
	}
	// It holds on later passes too, while the Provider is allowed.
	rg.clock.Advance(10 * time.Second)
	rg.Step()
	if c := rg.condition("md", v1alpha1.ConditionDegraded); c.Reason != v1alpha1.ReasonPartitionTooShort {
		t.Fatalf("on the next pass Degraded = %s/%s, want PartitionTooShort still", c.Status, c.Reason)
	}
}

// TestPartitionTooShortAloneWhenEveryProviderIsTooShort: with every usable
// Provider too short, the reason is PartitionTooShort alone, naming each, with
// no SpecUnsupported and no ProviderUnavailable.
func TestPartitionTooShortAloneWhenEveryProviderIsTooShort(t *testing.T) {
	rg := newRig(t, withMaxTime(time.Hour), withProfile(shortProfile))
	rg.addProvider("example-second", "gpu-short", 2*time.Hour, nil, nil)
	md := mdSpec("md")
	md.Spec.Providers.Names = []string{rigProvider, "example-second"}
	shortStep(rg, md)
	wantTooShort(t, rg, "md", "partition "+rigPartition, "partition gpu-short", "time limit 1h0m0s", "time limit 2h0m0s")
	c := rg.condition("md", v1alpha1.ConditionDegraded)
	for _, other := range []string{v1alpha1.ReasonSpecUnsupported, v1alpha1.ReasonProviderUnavailable, v1alpha1.ReasonSubmitLimitReached} {
		if strings.Contains(c.Message, other+":") {
			t.Errorf("message %q adds %s; PartitionTooShort explains it alone", c.Message, other)
		}
	}
}

// TestPartitionTooShortWithASizeRefusal: the rig's Provider is too short and
// the second refuses the spec's 2 GPUs (gpusPerNode 1). Every usable Provider
// that is not too short refused the size, so the spec is unsupported:
// SpecUnsupported ranks first, and PartitionTooShort is in the message
// (`@hpc-burst-science-agent`'s rule, relayed by qos, 1 Nov 2026).
func TestPartitionTooShortWithASizeRefusal(t *testing.T) {
	rg := newRig(t, withMaxTime(time.Hour), withProfile(shortProfile))
	rg.addProvider("example-second", "gpu-one", 48*time.Hour, func(pp *v1alpha1.PartitionProfile) { pp.GPUsPerNode = i32(1) }, nil)
	md := withFlags(mdSpec("md"), v1alpha1.EngineFlag{Name: "tensor-parallel-size", Value: "2"})
	md.Spec.Providers.Names = []string{rigProvider, "example-second"}
	shortStep(rg, md)
	if as := rg.allocations("md"); len(as) != 0 {
		t.Fatalf("%d Allocations, want none", len(as))
	}
	c := rg.condition("md", v1alpha1.ConditionDegraded)
	if c.Reason != v1alpha1.ReasonSpecUnsupported || !strings.Contains(c.Message, v1alpha1.ReasonPartitionTooShort+": ") || !strings.Contains(c.Message, "gpu-one") {
		t.Fatalf("Degraded = %s/%s %q, want SpecUnsupported naming gpu-one, with PartitionTooShort in the message", c.Status, c.Reason, c.Message)
	}
}
