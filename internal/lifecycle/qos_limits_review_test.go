package lifecycle

// Review of PR 171 (Astra B2, B3): the memory request against a cap in decimal
// units, and a known time limit under a minute. Every expected value is worked
// out by hand in the comment beside it.

import (
	"errors"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// TestMemoryCapIsClampedInWholeMiB: the provider (internal/slurm) rounds a
// request up to whole MiB and a cap down, and refuses a request whose MiB are
// above the cap's. A request equal to a decimal-unit cap is therefore refused
// for ever, with the immutable Allocation holding its slot. The sizing clamps
// to the cap in whole MiB, rounded down. 1 MiB is 1,048,576 bytes.
//   - cap 100G = 100,000,000,000 bytes = 95,367.43 MiB, so 95367 MiB;
//   - cap 100Gi = 102,400 MiB, exactly;
//   - a request of 100G (no clamp by a comparison with the cap itself, since it
//     equals the cap) rounds up to 95,368 MiB, above 95,367, so it is clamped
//     too; so is 99,999,600,000 bytes, which is 95,367.05 MiB (95,367 MiB are 99,999,547,392 bytes) and under the cap.
func TestMemoryCapIsClampedInWholeMiB(t *testing.T) {
	for _, tc := range []struct {
		name   string
		perGPU string // the partition's memPerGpu
		gpus   int32
		cap    string
		want   string
	}{
		{"120Gi against 100G", "120Gi", 1, "100G", "95367Mi"},
		{"2 x 120Gi against 100G", "120Gi", 2, "100G", "95367Mi"},
		{"a request equal to the cap, 100G against 100G", "100G", 1, "100G", "95367Mi"},
		{"120Gi against 100Gi", "120Gi", 1, "100Gi", "100Gi"},
		{"a request under the cap is as it was, 120Gi against 200Gi", "120Gi", 1, "200Gi", "120Gi"},
		{"a request exactly at the cap's MiB is as it was, 95367Mi against 100G", "95367Mi", 1, "100G", "95367Mi"},
		{"a request inside the cap's last partial MiB, 99999600000 bytes against 100G", "99999600000", 1, "100G", "95367Mi"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pv := planProvider()
			pp := resource.MustParse(tc.perGPU)
			pv.partition.MemPerGPU = &pp
			c := resource.MustParse(tc.cap)
			pv.limits = v1alpha1.QOSLimits{MaxMemPerUser: &c, From: map[string]string{v1alpha1.LimitMaxMemPerUser: "example-now"}}
			got, err := partitionSizing(pv, tc.gpus, nil)
			if err != nil {
				t.Fatal(err)
			}
			if want := resource.MustParse(tc.want); got.Memory.Cmp(want) != 0 || got.Memory.String() != tc.want {
				t.Fatalf("memory %s, want %s", got.Memory.String(), tc.want)
			}
		})
	}
	// A cap under one MiB leaves no memory: --mem=0 would take the node's.
	pv := planProvider()
	c := resource.MustParse("512Ki")
	pv.limits = v1alpha1.QOSLimits{MaxMemPerUser: &c, From: map[string]string{v1alpha1.LimitMaxMemPerUser: "example-now"}}
	if _, err := partitionSizing(pv, 1, nil); err == nil || !strings.Contains(err.Error(), "no memory") {
		t.Fatalf("a cap of 512Ki: error %v, want one saying no memory is left", err)
	}
}

// TestMemoryCapThroughTheController: the Allocation a QOS maxMemPerUser of 100G
// gets carries 95367Mi, and its CPUs and GPUs are as sized (1 GPU: 16 CPUs).
func TestMemoryCapThroughTheController(t *testing.T) {
	rg := newRig(t, qosOpts([]v1alpha1.QOSProfile{{Name: "example-now", MaxMemPerUser: memQ("100G")}}, "", "example-now")...)
	rg.create(mdSpec("md"))
	rg.clock.Advance(10 * time.Second)
	rg.Step()
	as := rg.allocations("md")
	if len(as) != 1 {
		t.Fatalf("%d Allocations, want 1", len(as))
	}
	if b := as[0].Spec.ForProvider.Batch; b.GPUs != 1 || b.CPUsPerTask != 16 || b.Memory.String() != "95367Mi" {
		t.Fatalf("sized %d GPUs, %d CPUs, %s; want 1, 16, 95367Mi", b.GPUs, b.CPUsPerTask, b.Memory.String())
	}
}

// TestKnownTimeLimitUnderAMinuteIsTooShort: a limit that is set but under a
// minute is a known, unusably short limit, told apart from an absent one. The
// rig's profile makes the lead 3h40m (shortProfile), so every case is too short
// by far; the message names the raw value of the bound, as written, and the
// lead.
//   - QOS maxWall 30s: time limit 30s, set by maxWall;
//   - leaseLimit 30s: time limit 30s, set by leaseLimit;
//   - leaseLimit 59s: 59s, set by leaseLimit (the last second below a minute);
//   - leaseLimit 1m: whole minutes, 1m0s, set by leaseLimit (the old path).
func TestKnownTimeLimitUnderAMinuteIsTooShort(t *testing.T) {
	maxWall := func(d time.Duration) []rigOption {
		return append(qosOpts([]v1alpha1.QOSProfile{{Name: "example-now", MaxWall: dur(d)}}, "", "example-now"), withProfile(shortProfile))
	}
	leaseLimit := func(d time.Duration) func(*v1alpha1.ModelDeployment) {
		return func(md *v1alpha1.ModelDeployment) { md.Spec.LeaseLimit = dur(d) }
	}
	for _, tc := range []struct {
		name  string
		opts  []rigOption
		edit  func(*v1alpha1.ModelDeployment)
		wants []string
	}{
		{"maxWall 30s", maxWall(30 * time.Second), nil, []string{"time limit 30s,", "set by maxWall", "lead 3h40m0s"}},
		{"leaseLimit 30s", []rigOption{withProfile(shortProfile)}, leaseLimit(30 * time.Second), []string{"time limit 30s,", "set by leaseLimit", "lead 3h40m0s"}},
		{"leaseLimit 59s", []rigOption{withProfile(shortProfile)}, leaseLimit(59 * time.Second), []string{"time limit 59s,", "set by leaseLimit", "lead 3h40m0s"}},
		{"leaseLimit 1m", []rigOption{withProfile(shortProfile)}, leaseLimit(time.Minute), []string{"time limit 1m0s,", "set by leaseLimit", "lead 3h40m0s"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rg := newRig(t, tc.opts...)
			md := mdSpec("md")
			if tc.edit != nil {
				tc.edit(md)
			}
			shortStep(rg, md)
			wantTooShort(t, rg, "md", append([]string{"partition " + rigPartition}, tc.wants...)...)
			if c := rg.condition("md", v1alpha1.ConditionDegraded); strings.Contains(c.Message, v1alpha1.ReasonProviderUnavailable) {
				t.Fatalf("message %q also names ProviderUnavailable: a known short limit was read as an unknown one", c.Message)
			}
		})
	}
}

// TestAbsentTimeLimitIsNotPartitionTooShort: with no maxTime, no maxWall and no
// leaseLimit, the profile cannot give a time limit. That is a profile problem
// (ProviderUnavailable naming the three), not a limit that is too short.
func TestAbsentTimeLimitIsNotPartitionTooShort(t *testing.T) {
	rg := newRig(t, withProfile(shortProfile), withPartition(func(pp *v1alpha1.PartitionProfile) { pp.MaxTime = nil }))
	shortStep(rg, mdSpec("md"))
	if as := rg.allocations("md"); len(as) != 0 {
		t.Fatalf("%d Allocations, want none without a time limit", len(as))
	}
	c := rg.condition("md", v1alpha1.ConditionDegraded)
	if c.Reason != v1alpha1.ReasonProviderUnavailable || !strings.Contains(c.Message, "has no maxTime") {
		t.Fatalf("Degraded = %s/%s %q, want ProviderUnavailable saying there is no time limit", c.Status, c.Reason, c.Message)
	}
	if strings.Contains(c.Message, v1alpha1.ReasonPartitionTooShort) {
		t.Fatalf("message %q names PartitionTooShort for an absent limit", c.Message)
	}
}

// TestAllocationTimeLimitTellsShortFromAbsent is the same distinction on the
// function: a known 30s limit comes back with its bound and its raw value and
// no error; an absent one is an error.
func TestAllocationTimeLimitTellsShortFromAbsent(t *testing.T) {
	pv := planProvider()
	pv.limits = v1alpha1.QOSLimits{MaxWall: ptrTo(30 * time.Second)}
	pv.partition.MaxTime = nil
	lim, bound, err := allocationTimeLimit(pv, nil)
	if err != nil || lim != 30*time.Second || bound != v1alpha1.TimeLimitMaxWall {
		t.Fatalf("a maxWall of 30s: (%s, %s, %v), want (30s, maxWall, nil)", lim, bound, err)
	}
	// partitionSizing never sizes an Allocation for it, whoever calls it.
	_, err = partitionSizing(pv, 1, nil)
	var u *unsupported
	if err == nil || errors.As(err, &u) || !strings.Contains(err.Error(), "under a minute") {
		t.Fatalf("sizing under a 30s limit: error %v, want a plain error saying under a minute", err)
	}
	pv.limits = v1alpha1.QOSLimits{}
	if _, _, err := allocationTimeLimit(pv, nil); err == nil || !strings.Contains(err.Error(), "has no maxTime") {
		t.Fatalf("no limit at all: error %v, want one saying there is no maxTime", err)
	}
}
