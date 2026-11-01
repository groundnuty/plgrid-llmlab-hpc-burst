package lifecycle

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/yaml"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// Athena's plgrid-now is for conformance runs and short checks only; the lifecycle
// controller must never plan an Instance there (`@hpc-burst-science-agent`, 1 Nov 2026,
// issue #93, comment 6557245889). HBEP-7's PartitionTooShort is what keeps it off: the
// partition's time limit, 1 h, is not longer than the lead an Instance needs (the drain
// window plus handoverMargin plus health.startTimeout). Its cpusPerGpu and memPerGpu are
// given, so absent sizing no longer refuses it (the plgrid-now sizing restore of #151,
// after #168 typed the profile's QOS limits).
//
// The expected values are worked out by hand from the shipped profile, never read back
// through the code under test:
//   - the lead with the profile's defaults: drainWindow 2h10m + handoverMargin 30m +
//     health.startTimeout 1h = 3h40m;
//   - with a 5 min routing.maxRequestDuration: drain window min(2h10m, 5m + drainMargin
//     10m) = 15m, lead 15m + 30m + 1h = 1h45m. Either way it is above 1h;
//   - one GPU's sizing: 16 CPUs and 128000Mi (the A100 node's share), each within the
//     QOS plgrid-now caps of 16 CPUs and 125Gi (= 128000Mi), and a 1 h limit.

// shippedAthena reads profiles/athena.yaml; a missing file fails, it never skips.
func shippedAthena(t *testing.T) (v1alpha1.PlatformProfile, func(string) v1alpha1.PartitionProfile) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "profiles", "athena.yaml"))
	if err != nil {
		t.Fatalf("read the shipped profile: %v", err)
	}
	var prof v1alpha1.PlatformProfile
	if err := yaml.UnmarshalStrict(raw, &prof); err != nil {
		t.Fatal(err)
	}
	entry := func(name string) v1alpha1.PartitionProfile {
		for _, p := range prof.Spec.Partitions {
			if p.Name == name {
				return p
			}
		}
		t.Fatalf("athena.yaml does not list %s", name)
		return v1alpha1.PartitionProfile{}
	}
	return prof, entry
}

// The shipped plgrid-now is sized now, as plgrid-gpu-a100 is, under its QOS caps, and a
// second GPU is refused by the QOS's one GPU per user; sizing is not what keeps it off.
func TestShippedAthenaNowIsSized(t *testing.T) {
	prof, entry := shippedAthena(t)
	view := func(name string) *providerView {
		part := entry(name)
		limits, err := v1alpha1.EffectiveQOSLimits(&prof.Spec, part, "")
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		return &providerView{name: "athena", profile: &prof, partition: part, limits: limits, agentDigest: strings.Repeat("a", 64)}
	}
	for _, tc := range []struct {
		partition string
		limit     v1alpha1.Duration
	}{
		{"plgrid-gpu-a100", "48h0m0s"},
		{"plgrid-now", "1h0m0s"},
	} {
		got, err := partitionSizing(view(tc.partition), 1, nil)
		if err != nil {
			t.Fatalf("%s: an Instance of 1 GPU is not sized: %v", tc.partition, err)
		}
		if want := resource.MustParse("128000Mi"); got.GPUs != 1 || got.CPUsPerTask != 16 || got.Memory.Cmp(want) != 0 || got.TimeLimit != tc.limit {
			t.Errorf("%s sized as %+v; want 1 GPU, 16 CPUs, 128000Mi, %s", tc.partition, got, tc.limit)
		}
	}
	if got, err := partitionSizing(view("plgrid-now"), 2, nil); err == nil {
		t.Errorf("an Instance of 2 GPUs is sized on plgrid-now, above its QOS's one GPU per user: %+v", got)
	}
}

// The shipped plgrid-now gives PartitionTooShort and no Allocation, with the profile's
// defaults and with a short routing.maxRequestDuration. The rig carries the shipped
// profile's lead values, its QOS list and the plgrid-now entry's limits and sizing; the
// rig's own partition name stands for plgrid-now. The control is the same deployment on
// the shipped plgrid-gpu-a100 values, which gets an Allocation.
func TestShippedAthenaNowIsTooShort(t *testing.T) {
	prof, entry := shippedAthena(t)
	shipped := func(name string) []rigOption {
		part := entry(name)
		maxTime, err := part.MaxTime.AsDuration()
		if err != nil {
			t.Fatal(err)
		}
		return []rigOption{
			withMaxTime(maxTime),
			withProfile(func(s *v1alpha1.PlatformProfileSpec) {
				s.DrainWindow, s.DrainMargin, s.HandoverMargin = prof.Spec.DrainWindow, prof.Spec.DrainMargin, prof.Spec.HandoverMargin
				s.Health, s.QOS = prof.Spec.Health, prof.Spec.QOS
			}),
			withPartition(func(p *v1alpha1.PartitionProfile) {
				p.MaxTime, p.GPUsPerNode, p.CPUsPerGPU, p.MemPerGPU, p.QOS = part.MaxTime, part.GPUsPerNode, part.CPUsPerGPU, part.MemPerGPU, part.QOS
			}),
		}
	}
	for _, tc := range []struct {
		name   string
		maxReq *v1alpha1.Duration
		lead   string
	}{
		{"the profile's defaults", nil, "lead 3h40m0s"},
		{"a 5 min routing.maxRequestDuration", dur(5 * time.Minute), "lead 1h45m0s"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rg := newRig(t, shipped("plgrid-now")...)
			md := mdSpec("md")
			if tc.maxReq != nil {
				md.Spec.Routing = &v1alpha1.Routing{MaxRequestDuration: tc.maxReq}
			}
			shortStep(rg, md)
			wantTooShort(t, rg, "md", "partition "+rigPartition, "time limit 1h0m0s", "set by maxTime", tc.lead)
		})
	}
	t.Run("control: the shipped plgrid-gpu-a100", func(t *testing.T) {
		rg := newRig(t, shipped("plgrid-gpu-a100")...)
		shortStep(rg, mdSpec("md"))
		wantAllocated(t, rg, "md", "48h0m0s")
	})
}
