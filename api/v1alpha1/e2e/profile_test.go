package e2e

import (
	"testing"

	"k8s.io/utils/ptr"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

func TestPlatformProfileTypedFields(t *testing.T) {
	refused := []struct {
		name   string
		mutate func(*v1alpha1.PlatformProfile)
		want   string
	}{
		{"unknown kind", func(p *v1alpha1.PlatformProfile) { p.Spec.Kind = "cloud" }, "Unsupported value"},
		{"lost_grace is not a duration", func(p *v1alpha1.PlatformProfile) { p.Spec.LostGrace = ptr.To(v1alpha1.Duration("soon")) }, "lost_grace"},
		{"inflight_window is not a duration", func(p *v1alpha1.PlatformProfile) { p.Spec.InflightWindow = ptr.To(v1alpha1.Duration("5")) }, "inflight_window"},
		{"unknown_bound is not a duration", func(p *v1alpha1.PlatformProfile) { p.Spec.UnknownBound = ptr.To(v1alpha1.Duration("10 minutes")) }, "unknown_bound"},
		{"partition without a GPU model", func(p *v1alpha1.PlatformProfile) { p.Spec.Partitions[0].GPUModel = "" }, "gpu_model"},
		{"partition without a CPU architecture", func(p *v1alpha1.PlatformProfile) { p.Spec.Partitions[0].CPUArch = "" }, "cpu_arch"},
		{"two entries for one partition", func(p *v1alpha1.PlatformProfile) { p.Spec.Partitions[1].Name = p.Spec.Partitions[0].Name }, "Duplicate value"},
	}
	for _, tc := range refused {
		t.Run("refuse "+tc.name, func(t *testing.T) {
			p := newProfile(uniq("prof"))
			tc.mutate(p)
			requireRefused(t, c.Create(bg(), p), tc.want)
		})
	}
	accepted := []struct {
		name   string
		mutate func(*v1alpha1.PlatformProfile)
	}{
		{"the base profile", func(*v1alpha1.PlatformProfile) {}},
		{"every typed field set", func(p *v1alpha1.PlatformProfile) {
			p.Spec.HoldRelease = true
			p.Spec.LostGrace = ptr.To(v1alpha1.Duration("90s"))
			p.Spec.InflightWindow = ptr.To(v1alpha1.Duration("1m30s"))
			p.Spec.UnknownBound = ptr.To(v1alpha1.Duration("15m"))
		}},
		{"a managed platform without partitions", func(p *v1alpha1.PlatformProfile) {
			p.Spec.Kind, p.Spec.Partitions = v1alpha1.PlatformManaged, nil
		}},
	}
	for _, tc := range accepted {
		t.Run("accept "+tc.name, func(t *testing.T) {
			p := newProfile(uniq("prof"))
			tc.mutate(p)
			must(t, c.Create(bg(), p), "create PlatformProfile")
			cleanup(t, p)
		})
	}
}

// The untyped block keeps whatever the profile holds beyond the typed fields.
func TestPlatformProfileKeepsTheUntypedBlock(t *testing.T) {
	p := newProfile(uniq("prof"))
	p.Spec.Profile = jsonOf(t, `{"time_zone":"Europe/Warsaw","requeue_default":true,"nested":{"a":[1,2,3]}}`)
	must(t, c.Create(bg(), p), "create PlatformProfile with a profile block")
	cleanup(t, p)
	got := &v1alpha1.PlatformProfile{}
	must(t, c.Get(bg(), objKey(p), got), "get")
	if got.Spec.Profile == nil || string(got.Spec.Profile.Raw) != `{"nested":{"a":[1,2,3]},"requeue_default":true,"time_zone":"Europe/Warsaw"}` {
		t.Fatalf("the untyped profile block was pruned or changed: %v", got.Spec.Profile)
	}
}
