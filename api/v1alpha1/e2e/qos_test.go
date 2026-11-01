package e2e

import (
	"fmt"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The QOS fields of HBEP-4's amendment of 1 Nov 2026 (QOS limits; Platform
// profile schema; Provider settings): a Provider's qos, a partition's own qos,
// and the profile's list of QOSes with their limits.

// A partition's qos must be listed in the profile's qos, so the plan knows
// every limit it names (rule platformprofiles:$.spec#1).
func TestPartitionQOSIsListed(t *testing.T) {
	const msg = "every partition's qos must be listed in qos"
	refused := []struct {
		name   string
		mutate func(*v1alpha1.PlatformProfile)
	}{
		{"a partition qos with no qos list", func(p *v1alpha1.PlatformProfile) { p.Spec.Partitions[0].QOS = "part-qos" }},
		{"a partition qos the list lacks", func(p *v1alpha1.PlatformProfile) {
			p.Spec.QOS = []v1alpha1.QOSProfile{{Name: "fast"}}
			p.Spec.Partitions[0].QOS = "part-qos"
		}},
		{"the second partition's qos unlisted", func(p *v1alpha1.PlatformProfile) {
			p.Spec.QOS = []v1alpha1.QOSProfile{{Name: "part-qos"}}
			p.Spec.Partitions[0].QOS = "part-qos"
			p.Spec.Partitions[1].QOS = "other-qos"
		}},
		{"a name that differs only in case", func(p *v1alpha1.PlatformProfile) {
			p.Spec.QOS = []v1alpha1.QOSProfile{{Name: "Part-QOS"}}
			p.Spec.Partitions[0].QOS = "part-qos"
		}},
	}
	for _, tc := range refused {
		t.Run("refuse "+tc.name, func(t *testing.T) {
			p := newProfile(uniq("prof"))
			tc.mutate(p)
			requireRefused(t, c.Create(bg(), p), msg)
		})
	}
	accepted := []struct {
		name   string
		mutate func(*v1alpha1.PlatformProfile)
	}{
		{"no partition qos and no list", func(*v1alpha1.PlatformProfile) {}},
		{"a list and no partition qos", func(p *v1alpha1.PlatformProfile) { p.Spec.QOS = []v1alpha1.QOSProfile{{Name: "fast"}} }},
		{"a listed partition qos", func(p *v1alpha1.PlatformProfile) {
			p.Spec.QOS = []v1alpha1.QOSProfile{{Name: "fast"}, {Name: "part-qos"}}
			p.Spec.Partitions[0].QOS = "part-qos"
		}},
		{"two partitions sharing one listed qos", func(p *v1alpha1.PlatformProfile) {
			p.Spec.QOS = []v1alpha1.QOSProfile{{Name: "part-qos"}}
			p.Spec.Partitions[0].QOS, p.Spec.Partitions[1].QOS = "part-qos", "part-qos"
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

// maxMemPerUser is positive when set (rule
// platformprofiles:$.spec.qos[].maxMemPerUser#0; `@hpc-burst-science-agent`,
// 1 Nov 2026, from PR #155's review): zero in any form and a negative value
// are refused.
func TestQOSMaxMemPerUserIsPositive(t *testing.T) {
	const msg = "maxMemPerUser must be positive"
	for _, q := range []string{"-1Gi", "-1", "-125G", "0", "0Gi", "0.0"} {
		t.Run("refuse "+q, func(t *testing.T) {
			p := newProfile(uniq("prof"))
			p.Spec.QOS = []v1alpha1.QOSProfile{{Name: "fast", MaxMemPerUser: ptr.To(resource.MustParse(q))}}
			requireRefused(t, c.Create(bg(), p), msg)
		})
	}
	for _, q := range []string{"1", "1Mi", "125G", "122400Mi"} {
		t.Run("accept "+q, func(t *testing.T) {
			p := newProfile(uniq("prof"))
			p.Spec.QOS = []v1alpha1.QOSProfile{{Name: "fast", MaxMemPerUser: ptr.To(resource.MustParse(q))}}
			must(t, c.Create(bg(), p), "create with maxMemPerUser "+q)
			cleanup(t, p)
		})
	}
}

// maxCpusPerUser is positive when set (rule
// platformprofiles:$.spec.qos[].maxCpusPerUser#0; `@hpc-burst-science-agent`,
// 1 Nov 2026, from PR #155's review).
func TestQOSMaxCpusPerUserIsPositive(t *testing.T) {
	const msg = "maxCpusPerUser must be positive"
	for _, n := range []int32{0, -1, -72} {
		t.Run(fmt.Sprintf("refuse %d", n), func(t *testing.T) {
			p := newProfile(uniq("prof"))
			p.Spec.QOS = []v1alpha1.QOSProfile{{Name: "fast", MaxCPUsPerUser: ptr.To(n)}}
			requireRefused(t, c.Create(bg(), p), msg)
		})
	}
	for _, n := range []int32{1, 16, 72} {
		t.Run(fmt.Sprintf("accept %d", n), func(t *testing.T) {
			p := newProfile(uniq("prof"))
			p.Spec.QOS = []v1alpha1.QOSProfile{{Name: "fast", MaxCPUsPerUser: ptr.To(n)}}
			must(t, c.Create(bg(), p), fmt.Sprintf("create with maxCpusPerUser %d", n))
			cleanup(t, p)
		})
	}
}

// The QOS list round-trips, every limit optional: an absent limit stays
// absent, overPartQos defaults to false, and two entries for one name are
// refused (a map list keyed by name). The counts are non-negative.
func TestQOSListRoundTrips(t *testing.T) {
	p := newProfile(uniq("prof"))
	p.Spec.QOS = []v1alpha1.QOSProfile{
		{Name: "fast", MaxWall: ptr.To(v1alpha1.Duration("12h")), MaxSubmitPerUser: ptr.To(int32(1)), MaxGPUsPerUser: ptr.To(int32(1)),
			MaxCPUsPerUser: ptr.To(int32(72)), MaxMemPerUser: ptr.To(resource.MustParse("122400Mi"))},
		{Name: "part-qos", MaxSubmitPerUser: ptr.To(int32(2000)), OverPartQOS: true},
		{Name: "plain"},
	}
	p.Spec.Partitions[0].QOS = "part-qos"
	must(t, c.Create(bg(), p), "create with a QOS list")
	cleanup(t, p)
	var got v1alpha1.PlatformProfile
	must(t, c.Get(bg(), client.ObjectKeyFromObject(p), &got), "read back")
	if len(got.Spec.QOS) != 3 {
		t.Fatalf("read back %d QOS entries, want 3", len(got.Spec.QOS))
	}
	fast, part, plain := got.Spec.QOS[0], got.Spec.QOS[1], got.Spec.QOS[2]
	if fast.Name != "fast" || *fast.MaxWall != "12h" || *fast.MaxSubmitPerUser != 1 || *fast.MaxGPUsPerUser != 1 || *fast.MaxCPUsPerUser != 72 ||
		fast.MaxMemPerUser.Cmp(resource.MustParse("122400Mi")) != 0 || fast.OverPartQOS {
		t.Errorf("fast read back as %+v", fast)
	}
	if part.Name != "part-qos" || *part.MaxSubmitPerUser != 2000 || !part.OverPartQOS || part.MaxWall != nil || part.MaxGPUsPerUser != nil {
		t.Errorf("part-qos read back as %+v", part)
	}
	if plain.Name != "plain" || plain.MaxWall != nil || plain.MaxSubmitPerUser != nil || plain.MaxGPUsPerUser != nil ||
		plain.MaxCPUsPerUser != nil || plain.MaxMemPerUser != nil || plain.OverPartQOS {
		t.Errorf("plain read back as %+v, want only its name", plain)
	}
	if got.Spec.Partitions[0].QOS != "part-qos" || got.Spec.Partitions[1].QOS != "" {
		t.Errorf("partition qos read back as %q and %q", got.Spec.Partitions[0].QOS, got.Spec.Partitions[1].QOS)
	}

	refused := []struct {
		name   string
		mutate func(*v1alpha1.PlatformProfile)
		want   string
	}{
		{"two entries for one QOS", func(p *v1alpha1.PlatformProfile) {
			p.Spec.QOS = []v1alpha1.QOSProfile{{Name: "fast"}, {Name: "fast"}}
		}, "Duplicate value"},
		{"a QOS without a name", func(p *v1alpha1.PlatformProfile) { p.Spec.QOS = []v1alpha1.QOSProfile{{}} }, "name"},
		{"a negative submit limit", func(p *v1alpha1.PlatformProfile) {
			p.Spec.QOS = []v1alpha1.QOSProfile{{Name: "fast", MaxSubmitPerUser: ptr.To(int32(-1))}}
		}, "maxSubmitPerUser"},
		{"a negative GPU cap", func(p *v1alpha1.PlatformProfile) {
			p.Spec.QOS = []v1alpha1.QOSProfile{{Name: "fast", MaxGPUsPerUser: ptr.To(int32(-1))}}
		}, "maxGpusPerUser"},
		{"a QOS name longer than 64 characters", func(p *v1alpha1.PlatformProfile) {
			p.Spec.QOS = []v1alpha1.QOSProfile{{Name: strings.Repeat("q", 65)}}
		}, "name"},
		{"more than 8 QOS entries", func(p *v1alpha1.PlatformProfile) {
			for i := range 9 {
				p.Spec.QOS = append(p.Spec.QOS, v1alpha1.QOSProfile{Name: "q" + strings.Repeat("x", i)})
			}
		}, "spec.qos: Too many: 9: must have at most 8 items"},
	}
	for _, tc := range refused {
		t.Run("refuse "+tc.name, func(t *testing.T) {
			p := newProfile(uniq("prof"))
			tc.mutate(p)
			requireRefused(t, c.Create(bg(), p), tc.want)
		})
	}

	// The other side of each bound: exactly 8 entries, each with every limit
	// set, so the CRD's cost estimate is met by real objects, and a submit limit
	// and a GPU cap of 0, which stay valid (an effective 0 means no
	// Allocation can be hosted).
	for _, tc := range []struct {
		name   string
		mutate func(*v1alpha1.PlatformProfile)
	}{
		{"exactly 8 QOS entries", func(p *v1alpha1.PlatformProfile) {
			for i := range 8 {
				p.Spec.QOS = append(p.Spec.QOS, v1alpha1.QOSProfile{Name: "q" + strings.Repeat("x", i),
					MaxWall: ptr.To(v1alpha1.Duration("12h")), MaxSubmitPerUser: ptr.To(int32(1)), MaxGPUsPerUser: ptr.To(int32(1)),
					MaxCPUsPerUser: ptr.To(int32(72)), MaxMemPerUser: ptr.To(resource.MustParse("122400Mi"))})
			}
		}},
		{"a submit limit and a GPU cap of 0", func(p *v1alpha1.PlatformProfile) {
			p.Spec.QOS = []v1alpha1.QOSProfile{{Name: "fast", MaxSubmitPerUser: ptr.To(int32(0)), MaxGPUsPerUser: ptr.To(int32(0))}}
		}},
	} {
		t.Run("accept "+tc.name, func(t *testing.T) {
			p := newProfile(uniq("prof"))
			tc.mutate(p)
			must(t, c.Create(bg(), p), "create PlatformProfile")
			cleanup(t, p)
		})
	}
}

// A Provider names its QOS in its settings; with none, the field stays absent
// (HBEP-4, Provider settings).
func TestProviderQOS(t *testing.T) {
	p := newProvider(uniq("prov"))
	p.Spec.QOS = "fast"
	must(t, c.Create(bg(), p), "create a Provider with a qos")
	cleanup(t, p)
	var got v1alpha1.Provider
	must(t, c.Get(bg(), client.ObjectKeyFromObject(p), &got), "read back")
	if got.Spec.QOS != "fast" {
		t.Errorf("qos read back as %q, want fast", got.Spec.QOS)
	}

	none := newProvider(uniq("prov"))
	must(t, c.Create(bg(), none), "create a Provider without a qos")
	cleanup(t, none)
	must(t, c.Get(bg(), client.ObjectKeyFromObject(none), &got), "read back")
	if got.Spec.QOS != "" {
		t.Errorf("an absent qos read back as %q", got.Spec.QOS)
	}

	long := newProvider(uniq("prov"))
	long.Spec.QOS = strings.Repeat("q", 65)
	requireRefused(t, c.Create(bg(), long), "qos")
}
