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

// The QOS by GPU count and the per-job QOS caps (HBEP-4 as amended by #745,
// from #735's Sano probes of 18 Nov 2026): a partition's qosByGpus, a QOS's
// maxGpusPerJob, maxCpusPerJob and maxMemPerJob.

// sanoQOS is the four QOS entries of Sano's gpu partition, listed in a
// profile's qos so a qosByGpus entry can name them.
func sanoQOS() []v1alpha1.QOSProfile {
	return []v1alpha1.QOSProfile{{Name: "gpu1"}, {Name: "gpu2"}, {Name: "gpu4"}, {Name: "gpu8"}}
}

func byGPUs(pairs ...any) []v1alpha1.QOSByGPUs {
	var out []v1alpha1.QOSByGPUs
	for i := 0; i < len(pairs); i += 2 {
		out = append(out, v1alpha1.QOSByGPUs{GPUs: int32(pairs[i].(int)), QOS: pairs[i+1].(string)})
	}
	return out
}

// Every qosByGpus entry's qos must be listed in the profile's qos (rule
// platformprofiles:$.spec#4, #745).
func TestQOSByGPUsQOSIsListed(t *testing.T) {
	const msg = "every qosByGpus qos must be listed in qos"
	refused := []struct {
		name   string
		mutate func(*v1alpha1.PlatformProfile)
	}{
		{"entries with no qos list", func(p *v1alpha1.PlatformProfile) {
			p.Spec.Partitions[0].QOSByGPUs = byGPUs(1, "gpu1")
		}},
		{"an entry the list lacks", func(p *v1alpha1.PlatformProfile) {
			p.Spec.QOS = []v1alpha1.QOSProfile{{Name: "gpu1"}, {Name: "gpu2"}}
			p.Spec.Partitions[0].QOSByGPUs = byGPUs(1, "gpu1", 2, "gpu2", 4, "gpu4")
		}},
		{"the second partition's entry unlisted", func(p *v1alpha1.PlatformProfile) {
			p.Spec.QOS = sanoQOS()
			p.Spec.Partitions[0].QOSByGPUs = byGPUs(1, "gpu1")
			p.Spec.Partitions[1].QOSByGPUs = byGPUs(1, "gpu1", 2, "gpu3")
		}},
		{"a name that differs only in case", func(p *v1alpha1.PlatformProfile) {
			p.Spec.QOS = []v1alpha1.QOSProfile{{Name: "GPU1"}}
			p.Spec.Partitions[0].QOSByGPUs = byGPUs(1, "gpu1")
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
		{"no qosByGpus and no list", func(*v1alpha1.PlatformProfile) {}},
		{"Sano's four entries", func(p *v1alpha1.PlatformProfile) {
			p.Spec.QOS = sanoQOS()
			p.Spec.Partitions[0].QOSByGPUs = byGPUs(1, "gpu1", 2, "gpu2", 4, "gpu4", 8, "gpu8")
		}},
		{"two partitions sharing listed qos names", func(p *v1alpha1.PlatformProfile) {
			p.Spec.QOS = sanoQOS()
			p.Spec.Partitions[0].QOSByGPUs = byGPUs(1, "gpu1", 8, "gpu8")
			p.Spec.Partitions[1].QOSByGPUs = byGPUs(2, "gpu2", 4, "gpu1")
		}},
		{"one QOS for two entries", func(p *v1alpha1.PlatformProfile) {
			p.Spec.QOS = sanoQOS()
			p.Spec.Partitions[0].QOSByGPUs = byGPUs(1, "gpu4", 2, "gpu4")
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

// A partition's qosByGpus is strictly increasing in gpus (rule
// platformprofiles:$.spec.partitions[]#1, #745), has at most 8 entries and each
// entry's gpus is at least 1 and qos is not empty.
func TestQOSByGPUsIsStrictlyIncreasing(t *testing.T) {
	const msg = "qosByGpus must be strictly increasing in gpus"
	profileWith := func(entries []v1alpha1.QOSByGPUs) *v1alpha1.PlatformProfile {
		p := newProfile(uniq("prof"))
		p.Spec.QOS = sanoQOS()
		p.Spec.Partitions[0].QOSByGPUs = entries
		return p
	}
	for _, tc := range []struct {
		name    string
		entries []v1alpha1.QOSByGPUs
	}{
		{"two entries of one count", byGPUs(1, "gpu1", 1, "gpu2")},
		{"a descending pair", byGPUs(2, "gpu2", 1, "gpu1")},
		{"a dip in the middle", byGPUs(1, "gpu1", 4, "gpu4", 2, "gpu2")},
		{"a repeat that is not adjacent", byGPUs(1, "gpu1", 2, "gpu2", 1, "gpu8")},
		{"a repeated last count", byGPUs(1, "gpu1", 8, "gpu4", 8, "gpu8")},
	} {
		t.Run("refuse "+tc.name, func(t *testing.T) {
			requireRefused(t, c.Create(bg(), profileWith(tc.entries)), msg)
		})
	}
	for _, tc := range []struct {
		name    string
		entries []v1alpha1.QOSByGPUs
	}{
		{"one entry", byGPUs(8, "gpu8")},
		{"Sano's four entries", byGPUs(1, "gpu1", 2, "gpu2", 4, "gpu4", 8, "gpu8")},
		{"gaps between the counts", byGPUs(1, "gpu1", 3, "gpu2", 100, "gpu8")},
	} {
		t.Run("accept "+tc.name, func(t *testing.T) {
			p := profileWith(tc.entries)
			must(t, c.Create(bg(), p), "create PlatformProfile")
			cleanup(t, p)
		})
	}

	// The schema's own bounds.
	for _, tc := range []struct {
		name    string
		entries []v1alpha1.QOSByGPUs
		want    string
	}{
		{"a count of 0", byGPUs(0, "gpu1"), "gpus"},
		{"a negative count", byGPUs(-1, "gpu1"), "gpus"},
		{"an empty qos", byGPUs(1, ""), "qos"},
	} {
		t.Run("refuse "+tc.name, func(t *testing.T) {
			requireRefused(t, c.Create(bg(), profileWith(tc.entries)), tc.want)
		})
	}
	var nine []v1alpha1.QOSByGPUs
	for i := range 9 {
		nine = append(nine, v1alpha1.QOSByGPUs{GPUs: int32(i + 1), QOS: "gpu1"})
	}
	t.Run("refuse more than 8 entries", func(t *testing.T) {
		requireRefused(t, c.Create(bg(), profileWith(nine)), "qosByGpus: Too many: 9: must have at most 8 items")
	})
	// The other side of the bound: exactly 8 entries on every partition the
	// profile may hold, and 8 QOS entries to name, so the CRD's cost estimate is
	// met by a real object.
	t.Run("accept exactly 8 entries on two partitions", func(t *testing.T) {
		p := newProfile(uniq("prof"))
		for i := range 8 {
			p.Spec.QOS = append(p.Spec.QOS, v1alpha1.QOSProfile{Name: "q" + strings.Repeat("x", i)})
		}
		var eight []v1alpha1.QOSByGPUs
		for i := range 8 {
			eight = append(eight, v1alpha1.QOSByGPUs{GPUs: int32(i + 1), QOS: p.Spec.QOS[7-i].Name})
		}
		p.Spec.Partitions[0].QOSByGPUs, p.Spec.Partitions[1].QOSByGPUs = eight, eight
		must(t, c.Create(bg(), p), "create PlatformProfile")
		cleanup(t, p)
	})
}

// qosByGpus and the per-job caps round-trip: an absent field stays absent.
func TestQOSByGPUsAndPerJobCapsRoundTrip(t *testing.T) {
	p := newProfile(uniq("prof"))
	p.Spec.QOS = []v1alpha1.QOSProfile{
		{Name: "gpu1", MaxGPUsPerJob: ptr.To(int32(1)), MaxCPUsPerJob: ptr.To(int32(12)), MaxMemPerJob: ptr.To(resource.MustParse("143360Mi"))},
		{Name: "gpu8"},
	}
	p.Spec.Partitions[0].QOSByGPUs = byGPUs(1, "gpu1", 8, "gpu8")
	must(t, c.Create(bg(), p), "create with qosByGpus and per-job caps")
	cleanup(t, p)
	var got v1alpha1.PlatformProfile
	must(t, c.Get(bg(), client.ObjectKeyFromObject(p), &got), "read back")
	g1, g8 := got.Spec.QOS[0], got.Spec.QOS[1]
	if g1.MaxGPUsPerJob == nil || *g1.MaxGPUsPerJob != 1 || g1.MaxCPUsPerJob == nil || *g1.MaxCPUsPerJob != 12 ||
		g1.MaxMemPerJob == nil || g1.MaxMemPerJob.Cmp(resource.MustParse("143360Mi")) != 0 {
		t.Errorf("gpu1 read back as %+v", g1)
	}
	if g8.MaxGPUsPerJob != nil || g8.MaxCPUsPerJob != nil || g8.MaxMemPerJob != nil {
		t.Errorf("gpu8 read back as %+v, want no per-job cap", g8)
	}
	if fmt.Sprint(got.Spec.Partitions[0].QOSByGPUs) != fmt.Sprint(byGPUs(1, "gpu1", 8, "gpu8")) || got.Spec.Partitions[1].QOSByGPUs != nil {
		t.Errorf("qosByGpus read back as %v and %v", got.Spec.Partitions[0].QOSByGPUs, got.Spec.Partitions[1].QOSByGPUs)
	}
}

// maxGpusPerJob, maxCpusPerJob and maxMemPerJob are positive when set (rules
// platformprofiles:$.spec.qos[].maxGpusPerJob#0, .maxCpusPerJob#0 and
// .maxMemPerJob#0, #745), as maxCpusPerUser and maxMemPerUser are.
func TestQOSMaxPerJobCapsArePositive(t *testing.T) {
	for _, n := range []int32{0, -1, -92} {
		t.Run(fmt.Sprintf("refuse maxGpusPerJob %d", n), func(t *testing.T) {
			p := newProfile(uniq("prof"))
			p.Spec.QOS = []v1alpha1.QOSProfile{{Name: "gpu1", MaxGPUsPerJob: ptr.To(n)}}
			requireRefused(t, c.Create(bg(), p), "maxGpusPerJob must be positive")
		})
		t.Run(fmt.Sprintf("refuse maxCpusPerJob %d", n), func(t *testing.T) {
			p := newProfile(uniq("prof"))
			p.Spec.QOS = []v1alpha1.QOSProfile{{Name: "gpu1", MaxCPUsPerJob: ptr.To(n)}}
			requireRefused(t, c.Create(bg(), p), "maxCpusPerJob must be positive")
		})
	}
	for _, n := range []int32{1, 12, 92} {
		t.Run(fmt.Sprintf("accept maxGpusPerJob and maxCpusPerJob %d", n), func(t *testing.T) {
			p := newProfile(uniq("prof"))
			p.Spec.QOS = []v1alpha1.QOSProfile{{Name: "gpu1", MaxGPUsPerJob: ptr.To(n), MaxCPUsPerJob: ptr.To(n)}}
			must(t, c.Create(bg(), p), "create with per-job caps")
			cleanup(t, p)
		})
	}
	for _, q := range []string{"-1Gi", "-1", "-140G", "0", "0Gi", "0.0"} {
		t.Run("refuse maxMemPerJob "+q, func(t *testing.T) {
			p := newProfile(uniq("prof"))
			p.Spec.QOS = []v1alpha1.QOSProfile{{Name: "gpu1", MaxMemPerJob: ptr.To(resource.MustParse(q))}}
			requireRefused(t, c.Create(bg(), p), "maxMemPerJob must be positive")
		})
	}
	for _, q := range []string{"1", "1Mi", "140G", "1087232Mi"} {
		t.Run("accept maxMemPerJob "+q, func(t *testing.T) {
			p := newProfile(uniq("prof"))
			p.Spec.QOS = []v1alpha1.QOSProfile{{Name: "gpu1", MaxMemPerJob: ptr.To(resource.MustParse(q))}}
			must(t, c.Create(bg(), p), "create with maxMemPerJob "+q)
			cleanup(t, p)
		})
	}
}
