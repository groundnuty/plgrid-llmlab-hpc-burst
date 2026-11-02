package slurm

// The partition QOS of the shipped profiles (HBEP-4, QOS limits). Slurm applies
// a partition's own QOS to every job on it, before the job's QOS, so a profile
// that omits it plans with the wrong submit limit (PR 168's review, Opus N8).
// Expected values are written out from the measurements the profiles cite
// (sacctmgr, 30 Oct 2026 for the facts; issue #94, comment 6557416899 for
// Helios's plgrid), never read from the profile.

import (
	"os"
	"path/filepath"
	"testing"

	"sigs.k8s.io/yaml"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

func TestShippedPartitionsTypeTheirOwnQOS(t *testing.T) {
	for _, tc := range []struct {
		file, partition string
		providerQOS     string
		wantPartQOS     string
		submit          int32  // the effective maxSubmitPerUser
		submitFrom      string // the QOS that sets it
	}{
		// Helios's CPU partition: plgrid's 2000 comes before now's 1.
		{"helios.yaml", "plgrid", "now", "plgrid", 2000, "plgrid"},
		{"helios.yaml", "plgrid", "", "plgrid", 2000, "plgrid"},
		// Helios's GPU partition, the control that already had it.
		{"helios.yaml", "plgrid-gpu-gh200", "now", "plgrid", 2000, "plgrid"},
		// Athena's A100 partition: plgrid allows 1000, whatever the Provider's qos.
		{"athena.yaml", "plgrid-gpu-a100", "", "plgrid", 1000, "plgrid"},
		// Athena's one-job partition keeps its own QOS.
		{"athena.yaml", "plgrid-now", "", "plgrid-now", 1, "plgrid-now"},
	} {
		t.Run(tc.file+"/"+tc.partition+"/qos="+tc.providerQOS, func(t *testing.T) {
			raw, err := os.ReadFile(filepath.Join("..", "..", "profiles", tc.file))
			if err != nil {
				t.Fatal(err)
			}
			var pp v1alpha1.PlatformProfile
			if err := yaml.UnmarshalStrict(raw, &pp); err != nil {
				t.Fatal(err)
			}
			var part *v1alpha1.PartitionProfile
			for i := range pp.Spec.Partitions {
				if pp.Spec.Partitions[i].Name == tc.partition {
					part = &pp.Spec.Partitions[i]
				}
			}
			if part == nil {
				t.Fatalf("%s lists no partition %s", tc.file, tc.partition)
			}
			if part.QOS != tc.wantPartQOS {
				t.Fatalf("partition %s names the partition QOS %q, want %q", tc.partition, part.QOS, tc.wantPartQOS)
			}
			limits, err := v1alpha1.EffectiveQOSLimits(&pp.Spec, *part, tc.providerQOS)
			if err != nil {
				t.Fatal(err)
			}
			if limits.MaxSubmitPerUser == nil || *limits.MaxSubmitPerUser != tc.submit || limits.From[v1alpha1.LimitMaxSubmitPerUser] != tc.submitFrom {
				t.Errorf("the submit limit is %v from %q, want %d from %q", limits.MaxSubmitPerUser, limits.From[v1alpha1.LimitMaxSubmitPerUser], tc.submit, tc.submitFrom)
			}
		})
	}
}
