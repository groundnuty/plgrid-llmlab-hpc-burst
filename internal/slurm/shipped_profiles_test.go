package slurm

import (
	"os"
	"path/filepath"
	"testing"

	"sigs.k8s.io/yaml"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The Slurm provider's own checks (HBEP-4, Core operations, ReadProfile) accept
// the alpha profiles that profiles/ ships, one Provider per alpha partition.
// The checks are the plugin's, not a comparison with the facts the values were
// copied from. The expected partitions come from the issue (#100): Helios plgrid
// and plgrid-gpu-gh200, Athena plgrid-gpu-a100.
func TestShippedProfilesPassReadProfile(t *testing.T) {
	shipped := []struct {
		file       string
		partitions []string
	}{
		{"helios.yaml", []string{"plgrid", "plgrid-gpu-gh200"}},
		{"athena.yaml", []string{"plgrid-gpu-a100"}},
	}
	for _, sp := range shipped {
		raw, err := os.ReadFile(filepath.Join("..", "..", "profiles", sp.file))
		if err != nil {
			t.Fatalf("read the shipped profile: %v", err) // a missing file fails; it never skips
		}
		var pp v1alpha1.PlatformProfile
		if err := yaml.UnmarshalStrict(raw, &pp); err != nil {
			t.Fatalf("%s: %v", sp.file, err)
		}
		listed := map[string]bool{}
		for _, p := range pp.Spec.Partitions {
			listed[p.Name] = true
		}
		if len(listed) != len(sp.partitions) {
			t.Errorf("%s lists partitions %v; want exactly %v", sp.file, listed, sp.partitions)
		}
		for _, part := range sp.partitions {
			t.Run(pp.Name+"/"+part, func(t *testing.T) {
				r := newProfileRig(t)
				r.pp = pp.DeepCopy()
				r.prov.Spec.PlatformProfileRef.Name = pp.Name
				r.prov.Spec.AllowedPartitions = []string{part}
				v, err := r.read()
				if err != nil {
					t.Fatalf("ReadProfile refuses the shipped profile: %v", err)
				}
				if v.Profile.Partition.Name != part {
					t.Errorf("the partition read is %q; want %q", v.Profile.Partition.Name, part)
				}
			})
		}
	}
}
