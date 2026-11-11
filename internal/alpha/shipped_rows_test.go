package alpha

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// HBEP-4: alpha uses only shipped profiles. Every row runs on a platform whose profile is a file
// of profiles/, and that profile lists the row's partition, so a row cannot be written for a
// partition no shipped profile describes. The profile for short test deployments on plgrid-now
// (profiles/athena-now.yaml, issue #376) is one of them, and the rows that will use it name it
// as a file of that directory, not as a profile the harness builds.
func TestEveryRowRunsOnAShippedProfileThatListsItsPartition(t *testing.T) {
	for _, r := range Rows() {
		t.Run(r.Name, func(t *testing.T) {
			spec := shippedProfile(t, r.Cluster) // reads profiles/<cluster>.yaml; a missing file fails
			listed := false
			for _, p := range spec.Partitions {
				listed = listed || p.Name == r.Partition
			}
			if !listed {
				t.Errorf("row %s runs on partition %s, which profiles/%s.yaml does not list", r.Name, r.Partition, r.Cluster)
			}
		})
	}
}

// The profile files that profiles/ ships include athena-now, and the live harness reads a profile
// by its file name (config.go: profiles/<name>.yaml), so the new profile is one a run can name.
func TestTheShippedAthenaNowProfileIsAFileTheHarnessCanName(t *testing.T) {
	if _, err := os.Stat(filepath.Join("..", "..", "profiles", "athena-now.yaml")); err != nil {
		t.Fatal(err)
	}
	spec := shippedProfile(t, "athena-now")
	if len(spec.Partitions) != 1 || spec.Partitions[0].Name != "plgrid-now" {
		t.Errorf("athena-now lists %+v; want plgrid-now only", spec.Partitions)
	}
	b, err := os.ReadFile(filepath.Join("..", "..", "profiles", "athena-now.yaml"))
	if err != nil || !strings.Contains(string(b), "name: athena-now") {
		t.Errorf("profiles/athena-now.yaml is not the PlatformProfile athena-now: %v", err)
	}
}
