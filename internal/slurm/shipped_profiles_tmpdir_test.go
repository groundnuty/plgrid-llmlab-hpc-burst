package slurm

import (
	"os"
	"path/filepath"
	"testing"

	"sigs.k8s.io/yaml"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// On Helios GH200 the job's own TMPDIR is on Lustre, where the agent's run
// directory and Apptainer's layer cache and build tree would put tens of
// thousands of inodes on a scratch quota that is already near its inode limit
// (issue #94, finding 1). The shipped profile therefore names the job's RAM
// disk, $MEMFS, as the job's temporary directory, and the node feature that
// gives a job that disk, memfs; the node-local disk's localfs is refused at
// submission (`@hpc-burst-science-agent`, 1 Nov 2026, option a; HBEP-4,
// Platform profile schema). The expected values are the ruling's, not read back from the
// file. They are read through the provider's own ReadProfile, so the check is of
// what the Slurm provider sees of the shipped partition.
func TestShippedHeliosGH200HasTheNodeLocalTmpDir(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "profiles", "helios.yaml"))
	if err != nil {
		t.Fatalf("read the shipped profile: %v", err) // a missing file fails; it never skips
	}
	var pp v1alpha1.PlatformProfile
	if err := yaml.UnmarshalStrict(raw, &pp); err != nil {
		t.Fatal(err)
	}
	const partition = "plgrid-gpu-gh200"
	r := newProfileRig(t)
	r.pp = pp.DeepCopy()
	r.prov.Spec.PlatformProfileRef.Name = pp.Name
	r.prov.Spec.AllowedPartitions = []string{partition}
	v, err := r.read()
	if err != nil {
		t.Fatalf("ReadProfile refuses the shipped profile: %v", err)
	}
	if got := v.Profile.Partition.Name; got != partition {
		t.Fatalf("the partition read is %q; want %q", got, partition)
	}
	if got, want := v.Profile.Partition.TmpDir, "$MEMFS"; got != want {
		t.Errorf("%s tmpDir is %q; want %q", partition, got, want)
	}
	if got, want := v.Profile.Partition.TmpDirFeature, "memfs"; got != want {
		t.Errorf("%s tmpDirFeature is %q; want %q", partition, got, want)
	}
}
