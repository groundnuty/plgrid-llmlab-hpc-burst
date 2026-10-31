package slurm

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/yaml"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
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

// Helios plgrid is a CPU partition for conformance runs only and hosts no
// Instance (`@hpc-burst-science-agent`, 31 Oct 2026). What keeps a served
// deployment off it today is this provider check: the shipped profile gives the
// partition gpusPerNode 0, so every request for a GPU is refused before any
// call. Each refusal is paired with the same request, with the same GPUs, CPUs
// and memory, on plgrid-gpu-gh200, which is accepted: the request differs only in
// the partition, so the refusal comes from the partition's entry.
//
// A request for no GPU is not refused here: the partition's gpuModel is none, and
// the conformance runs submit CPU jobs. That case is outside this test.
func TestShippedCPUPartitionRefusesGPUAllocations(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "profiles", "helios.yaml"))
	if err != nil {
		t.Fatalf("read the shipped profile: %v", err)
	}
	var pp v1alpha1.PlatformProfile
	if err := yaml.UnmarshalStrict(raw, &pp); err != nil {
		t.Fatal(err)
	}
	// check submits a request of gpus GPUs on a partition. The CPUs and the memory
	// are those of gpus GH200 shares (72 CPUs and 122375Mi each), whatever the
	// partition, so two partitions get the same request.
	check := func(partition string, gpus int32) error {
		var entry v1alpha1.PartitionProfile
		for _, p := range pp.Spec.Partitions {
			if p.Name == partition {
				entry = p
			}
		}
		if entry.Name == "" {
			t.Fatalf("helios.yaml does not list %s", partition)
		}
		mem := resource.MustParse("122375Mi")
		mem.Mul(int64(gpus))
		req := provider.SubmitRequest{
			Tag: "hb-example-0123456789ab", Hold: true, Account: "example-account",
			Params: v1alpha1.BatchParameters{
				Partition: partition, TimeLimit: "1h", GPUs: gpus, CPUsPerTask: 72 * gpus,
				Memory: mem, Nodes: 1, AgentDigest: strings.Repeat("a", 64),
			},
		}
		return CheckRequest(req, provider.Profile{Name: pp.Name, Spec: pp.Spec, Partition: entry},
			provider.Settings{Provider: "example", Account: "example-account", Partition: partition})
	}

	for _, gpus := range []int32{1, 2, 4} {
		t.Run(fmt.Sprintf("%d GPUs", gpus), func(t *testing.T) {
			if err := check("plgrid-gpu-gh200", gpus); err != nil {
				t.Fatalf("the same request on plgrid-gpu-gh200 is refused, so the control proves nothing: %v", err)
			}
			err := check("plgrid", gpus)
			if !errors.Is(err, provider.ErrInvalidRequest) || !strings.Contains(err.Error(), "gpusPerNode 0") {
				t.Errorf("got %v; want a refusal wrapping ErrInvalidRequest that names gpusPerNode 0", err)
			}
		})
	}
}
