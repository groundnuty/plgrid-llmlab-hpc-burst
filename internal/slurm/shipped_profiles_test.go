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
// and plgrid-gpu-gh200, Athena plgrid-gpu-a100; and from the science agent's
// ruling of 1 Nov 2026 on issue #93 (comment 6557245889), which adds Athena
// plgrid-now for conformance runs and short checks.
func TestShippedProfilesPassReadProfile(t *testing.T) {
	shipped := []struct {
		file       string
		partitions []string
	}{
		{"helios.yaml", []string{"plgrid", "plgrid-gpu-gh200"}},
		{"athena.yaml", []string{"plgrid-gpu-a100", "plgrid-now"}},
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

// Athena plgrid-now is a short-job lane (issue #93, `@hpc-burst-science-agent`,
// 1 Nov 2026): the partition's time limit is 1 h, and its QoS caps a user at one
// GPU (cpu=16,gres/gpu=1,mem=125G, one running and one submitted job). The
// expected values are those two measured limits, written out here. A request
// the partition's limits forbid is refused before any call; the control is the
// same request on plgrid-gpu-a100, whose profile allows 8 GPUs and 48 h, so
// the refusal comes from the plgrid-now entry.
func TestShippedAthenaNowRefusesWhatItsLimitsForbid(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "profiles", "athena.yaml"))
	if err != nil {
		t.Fatalf("read the shipped profile: %v", err)
	}
	var pp v1alpha1.PlatformProfile
	if err := yaml.UnmarshalStrict(raw, &pp); err != nil {
		t.Fatal(err)
	}
	check := func(partition string, gpus int32, limit v1alpha1.Duration) error {
		var entry v1alpha1.PartitionProfile
		for _, p := range pp.Spec.Partitions {
			if p.Name == partition {
				entry = p
			}
		}
		if entry.Name == "" {
			t.Fatalf("athena.yaml does not list %s", partition)
		}
		req := provider.SubmitRequest{
			Tag: "hb-example-0123456789ab", Hold: true, Account: "example-account",
			Params: v1alpha1.BatchParameters{
				Partition: partition, TimeLimit: limit, GPUs: gpus, CPUsPerTask: 4,
				Memory: resource.MustParse("32Gi"), Nodes: 1, AgentDigest: strings.Repeat("a", 64),
			},
		}
		return CheckRequest(req, provider.Profile{Name: pp.Name, Spec: pp.Spec, Partition: entry},
			provider.Settings{Provider: "example", Account: "example-account", Partition: partition})
	}
	// The conformance run's shape and the longest the partition allows are accepted.
	for _, limit := range []v1alpha1.Duration{"5m", "1h"} {
		if err := check("plgrid-now", 1, limit); err != nil {
			t.Errorf("1 GPU for %s on plgrid-now is refused: %v", limit, err)
		}
	}
	for _, tc := range []struct {
		name  string
		gpus  int32
		limit v1alpha1.Duration
		want  string
	}{
		{"two GPUs", 2, "5m", "gpusPerNode 1"},
		{"a time limit above 1 h", 1, "61m", "maxTime 1h0m0s"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := check("plgrid-gpu-a100", tc.gpus, tc.limit); err != nil {
				t.Fatalf("the same request on plgrid-gpu-a100 is refused, so the control proves nothing: %v", err)
			}
			err := check("plgrid-now", tc.gpus, tc.limit)
			if !errors.Is(err, provider.ErrInvalidRequest) || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("got %v; want a refusal wrapping ErrInvalidRequest that names %s", err, tc.want)
			}
		})
	}
}

// The job-name rules of the shipped profiles (issue #93). Helios has measured
// them: the conformance run on plgrid of 31 Oct 2026, step 5, a 56-character
// name read back unchanged from squeue and sacct. The schema holds the rules
// per profile, not per partition, so the value stands for plgrid-gpu-gh200
// too (`@hpc-burst-science-agent`, option (b), 1 Nov 2026). Athena has not
// run the step, and its rules stay unset: 56 is neither a fact nor a default
// there. The values are the ruling's, written out here.
func TestShippedProfilesJobNameRules(t *testing.T) {
	for _, tc := range []struct {
		file      string
		maxLength int32 // 0: unset
	}{
		{"helios.yaml", 56},
		{"athena.yaml", 0},
	} {
		raw, err := os.ReadFile(filepath.Join("..", "..", "profiles", tc.file))
		if err != nil {
			t.Fatalf("read the shipped profile: %v", err) // a missing file fails; it never skips
		}
		var pp v1alpha1.PlatformProfile
		if err := yaml.UnmarshalStrict(raw, &pp); err != nil {
			t.Fatalf("%s: %v", tc.file, err)
		}
		rules := pp.Spec.JobName
		switch {
		case tc.maxLength == 0 && rules != nil && rules.MaxLength != nil:
			t.Errorf("%s: jobName.maxLength is %d; want it unset until a run on that platform measures it", tc.file, *rules.MaxLength)
		case tc.maxLength != 0 && (rules == nil || rules.MaxLength == nil || *rules.MaxLength != tc.maxLength):
			t.Errorf("%s: jobName = %+v; want maxLength %d", tc.file, rules, tc.maxLength)
		}
		if rules != nil && rules.Pattern != "" {
			t.Errorf("%s: jobName.pattern is %q; no fact states a pattern", tc.file, rules.Pattern)
		}
	}
}
