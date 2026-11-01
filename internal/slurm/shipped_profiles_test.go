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
// 1 Nov 2026): the partition's time limit is 1 h, and its gpusPerNode is the
// node's 8, since the field is a node fact. The QoS cap of one GPU per user is
// typed (qos plgrid-now, maxGpusPerUser), and TestShippedProfilesTypeTheirQOS
// checks the refusal of a second GPU; the absent per-GPU sizing keeps
// Instances off the partition (internal/lifecycle,
// TestShippedAthenaNowCannotBeSized). The expected values are the measured
// time limit and the node's GPU count, written out here. A
// request above the time limit is refused before any call; the control is the
// same request on plgrid-gpu-a100, whose profile allows 48 h, so the refusal
// comes from the plgrid-now entry.
func TestShippedAthenaNowRefusesATimeLimitAboveItsMaxTime(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "profiles", "athena.yaml"))
	if err != nil {
		t.Fatalf("read the shipped profile: %v", err)
	}
	var pp v1alpha1.PlatformProfile
	if err := yaml.UnmarshalStrict(raw, &pp); err != nil {
		t.Fatal(err)
	}
	entryOf := func(partition string) v1alpha1.PartitionProfile {
		for _, p := range pp.Spec.Partitions {
			if p.Name == partition {
				return p
			}
		}
		t.Fatalf("athena.yaml does not list %s", partition)
		return v1alpha1.PartitionProfile{}
	}
	if n := entryOf("plgrid-now").GPUsPerNode; n == nil || *n != 8 {
		t.Errorf("plgrid-now gpusPerNode = %v; want the node's 8", n)
	}
	check := func(partition string, limit v1alpha1.Duration) error {
		req := provider.SubmitRequest{
			Tag: "hb-example-0123456789ab", Hold: true, Account: "example-account",
			Params: v1alpha1.BatchParameters{
				Partition: partition, TimeLimit: limit, GPUs: 1, CPUsPerTask: 4,
				Memory: resource.MustParse("32Gi"), Nodes: 1, AgentDigest: strings.Repeat("a", 64),
			},
		}
		return CheckRequest(req, provider.Profile{Name: pp.Name, Spec: pp.Spec, Partition: entryOf(partition)},
			provider.Settings{Provider: "example", Account: "example-account", Partition: partition})
	}
	// The conformance run's shape and the longest the partition allows are accepted.
	for _, limit := range []v1alpha1.Duration{"5m", "1h"} {
		if err := check("plgrid-now", limit); err != nil {
			t.Errorf("1 GPU for %s on plgrid-now is refused: %v", limit, err)
		}
	}
	if err := check("plgrid-gpu-a100", "61m"); err != nil {
		t.Fatalf("the same request on plgrid-gpu-a100 is refused, so the control proves nothing: %v", err)
	}
	err = check("plgrid-now", "61m")
	if !errors.Is(err, provider.ErrInvalidRequest) || !strings.Contains(err.Error(), "maxTime 1h0m0s") {
		t.Errorf("got %v; want a refusal wrapping ErrInvalidRequest that names maxTime 1h0m0s", err)
	}
}

// The job-name rules of the shipped profiles (issue #93). Helios has measured
// them: the conformance run on plgrid of 31 Oct 2026, step 5, a 56-character
// name read back unchanged from squeue and sacct. The schema holds the rules
// per profile, not per partition, so the value stands for plgrid-gpu-gh200
// too (`@hpc-burst-science-agent`, option (b), 1 Nov 2026). Athena measured
// the same on plgrid-now, 1 Nov 2026 (issue #93), and the value stands for
// plgrid-gpu-a100 on the same ruling. The values are the runs', written out
// here.
func TestShippedProfilesJobNameRules(t *testing.T) {
	for _, tc := range []struct {
		file      string
		maxLength int32 // 0: unset
	}{
		{"helios.yaml", 56},
		{"athena.yaml", 56},
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

// The typed QOS limits of the shipped profiles (HBEP-4, QOS limits), with the
// Provider's qos that each alpha partition names. The expected numbers are
// written out from the measurements the HBEP cites (the QOS now: 12 h, one
// submitted job, one GPU, 72 CPUs, 122,400 MiB; plgrid-now: no wall limit, one
// job, 1 GPU, 16 CPUs, 125 GiB, which is 128,000 MiB), never read from the
// profile. Each refusal is paired with a control that the same code accepts,
// so the refusal comes from the one limit changed.
func TestShippedProfilesTypeTheirQOS(t *testing.T) {
	load := func(file string) v1alpha1.PlatformProfile {
		raw, err := os.ReadFile(filepath.Join("..", "..", "profiles", file))
		if err != nil {
			t.Fatalf("read the shipped profile: %v", err)
		}
		var pp v1alpha1.PlatformProfile
		if err := yaml.UnmarshalStrict(raw, &pp); err != nil {
			t.Fatalf("%s: %v", file, err)
		}
		return pp
	}
	type shape struct {
		gpus, cpus int32
		mem, limit string
	}
	for _, tc := range []struct {
		file, partition, qos string
		atCaps               shape // accepted: exactly at every cap
		refused              []struct {
			name string
			s    shape
			want string // named by the refusal
		}
	}{
		{"helios.yaml", "plgrid-gpu-gh200", "now", shape{1, 72, "122400Mi", "12h"}, []struct {
			name string
			s    shape
			want string
		}{
			{"12h and a minute", shape{1, 72, "122375Mi", "12h1m"}, "maxWall"},
			{"two GPUs", shape{2, 72, "122375Mi", "12h"}, "maxGpusPerUser"},
			{"73 CPUs", shape{1, 73, "122375Mi", "12h"}, "maxCpusPerUser"},
			{"one MiB over the memory", shape{1, 72, "122401Mi", "12h"}, "maxMemPerUser"},
		}},
		{"athena.yaml", "plgrid-now", "plgrid-now", shape{1, 16, "125Gi", "1h"}, []struct {
			name string
			s    shape
			want string
		}{
			{"an hour and a minute", shape{1, 16, "125Gi", "61m"}, "maxTime"},
			{"two GPUs of the node's eight", shape{2, 16, "125Gi", "1h"}, "maxGpusPerUser"},
			{"17 CPUs", shape{1, 17, "125Gi", "1h"}, "maxCpusPerUser"},
			{"one MiB over 125Gi", shape{1, 16, "128001Mi", "1h"}, "maxMemPerUser"},
		}},
	} {
		t.Run(tc.file+"/"+tc.partition, func(t *testing.T) {
			pp := load(tc.file)
			// ReadProfile accepts the Provider's qos and fills the settings.
			r := newProfileRig(t)
			r.pp = pp.DeepCopy()
			r.prov.Spec.PlatformProfileRef.Name = pp.Name
			r.prov.Spec.AllowedPartitions = []string{tc.partition}
			r.prov.Spec.QOS = tc.qos
			v, err := r.read()
			if err != nil {
				t.Fatalf("ReadProfile refuses the shipped profile with qos %s: %v", tc.qos, err)
			}
			if v.Settings.QOS != tc.qos {
				t.Fatalf("Settings.QOS = %q, want %q", v.Settings.QOS, tc.qos)
			}
			check := func(s shape) error {
				req := provider.SubmitRequest{
					Tag: "hb-example-0123456789ab", Hold: true, Account: "example-account", QOS: tc.qos,
					Params: v1alpha1.BatchParameters{
						Partition: tc.partition, TimeLimit: v1alpha1.Duration(s.limit), GPUs: s.gpus, CPUsPerTask: s.cpus,
						Memory: resource.MustParse(s.mem), Nodes: 1, AgentDigest: strings.Repeat("a", 64),
					},
				}
				return CheckRequest(req, v.Profile, v.Settings)
			}
			if err := check(tc.atCaps); err != nil {
				t.Fatalf("the request at every cap is refused, so the refusals below prove nothing: %v", err)
			}
			for _, c := range tc.refused {
				err := check(c.s)
				if !errors.Is(err, provider.ErrInvalidRequest) || !strings.Contains(err.Error(), c.want) {
					t.Errorf("%s: got %v; want a refusal wrapping ErrInvalidRequest that names %s", c.name, err, c.want)
				}
			}
		})
	}
}

// On Helios plgrid-gpu-gh200 the partition's own QOS, plgrid, comes before the
// Provider's now (HBEP-4, QOS limits): its submit limit of 2000 is the one that
// applies, and now's cap of one GPU holds because plgrid sets none. With no
// qos the Provider runs under the association's default, normal, which sets
// nothing: four GPUs and 48 h are within the limits. Expected values by hand.
func TestShippedHeliosGH200AppliesThePartitionQOSFirst(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "profiles", "helios.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var pp v1alpha1.PlatformProfile
	if err := yaml.UnmarshalStrict(raw, &pp); err != nil {
		t.Fatal(err)
	}
	var part v1alpha1.PartitionProfile
	for _, p := range pp.Spec.Partitions {
		if p.Name == "plgrid-gpu-gh200" {
			part = p
		}
	}
	if part.QOS != "plgrid" {
		t.Fatalf("plgrid-gpu-gh200 names the partition QOS %q, want plgrid", part.QOS)
	}
	limits, err := v1alpha1.EffectiveQOSLimits(&pp.Spec, part, "now")
	if err != nil {
		t.Fatal(err)
	}
	if limits.MaxSubmitPerUser == nil || *limits.MaxSubmitPerUser != 2000 || limits.From[v1alpha1.LimitMaxSubmitPerUser] != "plgrid" {
		t.Errorf("the submit limit under now is %v from %q, want 2000 from plgrid", limits.MaxSubmitPerUser, limits.From[v1alpha1.LimitMaxSubmitPerUser])
	}
	if limits.MaxGPUsPerUser == nil || *limits.MaxGPUsPerUser != 1 || limits.From[v1alpha1.LimitMaxGPUsPerUser] != "now" {
		t.Errorf("the GPU cap under now is %v from %q, want 1 from now", limits.MaxGPUsPerUser, limits.From[v1alpha1.LimitMaxGPUsPerUser])
	}
	if limits.MaxWall == nil || limits.MaxWall.Hours() != 12 {
		t.Errorf("the wall limit under now is %v, want 12h", limits.MaxWall)
	}
	// The association's default sets no limit but the partition QOS's.
	check := func(qos string, gpus int32, limit string) error {
		req := provider.SubmitRequest{
			Tag: "hb-example-0123456789ab", Hold: true, Account: "example-account", QOS: qos,
			Params: v1alpha1.BatchParameters{
				Partition: "plgrid-gpu-gh200", TimeLimit: v1alpha1.Duration(limit), GPUs: gpus, CPUsPerTask: 72 * gpus,
				Memory: resource.MustParse("122375Mi"), Nodes: 1, AgentDigest: strings.Repeat("a", 64),
			},
		}
		req.Params.Memory.Mul(int64(gpus))
		return CheckRequest(req, provider.Profile{Name: pp.Name, Spec: pp.Spec, Partition: part},
			provider.Settings{Provider: "example", Account: "example-account", Partition: "plgrid-gpu-gh200", QOS: qos})
	}
	if err := check("", 4, "48h"); err != nil {
		t.Errorf("no qos, 4 GPUs for 48h: %v; want it accepted (only the partition's plgrid applies)", err)
	}
	if err := check("normal", 4, "48h"); err != nil {
		t.Errorf("qos normal, 4 GPUs for 48h: %v; want it accepted", err)
	}
	if err := check("now", 4, "12h"); err == nil {
		t.Error("qos now, 4 GPUs: accepted, but now's cap is one GPU")
	}
}
