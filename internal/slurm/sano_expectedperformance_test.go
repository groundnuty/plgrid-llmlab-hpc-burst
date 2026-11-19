package slurm

// Sano's expectedPerformance entries (#735, PR 770; HBEP-4, Expected performance; HBEP-7, The throughput gate): two reference measurements, each keyed on a
// deployment ID, the capacity flags and the concurrent shape. The key of each is derived HERE, from the example manifest the entry is for, the checkpoint's
// file list (testdata/checkpoints, read from the Hub by the controller's own package, not from the entry) and the class of the profile's partition, by
// deploymentid.Compute, and the entry that has the key must be there, once. Nothing is read back from an entry to make its own expectation: an entry whose
// deployment ID is another's, or whose example drifted from what the run deployed, is found here. The measured values and the envelope are the posted reference
// record's, written out as literals (issue #735, comment 6983826975).

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"sigs.k8s.io/yaml"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/deploymentid"
)

// checkpointFixture is a checkpoint's file list at its pinned revision and its resolved chat template's hash.
type checkpointFixture struct {
	Repository         string `json:"repository"`
	Revision           string `json:"revision"`
	ChatTemplateSHA256 string `json:"chatTemplateSha256"`
	Files              []struct {
		Path   string `json:"path"`
		SHA256 string `json:"sha256"`
	} `json:"files"`
}

func readExample(t *testing.T, name string) v1alpha1.ModelDeployment {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "examples", name))
	if err != nil {
		t.Fatalf("read the example: %v", err) // a missing file fails; it never skips
	}
	var md v1alpha1.ModelDeployment
	if err := yaml.UnmarshalStrict(raw, &md); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return md
}

func readCheckpoint(t *testing.T, name string) checkpointFixture {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "checkpoints", name))
	if err != nil {
		t.Fatalf("read the checkpoint's file list: %v", err)
	}
	var fx checkpointFixture
	if err := json.Unmarshal(raw, &fx); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return fx
}

// derivedKey is the key the controller gives an Instance of the example on the partition (internal/lifecycle: computeIDs and instanceKey): the
// display form of the deployment ID, computed from the example's serving spec, the fixture's files and template and the partition's class, and the
// capacity flags, the sessions and the typical shapes from the example. With an error the test stops: an example this cannot derive a key for is one the
// entries cannot be checked against.
func derivedKey(t *testing.T, md v1alpha1.ModelDeployment, fx checkpointFixture, part v1alpha1.PartitionProfile) v1alpha1.ReferenceKey {
	t.Helper()
	s := md.Spec.Source.Served
	if s == nil {
		t.Fatalf("%s is not a served deployment", md.Name)
	}
	if s.Checkpoint.Repository != fx.Repository || s.Checkpoint.Revision != fx.Revision {
		t.Fatalf("%s names %s at %s, the file list is of %s at %s", md.Name, s.Checkpoint.Repository, s.Checkpoint.Revision, fx.Repository, fx.Revision)
	}
	if s.ChatTemplate != nil || md.Spec.WorkersPerInstance != nil || md.Spec.Workload == nil {
		t.Fatalf("%s sets a chat template or a worker count, or has no workload: the fixture's template and one worker are what it derives from", md.Name)
	}
	spec := deploymentid.ServingSpec{
		Checkpoint:         deploymentid.Checkpoint{Repository: s.Checkpoint.Repository, Revision: s.Checkpoint.Revision},
		Engine:             deploymentid.Engine{Name: s.Engine.Name, Version: s.Engine.Version, ImageDigest: s.Engine.ImageDigest},
		ChatTemplateSHA256: fx.ChatTemplateSHA256,
	}
	capacity := map[string]v1alpha1.FlagValue{"max-num-seqs": v1alpha1.FlagValue(strconv.Itoa(v1alpha1.DefaultMaxNumSeqs))}
	for _, f := range s.Flags {
		spec.Flags = append(spec.Flags, deploymentid.Flag{Name: f.Name, Value: f.Value})
		if deploymentid.IsCapacityFlag(f.Name) {
			capacity[f.Name] = v1alpha1.FlagValue(f.Value)
		}
	}
	for _, e := range s.Env {
		spec.Env = append(spec.Env, deploymentid.EnvVar{Name: e.Name, Value: e.Value})
	}
	var files []deploymentid.File
	for _, f := range fx.Files {
		files = append(files, deploymentid.File{Path: f.Path, SHA256: f.SHA256})
	}
	id, err := deploymentid.Compute(spec, deploymentid.Accelerator{GPUModel: part.GPUModel, CPUArch: part.CPUArch}, files)
	if err != nil {
		t.Fatalf("%s: the deployment ID: %v", md.Name, err)
	}
	w := md.Spec.Workload
	return v1alpha1.ReferenceKey{DeploymentID: id.Name, CapacityFlags: capacity, Sessions: w.TargetSessions, ContextTokens: w.ContextTokens.Typical, OutputTokens: w.OutputTokens.Typical}
}

// Each entry of the shipped profile is the reference of one example, found by the key derived from that example, with the values of the posted record and its
// envelope. The two keys differ, and the posted deployment IDs are the derived ones (written out below from the record, as a second witness).
func TestTheSanoExpectedPerformanceEntriesAreKeyedOnTheExamplesTheyAreFor(t *testing.T) {
	pp := readShippedSano(t)
	part := pp.Spec.Partitions[0]
	const source = "https://github.com/groundnuty/plgrid-llmlab-hpc-burst/issues/735#issuecomment-6983826975"
	const recordMethod = "3 warm runs on one Sano A100 (gpu1), the slowest of each; all on 8-GPU nodes; it applies to the 4-GPU nodes (the same A100 80 GB class) within tolerancePercent; 18 Nov 2026"
	// The assessment of 19 Nov 2026: the runs were at 11 CPUs and 135904Mi per GPU and the profile now asks for 4 and 64 GiB; the confirming run is pending. The figures and keys are the record's.
	const shape = ". Measured at 11 CPUs and 135904Mi per GPU. Expected to hold at 4 CPUs and 64 GiB, since decode is GPU-bound and the gate's default tolerance allows 2x; pending one confirming run at the new shape"
	const method = recordMethod + shape
	if len(pp.Spec.ExpectedPerformance) != 2 {
		t.Fatalf("%d entries, want the two of the reference record", len(pp.Spec.ExpectedPerformance))
	}
	cases := []struct {
		name, example, fixture, postedID string
		tpot, ttft                       string
	}{
		{"Qwen2.5-7B-Instruct", "modeldeployment-sano-qwen25-7b-instruct.yaml", "qwen25-7b-instruct-a09a35458c70.json", "367d47b43853", "12.326323ms", "399.394255ms"},
		{"Qwen3-VL-8B-Instruct", "modeldeployment-sano-qwen3-vl-8b-instruct.yaml", "qwen3-vl-8b-instruct-0c351dd01ed8.json", "965fa2409193", "13.679505ms", "429.065029ms"},
	}
	var ids []string
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			key := derivedKey(t, readExample(t, c.example), readCheckpoint(t, c.fixture), part)
			ids = append(ids, key.DeploymentID)
			if key.DeploymentID != c.postedID {
				t.Fatalf("the example derives the deployment ID %s, the reference record's is %s: the example is not what the run deployed", key.DeploymentID, c.postedID)
			}
			var found []v1alpha1.ExpectedPerformance
			for _, e := range pp.Spec.ExpectedPerformance {
				if e.DeploymentID == key.DeploymentID {
					found = append(found, e)
				}
			}
			if len(found) != 1 {
				t.Fatalf("%d entries have the derived deployment ID %s, want exactly one", len(found), key.DeploymentID)
			}
			e := found[0]
			if !slices.Equal(sortedFlags(e.CapacityFlags), sortedFlags(key.CapacityFlags)) || e.Sessions != key.Sessions || e.ContextTokens != key.ContextTokens || e.OutputTokens != key.OutputTokens {
				t.Errorf("the entry's key is %+v, the example's is %+v", e.ReferenceKey, key)
			}
			if got, _ := e.TimePerOutputToken.AsDuration(); got != mustDuration(t, c.tpot) {
				t.Errorf("timePerOutputToken %s, want the record's %s", e.TimePerOutputToken, c.tpot)
			}
			if got, _ := e.TimeToFirstToken.AsDuration(); got != mustDuration(t, c.ttft) {
				t.Errorf("timeToFirstToken %s, want the record's %s", e.TimeToFirstToken, c.ttft)
			}
			if e.Method != method || e.Source != source || e.Date != "2026-11-18" || e.TolerancePercent != nil {
				t.Errorf("the envelope is method %q, source %q, date %q, tolerancePercent %v; want the record's, with the default tolerance", e.Method, e.Source, e.Date, e.TolerancePercent)
			}
		})
	}
	if len(ids) == 2 && ids[0] == ids[1] {
		t.Errorf("both examples derive the deployment ID %s", ids[0])
	}
}

// The reference run's own manifest for the 7B is the client example's spec under another name, so it derives the same key and the entry holds for it; and a
// change the key must notice changes it: an example whose max-model-len differs, an engine digest, a revision or a different class. Without these the
// derivation could be one that returns the same ID whatever the example says.
func TestTheDerivedKeyFollowsTheExample(t *testing.T) {
	pp := readShippedSano(t)
	part := pp.Spec.Partitions[0]
	fx := readCheckpoint(t, "qwen25-7b-instruct-a09a35458c70.json")
	client := derivedKey(t, readExample(t, "modeldeployment-sano-qwen25-7b-instruct.yaml"), fx, part)
	if ref := derivedKey(t, readExample(t, "modeldeployment-sano-reference-qwen25-7b-instruct.yaml"), fx, part); ref.DeploymentID != client.DeploymentID || ref.Sessions != client.Sessions {
		t.Errorf("the reference manifest derives %+v, the client example %+v: the entry would not hold for the run", ref, client)
	}
	md := readExample(t, "modeldeployment-sano-qwen25-7b-instruct.yaml")
	md.Spec.Source.Served.Flags[0].Value = "32767"
	if k := derivedKey(t, md, fx, part); k.DeploymentID == client.DeploymentID {
		t.Errorf("another max-model-len derives the same deployment ID %s", k.DeploymentID)
	}
	md = readExample(t, "modeldeployment-sano-qwen25-7b-instruct.yaml")
	md.Spec.Source.Served.Engine.ImageDigest = "sha256:" + strings.Repeat("0", 64)
	if k := derivedKey(t, md, fx, part); k.DeploymentID == client.DeploymentID {
		t.Errorf("another engine digest derives the same deployment ID %s", k.DeploymentID)
	}
	other := part
	other.GPUModel = "GH200"
	if k := derivedKey(t, readExample(t, "modeldeployment-sano-qwen25-7b-instruct.yaml"), fx, other); k.DeploymentID == client.DeploymentID {
		t.Errorf("another accelerator class derives the same deployment ID %s", k.DeploymentID)
	}
}

func sortedFlags(m map[string]v1alpha1.FlagValue) []string {
	var out []string
	for k, v := range m {
		out = append(out, k+"="+string(v))
	}
	slices.Sort(out)
	return out
}

func mustDuration(t *testing.T, s string) time.Duration {
	t.Helper()
	d, err := time.ParseDuration(s)
	if err != nil {
		t.Fatal(err)
	}
	return d
}
