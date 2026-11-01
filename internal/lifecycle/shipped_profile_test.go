package lifecycle

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/yaml"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// Athena's plgrid-now is for conformance runs and short checks only; the lifecycle
// controller must never plan an Instance there (`@hpc-burst-science-agent`, 1 Nov 2026,
// issue #93, comment 6557245889). The partition's time limit is 1 h, below the drain lead
// and the handover margin of the shipped profile, and nothing in the controller compares
// the two. What keeps it off the partition today is that the shipped profile gives the
// partition neither cpusPerGpu nor memPerGpu, so partitionSizing refuses to size a request
// there. That is a consequence of two absent values, not a rule about time limits:
// sourcing them (the QoS cap is cpu=16 and 125G) would lift it, and this test would then
// fail. Both hold until HBEP-7's PartitionTooShort check lands (maxTime must exceed
// drainWindow plus handoverMargin plus health.startTimeout).
//
// The control is the shipped plgrid-gpu-a100 entry with the same call: it is sized, so
// the refusal comes from the plgrid-now entry and not from the test's setup. The expected
// values are the issue's: the refusal on plgrid-now, a 48 h limit on plgrid-gpu-a100.
func TestShippedAthenaNowCannotBeSized(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "profiles", "athena.yaml"))
	if err != nil {
		t.Fatalf("read the shipped profile: %v", err) // a missing file fails; it never skips
	}
	var prof v1alpha1.PlatformProfile
	if err := yaml.UnmarshalStrict(raw, &prof); err != nil {
		t.Fatal(err)
	}
	view := func(partition string) *providerView {
		for _, p := range prof.Spec.Partitions {
			if p.Name == partition {
				return &providerView{name: "athena", profile: &prof, partition: p, agentDigest: strings.Repeat("a", 64)}
			}
		}
		t.Fatalf("athena.yaml does not list %s", partition)
		return nil
	}

	got, err := partitionSizing(view("plgrid-gpu-a100"), 1, nil)
	if err != nil {
		t.Fatalf("plgrid-gpu-a100 is not sized, so the control proves nothing: %v", err)
	}
	if want := resource.MustParse("128000Mi"); got.GPUs != 1 || got.CPUsPerTask != 16 || got.Memory.Cmp(want) != 0 || got.TimeLimit != "48h0m0s" {
		t.Errorf("plgrid-gpu-a100 sized as %+v; want 1 GPU, 16 CPUs, 128000Mi, 48h0m0s", got)
	}

	got, err = partitionSizing(view("plgrid-now"), 1, nil)
	if err == nil {
		t.Fatalf("an Instance of 1 GPU is sized on plgrid-now: %+v", got)
	}
	if !strings.Contains(err.Error(), "partition plgrid-now") {
		t.Errorf("the refusal does not name plgrid-now: %v", err)
	}
	if got.Partition != "" {
		t.Errorf("parameters returned with the refusal: %+v", got)
	}
}
