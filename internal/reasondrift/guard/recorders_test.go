package guard_test

import (
	"fmt"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/reasondrift"
)

// The raised-events counter (HBEP-8, Raised events; #256) holds only if no
// recorder is built without its counting wrapper, which is a check of the scan
// (RecorderNotCounted; the mutants are in guard_test.go), and if each
// controller's series are created at 0 for exactly the events its emit sites
// raise (zerolists_test.go).

// lastName returns the part of a full name after its last dot.
func lastName(full string) string { return full[strings.LastIndex(full, ".")+1:] }

// counterName is the counting wrapper of a recorder: Events or Legacy.
func counterName(r reasondrift.Recorder) string { return lastName(r.Counter) }

// TestTheScanSawTheRecorders guards the check that every recorder is wrapped: a
// matcher that missed a builder would pass for the wrong reason, as nothing
// would be left to fail. The module builds its recorders at five places, and
// each is seen, with the wrapper the controller's events go through.
func TestTheScanSawTheRecorders(t *testing.T) {
	code := realScan(t)
	var got []string
	for _, r := range code.Recorders {
		got = append(got, fmt.Sprintf("%s.%s %s %s", r.Package, r.Func, lastName(r.Builder), counterName(r)))
	}
	sort.Strings(got)
	// The reconciler's recorder and the tunnel-state reader's, the batch
	// Provider managers' (core/v1, through crossplane-runtime), the managed
	// Provider's, and the front's from its own broadcaster.
	want := []string{
		"cmd/hpcburst-controller.addTunnel GetEventRecorder Events",
		"cmd/hpcburst-controller.wiring.setupBatch GetEventRecorderFor Legacy",
		"internal/front.NewKubeRecorder NewRecorder Events",
		"internal/lifecycle.NewManager GetEventRecorder Events",
		"internal/managed.Setup GetEventRecorder Events",
	}
	if !slices.Equal(got, want) {
		t.Errorf("recorders built: %v, want %v", got, want)
	}
	for _, p := range code.Problems {
		if p.Kind == reasondrift.RecorderNotCounted {
			t.Errorf("a recorder is not counted: %s: %s", p.Package, p.Detail)
		}
	}
}
