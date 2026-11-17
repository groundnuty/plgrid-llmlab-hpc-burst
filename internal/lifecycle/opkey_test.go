package lifecycle

// Which Operations keep a deleted deployment's key requeueing (#589, PR 4): only an unfinished one. Written by
// hand from the phases: no phase yet, Planned, AwaitingApproval, Executing and PartlyFailed are not terminal;
// Done, Stale and Refused are.

import (
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

func TestOnlyAnUnfinishedOperationKeepsTheKey(t *testing.T) {
	for _, tc := range []struct {
		phase v1alpha1.OperationPhase
		want  bool
	}{
		{"", true},
		{v1alpha1.OperationPhasePlanned, true},
		{v1alpha1.OperationPhaseAwaitingApproval, true},
		{v1alpha1.OperationPhaseExecuting, true},
		{v1alpha1.OperationPhasePartlyFailed, true},
		{v1alpha1.OperationPhaseDone, false},
		{v1alpha1.OperationPhaseStale, false},
		{v1alpha1.OperationPhaseRefused, false},
	} {
		op := &v1alpha1.Operation{}
		op.Status.Phase = tc.phase
		p := &pass{ops: []*v1alpha1.Operation{op}}
		if got := p.opsNamingKey(); got != tc.want {
			t.Errorf("an Operation in phase %q: keeps the key = %v, want %v", tc.phase, got, tc.want)
		}
	}
	if (&pass{}).opsNamingKey() {
		t.Error("no Operation keeps the key")
	}
	// One unfinished Operation among finished ones keeps it.
	done, refused, planned := &v1alpha1.Operation{}, &v1alpha1.Operation{}, &v1alpha1.Operation{}
	done.Status.Phase, refused.Status.Phase, planned.Status.Phase = v1alpha1.OperationPhaseDone, v1alpha1.OperationPhaseRefused, v1alpha1.OperationPhasePlanned
	if !(&pass{ops: []*v1alpha1.Operation{done, refused, planned}}).opsNamingKey() {
		t.Error("a Planned Operation beside a Done and a Refused one keeps the key")
	}
	if (&pass{ops: []*v1alpha1.Operation{done, refused}}).opsNamingKey() {
		t.Error("a Done and a Refused Operation keep the key")
	}
}
