package lifecycle

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// frozenByOperation (HBEP-7, Hedging, Interactions, Operations): a hedge is frozen while an Operation is Executing, or
// PartlyFailed, with a committed Instance target among its members; not before the commit, not once the Operation is
// finished, and not for another hedge's members or a committed target that is not an Instance. The table is written out
// by hand.
func TestFrozenByOperation(t *testing.T) {
	member := func(uid string) *v1alpha1.Instance {
		return &v1alpha1.Instance{ObjectMeta: metav1.ObjectMeta{Name: "in-" + uid, UID: types.UID(uid)}}
	}
	op := func(phase v1alpha1.OperationPhase, committed ...v1alpha1.CommittedTarget) *v1alpha1.Operation {
		return &v1alpha1.Operation{Status: v1alpha1.OperationStatus{Phase: phase, Committed: committed}}
	}
	inst := func(uid string) v1alpha1.CommittedTarget {
		return v1alpha1.CommittedTarget{Kind: v1alpha1.TargetKindInstance, Name: "in-" + uid, UID: types.UID(uid), Effect: v1alpha1.PlanEffectCancelPending}
	}
	alloc := func(uid string) v1alpha1.CommittedTarget {
		return v1alpha1.CommittedTarget{Kind: v1alpha1.TargetKindAllocation, Name: "al-" + uid, UID: types.UID(uid), Effect: v1alpha1.PlanEffectCancelPending}
	}
	hedge := []*v1alpha1.Instance{member("m1"), member("m2")}
	for _, tc := range []struct {
		name string
		ops  []*v1alpha1.Operation
		want bool
	}{
		{"no Operation", nil, false},
		{"Executing, a member committed", []*v1alpha1.Operation{op(v1alpha1.OperationPhaseExecuting, inst("m2"))}, true},
		{"Executing, the first member committed among others", []*v1alpha1.Operation{op(v1alpha1.OperationPhaseExecuting, inst("x"), inst("m1"))}, true},
		{"PartlyFailed, a member committed: still carried out", []*v1alpha1.Operation{op(v1alpha1.OperationPhasePartlyFailed, inst("m1"))}, true},
		{"Executing, another hedge's member", []*v1alpha1.Operation{op(v1alpha1.OperationPhaseExecuting, inst("other"))}, false},
		{"Executing, only an Allocation by a member's UID", []*v1alpha1.Operation{op(v1alpha1.OperationPhaseExecuting, alloc("m1"))}, false},
		{"Done", []*v1alpha1.Operation{op(v1alpha1.OperationPhaseDone, inst("m1"))}, false},
		{"Stale", []*v1alpha1.Operation{op(v1alpha1.OperationPhaseStale, inst("m1"))}, false},
		{"AwaitingApproval, not committed", []*v1alpha1.Operation{op(v1alpha1.OperationPhaseAwaitingApproval)}, false},
		{"a finished one and an Executing one", []*v1alpha1.Operation{op(v1alpha1.OperationPhaseDone, inst("m1")), op(v1alpha1.OperationPhaseExecuting, inst("m2"))}, true},
	} {
		if got := frozenByOperation(hedge, tc.ops); got != tc.want {
			t.Errorf("%s: frozenByOperation = %v, want %v", tc.name, got, tc.want)
		}
	}
	if frozenByOperation(nil, []*v1alpha1.Operation{op(v1alpha1.OperationPhaseExecuting, inst("m1"))}) {
		t.Error("a hedge with no member is frozen")
	}
}
