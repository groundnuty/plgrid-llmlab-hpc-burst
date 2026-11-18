package e2e

import (
	"fmt"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The wait for "the Approval is counted" ends at the commit, whichever of the two signs shows first (the Executable reason, or the
// phase), and not at a plan that still waits. No cluster here: like the token matcher's, this test starts none.
func TestAnOperationIsCommittedOnceItsExecutableReasonOrItsPhaseSaysSo(t *testing.T) {
	op := func(phase v1alpha1.OperationPhase, reason string) *v1alpha1.Operation {
		o := &v1alpha1.Operation{}
		o.Status.Phase = phase
		if reason != "" {
			o.Status.Conditions = []metav1.Condition{{Type: v1alpha1.ConditionExecutable, Reason: reason, Message: "example message"}}
		}
		return o
	}
	for _, tc := range []struct {
		name string
		op   *v1alpha1.Operation
		want bool
	}{
		{"executing by reason", op("", v1alpha1.ReasonExecuting), true},
		{"done by reason", op("", v1alpha1.ReasonDone), true},
		{"executing by phase", op(v1alpha1.OperationPhaseExecuting, ""), true},
		{"partly failed by phase", op(v1alpha1.OperationPhasePartlyFailed, ""), true},
		{"done by phase", op(v1alpha1.OperationPhaseDone, ""), true},
		{"planned, waiting for its owner", op(v1alpha1.OperationPhasePlanned, v1alpha1.ReasonNotConfirmed), false},
		{"awaiting approval", op(v1alpha1.OperationPhaseAwaitingApproval, v1alpha1.ReasonAwaitingApproval), false},
		{"another Operation executing", op(v1alpha1.OperationPhaseAwaitingApproval, v1alpha1.ReasonAnotherOperationExecuting), false},
		{"stale", op(v1alpha1.OperationPhaseStale, v1alpha1.ReasonPlanStale), false},
		{"refused", op(v1alpha1.OperationPhaseRefused, v1alpha1.ReasonRefused), false},
		{"no condition yet", op(v1alpha1.OperationPhasePlanning, ""), false},
	} {
		got, msg := operationCommitted(tc.op)
		if got != tc.want {
			t.Errorf("%s: committed = %v, want %v (%s)", tc.name, got, tc.want, msg)
		}
		if !strings.Contains(msg, fmt.Sprintf("phase %q", string(tc.op.Status.Phase))) {
			t.Errorf("%s: the message %q does not report the phase", tc.name, msg)
		}
	}
}
