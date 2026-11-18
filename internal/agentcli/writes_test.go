package agentcli

import (
	"slices"
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The commands write only an Operation (create, and update to confirm) and an Approval (create). Every
// write of every command goes through the recorder; a ModelDeployment, an Allocation or an Instance is
// never written, and nothing is deleted or patched.
func TestTheCommandsWriteOnlyOperationsAndApprovals(t *testing.T) {
	objs := []client.Object{
		alloc("a1", "m1", map[string]string{"tier": "kept"}), alloc("a2", "m2", map[string]string{"tier": "kept"}),
		planned("to-confirm", planAB), waitingOnA("to-approve"),
	}
	w := newWorld(t, objs)
	w.tty = true
	w.names = []string{"op-1111111111", "op-2222222222", "op-3333333333", "op-4444444444", "op-5555555555", "op-6666666666"}
	w.onPause = func() { // the controller plans whatever has no plan, and moves a confirmed plan on
		for _, n := range w.operations() {
			op := w.operation(n)
			if op.Status.Plan == nil {
				w.planFor(n, simplePlan(planAB))
			} else if op.Spec.Confirm != nil && op.Status.Phase == v1alpha1.OperationPhasePlanned {
				op.Status.Phase = v1alpha1.OperationPhaseAwaitingApproval
				if err := w.store.Status().Update(t.Context(), op); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	for _, args := range [][]string{
		{"scale", "m", "--instances", "1"}, {"end", "m"}, {"end", "m", "--delete"}, {"release", "a1"}, {"release", "-l", "tier=kept"},
		{"confirm", "to-confirm", "--plan", planAB}, {"approve", "to-approve", "--plan", planAB},
		{"approvals"}, {"approvals", "-A"}, {"operation", "to-confirm"},
	} {
		if code := w.run("y\ny\ny\n", args...); code != exitOK {
			t.Fatalf("%v: exit %d, stderr %q", args, code, w.errs.String())
		}
	}
	allowed := []string{"create *v1alpha1.Operation", "update *v1alpha1.Operation", "create *v1alpha1.Approval"}
	seen := map[string]bool{}
	for _, wr := range w.writes() {
		seen[wr] = true
		if !slices.Contains(allowed, wr) {
			t.Errorf("a command wrote %q; only %v are allowed", wr, allowed)
		}
	}
	for _, want := range allowed {
		if !seen[want] {
			t.Errorf("no command made %q: the run did not exercise it", want)
		}
	}
}
