package alpha

// The End's job check of the stand-in (standin_operations_test.go, endJobProblem), on its own: the stand-in itself needs a stack and
// runs only by hand or on a hosted runner, so the rule it applies to each job is pinned here, row by row, with no gate. The rows are
// written out from HBEP-10's release cause: an End that released an Allocation is named by its cause, and one it kept is released by
// retention under retention's own.

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
)

func TestEndJobProblem(t *testing.T) {
	end := &v1alpha1.Operation{}
	end.Namespace, end.Name, end.UID = "default", "end-q", types.UID("77900ad8-6861-4c91-aa80-5ba33a615a19")
	const (
		byEnd     = "operation default/end-q 77900ad8-6861-4c91-aa80-5ba33a615a19"
		byOther   = "operation default/scale-p 11111111-2222-3333-4444-555555555555"
		retention = "released when idle, after 10s"
	)
	rel, keep := v1alpha1.PlanEffectReleaseStarted, v1alpha1.PlanEffectKeep
	for _, c := range []struct {
		name   string
		state  string
		effect v1alpha1.PlanEffect
		cause  string
		want   string // "" ok, else a phrase of the problem
	}{
		// A target the End committed to release: today's rule.
		{"released, cancelled, by the End", fakeslurm.Cancelled, rel, byEnd, ""},
		{"released, cancelled, whatever the cause", fakeslurm.Cancelled, rel, retention, ""},
		{"released, completed, by the End's cause", fakeslurm.Completed, rel, byEnd, ""},
		{"released, completed, by retention's cause", fakeslurm.Completed, rel, retention, "want CANCELLED or the cause of Operation"},
		{"released, completed, by another Operation's cause", fakeslurm.Completed, rel, byOther, "want CANCELLED or the cause of Operation"},
		{"released, completed, no cause", fakeslurm.Completed, rel, "", "want CANCELLED or the cause of Operation"},
		// A target the End committed as Keep: either end, but retention's cause.
		{"kept, completed, by retention", fakeslurm.Completed, keep, retention, ""},
		{"kept, cancelled, by retention", fakeslurm.Cancelled, keep, retention, ""},
		{"kept, completed, with the End's own cause", fakeslurm.Completed, keep, byEnd, "retention's own cause"},
		{"kept, cancelled, with the End's own cause", fakeslurm.Cancelled, keep, byEnd, "retention's own cause"},
		{"kept, completed, with another Operation's cause", fakeslurm.Completed, keep, byOther, "retention's own cause"},
		{"kept, completed, no cause seen", fakeslurm.Completed, keep, "", "no release cause"},
		// Failures fail every case.
		{"released, timeout", fakeslurm.Timeout, rel, byEnd, "ran out its time limit or failed"},
		{"released, failed", fakeslurm.Failed, rel, byEnd, "ran out its time limit or failed"},
		{"released, node failure", fakeslurm.NodeFail, rel, byEnd, "ran out its time limit or failed"},
		{"kept, timeout", fakeslurm.Timeout, keep, retention, "ran out its time limit or failed"},
		{"kept, failed", fakeslurm.Failed, keep, retention, "ran out its time limit or failed"},
		{"kept, node failure", fakeslurm.NodeFail, keep, retention, "ran out its time limit or failed"},
		// A job that is not terminal is no end at all.
		{"kept, still running", fakeslurm.Running, keep, retention, "want CANCELLED or COMPLETED"},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := endJobProblem(c.state, c.effect, c.cause, end)
			if (got == "") != (c.want == "") || !strings.Contains(got, c.want) {
				t.Errorf("endJobProblem(%s, %s, %q) = %q, want %q", c.state, c.effect, c.cause, got, c.want)
			}
		})
	}
}
