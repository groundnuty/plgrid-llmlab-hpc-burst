package e2e

// finalizerCycle reads the operation-unfinished finalizer's life from the log, and what it refuses is what lets the Keep link's row
// say that the removal's delete did not wait on a finalizer. No cluster here: the events are written by hand, so each case is a
// log the check must accept or refuse for one named reason.

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

const cycleUser = "system:serviceaccount:hpcburst-system:hpcburst-lifecycle"

// cycleBody is the Operation as a request body carries it: its phase, whether it has finished and whether it carries the finalizer.
func cycleBody(t *testing.T, phase v1alpha1.OperationPhase, committed, finished, finalizer bool) json.RawMessage {
	t.Helper()
	op := v1alpha1.Operation{ObjectMeta: metav1.ObjectMeta{Name: "end-a", Namespace: exampleNS}}
	op.Status.Phase = phase
	if committed {
		op.Status.Committed = []v1alpha1.CommittedTarget{{Kind: v1alpha1.TargetKindAllocation, Name: "alloc"}}
	}
	if finished {
		at := metav1.NewTime(time.Date(2026, 11, 18, 3, 0, 0, 0, time.UTC))
		op.Status.FinishedAt = &at
	}
	if finalizer {
		op.Finalizers = []string{v1alpha1.FinalizerOperationUnfinished}
	}
	b, err := json.Marshal(op)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// cycleEvent is one logged request on the Operation end-a by the lifecycle identity, received sec seconds into the run.
func cycleEvent(sec int, status bool, code int, body json.RawMessage) auditEvent {
	var e auditEvent
	e.Verb, e.User.Username, e.ResponseStatus.Code, e.RequestObject = "update", cycleUser, code, body
	e.ObjectRef.Resource, e.ObjectRef.Name = "operations", "end-a"
	if status {
		e.ObjectRef.Subresource = "status"
	}
	e.RequestReceivedTimestamp = time.Date(2026, 11, 18, 3, 0, sec, 0, time.UTC)
	return e
}

func TestTheFinalizerCycleIsReadFromSuccessfulRequestsAtTheCommitAndTheFinish(t *testing.T) {
	commitW := func(sec, code int) auditEvent {
		return cycleEvent(sec, true, code, cycleBody(t, v1alpha1.OperationPhaseExecuting, true, false, false))
	}
	finishW := func(sec, code int) auditEvent {
		return cycleEvent(sec, true, code, cycleBody(t, v1alpha1.OperationPhaseDone, true, true, true))
	}
	add := func(sec, code int) auditEvent {
		return cycleEvent(sec, false, code, cycleBody(t, v1alpha1.OperationPhaseExecuting, true, false, true))
	}
	off := func(sec, code int, phase v1alpha1.OperationPhase, finished bool) auditEvent {
		return cycleEvent(sec, false, code, cycleBody(t, phase, true, finished, false))
	}
	done := v1alpha1.OperationPhaseDone
	for _, tc := range []struct {
		name   string
		events []auditEvent
		want   string // a text every problem list must hold, or "" for no problem
	}{
		{"the commit, the add, the finish and the removal, in order", []auditEvent{commitW(1, 200), add(2, 200), finishW(3, 200), off(4, 200, done, true)}, ""},
		{"a conflicted add before the commit is no add", []auditEvent{add(0, 409), commitW(1, 200), add(2, 200), finishW(3, 200), off(4, 200, done, true)}, ""},
		{"a conflicted retry of the removal is no removal, the later success is", []auditEvent{commitW(1, 200), add(2, 200), finishW(3, 200), off(4, 409, done, true), off(5, 200, done, true)}, ""},
		{"an add that only conflicted", []auditEvent{commitW(1, 200), add(2, 409), finishW(3, 200), off(4, 200, done, true)}, "never on"},
		{"an add before the commit", []auditEvent{add(0, 200), commitW(1, 200), finishW(3, 200), off(4, 200, done, true)}, "not after the commit"},
		{"an add after the finish", []auditEvent{commitW(1, 200), finishW(2, 200), add(3, 200), off(4, 200, done, true)}, "not before the finish"},
		{"a removal that only conflicted", []auditEvent{commitW(1, 200), add(2, 200), finishW(3, 200), off(4, 409, done, true)}, "finalizer off after the one that added it"},
		{"a removal while the Operation is still Executing", []auditEvent{commitW(1, 200), add(2, 200), off(3, 200, v1alpha1.OperationPhaseExecuting, false), finishW(4, 200)}, "not finished"},
		{"a removal before the finish write whose body already says Done", []auditEvent{commitW(1, 200), add(2, 200), off(3, 200, done, false), finishW(4, 200)}, "not after the finish"},
		{"a removal of a PartlyFailed Operation with no finishedAt", []auditEvent{commitW(1, 200), add(2, 200), finishW(3, 200), off(4, 200, v1alpha1.OperationPhasePartlyFailed, false)}, "not finished"},
		{"a removal of a Refused Operation, which has no finishedAt in this body", []auditEvent{commitW(1, 200), add(2, 200), finishW(3, 200), off(4, 200, v1alpha1.OperationPhaseRefused, false)}, ""},
		{"no commit", []auditEvent{add(2, 200), finishW(3, 200), off(4, 200, done, true)}, "the commit is not in the log"},
		{"a commit that only conflicted", []auditEvent{commitW(1, 409), add(2, 200), finishW(3, 200), off(4, 200, done, true)}, "the commit is not in the log"},
		{"no finish", []auditEvent{commitW(1, 200), add(2, 200), off(4, 200, done, true)}, "the finish is not in the log"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			released, problems := finalizerCycle(t, &logSet{events: tc.events}, cycleUser, "end-a")
			all := strings.Join(problems, "\n")
			switch {
			case tc.want == "" && len(problems) != 0:
				t.Errorf("a clean log was refused: %s", all)
			case tc.want != "" && !strings.Contains(all, tc.want):
				t.Errorf("the problems are %q, want one that holds %q", all, tc.want)
			case tc.want == "" && released == nil:
				t.Errorf("a clean log gave no removal")
			}
		})
	}
}
