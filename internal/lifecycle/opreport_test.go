package lifecycle

// The report's own rules, on a fixture pass over a fake client (#589, PR 5): which failures are final and what the outcomes
// make of the Operation, as HBEP-10, The report, says as amended by #665 (the science agent's ruling on #664, 16 Nov 2026);
// and why a target fails (a Provider that cannot finish a delete, or the controller's own write). The expected values are
// written out by hand.

import (
	"errors"
	"strings"
	"testing"
	"time"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// targetFailure: an Allocation fails to go when it was being deleted before the pass and the Provider's controller reports
// Synced=False, with its reason and its message cut to 1,024 bytes; not when the delete was asked in this pass, when Synced
// is True, or when the target is no release; and an error of the controller's own write fails any target.
func TestATargetFailsByItsOwnWriteOrAProviderThatCannotFinishTheDelete(t *testing.T) {
	synced := func(status corev1.ConditionStatus, reason, msg string) []xpv2.Condition {
		return []xpv2.Condition{{Type: xpv2.TypeSynced, Status: status, Reason: xpv2.ConditionReason(reason), Message: msg}}
	}
	long := strings.Repeat("x", 3000)
	target := v1alpha1.CommittedTarget{Kind: v1alpha1.TargetKindAllocation, Name: "alloc-k", UID: "uid-alloc-k", Effect: v1alpha1.PlanEffectReleaseKept}
	for _, tc := range []struct {
		name         string
		conds        []xpv2.Condition
		deleting     bool // being deleted before the pass
		deletingNow  bool // the object carries a deletion time
		effect       v1alpha1.PlanEffect
		own          error
		wantFailed   bool
		wantReason   string
		wantMsgBytes int
	}{
		{"Synced False on a delete asked for before the pass", synced(corev1.ConditionFalse, "ReconcileError", "cancel job 1234: timed out"), true, true, v1alpha1.PlanEffectReleaseKept, nil, true, "ReconcileError", len("cancel job 1234: timed out")},
		{"a message over 1,024 bytes is cut", synced(corev1.ConditionFalse, "ReconcileError", long), true, true, v1alpha1.PlanEffectReleaseKept, nil, true, "ReconcileError", 1024},
		{"no reason in the condition", synced(corev1.ConditionFalse, "", "m"), true, true, v1alpha1.PlanEffectReleaseKept, nil, true, reasonReleaseFailed, 1},
		{"Synced True", synced(corev1.ConditionTrue, "ReconcileSuccess", ""), true, true, v1alpha1.PlanEffectReleaseKept, nil, false, "", 0},
		{"Synced False from before: the delete was asked for in this pass", synced(corev1.ConditionFalse, "ReconcileError", "old"), false, true, v1alpha1.PlanEffectReleaseKept, nil, false, "", 0},
		{"not being deleted", synced(corev1.ConditionFalse, "ReconcileError", "old"), false, false, v1alpha1.PlanEffectReleaseKept, nil, false, "", 0},
		{"a Keep is no release", synced(corev1.ConditionFalse, "ReconcileError", "m"), true, true, v1alpha1.PlanEffectKeep, nil, false, "", 0},
		{"our own write's error", nil, false, false, v1alpha1.PlanEffectReleaseKept, errors.New("the API server refused: conflict"), true, reasonWriteFailed, len("the API server refused: conflict")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newOpFixture(t, 1)
			a := f.emptyAllocation("alloc-k", nil)
			a.Status.Conditions = tc.conds
			if tc.deletingNow {
				a.DeletionTimestamp = opTime(opAt)
			}
			p := f.pass()
			tg := target
			tg.Effect = tc.effect
			deleting := map[types.UID]bool{}
			if tc.deleting {
				deleting[tg.UID] = true
			}
			p.opAllocs = map[types.UID]*v1alpha1.Allocation{tg.UID: a}
			reason, msg, failed, err := p.targetFailure(tg, tc.own, deleting)
			if err != nil {
				t.Fatal(err)
			}
			if failed != tc.wantFailed || reason != tc.wantReason || len(msg) != tc.wantMsgBytes {
				t.Errorf("failed %v reason %q message %d bytes, want %v %q %d bytes", failed, reason, len(msg), tc.wantFailed, tc.wantReason, tc.wantMsgBytes)
			}
		})
	}
}

// Only a job that started after its cancel was approved is final; a refused cancel and the controller's own write's error
// are retried, and any other reason is not final.
func TestWhichFailuresAreFinal(t *testing.T) {
	for _, tc := range []struct {
		reason string
		want   bool
	}{
		{"JobStartedSinceCommit", true},
		{"SpecChangedSinceCommit", true},
		{"LastReadySinceCommit", true},
		{"ReconcileError", false},
		{"WriteFailed", false},
		{"ReleaseFailed", false},
		{"", false},
		{"jobstartedsincecommit", false},
	} {
		if got := failureIsFinal(tc.reason); got != tc.want {
			t.Errorf("failureIsFinal(%q) = %v, want %v", tc.reason, got, tc.want)
		}
	}
}

// phaseOf, as #665 says: Done when every target is done; PartlyFailed and finished when every target is done or failed for
// good; PartlyFailed and not finished while a retriable failure remains; Executing while none has failed.
func TestWhatTheOutcomesMakeOfTheOperation(t *testing.T) {
	done := v1alpha1.TargetOutcome{State: v1alpha1.TargetStateDone}
	todo := v1alpha1.TargetOutcome{State: v1alpha1.TargetStateNotStarted}
	final := v1alpha1.TargetOutcome{State: v1alpha1.TargetStateFailed, Reason: "JobStartedSinceCommit"}
	retried := v1alpha1.TargetOutcome{State: v1alpha1.TargetStateFailed, Reason: "ReconcileError"}
	for _, tc := range []struct {
		name         string
		outcomes     []v1alpha1.TargetOutcome
		wantPhase    v1alpha1.OperationPhase
		wantFinished bool
	}{
		{"all done", []v1alpha1.TargetOutcome{done, done}, v1alpha1.OperationPhaseDone, true},
		{"none started", []v1alpha1.TargetOutcome{todo, todo}, v1alpha1.OperationPhaseExecuting, false},
		{"some done, some not", []v1alpha1.TargetOutcome{done, todo}, v1alpha1.OperationPhaseExecuting, false},
		{"the rest done, one failed for good", []v1alpha1.TargetOutcome{done, final, done}, v1alpha1.OperationPhasePartlyFailed, true},
		{"every target failed for good", []v1alpha1.TargetOutcome{final, final}, v1alpha1.OperationPhasePartlyFailed, true},
		{"a final failure and a target still to do", []v1alpha1.TargetOutcome{final, todo}, v1alpha1.OperationPhasePartlyFailed, false},
		{"a retried failure", []v1alpha1.TargetOutcome{done, retried}, v1alpha1.OperationPhasePartlyFailed, false},
		{"a retried failure beside a final one", []v1alpha1.TargetOutcome{retried, final, done}, v1alpha1.OperationPhasePartlyFailed, false},
		{"no targets", nil, v1alpha1.OperationPhaseDone, true},
	} {
		phase, finished := phaseOf(tc.outcomes)
		if phase != tc.wantPhase || finished != tc.wantFinished {
			t.Errorf("%s: phaseOf = %s, finished %v, want %s, finished %v", tc.name, phase, finished, tc.wantPhase, tc.wantFinished)
		}
	}
}

// A finished PartlyFailed Operation holds nothing: it is no longer the one that is executing. One with a retried failure, and
// Executing, are.
func TestWhichOperationsAreExecuting(t *testing.T) {
	at := metav1.NewTime(opAt)
	for _, tc := range []struct {
		phase    v1alpha1.OperationPhase
		finished *metav1.Time
		want     bool
	}{
		{v1alpha1.OperationPhaseExecuting, nil, true},
		{v1alpha1.OperationPhasePartlyFailed, nil, true},
		{v1alpha1.OperationPhasePartlyFailed, &at, false},
		{v1alpha1.OperationPhaseDone, &at, false},
		{v1alpha1.OperationPhaseStale, &at, false},
		{v1alpha1.OperationPhaseAwaitingApproval, nil, false},
		{v1alpha1.OperationPhasePlanned, nil, false},
	} {
		op := &v1alpha1.Operation{Status: v1alpha1.OperationStatus{Phase: tc.phase, FinishedAt: tc.finished}}
		if got := isExecuting(op); got != tc.want {
			t.Errorf("phase %s, finished %v: isExecuting = %v, want %v", tc.phase, tc.finished != nil, got, tc.want)
		}
	}
}

// A final failure, once recorded, stays: the Allocation's later release by retention is not the approved act, and the target is
// never reported done when that act did not happen. Without a record, an Allocation that started is the failure; one that
// is gone is done (the Operation cancelled it while it was queued); and a started one with another effect is no failure.
func TestAFinalFailureStaysAndAStartedCancelIsNotDone(t *testing.T) {
	cancel := committedOf(v1alpha1.TargetKindAllocation, "alloc-p", v1alpha1.PlanEffectCancelPending)
	record := v1alpha1.TargetOutcome{Kind: cancel.Kind, Name: cancel.Name, UID: cancel.UID, State: v1alpha1.TargetStateFailed, Reason: reasonJobStartedSinceCommit,
		Message: "kept words", LastTransitionTime: metav1.NewTime(opT0)}
	for _, tc := range []struct {
		name      string
		started   bool
		marked    bool // the kept-warm mark is on the Allocation (written by the act, before the record)
		gone      bool
		prior     []v1alpha1.TargetOutcome
		effect    v1alpha1.PlanEffect
		wantState v1alpha1.TargetState
	}{
		{"queued, not cancelled yet", false, false, false, nil, v1alpha1.PlanEffectCancelPending, v1alpha1.TargetStateNotStarted},
		{"gone: the Operation cancelled it queued", false, false, true, nil, v1alpha1.PlanEffectCancelPending, v1alpha1.TargetStateDone},
		{"started since the commit and marked", true, true, false, nil, v1alpha1.PlanEffectCancelPending, v1alpha1.TargetStateFailed},
		{"started since the commit, not marked yet: not final until the mark is written", true, false, false, nil, v1alpha1.PlanEffectCancelPending, v1alpha1.TargetStateNotStarted},
		{"started, then released by retention: still the failure", true, true, true, []v1alpha1.TargetOutcome{record}, v1alpha1.PlanEffectCancelPending, v1alpha1.TargetStateFailed},
		{"started with another effect is no failure", true, false, false, nil, v1alpha1.PlanEffectReleaseKept, v1alpha1.TargetStateNotStarted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newOpFixture(t, 1)
			f.addInstance(instOpts{name: "r", phase: v1alpha1.InstanceReady, started: true, hardEnd: tp(opHardEnd)})
			a := f.emptyAllocation("alloc-p", nil)
			a.Status.AtProvider.StartTime = nil
			if tc.started {
				a.Status.AtProvider.StartTime = opTime(opT0)
			}
			if tc.marked {
				a.Annotations = map[string]string{v1alpha1.AnnotationKeptWarm: "operation ns/op uid-op"}
			}
			target := cancel
			target.Effect = tc.effect
			target.UID = a.UID
			op := executingOp("op", scaleOp(1), []v1alpha1.CommittedTarget{target})
			for i := range tc.prior {
				tc.prior[i].UID = a.UID
			}
			op.Status.Targets = tc.prior
			g := newOpRig(t, f, op)
			if tc.gone {
				delete(f.allocs, "alloc-p")
				if err := g.c.Delete(g.ctx, a); err != nil {
					t.Fatal(err)
				}
			}
			p := f.pass()
			p.r, p.ctx, p.now, p.mem = g.r, g.ctx, opAt, g.r.memory()
			out, err := p.outcomes(g.op("op"), nil, nil, true)
			if err != nil {
				t.Fatal(err)
			}
			if len(out) != 1 || out[0].State != tc.wantState {
				t.Fatalf("outcomes %+v, want the one target %s", out, tc.wantState)
			}
			if tc.wantState == v1alpha1.TargetStateFailed && (out[0].Reason != reasonJobStartedSinceCommit || out[0].UID != types.UID(a.UID)) {
				t.Errorf("the failure is %+v, want %s", out[0], reasonJobStartedSinceCommit)
			}
			if len(tc.prior) == 1 && (!out[0].LastTransitionTime.Time.Equal(opT0) || out[0].Message != "kept words") {
				t.Errorf("the recorded failure changed: %+v", out[0])
			}
		})
	}
	_ = time.Second
}
