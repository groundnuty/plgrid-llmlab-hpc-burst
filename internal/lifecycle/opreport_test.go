package lifecycle

import (
	"errors"
	"strings"
	"testing"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	corev1 "k8s.io/api/core/v1"
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
