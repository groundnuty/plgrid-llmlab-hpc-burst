package e2e

// A rule that the write it guards never exercises proves nothing (the science agent's ruling on #664's Astra review): the rules on
// status.countGivenBack sit on status and on the record, so a write that removes status ITSELF ran none of them. Three rules now
// guard the record, each with a write of its own that only it refuses: the rule at the Operation's root refuses removing the status
// that holds it (subtests A and C), the rule on status refuses dropping the record while status stays (B), and the rule on the
// record refuses changing it (TestOperationCountGivenBackIsSetOnce). Each row sends a real write to the status subresource of an
// Operation whose record is set, and every one must be refused, with the API server's Invalid error naming the rule.

import (
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// rootRule is the message of the rule at the Operation's root, which guards the removal of the whole status.
const rootRule = "removing the status that holds it is refused"

// operationWithARecord creates an Operation whose status.countGivenBack is set (one unit given back, the count written 2).
func operationWithARecord(t *testing.T) *v1alpha1.Operation {
	t.Helper()
	ns := newNamespace(t)
	op := newOperation(ns, uniq("op"))
	must(t, c.Create(bg(), op), "create Operation")
	two := int32(2)
	got := &v1alpha1.Operation{}
	must(t, c.Get(bg(), objKey(op), got), "get Operation")
	got.Status.CountGivenBack = &v1alpha1.CountGivenBack{Units: 1, Instances: &two}
	must(t, c.Status().Update(bg(), got), "the record")
	return got
}

// recordIsStill fails when the stored Operation's record is not the one written.
func recordIsStill(t *testing.T, op *v1alpha1.Operation) {
	t.Helper()
	got := &v1alpha1.Operation{}
	must(t, c.Get(bg(), objKey(op), got), "get Operation")
	rec := got.Status.CountGivenBack
	if rec == nil || rec.Units != 1 || rec.Instances == nil || *rec.Instances != 2 {
		t.Errorf("the stored record is %+v, want {Units:1 Instances:2}: it was removed or replaced", rec)
	}
}

// refusedAsInvalid fails unless err is the API server's Invalid refusal carrying the message of the rule that must refuse the
// write (the phrase), which names the record; the failure line says what the write did instead.
func refusedAsInvalid(t *testing.T, what, phrase string, err error) {
	t.Helper()
	switch {
	case err == nil:
		t.Errorf("%s was ACCEPTED; want an Invalid refusal naming status.countGivenBack", what)
	case !apierrors.IsInvalid(err):
		t.Errorf("%s failed, but not as Invalid: %T: %v", what, err, err)
	case !strings.Contains(err.Error(), "countGivenBack") || !strings.Contains(err.Error(), phrase):
		t.Errorf("%s was refused, but not by the rule that guards it (want %q): %v", what, phrase, err)
	}
}

// The three ways a write takes status away from an Operation whose record is set. Each is its own Operation, so a bypass in one
// leaves the others' state alone.
func TestOperationStatusRemovalDoesNotBypassTheCountGivenBackRules(t *testing.T) {
	t.Run("a merge patch of status:null through the status subresource, then a replacement of the record", func(t *testing.T) {
		op := operationWithARecord(t)
		patch := client.RawPatch(types.MergePatchType, []byte(`{"status":null}`))
		err := c.Status().Patch(bg(), op.DeepCopy(), patch)
		refusedAsInvalid(t, `the merge patch {"status":null} on /status`, rootRule, err)
		// With the status gone a record can be written afresh, and a different one with it: the second write.
		three := int32(3)
		got := &v1alpha1.Operation{}
		must(t, c.Get(bg(), objKey(op), got), "get Operation")
		got.Status.CountGivenBack = &v1alpha1.CountGivenBack{Units: 2, Instances: &three}
		refusedAsInvalid(t, "the later replacement of the record (units 2, count written 3) through /status", "it cannot be changed", c.Status().Update(bg(), got))
		recordIsStill(t, op)
	})

	t.Run("a status update that drops the record and keeps the rest", func(t *testing.T) {
		op := operationWithARecord(t)
		got := &v1alpha1.Operation{}
		must(t, c.Get(bg(), objKey(op), got), "get Operation")
		got.Status.Phase = v1alpha1.OperationPhaseExecuting // the rest of the status is kept, and moves
		got.Status.CountGivenBack = nil
		err := c.Status().Update(bg(), got)
		refusedAsInvalid(t, "the /status update with the record dropped and the rest kept", "it cannot be removed", err)
		if err != nil {
			t.Logf("refused: %v", err)
			if strings.Contains(err.Error(), rootRule) {
				t.Errorf("the root rule refused a write that keeps status: it must guard whole-status removal only, or the rule on status has no write of its own: %v", err)
			}
		}
		recordIsStill(t, op)
	})

	t.Run("a status update of an object with no status at all", func(t *testing.T) {
		op := operationWithARecord(t)
		u := &unstructured.Unstructured{}
		u.SetGroupVersionKind(v1alpha1.GroupVersion.WithKind("Operation"))
		must(t, c.Get(bg(), objKey(op), u), "get Operation")
		unstructured.RemoveNestedField(u.Object, "status")
		refusedAsInvalid(t, "the /status update of an object with no status", rootRule, c.Status().Update(bg(), u))
		recordIsStill(t, op)
	})
}
