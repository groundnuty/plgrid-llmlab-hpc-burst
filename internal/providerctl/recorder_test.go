package providerctl

import (
	"testing"

	"github.com/crossplane/crossplane-runtime/v2/pkg/event"
	corev1 "k8s.io/api/core/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// createdFilter drops crossplane's Normal CreatedExternalResource event only
// for an Allocation whose current record is a Rejected attempt with the code
// SubmitLimitReached; every other event passes, and so does the same event for
// every other record (a creation that did happen, or a plain refusal).
func TestCreatedFilterDropsOnlyTheEventOfARecordedFullSlot(t *testing.T) {
	alloc := func(sub *v1alpha1.SubmissionRecord) *v1alpha1.Allocation {
		a := &v1alpha1.Allocation{}
		if sub != nil {
			a.Annotations = map[string]string{v1alpha1.AnnotationSubmission: recordValue(*sub)}
		}
		return a
	}
	rejected := func(code string) *v1alpha1.SubmissionRecord {
		return &v1alpha1.SubmissionRecord{Attempt: 1, Token: "t", State: v1alpha1.SubmissionRejected, Code: code}
	}
	created := event.Normal(reasonCreated, "Successfully requested creation of external resource")
	for _, c := range []struct {
		name string
		obj  *v1alpha1.Allocation
		ev   event.Event
		drop bool
	}{
		{"created, a recorded full slot", alloc(rejected(v1alpha1.ReasonSubmitLimitReached)), created, true},
		{"created, a plain Rejected attempt", alloc(rejected("")), created, false},
		{"created, an accepted attempt", alloc(&v1alpha1.SubmissionRecord{Attempt: 1, Token: "t", State: v1alpha1.SubmissionDone}), created, false},
		{"created, an unknown attempt", alloc(&v1alpha1.SubmissionRecord{Attempt: 1, Token: "t", State: v1alpha1.SubmissionUnknown}), created, false},
		{"created, no record", alloc(nil), created, false},
		{"a Warning of the same reason, a recorded full slot", alloc(rejected(v1alpha1.ReasonSubmitLimitReached)), event.Warning(reasonCreated, errNotAllocation), false},
		{"another event, a recorded full slot", alloc(rejected(v1alpha1.ReasonSubmitLimitReached)), event.Warning("CannotCreateExternalResource", errNotAllocation), false},
		{"our own SubmissionRejected, a recorded full slot", alloc(rejected(v1alpha1.ReasonSubmitLimitReached)), event.Warning("SubmissionRejected", errNotAllocation), false},
	} {
		t.Run(c.name, func(t *testing.T) {
			rec := &recorder{}
			createdFilter{inner: rec}.WithAnnotations("external-name", "x").Event(c.obj, c.ev)
			if got := len(rec.reasons()); (got == 0) != c.drop {
				t.Errorf("%d events reached the recorder, dropped = %v, want dropped = %v", got, got == 0, c.drop)
			}
		})
	}
	// An object that is no Allocation passes.
	rec := &recorder{}
	createdFilter{inner: rec}.Event(&corev1.Pod{}, created)
	if len(rec.reasons()) != 1 {
		t.Error("an event about a Pod was dropped")
	}
}
