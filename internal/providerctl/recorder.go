package providerctl

import (
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/crossplane/crossplane-runtime/v2/pkg/event"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// reasonCreated is the reason of crossplane-runtime's Normal event "Successfully
// requested creation of external resource", which its reconciler emits after
// every Create that returns no error (managed/reconciler.go:92 and :1473 in
// v2.4.3). The constant is not exported.
const reasonCreated event.Reason = "CreatedExternalResource"

// createdFilter wraps the recorder that crossplane-runtime's reconciler writes
// its events to. A Create that records a refusal for a full submit limit
// returns no error (external.rejected), so that Synced does not flip at every
// backoff; the reconciler then reports a creation that did not happen, once
// per attempt, for as long as the slot stays full. The filter drops that one
// event when the Allocation it names has a current submission record that is
// Rejected with the code SubmitLimitReached: the gated SubmissionRejected event
// reports the refusal (HBEP-8, Repeating conditions). Every other event, and
// the same event for a creation that did happen, passes through unchanged.
type createdFilter struct{ inner event.Recorder }

var _ event.Recorder = createdFilter{}

// Event implements event.Recorder.
func (f createdFilter) Event(obj runtime.Object, e event.Event) {
	if e.Reason == reasonCreated && e.Type == event.Type(provider.EventKindOrdinary) && refusedForAFullSlot(obj) {
		return
	}
	f.inner.Event(obj, e)
}

// WithAnnotations implements event.Recorder; the reconciler asks for a recorder
// with the external name before it emits any event of a Create.
func (f createdFilter) WithAnnotations(keysAndValues ...string) event.Recorder {
	return createdFilter{inner: f.inner.WithAnnotations(keysAndValues...)}
}

// refusedForAFullSlot reports whether the object is an Allocation whose current
// submission record is a Rejected attempt with the code SubmitLimitReached. A
// record that cannot be read is not one.
func refusedForAFullSlot(obj runtime.Object) bool {
	a, ok := obj.(*v1alpha1.Allocation)
	if !ok {
		return false
	}
	rec, err := readRecord(a)
	return err == nil && rec.sub != nil && rec.sub.State == v1alpha1.SubmissionRejected && rec.sub.Code == v1alpha1.ReasonSubmitLimitReached
}
