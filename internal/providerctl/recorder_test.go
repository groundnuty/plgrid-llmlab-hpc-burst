package providerctl

import (
	"testing"

	"github.com/crossplane/crossplane-runtime/v2/pkg/event"
	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	clientrecord "k8s.io/client-go/tools/record"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventcount"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
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

// TestCreatedFilterCountsOnlyWhatItPasses: the batch Provider's recorder, as
// cmd/hpcburst-controller builds it, counts each event inside the created
// filter (HBEP-8, Raised events): crossplane-runtime's APIRecorder over
// eventcount.Legacy over client-go's recorder, with the filter around it. An
// event raised through WithAnnotations is counted once, under provider-<name>
// with action none and its type; the CreatedExternalResource the filter
// suppresses is not counted (HBEP-8, Test plan, Raised events: Counting).
func TestCreatedFilterCountsOnlyWhatItPasses(t *testing.T) {
	full := &v1alpha1.Allocation{}
	full.Annotations = map[string]string{v1alpha1.AnnotationSubmission: recordValue(v1alpha1.SubmissionRecord{
		Attempt: 1, Token: "t", State: v1alpha1.SubmissionRejected, Code: v1alpha1.ReasonSubmitLimitReached})}
	accepted := &v1alpha1.Allocation{}
	accepted.Annotations = map[string]string{v1alpha1.AnnotationSubmission: recordValue(v1alpha1.SubmissionRecord{
		Attempt: 1, Token: "t", State: v1alpha1.SubmissionDone})}
	created := event.Normal(reasonCreated, "Successfully requested creation of external resource")
	ctl := eventcount.ControllerProvider("example-batch")
	for _, c := range []struct {
		name    string
		obj     *v1alpha1.Allocation
		ev      event.Event
		typ     string
		counted float64
	}{
		{"a suppressed creation", full, created, provider.EventKindOrdinary, 0},
		{"a creation that happened", accepted, created, provider.EventKindOrdinary, 1},
		{"a Warning on a full slot", full, event.Warning("CannotCreateExternalResource", errNotAllocation), provider.EventKindWarning, 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			counter := eventcount.New()
			inner := clientrecord.NewFakeRecorder(10)
			createdFilter{inner: event.NewAPIRecorder(eventcount.Legacy(inner, counter, ctl))}.WithAnnotations("external-name", "x").Event(c.obj, c.ev)
			k := eventcount.Key{Controller: ctl, Reason: string(c.ev.Reason), Action: eventcount.NoAction, Type: c.typ}
			got := seriesValue(t, counter, k)
			if got != c.counted || float64(len(inner.Events)) != c.counted {
				t.Fatalf("%s = %g with %d events handed over, want %g and %g", k, got, len(inner.Events), c.counted, c.counted)
			}
		})
	}
}

// seriesValue reads one series of the counter through a registry, as a
// scrape would; an absent series reads 0 (a batch Provider's series appear at
// its first event).
func seriesValue(t *testing.T, c *eventcount.Counter, k eventcount.Key) float64 {
	t.Helper()
	reg := prometheus.NewRegistry()
	reg.MustRegister(c)
	fams, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range fams {
		for _, m := range f.GetMetric() {
			got := map[string]string{}
			for _, l := range m.GetLabel() {
				got[l.GetName()] = l.GetValue()
			}
			if got[eventcount.LabelController] == k.Controller && got[eventcount.LabelReason] == k.Reason &&
				got[eventcount.LabelAction] == k.Action && got[eventcount.LabelType] == k.Type {
				return m.GetCounter().GetValue()
			}
		}
	}
	return 0
}
