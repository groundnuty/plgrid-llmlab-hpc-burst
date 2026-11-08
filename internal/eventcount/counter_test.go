package eventcount

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/crossplane/crossplane-runtime/v2/pkg/event"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// value reads one series of c through a registry, as a scrape would: the
// count, and whether the series exists. Reading it through the CounterVec
// would create it.
func value(t *testing.T, c *Counter, k Key) (float64, bool) {
	t.Helper()
	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(c)
	fams, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range fams {
		if f.GetName() != Name {
			continue
		}
		for _, m := range f.GetMetric() {
			if matches(m, k) {
				return m.GetCounter().GetValue(), true
			}
		}
	}
	return 0, false
}

// handOver is one call an inner recorder received, with the count of the
// call's own series read at the moment of the hand-over.
type handOver struct {
	method                   string
	reason, action, typ, msg string
	annotations              map[string]string
	countAtHandOver          float64
}

// innerEvents stands for client-go's events.k8s.io recorder: it records each
// call and reads the counter at the hand-over.
type innerEvents struct {
	t          *testing.T
	c          *Counter
	controller string
	mu         sync.Mutex
	got        []handOver
}

func (f *innerEvents) Eventf(_ runtime.Object, _ runtime.Object, eventtype, reason, action, note string, args ...interface{}) {
	a := action
	if a == "" {
		a = NoAction
	}
	n, _ := value(f.t, f.c, Key{f.controller, reason, a, eventtype})
	f.mu.Lock()
	defer f.mu.Unlock()
	f.got = append(f.got, handOver{method: "Eventf", reason: reason, action: action, typ: eventtype, msg: fmt.Sprintf(note, args...), countAtHandOver: n})
}

// innerLegacy stands for client-go's core/v1 recorder.
type innerLegacy struct {
	t          *testing.T
	c          *Counter
	controller string
	got        []handOver
}

func (f *innerLegacy) at(reason, typ string) float64 {
	n, _ := value(f.t, f.c, Key{f.controller, reason, NoAction, typ})
	return n
}

func (f *innerLegacy) Event(_ runtime.Object, eventtype, reason, message string) {
	f.got = append(f.got, handOver{method: "Event", reason: reason, typ: eventtype, msg: message, countAtHandOver: f.at(reason, eventtype)})
}

func (f *innerLegacy) Eventf(_ runtime.Object, eventtype, reason, messageFmt string, args ...interface{}) {
	f.got = append(f.got, handOver{method: "Eventf", reason: reason, typ: eventtype, msg: fmt.Sprintf(messageFmt, args...), countAtHandOver: f.at(reason, eventtype)})
}

func (f *innerLegacy) AnnotatedEventf(_ runtime.Object, annotations map[string]string, eventtype, reason, messageFmt string, args ...interface{}) {
	f.got = append(f.got, handOver{method: "AnnotatedEventf", reason: reason, typ: eventtype, msg: fmt.Sprintf(messageFmt, args...),
		annotations: annotations, countAtHandOver: f.at(reason, eventtype)})
}

var subject = &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "subject"}}

// TestEventsCountsInTheCallThatHandsOver: each event handed to the
// events.k8s.io recorder adds one to its series, with the reason, action and
// type it carries, before the recorder's queue (the count has risen when the
// recorder receives the event) and in the same call (it has risen once the
// call returns). The expected labels are the arguments, not the code's.
func TestEventsCountsInTheCallThatHandsOver(t *testing.T) {
	c := New()
	inner := &innerEvents{t: t, c: c, controller: ControllerLifecycle}
	r := Events(inner, c, ControllerLifecycle)
	calls := []struct{ typ, reason, action string }{
		{provider.EventKindWarning, "TunnelOrphaned", "degraded"},
		{provider.EventKindOrdinary, "Ready", "ready"},
		{provider.EventKindWarning, "TunnelOrphaned", "degraded"},
		{provider.EventKindWarning, "RestartLoop", "failing"},
		{provider.EventKindWarning, "RestartLoop", "degraded"},
	}
	want := map[Key]float64{}
	for i, call := range calls {
		k := Key{ControllerLifecycle, call.reason, call.action, call.typ}
		before, _ := value(t, c, k)
		r.Eventf(subject, nil, call.typ, call.reason, call.action, "note %d", i)
		want[k]++
		after, ok := value(t, c, k)
		if !ok || after != before+1 {
			t.Fatalf("call %d (%s): the series reads %g (present %v) after the call, want %g", i, k, after, ok, before+1)
		}
		got := inner.got[len(inner.got)-1]
		if len(inner.got) != i+1 || got.reason != call.reason || got.action != call.action || got.typ != call.typ || got.msg != fmt.Sprintf("note %d", i) {
			t.Fatalf("call %d: the recorder received %d calls, the last %+v; want the call handed over unchanged", i, len(inner.got), got)
		}
		if got.countAtHandOver != before+1 {
			t.Fatalf("call %d (%s): the count was %g when the recorder received the event, want %g: it is counted before the recorder's queue", i, k, got.countAtHandOver, before+1)
		}
	}
	for k, n := range want {
		if got, _ := value(t, c, k); got != n {
			t.Errorf("%s = %g, want %g", k, got, n)
		}
	}
}

// TestEventsCountsAnEmptyActionAsNone: an event with no action is counted
// under action "none".
func TestEventsCountsAnEmptyActionAsNone(t *testing.T) {
	c := New()
	inner := &innerEvents{t: t, c: c, controller: ControllerFront}
	Events(inner, c, ControllerFront).Eventf(subject, nil, provider.EventKindOrdinary, "Something", "", "n")
	if got, ok := value(t, c, Key{ControllerFront, "Something", NoAction, provider.EventKindOrdinary}); !ok || got != 1 {
		t.Fatalf("the series with action none reads %g (present %v), want 1", got, ok)
	}
}

// TestLegacyCountsEachMethod: each of the core/v1 recorder's three methods
// adds one, with action none, before it hands the event over.
func TestLegacyCountsEachMethod(t *testing.T) {
	c := New()
	ctl := ControllerProvider("example-batch")
	inner := &innerLegacy{t: t, c: c, controller: ctl}
	r := Legacy(inner, c, ctl)
	r.Event(subject, provider.EventKindOrdinary, "CreatedExternalResource", "m")
	r.Eventf(subject, provider.EventKindWarning, "CannotObserveExternalResource", "m %d", 1)
	r.AnnotatedEventf(subject, map[string]string{"k": "v"}, provider.EventKindWarning, "CannotObserveExternalResource", "m %d", 2)
	wantCalls := []struct {
		method, reason, typ string
		at                  float64
	}{
		{"Event", "CreatedExternalResource", provider.EventKindOrdinary, 1},
		{"Eventf", "CannotObserveExternalResource", provider.EventKindWarning, 1},
		{"AnnotatedEventf", "CannotObserveExternalResource", provider.EventKindWarning, 2},
	}
	if len(inner.got) != len(wantCalls) {
		t.Fatalf("the recorder received %d calls, want %d", len(inner.got), len(wantCalls))
	}
	for i, w := range wantCalls {
		g := inner.got[i]
		if g.method != w.method || g.reason != w.reason || g.typ != w.typ || g.countAtHandOver != w.at {
			t.Errorf("call %d: %+v, want %s %s %s with the count at %g at the hand-over", i, g, w.method, w.reason, w.typ, w.at)
		}
	}
	if got, _ := value(t, c, Key{ctl, "CreatedExternalResource", NoAction, provider.EventKindOrdinary}); got != 1 {
		t.Errorf("CreatedExternalResource = %g, want 1", got)
	}
	if got, _ := value(t, c, Key{ctl, "CannotObserveExternalResource", NoAction, provider.EventKindWarning}); got != 2 {
		t.Errorf("CannotObserveExternalResource = %g, want 2", got)
	}
}

// TestLegacyCountsThroughCrossplanesWithAnnotations: crossplane-runtime's
// APIRecorder over the wrapper, as cmd/hpcburst-controller builds the batch
// Provider's, counts an event raised through WithAnnotations, which builds a
// new APIRecorder over the same inner recorder (HBEP-8, Test plan).
func TestLegacyCountsThroughCrossplanesWithAnnotations(t *testing.T) {
	c := New()
	ctl := ControllerProvider("example-batch")
	inner := &innerLegacy{t: t, c: c, controller: ctl}
	rec := event.NewAPIRecorder(Legacy(inner, c, ctl))
	rec.WithAnnotations("external-name", "x").Event(subject, event.Normal("CreatedExternalResource", "m"))
	rec.Event(subject, event.Warning("CannotCreateExternalResource", fmt.Errorf("refused")))
	if got, _ := value(t, c, Key{ctl, "CreatedExternalResource", NoAction, provider.EventKindOrdinary}); got != 1 {
		t.Errorf("the annotated CreatedExternalResource = %g, want 1", got)
	}
	if got, _ := value(t, c, Key{ctl, "CannotCreateExternalResource", NoAction, provider.EventKindWarning}); got != 1 {
		t.Errorf("CannotCreateExternalResource = %g, want 1", got)
	}
	if len(inner.got) != 2 || inner.got[0].annotations["external-name"] != "x" {
		t.Fatalf("the recorder received %+v, want two events, the first annotated", inner.got)
	}
}

// TestZeroCreatesTheSeriesAtZero: Zero creates each series at 0 and keeps the
// count of one that exists.
func TestZeroCreatesTheSeriesAtZero(t *testing.T) {
	c := New()
	series := []Series{{"Ready", "ready", provider.EventKindOrdinary}, {"RestartLoop", "failing", provider.EventKindWarning}}
	c.Zero(ControllerLifecycle, series)
	for _, s := range series {
		if got, ok := value(t, c, Key{ControllerLifecycle, s.Reason, s.Action, s.Type}); !ok || got != 0 {
			t.Errorf("%+v reads %g (present %v), want 0", s, got, ok)
		}
	}
	inner := &innerEvents{t: t, c: c, controller: ControllerLifecycle}
	Events(inner, c, ControllerLifecycle).Eventf(subject, nil, provider.EventKindOrdinary, "Ready", "ready", "n")
	c.Zero(ControllerLifecycle, series)
	if got, _ := value(t, c, Key{ControllerLifecycle, "Ready", "ready", provider.EventKindOrdinary}); got != 1 {
		t.Errorf("Ready reads %g after a second Zero, want 1", got)
	}
}

// TestTheCounterHasHBEP8sNameAndHelp: the exposition carries HBEP-8's name,
// help text and labels.
func TestTheCounterHasHBEP8sNameAndHelp(t *testing.T) {
	c := New()
	c.Zero(ControllerFront, []Series{{"UsageWriteFailed", "failing", provider.EventKindWarning}})
	want := `# HELP hpcburst_events_raised_total events handed to the event recorder, not events delivered: the recorder may aggregate or drop them
# TYPE hpcburst_events_raised_total counter
hpcburst_events_raised_total{action="failing",controller="front",reason="UsageWriteFailed",type="Warning"} 0
`
	if err := testutil.CollectAndCompare(c, strings.NewReader(want)); err != nil {
		t.Fatal(err)
	}
}

// TestTheWrappersRefuseNoCounter: a wrapper with no Counter or no controller
// would hand events over uncounted, so it is refused at once.
func TestTheWrappersRefuseNoCounter(t *testing.T) {
	inner := &innerEvents{t: t}
	legacy := &innerLegacy{t: t}
	for name, build := range map[string]func(){
		"Events with no Counter":    func() { Events(inner, nil, ControllerLifecycle) },
		"Events with no controller": func() { Events(inner, New(), "") },
		"Events with no recorder":   func() { Events(nil, New(), ControllerLifecycle) },
		"Legacy with no Counter":    func() { Legacy(legacy, nil, "provider-x") },
		"Legacy with no controller": func() { Legacy(legacy, New(), "") },
		"Legacy with no recorder":   func() { Legacy(nil, New(), "provider-x") },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s: no panic", name)
				}
			}()
			build()
		}()
	}
}
