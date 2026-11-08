package front

import (
	"fmt"
	"sync"
	"unicode/utf8"

	coordinationv1 "k8s.io/api/coordination/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/events"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventcount"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// HBEP-8's event type the front emits, and its reason codes. CapacityDropped
// and DrainWindowShort are the contracts' (api/v1alpha1/conditions.go);
// CountsDrift is the front's: HBEP-5 names the event (Operational readiness
// 3) but no reason, so this name awaits the contracts owner.
const (
	eventDegraded       = "degraded"
	eventFailing        = "failing" // HBEP-8's type of UsageWriteFailed
	reasonCountsDrift   = "CountsDrift"
	reasonCapacityDrop  = v1alpha1.ReasonCapacityDropped
	reasonDrainWinShort = v1alpha1.ReasonDrainWindowShort
	// reasonUsageWriteFailed: a usage record could not be written and synced
	// (@hpc-burst-science-agent, 31 Oct 2026; HBEP-8 will list it).
	reasonUsageWriteFailed = "UsageWriteFailed"
	// reasonHeartbeatMissed: the front's end-to-end probe has given no result
	// for more than three probe intervals (proposed in HBEP-8, Observers).
	reasonHeartbeatMissed = "HeartbeatMissed"
)

// maxNoteBytes is the API's limit on an event's note (HBEP-8, Events, Form):
// the API refuses a longer one, and the recorder does not retry a refusal.
const maxNoteBytes = 1024

// cutNote cuts a note to maxNoteBytes at a character boundary, so the result
// is valid UTF-8.
func cutNote(s string) string {
	if len(s) <= maxNoteBytes {
		return s
	}
	cut := maxNoteBytes
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}

// Recorder emits HBEP-8's events. Events are best effort and never block a
// request. Messages carry names, counts and times, never a key or a body.
type Recorder interface {
	Event(obj runtime.Object, warning bool, typ, code, message string)
}

// KubeRecorder emits events.k8s.io Events whose reason is the CamelCase code
// alone and whose action is HBEP-8's type (@hpc-burst-science-agent's HBEP-8
// draft, 31 Oct 2026; it replaces the "<type>/<code>" reason).
type KubeRecorder struct {
	R events.EventRecorder
	// Counter is the raised-events counter R counts into, when
	// NewKubeRecorder built R. Run exports this one on the front's /metrics,
	// so the recorder and the export cannot name two counters (HBEP-8,
	// Raised events).
	Counter *eventcount.Counter
}

// Event implements Recorder.
func (k KubeRecorder) Event(obj runtime.Object, warning bool, typ, code, message string) {
	et := provider.EventKindOrdinary
	if warning {
		et = provider.EventKindWarning
	}
	k.R.Eventf(obj, nil, et, code, typ, "%s", cutNote(message))
}

// EventsCounter returns the raised-events counter the recorder counts into,
// nil when NewKubeRecorder did not build it. It is a method of the value, so
// a *KubeRecorder, which Options.Recorder accepts as well, has it too.
func (k KubeRecorder) EventsCounter() *eventcount.Counter { return k.Counter }

// countingRecorder is a Recorder that names the raised-events counter it
// counts into: a KubeRecorder, or a pointer to one.
type countingRecorder interface {
	Recorder
	EventsCounter() *eventcount.Counter
}

// raisedEvents is the counter the front's /metrics exports: the one its
// recorder counts into, when the recorder names one (a KubeRecorder that
// NewKubeRecorder built, passed as a value or a pointer), so the two cannot
// come apart; otherwise a new one, into which nothing counts (a test's
// MemoryRecorder).
func raisedEvents(rec Recorder) *eventcount.Counter {
	if k, ok := rec.(*KubeRecorder); ok && k == nil {
		return eventcount.New() // a nil pointer names no counter; its Event would panic first
	}
	if k, ok := rec.(countingRecorder); ok {
		if c := k.EventsCounter(); c != nil {
			return c
		}
	}
	return eventcount.New()
}

// RaisedEvents returns the series the front can raise, each created at 0 on
// its /metrics (HBEP-8, Raised events): the rows of HBEP-8's Reasons table
// whose emitter is the front, with ObservationGap at its start and
// HeartbeatMissed for its probe and the lifecycle Lease. Every one is
// degraded or failing, so every one is a Warning. The drift guard checks this
// list against the emit sites.
func RaisedEvents() []eventcount.Series {
	var out []eventcount.Series
	for _, r := range []struct{ action, reason string }{
		{eventDegraded, reasonCapacityDrop}, {eventDegraded, reasonCountsDrift}, {eventDegraded, reasonDrainWinShort},
		{eventDegraded, reasonPathProbeFailing}, {eventDegraded, reasonObservationGap},
		{eventFailing, reasonHeartbeatMissed}, {eventFailing, reasonUsageWriteFailed},
	} {
		out = append(out, eventcount.Series{Reason: r.reason, Action: r.action, Type: provider.EventKindWarning})
	}
	return out
}

// NewKubeRecorder starts an events broadcaster on the clientset and returns
// its recorder and a stop function that flushes it. The recorder counts each
// event it hands over in a raised-events counter of its own, one per front,
// which it carries as Counter for Run to export (HBEP-8, Raised events).
func NewKubeRecorder(cs kubernetes.Interface) (KubeRecorder, func(), error) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		return KubeRecorder{}, nil, err
	}
	// Events about a Lease need its kind: the recorder drops an event whose
	// reference it cannot build (client-go tools/events event_recorder.go).
	if err := coordinationv1.AddToScheme(scheme); err != nil {
		return KubeRecorder{}, nil, err
	}
	b := events.NewBroadcaster(&events.EventSinkImpl{Interface: cs.EventsV1()})
	stop := make(chan struct{})
	b.StartRecordingToSink(stop)
	counted := eventcount.New()
	return KubeRecorder{R: eventcount.Events(b.NewRecorder(scheme, fieldManagerFront), counted, eventcount.ControllerFront), Counter: counted},
		func() { close(stop); b.Shutdown() }, nil
}

// RecordedEvent is one event a MemoryRecorder holds.
type RecordedEvent struct {
	Object  string // Go type, namespace and name
	Warning bool
	Type    string
	Code    string
	Message string
}

// MemoryRecorder keeps every event, for tests.
type MemoryRecorder struct {
	mu     sync.Mutex
	events []RecordedEvent
}

// Event implements Recorder.
func (m *MemoryRecorder) Event(obj runtime.Object, warning bool, typ, code, message string) {
	name := fmt.Sprintf("%T", obj)
	if o, ok := obj.(interface {
		GetNamespace() string
		GetName() string
	}); ok {
		name += " " + o.GetNamespace() + "/" + o.GetName()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, RecordedEvent{Object: name, Warning: warning, Type: typ, Code: code, Message: message})
}

// Events returns a copy, oldest first.
func (m *MemoryRecorder) Events() []RecordedEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]RecordedEvent(nil), m.events...)
}

// Find returns the events of one type and code.
func (m *MemoryRecorder) Find(typ, code string) []RecordedEvent {
	var out []RecordedEvent
	for _, e := range m.Events() {
		if e.Type == typ && e.Code == code {
			out = append(out, e)
		}
	}
	return out
}
