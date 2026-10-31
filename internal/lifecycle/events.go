package lifecycle

import (
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Clock is the controller's time source. fake.Clock of pkg/provider/fake
// satisfies it; production uses SystemClock.
type Clock interface{ Now() time.Time }

// SystemClock reads the wall clock, in UTC.
type SystemClock struct{}

// Now returns the current time in UTC.
func (SystemClock) Now() time.Time { return time.Now().UTC() }

// Recorder emits HBEP-8's events. Events are best-effort; conditions are the
// level truth (HBEP-7, Events and conditions). typ is one of HBEP-8's types
// (eventFailing and the others), code its reason code.
type Recorder interface {
	Event(obj client.Object, warning bool, typ, code, message string)
}

// KubeRecorder emits Kubernetes Events through client-go's events recorder,
// with reason "<type>/<code>" and the type as the action.
type KubeRecorder struct{ R events.EventRecorder }

// Event implements Recorder.
func (k KubeRecorder) Event(obj client.Object, warning bool, typ, code, message string) {
	et := corev1.EventTypeNormal
	if warning {
		et = corev1.EventTypeWarning
	}
	k.R.Eventf(obj, nil, et, typ+"/"+code, typ, "%s", message)
}

// RecordedEvent is one event a MemoryRecorder holds.
type RecordedEvent struct {
	Seq       int
	Kind      string
	Namespace string
	Name      string
	Warning   bool
	Type      string
	Code      string
	Message   string
}

// MemoryRecorder keeps every event in order, for tests and diagnostics. It is
// safe for concurrent use.
type MemoryRecorder struct {
	mu     sync.Mutex
	events []RecordedEvent
}

// Event implements Recorder.
func (m *MemoryRecorder) Event(obj client.Object, warning bool, typ, code, message string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	kind := fmt.Sprintf("%T", obj)
	m.events = append(m.events, RecordedEvent{
		Seq: len(m.events) + 1, Kind: kind, Namespace: obj.GetNamespace(), Name: obj.GetName(),
		Warning: warning, Type: typ, Code: code, Message: message,
	})
}

// Events returns a copy of the events, oldest first.
func (m *MemoryRecorder) Events() []RecordedEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]RecordedEvent(nil), m.events...)
}

// Find returns the events of one type and code, oldest first; an empty code
// matches every code of the type.
func (m *MemoryRecorder) Find(typ, code string) []RecordedEvent {
	var out []RecordedEvent
	for _, e := range m.Events() {
		if e.Type == typ && (code == "" || e.Code == code) {
			out = append(out, e)
		}
	}
	return out
}
