package lifecycle

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// Clock is the controller's time source. fake.Clock of pkg/provider/fake
// satisfies it; production uses SystemClock.
type Clock interface{ Now() time.Time }

// SystemClock reads the wall clock, in UTC.
type SystemClock struct{}

// Now returns the current time in UTC.
func (SystemClock) Now() time.Time { return time.Now().UTC() }

// EventDegraded is HBEP-8's degraded type for an emitter outside this package
// (the tunnel's TunnelOrphaned, which cmd/hpcburst-controller raises through
// the lifecycle controller's own recorder). KubeRecorder gives it the Warning
// severity.
const EventDegraded = eventDegraded

// Recorder emits HBEP-8's events. Events are best-effort; conditions are the
// level truth (HBEP-7, Events and conditions). regarding is the object the
// event is about and related, when not nil, another object it names (the
// Instance whose drain start dropped a ModelDeployment's capacity). typ is one
// of HBEP-8's types (eventFailing and the others), and it alone decides the
// severity (warningType); code is the reason code.
type Recorder interface {
	Event(regarding, related client.Object, typ, code, message string)
}

// maxNoteBytes is the longest note the events API admits: it refuses a longer
// one (k8s.io/kubernetes, pkg/apis/events/validation/events.go, lines 35-38).
const maxNoteBytes = 1024

// warningType reports whether an event of HBEP-8's type typ is a Warning:
// degraded and failing are, every other type is Normal.
func warningType(typ string) bool { return typ == eventDegraded || typ == eventFailing }

// eventNote cuts a message to at most maxNoteBytes, at a UTF-8 boundary, so
// the API server admits the event.
func eventNote(msg string) string {
	if len(msg) <= maxNoteBytes {
		return msg
	}
	cut := maxNoteBytes
	for cut > 0 && !utf8.RuneStart(msg[cut]) {
		cut--
	}
	return msg[:cut]
}

// KubeRecorder emits Kubernetes Events through client-go's events recorder.
// The event's reason is the reason code alone, a single CamelCase word, and
// HBEP-8's type is the events.k8s.io action, so a reader selects on either
// field without parsing the other out of a compound string.
type KubeRecorder struct{ R events.EventRecorder }

// Event implements Recorder.
func (k KubeRecorder) Event(regarding, related client.Object, typ, code, message string) {
	et := provider.EventKindOrdinary
	if warningType(typ) {
		et = provider.EventKindWarning
	}
	var rel runtime.Object
	if related != nil {
		rel = related
	}
	k.R.Eventf(regarding, rel, et, code, typ, "%s", eventNote(message))
}

// RecordedEvent is one event a MemoryRecorder holds.
type RecordedEvent struct {
	Seq       int
	Kind      string
	Namespace string
	Name      string
	Warning   bool
	// RelatedKind and RelatedName name the related object, empty for none.
	RelatedKind string
	RelatedName string
	Type        string
	Code        string
	Message     string
}

// Sequence numbers calls from several sources from one counter: HBEP-8's
// shared sequence, on which a test checks the order of a recorder call against
// a status write, which the API cannot show (HBEP-8, What the recorder merges
// and drops). The zero value starts at 1. It is safe for concurrent use.
type Sequence struct{ n atomic.Int64 }

// Next returns the next number.
func (s *Sequence) Next() int { return int(s.n.Add(1)) }

// MemoryRecorder keeps every event in order, for tests and diagnostics. It is
// safe for concurrent use.
type MemoryRecorder struct {
	// Seq, when set, numbers the events from a shared Sequence; otherwise
	// they are numbered from 1 on their own.
	Seq *Sequence

	mu     sync.Mutex
	events []RecordedEvent
}

// Event implements Recorder.
// It applies KubeRecorder's severity and note rules.
func (m *MemoryRecorder) Event(regarding, related client.Object, typ, code, message string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	seq := len(m.events) + 1
	if m.Seq != nil {
		seq = m.Seq.Next()
	}
	e := RecordedEvent{
		Seq: seq, Kind: fmt.Sprintf("%T", regarding), Namespace: regarding.GetNamespace(), Name: regarding.GetName(),
		Warning: warningType(typ), Type: typ, Code: code, Message: eventNote(message),
	}
	if related != nil {
		e.RelatedKind, e.RelatedName = fmt.Sprintf("%T", related), related.GetName()
	}
	m.events = append(m.events, e)
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
