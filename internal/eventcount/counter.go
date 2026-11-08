// Package eventcount counts the events each controller hands to its event
// recorder: HBEP-8's raised-events counter, hpcburst_events_raised_total
// (Metrics, Controller metrics; `@hpc-burst-science-agent`, 8 Nov 2026, for
// #256).
//
// client-go's event recorders deliver asynchronously, so a list of the Events
// the API holds cannot prove that an event was not raised. The counter can: it
// rises in the same call that hands the event to the recorder, before the
// recorder's queue, so it is final once the emitting code has returned. Events
// the recorder later merges or drops are still counted, which the help text
// says.
//
// The count is taken by a wrapper where each recorder is built (Events for
// client-go's events.k8s.io recorder, Legacy for its core/v1 one), so no emit
// site can raise an event uncounted; the drift guard (internal/reasondrift)
// fails on a recorder built without one. Each copy of a manager has one
// Counter, never a package-level one, and it is exported through the copy's
// serving collector (the lifecycle controller) or the front's own registry,
// never registered on its own.
package eventcount

import (
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/tools/record"
)

// The counter's name and help text, HBEP-8's.
const (
	Name = "hpcburst_events_raised_total"
	Help = "events handed to the event recorder, not events delivered: the recorder may aggregate or drop them"
)

// Label names, in the counter's order.
const (
	LabelController = "controller"
	LabelReason     = "reason"
	LabelAction     = "action"
	LabelType       = "type"
)

// NoAction is the action label of an event that has no action: one raised
// through client-go's core/v1 recorder (Legacy), the Provider controller's.
const NoAction = "none"

// The controller label's values (HBEP-8, Raised events).
const (
	ControllerLifecycle = "lifecycle"
	ControllerFront     = "front"
)

// ControllerProvider is the controller label of a Provider's manager, a batch
// or a managed one: provider-<Provider name>.
func ControllerProvider(provider string) string { return "provider-" + provider }

// Series is one series a controller can raise, without its controller: the
// reason code, HBEP-8's type (the action), and the Kubernetes event type
// (Normal or Warning).
type Series struct {
	Reason, Action, Type string
}

// Counter is one manager copy's hpcburst_events_raised_total. It is a
// prometheus.Collector so that the collector that exports it (the lifecycle
// controller's serving collector, the front's registry) can describe and
// collect it; it is never registered on its own. It is safe for concurrent
// use.
type Counter struct {
	vec *prometheus.CounterVec
}

// New returns a Counter with no series.
func New() *Counter {
	return &Counter{vec: prometheus.NewCounterVec(prometheus.CounterOpts{Name: Name, Help: Help},
		[]string{LabelController, LabelReason, LabelAction, LabelType})}
}

// Describe implements prometheus.Collector.
func (c *Counter) Describe(ch chan<- *prometheus.Desc) { c.vec.Describe(ch) }

// Collect implements prometheus.Collector.
func (c *Counter) Collect(ch chan<- prometheus.Metric) { c.vec.Collect(ch) }

// Zero creates the controller's series at 0, so that an event never raised
// shows a real 0 rather than an absent series (HBEP-8, Raised events: series
// are created at 0 for every reason, action and type that the lifecycle
// controller, the front and a managed Provider can raise). A series that
// exists already keeps its count.
func (c *Counter) Zero(controller string, series []Series) {
	for _, s := range series {
		c.vec.WithLabelValues(controller, s.Reason, s.Action, s.Type)
	}
}

// count adds one event to its series.
func (c *Counter) count(controller, reason, action, eventtype string) {
	if action == "" {
		action = NoAction
	}
	c.vec.WithLabelValues(controller, reason, action, eventtype).Inc()
}

// mustCounter refuses a wrapper with no counter or no controller: one would
// hand events over uncounted, or count them under no controller.
func mustCounter(c *Counter, controller string) {
	if c == nil {
		panic("eventcount: a recorder wrapped with no Counter")
	}
	if controller == "" {
		panic("eventcount: a recorder wrapped with no controller label")
	}
}

// eventsRecorder counts each event, then hands it to client-go's events.k8s.io
// recorder, in the same call.
type eventsRecorder struct {
	inner      events.EventRecorder
	c          *Counter
	controller string
}

// Events wraps client-go's events.k8s.io recorder: each Eventf call counts the
// event under controller, with the reason, action and type it hands over, and
// then hands it over. It panics on a nil recorder or Counter, or an empty
// controller.
func Events(inner events.EventRecorder, c *Counter, controller string) events.EventRecorder {
	if inner == nil {
		panic("eventcount: Events wraps no recorder")
	}
	mustCounter(c, controller)
	return eventsRecorder{inner: inner, c: c, controller: controller}
}

// Eventf implements events.EventRecorder.
func (r eventsRecorder) Eventf(regarding runtime.Object, related runtime.Object, eventtype, reason, action, note string, args ...interface{}) {
	r.c.count(r.controller, reason, action, eventtype)
	r.inner.Eventf(regarding, related, eventtype, reason, action, note, args...)
}

// legacyRecorder counts each event, then hands it to client-go's core/v1
// recorder, in the same call. Such an event has no action.
type legacyRecorder struct {
	inner      record.EventRecorder
	c          *Counter
	controller string
}

// Legacy wraps client-go's core/v1 recorder, the one crossplane-runtime's
// event.APIRecorder writes through: Event, Eventf and AnnotatedEventf each
// count the event under controller, with action NoAction and the reason and
// type it hands over, and then hand it over. crossplane-runtime's
// WithAnnotations builds a new APIRecorder over the same inner recorder, so
// an annotated event is counted too. It panics on a nil recorder or Counter,
// or an empty controller.
func Legacy(inner record.EventRecorder, c *Counter, controller string) record.EventRecorder {
	if inner == nil {
		panic("eventcount: Legacy wraps no recorder")
	}
	mustCounter(c, controller)
	return legacyRecorder{inner: inner, c: c, controller: controller}
}

// Event implements record.EventRecorder.
func (r legacyRecorder) Event(object runtime.Object, eventtype, reason, message string) {
	r.c.count(r.controller, reason, NoAction, eventtype)
	r.inner.Event(object, eventtype, reason, message)
}

// Eventf implements record.EventRecorder.
func (r legacyRecorder) Eventf(object runtime.Object, eventtype, reason, messageFmt string, args ...interface{}) {
	r.c.count(r.controller, reason, NoAction, eventtype)
	r.inner.Eventf(object, eventtype, reason, messageFmt, args...)
}

// AnnotatedEventf implements record.EventRecorder.
func (r legacyRecorder) AnnotatedEventf(object runtime.Object, annotations map[string]string, eventtype, reason, messageFmt string, args ...interface{}) {
	r.c.count(r.controller, reason, NoAction, eventtype)
	r.inner.AnnotatedEventf(object, annotations, eventtype, reason, messageFmt, args...)
}
