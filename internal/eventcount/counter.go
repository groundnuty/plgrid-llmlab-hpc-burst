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
	"sort"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/tools/record"
)

// The counter's name and help text, HBEP-8's.
const (
	Name = "hpcburst_events_raised_total"
	Help = "events handed to the event recorder, not events delivered: the recorder may aggregate or drop them"
)

// The unlogged counter's name and help text (HBEP-8, The durable log, When it is down; #625).
// It exists only on a copy that runs with the durable log: without it nothing is published, so
// nothing can be missed.
const (
	UnloggedName = "hpcburst_events_unlogged_total"
	UnloggedHelp = "typed events the durable log did not take: the publisher's queue was full, its retries ran out, the events stream was full, the event had no subject, or the event's note was refused. They are in the cluster's own events only"
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
	// unlogged is hpcburst_events_unlogged_total{controller}, the events the durable log did not
	// take. A controller's series is created, at 0, when its recorder is wrapped with a log
	// (WithLog), so a copy with no log exports none.
	unlogged *prometheus.CounterVec

	// onForget are run when a controller's series are forgotten: the publisher drops the controller's
	// route and its loss gate then, since its manager has stopped for good.
	// lc is the lifecycle mutex. Zero, Forget and a recorder's attach (route.attach) each hold it for the whole
	// of one transition: the registration, the retirement or revival of the label's route, and the explicit
	// creation and deletion of its series. A registration and a retirement are therefore never interleaved.
	//
	// LOCK ORDER: lc, then a route's retirement lock (route.retire), then its r.mu. Forget and Zero take the
	// retirement lock for writing while they hold lc: that is how retirement waits for a raise that is handing
	// EventsNotLogged to the recorder. So nothing that runs under the retirement lock's read side, the
	// recorder call and everything it reaches (the wrapper's count, route.publish, route.lost), may take lc; the
	// hot paths count through prometheus's own locks only. mu below is a leaf: it guards the maps, and no hook
	// is run under it.
	lc sync.Mutex

	mu       sync.Mutex
	onForget map[string][]func()
	// onZero are run when a controller's series are zeroed, a new registration of the label: the publisher
	// revives the label's route then, if a Forget retired it.
	onZero map[string][]func()
	// regs are the managers of each controller label that have set their series up and not yet had them
	// forgotten: a registration per series set, the one a manager's Setup passes to Zero and its Dropped hook
	// to Forget. Two managers of one label can run at once, as when a Provider changes kind, and the label's
	// route and unlogged series live as long as either does. A registration is a member of a set, not a
	// count, on two facts about the supervisors (internal/providerctl/supervisor.go): a manager that fails is
	// set up again, and so zeroed again, with no Forget between, so a count would never return to 0; and one
	// Supervisor never has two managers of one name, its pass stopping a dropped manager and calling Dropped
	// before it starts any (TestSupervisorNeverRunsTwoManagersOfOneNameAtOnce pins that order).
	regs map[string]map[string][]Series

	// forgetMid, when set (tests only), is called by Forget between retiring the controller's route and
	// deleting its series, the window in which an event of the controller can still be lost.
	forgetMid func()
	// forgetDecided, when set (tests only), is called by Forget after it has decided whether the label has
	// another registration, and before it retires the route: the window in which a Zero could register, if
	// nothing ordered it behind the Forget.
	forgetDecided func()
}

// forgetHook registers f to run when the controller's series are forgotten.
func (c *Counter) forgetHook(controller string, f func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.onForget == nil {
		c.onForget = map[string][]func(){}
	}
	c.onForget[controller] = append(c.onForget[controller], f)
}

// zeroHook registers f to run when the controller's series are zeroed.
func (c *Counter) zeroHook(controller string, f func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.onZero == nil {
		c.onZero = map[string][]func(){}
	}
	c.onZero[controller] = append(c.onZero[controller], f)
}

// New returns a Counter with no series.
func New() *Counter {
	return &Counter{
		vec: prometheus.NewCounterVec(prometheus.CounterOpts{Name: Name, Help: Help},
			[]string{LabelController, LabelReason, LabelAction, LabelType}),
		unlogged: prometheus.NewCounterVec(prometheus.CounterOpts{Name: UnloggedName, Help: UnloggedHelp}, []string{LabelController}),
	}
}

// Describe implements prometheus.Collector.
func (c *Counter) Describe(ch chan<- *prometheus.Desc) {
	c.vec.Describe(ch)
	c.unlogged.Describe(ch)
}

// Collect implements prometheus.Collector.
func (c *Counter) Collect(ch chan<- prometheus.Metric) {
	c.vec.Collect(ch)
	c.unlogged.Collect(ch)
}

// Unlogged is the value of the controller's hpcburst_events_unlogged_total series, and whether the
// series exists. Reading it creates none.
func (c *Counter) Unlogged(controller string) (value float64, exists bool) {
	ch := make(chan prometheus.Metric)
	go func() { c.unlogged.Collect(ch); close(ch) }()
	for m := range ch {
		var d dto.Metric
		if err := m.Write(&d); err != nil {
			continue
		}
		for _, l := range d.GetLabel() {
			if l.GetName() == LabelController && l.GetValue() == controller {
				value, exists = d.GetCounter().GetValue(), true
			}
		}
	}
	return value, exists
}

// Zero creates the controller's series at 0, so that an event never raised
// shows a real 0 rather than an absent series (HBEP-8, Raised events: series
// are created at 0 for every reason, action and type that the lifecycle
// controller, the front and a managed Provider can raise). A series that
// exists already keeps its count.
//
// The series set is also the registration of the manager that asks for it: see Forget.
func (c *Counter) Zero(controller string, series []Series) {
	c.lc.Lock()
	defer c.lc.Unlock()
	for _, s := range series {
		c.vec.WithLabelValues(controller, s.Reason, s.Action, s.Type)
	}
	c.mu.Lock()
	if c.regs == nil {
		c.regs = map[string]map[string][]Series{}
	}
	if c.regs[controller] == nil {
		c.regs[controller] = map[string][]Series{}
	}
	c.regs[controller][seriesKey(series)] = append([]Series(nil), series...)
	hooks := append([]func(){}, c.onZero[controller]...)
	c.mu.Unlock()
	// A new registration means the label is live again: a route that a Forget retired is revived, and its
	// unlogged series comes back at 0.
	for _, f := range hooks {
		f()
	}
}

// lifecycle runs f as one lifecycle transition (see lc).
func (c *Counter) lifecycle(f func()) {
	c.lc.Lock()
	defer c.lc.Unlock()
	f()
}

// seriesKey names a series set: its entries sorted, so the set a manager zeroes is the set it forgets.
func seriesKey(series []Series) string {
	keys := make([]string, 0, len(series))
	for _, s := range series {
		keys = append(keys, s.Reason+"|"+s.Action+"|"+s.Type)
	}
	sort.Strings(keys)
	return strings.Join(keys, ",")
}

// Forget deletes the controller's series named in series, whatever their
// counts: a Provider's manager that has stopped for good (its Provider gone, or
// now of another kind) raises nothing more, so series created at 0 for it would
// read as a Provider that raised nothing. A series that does not exist is
// skipped.
//
// The series set is the registration of the manager that Zero was given it for, and the manager that is
// forgotten is the one whose set this is. Two managers of one label can run at once, when a Provider changes
// kind: while another registration of the label is live, the label's route, its unlogged series and the
// series the other manager also raises are left alone, and only the series of this manager's own are deleted.
// When it was the last, the controller is retired first and its series deleted after: the hooks mark its
// publisher route dead, and a dead route counts no loss and raises nothing, so no event of the controller
// that is still queued, or whose EventsNotLogged is being raised, can bring a series back once it is deleted
// (the delta reviews of #697, 17 Nov 2026).
func (c *Counter) Forget(controller string, series []Series) {
	c.lc.Lock()
	defer c.lc.Unlock()
	c.mu.Lock()
	delete(c.regs[controller], seriesKey(series))
	live := len(c.regs[controller]) > 0
	kept := map[Series]bool{} // series another live registration of the label raises too
	for _, ss := range c.regs[controller] {
		for _, s := range ss {
			kept[s] = true
		}
	}
	var hooks []func()
	if !live {
		delete(c.regs, controller)
		hooks = append([]func(){}, c.onForget[controller]...)
	}
	c.mu.Unlock()
	if c.forgetDecided != nil {
		c.forgetDecided()
	}
	for _, f := range hooks {
		f() // retire the route: this waits for a raise that is handing its event to the recorder
	}
	if c.forgetMid != nil {
		c.forgetMid()
	}
	for _, s := range series {
		if !kept[s] {
			c.vec.DeleteLabelValues(controller, s.Reason, s.Action, s.Type)
		}
	}
	if !live {
		c.unlogged.DeleteLabelValues(controller)
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
// recorder, in the same call. With a log (WithLog) it also hands a typed event to the durable
// log's publisher, built from the same call's arguments.
type eventsRecorder struct {
	inner      events.EventRecorder
	c          *Counter
	controller string
	// rt is the controller's route into the publisher, nil for a wrapper with no log.
	rt *route
}

// IDRecorder is the recorder Events returns: client-go's events.k8s.io
// recorder, with a second method for an event that carries its own id (beta;
// HBEP-8, The durable log, Streams, The event's id). A caller that holds only
// an events.EventRecorder is unaffected.
type IDRecorder interface {
	events.EventRecorder
	// EventfID raises the event as Eventf does and gives it the id: the Nats-Msg-Id the durable
	// log's publisher stores the event under. An event raised through Eventf takes a fresh UUID,
	// kept across the publisher's retries. A wrapper with no log reads no id.
	EventfID(id string, regarding runtime.Object, related runtime.Object, eventtype, reason, action, note string, args ...interface{})
	// NotLogged counts an event the caller refused to raise, so that it reached neither the
	// Kubernetes recorder nor the publisher, in hpcburst_events_unlogged_total, as any event the log
	// did not take: the Provider controller refuses a typed note that names its job ID, and a refused
	// note is not silently lost. It does nothing on a wrapper with no log.
	NotLogged(regarding runtime.Object, reason string)
}

// Option changes what Events builds.
type Option func(*options)

type options struct{ log *Log }

// Log is what a wrapper needs to publish its typed events to the durable log (beta; HBEP-8, The
// durable log, Who writes). One Publisher serves the whole process; each wrapper brings its own
// controller's way of describing and regarding its events.
type Log struct {
	// Publisher is the process's publisher. Nil is no log: the option does nothing, so a command
	// without --event-log-token-file builds exactly what it built before.
	Publisher *Publisher
	// Scheme gives the kind of each object an event names, for the subject and the envelope.
	Scheme *runtime.Scheme
	// ReportingController is the field manager the wrapped recorder reports its events under, which
	// is what the Kubernetes copy's reportingController says and so what the envelope's data does.
	ReportingController string
	// Lease reads the controller's own Lease from the API, for EventsNotLogged to regard it. Nil
	// raises none: the loss is counted all the same.
	Lease LeaseFunc
}

// Option returns WithLog for this process's publisher, for a controller whose recorder is built with
// scheme and reports its events under reporting, and whose own Lease lease reads. It is safe on a nil
// publisher, a process with no log, and then does nothing: a call site wraps its recorder with it
// whether or not the process has a log.
func (p *Publisher) Option(scheme *runtime.Scheme, reporting string, lease LeaseFunc) Option {
	if p == nil {
		return func(*options) {}
	}
	return WithLog(Log{Publisher: p, Scheme: scheme, ReportingController: reporting, Lease: lease})
}

// OptionFor is Option for a call site whose scheme and Lease source are not at hand without a cost or a risk
// when there is no log (a manager's scheme and API reader): f is called only when the process has a
// publisher, so a process with none builds its recorder exactly as before, touching nothing new.
func (p *Publisher) OptionFor(f func() (scheme *runtime.Scheme, reporting string, lease LeaseFunc)) Option {
	if p == nil {
		return func(*options) {}
	}
	scheme, reporting, lease := f()
	return p.Option(scheme, reporting, lease)
}

// WithLog makes the wrapper publish each typed event, an event with an action, to the durable log as
// well as to the Kubernetes recorder, and count what the log did not take in
// hpcburst_events_unlogged_total{controller}, whose series starts at 0 here. Every wrapper of one
// controller shares one loss gate, so the controller raises one EventsNotLogged between them.
func WithLog(l Log) Option { return func(o *options) { o.log = &l } }

// Events wraps client-go's events.k8s.io recorder: each Eventf or EventfID call
// counts the event under controller, with the reason, action and type it hands
// over, and then hands it over. It panics on a nil recorder or Counter, or an
// empty controller. With WithLog it also publishes the typed events to the durable log.
func Events(inner events.EventRecorder, c *Counter, controller string, opts ...Option) IDRecorder {
	if inner == nil {
		panic("eventcount: Events wraps no recorder")
	}
	mustCounter(c, controller)
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	r := eventsRecorder{inner: inner, c: c, controller: controller}
	if o.log != nil && o.log.Publisher != nil {
		if o.log.Scheme == nil || o.log.ReportingController == "" {
			panic("eventcount: a recorder wrapped with a log needs the scheme and the reporting controller")
		}
		r.rt = o.log.Publisher.routeFor(controller, c)
		r.rt.attach(routeSpec{scheme: o.log.Scheme, reporting: o.log.ReportingController, lease: o.log.Lease, rec: r})
	}
	return r
}

// Eventf implements events.EventRecorder. The time of the log's copy is read once, just before the
// Kubernetes recorder's own call, which reads its own clock for eventTime inside it: the two copies
// agree on their fields and on their times within microseconds (HBEP-8, The durable log, The
// envelope; the science agent's ruling on #625's PR E2).
func (r eventsRecorder) Eventf(regarding runtime.Object, related runtime.Object, eventtype, reason, action, note string, args ...interface{}) {
	r.c.count(r.controller, reason, action, eventtype)
	at := r.rt.clock()
	r.inner.Eventf(regarding, related, eventtype, reason, action, note, args...)
	r.rt.publish(at, "", regarding, related, eventtype, reason, action, note, args)
}

// EventfID implements IDRecorder: it counts and hands the event over exactly as Eventf does, and
// publishes it to the log, if there is one, under the id.
func (r eventsRecorder) EventfID(id string, regarding runtime.Object, related runtime.Object, eventtype, reason, action, note string, args ...interface{}) {
	r.c.count(r.controller, reason, action, eventtype)
	at := r.rt.clock()
	r.inner.Eventf(regarding, related, eventtype, reason, action, note, args...)
	r.rt.publish(at, id, regarding, related, eventtype, reason, action, note, args)
}

// NotLogged implements IDRecorder.
func (r eventsRecorder) NotLogged(regarding runtime.Object, reason string) {
	r.rt.refused(regarding, reason)
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
