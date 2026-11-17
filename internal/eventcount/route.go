package eventcount

// A route is one controller's way into the publisher: the controller's label, the counter its
// recorder counts into, what builds its envelopes (the scheme and the reporting controller), and its
// loss gate, which decides when EventsNotLogged is raised (notlogged.go). The wrapper that
// Events returns holds the route of its controller; every wrapper of one controller shares it.

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// LeaseFunc reads the controller's own Lease from the API, for its UID: the regarding object of
// EventsNotLogged (HBEP-8, The durable log, When it is down). The lifecycle controller's is
// lifecycle and the front's front, both in hpcburst-system, and a Provider manager's is
// provider-<name>.
type LeaseFunc func(ctx context.Context) (*coordinationv1.Lease, error)

// LeaseOf reads the Lease namespace/name through a controller-runtime reader, an uncached one so that
// the read is of the API, for the controllers that run in a manager.
func LeaseOf(reader client.Reader, namespace, name string) LeaseFunc {
	return func(ctx context.Context) (*coordinationv1.Lease, error) {
		var l coordinationv1.Lease
		if err := reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &l); err != nil {
			return nil, err
		}
		return &l, nil
	}
}

// LeaseFromClientset reads the Lease namespace/name through a clientset, for the front, which has no
// manager.
func LeaseFromClientset(cs kubernetes.Interface, namespace, name string) LeaseFunc {
	return func(ctx context.Context) (*coordinationv1.Lease, error) {
		return cs.CoordinationV1().Leases(namespace).Get(ctx, name, metav1.GetOptions{})
	}
}

// routeSpec is what the latest wrapper of a controller registered. A Provider's manager is built
// again after a failure under the same label, so the latest registration replaces the earlier.
type routeSpec struct {
	scheme    *runtime.Scheme
	reporting string
	lease     LeaseFunc
	// rec raises EventsNotLogged: the controller's own counted wrapper, so the event is counted
	// in hpcburst_events_raised_total and published like any other.
	rec events.EventRecorder
}

type route struct {
	p          *Publisher
	controller string
	counter    *Counter

	mu   sync.Mutex
	spec routeSpec

	// The loss gate (HBEP-8, Repeating conditions): a run of losses starts at the first loss after a
	// success, where the process's start counts as one (R10, #625), and ends at the next success.
	losing     bool
	runStart   time.Time
	runCount   int
	lastRaise  time.Time // when the run's latest EventsNotLogged was handed over; zero until one was
	lossRaise  bool      // a raise for the run is queued or running
	recovery   *raiseJob // the closing event of a run, owed until it is handed over
	recoveryAt bool      // it is queued or running

	warnedAt time.Time

	// handled counts the losses whose decision, and the raise it led to, have finished. It is the
	// barrier a test reads before it reads the raised-events counter for an absence.
	handled atomic.Uint64
}

func (r *route) attach(s routeSpec) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.spec = s
}

func (r *route) snapshot() routeSpec {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.spec
}

// clock reads the publisher's clock: the time of the call that hands an event to the recorder. A
// route that is nil, a wrapper with no log, reads none.
func (r *route) clock() time.Time {
	if r == nil {
		return time.Time{}
	}
	return r.p.now()
}

// An event that cannot be logged for what it is, and not because the log failed, is counted in
// hpcburst_events_unlogged_total and named in one log line, and goes no further: an event with no
// subject (its regarding object has no UID) and a note its emitter refused. Neither says anything
// about the log, so neither raises EventsNotLogged, which is the log's: an event the log could not take
// (route.lost). Were they to, a controller that kept refusing notes would raise two EventsNotLogged, the
// run's first loss and its closing event, for every one, with the log healthy throughout.
//
// publish hands one event to the publisher, built from the arguments of the Eventf call that also
// handed it to the Kubernetes recorder. at is the time that call read from clock just before it.
// Only typed events are published: an event with no action, crossplane-runtime's own and HBEP-4's
// diagnostics, stays in the cluster's own events (HBEP-8, The durable log, Streams). It never waits.
func (r *route) publish(at time.Time, id string, regarding, related runtime.Object, eventtype, reason, action, note string, args []interface{}) {
	if r == nil || action == "" {
		return
	}
	spec := r.snapshot()
	if id == "" {
		id = string(uuid.NewUUID())
	}
	c := call{id: id, controller: r.controller, reportingController: spec.reporting, eventtype: eventtype, reason: reason,
		action: action, note: fmt.Sprintf(note, args...), regarding: regarding, related: related}
	body, subject, err := c.envelope(spec.scheme, at)
	if err != nil {
		// The event cannot be a CloudEvent on a subject: one with no UID, or of a kind the scheme lacks.
		// One log line names the kind and name of the object, never a job ID.
		r.p.log.Warn("an event cannot be logged: it has no subject", "controller", r.controller, "reason", reason, "regarding", describe(regarding, spec.scheme), "error", err)
		r.counter.unlogged.WithLabelValues(r.controller).Inc()
		return
	}
	it := &item{r: r, id: id, reason: reason, subject: subject, body: body, queuedAt: at}
	if !r.p.enqueue(it) {
		r.lost(lostQueue, reason, id, nil)
	}
}

// refused counts an event the caller refused to raise, so that it reached neither the Kubernetes
// recorder nor the publisher, as not logged: the Provider controller's refusal of a note that names
// its job ID. A route that is nil, a wrapper with no log, counts nothing: nothing is published, so
// nothing is missed.
func (r *route) refused(regarding runtime.Object, reason string) {
	if r == nil {
		return
	}
	r.p.log.Warn("a typed event was refused by its emitter and so is not logged", "controller", r.controller, "reason", reason, "regarding", describe(regarding, r.snapshot().scheme))
	r.counter.unlogged.WithLabelValues(r.controller).Inc()
}

// describe names an object by kind, namespace and name for a log line.
func describe(obj runtime.Object, scheme *runtime.Scheme) string {
	if obj == nil {
		return "(none)"
	}
	if ref, err := refOf(obj, scheme); err == nil {
		return ref.Kind + " " + nameOf(ref)
	}
	return fmt.Sprintf("%T", obj)
}

// warnEvery rate-limits the loss warnings of one controller.
const warnEvery = time.Minute

// lost counts an event the log did not take, decides whether EventsNotLogged is due, and, if it
// is, queues it for the notifier. why is the reason for the log line; reason is the lost event's own,
// and an EventsNotLogged that is itself lost counts but never raises another (HBEP-8, Raised events;
// #625).
func (r *route) lost(why, reason, id string, err error) {
	r.counter.unlogged.WithLabelValues(r.controller).Inc()
	now := r.p.now()
	r.mu.Lock()
	if now.Sub(r.warnedAt) >= warnEvery || r.warnedAt.IsZero() {
		r.warnedAt = now
		r.mu.Unlock()
		r.p.log.Warn("an event was not logged", "controller", r.controller, "why", why, "reason", reason, "id", id, "error", err)
		r.mu.Lock()
	}
	if reason == ReasonEventsNotLogged {
		r.mu.Unlock()
		r.handled.Add(1)
		return
	}
	if !r.losing {
		r.losing, r.runStart, r.runCount, r.lastRaise = true, now, 0, time.Time{}
		r.recovery = nil
	}
	r.runCount++
	var job *raiseJob
	if !r.lossRaise && (r.lastRaise.IsZero() || now.Sub(r.lastRaise) >= repeatEvery) {
		r.lossRaise = true
		job = &raiseJob{r: r, kind: jobLoss, count: r.runCount, since: r.runStart, at: now}
	}
	r.mu.Unlock()
	if job == nil {
		r.handled.Add(1)
		return
	}
	r.p.notify.push(job)
}

// logged records that the log took an event: the end of a run of losses owes one EventsNotLogged
// with this first success, so the log itself shows the span it lacks (HBEP-8, When it is down).
func (r *route) logged() {
	now := r.p.now()
	r.mu.Lock()
	var job *raiseJob
	switch {
	case r.losing:
		r.losing = false
		job = &raiseJob{r: r, kind: jobRecovery, count: r.runCount, since: r.runStart, at: now}
		r.recovery, r.recoveryAt = job, true
	case r.recovery != nil && !r.recoveryAt:
		// The closing event of an earlier run was not handed over (its Lease could not be read): try
		// again with this success.
		job = r.recovery
		r.recoveryAt = true
	}
	r.mu.Unlock()
	if job != nil {
		r.p.notify.push(job)
	}
}

// finish records the outcome of a raise: handed over, or not (the Lease could not be read, or the
// publisher is stopping). A gate is marked only once its event is handed over, so a raise that could
// not be made is tried again with the next loss.
func (r *route) finish(j *raiseJob, handedOver bool) {
	r.mu.Lock()
	switch j.kind {
	case jobLoss:
		r.lossRaise = false
		if handedOver {
			r.lastRaise = j.at
		}
	case jobRecovery:
		r.recoveryAt = false
		if handedOver {
			r.recovery = nil
		}
	}
	r.mu.Unlock()
	if j.kind == jobLoss {
		r.handled.Add(1)
	}
}
