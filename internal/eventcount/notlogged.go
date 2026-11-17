package eventcount

// EventsNotLogged (beta; HBEP-8, The durable log, When it is down, and Repeating conditions; #625,
// PR E2). An event the log cannot take is counted in hpcburst_events_unlogged_total{controller}; the
// emitter also raises one `degraded` event, regarding its own Lease (lifecycle, front, or
// provider-<name>), with the count and since when in its note. The rules are the loss gate's (gate.go):
//
//   - at the first loss after the process started or after the log last took an event, so a log that
//     is refused at start, or not up yet, raises it too (R10, the science agent's erratum);
//   - then at most once an hour while losses last;
//   - and, when the run of losses ends, one closing event saying that the log takes events again, the
//     count and since when, so the log itself shows the span it lacks. The closing stays owed until a
//     closing event is stored: one that is lost is covered by the next, since the earliest unreported
//     loss with the counts summed. A run of queue-full losses ends only when a publish succeeds with the
//     queue below half its size.
//
// The event goes through the same counted recorder and is published like any event. Its own failure
// to publish counts, but never raises another one: an EventsNotLogged that is lost is counted and that
// is all (route.lost), or a down log would raise one for each it could not take. A controller whose manager
// has stopped for good counts nothing and raises nothing for the events it left queued.
//
// The Lease is read from the API for its UID, by the notifier and not by the emit site, which never
// waits. A Lease that cannot be read (not created yet, or the lifecycle controller run with
// --leader-elect=false, which holds none) raises nothing: the loss is counted, one log line says so,
// and the gate stays open, so the next loss tries again. Nothing is raised without a regarding
// object, since an event without a UID has no subject in the log.

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// The event's reason and action: HBEP-8's Reasons table lists it as a degraded event regarding the
// emitter's own Lease.
const (
	ReasonEventsNotLogged = "EventsNotLogged"
	ActionDegraded        = "degraded"
)

// repeatEvery is the least time between two EventsNotLogged of one run (HBEP-8, Repeating
// conditions).
const repeatEvery = provider.EventRepeatInterval

// leaseReadTimeout bounds the read of the Lease for its UID.
const leaseReadTimeout = 5 * time.Second

type jobKind int

const (
	jobLoss jobKind = iota
	jobRecovery
)

// raiseJob is one EventsNotLogged that is owed: a loss run's, or the closing of the runs that have ended.
// Its fields are the route's to change, under the route's lock, until the notifier starts it.
type raiseJob struct {
	r       *route
	kind    jobKind
	gen     uint64    // a loss job: the run it raises for
	upTo    uint64    // a closing: the last run it reports
	count   int       // the events not logged: the run's so far, or the runs' summed
	since   time.Time // the first loss: the run's, or the earliest unreported
	at      time.Time // when it was decided
	handles int       // a loss job: the losses whose barrier it carries (route.handled)
	started bool      // the notifier has taken it: it is no longer updated in place
}

// hold makes the closing job report sp as well as what it holds already: the earliest loss, the counts
// summed, the latest generation.
func (j *raiseJob) hold(sp span) {
	h := span{gen: j.upTo, since: j.since, count: j.count}
	h.add(sp)
	j.upTo, j.since, j.count = h.gen, h.since, h.count
}

// jobData is what a started job says.
type jobData struct {
	kind      jobKind
	count     int
	since, at time.Time
}

// note is the event's text: the count and since when. It holds names, counts and times only, never a
// job ID, a path or an account (HBEP-8, Allocation events, No job ID in a note).
func (d jobData) note() string {
	since := d.since.UTC().Format(time.RFC3339)
	if d.kind == jobRecovery {
		return fmt.Sprintf("the durable log takes events again; %d events were not logged from %s to %s: they are in the cluster's own events only", d.count, since, d.at.UTC().Format(time.RFC3339))
	}
	return fmt.Sprintf("%d events not logged since %s: the durable log did not take them. They are counted in hpcburst_events_unlogged_total and stay in the cluster's own events", d.count, since)
}

// run raises the event, and records whether it was handed over.
func (j *raiseJob) run(stopped func() bool) {
	r := j.r
	data, live := r.start(j)
	spec := r.snapshot()
	if !live || spec.lease == nil || spec.rec == nil || stopped() {
		if live && spec.lease == nil {
			r.p.log.Warn("EventsNotLogged cannot be raised: the controller has no Lease to regard", "controller", r.controller)
		}
		r.finish(j, false)
		return
	}
	ctx, cancel := context.WithTimeout(r.p.ctx, leaseReadTimeout)
	lease, err := spec.lease(ctx)
	cancel()
	if err != nil || lease == nil || lease.UID == "" {
		r.p.log.Warn("EventsNotLogged is not raised: its Lease cannot be read for its UID; it is tried again with the next loss", "controller", r.controller, "error", err)
		r.finish(j, false)
		return
	}
	if r.isDead() {
		r.finish(j, false)
		return
	}
	// The wrapper counts the event and hands it to the Kubernetes recorder and the publisher, as it does
	// any event. The call is through the recorder's interface, so the drift guard reads it as the emit
	// site of EventsNotLogged, with its constant reason and action and its Lease. The route is told which
	// job the event is, so that the event's own outcome settles a closing.
	r.setEmitting(j)
	spec.rec.Eventf(lease, nil, provider.EventKindWarning, ReasonEventsNotLogged, ActionDegraded, "%s", provider.CutNote(data.note()))
	r.setEmitting(nil)
	r.finish(j, true)
}

// notifier raises the owed EventsNotLogged events in a goroutine of its own, so that neither the emit
// site nor the publishing goroutine waits for a read of the API. jobs holds the jobs that wait: at most one
// loss job and one closing job per route (route.lost, route.closingJobLocked), so it is bounded by twice the
// number of routes.
type notifier struct {
	p       *Publisher
	mu      sync.Mutex
	cond    *sync.Cond
	jobs    []*raiseJob
	stopped bool
	done    chan struct{}
}

func newNotifier(p *Publisher) *notifier {
	n := &notifier{p: p, done: make(chan struct{})}
	n.cond = sync.NewCond(&n.mu)
	go n.loop()
	return n
}

func (n *notifier) push(j *raiseJob) {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		j.r.finish(j, false)
		return
	}
	n.jobs = append(n.jobs, j)
	n.cond.Signal()
	n.mu.Unlock()
}

func (n *notifier) isStopped() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.stopped
}

func (n *notifier) loop() {
	defer close(n.done)
	for {
		n.mu.Lock()
		for len(n.jobs) == 0 && !n.stopped {
			n.cond.Wait()
		}
		if n.stopped {
			rest := n.jobs
			n.jobs = nil
			n.mu.Unlock()
			for _, j := range rest {
				j.r.finish(j, false)
			}
			return
		}
		j := n.jobs[0]
		n.jobs = n.jobs[1:]
		n.mu.Unlock()
		j.run(n.isStopped)
	}
}

func (n *notifier) stop() {
	n.mu.Lock()
	n.stopped = true
	n.cond.Broadcast()
	n.mu.Unlock()
	<-n.done
}
