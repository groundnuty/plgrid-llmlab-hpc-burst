package eventcount

// The loss gate of one controller's route: when EventsNotLogged is raised, and what it says (HBEP-8, The
// durable log, When it is down, and Repeating conditions; #625, PR E2, as amended by the rulings of the
// science agent on #697, 17 Nov 2026).
//
// A RUN of losses starts at the first loss after the process started or after the log last took an event
// (R10: the start counts as a success). Each run has a generation. While a run lasts, a loss raises one
// EventsNotLogged at the first loss and then at most one an hour. A run ends at the first publish the log
// takes, except that a run that holds a queue-full loss ends only when the publisher's queue is below half
// its size when the publish succeeds (a low-water mark): on a live but slow log with a full queue, every
// head publish would otherwise end the run that the next dropped event starts again, with a pair of
// EventsNotLogged each time.
//
// A run that has ended owes the log ONE closing EventsNotLogged, saying that the log takes events again
// and how many events were not logged, since when. It is owed until a closing event is STORED, not until
// one is handed to the recorder: the span of a run stays owed while its closing is queued, and if the
// closing is itself lost, the next closing covers every unreported run, since the earliest unreported loss,
// with the counts summed. A closing is raised at the end of a run and at any later publish the log takes
// while a span is still owed, but never while a run is open: on a live, slow log with a full queue the
// publishes the log takes do not end the run, and the next run's closing covers the span owed as well. The runs not yet reported are kept as ONE aggregate (the earliest loss, the
// summed count, the latest generation), however many there are, so that a controller that can never raise a
// closing (it has no Lease) holds a fixed amount of state and does a fixed amount of work per publish.
//
// Raising is the notifier's, in a goroutine of its own (notlogged.go). At most one loss job and one closing
// job wait for it per route, and a new one for the same route and kind updates the waiting job in place, so
// the jobs waiting are bounded by twice the number of routes, however many events are dropped. A job
// belongs to the run it was made for: its outcome changes the gate only for that run, and a new run is
// never held back by an older run's job.

import "time"

// span is runs that have ended and that no stored closing event reports yet: one run, or the aggregate of
// several. The zero span, with no events, is nothing.
type span struct {
	gen   uint64    // the latest run's generation
	since time.Time // the earliest first loss
	count int       // the events not logged, summed
}

// add folds o into the span: the earliest loss, the counts summed, the latest generation.
func (s *span) add(o span) {
	switch {
	case o.count == 0:
	case s.count == 0:
		*s = o
	default:
		if o.since.Before(s.since) {
			s.since = o.since
		}
		s.count += o.count
		s.gen = max(s.gen, o.gen)
	}
}

// gate is the loss state of a route, guarded by route.mu.
type gate struct {
	gen       uint64    // the generation of the latest run
	losing    bool      // a run is going on
	runStart  time.Time // its first loss
	runCount  int       // the events it has not logged so far
	runFull   bool      // it holds a queue-full loss, so it ends only at the low-water mark
	lastRaise time.Time // when this run's latest loss raise was handed over; zero until one was

	loss    *raiseJob // the outstanding loss job: waiting for the notifier or running
	owed    span      // ended runs that no closing job holds and no stored closing event reports: one aggregate
	closing *raiseJob // the outstanding closing job: waiting, running, or its event queued and not yet settled; it holds the runs it reports

	emitting *raiseJob // the job whose recorder call is in progress on the notifier's goroutine
}

// lost counts an event the log did not take and decides whether EventsNotLogged is due. why is the reason for
// the log line; reason is the lost event's own, and an EventsNotLogged that is itself lost counts but never
// raises another (HBEP-8, Raised events); closes is the closing job that event was, if it was one. A lost
// event of a controller that has stopped for good is one log line and nothing else.
func (r *route) lost(why, reason, id string, err error, closes *raiseJob) {
	now := r.p.now()
	r.mu.Lock()
	if r.dead {
		r.mu.Unlock()
		r.p.log.Warn("an event of a controller that has stopped for good was not logged", "controller", r.controller, "why", why, "reason", reason, "id", id, "error", err)
		return
	}
	r.counter.unlogged.WithLabelValues(r.controller).Inc()
	warn := r.warnedAt.IsZero() || now.Sub(r.warnedAt) >= warnEvery
	if warn {
		r.warnedAt = now
	}
	if reason == ReasonEventsNotLogged {
		// Its own loss starts no run and raises nothing. A closing that is lost stays owed.
		if closes != nil {
			r.closingLostLocked(closes)
		}
		r.mu.Unlock()
		if warn {
			r.p.log.Warn("an event was not logged", "controller", r.controller, "why", why, "reason", reason, "id", id, "error", err)
		}
		r.handled.Add(1)
		return
	}
	if !r.losing {
		r.gen++
		r.losing, r.runStart, r.runCount, r.runFull, r.lastRaise = true, now, 0, false, time.Time{}
	}
	r.runCount++
	if why == lostQueue {
		r.runFull = true
	}
	r.fullOpen.Store(r.losing && r.runFull)
	var job *raiseJob
	rode := false // this loss's raise is a job already waiting
	if j := r.loss; j != nil && !j.started {
		// A loss job is waiting for the notifier: it raises for the run as it stands now. It is the run's own
		// job, or an older run's that has not started, which this run takes over.
		j.gen, j.count, j.since = r.gen, r.runCount, r.runStart
		j.covered = false // it raises for this run now, which no closing covers
		if j.at.IsZero() || now.After(j.at) {
			j.at = now
		}
		j.handles++
		rode = true
	} else if (r.lastRaise.IsZero() || now.Sub(r.lastRaise) >= repeatEvery) && !(r.loss != nil && r.loss.gen == r.gen) {
		job = &raiseJob{r: r, kind: jobLoss, epoch: r.epoch, gen: r.gen, count: r.runCount, since: r.runStart, at: now, handles: 1}
		r.loss = job
	}
	r.mu.Unlock()
	if warn {
		r.p.log.Warn("an event was not logged", "controller", r.controller, "why", why, "reason", reason, "id", id, "error", err)
	}
	switch {
	case job != nil:
		r.p.notify.push(job)
	case !rode:
		r.handled.Add(1)
	}
}

// logged records that the log took an event, it, which may be a closing: the closing it was is then settled.
// A run ends with it, unless the run holds a queue-full loss and the queue is not yet below half its size.
// While a span is owed, no run is open and no closing is outstanding, one is raised.
func (r *route) logged(it *item) {
	pending := r.p.Pending() // before the route's lock: the publisher's lock is never taken inside it
	now := r.p.now()
	r.mu.Lock()
	if r.dead {
		r.mu.Unlock()
		return
	}
	if it != nil && it.closes != nil {
		r.closingStoredLocked(it.closes)
	}
	// pending counts the event just stored, which is still in flight.
	if r.losing && (!r.runFull || r.belowHalf(pending)) {
		r.endRunLocked()
	}
	var job *raiseJob
	if !r.losing {
		job = r.closingJobLocked(now)
	}
	r.mu.Unlock()
	if job != nil {
		r.p.notify.push(job)
	}
}

// belowHalf reports whether the publisher's queue, pending events including the one just stored and still in
// flight, is below half its size: the low-water mark that ends a queue-full run.
func (r *route) belowHalf(pending int) bool { return (pending-1)*2 < r.p.o.QueueSize }

// endRunLocked ends the open run: its span is owed to a closing.
func (r *route) endRunLocked() {
	r.losing = false
	r.fullOpen.Store(false)
	r.owed.add(span{gen: r.gen, since: r.runStart, count: r.runCount})
}

// queueDrained ends this route's queue-full run, if it holds one, because ANOTHER route's publish succeeded with
// the shared queue below half its size: the run's cause is the queue the routes share, and it has drained, so the
// run need not wait for this route to publish (the science agent's ruling on #697, 17 Nov 2026). The closing the
// ended run owes is raised as usual.
func (r *route) queueDrained(pending int) {
	if !r.fullOpen.Load() { // the cheap test: no lock for a route that holds no queue-full run
		return
	}
	now := r.p.now()
	r.mu.Lock()
	if r.dead || !r.losing || !r.runFull || !r.belowHalf(pending) {
		r.mu.Unlock()
		return
	}
	r.endRunLocked()
	job := r.closingJobLocked(now)
	r.mu.Unlock()
	if job != nil {
		r.p.notify.push(job)
	}
}

// closingJobLocked returns the closing job to push, if a span is owed and none is outstanding. The job takes
// the owed span: it holds the runs it reports until its event is stored or lost. A closing that waits for the
// notifier takes the span in place instead, and nil is returned; one that has started leaves it owed.
func (r *route) closingJobLocked(now time.Time) *raiseJob {
	if r.owed.count == 0 {
		return nil
	}
	if j := r.closing; j != nil {
		if !j.started {
			j.hold(r.owed)
			j.at = now
			r.owed = span{}
			// The waiting closing now covers runs whose loss job may wait behind it, and would be emitted after it:
			// a log that says it takes events again and then that it is losing them, for a run it has just
			// reported. That loss job is dropped.
			if l := r.loss; l != nil && !l.started && l.gen <= j.upTo {
				l.covered = true
			}
		}
		return nil
	}
	j := &raiseJob{r: r, kind: jobRecovery, epoch: r.epoch, at: now}
	j.hold(r.owed)
	r.owed = span{}
	r.closing = j
	return j
}

// closingStoredLocked settles a closing whose event the log has stored: the runs it held are reported. A run
// that ended after the closing started is in r.owed and stays owed.
func (r *route) closingStoredLocked(j *raiseJob) {
	if j.kind == jobRecovery && r.closing == j {
		r.closing = nil
	}
}

// closingLostLocked settles a closing whose event was lost, or never made: the runs it held are owed again,
// and the next publish the log takes raises a closing for them with whatever has ended since.
func (r *route) closingLostLocked(j *raiseJob) {
	if j.kind == jobRecovery && r.closing == j {
		r.closing = nil
		r.owed.add(span{gen: j.upTo, since: j.since, count: j.count})
	}
}

// start marks a job as taken by the notifier, which ends its update in place, and returns what it says. It
// returns false when the route's controller has stopped for good, or when it is a loss job whose run a closing
// has come to cover: nothing is raised for it.
func (r *route) start(j *raiseJob) (jobData, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	j.started = true
	return jobData{kind: j.kind, count: j.count, since: j.since, at: j.at}, !r.dead && !j.covered
}

// setEmitting records the job whose recorder call is about to be made, so that the event it raises carries it.
func (r *route) setEmitting(j *raiseJob) {
	r.mu.Lock()
	r.emitting = j
	r.mu.Unlock()
}

// takeEmitting returns the job whose recorder call this EventsNotLogged is, once.
func (r *route) takeEmitting(reason string) *raiseJob {
	if reason != ReasonEventsNotLogged {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	j := r.emitting
	r.emitting = nil
	return j
}

// finish records the outcome of a raise: the event was handed to the recorder, or it was not (the Lease could
// not be read, the controller has stopped for good, or the publisher is stopping). A loss raise changes the gate
// only for its own run, and only if it was handed over. A closing handed over stays outstanding until its event
// is stored or lost; one that was not made is owed again.
func (r *route) finish(j *raiseJob, handedOver bool) {
	r.mu.Lock()
	handles := 0
	switch j.kind {
	case jobLoss:
		if r.loss == j {
			r.loss = nil
		}
		if handedOver && j.gen == r.gen && r.losing {
			r.lastRaise = j.at
		}
		handles = j.handles
	case jobRecovery:
		if !handedOver {
			r.closingLostLocked(j)
		}
	}
	r.mu.Unlock()
	if handles > 0 {
		r.handled.Add(uint64(handles))
	}
}

// isDead reports whether the route's controller has stopped for good.
func (r *route) isDead() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.dead
}

// revive undoes markDead: a new manager of the label has registered, so the label is live again. The gate starts
// afresh (its jobs and spans belonged to the manager that stopped), and the unlogged series comes back at 0.
// Like markDead it takes the retirement lock for writing, so it waits for a raise in flight.
func (r *route) revive() {
	if !r.isDead() {
		return // the usual case: no write lock for a live route
	}
	r.retire.Lock()
	defer r.retire.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.dead {
		return
	}
	r.dead = false
	r.epoch++
	r.gate = gate{gen: r.gen}
	r.fullOpen.Store(false)
	r.warnedAt = time.Time{}
	r.counter.unlogged.WithLabelValues(r.controller)
}

// markDead ends the route: its controller's manager has stopped for good. It waits for a raise that is
// handing its EventsNotLogged to the recorder, and after it no raise does (raiseJob.emit).
func (r *route) markDead() {
	r.retire.Lock()
	defer r.retire.Unlock()
	r.mu.Lock()
	if !r.dead {
		r.dead = true
		r.epoch++
	}
	r.mu.Unlock()
}
