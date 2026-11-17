package eventcount

// The publisher of the durable log's HB_EVENTS stream (beta; HBEP-8, The durable log, Who writes,
// Delivery and When it is down; #625, PR E2). One per process: the wrapper where each events.k8s.io
// recorder is built (Events, WithLog) hands every typed event to it, beside the Kubernetes recorder,
// and one goroutine publishes them in order.
//
//   - The queue holds 1,000 events per process, in flight included. A full queue counts the event as
//     not logged at once: the emit site never waits.
//   - Each event is retried with backoff for at most Budget (100 s) from when it was queued, inside
//     the stream's two-minute duplicate window, so a retry of a publish whose acknowledgement was lost
//     stores nothing more. Connecting is part of the retried operation: after the log restarts,
//     nats-server lists its restored streams on its monitor before its client port listens, and the
//     supervisor's start barrier covers only the start, so a later restart is the clients' to ride out
//     (#669; the science agent's ruling of 17 Nov 2026).
//   - A stream-full refusal ("maximum bytes exceeded", discard new) is terminal for that event: it is
//     counted at once and never retried (team-lead, 16 Nov 2026).
//   - Whatever the log did not take is counted in hpcburst_events_unlogged_total{controller}, and the
//     emitting controller raises EventsNotLogged (notlogged.go).
//
// Origin of the publish step: spike S8's evlayer.Publish and PublishRetry (spikes/s8/evlayer/
// publish.go, this repository, PR #28), the event id as Nats-Msg-Id, with the retry's bound moved
// from a context to the budget above and the connect inside the loop.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Defaults of PublisherOptions.
const (
	// DefaultQueue is HBEP-8's queue of 1,000 events per process.
	DefaultQueue = 1000
	// DefaultBudget is how long an event is retried from when it was queued: inside the stream's
	// two-minute duplicate window.
	DefaultBudget = 100 * time.Second
	// DefaultConnectTimeout and DefaultPublishTimeout bound one attempt; each is cut to what is left
	// of the event's budget.
	DefaultConnectTimeout = 3 * time.Second
	DefaultPublishTimeout = 5 * time.Second
	// DefaultMinBackoff and DefaultMaxBackoff bound the wait between two attempts, which doubles.
	DefaultMinBackoff = 100 * time.Millisecond
	DefaultMaxBackoff = 2 * time.Second
)

// PublisherOptions are the inputs of NewPublisher.
type PublisherOptions struct {
	// Addr is the log's client address, 127.0.0.1:<port base + 16>, and Token the token read from
	// <state>/eventlog/token.
	Addr, Token string
	// Now is the clock: the envelope's time (the publishing wrapper's wall clock, read once per
	// call), an event's queue time and so its budget, and the hours of EventsNotLogged's gate. Nil is
	// time.Now. A test injects one.
	Now func() time.Time
	// Budget is how long an event is retried from when it was queued; 0 is DefaultBudget. A test
	// shortens it; production keeps it inside the duplicate window.
	Budget time.Duration
	// QueueSize is the queue's length, in flight included; 0 is DefaultQueue.
	QueueSize int
	// MinBackoff and MaxBackoff bound the wait between two attempts; 0 takes the default.
	MinBackoff, MaxBackoff time.Duration
	// ConnectTimeout and PublishTimeout bound one attempt; 0 takes the default.
	ConnectTimeout, PublishTimeout time.Duration
	// Logger gets the publisher's warnings; nil logs nothing.
	Logger *slog.Logger
}

// Publisher publishes typed events to HB_EVENTS. It is safe for concurrent use.
type Publisher struct {
	o   PublisherOptions
	log *slog.Logger
	ctx context.Context
	end context.CancelFunc

	mu       sync.Mutex
	cond     *sync.Cond
	queue    []*item
	inflight bool
	stopping bool
	done     chan struct{}
	routes   map[routeKey]*route

	// conn and js are used by the publishing goroutine only.
	nc *nats.Conn
	js jetstream.JetStream

	notify *notifier

	// attempts counts the attempts to publish, a connect included: a diagnostic the tests wait on.
	attempts atomic.Int64

	// afterPublish, when set (tests only), is called with the result of each publish, as the
	// library returned it, and its result is what the publisher then sees: a test injects the loss of
	// an acknowledgement after the broker has stored the message (spike S8's lostAckJS).
	afterPublish func(id string, ack *jetstream.PubAck, err error) error
	// emitHook, when set (tests only), is called by a raise between its dead check and its recorder call.
	emitHook func()
}

// item is one event waiting for the log.
type item struct {
	r        *route
	id       string
	reason   string
	subject  string
	body     []byte
	queuedAt time.Time
	// closes is the closing EventsNotLogged job this event is, if it is one: its outcome settles the closing.
	closes *raiseJob
}

// NewPublisher starts a publisher. It connects when the first event arrives, and again whenever its
// connection is lost; Shutdown stops it.
func NewPublisher(o PublisherOptions) *Publisher {
	if o.Addr == "" || o.Token == "" {
		panic("eventcount: a publisher with no address or no token")
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.Budget <= 0 {
		o.Budget = DefaultBudget
	}
	if o.QueueSize <= 0 {
		o.QueueSize = DefaultQueue
	}
	if o.MinBackoff <= 0 {
		o.MinBackoff = DefaultMinBackoff
	}
	if o.MaxBackoff <= 0 {
		o.MaxBackoff = DefaultMaxBackoff
	}
	o.MaxBackoff = max(o.MaxBackoff, o.MinBackoff)
	if o.ConnectTimeout <= 0 {
		o.ConnectTimeout = DefaultConnectTimeout
	}
	if o.PublishTimeout <= 0 {
		o.PublishTimeout = DefaultPublishTimeout
	}
	p := &Publisher{o: o, log: o.Logger, done: make(chan struct{}), routes: map[routeKey]*route{}}
	if p.log == nil {
		p.log = slog.New(slog.NewTextHandler(discard{}, nil))
	}
	p.ctx, p.end = context.WithCancel(context.Background())
	p.cond = sync.NewCond(&p.mu)
	p.notify = newNotifier(p)
	go p.loop()
	return p
}

type discard struct{}

func (discard) Write(b []byte) (int, error) { return len(b), nil }

func (p *Publisher) now() time.Time { return p.o.Now() }

// Pending is how many events wait for the log, the one in flight included.
func (p *Publisher) Pending() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.pendingLocked()
}

func (p *Publisher) pendingLocked() int {
	n := len(p.queue)
	if p.inflight {
		n++
	}
	return n
}

// enqueue takes an event unless the queue is full or the publisher is stopping.
func (p *Publisher) enqueue(it *item) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopping || p.pendingLocked() >= p.o.QueueSize {
		return false
	}
	p.queue = append(p.queue, it)
	p.cond.Signal()
	return true
}

// next blocks for the next event, in the order they were queued. It returns nil once the publisher
// has stopped and nothing is left, or has been told to stop at once.
func (p *Publisher) next() *item {
	p.mu.Lock()
	defer p.mu.Unlock()
	for len(p.queue) == 0 {
		if p.stopping {
			return nil
		}
		p.cond.Wait()
	}
	if p.ctx.Err() != nil {
		return nil
	}
	it := p.queue[0]
	p.queue[0] = nil
	p.queue = p.queue[1:]
	p.inflight = true
	return it
}

func (p *Publisher) loop() {
	defer close(p.done)
	for {
		it := p.next()
		if it == nil {
			return
		}
		p.deliver(it)
		p.mu.Lock()
		p.inflight = false
		p.mu.Unlock()
	}
}

// Why an event was not logged, for the log line.
const (
	lostBudget = "the retries ran out"
	lostFull   = "the events stream is full"
	lostQueue  = "the publisher's queue is full"
	lostStop   = "the publisher stopped"
)

// deliver publishes one event, retrying with backoff until it is stored, the stream refuses it for
// space, or its budget, counted from when it was queued, is spent.
func (p *Publisher) deliver(it *item) {
	deadline := it.queuedAt.Add(p.o.Budget)
	backoff := p.o.MinBackoff
	var last error
	for {
		if p.ctx.Err() != nil {
			it.r.lost(lostStop, it.reason, it.id, last, it.closes)
			return
		}
		left := deadline.Sub(p.now())
		if left <= 0 {
			it.r.lost(lostBudget, it.reason, it.id, last, it.closes)
			return
		}
		full, err := p.attempt(it, left)
		switch {
		case err == nil:
			it.r.logged(it)
			return
		case full:
			it.r.lost(lostFull, it.reason, it.id, err, it.closes)
			return
		}
		last = err
		wait := min(backoff, max(left, 0))
		backoff = min(backoff*2, p.o.MaxBackoff)
		select {
		case <-time.After(wait):
		case <-p.ctx.Done():
		}
	}
}

// attempt connects if it must and publishes once, each bounded by what is left of the event's
// budget, so the last send cannot land past the duplicate window. full reports a stream-full
// refusal, which is terminal.
func (p *Publisher) attempt(it *item, left time.Duration) (full bool, err error) {
	p.attempts.Add(1)
	js, err := p.jetstream(min(p.o.ConnectTimeout, left))
	if err != nil {
		return false, err
	}
	pctx, cancel := context.WithTimeout(p.ctx, min(p.o.PublishTimeout, left))
	defer cancel()
	ack, err := js.PublishMsg(pctx, &nats.Msg{Subject: it.subject, Data: it.body}, jetstream.WithMsgID(it.id))
	if p.afterPublish != nil {
		err = p.afterPublish(it.id, ack, err)
	}
	if err == nil {
		return false, nil
	}
	if streamFull(err) {
		return true, err
	}
	// Any other failure leaves the connection suspect: drop it, and connect again at the next try.
	p.drop()
	return false, err
}

// streamFull reports nats-server's refusal of a message for a stream at its cap under discard new:
// "maximum bytes exceeded" (HBEP-8, Retention, In the durable log). It is never retried.
func streamFull(err error) bool {
	var api *jetstream.APIError
	if errors.As(err, &api) {
		d := strings.ToLower(api.Description)
		return strings.Contains(d, "maximum bytes exceeded") || strings.Contains(d, "maximum messages exceeded")
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "maximum bytes exceeded") || strings.Contains(s, "maximum messages exceeded")
}

// jetstream returns the open JetStream handle, connecting first if there is none or the connection
// has closed.
func (p *Publisher) jetstream(timeout time.Duration) (jetstream.JetStream, error) {
	if p.js != nil && p.nc != nil && p.nc.IsConnected() {
		return p.js, nil
	}
	p.drop()
	nc, err := nats.Connect("nats://"+p.o.Addr, nats.Token(p.o.Token), nats.NoReconnect(), nats.Timeout(timeout), nats.Name("hpcburst-event-publisher"))
	if err != nil {
		return nil, fmt.Errorf("connect to the durable log: %w", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("open JetStream on the durable log: %w", err)
	}
	p.nc, p.js = nc, js
	return js, nil
}

// drop closes the connection, so the next attempt opens a new one.
func (p *Publisher) drop() {
	if p.nc != nil {
		p.nc.Close()
	}
	p.nc, p.js = nil, nil
}

// Shutdown stops the publisher: it takes no more events, publishes what is queued until ctx ends,
// and counts what is left as not logged. A crash loses the events still queued in the process; an
// orderly stop is given the chance to publish them (HBEP-8, When it is down).
func (p *Publisher) Shutdown(ctx context.Context) {
	p.mu.Lock()
	p.stopping = true
	p.cond.Broadcast()
	p.mu.Unlock()
	select {
	case <-p.done:
	case <-ctx.Done():
		p.end()
		<-p.done
	}
	p.end()
	p.mu.Lock()
	rest := p.queue
	p.queue = nil
	p.mu.Unlock()
	for _, it := range rest {
		it.r.lost(lostStop, it.reason, it.id, nil, it.closes)
	}
	p.notify.stop()
	p.drop()
	if len(rest) > 0 {
		p.log.Warn("the durable log's publisher stopped with events not yet logged", "events", len(rest))
	}
}

// routeFor returns the route of a controller's events into counter, one per pair, so that every
// wrapper of one controller shares one loss gate: the lifecycle controller's recorders, the
// manager's and the tunnel-state reader's, raise one EventsNotLogged between them.
func (p *Publisher) routeFor(controller string, counter *Counter) *route {
	p.mu.Lock()
	defer p.mu.Unlock()
	k := routeKey{controller: controller, counter: counter}
	if r, ok := p.routes[k]; ok {
		return r
	}
	r := &route{p: p, controller: controller, counter: counter}
	p.routes[k] = r
	// The controller's manager stopped for good (its series are forgotten): its gate goes with it.
	counter.forgetHook(controller, func() { p.Unregister(controller, counter) })
	return r
}

// Wrapped is how many recorders of the controller have been built with this publisher (WithLog), over all the
// copies' counters. It is a diagnostic for the tests of a program's wiring: the option does nothing in a process
// with no log, so a recorder built without the publisher by mistake looks like one built with it, except here.
func (p *Publisher) Wrapped(controller string) int {
	p.mu.Lock()
	routes := make([]*route, 0, len(p.routes))
	for k, r := range p.routes {
		if k.controller == controller {
			routes = append(routes, r)
		}
	}
	p.mu.Unlock()
	n := 0
	for _, r := range routes {
		r.mu.Lock()
		n += r.wrapped
		r.mu.Unlock()
	}
	return n
}

// Unregister drops a controller's route and its loss gate: its manager has stopped for good. The
// next wrapper of the controller starts a new one.
func (p *Publisher) Unregister(controller string, counter *Counter) {
	k := routeKey{controller: controller, counter: counter}
	p.mu.Lock()
	r := p.routes[k]
	delete(p.routes, k)
	p.mu.Unlock()
	// Events of the controller still queued keep their route, which is dead now: they publish, and their
	// late losses are one log line each, uncounted (route.lost).
	if r != nil {
		r.markDead()
	}
}

type routeKey struct {
	controller string
	counter    *Counter
}
