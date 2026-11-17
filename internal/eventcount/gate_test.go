package eventcount

// The EventsNotLogged gate across runs of losses (HBEP-8, The durable log, When it is down; Repeating
// conditions; #625, PR E2's fix round after the reviews of #697). Each row drives the real publisher
// against the real log (nodetest) and reads what the gate raised from the recorder the wrapper hands
// the event to, and, where it says, from the log.

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"

	"github.com/nats-io/nats.go/jetstream"
)

// within waits for cond for at most d, and fails the test naming what, so a row that shows a bug does not wait
// a minute for it.
func within(t testing.TB, d time.Duration, what string, cond func() bool) {
	t.Helper()
	for end := time.Now().Add(d); !cond(); time.Sleep(2 * time.Millisecond) {
		if time.Now().After(end) {
			t.Fatalf("timed out after %s waiting for %s", d, what)
		}
	}
}

// pendingJobs is how many raise jobs wait for the notifier.
func (r *rig) pendingJobs() int {
	n := r.pub.notify
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.jobs)
}

// blockedLease is a Lease source whose reads from the first one on wait for the test to release them (or for
// the read's own deadline), and that counts them.
type blockedLease struct {
	name    string
	from    int32 // the first read that waits; 0 never waits
	release chan struct{}
	calls   atomic.Int32
}

func newBlockedLease(name string, from int32) *blockedLease {
	return &blockedLease{name: name, from: from, release: make(chan struct{})}
}

func (b *blockedLease) read(ctx context.Context) (*coordinationv1.Lease, error) {
	n := b.calls.Add(1)
	if b.from > 0 && n >= b.from {
		select {
		case <-b.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return lease(b.name), nil
}

// A loss run that ends while its first raise's Lease read is still in flight must not gate the next run: run 2's
// first loss raises its own EventsNotLogged (HBEP-8, Repeating conditions: the first loss after a success).
func TestANewRunIsNotGatedByAnOlderRunsRaise(t *testing.T) {
	clk := &fakeClock{t: base}
	ctl := ControllerProvider("p1")
	l := newBlockedLease("provider-p1", 1) // every read waits until released
	r := newRig(t, ctl, true, bigCaps, func(o *PublisherOptions) { o.Now = clk.now }, l.read)
	r.pub.leaseTimeoutFor = time.Hour // the blocked read waits for the test, whatever the time between the two runs
	// Run 1: the log is down, and the event's retries run out. Its raise's Lease read waits.
	r.raise(crashUID, "gate-a", "JobStarted")
	clk.set(base.Add(101 * time.Second))
	within(t, 20*time.Second, "run 1's first loss", func() bool { return r.unlogged(ctl) == 1 })
	within(t, 20*time.Second, "run 1's raise to start its Lease read", func() bool { return l.calls.Load() == 1 })
	// The log returns and takes an event: run 1 ends while the raise is still in flight.
	r.proc = r.d.Start(t)
	r.proc.Ready()
	r.raise(crashUID, "gate-b", "JobStarted")
	within(t, 20*time.Second, "the log to hold an event", func() bool { return r.streamMsgs() >= 1 })
	r.settled()
	// The log dies again and run 2 starts, still inside the one Lease read.
	r.proc.Kill()
	r.raise(crashUID, "gate-c", "JobStarted")
	clk.set(base.Add(202 * time.Second))
	within(t, 20*time.Second, "run 2's first loss", func() bool { return r.unlogged(ctl) >= 2 })
	r.settled()
	close(l.release)
	// Run 2's first loss raises an EventsNotLogged of its own, saying since when.
	since := base.Add(202 * time.Second).Format(time.RFC3339)
	within(t, 10*time.Second, "an EventsNotLogged for run 2, since "+since, func() bool {
		for _, c := range r.inner.byReason(ReasonEventsNotLogged) {
			if strings.Contains(c.note, "since "+since) {
				return true
			}
		}
		return false
	})
}

// oscillation drives a publisher whose queue is full and whose log is live but slow: each head publish succeeds
// when the test lets it, and a tail event is dropped each time the queue has a free place. It returns the largest
// number of jobs that waited for the notifier, and a function that lets every publish through.
func oscillation(t *testing.T, r *rig, cycles int) (maxJobs int, drain func()) {
	t.Helper()
	gate, done := make(chan struct{}, 4096), make(chan struct{})
	r.pub.afterPublish = func(id string, ack *jetstream.PubAck, err error) error {
		select {
		case <-gate:
		case <-done:
		}
		return err
	}
	var once sync.Once
	drain = func() { once.Do(func() { close(done) }) }
	t.Cleanup(drain)
	n := 0
	raise := func() {
		n++
		r.raise(crashUID, fmt.Sprintf("osc-%04d", n), "JobStarted")
	}
	size := r.pub.o.QueueSize
	for i := 0; i < size+4; i++ { // the burst: the queue fills, the rest are dropped
		raise()
	}
	for c := 0; c < cycles; c++ {
		gate <- struct{}{} // the head is stored, when the publisher's hook takes the token
		within(t, 20*time.Second, "the publisher to take the token", func() bool { return len(gate) == 0 })
		raise() // takes the free place, if an EventsNotLogged has not
		raise() // dropped
		maxJobs = max(maxJobs, r.pendingJobs())
	}
	return maxJobs, drain
}

// With a full queue and a live log, the raise jobs waiting for the notifier stay bounded however many events are
// dropped, and the API is not asked for the Lease again and again.
func TestAFullQueueOnALiveLogQueuesAtMostTwoRaiseJobsAndFewLeaseReads(t *testing.T) {
	ctl := ControllerProvider("p1")
	l := newBlockedLease("provider-p1", 1) // the first read waits: every later job waits behind it
	r := newRig(t, ctl, false, bigCaps, func(o *PublisherOptions) { o.QueueSize = 4 }, l.read)
	maxJobs, drain := oscillation(t, r, 30)
	if maxJobs > 2 {
		t.Errorf("%d raise jobs waited for the notifier at once, want at most 2 (one loss, one closing)", maxJobs)
	}
	close(l.release)
	drain()
	within(t, 30*time.Second, "the notifier to finish", func() bool { return r.pendingJobs() == 0 })
	if got := l.calls.Load(); got > 4 {
		t.Errorf("%d Lease reads for one burst of drops, want at most 4", got)
	}
}

// Under the same oscillation, with the Lease at hand, at most two EventsNotLogged are raised: the run's first
// loss and its closing, since a run of queue-full losses ends only once the queue has drained below half
// (the science agent's ruling on #697, 17 Nov 2026).
func TestAFullQueueOnALiveLogRaisesAtMostTwoEventsNotLogged(t *testing.T) {
	ctl := ControllerProvider("p1")
	r := newRig(t, ctl, false, bigCaps, func(o *PublisherOptions) { o.QueueSize = 4 }, leaseFor("provider-p1"))
	_, drain := oscillation(t, r, 30)
	drain() // the log is live, so every event queued is stored
	r.settled()
	within(t, 30*time.Second, "the notifier to finish", func() bool { return r.pendingJobs() == 0 })
	time.Sleep(200 * time.Millisecond)
	if got := len(r.inner.byReason(ReasonEventsNotLogged)); got > 2 {
		t.Errorf("%d EventsNotLogged raised under one oscillation, want at most 2: the loss and its closing", got)
	}
	if got := len(r.inner.byReason(ReasonEventsNotLogged)); got < 2 {
		t.Errorf("%d EventsNotLogged raised, want the loss and its closing", got)
	}
}

// The closing stays owed until the closing event itself is stored: one that is lost with the log down leaves the
// span to the next closing, which then says since the earliest unreported loss, with the counts of both runs
// (the science agent's ruling on #697, 17 Nov 2026).
func TestALostClosingIsCoveredByTheNextOne(t *testing.T) {
	clk := &fakeClock{t: base}
	ctl := ControllerProvider("p1")
	l := newBlockedLease("provider-p1", 2) // the second read, the first closing's, waits
	r := newRig(t, ctl, true, bigCaps, func(o *PublisherOptions) { o.Now = clk.now }, l.read)
	t1 := base.Add(101 * time.Second)
	// Run 1: one event is lost with the log down; its raise, E1, is queued.
	r.raise(crashUID, "close-a", "JobStarted")
	clk.set(t1)
	within(t, 20*time.Second, "run 1's loss", func() bool { return r.unlogged(ctl) == 1 })
	within(t, 20*time.Second, "run 1's raise", func() bool { return len(r.inner.byReason(ReasonEventsNotLogged)) == 1 })
	// The log returns, takes E1, and run 1 ends: its closing's Lease read waits.
	r.proc = r.d.Start(t)
	r.proc.Ready()
	within(t, 20*time.Second, "the closing's Lease read", func() bool { return l.calls.Load() >= 2 })
	within(t, 20*time.Second, "the log to hold E1", func() bool { return r.streamMsgs() >= 1 })
	r.proc.Kill()
	// The closing is raised with the log down, and its retries run out: it is lost.
	close(l.release)
	within(t, 20*time.Second, "the closing to be raised", func() bool { return len(r.inner.byReason(ReasonEventsNotLogged)) == 2 })
	clk.set(base.Add(202 * time.Second))
	r.settled()
	// Run 2: one more event is lost; the log returns and takes an event: run 2's closing.
	r.raise(crashUID, "close-d", "JobStarted")
	clk.set(base.Add(303 * time.Second))
	within(t, 20*time.Second, "run 2's loss", func() bool { return r.unlogged(ctl) >= 3 })
	within(t, 20*time.Second, "run 2's event to be lost (its raise stays queued: the clock is still)", func() bool { return r.pub.Pending() <= 1 })
	r.proc = r.d.Start(t)
	r.proc.Ready()
	r.raise(crashUID, "close-e", "JobEnded")
	closings := func() []string {
		var out []string
		for _, m := range r.read() {
			_, d, err := Decode(m.Payload)
			if err == nil && d.Reason == ReasonEventsNotLogged && strings.Contains(d.Note, "takes events again") {
				out = append(out, d.Note)
			}
		}
		return out
	}
	within(t, 10*time.Second, "a closing EventsNotLogged in the log", func() bool { return len(closings()) >= 1 })
	r.settled()
	time.Sleep(300 * time.Millisecond)
	got := closings()
	want := "2 events were not logged from " + t1.Format(time.RFC3339)
	if len(got) != 1 || !strings.Contains(got[0], want) {
		t.Errorf("the log holds closings %q, want exactly one saying %q: the first closing was lost, so the next one covers both runs", got, want)
	}
}

// A controller whose manager has stopped for good (its series forgotten) counts nothing and raises nothing for
// an event still queued when it went: one log line, and the event still publishes (the science agent's ruling on
// #697, 17 Nov 2026).
func TestAForgottenControllersLateLossIsNeitherCountedNorRaised(t *testing.T) {
	clk := &fakeClock{t: base}
	ctl := ControllerProvider("p1")
	r := newRig(t, ctl, true, bigCaps, func(o *PublisherOptions) { o.Now = clk.now }, leaseFor("provider-p1"))
	r.raise(crashUID, "forgot-a", "JobStarted")
	r.counter.Forget(ctl, nil) // the Provider is gone
	if _, exists := r.counter.Unlogged(ctl); exists {
		t.Fatal("Forget left the unlogged series")
	}
	clk.set(base.Add(101 * time.Second)) // the event's retries run out
	// The event is lost; a raise, were there one, would stay queued, the clock being still.
	within(t, 20*time.Second, "the event to be lost", func() bool { return r.pub.Pending() <= 1 })
	time.Sleep(300 * time.Millisecond) // a raise would be handed over by now
	if v, exists := r.counter.Unlogged(ctl); exists {
		t.Errorf("the forgotten controller's unlogged series is back, at %v", v)
	}
	if got := r.inner.byReason(ReasonEventsNotLogged); len(got) != 0 {
		t.Errorf("a forgotten controller raised %d EventsNotLogged", len(got))
	}
	if n := strings.Count(r.logs.String(), "stopped for good"); n != 1 {
		t.Errorf("the publisher logged %d lines for the late loss, want one: %q", n, r.logs.String())
	}
}

// Many short runs of losses, each ended by a publish the log takes, queue at most one loss job and one
// closing job for the notifier, whatever happens to be waiting on the API: a new job for the same route and
// kind updates the waiting one in place. The route's gate is driven directly, a run at a time, with the
// notifier held in a Lease read.
func TestShortRunsQueueAtMostOneLossJobAndOneClosingJob(t *testing.T) {
	ctl := ControllerProvider("p1")
	l := newBlockedLease("provider-p1", 1)
	r := newRig(t, ctl, false, bigCaps, nil, l.read)
	rt := r.route(ctl)
	rt.lost(lostBudget, "JobStarted", "short-0", nil, nil) // the first job is taken by the notifier, which waits for the Lease
	within(t, 20*time.Second, "the notifier to take the first job", func() bool { return l.calls.Load() == 1 })
	maxJobs := 0
	for i := 1; i <= 40; i++ {
		rt.logged(&item{r: rt, id: "ok"})                                       // the run ends: a closing is owed
		rt.lost(lostBudget, "JobStarted", fmt.Sprintf("short-%d", i), nil, nil) // a new run starts
		maxJobs = max(maxJobs, r.pendingJobs())
	}
	if maxJobs > 2 {
		t.Errorf("%d raise jobs waited at once, want at most 2: one loss job and one closing job, updated in place", maxJobs)
	}
	close(l.release)
	within(t, 20*time.Second, "the notifier to finish", func() bool { return r.pendingJobs() == 0 })
}
