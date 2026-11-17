package eventcount

// Two routes on one publisher (the late review of #697's first delta, 17 Nov 2026, and the science agent's ruling on
// it): the queue is shared by every controller of the process, and a queue-full run of one route ends when any
// route's publish succeeds with the queue below half its size.

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// One queue is shared by the lifecycle controller and every provider-<name> route. A route whose run holds a
// queue-full loss, and which then publishes nothing more, must not stay in that run for as long as the process
// lives because the queue it dropped from has drained under the other route's publishes.
func TestAQueueFullRunOfAnIdleRouteEndsWhenAnotherRoutesBacklogDrains(t *testing.T) {
	ctl := ControllerProvider("p1")
	r := newRig(t, ctl, false, bigCaps, func(o *PublisherOptions) { o.QueueSize = 4 }, leaseFor("provider-p1"))
	lifeInner := &recInner{}
	life := Events(lifeInner, r.counter, ControllerLifecycle, WithLog(Log{Publisher: r.pub, Scheme: testScheme(), ReportingController: "hpcburst-test", Lease: leaseFor("lifecycle")}))
	rt := r.route(ctl)
	_, drain := gateAll(t, r)
	for i := 0; i < 8; i++ { // the lifecycle controller fills the queue; the rest are its own drops
		life.EventfID(fmtID("life", i), alloc(crashUID), nil, provider.EventKindOrdinary, "JobStarted", "started", "%s", "a note")
	}
	r.raise(crashUID, "p1-dropped", "JobStarted") // provider-p1's one event meets the full queue: its run starts, queue-full
	within(t, 20*time.Second, "provider-p1's run to hold a queue-full loss", func() bool {
		rt.mu.Lock()
		defer rt.mu.Unlock()
		return rt.losing && rt.runFull
	})
	drain() // the log is live: the lifecycle backlog is stored, and the queue empties
	r.settled()
	within(t, 20*time.Second, "the notifier to settle", func() bool { return r.pendingJobs() == 0 })
	ended := func() bool {
		rt.mu.Lock()
		defer rt.mu.Unlock()
		return !rt.losing
	}
	// The queue is empty and the lifecycle controller keeps publishing: provider-p1's run has no reason to be open.
	for i := 0; i < 20 && !ended(); i++ {
		life.EventfID(fmtID("life-more", i), alloc(crashUID), nil, provider.EventKindOrdinary, "JobStarted", "started", "%s", "a note")
		r.settled()
		time.Sleep(20 * time.Millisecond)
	}
	if !ended() {
		t.Fatal("provider-p1's queue-full run is still open after the queue drained below half and the lifecycle controller published 20 more events: it would stay open until provider-p1 itself publishes, and its closing would not be raised")
	}
	within(t, 20*time.Second, "provider-p1's closing", func() bool {
		for _, c := range r.inner.byReason(ReasonEventsNotLogged) {
			if strings.Contains(c.note, "takes events again") {
				return true
			}
		}
		return false
	})
}

func fmtID(prefix string, i int) string { return fmt.Sprintf("%s-%03d", prefix, i) }

// gateAll holds every publish of the rig's publisher until drain is called, as the oscillation helper's hook does.
func gateAll(t *testing.T, r *rig) (struct{}, func()) {
	t.Helper()
	done := make(chan struct{})
	r.pub.afterPublish = func(id string, ack *jetstream.PubAck, err error) error {
		<-done
		return err
	}
	var once sync.Once
	drain := func() { once.Do(func() { close(done) }) }
	t.Cleanup(drain)
	return struct{}{}, drain
}
