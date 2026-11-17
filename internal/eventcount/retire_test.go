package eventcount

// Retiring a controller (Counter.Forget) against the events and raises it still has in flight (the delta review of
// #697, 17 Nov 2026): once the controller's series are deleted, nothing of the controller may bring one back.

import (
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// raisedSeries is how many series of hpcburst_events_raised_total the counter holds for the controller and the
// reason. Reading it creates none.
func raisedSeries(c *Counter, controller, reason string) int {
	ch := make(chan prometheus.Metric)
	go func() { c.vec.Collect(ch); close(ch) }()
	n := 0
	for m := range ch {
		var d dto.Metric
		if err := m.Write(&d); err != nil {
			continue
		}
		match := map[string]string{}
		for _, l := range d.GetLabel() {
			match[l.GetName()] = l.GetValue()
		}
		if match[LabelController] == controller && match[LabelReason] == reason {
			n++
		}
	}
	return n
}

// notLoggedSeries is the series Forget is told to delete for EventsNotLogged.
var notLoggedSeries = []Series{{Reason: ReasonEventsNotLogged, Action: ActionDegraded, Type: provider.EventKindWarning}}

// An event of the controller whose retries run out between Forget's retiring the controller and its deleting
// the series is lost by a route that is already dead: one log line, nothing counted, so the series Forget
// deletes stays deleted. Retiring after the delete would let the loss count into a series that is no longer
// there.
func TestAnEventLostInTheMiddleOfForgetLeavesNoSeries(t *testing.T) {
	clk := &fakeClock{t: base}
	ctl := ControllerProvider("p1")
	r := newRig(t, ctl, true, bigCaps, func(o *PublisherOptions) { o.Now = clk.now }, leaseFor("provider-p1"))
	r.raise(crashUID, "retire-a", "JobStarted") // queued: the log is down
	r.counter.forgetMid = func() {
		clk.set(base.Add(101 * time.Second)) // the event's retries run out now
		within(t, 20*time.Second, "the event to be lost", func() bool {
			_, exists := r.counter.Unlogged(ctl) // a live route counts it
			return exists || strings.Contains(r.logs.String(), "stopped for good")
		})
	}
	r.counter.Forget(ctl, notLoggedSeries)
	if v, exists := r.counter.Unlogged(ctl); exists {
		t.Errorf("the forgotten controller's unlogged series is back, at %v", v)
	}
	if n := r.inner.byReason(ReasonEventsNotLogged); len(n) != 0 {
		t.Errorf("a forgotten controller raised %d EventsNotLogged", len(n))
	}
}

// A raise that is between its dead check and its recorder call when Forget runs either completes before the
// controller is retired, or is skipped as dead: retirement waits for it. Either way the series Forget deletes
// stay deleted: the stopped manager's recorder is not called after Forget has finished.
func TestAForgetBetweenARaisesDeadCheckAndItsEmitLeavesNoSeries(t *testing.T) {
	clk := &fakeClock{t: base}
	ctl := ControllerProvider("p1")
	r := newRig(t, ctl, true, bigCaps, func(o *PublisherOptions) { o.Now = clk.now }, leaseFor("provider-p1"))
	retired := make(chan struct{}) // Forget has retired the route
	finished := make(chan struct{})
	r.counter.forgetMid = func() { close(retired) }
	r.pub.emitHook = func() {
		go func() {
			r.counter.Forget(ctl, notLoggedSeries)
			close(finished)
		}()
		// Without retirement waiting for the emit, Forget retires at once. With it, Forget waits, so give it
		// a second to show it does not.
		select {
		case <-retired:
		case <-time.After(time.Second):
		}
	}
	r.raise(crashUID, "retire-b", "JobStarted")
	clk.set(base.Add(101 * time.Second)) // the event is lost, and its first loss starts a raise
	within(t, 30*time.Second, "Forget to finish", func() bool {
		select {
		case <-finished:
			return true
		default:
			return false
		}
	})
	within(t, 30*time.Second, "the raise to finish", func() bool { return r.pendingJobs() == 0 && r.route(ctl).gateIdle() })
	if n := raisedSeries(r.counter, ctl, ReasonEventsNotLogged); n != 0 {
		t.Errorf("%d EventsNotLogged series of the forgotten controller are back: its recorder was called after Forget had deleted them", n)
	}
	if v, exists := r.counter.Unlogged(ctl); exists {
		t.Errorf("the forgotten controller's unlogged series is back, at %v", v)
	}
}

// Two managers of one label at once, as when a Provider changes kind (a batch manager and a managed one): the
// first is dropped, and its Forget names its own series. The route and the unlogged series belong to the label,
// so they live as long as any manager of the label does: the second's losses still count, its
// EventsNotLogged is still raised, and only the last Forget retires the route and deletes the series (the delta
// review of #697, 17 Nov 2026).
func TestAForgetOfOneManagerOfALabelLeavesTheOthersRouteAlone(t *testing.T) {
	clk := &fakeClock{t: base}
	ctl := ControllerProvider("p1")
	batch := []Series{{Reason: "JobStarted", Action: "start", Type: provider.EventKindOrdinary}}
	managed := []Series{{Reason: "JobEnded", Action: "end", Type: provider.EventKindOrdinary}}
	r := newRig(t, ctl, true, bigCaps, func(o *PublisherOptions) { o.Now = clk.now }, leaseFor("provider-p1"))
	r.counter.Zero(ctl, batch) // the batch manager's Setup
	// The managed manager of the same name is set up before the batch one is dropped.
	inner2 := &recInner{}
	rec2 := Events(inner2, r.counter, ctl, WithLog(Log{Publisher: r.pub, Scheme: testScheme(), ReportingController: "hpcburst-test", Lease: leaseFor("provider-p1")}))
	r.counter.Zero(ctl, managed)
	r.counter.Forget(ctl, batch) // the batch pass drops its manager
	if _, exists := r.counter.Unlogged(ctl); !exists {
		t.Fatal("the first manager's Forget deleted the unlogged series that the second manager still counts into")
	}
	// The second manager's event is lost with the log down: counted, and its EventsNotLogged raised through
	// the second manager's recorder.
	rec2.EventfID("live-a", alloc(crashUID), nil, provider.EventKindOrdinary, "JobEnded", "ended", "%s", "a note")
	clk.set(base.Add(101 * time.Second))
	within(t, 20*time.Second, "the second manager's loss to be counted", func() bool { v, _ := r.counter.Unlogged(ctl); return v >= 1 })
	within(t, 20*time.Second, "the second manager's EventsNotLogged", func() bool { return len(inner2.byReason(ReasonEventsNotLogged)) == 1 })
	if strings.Contains(r.logs.String(), "stopped for good") {
		t.Errorf("a live manager's loss was treated as a stopped controller's: %q", r.logs.String())
	}
	// The last manager's Forget retires the route and deletes the series; a late loss is one log line.
	r.counter.Forget(ctl, managed)
	if _, exists := r.counter.Unlogged(ctl); exists {
		t.Error("the last Forget left the unlogged series")
	}
	rec2.EventfID("live-b", alloc(crashUID), nil, provider.EventKindOrdinary, "JobEnded", "ended", "%s", "a note")
	clk.set(base.Add(202 * time.Second))
	within(t, 20*time.Second, "the late loss to be refused", func() bool { return strings.Contains(r.logs.String(), "stopped for good") })
	if _, exists := r.counter.Unlogged(ctl); exists {
		t.Error("a late loss after the last Forget brought the series back")
	}
}
