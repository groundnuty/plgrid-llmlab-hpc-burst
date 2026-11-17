package eventcount

// Retiring a controller (Counter.Forget) against the events and raises it still has in flight (the delta review of
// #697, 17 Nov 2026): once the controller's series are deleted, nothing of the controller may bring one back.

import (
	"strings"
	"sync"
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

// peekRoute is the controller's route, if the publisher holds one, without making one or reviving a retired one.
func (r *rig) peekRoute(controller string) *route {
	r.pub.mu.Lock()
	defer r.pub.mu.Unlock()
	return r.pub.routes[routeKey{controller: controller, counter: r.counter}]
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
			v, _ := r.counter.Unlogged(ctl) // a live route counts it; the series stood at 0 before, so 0 is no sign of it
			return v >= 1 || strings.Contains(r.logs.String(), "stopped for good")
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
	finished := make(chan struct{}) // Forget has retired the route and deleted the series
	r.pub.emitHook = func() {
		go func() {
			r.counter.Forget(ctl, notLoggedSeries)
			close(finished)
		}()
		// Without retirement waiting for the emit, Forget finishes at once, deletion included, before the emit
		// counts: the series the emit then creates is the bug. With it, Forget waits for the emit, so give it
		// a second to show it does not.
		select {
		case <-finished:
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

// A manager that is started again after a failure runs its Setup, and so Zero, again for the same series set
// with no Forget between. That is one registration, not two: the one Forget of the Provider, when it is
// deleted, retires the route and deletes the series.
func TestAManagerSetUpAgainAfterAFailureIsOneRegistration(t *testing.T) {
	ctl := ControllerProvider("p1")
	set := []Series{{Reason: "JobStarted", Action: "start", Type: provider.EventKindOrdinary}}
	r := newRig(t, ctl, true, bigCaps, nil, leaseFor("provider-p1"))
	r.counter.Zero(ctl, set) // the manager's Setup
	r.counter.Zero(ctl, set) // the manager failed and was built again
	r.counter.Forget(ctl, set)
	if v, exists := r.counter.Unlogged(ctl); exists {
		t.Errorf("the deleted Provider's unlogged series survives its Forget, at %v: a rebuilt manager was counted twice", v)
	}
	if rt := r.peekRoute(ctl); rt != nil && !rt.isDead() {
		t.Error("the deleted Provider's route survives its Forget: a rebuilt manager was counted twice")
	}
}

// A manager that has attached its recorder but not yet reached the Zero at the end of its Setup, when the other
// kind's manager of the label is forgotten: the route is retired then, and the Zero revives it, with its series
// at 0 and a gate that starts afresh. The new manager's losses count and raise, as they would had the Forget come
// a moment later (the delta review of #697, 17 Nov 2026).
func TestAZeroAfterTheLabelsForgetRevivesItsRoute(t *testing.T) {
	clk := &fakeClock{t: base}
	ctl := ControllerProvider("p1")
	batch := []Series{{Reason: "JobStarted", Action: "start", Type: provider.EventKindOrdinary}}
	managed := []Series{{Reason: "JobEnded", Action: "end", Type: provider.EventKindOrdinary}}
	r := newRig(t, ctl, true, bigCaps, func(o *PublisherOptions) { o.Now = clk.now }, leaseFor("provider-p1"))
	r.counter.Zero(ctl, batch)
	inner2 := &recInner{}
	rec2 := Events(inner2, r.counter, ctl, WithLog(Log{Publisher: r.pub, Scheme: testScheme(), ReportingController: "hpcburst-test", Lease: leaseFor("provider-p1")}))
	r.counter.Forget(ctl, batch) // the batch manager is dropped before the managed one has zeroed its series
	if _, exists := r.counter.Unlogged(ctl); exists {
		t.Fatal("the Forget left the unlogged series, though no other manager had registered yet")
	}
	r.counter.Zero(ctl, managed) // the managed manager's Setup ends
	if v, exists := r.counter.Unlogged(ctl); !exists || v != 0 {
		t.Fatalf("the unlogged series after the Zero is %v (exists %v), want 0", v, exists)
	}
	rec2.EventfID("revive-a", alloc(crashUID), nil, provider.EventKindOrdinary, "JobEnded", "ended", "%s", "a note")
	clk.set(base.Add(101 * time.Second))
	within(t, 20*time.Second, "the revived route to count the loss", func() bool { v, _ := r.counter.Unlogged(ctl); return v >= 1 })
	within(t, 20*time.Second, "the revived route to raise EventsNotLogged", func() bool { return len(inner2.byReason(ReasonEventsNotLogged)) == 1 })
	if strings.Contains(r.logs.String(), "stopped for good") {
		t.Errorf("a revived manager's loss was treated as a stopped controller's: %q", r.logs.String())
	}
}

// A registration that arrives while the last Forget waits for an emit in flight is not retired with it: the
// Forget and the Zero are each one transition, in the order they took the lifecycle lock, so the label ends live,
// and the new manager's losses count and raise (the second delta review of #697, 17 Nov 2026).
func TestARegistrationDuringARetirementThatWaitsForAnEmitIsNotRetired(t *testing.T) {
	clk := &fakeClock{t: base}
	ctl := ControllerProvider("p1")
	setA, setB := seriesSetA(), seriesSetB()
	r := newRig(t, ctl, true, bigCaps, func(o *PublisherOptions) { o.Now = clk.now }, leaseFor("provider-p1"))
	r.counter.Zero(ctl, setA)
	inner2 := &recInner{}
	rec2 := Events(inner2, r.counter, ctl, WithLog(Log{Publisher: r.pub, Scheme: testScheme(), ReportingController: "hpcburst-test", Lease: leaseFor("provider-p1")}))
	inEmit, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	r.pub.emitHook = func() { // the first raise holds inside its recorder call until the test lets it go
		once.Do(func() { close(inEmit) })
		<-release
	}
	r.raise(crashUID, "reg-a", "JobStarted")
	clk.set(base.Add(101 * time.Second)) // the event is lost, and its first loss starts a raise
	select {
	case <-inEmit:
	case <-time.After(20 * time.Second):
		t.Fatal("the raise did not reach its recorder call")
	}
	forgot, zeroed := make(chan struct{}), make(chan struct{})
	go func() { r.counter.Forget(ctl, setA); close(forgot) }() // the last registration: it waits for the emit
	within(t, 20*time.Second, "the Forget to take its registration out", func() bool { return !r.counter.registered(ctl, setA) })
	go func() { r.counter.Zero(ctl, setB); close(zeroed) }() // the new manager registers meanwhile
	select {
	case <-zeroed: // nothing orders it behind the Forget
	case <-time.After(time.Second): // it waits its turn
	}
	close(release)
	for _, ch := range []chan struct{}{forgot, zeroed} {
		select {
		case <-ch:
		case <-time.After(20 * time.Second):
			t.Fatal("Forget or Zero did not finish: a deadlock between the lifecycle lock and an emit")
		}
	}
	if rt := r.peekRoute(ctl); rt == nil || rt.isDead() {
		t.Fatal("the label's route is dead though a manager registered: the retirement that waited for the emit took the registration with it")
	}
	// The route's raises go through the latest manager's recorder, the new one's, and the first raise's EventsNotLogged
	// went through it too: only what comes after counts.
	before := len(inner2.byReason(ReasonEventsNotLogged))
	rec2.EventfID("reg-b", alloc(crashUID), nil, provider.EventKindOrdinary, "JobEnded", "ended", "%s", "a note")
	clk.set(base.Add(202 * time.Second))
	within(t, 20*time.Second, "the new manager's loss to be counted", func() bool { v, _ := r.counter.Unlogged(ctl); return v >= 1 })
	within(t, 20*time.Second, "the new manager's EventsNotLogged", func() bool { return len(inner2.byReason(ReasonEventsNotLogged)) == before+1 })
}

// A registration in the middle of a Forget keeps what it created: the Zero waits for the Forget to finish, so the
// Forget's deletion, which knew only the registrations before it, cannot take the series the Zero made, nor the
// unlogged counter that the new manager's loss then counts into.
func TestARegistrationInTheMiddleOfAForgetKeepsTheSeriesItCreated(t *testing.T) {
	clk := &fakeClock{t: base}
	ctl := ControllerProvider("p1")
	setA, setB := seriesSetA(), seriesSetB()
	r := newRig(t, ctl, true, bigCaps, func(o *PublisherOptions) { o.Now = clk.now }, leaseFor("provider-p1"))
	r.counter.Zero(ctl, setA)
	inner2 := &recInner{}
	rec2 := Events(inner2, r.counter, ctl, WithLog(Log{Publisher: r.pub, Scheme: testScheme(), ReportingController: "hpcburst-test", Lease: leaseFor("provider-p1")}))
	zeroed := make(chan struct{})
	r.counter.forgetMid = func() { // the Forget has decided it was the last, and retired the route
		go func() { r.counter.Zero(ctl, setB); close(zeroed) }()
		select {
		case <-zeroed: // nothing ordered it behind the Forget
		case <-time.After(time.Second): // it waits its turn
		}
	}
	r.counter.Forget(ctl, setA)
	select {
	case <-zeroed:
	case <-time.After(20 * time.Second):
		t.Fatal("Zero did not finish after the Forget")
	}
	if n := raisedSeries(r.counter, ctl, ReasonEventsNotLogged); n != 1 {
		t.Errorf("%d EventsNotLogged series after the Zero, want the one the new manager created: the Forget deleted it", n)
	}
	if n := raisedSeries(r.counter, ctl, "JobEnded"); n != 1 {
		t.Errorf("%d JobEnded series after the Zero, want the new manager's own", n)
	}
	rec2.EventfID("mid-b", alloc(crashUID), nil, provider.EventKindOrdinary, "JobEnded", "ended", "%s", "a note")
	clk.set(base.Add(101 * time.Second))
	within(t, 20*time.Second, "the new manager's loss to be counted", func() bool { v, _ := r.counter.Unlogged(ctl); return v >= 1 })
	within(t, 20*time.Second, "the new manager's EventsNotLogged", func() bool { return len(inner2.byReason(ReasonEventsNotLogged)) == 1 })
}

func seriesSetA() []Series {
	return []Series{{Reason: "JobStarted", Action: "start", Type: provider.EventKindOrdinary}, {Reason: ReasonEventsNotLogged, Action: ActionDegraded, Type: provider.EventKindWarning}}
}

func seriesSetB() []Series {
	return []Series{{Reason: "JobEnded", Action: "end", Type: provider.EventKindOrdinary}, {Reason: ReasonEventsNotLogged, Action: ActionDegraded, Type: provider.EventKindWarning}}
}

// registered is whether the series set is one of the label's registrations.
func (c *Counter) registered(controller string, series []Series) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.regs[controller][seriesKey(series)]
	return ok
}
