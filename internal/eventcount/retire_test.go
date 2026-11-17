package eventcount

// Retiring a controller (Counter.Forget) against the events and raises it still has in flight (the delta review of
// #697, 17 Nov 2026): once the controller's series are deleted, nothing of the controller may bring one back.

import (
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

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
