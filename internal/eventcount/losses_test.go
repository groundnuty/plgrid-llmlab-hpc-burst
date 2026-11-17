package eventcount

// What the publisher does when the log cannot take an event (HBEP-8, The durable log, When it is down;
// Test plan, Beta). The events that are lost are counted in hpcburst_events_unlogged_total, and the
// emitter raises EventsNotLogged at the first loss after the start or after a success (R10), at most once
// an hour while losses last, and once more with the first publish that succeeds after the log returns.
//
// Presence is read from the log, or from what the wrapper handed to the recorder; the raised-events
// counter shows absence only (HBEP-8, Raised events).

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/common/expfmt"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventlog"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventlog/node/nodetest"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

const crashUID = "0000000a-beef-4000-8000-00000000000a"

// leaseSubject is the subject of EventsNotLogged about the synthetic Lease of provider-p1.
const leaseSubject = "hb.ev.lease.uid-of-provider-p1"

// Connecting is retried, never a single attempt (#669): the log is not up when the first event
// arrives, the publisher tries again and again, and the event is stored once the log serves.
func TestAConnectIsRetriedUntilTheLogIsUp(t *testing.T) {
	r := newRig(t, ControllerProvider("p1"), true, bigCaps, nil, nil)
	r.raise(crashUID, "retry-connect-1", "JobStarted")
	eventually(t, "several failed attempts to connect", func() bool { return r.pub.attempts.Load() >= 3 })
	if got := r.unlogged(ControllerProvider("p1")); got != 0 {
		t.Fatalf("unlogged is %v while the budget is not spent", got)
	}
	r.proc = r.d.Start(t)
	r.proc.Ready()
	r.settled()
	if got := ids(r.read()); len(got) != 1 || got[0] != "retry-connect-1" {
		t.Fatalf("the stream holds %v, want the one event, stored once the log served", got)
	}
	if got := r.unlogged(ControllerProvider("p1")); got != 0 {
		t.Errorf("unlogged is %v, want 0: the log was back within the retries", got)
	}
}

// The log is killed while the publisher is connected, restarted on its store, and the publisher
// connects again with no event lost: the window after a restart in which nats-server lists its
// streams before its client port listens (#669) is ridden out by the retries, not by the supervisor's
// barrier, which covers the start only.
func TestAKilledLogIsReconnectedWithNoEventLost(t *testing.T) {
	r := newRig(t, ControllerProvider("p1"), false, bigCaps, nil, nil)
	r.raise(crashUID, "reconnect-1", "JobStarted")
	r.settled()
	r.proc.Kill()
	before := r.pub.attempts.Load()
	r.raise(crashUID, "reconnect-2", "JobEnded")
	eventually(t, "a failed attempt against the dead log", func() bool { return r.pub.attempts.Load() >= before+2 })
	r.restart()
	r.settled()
	if got := ids(r.read()); strings.Join(got, ",") != "reconnect-1,reconnect-2" {
		t.Fatalf("the stream holds %v, want both events, none lost", got)
	}
	if got := r.unlogged(ControllerProvider("p1")); got != 0 {
		t.Errorf("unlogged is %v, want 0", got)
	}
}

// The queue holds 1,000 events per process, the one in flight included; the 1,001st is counted at once
// and the emit site does not wait.
func TestAFullQueueCountsTheEventAtOnce(t *testing.T) {
	r := newRig(t, ControllerProvider("p1"), true, bigCaps, nil, nil)
	for n := 0; n < 1000; n++ {
		r.raise(crashUID, fmt.Sprintf("queued-%04d", n), "JobStarted")
	}
	if got := r.unlogged(ControllerProvider("p1")); got != 0 {
		t.Fatalf("unlogged is %v after 1,000 events, which the queue holds", got)
	}
	r.raise(crashUID, "queued-1000", "JobStarted")
	if got := r.unlogged(ControllerProvider("p1")); got != 1 {
		t.Fatalf("unlogged is %v right after the 1,001st event, want 1: a full queue counts at once", got)
	}
	if got := r.pub.Pending(); got != 1000 {
		t.Errorf("%d events pending, want 1,000", got)
	}
}

// An event is retried for 100 s from when it was queued, not for less and not for more: the budget is
// counted on the publisher's clock, which the test moves.
func TestTheBudgetCountsFromWhenTheEventWasQueued(t *testing.T) {
	clk := &fakeClock{t: base}
	ctl := ControllerProvider("p1")
	r := newRig(t, ctl, true, bigCaps, func(o *PublisherOptions) { o.Now = clk.now }, nil)
	if DefaultBudget != 100*time.Second {
		t.Fatalf("the default budget is %s, want 100 s", DefaultBudget)
	}
	r.raise(crashUID, "budget-1", "JobStarted")
	clk.set(base.Add(99 * time.Second))
	seen := r.pub.attempts.Load()
	eventually(t, "the publisher to try again at 99 s", func() bool { return r.pub.attempts.Load() >= seen+2 })
	if got := r.unlogged(ctl); got != 0 {
		t.Fatalf("unlogged is %v at 99 s: the event still has its retries", got)
	}
	clk.set(base.Add(101 * time.Second))
	eventually(t, "the event to be lost at 101 s", func() bool { return r.unlogged(ctl) == 1 })
	if got := r.pub.Pending(); got != 0 {
		t.Errorf("%d events still pending after the budget was spent", got)
	}
}

// The budget keeps every retry inside the stream's duplicate window, so that a retry never stores a
// second copy.
func TestTheBudgetIsInsideTheDuplicateWindow(t *testing.T) {
	if DefaultBudget+DefaultPublishTimeout >= eventlog.DuplicateWindow {
		t.Fatalf("a retry may start at %s and last %s after the event was queued: past the %s window", DefaultBudget, DefaultPublishTimeout, eventlog.DuplicateWindow)
	}
}

// A refused note is counted in the same series as the rest. It reached neither the Kubernetes recorder
// nor the log, and, since the log did not fail, it raises no EventsNotLogged: the call counts and
// returns, and nothing is left running that could raise one.
func TestARefusedNoteCountsInTheUnloggedSeries(t *testing.T) {
	ctl := ControllerProvider("p1")
	r := newRig(t, ctl, false, bigCaps, nil, leaseFor("provider-p1"))
	r.rec.NotLogged(alloc(crashUID), "JobEnded")
	if got := r.unlogged(ctl); got != 1 {
		t.Fatalf("unlogged is %v after one refused note, want 1", got)
	}
	r.settled()
	if n := len(r.inner.calls); n != 0 {
		t.Errorf("the Kubernetes recorder got %d events after a refused note, want none (not the event, not an EventsNotLogged): %+v", n, r.inner.calls)
	}
	if got := ids(r.read()); len(got) != 0 {
		t.Errorf("the log holds %v after a refused note, want nothing", got)
	}
}

// A regarding object with no UID cannot form a subject: the event is counted as not logged, with one
// log line naming its kind and name, and nothing reaches the stream.
func TestAnEventWithNoUIDIsCountedAndNamedInOneLogLine(t *testing.T) {
	ctl := ControllerProvider("p1")
	r := newRig(t, ctl, false, bigCaps, nil, nil)
	a := alloc("")
	a.Name = "no-uid-allocation"
	r.rec.EventfID("no-uid-1", a, nil, provider.EventKindOrdinary, "JobStarted", "ready", "n")
	if got := r.unlogged(ctl); got != 1 {
		t.Fatalf("unlogged is %v, want 1", got)
	}
	if lines := strings.Count(r.logs.String(), "\n"); lines != 1 || !strings.Contains(r.logs.String(), "Allocation ns-a/no-uid-allocation") {
		t.Errorf("the log is %q, want one line naming Allocation ns-a/no-uid-allocation", r.logs.String())
	}
	if n := len(r.inner.byReason(ReasonEventsNotLogged)); n != 0 {
		t.Errorf("%d EventsNotLogged handed over for an event with no subject, which says nothing about the log", n)
	}
	r.settled()
	if got := ids(r.read()); len(got) != 0 {
		t.Errorf("the stream holds %v, want nothing", got)
	}
}

// R10: a process start counts as a success, so the first loss after it raises EventsNotLogged, a log
// that is not up yet included. It regards the controller's own Lease, and its note gives the count and
// since when. The log, when it comes up inside the EventsNotLogged's own retries, holds it.
func TestTheFirstLossAfterTheStartRaisesEventsNotLogged(t *testing.T) {
	clk := &fakeClock{t: base}
	ctl := ControllerProvider("p1")
	r := newRig(t, ctl, true, bigCaps, func(o *PublisherOptions) { o.Now = clk.now }, leaseFor("provider-p1"))
	r.raise(crashUID, "r10-1", "JobStarted")
	clk.set(base.Add(101 * time.Second))
	eventually(t, "the first loss to be handled", func() bool { return r.route(ctl).handled.Load() >= 1 })
	got := r.inner.byReason(ReasonEventsNotLogged)
	if len(got) != 1 {
		t.Fatalf("%d EventsNotLogged handed to the recorder after the first loss, want 1", len(got))
	}
	first := got[0]
	if l, ok := first.regarding.(interface{ GetName() string }); !ok || l.GetName() != "provider-p1" || first.action != "degraded" || first.eventtype != provider.EventKindWarning {
		t.Errorf("EventsNotLogged regards %v as %s/%s, want the provider-p1 Lease as degraded/Warning", first.regarding, first.action, first.eventtype)
	}
	if !strings.Contains(first.note, "1 events not logged since 2026-11-17T12:01:41Z") {
		t.Errorf("the note is %q, want the count and since when", first.note)
	}
	// The log comes up: the event the first loss raised, still within its own retries, is stored.
	r.proc = r.d.Start(t)
	r.proc.Ready()
	eventually(t, "EventsNotLogged to be stored in the log", func() bool {
		_, js := r.d.Connect(t)
		return nodetest.StreamInfo(t, js, "HB_EVENTS").State.Msgs >= 1
	})
	r.settled()
	// The log then holds the event the first loss raised, and, since it was the first publish the log
	// took, the run's closing one.
	ms := r.read()
	if len(ms) != 2 {
		t.Fatalf("the log holds %v, want the first loss's EventsNotLogged and the run's closing one", ids(ms))
	}
	for i, wantNote := range []string{"1 events not logged since 2026-11-17T12:01:41Z", "the durable log takes events again; 1 events were not logged from 2026-11-17T12:01:41Z"} {
		_, d, err := Decode(ms[i].Payload)
		if err != nil {
			t.Fatal(err)
		}
		if d.Reason != ReasonEventsNotLogged || ms[i].Subject != leaseSubject || !strings.Contains(d.Note, wantNote) {
			t.Errorf("message %d is %s on %s with note %q, want EventsNotLogged on %s saying %q", i, d.Reason, ms[i].Subject, d.Note, leaseSubject, wantNote)
		}
	}
}

// Losses that last three hours, on a fake clock: at most four EventsNotLogged are raised, and one more
// once the first publish succeeds again, which the log holds. Each tick waits on the loss being handled.
func TestEventsNotLoggedAtMostOncePerHourAndOnceMoreOnReturn(t *testing.T) {
	clk := &fakeClock{t: base}
	ctl := ControllerProvider("p1")
	r := newRig(t, ctl, true, bigCaps, func(o *PublisherOptions) { o.Now = clk.now }, leaseFor("provider-p1"))
	r.counter.Zero(ctl, []Series{{Reason: ReasonEventsNotLogged, Action: ActionDegraded, Type: provider.EventKindWarning}})
	rt := r.route(ctl)
	for i := 0; i <= 6; i++ { // one loss each half hour, from 0 to 3 h
		clk.set(base.Add(time.Duration(i) * 30 * time.Minute))
		want := r.unlogged(ctl) + 1
		r.raise(crashUID, fmt.Sprintf("hourly-%d", i), "JobStarted")
		clk.advance(101 * time.Second)
		eventually(t, fmt.Sprintf("tick %d's loss to be handled", i), func() bool {
			u := r.unlogged(ctl)
			return u >= want && rt.handled.Load() == uint64(u) && r.pub.Pending() <= 1
		})
	}
	raised, _ := value(t, r.counter, Key{ctl, ReasonEventsNotLogged, ActionDegraded, provider.EventKindWarning})
	if raised > 4 {
		t.Fatalf("EventsNotLogged raised %v times in 3 h of losses, want at most 4", raised)
	}
	if got := len(r.inner.byReason(ReasonEventsNotLogged)); got != 4 {
		t.Fatalf("%d EventsNotLogged handed to the recorder in 3 h, want 4: at 0, 1 h, 2 h and 3 h", got)
	}
	// The log returns; the first publish it takes closes the run with one more.
	r.proc = r.d.Start(t)
	r.proc.Ready()
	r.raise(crashUID, "hourly-after", "JobEnded")
	eventually(t, "the closing EventsNotLogged to be raised", func() bool { return len(r.inner.byReason(ReasonEventsNotLogged)) == 5 })
	r.settled()
	closing := r.inner.byReason(ReasonEventsNotLogged)[4]
	if !strings.Contains(closing.note, "takes events again") || !strings.Contains(closing.note, "from 2026-11-17T12:01:41Z") {
		t.Errorf("the closing note is %q, want it to say the log takes events again, and since when", closing.note)
	}
	var inLog int
	for _, m := range r.read() {
		_, d, _ := Decode(m.Payload)
		if d.Reason == ReasonEventsNotLogged && strings.Contains(d.Note, "takes events again") {
			inLog++
		}
	}
	if inLog != 1 {
		t.Errorf("the log holds %d closing EventsNotLogged, want 1: it shows the span it lacks", inLog)
	}
}

// An EventsNotLogged that is itself lost counts, and never raises another: HBEP-8's absence proof, with
// its four conditions. The first EventsNotLogged is raised at the first loss, the log being down; the
// clock then moves more than an hour at once, so that its own retries run out at a time when the gate,
// were it applied, would be due; the counter of the raised events must not have moved.
func TestAnEventsNotLoggedThatIsLostRaisesNoSecondOne(t *testing.T) {
	clk := &fakeClock{t: base}
	ctl := ControllerProvider("p1")
	r := newRig(t, ctl, true, bigCaps, func(o *PublisherOptions) { o.Now = clk.now }, leaseFor("provider-p1"))
	key := Key{ctl, ReasonEventsNotLogged, ActionDegraded, provider.EventKindWarning}
	r.counter.Zero(ctl, []Series{{Reason: ReasonEventsNotLogged, Action: ActionDegraded, Type: provider.EventKindWarning}})
	rt := r.route(ctl)

	r.raise(crashUID, "recursion-1", "JobStarted")
	clk.set(base.Add(101 * time.Second))
	eventually(t, "the first loss to be handled, with its EventsNotLogged handed over", func() bool {
		return rt.handled.Load() == 1 && len(r.inner.byReason(ReasonEventsNotLogged)) == 1
	})
	first := scrapeRead(t, r.counter, key)
	change := time.Now()
	// The EventsNotLogged just raised is queued at 101 s with 100 s of retries. One step takes the clock
	// past them and past the hour since the raise.
	clk.set(base.Add(101*time.Second + time.Hour + 10*time.Second))
	passStart := time.Now()
	eventually(t, "the EventsNotLogged's own loss to be handled", func() bool {
		return r.unlogged(ctl) == 2 && rt.handled.Load() == 2 && r.pub.Pending() == 0
	})
	observed := time.Now()
	second := scrapeRead(t, r.counter, key)
	a := Absence{
		Key: key, First: first, Second: second, Change: change,
		Barriers: []Barrier{{What: "the publisher's handling of the EventsNotLogged's own loss: handled reached 2, and nothing is pending", PassStart: passStart, Observed: observed, Due: change}},
		Quiet:    "EventsNotLogged has one source here, the loss gate of provider-p1, and the one event lost is its own",
	}
	if err := a.Require(); err != nil {
		t.Fatal(err)
	}
	if got := len(r.inner.byReason(ReasonEventsNotLogged)); got != 1 {
		t.Errorf("%d EventsNotLogged handed to the recorder, want only the first", got)
	}
}

// scrapeRead is one read of the counter's /metrics text, with the process collector, for the series.
func scrapeRead(t *testing.T, c *Counter, k Key) Read {
	t.Helper()
	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(c, collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	sent := time.Now()
	fams, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	enc := expfmt.NewEncoder(&buf, expfmt.NewFormat(expfmt.TypeTextPlain))
	for _, f := range fams {
		if err := enc.Encode(f); err != nil {
			t.Fatal(err)
		}
	}
	rd, err := ParseRead(buf.Bytes(), k)
	if err != nil {
		t.Fatal(err)
	}
	rd.Sent, rd.Answered = sent, time.Now()
	return rd
}
