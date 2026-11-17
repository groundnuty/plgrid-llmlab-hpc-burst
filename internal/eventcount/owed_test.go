package eventcount

// The runs of losses that no stored closing reports yet are kept as one aggregate (the delta review of #697,
// 17 Nov 2026): a controller that can never raise a closing, one run with no leader election and so no Lease,
// holds a fixed amount of state, however many times the log fails and returns.

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// owedSpans is how many runs' spans the route holds as owed, or inside an outstanding closing job.
func (r *route) owedSpans() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	if r.owed.count > 0 {
		n++
	}
	if r.closing != nil {
		n++
	}
	return n
}

// gateIdle is true when no raise job of the route is waiting or running.
func (r *route) gateIdle() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.loss == nil && r.closing == nil
}

// Many runs of losses, each ended by a publish the log takes, with no Lease to raise a closing on, leave the
// owed runs as one span saying the earliest loss and the summed count. When a Lease appears, the next publish
// raises ONE closing that covers them all.
func TestOwedRunsStayOneAggregateWhileNoClosingCanBeRaised(t *testing.T) {
	ctl := ControllerProvider("p1")
	r := newRig(t, ctl, false, bigCaps, nil, nil) // no Lease: a controller run with --leader-elect=false
	rt := r.route(ctl)
	const cycles = 200
	var first time.Time
	for i := 0; i < cycles; i++ {
		rt.lost(lostBudget, "JobStarted", fmt.Sprintf("owed-%d", i), nil, nil) // a run starts
		if i == 0 {
			rt.mu.Lock()
			first = rt.runStart
			rt.mu.Unlock()
		}
		within(t, 20*time.Second, "the loss job to finish", rt.gateIdle)
		rt.logged(&item{r: rt, id: "ok"}) // the log takes an event: the run ends, and a closing is owed
		within(t, 20*time.Second, "the closing job to finish", rt.gateIdle)
		if n := rt.owedSpans(); n > 1 {
			t.Fatalf("after %d runs the route holds %d owed spans, want one aggregate", i+1, n)
		}
	}
	rt.mu.Lock()
	count, since := rt.owed.count, rt.owed.since
	rt.mu.Unlock()
	if count != cycles || !since.Equal(first) {
		t.Errorf("the owed aggregate is %d events since %v, want %d since %v: the earliest loss and the summed count", count, since, cycles, first)
	}
	if got := len(r.inner.byReason(ReasonEventsNotLogged)); got != 0 {
		t.Fatalf("%d EventsNotLogged raised with no Lease", got)
	}
	// A Lease appears (the manager is built again with one): the next publish the log takes raises one closing.
	rt.mu.Lock()
	rt.spec.lease = leaseFor("provider-p1")
	rt.mu.Unlock()
	rt.logged(&item{r: rt, id: "ok"})
	want := fmt.Sprintf("%d events were not logged from %s", cycles, first.UTC().Format(time.RFC3339))
	within(t, 20*time.Second, "the one closing to be raised", func() bool { return len(r.inner.byReason(ReasonEventsNotLogged)) >= 1 })
	r.settled()
	within(t, 20*time.Second, "the closing to be stored", func() bool { return rt.owedSpans() == 0 })
	got := r.inner.byReason(ReasonEventsNotLogged)
	if len(got) != 1 || !strings.Contains(got[0].note, want) {
		t.Errorf("EventsNotLogged raised %d times, notes %v, want one saying %q", len(got), notes(got), want)
	}
}

// notes are the notes of the recorded calls.
func notes(cs []recorded) []string {
	var out []string
	for _, c := range cs {
		out = append(out, c.note)
	}
	return out
}
