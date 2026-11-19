package front

import (
	"testing"
)

// Route order (HBEP-5, Workloads, Route order; HBEP-764's PR 5). Every expected number is typed by hand from the rule it names, and each request is held
// where it went (our engines, or the LLMLab double), so where a request went is read from the doubles' counts and the response's attribution, never from
// the front's own bookkeeping.

// ManagedFirst with managedSessions 2: the first two requests go to the managed target and are held there, and the third goes to our Instance a-0 and is
// answered by it, typed from the response's attribution. Cutting one request on the managed target frees one unit of the count, and the next goes to the
// managed target again. A request is cut by its client, which completes it on its target.
func TestManagedFirstTakesTheFirstTwoOnTheManagedTargetAndTheThirdGoesToOurInstance(t *testing.T) {
	f := newWrFix(t, wrOpts{managedFirst: true, managedSessions: 2, stallManaged: true})
	_, cut1 := f.start(f.alpha(), "mf-1")
	f.settled(1, 0, 0)
	f.start(f.alpha(), "mf-2")
	f.settled(2, 0, 0)
	if got := f.managedCount(); got != 2 {
		t.Fatalf("two requests are held on the managed target: the Workload's count is %d, want 2", got)
	}
	third, _ := f.start(f.alpha(), "mf-3")
	resp, body := third.wait(t)
	if resp.StatusCode != 200 || scInstanceOf(resp) != "ns/a-0" || scEmitterOf(resp) != "instance" {
		t.Fatalf("the third request: status %d, emitter %q, instance %q, want 200 from instance ns/a-0 (the bound is 2): %s", resp.StatusCode, scEmitterOf(resp), scInstanceOf(resp), body)
	}
	if got := f.arrivedManaged(); got != 2 {
		t.Errorf("the LLMLab double has had %d requests, want 2: the third went to ours", got)
	}

	cut1() // the first request on the managed target is cut by its client: one unit of the count is released
	errWaitFor(t, "the count to fall to 1", func() bool { return f.managedCount() == 1 })
	f.start(f.alpha(), "mf-4")
	f.settled(3, 0, 0) // the fourth reaches the managed target: the count was below the bound again
	if got := f.managedCount(); got != 2 {
		t.Errorf("the Workload's count is %d after the fourth, want 2", got)
	}
}

// OwnFirst with managedSessions 1: ours take requests to their cap (instanceCap 1, held), the managed target takes one overflow, and the next waits in the
// Workload's FIFO rather than going to LLMLab past its bound.
func TestOwnFirstBoundsTheOverflowToManagedSessions(t *testing.T) {
	f := newWrFix(t, wrOpts{cap: 1, holdOurs: true, stallManaged: true, managedSessions: 1})
	f.start(f.alpha(), "of-1")
	f.settled(0, 1, 0) // a-0 holds its one request
	f.start(f.alpha(), "of-2")
	f.settled(1, 1, 0) // the overflow: the managed target takes one
	f.start(f.alpha(), "of-3")
	f.settled(1, 1, 1) // the next waits: ours are at their cap and the managed target is at the bound
	if got := f.managedCount(); got != 1 {
		t.Errorf("the Workload's count is %d, want 1", got)
	}
	if ws := f.wlState(); ws == nil || ws.waiting.Load() != 1 {
		t.Errorf("the request waits in the Workload's FIFO: want 1 waiting there")
	}
}

// OwnFirst with no managedSessions bounds nothing: the overflow goes to LLMLab as before, however many.
func TestOwnFirstWithNoManagedSessionsSendsEveryOverflowToTheManagedTarget(t *testing.T) {
	f := newWrFix(t, wrOpts{cap: 1, holdOurs: true, stallManaged: true})
	f.start(f.alpha(), "ou-1")
	f.settled(0, 1, 0)
	for i, want := range []int{1, 2, 3} {
		f.start(f.alpha(), "ou-over-"+string(rune('a'+i)))
		f.settled(want, 1, 0)
	}
}

// An unnamed request is unchanged: it takes our Instance first, then overflows to the managed target with no bound and no count, and never queues in a
// Workload's FIFO. The Workload's managedSessions 1 does not touch it.
func TestAnUnnamedRequestOverflowsToTheManagedTargetWithNoBoundAndNoCount(t *testing.T) {
	f := newWrFix(t, wrOpts{cap: 1, holdOurs: true, stallManaged: true, managedSessions: 1})
	f.start(scModel, "un-1")
	f.settled(0, 1, 0)
	for i, want := range []int{1, 2, 3} {
		f.start(scModel, "un-over-"+string(rune('a'+i)))
		f.settled(want, 1, 0)
	}
	if got := f.managedCount(); got != 0 {
		t.Errorf("the Workload's count is %d after three unnamed requests, want 0", got)
	}
	if f.wlState() != nil {
		t.Errorf("an unnamed request made a Workload state")
	}
}

// ManagedFirst with no eligible managed target (none attached) takes our Instance, as for a Workload with no managed route.
func TestManagedFirstWithNoManagedTargetTakesOurInstance(t *testing.T) {
	f := newWrFix(t, wrOpts{managedFirst: true, managedSessions: 2, noManaged: true})
	c, _ := f.start(f.alpha(), "mn-1")
	resp, body := c.wait(t)
	if resp.StatusCode != 200 || scInstanceOf(resp) != "ns/a-0" {
		t.Fatalf("status %d from %q, want 200 from ns/a-0: %s", resp.StatusCode, scInstanceOf(resp), body)
	}
}
