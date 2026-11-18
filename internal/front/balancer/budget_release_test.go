package balancer

// Release of a charge (HBEP-5, The count in tokens: each charge is released exactly once, with its reservation), after
// release_test.go: every path that ends a reservation gives N or u back once, a second call changes nothing, and neither
// count is ever below the sum of the charges still open. Every expected N and u is typed by hand.

import (
	"context"
	"errors"
	"testing"
)

// wantCharged checks member m's N and u against numbers worked out by hand, and that neither is negative.
func wantCharged(t *testing.T, g *Group, m string, wantN, wantU int64, when string) {
	t.Helper()
	n, u, ok := g.Charged(m)
	if !ok || n != wantN || u != wantU {
		t.Fatalf("%s: %s holds N = %d, u = %d (member %v), want N = %d, u = %d", when, m, n, u, ok, wantN, wantU)
	}
	if n < 0 || u < 0 {
		t.Fatalf("%s: %s holds a negative count, N = %d, u = %d", when, m, n, u)
	}
}

// budgeted is a state with m's cap 4 in force and its B 100, and o with neither (no cap, no B).
func budgeted(members []string, eligible ...string) State {
	s := stateOf(members, noBound, 0)
	s.Caps = map[string]int{"m": 4}
	s.CapValues = map[string]int{"m": 4}
	s.Budgets = map[string]int64{"m": 100}
	s.Eligible = map[string]bool{}
	for _, m := range eligible {
		s.Eligible[m] = true
	}
	return s
}

// A completion: each reservation gives back its own part, and a second release of the same one gives back nothing.
func TestACompletionReleasesItsChargeOnceAndAnotherReleaseChangesNothing(t *testing.T) {
	g := New()
	g.Update(budgeted([]string{"m"}, "m"))
	pick := func(c Charge) *Reservation {
		t.Helper()
		r, err := g.TryPickFor("k", nil, c)
		if err != nil {
			t.Fatalf("TryPickFor(%+v): %v", c, err)
		}
		return r
	}
	// By hand against cap 4, B 100: w30 first (4*30 = 120 <= 400), then w20 (4*(30+20) = 200), then an unnamed one
	// (4*50 + 1*100 = 300 <= 400): N = 50, u = 1.
	w30, w20, un := pick(workload(30)), pick(workload(20)), pick(noWorkload)
	wantCharged(t, g, "m", 50, 1, "three in flight")
	w30.Release()
	wantCharged(t, g, "m", 20, 1, "w30 released")
	w30.Release() // a second release of the same reservation
	wantCharged(t, g, "m", 20, 1, "w30 released twice")
	un.Release()
	un.Release()
	wantCharged(t, g, "m", 20, 0, "the unnamed one released twice")
	w20.Release()
	w20.Release()
	wantCharged(t, g, "m", 0, 0, "everything released")
	wantCounts(t, g, 0, "everything released")
}

// A leave releases the member's charges with its counts, a completion that comes after changes nothing, and a member of the
// same name that joins again starts at zero.
func TestALeaveReleasesTheChargesAndALateCompletionChangesNothing(t *testing.T) {
	g := New()
	st := budgeted([]string{"m", "o"}, "m", "o")
	st.Budgets["o"], st.Caps["o"], st.CapValues["o"] = 1000, 10, 10
	g.Update(st)
	onlyM := func(c Charge) *Reservation {
		t.Helper()
		r, err := g.TryPickFor("k", onlyMember("m"), c)
		if err != nil || r.Member != "m" {
			t.Fatalf("on m: %v, %v", r, err)
		}
		return r
	}
	w30, un := onlyM(workload(30)), onlyM(noWorkload)
	ro, err := g.TryPickFor("k", onlyMember("o"), workload(5))
	if err != nil {
		t.Fatal(err)
	}
	wantCharged(t, g, "m", 30, 1, "before the leave")
	wantCharged(t, g, "o", 5, 0, "before the leave")

	if released := g.Update(budgeted([]string{"o"}, "o")); released != 2 {
		t.Fatalf("the leave released %d reservations, want the 2 open on m", released)
	}
	if _, _, ok := g.Charged("m"); ok {
		t.Fatal("m left but still reports a count")
	}
	wantCharged(t, g, "o", 5, 0, "o after m left")
	w30.Release() // the completions that come after the leave
	w30.Release()
	un.Release()
	wantCharged(t, g, "o", 5, 0, "o after m's late completions")

	g.Update(budgeted([]string{"m", "o"}, "m", "o")) // m joins again
	wantCharged(t, g, "m", 0, 0, "m back, a new member")
	w30.Release()
	un.Release()
	wantCharged(t, g, "m", 0, 0, "m back, after the old completions again")
	ro.Release()
	wantCharged(t, g, "o", 0, 0, "o's own completion")
	wantCounts(t, g, 0, "at the end")
}

// A skip keeps N and u as it keeps the count. When the member is eligible again, its condition is evaluated on them: one token
// either side of the edge, worked by hand, shows they were not reset.
func TestASkipKeepsTheChargesAndTheMemberIsHeldToThemWhenEligibleAgain(t *testing.T) {
	g := New()
	g.Update(budgeted([]string{"m", "o"}, "m", "o"))
	for _, c := range []Charge{workload(30), noWorkload} {
		if r, err := g.TryPickFor("k", onlyMember("m"), c); err != nil || r.Member != "m" {
			t.Fatalf("loading m: %v, %v", r, err)
		}
	}
	wantCharged(t, g, "m", 30, 1, "before the skip")

	g.Update(budgeted([]string{"m", "o"}, "o")) // m skipped: still a member, not eligible
	wantCharged(t, g, "m", 30, 1, "m skipped")
	if r, err := g.TryPickFor("k", onlyMember("m"), workload(1)); !errors.Is(err, ErrNoEligible) || r != nil {
		t.Fatalf("a request that may use only the skipped member got %v, %v, want ErrNoEligible", r, err)
	}

	g.Update(budgeted([]string{"m", "o"}, "m", "o")) // m eligible again
	wantCharged(t, g, "m", 30, 1, "m eligible again")
	// cap 4, B 100, N = 30, u = 1, count 2 < 4: a Workload request needs 4*(30+c) + 1*100 <= 400, so c <= 45. With N and u
	// reset to 0 it would take c <= 100, and c = 46 would be admitted.
	if r, err := g.TryPickFor("k", onlyMember("m"), workload(46)); !errors.Is(err, ErrAllAtCap) || r != nil {
		t.Fatalf("c = 46 on m: got %v, %v, want ErrAllAtCap (4*76 + 100 = 404 > 400)", r, err)
	}
	if r, err := g.TryPickFor("k", onlyMember("m"), workload(45)); err != nil || r.Member != "m" {
		t.Fatalf("c = 45 on m: got %v, %v, want it admitted (4*75 + 100 = 400 <= 400)", r, err)
	}
	wantCharged(t, g, "m", 75, 1, "after c = 45")
}

// A retry: the front releases the first reservation, then chooses again, with the same charge, another member. The first
// charge goes back once and the second member carries its own.
func TestARetryReleasesTheFirstChargeOnceAndReservesItsOwnOnTheNextMember(t *testing.T) {
	g := New()
	st := budgeted([]string{"m", "o"}, "m", "o")
	st.Budgets["o"], st.Caps["o"], st.CapValues["o"] = 100, 4, 4
	g.Update(st)
	first, err := g.TryPickFor("k", onlyMember("m"), workload(40))
	if err != nil || first.Member != "m" {
		t.Fatalf("first: %v, %v", first, err)
	}
	wantCharged(t, g, "m", 40, 0, "the first attempt")
	first.Release() // the front's t.release() before it chooses again
	wantCharged(t, g, "m", 0, 0, "the first charge released")
	retry, err := g.TryPickFor("k", func(m string, _ Attrs) bool { return m != "m" }, workload(40)) // m is excluded: the request was written to it
	if err != nil || retry.Member != "o" {
		t.Fatalf("retry: %v, %v, want a reservation on o", retry, err)
	}
	wantCharged(t, g, "o", 40, 0, "the retry's own charge")
	wantCharged(t, g, "m", 0, 0, "m after the retry")
	first.Release() // the front's deferred release of the first target runs again at the end of the request
	first.Release()
	wantCharged(t, g, "m", 0, 0, "the first released again")
	wantCharged(t, g, "o", 40, 0, "o untouched by the first one's release")
	retry.Release()
	wantCharged(t, g, "o", 0, 0, "the retry released")
	wantCounts(t, g, 0, "at the end")
}

// A waiter that is placed after a completion carries its own charge: N goes to the typed value and back, never negative. The
// waiter is a Workload request that the member's budget refuses while an unnamed one would be admitted (cap 4, B 100, u = 2:
// c = 60 needs 4*60 + 2*100 = 440 <= 400, no; an unnamed request needs 4*0 + 3*100 = 300 <= 400, yes), so a waiter re-picked
// as an unnamed request is placed too early and is counted as u instead of N.
func TestAWaiterIsPlacedByItsOwnChargeAfterACompletion(t *testing.T) {
	g := New()
	state := budgeted([]string{"m"}, "m")
	g.Update(state)
	var holds []*Reservation
	for range 2 {
		r, err := g.TryPickFor("k", nil, noWorkload)
		if err != nil {
			t.Fatal(err)
		}
		holds = append(holds, r)
	}
	wantCharged(t, g, "m", 0, 2, "two unnamed requests in flight")

	out := make(chan result, 1)
	go func() {
		r, err := g.PickOrWaitFor(context.Background(), "k", nil, workload(60))
		out <- result{id: 1, r: r, err: err}
	}()
	waitForWaiting(t, g, 1)
	wantCharged(t, g, "m", 0, 2, "the Workload request waits and reserves nothing")

	// An update that frees nothing places the waiters it can: the Workload request still does not fit.
	g.Update(state)
	if g.Waiting() != 1 {
		t.Fatalf("%d waiting after an update that frees nothing, want the Workload request still waiting", g.Waiting())
	}
	wantCharged(t, g, "m", 0, 2, "after an update that frees nothing")

	holds[0].Release() // u = 1: 4*60 + 1*100 = 340 <= 400
	res := recv(t, out)
	if res.err != nil || res.r.Member != "m" {
		t.Fatalf("the waiter got %v, %v, want a reservation on m", res.r, res.err)
	}
	wantCharged(t, g, "m", 60, 1, "the waiter placed")
	if g.Waiting() != 0 {
		t.Fatalf("%d still waiting", g.Waiting())
	}
	res.r.Release()
	wantCharged(t, g, "m", 0, 1, "the waiter's charge released")
	res.r.Release()
	holds[1].Release()
	wantCharged(t, g, "m", 0, 0, "everything released")
	wantCounts(t, g, 0, "at the end")
}

// A waiter that gives up just as it is placed hands its reservation back, and the charge with it.
func TestAWaiterPlacedAsItGivesUpReleasesItsCharge(t *testing.T) {
	g := New()
	g.Update(budgeted([]string{"m"}, "m"))
	hold, err := g.TryPickFor("k", nil, workload(80)) // N = 80: a Workload request of 30 needs 4*110 = 440 <= 400, no
	if err != nil {
		t.Fatal(err)
	}
	contended := make(chan struct{}, 1)
	g.onContended = func() {
		select {
		case contended <- struct{}{}:
		default:
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := make(chan result, 1)
	go func() {
		r, err := g.PickOrWaitFor(ctx, "k", nil, workload(30))
		out <- result{id: 1, r: r, err: err}
	}()
	waitForWaiting(t, g, 1)

	g.mu.Lock() // hold the lock so the waiter, once it sees its context end, finds it taken
	cancel()
	select {
	case <-contended:
	case res := <-out:
		g.mu.Unlock()
		t.Fatalf("the waiter returned %v, %v before it could be placed", res.r, res.err)
	}
	// Place it by hand under the lock, as a completion would: the waiter is placed before it can remove itself.
	g.releaseLocked(hold)
	g.placeWaitersLocked()
	if len(g.queue) != 0 {
		g.mu.Unlock()
		t.Fatalf("%d still queued after the completion", len(g.queue))
	}
	g.mu.Unlock()

	res := recv(t, out)
	if !errors.Is(res.err, context.Canceled) || res.r != nil {
		t.Fatalf("the waiter returned %v, %v, want the context's error and no reservation", res.r, res.err)
	}
	wantCharged(t, g, "m", 0, 0, "the placed reservation was handed back")
	wantCounts(t, g, 0, "at the end")
}
