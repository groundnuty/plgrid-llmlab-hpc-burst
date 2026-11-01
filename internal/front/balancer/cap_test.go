package balancer

// Bound and cap (HBEP-5, Test plan: Bound and cap), the hand-worked case of
// S10c: three members and a cap of 2. Below full membership the loose bound
// and no caps are in force and nothing spills; at full membership the tight
// bound and the caps are, and a request over a cap spills to the next member
// on the ring; with every member full a request waits and a release places it.
//
// All three members share one key, "k", whose walk (home H, then S1, then S2)
// is taken from the fork alone. The plain fork group is run alongside as the
// control: it has no cap, so wherever the cap moves a request the plain group
// picks the member the cap refused.
//
// A spill (Reservation.Spilled) is a move by the cap. A move by the bound is
// not one: KubeAI's own walk made it.

import (
	"context"
	"errors"
	"testing"
)

// step is one request for a key: the member it must land on and whether the
// cap must have moved it (both by hand), and, when set, what the plain fork
// group picks for the same request on its own trajectory (also by hand).
type step struct {
	member  string
	spilled bool
	plain   string
}

// runSteps makes one request per step on g, keeping every reservation.
func runSteps(t *testing.T, g *Group, plain acquirer, key string, steps []step) []*Reservation {
	t.Helper()
	var out []*Reservation
	for i, s := range steps {
		if s.plain != "" {
			addr, _, err := plain(key)
			if err != nil {
				t.Fatal(err)
			}
			if addr != s.plain {
				t.Fatalf("r%d: the plain fork group picked %s, by hand %s", i+1, addr, s.plain)
			}
		}
		r := mustPick(t, g, key)
		if r.Member != s.member || r.Spilled != s.spilled {
			t.Fatalf("r%d: member %s (spilled %v), want %s (spilled %v)", i+1, r.Member, r.Spilled, s.member, s.spilled)
		}
		out = append(out, r)
	}
	return out
}

// Below full membership: c = 2.0, no caps. By hand, T the total before the
// request and bound = 2.0 * (T + 1) / 3:
//
//	r1: T = 0 passes: H (1).
//	r2: T = 1, bound 1.33, H holds 1: H (2).
//	r3: T = 2, bound 2.0, H holds 2 <= 2.0: H (3).
//	r4: T = 3, bound 2.67, H holds 3: no; S1 holds 0: S1 (1), moved by the bound.
//	r5: T = 4, bound 3.33, H holds 3: H (4).
//	r6: T = 5, bound 4.0, H holds 4 <= 4.0: H (5).
//
// H ends at 5, far above a cap of 2, which would have bound at r3 (next test).
func TestNoSpillBelowFullMembership(t *testing.T) {
	w := forkWalk(t, three, "k")
	h, s1 := w[0], w[1]
	g := newGroupOf(three, loose, 0)
	runSteps(t, g, forkAcquirer(forkOf(three), loose), "k", []step{
		{h, false, h},
		{h, false, h},
		{h, false, h},
		{s1, false, s1},
		{h, false, h},
		{h, false, h},
	})
	wantInFlight(t, g, h, 5, "the home")
	wantInFlight(t, g, s1, 1, "the first member after the home")
	wantCounts(t, g, 6, "six requests")
}

// The control for the test above: the same bound with caps of 2 in force. r3
// finds H at 2 <= 2.0, so KubeAI chooses H (the plain group does), but H holds
// the cap, so the request spills to the next member on the ring, S1.
func TestTheCapIsWhatSpillsInTheSameSequence(t *testing.T) {
	w := forkWalk(t, three, "k")
	h, s1 := w[0], w[1]
	g := newGroupOf(three, loose, 2)
	runSteps(t, g, forkAcquirer(forkOf(three), loose), "k", []step{
		{h, false, h},
		{h, false, h},
		{s1, true, h},
	})
	wantInFlight(t, g, h, 2, "the home at its cap")
	wantInFlight(t, g, s1, 1, "the spill target")
}

// Full membership: c = 1.25, caps of 2. By hand, bound = 1.25 * (T + 1) / 3:
//
//	r1: T = 0 passes: H (1).
//	r2: T = 1, bound 0.83, H holds 1: no; S1 holds 0: S1 (1). Moved by the
//	    bound, not a spill.
//	r3: T = 2, bound 1.25, H holds 1: H (2).
//	r4: T = 3, bound 1.67, H holds 2: no; S1 holds 1: S1 (2).
//	r5: T = 4, bound 2.08, H holds 2 <= 2.08: KubeAI chooses H (the plain
//	    group does: its third), but H holds the cap. The next member on the
//	    ring is S1, full; then S2, which holds 0: S2 (1). The one spill.
//	r6: T = 5, bound 2.5, H holds 2: H chosen, at its cap; S1 full; S2 holds
//	    1: S2 (2). Every member now holds 2 and the total is 6.
//	r7: T = 6, bound 2.92, H chosen, every member at its cap: it waits.
//	H's r1 ends: H holds 1, T = 5, bound 2.5: the waiting request takes H
//	    (1 < 2, no spill) and every member is full again.
//
// Without the caps the same r5 would be H's third, which is what the plain
// group does: the run proves the cap bound.
func TestFullMembershipSpillsThenWaitsAndAReleasePlacesIt(t *testing.T) {
	w := forkWalk(t, three, "k")
	h, s1, s2 := w[0], w[1], w[2]
	g := newGroupOf(three, tight, 2)
	rs := runSteps(t, g, forkAcquirer(forkOf(three), tight), "k", []step{
		{h, false, h},
		{s1, false, s1},
		{h, false, h},
		{s1, false, s1},
		{s2, true, h},
	})
	spills := 0
	for _, r := range rs {
		if r.Spilled {
			spills++
		}
	}
	if spills != 1 {
		t.Fatalf("%d spills in five requests, want exactly 1 (r5); a run where the cap never binds is no pass", spills)
	}
	wantInFlight(t, g, h, 2, "H after r5")
	wantInFlight(t, g, s1, 2, "S1 after r5")
	wantInFlight(t, g, s2, 1, "S2 after r5")

	runSteps(t, g, nil, "k", []step{{s2, true, ""}}) // r6
	wantInFlight(t, g, s2, 2, "S2 after r6")
	wantCounts(t, g, 6, "every member full")

	out := make(chan result, 1)
	startWaiter(context.Background(), g, 7, "k", nil, out) // r7
	waitForWaiting(t, g, 1)
	wantCounts(t, g, 6, "r7 waits and reserves nothing")

	rs[0].Release() // H's r1 ends
	res := recv(t, out)
	if res.err != nil || res.r.Member != h || res.r.Spilled {
		t.Fatalf("r7 placed on %v (spilled %v, err %v), want H %s without a spill", res.r, res.r != nil && res.r.Spilled, res.err, h)
	}
	if g.Waiting() != 0 {
		t.Fatalf("%d requests still waiting after the release", g.Waiting())
	}
	wantInFlight(t, g, h, 2, "H after r7")
	wantCounts(t, g, 6, "every member full again")
}

// When the cap returns with a member above it, nothing is cut, and the member
// gets no new request until it is below the cap. The bound is c = 3.0 here, so
// that the bound alone would keep choosing H: at 1.25 the bound would refuse H
// at load 3, a spill to S1 would come from the bound and the cap would never
// be tested. The plain group proves it: it picks H for all four requests.
//
// Phase one, c = 2.0 and no caps: r1, r2, r3 go to H (the first test above), H
// holds 3. Then the caps of 2 return with c = 3.0, bound = 3.0 * (T + 1) / 3 =
// T + 1:
//
//	r4: T = 3, bound 4, H holds 3 <= 4: H chosen, above its cap: S1 (1).
//	r5: T = 4, bound 5: H chosen, above the cap: S1 (2).
//	r6: T = 5, bound 6: H chosen; S1 full; S2 (1).
//	r7: T = 6, bound 7: H chosen; S1 full; S2 (2).
//	r8: T = 7, bound 8: H chosen; every member at or above the cap: refused.
//
// The plain group, with no cap, takes H each time: loads 3, 4, 5, 6 against
// bounds 4, 5, 6, 7. Two of H's requests end (3 -> 1): after the first H holds
// 2, still at the cap, and a request is refused; after the second H holds 1,
// below the cap, and the next request is H's.
func TestCapReturnsWithAMemberAboveIt(t *testing.T) {
	w := forkWalk(t, three, "k")
	h, s1, s2 := w[0], w[1], w[2]
	g := newGroupOf(three, loose, 0)
	f := forkOf(three)
	onH := runSteps(t, g, forkAcquirer(f, loose), "k", []step{{h, false, h}, {h, false, h}, {h, false, h}})
	wantInFlight(t, g, h, 3, "H above a cap of 2, before the cap returns")

	if released := g.Update(stateOf(three, 300, 2)); released != 0 {
		t.Fatalf("the cap's return released %d reservations, want none", released)
	}
	wantInFlight(t, g, h, 3, "H when the cap returns: nothing is cut")
	wantCounts(t, g, 3, "when the cap returns")

	runSteps(t, g, forkAcquirer(f, 300), "k", []step{
		{s1, true, h},
		{s1, true, h},
		{s2, true, h},
		{s2, true, h},
	})
	wantInFlight(t, g, h, 3, "H after r4 to r7: it gets no new request while above the cap")
	wantInFlight(t, g, s1, 2, "S1")
	wantInFlight(t, g, s2, 2, "S2")

	refuse := func(when string) {
		t.Helper()
		if r, err := g.TryPick("k", nil); !errors.Is(err, ErrAllAtCap) || r != nil {
			t.Fatalf("%s: got %v, %v, want ErrAllAtCap and no reservation", when, r, err)
		}
	}
	refuse("every member at or above its cap")
	wantCounts(t, g, 7, "after the refused request")

	onH[0].Release() // H holds 2: at the cap, not below it
	refuse("H at its cap")
	wantCounts(t, g, 6, "after the refused request")

	onH[1].Release() // H holds 1
	r := mustPick(t, g, "k")
	if r.Member != h || r.Spilled {
		t.Fatalf("with H below its cap the request went to %s (spilled %v), want H %s", r.Member, r.Spilled, h)
	}
	wantInFlight(t, g, h, 2, "H back at its cap")
	wantCounts(t, g, 6, "at the end")
}
