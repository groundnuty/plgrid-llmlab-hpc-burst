package balancer

// A skip keeps the count (HBEP-5, Test plan: A skip keeps the count). A pause,
// AgentUnreachable, a return to verifying and a lost transport all reach the
// balancer as one thing: Eligible false for a member that stays in Members.
// The member keeps its place and its in-flight count, each of its requests is
// released when it completes, and when it is eligible again its count equals
// its open reservations, so the cap holds. A count reset to zero at the skip
// would let it take more than its cap; the last test shows that reset (what a
// leave does) so that the assertions of the first two can be seen to fail.
//
// Full membership: c = 1.25, caps of 2, three members sharing the key "k",
// walk H, S1, S2 (from the fork). Four requests first, by hand, with the bound
// = 1.25 * (T + 1) / 3 (the cap test, TestFullMembershipSpillsThenWaitsAndAReleasePlacesIt,
// works the same four):
//
//	r1: H (1).  r2: T = 1, bound 0.83, H holds 1: S1 (1).
//	r3: T = 2, bound 1.25: H (2).  r4: T = 3, bound 1.67, H holds 2: S1 (2).
//
// Loads H 2, S1 2, S2 0; T = 4.

import "testing"

type fixture struct {
	g          *Group
	h, s1, s2  string
	onH, onS1  []*Reservation
	skipH, all State
}

func fourRequests(t *testing.T) fixture {
	t.Helper()
	w := forkWalk(t, three, "k")
	fx := fixture{g: newGroupOf(three, tight, 2), h: w[0], s1: w[1], s2: w[2]}
	rs := runSteps(t, fx.g, forkAcquirer(forkOf(three), tight), "k", []step{
		{fx.h, false, fx.h},
		{fx.s1, false, fx.s1},
		{fx.h, false, fx.h},
		{fx.s1, false, fx.s1},
	})
	fx.onH, fx.onS1 = []*Reservation{rs[0], rs[2]}, []*Reservation{rs[1], rs[3]}
	fx.all = stateOf(three, tight, 2)
	fx.skipH = stateOf(three, tight, 2)
	fx.skipH.Eligible[fx.h] = false
	wantInFlight(t, fx.g, fx.h, 2, "before the skip")
	wantInFlight(t, fx.g, fx.s1, 2, "before the skip")
	wantInFlight(t, fx.g, fx.s2, 0, "before the skip")
	return fx
}

// H, at its cap with 2 open requests, is skipped. It takes nothing and keeps
// its count; it comes back with both requests still open and is still at its
// cap, so it takes nothing until one ends; then it takes exactly one.
//
//	H skipped, r5: KubeAI's choice (T = 4, bound 2.08, H holds 2) is H, which
//	  the request may not use; the walk of the eligible members, with T = 4 and
//	  n = 3, takes S1 (2 <= 2.08), at its cap; the next after S1 on the ring is
//	  S2 (0): S2 (1), a spill. H still holds 2.
//	H eligible, r6: T = 5, bound 2.5, H holds 2 <= 2.5: H chosen, at its cap
//	  (a reset to 0 would have taken it); S1 full; S2 holds 1: S2 (2), a spill.
//	H's r1 ends: H holds 1, T = 5, bound 2.5: r7 takes H (2), no spill.
func TestASkipKeepsTheCountAndTheCap(t *testing.T) {
	fx := fourRequests(t)
	g := fx.g
	if n := g.Update(fx.skipH); n != 0 {
		t.Fatalf("a skip released %d reservations, want none", n)
	}
	wantInFlight(t, g, fx.h, 2, "H skipped keeps its count")
	wantCounts(t, g, 4, "H skipped")

	r5 := mustPick(t, g, "k")
	if r5.Member != fx.s2 || !r5.Spilled {
		t.Fatalf("r5 with H skipped: %s (spilled %v), want S2 %s (spilled)", r5.Member, r5.Spilled, fx.s2)
	}
	wantInFlight(t, g, fx.h, 2, "H skipped, after r5")

	g.Update(fx.all) // H is eligible again, both its requests still open
	wantInFlight(t, g, fx.h, 2, "H eligible again: its count is its 2 open requests (a reset at the skip reads 0)")
	r6 := mustPick(t, g, "k")
	if r6.Member != fx.s2 || !r6.Spilled {
		t.Fatalf("r6 with H back at its cap: %s (spilled %v), want S2 %s (spilled): H must get nothing", r6.Member, r6.Spilled, fx.s2)
	}
	wantInFlight(t, g, fx.h, 2, "H at its cap takes nothing")
	wantCounts(t, g, 6, "after r6")

	fx.onH[0].Release()
	r7 := mustPick(t, g, "k")
	if r7.Member != fx.h || r7.Spilled {
		t.Fatalf("r7 after one of H's requests ended: %s (spilled %v), want H %s", r7.Member, r7.Spilled, fx.h)
	}
	wantInFlight(t, g, fx.h, 2, "H back at its cap")
	wantCounts(t, g, 6, "at the end")
}

// One of H's requests ends while H is skipped. H comes back with 1 open
// request, not 2 and not 0, and takes exactly one more.
//
//	H skipped, r1 ends: H holds 1, T = 3.
//	H eligible, r5: T = 3, bound 1.67, H holds 1: H (2), no spill.
//	r6: T = 4, bound 2.08, H holds 2: H chosen, at its cap; S1 full; S2 (1): a spill.
func TestASkipWhoseRequestsPartlyEndComesBackWithTheOpenCount(t *testing.T) {
	fx := fourRequests(t)
	g := fx.g
	g.Update(fx.skipH)
	fx.onH[0].Release()
	wantInFlight(t, g, fx.h, 1, "a request ends while H is skipped")
	wantCounts(t, g, 3, "a request ends while H is skipped")

	g.Update(fx.all)
	wantInFlight(t, g, fx.h, 1, "H eligible again")
	r5 := mustPick(t, g, "k")
	if r5.Member != fx.h || r5.Spilled {
		t.Fatalf("r5: %s (spilled %v), want H %s below its cap", r5.Member, r5.Spilled, fx.h)
	}
	r6 := mustPick(t, g, "k")
	if r6.Member != fx.s2 || !r6.Spilled {
		t.Fatalf("r6: %s (spilled %v), want S2 %s: H holds 2, its cap", r6.Member, r6.Spilled, fx.s2)
	}
	wantInFlight(t, g, fx.h, 2, "H never exceeds its cap")
}

// The negative control: a count reset at the skip. The group does exactly
// this at a leave, so H leaves and returns instead of being skipped. Its two
// open requests are released and, on return, H reads 0 while two of its
// streams (the test still holds them) would be running; by hand r5 then finds
// T = 2, bound 1.25, H holding 0: H, with the cap of 2 already half used by
// streams the group no longer counts. The two tests above assert 2 and a
// request elsewhere; this run gives 0 and a request on H, so they would fail
// under a reset.
func TestACountResetAtTheSkipWouldLetTheMemberExceedItsCap(t *testing.T) {
	fx := fourRequests(t)
	g := fx.g
	if n := g.Update(stateOf(without(three, fx.h), tight, 2)); n != 2 {
		t.Fatalf("H left and the group released %d reservations, want 2", n)
	}
	g.Update(fx.all)
	wantInFlight(t, g, fx.h, 0, "H returned after a leave")
	r5 := mustPick(t, g, "k")
	if r5.Member != fx.h {
		t.Fatalf("r5 after the reset: %s, want H %s (count 0 against 2 running streams)", r5.Member, fx.h)
	}
	// the requests the leave released complete later and change nothing
	for _, r := range fx.onH {
		r.Release()
	}
	wantInFlight(t, g, fx.h, 1, "the released requests completing")
}
