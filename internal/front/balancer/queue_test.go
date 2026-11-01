package balancer

// The FIFO (HBEP-5, Load bound and cap: the capped pick). With every eligible
// member at its cap a request waits at the front; a completion, a join, a
// member that is eligible again or the cap going off places the oldest one
// that fits, and a waiter whose context ends leaves the queue.
//
// Fixture: three members sharing the key "k" (walk H, S1, S2 from the fork),
// c = 3.0 and a cap of 1. By hand, bound = 3.0 * (T + 1) / 3 = T + 1:
//
//	r1: T = 0 passes: H (1).
//	r2: T = 1, bound 2, H holds 1: H chosen, at its cap; the next on the ring
//	    is S1 (0): S1 (1), a spill.
//	r3: T = 2, bound 3: H chosen, at its cap; S1 full; S2 (0): S2 (1), a spill.
//
// Every member holds 1; T = 3. A freed member is then the only one with room,
// so the waiter placed by its release lands on it.

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fullFixture struct {
	g         *Group
	byMember  map[string]*Reservation
	h, s1, s2 string
}

// state is the fixture's State for members: every member eligible, c = 3.0, a
// cap of capEach (0: none), H of provider "pa" and S1 and S2 of provider "pb".
func (fx fullFixture) state(members []string, capEach int) State {
	return withProviders(stateOf(members, 300, capEach), map[string]string{fx.h: "pa", fx.s1: "pb", fx.s2: "pb"})
}

func newFull(t *testing.T) fullFixture {
	t.Helper()
	w := forkWalk(t, three, "k")
	fx := fullFixture{g: New(), byMember: map[string]*Reservation{}, h: w[0], s1: w[1], s2: w[2]}
	fx.g.Update(fx.state(three, 1))
	rs := runSteps(t, fx.g, nil, "k", []step{
		{fx.h, false, ""},
		{fx.s1, true, ""},
		{fx.s2, true, ""},
	})
	for _, r := range rs {
		fx.byMember[r.Member] = r
	}
	wantCounts(t, fx.g, 3, "every member full")
	return fx
}

// Three waiters arrive in order; each release places the oldest, on the
// member that was freed.
func TestWaitersArePlacedOldestFirst(t *testing.T) {
	fx := newFull(t)
	out := make(chan result, 3)
	for id := 1; id <= 3; id++ {
		startWaiter(context.Background(), fx.g, id, "k", nil, out)
		waitForWaiting(t, fx.g, id)
	}
	wantCounts(t, fx.g, 3, "three waiting, nothing reserved for them")
	for i, freed := range []string{fx.h, fx.s1, fx.s2} {
		fx.byMember[freed].Release()
		res := recv(t, out)
		if res.id != i+1 || res.err != nil || res.r == nil || res.r.Member != freed {
			t.Fatalf("release %d (%s): waiter %d got %v, err %v; want waiter %d on %s", i+1, freed, res.id, res.r, res.err, i+1, freed)
		}
		if got := fx.g.Waiting(); got != 2-i {
			t.Fatalf("release %d: %d waiting, want %d", i+1, got, 2-i)
		}
		wantCounts(t, fx.g, 3, "after a waiter is placed")
	}
	for _, m := range three {
		wantInFlight(t, fx.g, m, 1, "at the end")
	}
}

// A waiter that does not fit keeps its place; a younger one whose filter
// admits a member with room is placed past it. The older waiter A may use only
// the members of provider "pa", which is H alone; the younger B may use any. S1 is freed: A still finds H full (T =
// 2, bound 3, H chosen, at its cap, no other member admitted), B takes S1 (H
// chosen, at its cap, the next on the ring with room is S1). Then H is freed
// and A takes it.
func TestAYoungerWaiterIsPlacedPastAnOlderOneThatCannotUseTheMember(t *testing.T) {
	fx := newFull(t)
	out := make(chan result, 2)
	startWaiter(context.Background(), fx.g, 1, "k", providerIs("pa"), out)
	waitForWaiting(t, fx.g, 1)
	startWaiter(context.Background(), fx.g, 2, "k", nil, out)
	waitForWaiting(t, fx.g, 2)

	fx.byMember[fx.s1].Release()
	res := recv(t, out)
	if res.id != 2 || res.err != nil || res.r.Member != fx.s1 {
		t.Fatalf("S1 freed: waiter %d got %v, err %v; want the younger waiter 2 on S1 %s", res.id, res.r, res.err, fx.s1)
	}
	if got := fx.g.Waiting(); got != 1 {
		t.Fatalf("%d waiting, want the older waiter alone", got)
	}

	fx.byMember[fx.h].Release()
	res = recv(t, out)
	if res.id != 1 || res.err != nil || res.r.Member != fx.h {
		t.Fatalf("H freed: waiter %d got %v, err %v; want the older waiter 1 on H %s", res.id, res.r, res.err, fx.h)
	}
	wantCounts(t, fx.g, 3, "both placed")
}

// A waiter whose context ends returns the context's error, leaves the queue
// and reserves nothing.
func TestAWaiterWhoseContextEndsLeavesTheQueue(t *testing.T) {
	fx := newFull(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := make(chan result, 1)
	startWaiter(ctx, fx.g, 1, "k", nil, out)
	waitForWaiting(t, fx.g, 1)
	cancel()
	res := recv(t, out)
	if !errors.Is(res.err, context.Canceled) || res.r != nil {
		t.Fatalf("cancelled waiter: %v, %v, want context.Canceled and no reservation", res.r, res.err)
	}
	if got := fx.g.Waiting(); got != 0 {
		t.Fatalf("%d waiting after the cancel, want 0", got)
	}
	wantCounts(t, fx.g, 3, "after the cancel")

	ctx2, cancel2 := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel2()
	r, err := fx.g.PickOrWait(ctx2, "k", nil)
	if !errors.Is(err, context.DeadlineExceeded) || r != nil {
		t.Fatalf("expired waiter: %v, %v, want context.DeadlineExceeded and no reservation", r, err)
	}
	if got := fx.g.Waiting(); got != 0 {
		t.Fatalf("%d waiting after the deadline, want 0", got)
	}
	wantCounts(t, fx.g, 3, "after the deadline")
}

// The race at the end of a wait, forced without timing: the test holds the
// group's lock, cancels the waiter's context and waits until the waiter has
// come for the lock (the onContended seam); room then appears and the queue is
// run, which places the waiter just as it gives up. The reservation placed for
// it must be handed back.
func TestAWaiterPlacedAsItGivesUpIsReleased(t *testing.T) {
	fx := newFull(t)
	g := fx.g
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := make(chan result, 1)
	startWaiter(ctx, g, 1, "k", nil, out)
	waitForWaiting(t, g, 1)

	// The seam is installed only now: the polling above can itself find the
	// lock held, and that must not count as the waiter coming for it. From
	// here the waiter is parked in its select and the test touches the group
	// only through the lock it holds itself.
	contended := make(chan struct{}, 1)
	g.onContended = func() { // never blocks: it runs on every contended lock
		select {
		case contended <- struct{}{}:
		default:
		}
	}
	g.mu.Lock()
	cancel()
	select {
	case <-contended:
	case <-time.After(10 * time.Second):
		g.mu.Unlock()
		t.Fatal("the waiter never came for the lock after its context ended")
	}
	g.releaseLocked(fx.byMember[fx.h])
	g.placeWaitersLocked()
	placed := len(g.queue) == 0
	g.mu.Unlock()

	res := recv(t, out)
	g.onContended = nil // the waiter has reported back: nothing reads the seam now
	if !placed {
		t.Fatal("the queue run did not place the waiter")
	}
	if !errors.Is(res.err, context.Canceled) || res.r != nil {
		t.Fatalf("waiter that gave up: %v, %v, want context.Canceled and no reservation", res.r, res.err)
	}
	if got := g.Waiting(); got != 0 {
		t.Fatalf("%d waiting at the end, want 0", got)
	}
	wantInFlight(t, g, fx.h, 0, "H: its request ended and the reservation placed for the leaver was handed back")
	wantCounts(t, g, 2, "S1 and S2 still hold their requests")
}

// A change of state places a waiter where it now fits.
//
//	The cap goes off (caps removed, c = 3.0): T = 3, bound 4, H holds 1: H, no spill.
//	A member joins (d, cap 1): only d has room, so the waiter lands on d
//	  whatever member the key's walk now starts with.
//	A member is eligible again: S1 was skipped and its request ended, so S1
//	  holds 0; the waiter found H and S2 full and S1 skipped; when S1 is
//	  eligible again it takes the waiter (H chosen, at its cap, the next on
//	  the ring with room is S1).
func TestUpdatePlacesWaiters(t *testing.T) {
	t.Run("the cap goes off", func(t *testing.T) {
		fx := newFull(t)
		out := make(chan result, 1)
		startWaiter(context.Background(), fx.g, 1, "k", nil, out)
		waitForWaiting(t, fx.g, 1)
		fx.g.Update(fx.state(three, 0))
		res := recv(t, out)
		if res.err != nil || res.r.Member != fx.h || res.r.Spilled {
			t.Fatalf("cap off: %v, err %v, want H %s without a spill", res.r, res.err, fx.h)
		}
		wantInFlight(t, fx.g, fx.h, 2, "H")
		wantCounts(t, fx.g, 4, "after the cap went off")
	})
	t.Run("a member joins", func(t *testing.T) {
		fx := newFull(t)
		out := make(chan result, 1)
		startWaiter(context.Background(), fx.g, 1, "k", nil, out)
		waitForWaiting(t, fx.g, 1)
		fx.g.Update(fx.state(four, 1))
		res := recv(t, out)
		if res.err != nil || res.r.Member != "d" {
			t.Fatalf("join: %v, err %v, want the joiner d", res.r, res.err)
		}
		wantInFlight(t, fx.g, "d", 1, "the joiner")
		wantCounts(t, fx.g, 4, "after the join")
	})
	t.Run("a member is eligible again", func(t *testing.T) {
		fx := newFull(t)
		skipped := fx.state(three, 1)
		skipped.Eligible[fx.s1] = false
		fx.g.Update(skipped)
		fx.byMember[fx.s1].Release()
		out := make(chan result, 1)
		startWaiter(context.Background(), fx.g, 1, "k", nil, out)
		waitForWaiting(t, fx.g, 1)
		fx.g.Update(fx.state(three, 1))
		res := recv(t, out)
		if res.err != nil || res.r.Member != fx.s1 {
			t.Fatalf("eligible again: %v, err %v, want S1 %s", res.r, res.err, fx.s1)
		}
		wantCounts(t, fx.g, 3, "after S1 is eligible again")
	})
}

// A waiter's filter reads the attributes in the State in force when it is
// placed, so it can be placed on a member that joined after it began to wait.
// The waiter may use only provider "pb"; here that is S1 and S2, both full
// (cap 1), and H (provider "pa") is not admitted. When d joins with provider
// "pb" and a cap of 1, only d is admitted and has room: T = 3, n = 4, bound
// 3.0 * 4 / 4 = 3, every load is within it, so KubeAI chooses the first
// member on the new walk. If that is d, d takes it. If it is S1 or S2, it is
// at its cap and the request spills on to d. If it is H, the filter refuses it
// and the first admitted member on the walk is taken, which is d or, again, a
// full member that spills on to d. Either way the waiter lands on d.
func TestAWaiterIsPlacedOnAMemberThatJoinedAfterIt(t *testing.T) {
	fx := newFull(t)
	out := make(chan result, 1)
	startWaiter(context.Background(), fx.g, 1, "k", providerIs("pb"), out)
	waitForWaiting(t, fx.g, 1)
	wantCounts(t, fx.g, 3, "the waiter reserves nothing")

	joined := fx.state(four, 1)
	joined.Attrs["d"] = Attrs{Provider: "pb"}
	fx.g.Update(joined)
	res := recv(t, out)
	if res.err != nil || res.r == nil || res.r.Member != "d" {
		t.Fatalf("join: %v, err %v, want the joiner d", res.r, res.err)
	}
	if got := fx.g.Waiting(); got != 0 {
		t.Fatalf("%d waiting after the join, want 0", got)
	}
	wantInFlight(t, fx.g, "d", 1, "the joiner")
	wantCounts(t, fx.g, 4, "after the join")
}

// With no eligible member the request does not wait: it is refused at once.
func TestNoEligibleMemberDoesNotWait(t *testing.T) {
	g := newGroupOf(three, tight, 0)
	none := stateOf(three, tight, 0)
	for m := range none.Eligible {
		none.Eligible[m] = false
	}
	g.Update(none)
	r, err := g.PickOrWait(context.Background(), "k", nil)
	if !errors.Is(err, ErrNoEligible) || r != nil {
		t.Fatalf("no member eligible: %v, %v, want ErrNoEligible", r, err)
	}
	if got := g.Waiting(); got != 0 {
		t.Fatalf("%d waiting, want 0", got)
	}
}
