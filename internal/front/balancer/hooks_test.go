package balancer

// The group's hooks that feed the front (HBEP-5, Workloads, Waiting; HBEP-8, Front metrics; the science agent's rulings Q5 and Q6 on #764):
//
//	OnFree    after a Release that released, outside the group's mutex, after the group's own waiters were placed
//	OnChange  after each Update, outside the mutex, after the waiters it placed
//	OnCount   +1 when a reservation is counted on a member, -1 when it is released, once, under the mutex
//	OnLeave   once when a member has left and its reservations are released, under the mutex
//
// "Outside the mutex" is tested by calling the group's own readers (Waiting, Open, Counts) from inside the hook, which take the mutex: a hook run
// under it would hang them, so each call runs under a 5 s deadline that turns a hang into a clear failure. "After the group's own waiters were
// placed" is read from the same calls: the queue is empty and the waiter holds its reservation. Every expected value is typed by hand from the rule;
// the groups have one or two members and a cap of 1, so no placement depends on the ring.

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"
)

// hkSeen is what a hook saw of the group when it ran, read through the group's own calls.
type hkSeen struct {
	hung               bool // the group's readers did not answer within 5 s: the hook ran under the mutex
	waiting            int
	openA, openB       int
	total, sum, opened int64
}

// hkProbe is an OnFree or OnChange hook that records one hkSeen per call.
type hkProbe struct {
	g     *Group
	mu    sync.Mutex
	calls []hkSeen
}

func (p *hkProbe) hook() {
	got := make(chan hkSeen, 1)
	go func() {
		var s hkSeen
		s.waiting = p.g.Waiting()
		s.openA, s.openB = p.g.Open("a"), p.g.Open("b")
		s.total, s.sum, s.opened = p.g.Counts()
		got <- s
	}()
	var s hkSeen
	select {
	case s = <-got:
	case <-time.After(5 * time.Second):
		s.hung = true
	}
	p.mu.Lock()
	p.calls = append(p.calls, s)
	p.mu.Unlock()
}

func (p *hkProbe) seen() []hkSeen {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.calls)
}

// wantSeen fails unless the hook ran exactly len(want) times and saw want, in order.
func (p *hkProbe) wantSeen(t *testing.T, when string, want ...hkSeen) {
	t.Helper()
	got := p.seen()
	for i, s := range got {
		if s.hung {
			t.Fatalf("%s: call %d: the group's readers hung for 5 s inside the hook: it ran under the group's mutex", when, i+1)
		}
	}
	if !slices.Equal(got, want) {
		t.Fatalf("%s: the hook saw %+v, want %+v", when, got, want)
	}
}

// hkOneMember is a group of the single member "a" with a cap of 1.
func hkOneMember() *Group {
	g := New()
	g.Update(stateOf([]string{"a"}, tight, 1))
	return g
}

// hkQueue holds one waiter in the group's FIFO and returns the channel its result arrives on.
func hkQueue(t *testing.T, g *Group, key string) <-chan result {
	t.Helper()
	out := make(chan result, 1)
	startWaiter(context.Background(), g, 1, key, nil, out)
	waitForWaiting(t, g, 1)
	return out
}

// OnFree runs once after a Release that released, once the group's mutex is free and the group's own waiter is placed: inside the hook the queue is
// empty and a's one open reservation is the waiter's (the cap is 1, so r1's release was the only room).
func TestOnFreeRunsOnceAfterAReleaseWithTheMutexFreeAndTheWaitersPlaced(t *testing.T) {
	g := hkOneMember()
	r1 := mustPick(t, g, "k1")
	out := hkQueue(t, g, "k2")
	p := &hkProbe{g: g}
	g.OnFree = p.hook
	r1.Release()
	// r1 is released and the waiter holds a's slot: waiting 0, a open 1, total = sum = opened = 1.
	p.wantSeen(t, "after r1's release", hkSeen{waiting: 0, openA: 1, total: 1, sum: 1, opened: 1})
	res := recv(t, out)
	if res.err != nil || res.r.Member != "a" {
		t.Fatalf("the waiter: member %v err %v, want a and nil", res.r, res.err)
	}
	// The waiter's own release is a release too, and empties the group.
	res.r.Release()
	p.wantSeen(t, "after the waiter's release",
		hkSeen{waiting: 0, openA: 1, total: 1, sum: 1, opened: 1},
		hkSeen{waiting: 0, openA: 0, total: 0, sum: 0, opened: 0})
}

// OnFree is not run for a second Release of one reservation (it released nothing), nor for ReleaseQuiet, which still releases once and still places
// the group's waiters.
func TestOnFreeIsNotRunByASecondReleaseNorByReleaseQuiet(t *testing.T) {
	g := hkOneMember()
	r1 := mustPick(t, g, "k1")
	out := hkQueue(t, g, "k2")
	p := &hkProbe{g: g}
	g.OnFree = p.hook

	r1.ReleaseQuiet()
	res := recv(t, out) // the quiet release placed the group's waiter
	if res.err != nil || res.r.Member != "a" {
		t.Fatalf("the waiter: member %v err %v, want a and nil", res.r, res.err)
	}
	wantCounts(t, g, 1, "r1 released once, quietly, and its waiter placed")
	wantInFlight(t, g, "a", 1, "r1 released once, quietly, and its waiter placed")
	p.wantSeen(t, "after ReleaseQuiet") // no call

	r1.Release() // a second release of r1: nothing is released
	wantCounts(t, g, 1, "r1 released again")
	p.wantSeen(t, "after the second release of r1") // still no call

	res.r.Release() // a real release: the hook runs, once
	p.wantSeen(t, "after the waiter's real release", hkSeen{waiting: 0, openA: 0, total: 0, sum: 0, opened: 0})
}

// A completion after its member left released nothing (the leave released it), so it frees no route and the hook does not run.
func TestOnFreeIsNotRunForACompletionAfterItsMemberLeft(t *testing.T) {
	g := New()
	g.Update(withProviders(stateOf([]string{"a", "b"}, tight, 1), map[string]string{"a": "pa", "b": "pb"}))
	p := &hkProbe{g: g}
	g.OnFree = p.hook
	ra := hkPick(t, g, "k1", "pa", Charge{})
	if ra.Member != "a" {
		t.Fatalf("a pick on provider pa: member %s, want a", ra.Member)
	}
	if released := g.Update(withProviders(stateOf([]string{"b"}, tight, 1), map[string]string{"b": "pb"})); released != 1 {
		t.Fatalf("a's leave released %d reservations, want 1", released)
	}
	ra.Release() // a's reservation was released by the leave: this releases nothing
	p.wantSeen(t, "after the completion of a reservation its member's leave released")
	// Control: a real release on b runs the hook once, so the hook is wired and the quiet above is not an unset field.
	hkPick(t, g, "k2", "pb", Charge{}).Release()
	p.wantSeen(t, "after b's real release", hkSeen{waiting: 0, openA: 0, openB: 0, total: 0, sum: 0, opened: 0})
}

// OnChange runs once after each Update, whether or not it changed anything, once the group's mutex is free and after the waiters the Update placed.
// a (cap 1) is full with r1 and one request waits; b joins, and the Update places the waiter on b before the hook runs.
func TestOnChangeRunsOnceAfterEachUpdateWithTheMutexFreeAndTheWaitersPlaced(t *testing.T) {
	g := hkOneMember()
	r1 := mustPick(t, g, "k1")
	out := hkQueue(t, g, "k2")
	p := &hkProbe{g: g}
	g.OnChange = p.hook

	g.Update(stateOf([]string{"a", "b"}, tight, 1))
	// b joined: the waiter holds b's slot, r1 holds a's: waiting 0, a 1, b 1, total 2.
	p.wantSeen(t, "after b's join", hkSeen{waiting: 0, openA: 1, openB: 1, total: 2, sum: 2, opened: 2})
	res := recv(t, out)
	if res.err != nil || res.r.Member != "b" {
		t.Fatalf("the waiter: member %v err %v, want b (a is at its cap)", res.r, res.err)
	}

	g.Update(stateOf([]string{"a", "b"}, tight, 1)) // nothing changes: the hook still runs, once
	p.wantSeen(t, "after an Update that changes nothing",
		hkSeen{waiting: 0, openA: 1, openB: 1, total: 2, sum: 2, opened: 2},
		hkSeen{waiting: 0, openA: 1, openB: 1, total: 2, sum: 2, opened: 2})

	// b leaves: its reservation, the waiter's, is released by the Update, so the hook sees a's one request alone.
	g.Update(stateOf([]string{"a"}, tight, 1))
	p.wantSeen(t, "after b's leave",
		hkSeen{waiting: 0, openA: 1, openB: 1, total: 2, sum: 2, opened: 2},
		hkSeen{waiting: 0, openA: 1, openB: 1, total: 2, sum: 2, opened: 2},
		hkSeen{waiting: 0, openA: 1, openB: 0, total: 1, sum: 1, opened: 1})
	r1.Release()
}

// hkEvent is one call of OnCount (a count change) or OnLeave.
type hkEvent struct {
	leave bool
	a     Attrs
	ch    Charge
	delta int
}

// hkLog records the calls of OnCount and OnLeave. Both run under the group's mutex, so the hooks only record, under a mutex of their own.
type hkLog struct {
	mu sync.Mutex
	ev []hkEvent
}

func (l *hkLog) count(a Attrs, ch Charge, delta int) {
	l.mu.Lock()
	l.ev = append(l.ev, hkEvent{a: a, ch: ch, delta: delta})
	l.mu.Unlock()
}

func (l *hkLog) leave(a Attrs) {
	l.mu.Lock()
	l.ev = append(l.ev, hkEvent{leave: true, a: a})
	l.mu.Unlock()
}

func (l *hkLog) events() []hkEvent {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.ev)
}

// want fails unless the log holds exactly these events, in order.
func (l *hkLog) want(t *testing.T, when string, want ...hkEvent) {
	t.Helper()
	if got := l.events(); !slices.Equal(got, want) {
		t.Fatalf("%s: OnCount/OnLeave calls\n got  %+v\n want %+v", when, got, want)
	}
}

// The members' attributes, with the Instance UIDs the in-flight series is labelled by.
var (
	hkAttrsA = Attrs{Provider: "pa", DisplayID: "id-a", UID: "uid-a"}
	hkAttrsB = Attrs{Provider: "pb", DisplayID: "id-b", UID: "uid-b"}
)

// The charges: a named request with its Workload, another Workload's, and a request that names none.
var (
	hkAlpha   = Charge{Named: true, Tokens: 125, Workload: "alpha"}
	hkBeta    = Charge{Named: true, Tokens: 50, Workload: "beta"}
	hkUnnamed = Charge{}
)

// hkTwoMembers is a group of a (provider pa) and b (provider pb), no cap, with the hooks recording into the returned log. The hooks are set after
// the first Update, which counts nothing anyway.
func hkTwoMembers() (*Group, *hkLog) {
	g := New()
	g.Update(hkState([]string{"a", "b"}))
	l := &hkLog{}
	g.OnCount, g.OnLeave = l.count, l.leave
	return g, l
}

// hkState is a State of the members, all eligible, with the attributes above for those present.
func hkState(members []string) State {
	s := stateOf(members, tight, 0)
	s.Attrs = map[string]Attrs{}
	for _, m := range members {
		s.Attrs[m] = map[string]Attrs{"a": hkAttrsA, "b": hkAttrsB}[m]
	}
	return s
}

// hkPick reserves on the member of provider p with charge ch; the filter fixes the member, so no ring is involved.
func hkPick(t *testing.T, g *Group, key, provider string, ch Charge) *Reservation {
	t.Helper()
	r, err := g.TryPickFor(key, providerIs(provider), ch)
	if err != nil {
		t.Fatalf("TryPickFor(%q, provider %s): %v", key, provider, err)
	}
	return r
}

// OnCount: +1 when a reservation is counted, with the member's Attrs (its UID included) and the reservation's Charge (its Workload name included),
// and -1 once when it is released, by Release or ReleaseQuiet; a second release counts nothing, and a pick that finds no member counts nothing.
func TestOnCountCountsEachReservationOnceUpAndOnceDown(t *testing.T) {
	g, l := hkTwoMembers()
	r1 := hkPick(t, g, "k1", "pa", hkAlpha)
	r2 := hkPick(t, g, "k2", "pa", hkUnnamed)
	r3 := hkPick(t, g, "k3", "pb", hkBeta)
	if r1.Member != "a" || r2.Member != "a" || r3.Member != "b" {
		t.Fatalf("members %s %s %s, want a a b", r1.Member, r2.Member, r3.Member)
	}
	if _, err := g.TryPickFor("k4", providerIs("no-such-provider"), hkAlpha); err != ErrNoEligible {
		t.Fatalf("a pick that no member matches: err %v, want ErrNoEligible", err)
	}
	l.want(t, "three reservations placed, one pick that found no member",
		hkEvent{a: hkAttrsA, ch: hkAlpha, delta: +1},
		hkEvent{a: hkAttrsA, ch: hkUnnamed, delta: +1},
		hkEvent{a: hkAttrsB, ch: hkBeta, delta: +1})

	r1.Release()
	r1.Release() // upstream's release would count again: this one counts nothing
	r3.ReleaseQuiet()
	r3.Release()
	r2.Release()
	l.want(t, "r1 released twice, r3 quietly then again, r2 once",
		hkEvent{a: hkAttrsA, ch: hkAlpha, delta: +1},
		hkEvent{a: hkAttrsA, ch: hkUnnamed, delta: +1},
		hkEvent{a: hkAttrsB, ch: hkBeta, delta: +1},
		hkEvent{a: hkAttrsA, ch: hkAlpha, delta: -1},
		hkEvent{a: hkAttrsB, ch: hkBeta, delta: -1},
		hkEvent{a: hkAttrsA, ch: hkUnnamed, delta: -1})
}

// A waiter is counted when it is placed, not when it queues: r1 fills a (cap 1), the waiter queues and counts nothing, and r1's release is -1 and
// then, as the waiter is placed in the same release, +1 on a with the waiter's own Charge.
func TestOnCountCountsAWaiterWhenItIsPlacedAndNotWhenItQueues(t *testing.T) {
	g := New()
	s := hkState([]string{"a"})
	s.Caps = map[string]int{"a": 1}
	g.Update(s)
	l := &hkLog{}
	g.OnCount, g.OnLeave = l.count, l.leave
	r1 := hkMustPick(t, g, "k1", hkAlpha)
	done := make(chan *Reservation, 1)
	go func() {
		r, err := g.PickOrWaitFor(context.Background(), "k2", nil, hkBeta)
		if err != nil {
			t.Errorf("the waiter: %v", err)
		}
		done <- r
	}()
	waitForWaiting(t, g, 1)
	l.want(t, "r1 placed, the waiter queued", hkEvent{a: hkAttrsA, ch: hkAlpha, delta: +1})
	r1.Release()
	r2 := <-done
	l.want(t, "r1 released, the waiter placed",
		hkEvent{a: hkAttrsA, ch: hkAlpha, delta: +1},
		hkEvent{a: hkAttrsA, ch: hkAlpha, delta: -1},
		hkEvent{a: hkAttrsA, ch: hkBeta, delta: +1})
	r2.Release()
}

func hkMustPick(t *testing.T, g *Group, key string, ch Charge) *Reservation {
	t.Helper()
	r, err := g.TryPickFor(key, nil, ch)
	if err != nil {
		t.Fatalf("TryPickFor(%q): %v", key, err)
	}
	return r
}

// A member that leaves through Update has -1 called for each of its open reservations, then OnLeave once, with the attributes the member had (the new
// State here names none for it). A completion after the leave calls neither again, and the member that stayed is untouched.
func TestOnCountAMemberThatLeavesHasEachOpenReservationReleasedThenOnLeaveOnce(t *testing.T) {
	g, l := hkTwoMembers()
	r1 := hkPick(t, g, "k1", "pa", hkAlpha)
	r2 := hkPick(t, g, "k2", "pa", hkUnnamed)
	r3 := hkPick(t, g, "k3", "pb", hkBeta)
	before := len(l.events())
	if before != 3 {
		t.Fatalf("%d calls for three placements, want 3", before)
	}

	// a leaves. The new State holds no attributes for a: the leave reports the ones a had.
	if released := g.Update(hkState([]string{"b"})); released != 2 {
		t.Fatalf("a's leave released %d reservations, want 2", released)
	}
	got := l.events()[before:]
	if len(got) != 3 {
		t.Fatalf("a's leave made %d calls %+v, want 3: -1 for r1, -1 for r2, then OnLeave", len(got), got)
	}
	// The two releases come in the order of a map; OnLeave is last.
	down := got[:2]
	wantDown := []hkEvent{{a: hkAttrsA, ch: hkAlpha, delta: -1}, {a: hkAttrsA, ch: hkUnnamed, delta: -1}}
	if !(down[0] == wantDown[0] && down[1] == wantDown[1]) && !(down[0] == wantDown[1] && down[1] == wantDown[0]) {
		t.Errorf("the releases at a's leave were %+v, want -1 for r1's charge and -1 for r2's, in either order", down)
	}
	if want := (hkEvent{leave: true, a: hkAttrsA}); got[2] != want {
		t.Errorf("the last call at a's leave was %+v, want %+v", got[2], want)
	}

	// The completions after the leave call nothing; b's reservation is still open.
	r1.Release()
	r2.Release()
	r1.ReleaseQuiet()
	if after := len(l.events()); after != before+3 {
		t.Fatalf("completions after the leave made %d more calls: %+v", after-before-3, l.events()[before+3:])
	}
	wantInFlight(t, g, "b", 1, "b untouched by a's leave")
	r3.Release()
	if last := l.events()[before+3:]; len(last) != 1 || last[0] != (hkEvent{a: hkAttrsB, ch: hkBeta, delta: -1}) {
		t.Errorf("b's release made %+v, want one -1 with b's attributes and beta's charge", last)
	}
}

// A member that leaves holding nothing calls OnLeave once and no count: its series must go all the same.
func TestOnLeaveIsCalledOnceForAMemberThatHoldsNothing(t *testing.T) {
	g, l := hkTwoMembers()
	g.Update(hkState([]string{"a"}))
	l.want(t, "b left holding nothing", hkEvent{leave: true, a: hkAttrsB})
	g.Update(hkState([]string{"a"})) // no change: no second leave
	l.want(t, "an Update that changes no member", hkEvent{leave: true, a: hkAttrsB})
}
