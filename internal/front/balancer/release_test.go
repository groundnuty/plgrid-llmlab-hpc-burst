package balancer

// Release (HBEP-5, Test plan: Release): two releases of one request leave the
// counts unchanged, and a member that leaves has its open requests released at
// once, their later completions changing nothing. KubeAI's total must equal
// the members' sum at every placement, and here also the test's own tally of
// the reservations it holds, so a reservation lost from all three counts at
// once is seen too. "No placement above the bound" proves nothing alone: with
// consistent counts it follows from the arithmetic (S10).

import (
	"fmt"
	"maps"
	"slices"
	"testing"
)

func TestReleaseTwiceIsANoOp(t *testing.T) {
	g := newGroupOf(four, tight, 0)
	r1, r2 := mustPick(t, g, "k1"), mustPick(t, g, "k2")
	wantCounts(t, g, 2, "two requests reserved")
	r1.Release()
	r1.Release() // upstream's release would decrement again
	// Two reserved, one released once: one is open, on r2's member.
	wantCounts(t, g, 1, "r1 released twice")
	for _, m := range four {
		want := int64(0)
		if m == r2.Member {
			want = 1
		}
		wantInFlight(t, g, m, want, "r1 released twice")
	}
	r2.Release()
	r2.Release()
	wantCounts(t, g, 0, "both released, r2 twice")
	var none *Reservation
	none.Release() // a nil reservation is a no-op, not a panic
}

// The control: the plain copied group decrements on every call (fork test
// TestUpstreamReleaseDecrementsOnEveryCall), so without the guard one open
// request would read as none.
func TestPlainGroupReleasesOnEveryCall(t *testing.T) {
	f := forkOf(four)
	acquire := forkAcquirer(f, tight)
	_, done1, err := acquire("k1")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := acquire("k2"); err != nil {
		t.Fatal(err)
	}
	done1()
	done1()
	if got := f.TotalInFlight(); got != 0 {
		t.Fatalf("plain total = %d after two requests and one released twice; the wrapper's guard keeps it at 1, upstream's reads 0", got)
	}
}

// tally is the test's own count of the reservations it holds open, member by
// member, against which every count of the group is checked.
type tally struct {
	t       *testing.T
	g       *Group
	percent int
	live    map[string]bool
	held    map[string][]*Reservation
	gone    []*Reservation // reservations a leave released
	step    int
}

func newTally(t *testing.T, members []string, percent int) *tally {
	tl := &tally{t: t, percent: percent, live: map[string]bool{}, held: map[string][]*Reservation{}}
	tl.g = newGroupOf(members, percent, 0)
	for _, m := range members {
		tl.live[m] = true
	}
	tl.g.OnDrift = func(total, sum, open int64) {
		t.Errorf("drift at placement %d: total %d, members' sum %d, open %d", tl.step, total, sum, open)
	}
	return tl
}

func (tl *tally) place(key string) *Reservation {
	tl.t.Helper()
	r, err := tl.g.TryPick(key, nil)
	if err != nil {
		tl.t.Fatalf("placement %d (%q): %v", tl.step+1, key, err)
	}
	tl.step++
	if !tl.live[r.Member] {
		tl.t.Fatalf("placement %d went to %s, which is not a member", tl.step, r.Member)
	}
	tl.held[r.Member] = append(tl.held[r.Member], r)
	tl.check(fmt.Sprintf("placement %d on %s", tl.step, r.Member))
	return r
}

// check compares every count with the tally: each member's, the total, the
// members' sum and the open reservations.
func (tl *tally) check(when string) {
	tl.t.Helper()
	var want int64
	for m := range tl.live {
		n := int64(len(tl.held[m]))
		want += n
		wantInFlight(tl.t, tl.g, m, n, when)
	}
	wantCounts(tl.t, tl.g, want, when)
}

// busiest is the member, other than skip, holding the most open requests.
func (tl *tally) busiest(skip string) string {
	best := ""
	for _, m := range slices.Sorted(maps.Keys(tl.live)) {
		if m != skip && (best == "" || len(tl.held[m]) > len(tl.held[best])) {
			best = m
		}
	}
	return best
}

// update moves the group to members. The members that left take their open
// requests with them: the group must report releasing exactly those.
func (tl *tally) update(members []string) {
	tl.t.Helper()
	next := map[string]bool{}
	for _, m := range members {
		next[m] = true
	}
	want := 0
	var left []string
	for m := range tl.live {
		if next[m] {
			continue
		}
		left = append(left, m)
		want += len(tl.held[m])
		tl.gone = append(tl.gone, tl.held[m]...)
		delete(tl.held, m)
	}
	if len(left) > 0 && want == 0 {
		tl.t.Fatalf("%v left holding nothing: the sequence tests no release", left)
	}
	if got := tl.g.Update(stateOf(members, tl.percent, 0)); got != want {
		tl.t.Fatalf("Update released %d reservations, want %d (those open on %v)", got, want, left)
	}
	tl.live = next
	for _, m := range left {
		if _, ok := tl.g.InFlight(m); ok {
			tl.t.Fatalf("%s left the group but still reports a count", m)
		}
	}
	tl.check(fmt.Sprintf("after the update to %v", members))
}

// finish completes every request, twice each: first those a leave released,
// then the live ones. No count may move except by the one real release.
func (tl *tally) finish() {
	tl.t.Helper()
	for _, r := range tl.gone {
		r.Release()
		r.Release()
		tl.check(fmt.Sprintf("a request a leave released completes (on %s)", r.Member))
	}
	for _, m := range slices.Sorted(maps.Keys(tl.held)) {
		for len(tl.held[m]) > 0 {
			r := tl.held[m][0]
			tl.held[m] = tl.held[m][1:]
			r.Release()
			r.Release()
			tl.check(fmt.Sprintf("a request on %s completes", m))
		}
	}
	wantCounts(tl.t, tl.g, 0, "every request completed")
}

// A drain with no replacement: the busiest member leaves with its requests
// open. The counts agree with the tally at every placement before and after.
func TestLeaveReleasesOpenReservationsAtOnce(t *testing.T) {
	tl := newTally(t, four, tight)
	for i := 0; i < 16; i++ {
		tl.place(fmt.Sprintf("conv-%d", i%6))
	}
	leaver := tl.busiest("")
	tl.update(without(four, leaver))
	for i := 0; i < 8; i++ {
		tl.place(fmt.Sprintf("later-%d", i%4))
	}
	tl.finish()
}

// A handover: a replacement joins with a count of zero, then the busiest of
// the old members leaves. Unique keys after the join spread the homes, so the
// joiner takes some requests (checked).
func TestHandoverKeepsTheCountsAtEveryPlacement(t *testing.T) {
	tl := newTally(t, three, tight)
	for i := 0; i < 9; i++ {
		tl.place(fmt.Sprintf("conv-%d", i%5))
	}
	tl.update(four)
	wantInFlight(t, tl.g, "d", 0, "the joiner")
	onD := 0
	for i := 0; i < 30; i++ {
		if tl.place(fmt.Sprintf("join-%d", i)).Member == "d" {
			onD++
		}
	}
	if onD == 0 {
		t.Fatal("nothing was placed on the joiner: the handover is not exercised")
	}
	leaver := tl.busiest("d")
	tl.update(without(four, leaver))
	for i := 0; i < 8; i++ {
		tl.place(fmt.Sprintf("after-%d", i))
	}
	tl.finish()
}

// The control: on the plain group a removed member's open requests stay in
// KubeAI's total, so total and members' sum disagree. This is the drift the
// two sequences above would show if the wrapper did not release at a leave.
func TestPlainGroupKeepsALeaversCountInTheTotal(t *testing.T) {
	f := forkOf(four)
	acquire := forkAcquirer(f, noBound)
	perMember := map[string]int{}
	for i := 0; i < 8; i++ {
		addr, _, err := acquire(fmt.Sprintf("conv-%d", i))
		if err != nil {
			t.Fatal(err)
		}
		perMember[addr]++
	}
	leaver := four[0]
	for _, m := range four {
		if perMember[m] > perMember[leaver] {
			leaver = m
		}
	}
	f.SetEndpoints(without(four, leaver))
	var sum int64
	for _, m := range without(four, leaver) {
		v, _ := f.InFlight(m)
		sum += v
	}
	if got, want := f.TotalInFlight(), int64(8); got != want {
		t.Fatalf("plain total = %d after %s left, want %d (upstream does not release a leaver)", got, leaver, want)
	}
	if want := int64(8 - perMember[leaver]); sum != want || perMember[leaver] == 0 {
		t.Fatalf("members' sum = %d, want %d; %s held %d", sum, want, leaver, perMember[leaver])
	}
}
