package leaderlock

import (
	"context"
	"errors"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// The tests in this file cover GapLock, HBEP-8's Gaps after a restart: the
// renewTime a controller read before it took its Lease. They call the lock the
// way client-go's elector does, a read, then a write, over an in-memory Lease
// whose every state the test sets by hand, and compare with hand-written
// instants.

const gapSelf = "host_2222"

var (
	// gapT0 and gapT1 are two heartbeats of a previous holder, to the
	// microsecond as the API stores them.
	gapT0 = time.Date(2026, 11, 1, 7, 59, 58, 123456000, time.UTC)
	gapT1 = time.Date(2026, 11, 1, 7, 59, 59, 654321000, time.UTC)
)

// gapDouble is a resourcelock.Interface over one in-memory record, standing in
// for the API's Lease. A nil record is a Lease that does not exist.
type gapDouble struct {
	rec *resourcelock.LeaderElectionRecord
	// getErr, when set, is what Get returns instead of the record.
	getErr error
	// getNil makes Get return no record and no error.
	getNil bool
	// writeErr, when set, is what the next Create or Update returns without
	// writing.
	writeErr error
	// commitThenFail makes the next Create or Update write the record and then
	// return an error, as a write whose answer is lost does.
	commitThenFail bool
}

// set stores a Lease held by holder and last renewed at renew, as another
// process would have written it, acquired an hour before: the gap starts at
// the renewTime, not at the acquireTime.
func (d *gapDouble) set(holder string, renew time.Time) {
	d.rec = &resourcelock.LeaderElectionRecord{
		HolderIdentity: holder, LeaseDurationSeconds: 15,
		AcquireTime: metav1.NewTime(renew.Add(-time.Hour)), RenewTime: metav1.NewTime(renew),
	}
}

func (d *gapDouble) Get(context.Context) (*resourcelock.LeaderElectionRecord, []byte, error) {
	switch {
	case d.getErr != nil:
		return nil, nil, d.getErr
	case d.getNil:
		return nil, nil, nil
	case d.rec == nil:
		return nil, nil, apierrors.NewNotFound(coordinationv1.Resource("leases"), "x")
	}
	r := *d.rec
	return &r, []byte("{}"), nil
}

func (d *gapDouble) write(r resourcelock.LeaderElectionRecord) error {
	if d.writeErr != nil {
		err := d.writeErr
		d.writeErr = nil
		return err
	}
	d.rec = &r
	if d.commitThenFail {
		d.commitThenFail = false
		return errors.New("the connection broke after the write committed")
	}
	return nil
}

func (d *gapDouble) Create(_ context.Context, r resourcelock.LeaderElectionRecord) error {
	return d.write(r)
}

func (d *gapDouble) Update(_ context.Context, r resourcelock.LeaderElectionRecord) error {
	return d.write(r)
}

func (d *gapDouble) RecordEvent(string) {}
func (d *gapDouble) Describe() string   { return "hpcburst-system/x" }
func (d *gapDouble) Identity() string   { return gapSelf }

// gapTake is this process's take of the Lease, as the elector writes it.
var gapTake = resourcelock.LeaderElectionRecord{HolderIdentity: gapSelf, LeaseDurationSeconds: 15, RenewTime: metav1.NewTime(time.Date(2026, 11, 1, 8, 0, 0, 0, time.UTC))}

// read calls Get as the elector does. What the read finds is the double's
// state, which each test has just set; the error, if any, is the test's own
// (getErr), so it is not looked at here.
func read(l *GapLock) { _, _, _ = l.Get(context.Background()) }

// mustGap asserts GapStart: whether there is a gap and, when there is, its
// start to the microsecond.
func mustGap(t *testing.T, l *GapLock, want time.Time, wantOK bool) {
	t.Helper()
	got, ok := l.GapStart()
	if ok != wantOK {
		t.Fatalf("GapStart reports a gap = %v (start %s), want %v", ok, GapTime(got), wantOK)
	}
	if wantOK && !got.Equal(want) {
		t.Fatalf("GapStart = %s, want %s", GapTime(got), GapTime(want))
	}
}

// takeBoth runs a case through each of the two writes that can take a Lease:
// the Create of a first start and the Update of a Lease that exists.
func takeBoth(t *testing.T, f func(t *testing.T, take func(*GapLock) error)) {
	t.Helper()
	t.Run("update", func(t *testing.T) {
		f(t, func(l *GapLock) error { return l.Update(context.Background(), gapTake) })
	})
	t.Run("create", func(t *testing.T) {
		f(t, func(l *GapLock) error { return l.Create(context.Background(), gapTake) })
	})
}

// A first start: the Lease does not exist when it is read, so the take has
// nothing before it. The elector's read found none and its Create took the
// Lease; there is no gap.
func TestGapLockNoLeaseThenTakeIsNoGap(t *testing.T) {
	d := &gapDouble{}
	l := NewGapLock(d)
	read(l)
	mustGap(t, l, time.Time{}, false)
	if err := l.Create(context.Background(), gapTake); err != nil {
		t.Fatal(err)
	}
	mustGap(t, l, time.Time{}, false)
}

// A Lease held by another identity, read and then taken: the gap starts at the
// renewTime the read found, to the microsecond, and only once the take has
// succeeded.
func TestGapLockForeignLeaseThenTakeIsAGapAtItsRenewTime(t *testing.T) {
	takeBoth(t, func(t *testing.T, take func(*GapLock) error) {
		d := &gapDouble{}
		d.set("host_1111", gapT0)
		l := NewGapLock(d)
		read(l)
		mustGap(t, l, time.Time{}, false) // read, not yet taken: nothing to report
		if err := take(l); err != nil {
			t.Fatal(err)
		}
		mustGap(t, l, gapT0, true)
	})
}

// A Lease released by a clean stop has no holder and keeps its last renewTime
// (client-go writes the release so): the gap starts at that renewTime.
func TestGapLockReleasedLeaseThenTakeIsAGapAtItsRenewTime(t *testing.T) {
	d := &gapDouble{}
	d.set("", gapT0)
	l := NewGapLock(d)
	read(l)
	if err := l.Update(context.Background(), gapTake); err != nil {
		t.Fatal(err)
	}
	mustGap(t, l, gapT0, true)
}

// The last read before the first successful write is the observation: a holder
// that renews while the elector waits for its Lease to expire moves the gap's
// start to its final heartbeat.
func TestGapLockTheLastReadBeforeTheTakeCounts(t *testing.T) {
	d := &gapDouble{}
	d.set("host_1111", gapT0)
	l := NewGapLock(d)
	read(l)
	d.set("host_1111", gapT1)
	read(l)
	if err := l.Update(context.Background(), gapTake); err != nil {
		t.Fatal(err)
	}
	mustGap(t, l, gapT1, true)
}

// A Lease that was deleted between two reads leaves nothing before the take:
// the last read, which found none, wins over the earlier one.
func TestGapLockALeaseGoneBeforeTheTakeIsNoGap(t *testing.T) {
	d := &gapDouble{}
	d.set("host_1111", gapT0)
	l := NewGapLock(d)
	read(l)
	d.rec = nil
	read(l)
	if err := l.Create(context.Background(), gapTake); err != nil {
		t.Fatal(err)
	}
	mustGap(t, l, time.Time{}, false)
}

// A Lease held by another identity with no renewTime has no heartbeat to
// report: no gap.
func TestGapLockALeaseWithNoRenewTimeIsNoGap(t *testing.T) {
	d := &gapDouble{}
	d.rec = &resourcelock.LeaderElectionRecord{HolderIdentity: "host_1111", LeaseDurationSeconds: 15}
	l := NewGapLock(d)
	read(l)
	if err := l.Update(context.Background(), gapTake); err != nil {
		t.Fatal(err)
	}
	mustGap(t, l, time.Time{}, false)
}

// A write that committed but reported an error leaves this process as the
// holder, so the elector's next read shows its own identity: that is this
// run's own heartbeat, not the previous holder's, and the gap still starts at
// the read made before it.
func TestGapLockItsOwnCommittedWriteDoesNotCount(t *testing.T) {
	d := &gapDouble{}
	d.set("host_1111", gapT0)
	l := NewGapLock(d)
	read(l)
	d.commitThenFail = true
	if err := l.Update(context.Background(), gapTake); err == nil {
		t.Fatal("the double's write was meant to report an error")
	}
	mustGap(t, l, time.Time{}, false) // the failed write is not a take
	d.set(gapSelf, gapT1)             // what the committed write left
	read(l)                           // this run's own heartbeat
	if err := l.Update(context.Background(), gapTake); err != nil {
		t.Fatal(err)
	}
	mustGap(t, l, gapT0, true)
}

// With no read before the committed write but this process's own, there was
// nothing observed at all: no gap, though the Lease has a renewTime.
func TestGapLockOnlyItsOwnHeartbeatIsNoGap(t *testing.T) {
	d := &gapDouble{}
	d.set(gapSelf, gapT1)
	l := NewGapLock(d)
	read(l)
	if err := l.Update(context.Background(), gapTake); err != nil {
		t.Fatal(err)
	}
	mustGap(t, l, time.Time{}, false)
}

// A read after the take does not change what was observed before it: the
// elector reads the Lease on every renew.
func TestGapLockAReadAfterTheTakeDoesNotChangeTheObservation(t *testing.T) {
	d := &gapDouble{}
	d.set("host_1111", gapT0)
	l := NewGapLock(d)
	read(l)
	if err := l.Update(context.Background(), gapTake); err != nil {
		t.Fatal(err)
	}
	d.set("host_3333", gapT1) // another holder's heartbeat, read after the take
	read(l)
	mustGap(t, l, gapT0, true)
	d.rec = nil // and a Lease that is gone
	read(l)
	mustGap(t, l, gapT0, true)
}

// A write that failed is not a take: the gap stays unreported while the
// elector keeps trying, and the reads it makes meanwhile still count, so the
// gap starts at the last of them.
func TestGapLockAFailedWriteIsNotATake(t *testing.T) {
	for _, tc := range []struct {
		name  string
		write func(*GapLock) error
	}{
		{"update", func(l *GapLock) error { return l.Update(context.Background(), gapTake) }},
		{"create", func(l *GapLock) error { return l.Create(context.Background(), gapTake) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := &gapDouble{}
			d.set("host_1111", gapT0)
			l := NewGapLock(d)
			read(l)
			d.writeErr = apierrors.NewConflict(coordinationv1.Resource("leases"), "x", errors.New("the Lease changed"))
			if err := tc.write(l); err == nil {
				t.Fatal("the double's write was meant to fail")
			}
			mustGap(t, l, time.Time{}, false)
			d.set("host_1111", gapT1)
			read(l)
			mustGap(t, l, time.Time{}, false)
			if err := l.Update(context.Background(), gapTake); err != nil {
				t.Fatal(err)
			}
			mustGap(t, l, gapT1, true)
		})
	}
}

// A read that fails, or returns nothing, observes nothing: what an earlier
// read found stays.
func TestGapLockAFailedReadKeepsTheObservation(t *testing.T) {
	d := &gapDouble{}
	d.set("host_1111", gapT0)
	l := NewGapLock(d)
	read(l)
	d.getErr = errors.New("the API server timed out")
	read(l)
	d.getErr, d.getNil = nil, true
	read(l) // no record and no error: must not panic
	d.getNil = false
	if err := l.Update(context.Background(), gapTake); err != nil {
		t.Fatal(err)
	}
	mustGap(t, l, gapT0, true)
}

// OnObserve runs after each read that counts, with the observation already
// kept, and only then: not for a read that shows this process as the holder
// before a write, not for a failed read, not for a read after the take. The
// hook may be unset.
func TestGapLockOnObserveRunsOnlyForReadsThatCount(t *testing.T) {
	d := &gapDouble{}
	l := NewGapLock(d)
	calls := 0
	var seen time.Time
	l.OnObserve = func() {
		calls++
		l.mu.Lock()
		seen = l.previous
		l.mu.Unlock()
	}
	step := func(name string, wantCalls int) {
		t.Helper()
		if calls != wantCalls {
			t.Fatalf("after %s: OnObserve ran %d times, want %d", name, calls, wantCalls)
		}
	}
	read(l) // no Lease: counts
	step("a read that found no Lease", 1)
	d.set("host_1111", gapT0)
	read(l) // another holder: counts, and the hook sees the renewTime
	step("a read of another holder's Lease", 2)
	if !seen.Equal(gapT0) {
		t.Fatalf("the hook ran before the observation was kept: it saw %s, want %s", GapTime(seen), GapTime(gapT0))
	}
	d.set(gapSelf, gapT1)
	read(l) // this process's own heartbeat: does not count
	step("a read that shows this process as the holder", 2)
	d.getErr = errors.New("the API server timed out")
	read(l) // a failed read: does not count
	step("a failed read", 2)
	d.getErr = nil
	d.set("host_1111", gapT1)
	read(l)
	step("another holder's heartbeat", 3)
	if err := l.Update(context.Background(), gapTake); err != nil {
		t.Fatal(err)
	}
	read(l) // after the take: does not count
	step("a read after the take", 3)
	d.rec = nil
	read(l)
	step("a read of a gone Lease after the take", 3)
}

// The lock delegates the rest of resourcelock.Interface to the lock it wraps,
// and a literal with Interface set, which the lifecycle controller's tests
// use, behaves as one built by NewGapLock.
func TestGapLockDelegatesAndAcceptsALiteral(t *testing.T) {
	for name, build := range map[string]func(resourcelock.Interface) *GapLock{
		"NewGapLock": NewGapLock,
		"literal":    func(i resourcelock.Interface) *GapLock { return &GapLock{Interface: i} },
	} {
		d := &gapDouble{}
		d.set("host_1111", gapT0)
		l := build(d)
		if l.Identity() != gapSelf || l.Describe() != "hpcburst-system/x" {
			t.Fatalf("%s: Identity %q, Describe %q: not the wrapped lock's", name, l.Identity(), l.Describe())
		}
		read(l)
		if err := l.Update(context.Background(), gapTake); err != nil {
			t.Fatal(err)
		}
		mustGap(t, l, gapT0, true)
	}
}

// GapTime names the instant in UTC to the microsecond, as the API stores a
// renewTime, truncating below it and keeping the trailing zeros.
func TestGapTimeIsUTCToTheMicrosecond(t *testing.T) {
	zone := time.FixedZone("plus-two", 2*3600)
	for _, tc := range []struct {
		name string
		in   time.Time
		want string
	}{
		{"utc", time.Date(2026, 11, 1, 7, 59, 58, 123456000, time.UTC), "2026-11-01T07:59:58.123456Z"},
		{"another zone", time.Date(2026, 11, 1, 9, 59, 58, 123456000, zone), "2026-11-01T07:59:58.123456Z"},
		{"nanoseconds below the microsecond", time.Date(2026, 11, 1, 7, 59, 58, 123456789, time.UTC), "2026-11-01T07:59:58.123456Z"},
		{"a whole second", time.Date(2026, 11, 1, 7, 59, 58, 0, time.UTC), "2026-11-01T07:59:58.000000Z"},
		{"a day boundary in another zone", time.Date(2026, 11, 2, 0, 30, 0, 500000000, zone), "2026-11-01T22:30:00.500000Z"},
	} {
		if got := GapTime(tc.in); got != tc.want {
			t.Errorf("%s: GapTime(%v) = %q, want %q", tc.name, tc.in, got, tc.want)
		}
	}
}
