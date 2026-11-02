package leaderlock

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-logr/logr/funcr"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

// fakeManager is a manager whose Start runs start; every other method is the
// nil interface's and is not called.
type fakeManager struct {
	manager.Manager
	start func(ctx context.Context) error
}

func (m *fakeManager) Start(ctx context.Context) error { return m.start(ctx) }

// stopsWhenCancelled is the Start of a manager that stops cleanly at its
// context's end, as controller-runtime's does on a requested stop.
func stopsWhenCancelled(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

// renew is a renew record of this holder, as the elector writes it.
var renew = resourcelock.LeaderElectionRecord{HolderIdentity: me, LeaseDurationSeconds: 15}

// runStopped runs the wrapper over a manager whose Start is start and the
// fake Lease f behind leaderlock's retrying lock, with Start's context
// cancelled first when cancelled is true, and returns the log and Start's
// result. The elector's last renew write is recorded as succeeded when led
// is true, as failed when it is false.
func runStopped(t *testing.T, f *fakeLease, cancelled, led bool, start func(context.Context) error) (*logs, error) {
	t.Helper()
	return runStoppedAfter(t, f, cancelled, led, nil, start)
}

// runStoppedAfter is runStopped with phaseOne, when set, run on the tracked
// lock before Start: the elector's release as it reached the lock.
func runStoppedAfter(t *testing.T, f *fakeLease, cancelled, led bool, phaseOne func(*Renewals), start func(context.Context) error) (*logs, error) {
	t.Helper()
	inner, l := newLock(f)
	lock := TrackRenewals(inner)
	var last error
	if !led {
		last = errors.New("injected: the renew timed out")
	}
	lock.wrote(context.Background(), renew, last)
	if phaseOne != nil {
		phaseOne(lock)
	}
	mgr := ReleaseAfterStart(&fakeManager{start: start}, lock, funcr.New(l.add, funcr.Options{}))
	if !ReleasesAfterStart(mgr) {
		t.Fatal("ReleasesAfterStart does not recognise ReleaseAfterStart's manager")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if cancelled {
		cancel()
	}
	err := mgr.Start(ctx)
	return l, err
}

// A clean, requested stop whose elector left the Lease naming this holder
// (its release read failed) releases it after Start: one write of client-go's
// release record, no holder and a 1 s duration, with the Lease's transitions
// kept.
func TestACleanStopReleasesALeaseTheElectorLeftHeld(t *testing.T) {
	f := &fakeLease{holder: me, transitions: 3}
	l, err := runStopped(t, f, true, true, stopsWhenCancelled)
	if err != nil {
		t.Fatalf("Start returned %v; want nil, Start's own result", err)
	}
	if holder, updates := f.state(); holder != "" || updates != 1 {
		t.Fatalf("after Start the Lease is held by %q after %d writes; want it free after 1", holder, updates)
	}
	if w := f.written[0]; w.HolderIdentity != "" || w.LeaseDurationSeconds != 1 || w.LeaderTransitions != 3 {
		t.Fatalf("the release wrote %+v; want no holder, a 1 s duration and the 3 transitions kept", w)
	}
	if !l.has("released the leader Lease after the manager stopped") {
		t.Errorf("the release was not logged: %v", l.lines)
	}
}

// A read that fails after Start is tried again, and the release lands.
func TestTheReleaseAfterStartRetriesAFailedRead(t *testing.T) {
	f := &fakeLease{holder: me, failGets: 2}
	if _, err := runStopped(t, f, true, true, stopsWhenCancelled); err != nil {
		t.Fatalf("Start returned %v", err)
	}
	if holder, updates := f.state(); holder != "" || updates != 1 {
		t.Fatalf("the Lease is held by %q after %d writes; want it released once the reads succeed", holder, updates)
	}
}

// A Lease that the elector released, that another holder took, or that does
// not exist is not written; the check costs a read.
func TestTheReleaseAfterStartLeavesALeaseItDoesNotHold(t *testing.T) {
	for _, tc := range []struct {
		name string
		f    *fakeLease
	}{
		{"released by the elector", &fakeLease{}},
		{"taken by another holder", &fakeLease{holder: "host_2222"}},
		{"missing", &fakeLease{missing: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l, err := runStopped(t, tc.f, true, true, stopsWhenCancelled)
			if err != nil {
				t.Fatalf("Start returned %v", err)
			}
			if _, updates := tc.f.state(); updates != 0 || tc.f.gets == 0 {
				t.Fatalf("%d writes after %d reads; want a read and no write", updates, tc.f.gets)
			}
			if l.has("released the leader Lease") || l.has("could not release") {
				t.Errorf("logged a release or a failure for a Lease this holder does not hold: %v", l.lines)
			}
		})
	}
}

// Reads that keep failing give up when the shared ReleaseBudget runs out (the
// elector's release never reached Update, so all of it is left): Start
// returns its own nil, the failure is logged, and the Lease stays held.
func TestTheReleaseAfterStartGivesUpWithinItsBudget(t *testing.T) {
	f := &fakeLease{holder: me, failGets: 1 << 20}
	l, err := runStopped(t, f, true, true, stopsWhenCancelled)
	if err != nil {
		t.Fatalf("Start returned %v; want its own nil", err)
	}
	if holder, updates := f.state(); holder != me || updates != 0 || f.gets < 2 {
		t.Fatalf("the Lease is held by %q after %d writes and %d reads; want it still held, the read retried and nothing written", holder, updates, f.gets)
	}
	if !l.has("could not release the leader Lease after the manager stopped") {
		t.Errorf("the give-up was not logged: %v", l.lines)
	}
}

// An involuntary stop releases nothing, and Start's result comes back
// unchanged: a lost leadership or a stop past its grace (an error after a
// requested stop), a failed runnable (an error with no stop requested), or a
// manager that returned nil on its own. The Lease still names this holder, so
// a release would have written it.
func TestAnInvoluntaryStopReleasesNothing(t *testing.T) {
	lost := errors.New("leader election lost")
	for _, tc := range []struct {
		name      string
		cancelled bool
		err       error
	}{
		{"an error after a requested stop", true, lost},
		{"an error with no stop requested", false, lost},
		{"nil with no stop requested", false, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeLease{holder: me}
			l, got := runStopped(t, f, tc.cancelled, true, func(context.Context) error { return tc.err })
			if !errors.Is(got, tc.err) || (tc.err == nil) != (got == nil) {
				t.Fatalf("Start returned %v; want %v unchanged", got, tc.err)
			}
			if holder, updates := f.state(); holder != me || updates != 0 || f.gets != 0 {
				t.Fatalf("the Lease is held by %q after %d writes and %d reads; want it untouched", holder, updates, f.gets)
			}
			if len(l.lines) != 0 {
				t.Errorf("logged %v; want nothing", l.lines)
			}
		})
	}
}

// A stop that Start reports as clean (nil after the cancel) but whose
// elector's last renew write failed may hide a lost leadership with a
// runnable still running (review of PR #215: B1): nothing is released, and
// the Lease is not even read.
func TestAStopAfterAFailedRenewReleasesNothing(t *testing.T) {
	f := &fakeLease{holder: me}
	l, err := runStopped(t, f, true, false, stopsWhenCancelled)
	if err != nil {
		t.Fatalf("Start returned %v; want its own nil", err)
	}
	if holder, updates := f.state(); holder != me || updates != 0 || f.gets != 0 {
		t.Fatalf("the Lease is held by %q after %d writes and %d reads; want it untouched", holder, updates, f.gets)
	}
	if len(l.lines) != 0 {
		t.Errorf("logged %v; want nothing", l.lines)
	}
}

// Renewals keeps the outcome of the last write that named this holder: an
// acquire's or a renew's. client-go's release record (no holder), another
// holder's record and a write cut short by a cancelled context leave it as
// it was; a write that ran out of time counts as failed.
func TestRenewalsKeepTheLastWriteThatNamesThisHolder(t *testing.T) {
	f := &fakeLease{}
	r := TrackRenewals(f)
	if r.ledToTheEnd() {
		t.Fatal("a lock with no write reports a leadership")
	}
	ctx := context.Background()
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	expired, cancel2 := context.WithDeadline(ctx, time.Now().Add(-time.Second))
	defer cancel2()
	other := resourcelock.LeaderElectionRecord{HolderIdentity: "host_2222", LeaseDurationSeconds: 15}
	for i, step := range []struct {
		what    string
		ctx     context.Context
		rec     resourcelock.LeaderElectionRecord
		create  bool
		failing int
		want    bool
	}{
		{"an acquire that creates the Lease", ctx, renew, true, 0, true},
		{"a renew that conflicts", ctx, renew, false, 1, false},
		{"a renew that lands", ctx, renew, false, 0, true},
		{"a renew cut short by the stop", cancelled, renew, false, 1, true},
		{"a release that fails", ctx, release, false, 1, true},
		{"another holder's record that fails", ctx, other, false, 1, true},
		{"a renew whose poll ran out of time", expired, renew, false, 1, false},
		{"a release that lands", ctx, release, false, 0, false},
		{"another holder's record that lands", ctx, other, false, 0, false},
		{"a renew that lands again", ctx, renew, false, 0, true},
	} {
		f.mu.Lock()
		f.failing = step.failing
		f.mu.Unlock()
		if step.create {
			if err := r.Create(step.ctx, step.rec); err != nil {
				t.Fatalf("step %d, %s: Create returned %v", i, step.what, err)
			}
		} else {
			_ = r.Update(step.ctx, step.rec)
		}
		if got := r.ledToTheEnd(); got != step.want {
			t.Fatalf("step %d, %s: the leadership reads %v, want %v", i, step.what, got, step.want)
		}
	}
}

// The stop's one ReleaseBudget is shared (review of PR #215: B2). When the
// elector's release landed, nothing follows it after Start, not even a read.
func TestNoReleaseAfterStartFollowsAnElectorReleaseThatLanded(t *testing.T) {
	f := &fakeLease{holder: me}
	l, err := runStoppedAfter(t, f, true, true, func(r *Renewals) {
		if err := r.Update(context.Background(), release); err != nil {
			t.Fatalf("the elector's release returned %v", err)
		}
	}, stopsWhenCancelled)
	if err != nil {
		t.Fatalf("Start returned %v", err)
	}
	if holder, updates := f.state(); holder != "" || updates != 1 || f.gets != 1 {
		t.Fatalf("the Lease is held by %q after %d writes and %d reads; want the elector's one read and write, nothing after Start", holder, updates, f.gets)
	}
	if len(l.lines) != 0 {
		t.Errorf("logged %v; want nothing", l.lines)
	}
}

// An elector's release whose retries spent the shared budget leaves nothing
// to the release after Start: it is skipped, and the skip is logged with the
// time left, which is negative.
func TestTheReleaseAfterStartIsSkippedOnceTheBudgetIsSpent(t *testing.T) {
	f := &fakeLease{holder: me}
	l, err := runStoppedAfter(t, f, true, true, func(r *Renewals) {
		r.releaseAt, r.released = time.Now().Add(-ReleaseBudget-500*time.Millisecond), false
	}, stopsWhenCancelled)
	if err != nil {
		t.Fatalf("Start returned %v", err)
	}
	if holder, updates := f.state(); holder != me || updates != 0 || f.gets != 0 {
		t.Fatalf("the Lease is held by %q after %d writes and %d reads; want it untouched", holder, updates, f.gets)
	}
	if !l.has("the shared release budget is spent") || !l.has(`"left"="-5`) {
		t.Errorf("the skip and the time left (about -500ms) were not logged: %v", l.lines)
	}
}

// The release after Start gets what the elector's release left of the shared
// budget, counted from its write, never a fresh one: with the elector's
// release 1 s in, the read's deadline is the write's start plus
// ReleaseBudget, not 2 s from now.
func TestTheReleaseAfterStartGetsWhatIsLeft(t *testing.T) {
	f := &fakeLease{holder: me}
	var began time.Time
	if _, err := runStoppedAfter(t, f, true, true, func(r *Renewals) {
		began = time.Now().Add(-time.Second)
		r.releaseAt, r.released = began, false
	}, stopsWhenCancelled); err != nil {
		t.Fatalf("Start returned %v", err)
	}
	if holder, _ := f.state(); holder != "" {
		t.Fatalf("the Lease is held by %q; want it released within what was left", holder)
	}
	if want, got := began.Add(ReleaseBudget), f.readBy; got.Before(want) || got.After(want.Add(500*time.Millisecond)) {
		t.Fatalf("the release after Start read with a deadline %s after the elector's release write; want %s, what was left, not a fresh %s", got.Sub(began), ReleaseBudget, ReleaseBudget)
	}
}

// An elector whose release never reached Update (its read failed) used none
// of the budget: the release after Start gets all of it.
func TestTheReleaseAfterStartGetsTheWholeBudgetWhenTheElectorNeverWrote(t *testing.T) {
	f := &fakeLease{holder: me}
	before := time.Now()
	if _, err := runStopped(t, f, true, true, stopsWhenCancelled); err != nil {
		t.Fatalf("Start returned %v", err)
	}
	if got := f.readBy.Sub(before); got < ReleaseBudget || got > ReleaseBudget+500*time.Millisecond {
		t.Fatalf("the release after Start read with a deadline %s after the stop; want the whole %s", got, ReleaseBudget)
	}
}

// Renewals records the first release record's start and outcome: a release
// that lands needs no release after Start; one that fails leaves what is left.
func TestRenewalsRecordTheElectorsRelease(t *testing.T) {
	for _, tc := range []struct {
		name    string
		failing int
		needed  bool
	}{
		{"a release that lands", 0, false},
		{"a release that fails", 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := TrackRenewals(&fakeLease{holder: me, failing: tc.failing})
			if left, needed := r.budgetLeft(time.Now()); !needed || left != ReleaseBudget {
				t.Fatalf("before any release: %s left, needed %v; want all of %s, needed", left, needed, ReleaseBudget)
			}
			_ = r.Update(context.Background(), release)
			left, needed := r.budgetLeft(time.Now())
			if needed != tc.needed || (needed && (left <= 0 || left > ReleaseBudget)) {
				t.Fatalf("%s left, needed %v; want needed %v with part of %s left", left, needed, tc.needed, ReleaseBudget)
			}
		})
	}
}
