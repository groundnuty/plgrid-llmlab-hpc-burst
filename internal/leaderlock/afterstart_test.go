package leaderlock

import (
	"context"
	"errors"
	"testing"

	"github.com/go-logr/logr/funcr"
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

// runStopped runs the wrapper over a manager whose Start is start and the
// fake Lease f behind leaderlock's retrying lock, with Start's context
// cancelled first when cancelled is true, and returns the log and Start's
// result.
func runStopped(t *testing.T, f *fakeLease, cancelled bool, start func(context.Context) error) (*logs, error) {
	t.Helper()
	lock, l := newLock(f)
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
	l, err := runStopped(t, f, true, stopsWhenCancelled)
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
	if _, err := runStopped(t, f, true, stopsWhenCancelled); err != nil {
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
			l, err := runStopped(t, tc.f, true, stopsWhenCancelled)
			if err != nil {
				t.Fatalf("Start returned %v", err)
			}
			if _, updates := tc.f.state(); updates != 0 || tc.f.gets == 0 {
				t.Fatalf("%d writes after %d reads; want a read and no write", updates, tc.f.gets)
			}
			if l.has("released the leader Lease") || l.has("could not be released") {
				t.Errorf("logged a release or a failure for a Lease this holder does not hold: %v", l.lines)
			}
		})
	}
}

// Reads that keep failing give up when AfterStartBudget runs out: Start
// returns its own nil, the failure is logged, and the Lease stays held.
func TestTheReleaseAfterStartGivesUpWithinItsBudget(t *testing.T) {
	f := &fakeLease{holder: me, failGets: 1 << 20}
	l, err := runStopped(t, f, true, stopsWhenCancelled)
	if err != nil {
		t.Fatalf("Start returned %v; want its own nil", err)
	}
	if holder, updates := f.state(); holder != me || updates != 0 || f.gets < 2 {
		t.Fatalf("the Lease is held by %q after %d writes and %d reads; want it still held, the read retried and nothing written", holder, updates, f.gets)
	}
	if !l.has("could not be released after the manager stopped") {
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
			l, got := runStopped(t, f, tc.cancelled, func(context.Context) error { return tc.err })
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
