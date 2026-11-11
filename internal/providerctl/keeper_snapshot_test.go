package providerctl

// One snapshot of the keeper per status pass (the delta review of PR 360's
// Stage B): a pass reads the claim and the keeper's reachability together,
// and records that very state as what it read, so neither a recovery nor an
// expiry between two reads can make Ready wrong. Each test changes the
// keeper's state from a hook the status pass runs right after its snapshot,
// with no keeper client running, so nothing else moves the state, and the
// controller's clock moves only by the test's hand.
//
// Mutants (keeper mode's compute, providerstatus.go): the reachability read
// again in compute, recording it (l.s.keeperSnapshot().unavailable, two
// separate reads), or without recording it (l.s.keeperUnavailable(now), the
// pass's record from an earlier read than compute uses): the first test fails
// for both, the second for the latter.

import (
	"context"
	"sync"
	"testing"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// keeperAt puts the keeper client's state under a lead whose last successful
// exchange was at okAt, with the claim granted, as a client would have it.
func keeperAt(e *env, okAt time.Time) *keeperState {
	k := e.ctl.shared.relays.keeper
	// A reply the test hands the client arms its deadline timer; no lead's
	// end turns it off here, so the test does.
	e.t.Cleanup(k.stopDeadline)
	k.mu.Lock()
	defer k.mu.Unlock()
	k.gen, k.okGen, k.okAt, k.answered, k.inUse, k.validity = 1, 1, okAt, true, "", 40*time.Second
	return k
}

// An outage, then, inside one status pass, a recovery whose reply refuses the
// login host: the pass's snapshot read the keeper unreachable with the claim
// still granted, and the recovery comes after it. Ready is written False, with
// RelayKeeperUnavailable, never True; the next pass reads the refusal.
func TestKeeperStatusPassUsesOneSnapshotOfTheKeeper(t *testing.T) {
	k0 := startFakeKeeper(t)
	e := newEnv(t, keeperMode(k0, 5))
	k := keeperAt(e, e.clock.Now().Add(-time.Minute)) // past the 40 s validity
	e.ctl.status.keeperRead = func() {
		k.received(1, wire.Report{}, &wire.RefusedError{Refused: wire.Refused{Reason: wire.RefusedLoginHost, LoginHost: fakeLoginHost, Holder: "older-provider"}}, e.ctl.shared)
	}
	if err := e.ctl.SyncStatus(e.ctx); err != nil {
		t.Fatal(err)
	}
	if r := providerReady(t, e); r.Status != "False" || r.Reason != v1alpha1.ReasonRelayKeeperUnavailable {
		t.Errorf("the pass whose snapshot read the outage wrote Ready %+v; want False/RelayKeeperUnavailable, never True", r)
	}
	e.ctl.status.keeperRead = nil
	if err := e.ctl.SyncStatus(e.ctx); err != nil {
		t.Fatal(err)
	}
	if r := providerReady(t, e); r.Status != "False" || r.Reason != v1alpha1.ReasonLoginHostInUse {
		t.Errorf("the next pass wrote Ready %+v; want False/LoginHostInUse, the refusal it reads", r)
	}
}

// The validity's deadline crossed right after a pass's snapshot, then a
// recovery: the pass used the snapshot it recorded (reachable), so it left
// Ready True, and the recovery, which leaves the state it recorded, needs no
// wake-up. The test waits for the pass to return, then hands the client the
// recovery, then reads Ready at once: True, with no further pass (the status
// loop's ticker is 10 min).
func TestKeeperStatusPassRecordsTheStateItUsed(t *testing.T) {
	k0 := startFakeKeeper(t)
	e := newEnv(t, keeperMode(k0, 5), func(e *env) { e.opts.PollInterval = 10 * time.Minute })
	k := keeperAt(e, e.clock.Now())
	l := e.ctl.status
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = l.Start(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	waitRelays(t, "Ready True with the keeper reachable", func() bool { return readyIs(t, e, "True") })

	passed := make(chan struct{})
	var once sync.Once
	l.mu.Lock()
	l.keeperRead = func() { e.clock.Advance(41 * time.Second) } // past the validity, after the snapshot
	l.keeperPassed = func() { once.Do(func() { close(passed) }) }
	l.mu.Unlock()
	wake(k.status)
	select {
	case <-passed:
	case <-time.After(30 * time.Second):
		t.Fatal("the status loop made no pass on the wake-up")
	}
	l.mu.Lock()
	l.keeperRead, l.keeperPassed = nil, nil
	l.mu.Unlock()
	// The recovery: a reply at the advanced time, which the client records,
	// then the client's own check for a wake-up.
	k.received(1, wire.Report{LoginHost: fakeLoginHost}, nil, e.ctl.shared)
	k.noteStatus()
	if !readyIs(t, e, "True") {
		t.Errorf("Ready after the pass and the recovery: %+v; want True, the state the pass used and the state now", providerReady(t, e))
	}
}

// hookClock is a clock that runs a hook, once, on its next read after it is
// armed, returning the time it read before the hook ran.
type hookClock struct {
	base *fake.Clock
	mu   sync.Mutex
	hook func()
}

func (c *hookClock) Now() time.Time {
	t := c.base.Now()
	c.mu.Lock()
	h := c.hook
	c.hook = nil
	c.mu.Unlock()
	if h != nil {
		h()
	}
	return t
}

func (c *hookClock) arm(h func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.hook = h
}

// The deadline passed between a pass's read of the clock and its snapshot:
// the pass reads the clock inside the snapshot's critical section, so the
// deadline's check, which takes that section too, runs either before the
// snapshot, which then reads the keeper expired, or after it, which then sees
// the pass's record differ and wakes the loop again. Either way Ready does not
// stay True past the deadline without a poll (the snapshot delta of PR 360).
// The pass's first read of the controller's clock moves the clock past the
// deadline and runs the deadline's check: at once when the critical section
// is free, as it is when the clock is read before it, else as soon as the
// pass leaves it.
func TestKeeperStatusPassReadsTheClockInsideItsSnapshot(t *testing.T) {
	k0 := startFakeKeeper(t)
	var hc *hookClock
	e := newEnv(t, keeperMode(k0, 5), func(e *env) {
		hc = &hookClock{base: e.clock}
		e.opts.Clock = hc
		e.opts.PollInterval = 10 * time.Minute
	})
	k := keeperAt(e, e.clock.Now())
	l := e.ctl.status
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = l.Start(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	waitRelays(t, "Ready True with the keeper reachable", func() bool { return readyIs(t, e, "True") })

	// The pass records the keeper expired, with the claim granted.
	e.clock.Advance(41 * time.Second)
	wake(k.status)
	waitRelays(t, "Ready False once the validity passed", func() bool { return readyIs(t, e, "False") })

	// A recovery, which the client records without a wake-up of its own; the
	// test then arms the clock and wakes the loop itself.
	k.received(1, wire.Report{LoginHost: fakeLoginHost}, nil, e.ctl.shared)
	hc.arm(func() {
		e.clock.Advance(41 * time.Second) // past the recovery's deadline
		if k.mu.TryLock() {
			k.mu.Unlock()
			k.atDeadline()
		} else {
			go k.atDeadline()
		}
	})
	passed := make(chan struct{})
	var once sync.Once
	l.mu.Lock()
	l.keeperPassed = func() { once.Do(func() { close(passed) }) }
	l.mu.Unlock()
	wake(k.status)
	select {
	case <-passed:
	case <-time.After(30 * time.Second):
		t.Fatal("the status loop made no pass on the wake-up")
	}
	l.mu.Lock()
	l.keeperPassed = nil
	l.mu.Unlock()
	// That pass read the clock before the deadline and the keeper recovered,
	// so it wrote Ready True; the deadline has passed since, so Ready must
	// turn False again with no poll.
	waitRelays(t, "Ready False past the recovery's deadline, with no poll", func() bool { return readyIs(t, e, "False") })
}

// readyIs reports whether the Provider's Ready condition has status, false
// while it has none.
func readyIs(t *testing.T, e *env, status string) bool {
	t.Helper()
	var prov v1alpha1.Provider
	if err := e.c.Get(e.ctx, client.ObjectKey{Name: testProvider}, &prov); err != nil {
		t.Fatal(err)
	}
	c := apimeta.FindStatusCondition(prov.Status.Conditions, v1alpha1.ConditionProviderReady)
	return c != nil && string(c.Status) == status
}
