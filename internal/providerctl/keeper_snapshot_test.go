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
	"testing"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
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
// recovery: the pass used the snapshot it recorded (reachable), so it wrote
// Ready True, and the recovery, which leaves the state it recorded, needs no
// wake-up. Ready is True throughout, with no poll (the status loop's ticker is
// 10 min).
func TestKeeperStatusPassRecordsTheStateItUsed(t *testing.T) {
	k0 := startFakeKeeper(t)
	e := newEnv(t, keeperMode(k0, 5), func(e *env) { e.opts.PollInterval = 10 * time.Minute })
	k := keeperAt(e, e.clock.Now())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = e.ctl.status.Start(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	waitRelays(t, "Ready True with the keeper reachable", func() bool { return readyIs(t, e, "True") })

	passed := make(chan struct{})
	e.ctl.status.mu.Lock()
	e.ctl.status.keeperRead = func() {
		e.clock.Advance(41 * time.Second) // past the validity, after the snapshot
		select {
		case <-passed:
		default:
			close(passed)
		}
	}
	e.ctl.status.mu.Unlock()
	wake(k.status)
	select {
	case <-passed:
	case <-time.After(30 * time.Second):
		t.Fatal("the status loop made no pass on the wake-up")
	}
	e.ctl.status.mu.Lock()
	e.ctl.status.keeperRead = nil
	e.ctl.status.mu.Unlock()
	// The recovery: a reply at the advanced time, which the client records,
	// then the client's own check for a wake-up.
	k.received(1, wire.Report{LoginHost: fakeLoginHost}, nil, e.ctl.shared)
	k.noteStatus()
	waitRelays(t, "Ready True after the recovery, without a poll", func() bool { return readyIs(t, e, "True") })
	if why := k.unavailable(e.clock.Now()); why != "" {
		t.Fatalf("the keeper reads unreachable after the recovery: %s", why)
	}
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
