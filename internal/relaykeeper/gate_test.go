package relaykeeper

// New for PR 3: the gate (Keeper.beforeStart, Want.BeforeStart), where a
// start's hold is taken since the science agent's ruling B ("checking
// authorization at launch time is better than holding from the listing").
// The listing holds nothing; the gate re-checks every condition and takes
// the hold, per channel, in one critical section under the keeper's lock.

import (
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// noHolds fails the test unless no start of any Provider is in flight.
func noHolds(t *testing.T, k *Keeper) {
	t.Helper()
	k.mu.Lock()
	defer k.mu.Unlock()
	if len(k.inflight) != 0 {
		t.Errorf("starts in flight: %+v, want none", k.inflight)
	}
}

// A start that never reaches the gate holds nothing: the listing passed,
// and the attempt ended before the gate (a relay line or an ssh command that
// failed, in hostdial). A's move then lets another Provider claim the host
// at once.
func TestAStartThatNeverReachesTheGateHoldsNothing(t *testing.T) {
	r := newRig(t)
	k, fr := r.open()
	e := entry(1)
	accepted(t, k, set("example-a", 1, true, "login-1.example", e))
	if !fr.listedOnly("example-a", e.Tag) {
		t.Fatal("the listing refused the start")
	}
	noHolds(t, k)
	accepted(t, k, set("example-a", 1, true, "login-2.example", e))
	accepted(t, k, set("example-c", 1, true, "login-1.example", entry(3)))
}

// The gate re-checks, in one critical section, every condition the listing
// checked, and takes no hold when one no longer holds: each case changes one
// condition between the listing and the gate, as a set, a clock tick or an
// end the keeper takes in that window would. The positive control changes
// nothing, and the gate allows the start and takes its hold.
func TestTheGateReChecksEveryConditionInOneStep(t *testing.T) {
	for _, c := range []struct {
		name   string
		change func(t *testing.T, r *rig, k *Keeper, fr *fakeRunner, e wire.Entry)
		allow  bool
	}{
		{"nothing changed", func(*testing.T, *rig, *Keeper, *fakeRunner, wire.Entry) {}, true},
		{"the set expired", func(t *testing.T, r *rig, k *Keeper, fr *fakeRunner, e wire.Entry) {
			r.clock.advance(41 * time.Second)
		}, false},
		{"the Want replaced by a new generation", func(t *testing.T, r *rig, k *Keeper, fr *fakeRunner, e wire.Entry) {
			accepted(t, k, set("example-a", 1, true, "login.example", newGen(e)))
		}, false},
		{"the Provider moved", func(t *testing.T, r *rig, k *Keeper, fr *fakeRunner, e wire.Entry) {
			accepted(t, k, set("example-a", 1, true, "login-2.example", e))
		}, false},
		{"the Allocation dropped", func(t *testing.T, r *rig, k *Keeper, fr *fakeRunner, e wire.Entry) {
			accepted(t, k, set("example-a", 1, true, "login.example"))
		}, false},
		{"the Allocation marked", func(t *testing.T, r *rig, k *Keeper, fr *fakeRunner, e wire.Entry) {
			m := e
			m.EndedForGood = true
			accepted(t, k, set("example-a", 1, true, "login.example", m))
		}, false},
		{"a final end held for the Allocation", func(t *testing.T, r *rig, k *Keeper, fr *fakeRunner, e wire.Entry) {
			w, _ := fr.want("example-a", e.Tag)
			w.OnFinal(agentapi.RelayServing, hostdial.EndFailed, stepLimit, k.Epoch(), 1)
		}, false},
		{"the claim another Provider's", func(t *testing.T, r *rig, k *Keeper, fr *fakeRunner, e wire.Entry) {
			k.mu.Lock()
			c := k.st.Claims["login.example"]
			c.Provider = "example-c"
			k.st.Claims["login.example"] = c
			k.mu.Unlock()
		}, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := newRig(t)
			k, fr := r.open()
			e := entry(1)
			accepted(t, k, set("example-a", 1, true, "login.example", e))
			in := fr.current("example-a", e.Tag)
			if !fr.lists(in) {
				t.Fatal("the listing refused the start")
			}
			c.change(t, r, k, fr, e)
			if got := fr.gate(in, agentapi.RelayControl); got != c.allow {
				t.Fatalf("the gate answered %v, want %v", got, c.allow)
			}
			if !c.allow {
				noHolds(t, k)
				return
			}
			k.mu.Lock()
			n := len(k.inflight)
			k.mu.Unlock()
			if n != 1 {
				t.Errorf("starts in flight after the gate allowed one: %d, want 1", n)
			}
			fr.launchFails(in, agentapi.RelayControl)
			noHolds(t, k)
		})
	}
}

// The gate keeps HBEP-4's 4 relays per login host: with two Allocations'
// four relays up on the host, a third Allocation's starts are refused and
// hold nothing; once one Allocation's relays are gone, the third starts.
func TestTheGateKeepsFourRelaysPerLoginHost(t *testing.T) {
	r := newRig(t)
	k, fr := r.open()
	accepted(t, k, set("example-a", 1, true, "login.example", entry(1), entry(2), entry(3)))
	fr.mustStart(t, "example-a", entry(1).Tag)
	fr.mustStart(t, "example-a", entry(2).Tag)
	if fr.start("example-a", entry(3).Tag) {
		t.Error("a fifth relay started on a login host with four up")
	}
	noHolds(t, k)
	accepted(t, k, set("example-a", 1, true, "login.example", entry(2), entry(3)))
	if fr.start("example-a", entry(3).Tag) {
		t.Error("the third Allocation started while the dropped one's processes still run")
	}
	fr.land("example-a", entry(1).Tag)
	fr.mustStart(t, "example-a", entry(3).Tag)
}

// A gate refusal is no start: nothing is resolved, no end is recorded or
// reported, and nothing is marked, however often it is refused; the next
// start the gate allows runs. Here the set has expired, and every attempt is
// refused until a fresh set comes.
func TestAGateRefusalIsNoStartAndMarksNothing(t *testing.T) {
	r := newRig(t)
	k, fr := r.open()
	e := entry(1)
	accepted(t, k, set("example-a", 1, true, "login.example", e))
	in := fr.current("example-a", e.Tag)
	r.clock.advance(41 * time.Second)
	for range 50 {
		for _, ch := range agentapi.RelayChannels {
			if fr.gate(in, ch) {
				t.Fatal("the gate allowed a start under an expired set")
			}
		}
	}
	noHolds(t, k)
	rep := accepted(t, k, set("example-a", 1, true, "login.example", e))
	if len(rep.Ends) != 0 || len(r.disk().Ends) != 0 {
		t.Errorf("after 100 refused starts: reported ends %+v, recorded %+v; want none", rep.Ends, r.disk().Ends)
	}
	fr.mustStart(t, "example-a", e.Tag)
}

// The gate's count takes starts in flight too: with one Allocation's two
// relays up and another's two starts allowed and not yet launched, a third
// Allocation's start is refused; once the two in flight fail, it starts.
func TestTheGatesCountTakesStartsInFlight(t *testing.T) {
	r := newRig(t)
	k, fr := r.open()
	accepted(t, k, set("example-a", 1, true, "login.example", entry(1), entry(2), entry(3)))
	fr.mustStart(t, "example-a", entry(1).Tag)
	control := fr.authorize("example-a", entry(2).Tag, agentapi.RelayControl)
	serving := fr.authorize("example-a", entry(2).Tag, agentapi.RelayServing)
	if control == nil || serving == nil {
		t.Fatal("the second Allocation's starts were refused")
	}
	if fr.start("example-a", entry(3).Tag) {
		t.Error("a fifth relay started with two up and two starting on the login host")
	}
	fr.launchFails(control, agentapi.RelayControl)
	fr.launchFails(serving, agentapi.RelayServing)
	fr.mustStart(t, "example-a", entry(3).Tag)
}
