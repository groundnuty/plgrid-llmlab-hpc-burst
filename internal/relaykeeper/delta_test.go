package relaykeeper

// New for the focused delta review of PR 354: a removal of protection takes
// effect only once synced, starts are bound to their Want's host and owner,
// a running relay is counted for the Provider UID it started under, and ping
// and status surface a state file that does not sync.

import (
	"context"
	"errors"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// failDirSync makes the state file's directory sync fail while *fail is set.
func failDirSync(t *testing.T, fail *bool) {
	t.Helper()
	saved := fsops.syncDir
	fsops.syncDir = func(d *os.File) error {
		if *fail {
			return errors.New("an injected EIO")
		}
		return saved(d)
	}
	t.Cleanup(func() { fsops.syncDir = saved })
}

// A complete set that omits an Allocation whose end the keeper holds: the
// write that removes the end renames but does not sync. The removal of that
// protection does not take effect: its relays are dropped first, and their
// starts refused, though the set itself is refused for the state file; the
// end stays in memory, so the seeds the Runner reads still name it; and only
// once a write syncs, and the removal with it, does the Allocation lose the
// end. Then a set that names it again starts it.
func TestAnUnsyncedRemovalNeverExposesTheAllocation(t *testing.T) {
	r := newRig(t)
	k, fr := r.open()
	x, y := entry(1), entry(2)
	accepted(t, k, set("example-a", 1, true, "login.example", x, y))
	fr.mustStart(t, "example-a", x.Tag)
	wx, _ := fr.want("example-a", x.Tag)
	wx.OnFinal(agentapi.RelayServing, hostdial.EndFailed, stepLimit, k.Epoch(), fr.seqOf("example-a", x.Tag, agentapi.RelayServing))
	fr.log()
	fail := true
	failDirSync(t, &fail)
	if got := refused(t, k, wire.KindSet, set("example-a", 1, true, "login.example", y)); got.Reason != wire.RefusedState {
		t.Fatalf("the set whose removal did not sync: %+v, want refused for the state file", got)
	}
	if calls := fr.log(); !slices.Contains(calls, "drop example-a "+x.Tag) {
		t.Errorf("the omitted Allocation's relays were not dropped before its end's removal: %q", calls)
	}
	if jobs, err := wx.Jobs.ListByTag(context.Background(), x.Tag); err != nil || len(jobs) != 0 {
		t.Errorf("the omitted Allocation's listing after the unsynced removal: %+v, %v; want no job", jobs, err)
	}
	if got := wx.Finals(); got[agentapi.RelayServing] == "" {
		t.Errorf("the seeds after the unsynced removal: %q, want the end still held in memory", got)
	}
	fail = false
	accepted(t, k, set("example-a", 1, true, "login.example", y))
	if got := wx.Finals(); len(got) != 0 || len(r.disk().Ends) != 0 {
		t.Errorf("after the removal synced: seeds %q, ends on disk %+v; want none", got, r.disk().Ends)
	}
	accepted(t, k, set("example-a", 1, true, "login.example", x, y))
	fr.mustStart(t, "example-a", x.Tag)
}

// A start is bound to its Want's host and owner: a relay on a host its
// Provider has left never starts there again, though the claim there has not
// lapsed yet, and once another Provider claims the host it can't either.
// Here A's relay on login-1 retries with no process when A's move to login-2
// is accepted, its replacement not started yet; C then claims login-1; and
// the old relay's start, which reads its own Want's Jobs, is refused at each
// step (the delta review of PR 354).
func TestAStartIsBoundToItsWantsHostAndOwner(t *testing.T) {
	r := newRig(t)
	k, fr := r.open()
	e := entry(1)
	accepted(t, k, set("example-a", 1, true, "login-1.example", e))
	fr.mustStart(t, "example-a", e.Tag)
	fr.stop("example-a", e.Tag) // it retries, with no process
	w1, _ := fr.want("example-a", e.Tag)
	accepted(t, k, set("example-a", 1, true, "login-2.example", e))
	if jobs, _ := w1.Jobs.ListByTag(context.Background(), e.Tag); len(jobs) != 0 {
		t.Errorf("the relay on login-1 after its Provider moved to login-2: listed %+v, want no start", jobs)
	}
	accepted(t, k, set("example-c", 1, true, "login-1.example", entry(3)))
	if jobs, _ := w1.Jobs.ListByTag(context.Background(), e.Tag); len(jobs) != 0 {
		t.Errorf("the relay on login-1 after example-c claimed it: listed %+v, want no start", jobs)
	}
	fr.mustStart(t, "example-a", e.Tag) // the replacement, on login-2, starts
}

// A Want's Jobs refuse a start for each of the guards they hold, one at a
// time: the claim on its host held by another Provider, the Want no longer
// the Allocation's, another Provider UID's set, and a set naming another
// host. Each is one the others would not catch alone.
func TestAWantsJobsRefuseEachUnheldGuard(t *testing.T) {
	for name, perturb := range map[string]func(k *Keeper){
		"the claim another's": func(k *Keeper) {
			c := k.st.Claims["login.example"]
			c.Provider = "example-c"
			k.st.Claims["login.example"] = c
		},
		"the claim another UID's": func(k *Keeper) {
			c := k.st.Claims["login.example"]
			c.ProviderUID = "ff"
			k.st.Claims["login.example"] = c
		},
		"no longer wanted": func(k *Keeper) { delete(k.wanted["example-a"], entry(1).Tag) },
		"wanted under another UID": func(k *Keeper) {
			w := k.wanted["example-a"][entry(1).Tag]
			w.uid = "ff"
			k.wanted["example-a"][entry(1).Tag] = w
		},
		"wanted on another host": func(k *Keeper) {
			w := k.wanted["example-a"][entry(1).Tag]
			w.host = "login-2.example"
			k.wanted["example-a"][entry(1).Tag] = w
		},
		"a set of another UID":  func(k *Keeper) { k.sets["example-a"].set.ProviderUID = "ff" },
		"a set on another host": func(k *Keeper) { k.sets["example-a"].host = "login-2.example" },
	} {
		t.Run(name, func(t *testing.T) {
			r := newRig(t)
			k, fr := r.open()
			accepted(t, k, set("example-a", 1, true, "login.example", entry(1)))
			w, _ := fr.want("example-a", entry(1).Tag)
			if jobs, _ := w.Jobs.ListByTag(context.Background(), entry(1).Tag); len(jobs) != 1 {
				t.Fatalf("the unperturbed Want's listing: %+v, want its job", jobs)
			}
			k.mu.Lock()
			perturb(k)
			k.mu.Unlock()
			if jobs, _ := w.Jobs.ListByTag(context.Background(), entry(1).Tag); len(jobs) != 0 {
				t.Errorf("with %s: listed %+v, want no start", name, jobs)
			}
		})
	}
}

// A running relay is counted for the Provider UID it started under, never for
// the latest Want of its tag (the delta review of PR 354): P under UID 1 holds
// login-1 with a relay that runs on after its sets expired; P re-created
// under UID 2 asks for the same Allocation on login-2, whose replacement does
// not stop the old process at once; so another Provider is still refused on
// login-1 until the old process is gone.
func TestARunningRelayCountsForTheUIDItStartedUnder(t *testing.T) {
	r := newRig(t)
	k, fr := r.open()
	e := entry(1)
	accepted(t, k, set("example-b", 1, true, "login-1.example", e))
	fr.mustStart(t, "example-b", e.Tag)
	r.clock.advance(40*time.Second + 6*time.Minute)
	accepted(t, k, recreated(set("example-b", 2, true, "login-2.example", e)))
	if got := refused(t, k, wire.KindSet, set("example-a", 1, true, "login-1.example", entry(2))); got.Holder != "example-b" {
		t.Errorf("while UID 1's process still runs on login-1: %+v, want refused naming example-b", got)
	}
	fr.land("example-b", e.Tag)
	accepted(t, k, set("example-a", 1, true, "login-1.example", entry(2)))
}

// Ping and status surface a state file that does not sync: while a write's
// directory sync fails, and while an end is pending, both are refused for
// the state file, so the launcher's health check fails; once a write syncs,
// both answer again (the delta review of PR 354).
func TestPingAndStatusSurfaceAStateFileThatDoesNotSync(t *testing.T) {
	r := newRig(t)
	k, fr := r.open()
	accepted(t, k, set("example-a", 1, true, "login.example", entry(1)))
	fail := true
	failDirSync(t, &fail)
	refused(t, k, wire.KindSet, set("example-a", 2, true, "login.example", entry(1)))
	for _, kind := range []wire.Kind{wire.KindPing, wire.KindStatus} {
		if got := refused(t, k, kind, struct{}{}); got.Reason != wire.RefusedState {
			t.Errorf("a %s while no write syncs: %+v, want refused for the state file", kind, got)
		}
	}
	fail = false
	for _, kind := range []wire.Kind{wire.KindPing, wire.KindStatus} {
		if got, reply := send(t, k, kind, struct{}{}); got == wire.KindRefused {
			t.Errorf("a %s after a write synced: %+v", kind, reply)
		}
	}
	w, _ := fr.want("example-a", entry(1).Tag)
	undo := r.readOnly(t)
	w.OnFinal(agentapi.RelayServing, hostdial.EndFailed, stepLimit, k.Epoch(), 2)
	if got := refused(t, k, wire.KindPing, struct{}{}); got.Reason != wire.RefusedState {
		t.Errorf("a ping while an end is pending: %+v, want refused for the state file", got)
	}
	undo()
	if got, reply := send(t, k, wire.KindPing, struct{}{}); got != wire.KindPong {
		t.Errorf("a ping once the pending end was written: %s %+v", got, reply)
	}
}

// A start the gate allowed holds its host until it resolves (the last
// delta review of PR 354, with the hold at the gate since PR 3), in Astra's
// interleaving: A's relay on login-1 retries with no process, and its next
// start passes the gate; before the process starts, A's move to login-2 is accepted, and C asks for login-1. C is
// refused while A's start is in flight. When the start fails, the host is
// released, and C claims it. When it succeeds instead, A's process runs on
// login-1, so C is still refused until that process is gone.
func TestAnAuthorizedStartHoldsItsHostUntilItResolves(t *testing.T) {
	for _, started := range []bool{false, true} {
		name := "the start fails"
		if started {
			name = "the start succeeds"
		}
		t.Run(name, func(t *testing.T) {
			r := newRig(t)
			k, fr := r.open()
			e := entry(1)
			accepted(t, k, set("example-a", 1, true, "login-1.example", e))
			fr.mustStart(t, "example-a", e.Tag)
			fr.stop("example-a", e.Tag) // it retries, with no process
			in := fr.authorize("example-a", e.Tag, agentapi.RelayControl)
			if in == nil {
				t.Fatal("the retry on login-1 was refused its start")
			}
			accepted(t, k, set("example-a", 1, true, "login-2.example", e))
			if got := refused(t, k, wire.KindSet, set("example-c", 1, true, "login-1.example", entry(3))); got.Holder != "example-a" {
				t.Fatalf("C while A's start on login-1 is in flight: %+v, want refused naming example-a", got)
			}
			if !started {
				fr.launchFails(in, agentapi.RelayControl)
				accepted(t, k, set("example-c", 1, true, "login-1.example", entry(3)))
				return
			}
			fr.launch(in, agentapi.RelayControl)
			if got := refused(t, k, wire.KindSet, set("example-c", 1, true, "login-1.example", entry(3))); got.Holder != "example-a" {
				t.Errorf("C while A's started process runs on login-1: %+v, want refused naming example-a", got)
			}
			fr.land("example-a", e.Tag)
			accepted(t, k, set("example-c", 1, true, "login-1.example", entry(3)))
		})
	}
}

// The holds are per channel: with both channels' starts in flight, one
// resolved still holds the host, and the host is free once both are. A
// second resolution of the control channel's start, while the serving
// channel's is in flight, changes nothing: it cannot take serving's hold, so
// the host stays held until serving resolves (the Runner's duty 2; under
// PR 2's count per relay it freed the host early).
func TestTheHoldsArePerChannel(t *testing.T) {
	r := newRig(t)
	k, fr := r.open()
	e := entry(1)
	accepted(t, k, set("example-a", 1, true, "login-1.example", e))
	control := fr.authorize("example-a", e.Tag, agentapi.RelayControl)
	serving := fr.authorize("example-a", e.Tag, agentapi.RelayServing)
	if control == nil || serving == nil {
		t.Fatal("a start was refused")
	}
	accepted(t, k, set("example-a", 1, true, "login-2.example", e))
	fr.launchFails(control, agentapi.RelayControl)
	fr.launchFails(control, agentapi.RelayControl) // a duplicate: serving's hold stays
	if got := refused(t, k, wire.KindSet, set("example-c", 1, true, "login-1.example", entry(3))); got.Holder != "example-a" {
		t.Errorf("C while A's serving start is in flight, after a duplicate control resolution: %+v, want refused naming example-a", got)
	}
	fr.launchFails(serving, agentapi.RelayServing)
	accepted(t, k, set("example-c", 1, true, "login-1.example", entry(3)))
}

// A start in flight holds only its own host: A's start on login-2, the host
// it moved to, does not keep login-1, which its move let go.
func TestAStartInFlightHoldsOnlyItsOwnHost(t *testing.T) {
	r := newRig(t)
	k, fr := r.open()
	e := entry(1)
	accepted(t, k, set("example-a", 1, true, "login-1.example", e))
	fr.mustStart(t, "example-a", e.Tag)
	fr.stop("example-a", e.Tag)
	fr.land("example-a", e.Tag)
	accepted(t, k, set("example-a", 1, true, "login-2.example", e))
	in := fr.authorize("example-a", e.Tag, agentapi.RelayControl)
	if in == nil {
		t.Fatal("the start on login-2 was refused")
	}
	accepted(t, k, set("example-c", 1, true, "login-1.example", entry(3)))
	fr.launchFails(in, agentapi.RelayControl)
}

// The hold is fail-closed by design, and this pins the Runner's duty to
// resolve every authorization (the focused delta of PR 354, its option b): a
// deadline would be unsafe, since once a hold expired another Provider could
// claim the host, and the authorized start, suspended, could still launch
// there. So an authorization left unresolved holds its host past the
// authorizing set's expiry and the claim's lapse, through each path that
// takes the relay away from it, and only its resolution, through the Want
// it was given, with that Want's Provider UID and host, frees the host:
//   - the Allocation dropped by its Provider's complete set (the Runner's
//     Drop cancels the attempt);
//   - the Provider moved to another host, the Allocation replaced there by a
//     new generation;
//   - the Provider re-created under another UID, which wants the Allocation
//     on another host.
func TestAnUnresolvedStartHoldsItsHostPastEveryLapse(t *testing.T) {
	for _, c := range []struct {
		name string
		away func(t *testing.T, k *Keeper, e wire.Entry)
	}{
		{"the Allocation dropped", func(t *testing.T, k *Keeper, e wire.Entry) {
			accepted(t, k, set("example-a", 1, true, "login-1.example"))
		}},
		{"the Provider moved, the Allocation replaced", func(t *testing.T, k *Keeper, e wire.Entry) {
			accepted(t, k, set("example-a", 1, true, "login-2.example", newGen(e)))
		}},
		{"the Provider re-created under another UID", func(t *testing.T, k *Keeper, e wire.Entry) {
			accepted(t, k, recreated(set("example-a", 2, true, "login-2.example", e)))
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := newRig(t)
			k, fr := r.open()
			e := entry(1)
			accepted(t, k, set("example-a", 1, true, "login-1.example", e))
			in := fr.authorize("example-a", e.Tag, agentapi.RelayControl)
			if in == nil {
				t.Fatal("the start on login-1 was refused")
			}
			c.away(t, k, e)
			r.clock.advance(40*time.Second + DefaultClaimLapse + time.Hour) // past the set's expiry and the claim's lapse
			if got := refused(t, k, wire.KindSet, set("example-c", 1, true, "login-1.example", entry(3))); got.Holder != "example-a" {
				t.Fatalf("C an hour past every lapse, with A's start unresolved: %+v, want refused naming example-a", got)
			}
			fr.launchFails(in, agentapi.RelayControl) // the cancelled attempt, resolved through its own Want
			accepted(t, k, set("example-c", 1, true, "login-1.example", entry(3)))
		})
	}
}
