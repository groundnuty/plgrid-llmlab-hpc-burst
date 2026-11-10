package relaykeeper

// New for issue #343 (PR 2): the login-host claims (claims.go; HBEP-6, The
// relay keeper, Capacity and the login host; the science agent's ruling on
// #343's Q2).

import (
	"slices"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
)

// A second Provider's set for a held login host is refused, naming the host
// and its holder, and counted: one with Allocations, and one with none, so
// its controller shows the host in use before it has an Allocation. The
// holder's sets go on being accepted.
func TestASecondProvidersSetForAHeldHostIsRefused(t *testing.T) {
	r := newRig(t)
	k, fr := r.open()
	accepted(t, k, set("example-b", 1, true, "login.example", entry(1)))
	fr.log()
	for _, s := range []wire.DesiredSet{
		set("example-c", 1, true, "LOGIN.example", entry(2)),
		set("example-c", 1, true, "login.example"),
	} {
		got := refused(t, k, wire.KindSet, s)
		if got.Reason != wire.RefusedLoginHost || got.Holder != "example-b" || got.LoginHost != "login.example" {
			t.Errorf("the second Provider's set with %d Allocations: %+v, want refused naming login.example and example-b", len(s.Allocations), got)
		}
	}
	if calls := fr.log(); len(calls) != 0 {
		t.Errorf("a refused set reached the Runner: %q", calls)
	}
	if st := statusOf(t, k, "example-c"); st.Refused[wire.RefusedLoginHost] != 2 {
		t.Errorf("refused counts %v, want 2 for login_host", st.Refused)
	}
	accepted(t, k, set("example-b", 1, true, "login.example", entry(1)))
	// An older Provider is refused too while the holder holds relays there.
	if got := refused(t, k, wire.KindSet, set("example-a", 1, true, "login.example", entry(3))); got.Holder != "example-b" {
		t.Errorf("the older Provider's set while example-b holds relays: %+v, want refused naming example-b", got)
	}
	if c := r.disk().Claims["login.example"]; c.Provider != "example-b" {
		t.Errorf("the claim on disk %+v, want example-b's", c)
	}
}

// The holder releases its claim with a complete set that has no Allocation;
// the host then goes to the next Provider that asks.
func TestTheHolderReleasesItsClaimWithACompleteEmptySet(t *testing.T) {
	r := newRig(t)
	k, _ := r.open()
	accepted(t, k, set("example-b", 1, true, "login.example", entry(1)))
	accepted(t, k, set("example-b", 1, false, "login.example"))
	if _, held := r.disk().Claims["login.example"]; !held {
		t.Error("an incomplete empty set released the claim")
	}
	accepted(t, k, set("example-b", 1, true, "login.example"))
	if c, held := r.disk().Claims["login.example"]; held {
		t.Errorf("the claim after the holder's complete empty set: %+v, want it released", c)
	}
	accepted(t, k, set("example-c", 1, true, "login.example", entry(2)))
	if c := r.disk().Claims["login.example"]; c.Provider != "example-c" {
		t.Errorf("the claim on disk %+v, want example-c's", c)
	}
}

// A holder's set that names another login host releases its claim on the
// first, and claims the other.
func TestAHolderThatMovesReleasesItsFirstHost(t *testing.T) {
	r := newRig(t)
	k, _ := r.open()
	accepted(t, k, set("example-b", 1, true, "login-1.example", entry(1)))
	accepted(t, k, set("example-b", 1, true, "login-2.example", entry(1)))
	claims := r.disk().Claims
	if _, held := claims["login-1.example"]; held || claims["login-2.example"].Provider != "example-b" || len(claims) != 1 {
		t.Errorf("the claims after example-b moved: %+v, want only login-2.example, example-b's", claims)
	}
}

// A persisted claim survives a keeper restart, where no relay is held any
// more: another Provider is refused until the claim lapses, 5 minutes after
// the keeper's start with no set from the holder. The holder, if it comes
// back first, keeps the host.
func TestAPersistedClaimSurvivesARestartUntilItLapses(t *testing.T) {
	r := newRig(t)
	k, _ := r.open()
	accepted(t, k, set("example-b", 1, true, "login.example", entry(1)))

	k2, _ := r.open()
	r.clock.advance(5*time.Minute - time.Millisecond)
	if got := refused(t, k2, wire.KindSet, set("example-c", 1, true, "login.example", entry(2))); got.Holder != "example-b" {
		t.Errorf("before the lapse: %+v, want refused naming example-b", got)
	}
	r.clock.advance(time.Millisecond)
	accepted(t, k2, set("example-c", 1, true, "login.example", entry(2)))
	if c := r.disk().Claims["login.example"]; c.Provider != "example-c" {
		t.Errorf("the claim after the lapse %+v, want example-c's", c)
	}

	r3 := newRig(t)
	k3, _ := r3.open()
	accepted(t, k3, set("example-b", 1, true, "login.example", entry(1)))
	k4, _ := r3.open()
	accepted(t, k4, set("example-b", 2, true, "login.example", entry(1)))
	if got := refused(t, k4, wire.KindSet, set("example-c", 1, true, "login.example", entry(2))); got.Holder != "example-b" {
		t.Errorf("after the holder came back first: %+v, want refused naming example-b", got)
	}
}

// A claim with no relay held lasts while its holder sends unexpired sets, and
// lapses ClaimLapse after its holder's last set expired, not after the
// keeper's start. Here the keeper restarted, so no relay is held, and the
// holder's only Allocation carries the mark, so none starts.
func TestAClaimLapsesAfterItsHoldersLastSetExpired(t *testing.T) {
	r := newRig(t)
	k, _ := r.open()
	accepted(t, k, set("example-b", 1, true, "login.example", entry(1)))
	k2, fr2 := r.open()
	marked := entry(1)
	marked.EndedForGood = true
	r.clock.advance(4 * time.Minute)
	accepted(t, k2, set("example-b", 2, true, "login.example", marked)) // expires 40 s later
	if calls := fr2.log(); len(calls) != 0 {
		t.Fatalf("the marked Allocation reached the Runner: %q", calls)
	}
	r.clock.advance(40*time.Second + 5*time.Minute - time.Millisecond)
	if got := refused(t, k2, wire.KindSet, set("example-c", 1, true, "login.example", entry(2))); got.Holder != "example-b" {
		t.Errorf("before the lapse, counted from the holder's last set: %+v, want refused naming example-b", got)
	}
	r.clock.advance(time.Millisecond)
	accepted(t, k2, set("example-c", 1, true, "login.example", entry(2)))
}

// For a host nobody holds, the oldest Provider wins, then by name, among the
// Providers whose latest unexpired sets with Allocations name it, refused
// ones included: one refused while a third held the host keeps its place.
func TestTheOldestWantingProviderWinsAFreedHost(t *testing.T) {
	r := newRig(t)
	k, _ := r.open()
	accepted(t, k, set("example-c", 1, true, "login.example", entry(1)))
	refused(t, k, wire.KindSet, set("example-a", 1, true, "login.example", entry(2)))
	accepted(t, k, set("example-c", 1, true, "login.example")) // the holder frees the host
	if got := refused(t, k, wire.KindSet, set("example-b", 1, true, "login.example", entry(3))); got.Holder != "example-a" {
		t.Errorf("a younger Provider after the host was freed: %+v, want refused naming the older example-a, which still asks", got)
	}
	accepted(t, k, set("example-a", 1, true, "login.example", entry(2)))
	if c := r.disk().Claims["login.example"]; c.Provider != "example-a" {
		t.Errorf("the claim %+v, want example-a's", c)
	}

	// A refused set whose time has run out keeps no place.
	r2 := newRig(t)
	k2, _ := r2.open()
	accepted(t, k2, set("example-c", 1, true, "login.example", entry(1)))
	refused(t, k2, wire.KindSet, set("example-a", 1, true, "login.example", entry(2)))
	r2.clock.advance(41 * time.Second)
	accepted(t, k2, set("example-c", 2, true, "login.example"))
	accepted(t, k2, set("example-b", 1, true, "login.example", entry(3)))
	if c := r2.disk().Claims["login.example"]; c.Provider != "example-b" {
		t.Errorf("the claim %+v, want example-b's: example-a's refused set had expired", c)
	}
}

// A claim lasts while its holder holds relays there, however long its leader
// is silent: the relays are kept with no leader, so the host stays in use.
func TestAHolderThatHoldsRelaysKeepsTheHostThroughALeaderOutage(t *testing.T) {
	r := newRig(t)
	k, _ := r.open()
	accepted(t, k, set("example-b", 1, true, "login.example", entry(1)))
	r.clock.advance(time.Hour)
	if got := refused(t, k, wire.KindSet, set("example-a", 1, true, "login.example", entry(2))); got.Holder != "example-b" {
		t.Errorf("an hour into the holder's leader outage, with its relays kept: %+v, want refused naming example-b", got)
	}
}

// An empty set claims nothing: a host it names stays free for the next
// Provider with Allocations.
func TestAnEmptySetClaimsNothing(t *testing.T) {
	r := newRig(t)
	k, _ := r.open()
	accepted(t, k, set("example-a", 1, true, "login.example"))
	if claims := r.disk().Claims; len(claims) != 0 {
		t.Errorf("an empty set claimed %+v", claims)
	}
	accepted(t, k, set("example-b", 1, true, "login.example", entry(1)))
	var holders []string
	for _, c := range r.disk().Claims {
		holders = append(holders, c.Provider)
	}
	if !slices.Equal(holders, []string{"example-b"}) {
		t.Errorf("the claims %v, want example-b's only", holders)
	}
}
