package relaykeeper

// New for issue #343 (PR 2): the login-host claims (claims.go; HBEP-6, The
// relay keeper, Capacity and the login host; the science agent's ruling on
// #343's Q2, as #347 states it; the review of PR 354). A relay holds a host
// only while it runs: the fake Runner shows a relay up only once the test
// starts it (fakeRunner.start).

import (
	"slices"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
)

// A second Provider's set for a claimed login host is refused, naming the
// host and its holder, and counted: one with Allocations, and one with none,
// so its controller shows the host in use before it has an Allocation. An
// older Provider is refused too, with the holder's relays running or not:
// the oldest-first rule is for an unclaimed host only (#347).
func TestASecondProvidersSetForAClaimedHostIsRefused(t *testing.T) {
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
	if got := refused(t, k, wire.KindSet, set("example-a", 1, true, "login.example", entry(3))); got.Holder != "example-b" {
		t.Errorf("the older Provider's set, the holder's relays not started: %+v, want refused naming example-b", got)
	}
	fr.start("example-b", entry(1).Tag)
	if got := refused(t, k, wire.KindSet, set("example-a", 1, true, "login.example", entry(3))); got.Holder != "example-b" {
		t.Errorf("the older Provider's set, the holder's relays running: %+v, want refused naming example-b", got)
	}
	if c := r.disk().Claims["login.example"]; c.Provider != "example-b" {
		t.Errorf("the claim on disk %+v, want example-b's", c)
	}
}

// The holder's complete set with no Allocation makes its claim lapse (rule
// 1). The host is then unclaimed, and its rule decides among the Providers
// whose unexpired sets name it: the older former holder still wins while its
// set is unexpired, so a younger Provider claims the host once it has
// expired, and an older one at once. An incomplete empty set releases
// nothing.
func TestTheHoldersCompleteEmptySetMakesItsClaimLapse(t *testing.T) {
	r := newRig(t)
	k, _ := r.open()
	accepted(t, k, set("example-b", 1, true, "login.example", entry(1)))
	accepted(t, k, set("example-b", 1, false, "login.example"))
	if got := refused(t, k, wire.KindSet, set("example-c", 1, true, "login.example", entry(2))); got.Holder != "example-b" {
		t.Errorf("after the holder's incomplete empty set: %+v, want the claim kept", got)
	}
	accepted(t, k, set("example-b", 1, true, "login.example"))
	if got := refused(t, k, wire.KindSet, set("example-c", 1, true, "login.example", entry(2))); got.Holder != "example-b" {
		t.Errorf("a younger Provider while the former holder's set names the host: %+v, want refused naming example-b", got)
	}
	r.clock.advance(41 * time.Second)
	accepted(t, k, set("example-c", 1, true, "login.example", entry(2)))
	if c := r.disk().Claims["login.example"]; c.Provider != "example-c" {
		t.Errorf("the claim on disk %+v, want example-c's", c)
	}

	r2 := newRig(t)
	k2, _ := r2.open()
	accepted(t, k2, set("example-b", 1, true, "login.example", entry(1)))
	accepted(t, k2, set("example-b", 1, true, "login.example"))
	accepted(t, k2, set("example-a", 1, true, "login.example", entry(2)))
	if c := r2.disk().Claims["login.example"]; c.Provider != "example-a" {
		t.Errorf("the claim on disk %+v, want the older example-a's, at once", c)
	}
}

// A claim never lapses while a relay of its holder still runs there, whatever
// its rules say: here the holder's complete empty set has asked for its
// relays' stop, which runs in the background, so an older Provider is
// refused until the relays are gone, then claims the host.
func TestAClaimIsNotReleasedWhileItsRelaysStillRun(t *testing.T) {
	r := newRig(t)
	k, fr := r.open()
	accepted(t, k, set("example-b", 1, true, "login.example", entry(1)))
	fr.start("example-b", entry(1).Tag)
	fr.linger = true
	accepted(t, k, set("example-b", 1, true, "login.example"))
	if got := refused(t, k, wire.KindSet, set("example-a", 1, true, "login.example", entry(2))); got.Holder != "example-b" {
		t.Errorf("while the holder's dropped relays still run: %+v, want refused naming example-b", got)
	}
	fr.reap("example-b", entry(1).Tag)
	accepted(t, k, set("example-a", 1, true, "login.example", entry(2)))
	if c := r.disk().Claims["login.example"]; c.Provider != "example-a" {
		t.Errorf("the claim on disk %+v, want example-a's once the relays were gone", c)
	}
}

// A holder whose accepted set names another host claims that one, and its
// first claim lapses (rule 2) once its relays there no longer run: the
// Runner moves them to the new host.
func TestAHolderThatMovesLetsItsFirstHostGo(t *testing.T) {
	r := newRig(t)
	k, fr := r.open()
	accepted(t, k, set("example-b", 1, true, "login-1.example", entry(1)))
	fr.start("example-b", entry(1).Tag)
	accepted(t, k, set("example-b", 1, true, "login-2.example", entry(1)))
	fr.start("example-b", entry(1).Tag) // now on login-2.example: they hold that host, not the first
	if c := r.disk().Claims["login-2.example"]; c.Provider != "example-b" {
		t.Errorf("the claim on login-2.example %+v, want example-b's", c)
	}
	accepted(t, k, set("example-c", 1, true, "login-1.example", entry(2)))
	if c := r.disk().Claims["login-1.example"]; c.Provider != "example-c" {
		t.Errorf("the claim on login-1.example %+v, want example-c's: example-b moved", c)
	}
}

// A holder's set that is refused for another host releases nothing: its
// relays on its own host run on, so a third Provider is still refused there
// (the review of PR 354, B3).
func TestARefusedMoveReleasesNoClaim(t *testing.T) {
	r := newRig(t)
	k, fr := r.open()
	accepted(t, k, set("example-a", 1, true, "login-1.example", entry(1)))
	fr.start("example-a", entry(1).Tag)
	accepted(t, k, set("example-b", 1, true, "login-2.example", entry(2)))
	fr.start("example-b", entry(2).Tag)
	if got := refused(t, k, wire.KindSet, set("example-a", 1, true, "login-2.example", entry(1))); got.Holder != "example-b" {
		t.Errorf("example-a's move to example-b's host: %+v, want refused naming example-b", got)
	}
	if c := r.disk().Claims["login-1.example"]; c.Provider != "example-a" {
		t.Errorf("example-a's claim on its own host after its refused move: %+v, want it kept", c)
	}
	if got := refused(t, k, wire.KindSet, set("example-c", 1, true, "login-1.example", entry(3))); got.Holder != "example-a" {
		t.Errorf("a third Provider on example-a's host after the refused move: %+v, want refused naming example-a", got)
	}
}

// A persisted claim survives a keeper restart, where no relay runs any more:
// another Provider is refused until the claim lapses, 5 minutes after the
// keeper's start with no set from the holder (rule 3); an older Provider too,
// since the host is claimed. The holder, if it comes back first, keeps it.
func TestAPersistedClaimSurvivesARestartUntilItLapses(t *testing.T) {
	r := newRig(t)
	k, _ := r.open()
	accepted(t, k, set("example-b", 1, true, "login.example", entry(1)))

	k2, _ := r.open()
	if got := refused(t, k2, wire.KindSet, set("example-a", 1, true, "login.example", entry(3))); got.Holder != "example-b" {
		t.Errorf("an older Provider after the restart: %+v, want refused naming the holder, example-b", got)
	}
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
	r3.clock.advance(time.Hour)
	accepted(t, k4, set("example-b", 2, true, "login.example", entry(1)))
	if got := refused(t, k4, wire.KindSet, set("example-c", 1, true, "login.example", entry(2))); got.Holder != "example-b" {
		t.Errorf("after the holder came back first: %+v, want refused naming example-b", got)
	}
}

// After a keeper restart, a persisted claim lapses by its first two rules
// too: its holder's complete set with no Allocation, and its holder's set
// naming another host. An older Provider then claims the host.
func TestAPersistedClaimLapsesByEachRuleAfterARestart(t *testing.T) {
	for name, release := range map[string]wire.DesiredSet{
		"a complete empty set":      set("example-b", 2, true, "login.example"),
		"a set naming another host": set("example-b", 2, true, "login-2.example", entry(1)),
	} {
		t.Run(name, func(t *testing.T) {
			r := newRig(t)
			k, _ := r.open()
			accepted(t, k, set("example-b", 1, true, "login.example", entry(1)))
			k2, _ := r.open()
			accepted(t, k2, release)
			accepted(t, k2, set("example-a", 1, true, "login.example", entry(3)))
			if c := r.disk().Claims["login.example"]; c.Provider != "example-a" {
				t.Errorf("the claim after %s: %+v, want example-a's", name, c)
			}
		})
	}
}

// A claim made under another UID of a Provider's name belongs to another
// Provider (#347): a re-created Provider of the same name is refused while
// the claim stands, and its own sets do not keep the old claim alive. Once
// the old Provider's claim lapses, the new one claims the host.
func TestAClaimUnderAnotherUIDBelongsToAnotherProvider(t *testing.T) {
	r := newRig(t)
	k, _ := r.open()
	accepted(t, k, set("example-b", 1, true, "login.example", entry(1)))
	k2, _ := r.open()
	again := recreated(set("example-b", 3, true, "login.example", entry(2)))
	for range 2 {
		got := refused(t, k2, wire.KindSet, again)
		if got.Reason != wire.RefusedLoginHost || got.Holder != "example-b" {
			t.Errorf("the re-created Provider's set: %+v, want refused for the old Provider's claim", got)
		}
		r.clock.advance(4 * time.Minute) // its sets keep coming; the old claim's clock runs from the keeper's start
	}
	accepted(t, k2, again)
	if c := r.disk().Claims["login.example"]; c.ProviderUID != again.ProviderUID {
		t.Errorf("the claim on disk %+v, want the re-created Provider's UID", c)
	}
}

// recreated is s from a Provider of s's name re-created under another UID.
func recreated(s wire.DesiredSet) wire.DesiredSet {
	s.ProviderUID = "0a1b2c3d-0000-4000-8000-0000000000ff"
	s.ProviderCreated = created["example-c"]
	return s
}

// A re-created Provider's sets do not shorten its predecessor's lapse: the
// lapse history is per Provider UID, never by name (the review of PR 354,
// B4). After a restart, the old Provider sends a set at minute 4, expiring
// at 4:40, so its claim lapses at 9:40; the re-created Provider's refused set
// at 4:10 changes nothing of that.
func TestARecreatedProviderDoesNotShortenItsPredecessorsLapse(t *testing.T) {
	r := newRig(t)
	k, _ := r.open()
	accepted(t, k, set("example-b", 1, true, "login.example", entry(1)))
	k2, _ := r.open()
	marked := entry(1)
	marked.EndedForGood = true
	r.clock.advance(4 * time.Minute)
	accepted(t, k2, set("example-b", 2, true, "login.example", marked))
	again := recreated(set("example-b", 3, true, "login.example", entry(2)))
	r.clock.advance(10 * time.Second)
	refused(t, k2, wire.KindSet, again)
	r.clock.advance(51 * time.Second) // 5:01
	if got := refused(t, k2, wire.KindSet, again); got.Holder != "example-b" {
		t.Errorf("at 5:01: %+v, want refused: the old claim lapses at 9:40", got)
	}
	r.clock.advance(4*time.Minute + 39*time.Second - time.Millisecond) // 9:39.999
	refused(t, k2, wire.KindSet, again)
	r.clock.advance(time.Millisecond)
	accepted(t, k2, again)
}

// A claim with no relay running lasts while its holder sends unexpired sets,
// and lapses ClaimLapse after its holder's last set expired, not after the
// keeper's start. Here the keeper restarted, so no relay runs, and the
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

// A claim lasts while a relay of its holder runs there, however long its
// leader is silent: the relays are kept with no leader, so the host stays in
// use.
func TestAHolderWhoseRelaysRunKeepsTheHostThroughALeaderOutage(t *testing.T) {
	r := newRig(t)
	k, fr := r.open()
	accepted(t, k, set("example-b", 1, true, "login.example", entry(1)))
	fr.start("example-b", entry(1).Tag)
	r.clock.advance(time.Hour)
	if got := refused(t, k, wire.KindSet, set("example-a", 1, true, "login.example", entry(2))); got.Holder != "example-b" {
		t.Errorf("an hour into the holder's leader outage, with its relays running: %+v, want refused naming example-b", got)
	}
}

// HBEP-6's third lapse rule fires while the keeper runs (the review of PR
// 354, B1): a holder whose leader went silent, and whose relays no longer
// run (their job ended, and with no set they retry with no process), loses
// its claim 5 minutes after its last set expired, with no keeper restart. A
// Want held is not a relay that runs.
func TestAHolderWhoseRelaysNoLongerRunLosesTheHost(t *testing.T) {
	r := newRig(t)
	k, fr := r.open()
	accepted(t, k, set("example-b", 1, true, "login.example", entry(1)))
	fr.start("example-b", entry(1).Tag)
	fr.stop("example-b", entry(1).Tag)
	r.clock.advance(40*time.Second + 5*time.Minute - time.Millisecond)
	if got := refused(t, k, wire.KindSet, set("example-a", 1, true, "login.example", entry(2))); got.Holder != "example-b" {
		t.Errorf("before the lapse: %+v, want refused naming example-b", got)
	}
	r.clock.advance(time.Millisecond)
	accepted(t, k, set("example-a", 1, true, "login.example", entry(2)))
	if c := r.disk().Claims["login.example"]; c.Provider != "example-a" {
		t.Errorf("the claim %+v, want example-a's", c)
	}
}

// For a host nobody holds, the oldest Provider wins, then by name, among the
// Providers whose latest unexpired sets name it, refused ones included: one
// refused while a third held the host keeps its place.
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

// An empty set claims nothing, but its Provider keeps its place in an
// unclaimed host's rule while the set is unexpired (#347: "among the
// Providers whose unexpired sets name it"): a younger Provider is refused
// until it expires, then claims the host.
func TestAnEmptySetClaimsNothingButKeepsItsPlace(t *testing.T) {
	r := newRig(t)
	k, _ := r.open()
	accepted(t, k, set("example-a", 1, true, "login.example"))
	if claims := r.disk().Claims; len(claims) != 0 {
		t.Errorf("an empty set claimed %+v", claims)
	}
	if got := refused(t, k, wire.KindSet, set("example-b", 1, true, "login.example", entry(1))); got.Holder != "example-a" {
		t.Errorf("a younger Provider while the older's empty set names the host: %+v, want refused naming example-a", got)
	}
	// A younger Provider's empty set too, so its controller shows the host in
	// use before it has an Allocation.
	if got := refused(t, k, wire.KindSet, set("example-c", 1, true, "login.example")); got.Holder != "example-a" {
		t.Errorf("a younger Provider's empty set while the older's names the host: %+v, want refused naming example-a", got)
	}
	r.clock.advance(41 * time.Second)
	accepted(t, k, set("example-b", 1, true, "login.example", entry(1)))
	var holders []string
	for _, c := range r.disk().Claims {
		holders = append(holders, c.Provider)
	}
	if !slices.Equal(holders, []string{"example-b"}) {
		t.Errorf("the claims %v, want example-b's only", holders)
	}
}
