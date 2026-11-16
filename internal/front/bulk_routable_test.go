package front

// The bulk fixture waits on the router's view of every Instance, not only on the tunnels' registrations (issue #637).
//
// TestABulk5xxIsRetriedAndCountsTowardThePause failed CI's go job once with its request's FIRST attempt sent to md-0-1 and md-0-0 never tried. The
// fixture registers the Instances with a probe round, which probes them concurrently, so either may register first, and waited until both were
// registered on the tunnels. A registration reaches the router's view by a rebuild (Tunnel.Subscribe, Router.markDirty), and Router.lockFresh is
// the shipped, reviewed one-rebuild bound of #417 and #424: a reader that came while a rebuild ran reads that rebuild's view when the rebuild ends,
// though a registration since has marked it stale. So when the second registration landed after a rebuild had read the first, a request that
// came before that rebuild ended was routed on a view that held one Instance, and the ring homed its session there. The router is as documented;
// the fixture did not wait for the state its first request relies on.
//
// Both tests force that window with the router's own seam (Router.afterBuild, through holdRebuilds of coalesce_test.go), by order, never by time: a
// rebuild is held after it has read the registrations and before its view is published, the other Instance registers, and the reader is released
// once the router counts it waiting. The test that shows the cause holds a second rebuild as well, until the request has made its pick (below).

import (
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// releaseWhenAReaderWaits releases a held rebuild once a reader of the router's view waits for it: Router.waiting counts the readers that wait for
// the lock the rebuild holds. released says that it did; a reader that never comes leaves it false, and the test's clean-up releases the rebuild.
func releaseWhenAReaderWaits(r *Router, release func()) (released *atomic.Bool) {
	released = new(atomic.Bool)
	go func() {
		for end := time.Now().Add(failAfter); r.waiting.Load() < 1; time.Sleep(time.Millisecond) {
			if time.Now().After(end) {
				return
			}
		}
		released.Store(true)
		release()
	}()
	return released
}

// releaseWhenInFlight releases a held rebuild once the group's in-flight count for member reads 1: the request has picked it and is blocked on the
// router's lock, which the held rebuild holds, so the count stays 1 until the release. It reads the group's own lock and not the router's, which
// would wait for the rebuild. It must not wait for the request to reach the engine: the request needs the router's lock (Router.LeaseEnd, armLease)
// before that, and would never come. released says that it did.
func releaseWhenInFlight(g interface{ InFlight(string) (int64, bool) }, member string, release func()) (released *atomic.Bool) {
	released = new(atomic.Bool)
	go func() {
		for end := time.Now().Add(failAfter); ; time.Sleep(time.Millisecond) {
			if n, ok := g.InFlight(member); ok && n == 1 {
				break
			}
			if time.Now().After(end) {
				return
			}
		}
		released.Store(true)
		release()
	}()
	return released
}

// publishedMembers counts the Instances of the router's view as published, without bringing a stale view up to date as a reader of it would.
func publishedMembers(r *Router) (n int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.view == nil {
		return 0
	}
	for _, m := range r.view.Models {
		for _, d := range m.Served {
			n += len(d.Members)
		}
	}
	return n
}

// A request that comes while a rebuild that read only md-0-1's registration is held reads that view, though md-0-0 has registered since, and its
// session, homed on md-0-0, goes to md-0-1 with md-0-0 never tried: CI's failure, from the first attempt. Both Instances are registered on the
// tunnels when it comes, which is all the fixture used to wait for. A request after the next rebuild goes to md-0-0. This pins the documented bound
// of #417 as the hazard for a fixture, and is the cause shown, not a fault of the router.
func TestAFirstRequestOnTheViewOfARebuildThatPredatesARegistrationGoesToAnotherInstance(t *testing.T) {
	f := buildBulkFront(t, 2, true)
	group := f.scGroup("ns/md-0") // the live group, read below without the router's lock
	holds := holdRebuilds(t, f.router, 2)
	f.tun.setRegistered(f.uids[1], true) // the rebuild this starts reads only this registration
	holds[0].waitHeld(t)
	f.tun.setRegistered(f.uids[0], true) // marks the view stale: the held rebuild has already read its registrations
	for i, u := range f.uids {
		if !f.tun.Registered(u) {
			t.Fatalf("Instance %d is not registered: the window needs both registered, as the fixture's wait ended", i)
		}
	}
	released := releaseWhenAReaderWaits(f.router, holds[0].open)
	// The drain's follow-up rebuild, for the registration the first did not read, starts as soon as the request leaves the router's lock. The
	// request reads the view, whose ring has md-0-1 alone, but picks from the LIVE group, which a rebuild rewrites (Group.Update): a follow-up
	// rebuild that ended between the two would put md-0-0 in the group, and the request would go there, an outcome that depends on which of
	// two goroutines is faster (the review of #642; forced through Router.beforePick it sends the request to md-0-0). So that rebuild is held
	// too, until the pick has been made: the group counts the request in flight on md-0-1 then, and the request stays there, blocked on the
	// router's lock (Router.LeaseEnd, which the held rebuild holds), until this releases it. Releasing on the engine's receipt of the request
	// would never come.
	releasedPick := releaseWhenInFlight(group, f.keys[1], holds[1].open)
	session := scKeys(t, f.keys, f.keys[0], 1)[0]
	resp := f.post("/v1/chat/completions", chatPlain, "x-session-id", session)
	readAll(t, resp)
	if !released.Load() || !releasedPick.Load() {
		t.Fatalf("the request did not wait for the held rebuild (%t), or the second rebuild was not held until the pick (%t)", released.Load(), releasedPick.Load())
	}
	if resp.StatusCode != 200 || scInstanceOf(resp) != f.keys[1] {
		t.Errorf("a request on the view of a rebuild that predates md-0-0's registration: %d from %q, want 200 from %s (the one Instance in that view)", resp.StatusCode, scInstanceOf(resp), f.keys[1])
	}
	if n := f.router.failures(f.keys[0]); n != 0 {
		t.Errorf("%d failures on %s: it was passed over, not tried", n, f.keys[0])
	}
	f.waitRoutable()
	resp = f.post("/v1/chat/completions", chatPlain, "x-session-id", session)
	readAll(t, resp)
	if scInstanceOf(resp) != f.keys[0] {
		t.Errorf("a request once the router's view holds both: from %q, want %s, the session's home", scInstanceOf(resp), f.keys[0])
	}
}

// The fixture's wait holds through that window. In either order of registration, one Instance's registration is read by the held rebuild and the
// other's is not: waitRoutable does not return while the router's published view lacks an Instance, though both are registered on the tunnels.
// The held rebuild is released once the wait itself waits on the router, and the wait then returns with both in the published view, and the
// first request goes to the session's home. The three things this fails on are the fixture that waits for the registrations alone (it returns
// at once, with the rebuild held), one that looks at the first Instance only, and one that looks at the second only (each returns on a view that
// lacks the other, which this reads without bringing it up to date).
func TestTheBulkFixtureWaitsForTheRoutersViewOfEveryInstance(t *testing.T) {
	for _, c := range []struct {
		name        string
		first, last int // the Instance the held rebuild reads, and the one that registers after it
	}{{"the second Instance registers last", 1, 0}, {"the first Instance registers last", 0, 1}} {
		t.Run(c.name, func(t *testing.T) {
			f := buildBulkFront(t, 2, true)
			holds := holdRebuilds(t, f.router, 1)
			f.tun.setRegistered(f.uids[c.first], true)
			holds[0].waitHeld(t)
			f.tun.setRegistered(f.uids[c.last], true)
			released := releaseWhenAReaderWaits(f.router, holds[0].open)
			f.waitRoutable()
			if !released.Load() {
				t.Fatal("waitRoutable returned while the router's rebuild was held: it waited for the tunnels' registrations and not for the router's view of them")
			}
			if n := publishedMembers(f.router); n != len(f.uids) {
				t.Errorf("the router's published view holds %d of %d Instances once waitRoutable returned", n, len(f.uids))
			}
			session := scKeys(t, f.keys, f.keys[0], 1)[0]
			resp := f.post("/v1/chat/completions", chatPlain, "x-session-id", session)
			readAll(t, resp)
			if scInstanceOf(resp) != f.keys[0] {
				t.Errorf("the first request after waitRoutable: from %q, want %s, the session's home", scInstanceOf(resp), f.keys[0])
			}
		})
	}
}

// The wait fails, naming the Instance and what it lacks, when an Instance is not routable within its bound. The bound is short: the test waits for
// the failure, not for a time. Each condition is tested alone, on the second Instance, with the first routable: an Instance paused by failed
// connections is registered on both tunnels and a member of its group, and not eligible, so only the router's part of the wait sees it; one whose
// token transport or whose bulk transport lost its registration is named for that.
func TestWaitRoutableFailsNamingTheInstanceItIsWaitingFor(t *testing.T) {
	for _, c := range []struct {
		name   string
		break_ func(f *bulkFront)
		want   []string
	}{
		{"paused", func(f *bulkFront) {
			for i := 0; i < f.cfg.PauseAfterFailures; i++ {
				f.router.ReportConnect(f.keys[1], true)
			}
		}, []string{"token transport registered true", "bulk transport registered true", "member of its group in the router's view true", "eligible false"}},
		{"its token transport lost its registration", func(f *bulkFront) { f.tun.setRegistered(f.uids[1], false) },
			[]string{"token transport registered false"}},
		{"its bulk transport lost its registration", func(f *bulkFront) { f.bulk.unregister(f.uids[1]) },
			[]string{"token transport registered true", "bulk transport registered false"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := newBulkFront(t, 2, false)
			c.break_(f)
			msg := f.notRoutableWithin(50 * time.Millisecond)
			for _, want := range append([]string{f.keys[1]}, c.want...) {
				if !strings.Contains(msg, want) {
					t.Errorf("%q does not say %q", msg, want)
				}
			}
			if strings.Contains(msg, f.keys[0]) {
				t.Errorf("%q names %s, which is routable", msg, f.keys[0])
			}
		})
	}
}
