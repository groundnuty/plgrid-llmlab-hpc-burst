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
// Both tests force that window with the router's own seam (Router.afterBuild), by order, never by time: a rebuild is held after it has read the
// registrations and before its view is published, the other Instance registers, and the reader is released once the router counts it waiting.

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// holdFirstRebuild makes the router's next rebuild stop once its view is built, before it is published, until release is called. entered is closed
// when it has stopped. release is also called when the test ends, so that a failing test does not leave the rebuild holding the router's lock.
func holdFirstRebuild(t *testing.T, r *Router) (entered <-chan struct{}, release func()) {
	t.Helper()
	in, out := make(chan struct{}), make(chan struct{})
	var first, once sync.Once
	r.afterBuild = func() {
		held := false
		first.Do(func() { held = true; close(in) })
		if held {
			<-out
		}
	}
	release = func() { once.Do(func() { close(out) }) }
	t.Cleanup(release)
	return in, release
}

// waitEntered waits until a held rebuild has stopped.
func waitEntered(t *testing.T, entered <-chan struct{}) {
	t.Helper()
	select {
	case <-entered:
	case <-time.After(failAfter):
		t.Fatalf("no rebuild of the router began within %s of a registration", failAfter)
	}
}

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
	entered, release := holdFirstRebuild(t, f.router)
	f.tun.setRegistered(f.uids[1], true) // the rebuild this starts reads only this registration
	waitEntered(t, entered)
	f.tun.setRegistered(f.uids[0], true) // marks the view stale: the held rebuild has already read its registrations
	for i, u := range f.uids {
		if !f.tun.Registered(u) {
			t.Fatalf("Instance %d is not registered: the window needs both registered, as the fixture's wait ended", i)
		}
	}
	released := releaseWhenAReaderWaits(f.router, release)
	session := scKeys(t, f.keys, f.keys[0], 1)[0]
	resp := f.post("/v1/chat/completions", chatPlain, "x-session-id", session)
	readAll(t, resp)
	if !released.Load() {
		t.Fatal("the request did not wait for the held rebuild")
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
			entered, release := holdFirstRebuild(t, f.router)
			f.tun.setRegistered(f.uids[c.first], true)
			waitEntered(t, entered)
			f.tun.setRegistered(f.uids[c.last], true)
			released := releaseWhenAReaderWaits(f.router, release)
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
