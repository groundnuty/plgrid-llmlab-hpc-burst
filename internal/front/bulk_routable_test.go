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
// once the router counts it waiting. The test that shows the cause also keeps the drain's follow-up rebuild from starting until the request has been
// served, by standing in for a waiting reader, which the drain yields to (below).

import (
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// releaseWhenReadersWait releases a held rebuild once n readers of the router's view wait for the lock it holds (Router.waiting counts them). released
// says that it did; readers that never come leave it false, and the test's clean-up releases the rebuild.
func releaseWhenReadersWait(r *Router, n int32, release func()) (released *atomic.Bool) {
	released = new(atomic.Bool)
	go func() {
		for end := time.Now().Add(failAfter); r.waiting.Load() < n; time.Sleep(time.Millisecond) {
			if time.Now().After(end) {
				return
			}
		}
		released.Store(true)
		release()
	}()
	return released
}

// standInForAReader makes the router's drain yield, as it does while a reader waits for the lock (Router.yieldToReaders reads Router.waiting), until the
// returned open is called, and in the test's clean-up: no rebuild of the drain's starts meanwhile. It holds nothing, no lock and no rebuild, so it can
// never stand in the way of the request. A held rebuild can: it keeps the router's lock, which the request takes after its pick (Router.LeaseEnd) and
// when it finishes (Router.usageWritten). The first version of the cause test held the drain's follow-up rebuild, to release it once the request was
// in flight, and hung in Proxy.finish when the follow-up came late and the request had been and gone.
func standInForAReader(t *testing.T, r *Router) (open func()) {
	t.Helper()
	r.waiting.Add(1)
	var once sync.Once
	open = func() { once.Do(func() { r.waiting.Add(-1) }) }
	t.Cleanup(open)
	return open
}

// postWithin is harness.post with a bound on the whole exchange, which returns the failure and does not fail the test, so that a request that is stuck,
// behind the router's lock for one, fails the test that sent it, naming what it waited on, and does not hang the package.
func (f *bulkFront) postWithin(d time.Duration, path, body string, hdr ...string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodPost, f.url+path, strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+f.clientKey)
	req.Header.Set("Content-Type", "application/json")
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	return (&http.Client{Timeout: d}).Do(req)
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
//
// The drain's follow-up rebuild, for the registration the first did not read, starts as soon as the request leaves the router's lock. The request
// reads the view, whose ring has md-0-1 alone, but picks from the LIVE group, which a rebuild rewrites (Group.Update): a follow-up rebuild that
// ended between the two would put md-0-0 in the group and the request would go there, an outcome that depends on which of two goroutines is faster
// (the review of #642; forced through Router.beforePick, it sends the request to md-0-0). So that rebuild is kept from starting until the request has
// been served, by standing in for a reader that waits (standInForAReader), which holds neither the lock nor a rebuild. The request itself is bounded:
// whatever it waits on, it fails the test naming it.
func TestAFirstRequestOnTheViewOfARebuildThatPredatesARegistrationGoesToAnotherInstance(t *testing.T) {
	f := buildBulkFront(t, 2, true)
	holds := holdRebuilds(t, f.router, 1)
	f.tun.setRegistered(f.uids[1], true) // the rebuild this starts reads only this registration
	holds[0].waitHeld(t)
	f.tun.setRegistered(f.uids[0], true) // marks the view stale: the held rebuild has already read its registrations
	for i, u := range f.uids {
		if !f.tun.Registered(u) {
			t.Fatalf("Instance %d is not registered: the window needs both registered, as the fixture's wait ended", i)
		}
	}
	followUp := standInForAReader(t, f.router)
	released := releaseWhenReadersWait(f.router, 2, holds[0].open) // the stand-in, and the request
	session := scKeys(t, f.keys, f.keys[0], 1)[0]
	resp, err := f.postWithin(failAfter, "/v1/chat/completions", chatPlain, "x-session-id", session)
	if err != nil {
		t.Fatalf("no response within %s: %v (the first rebuild released %t; %d readers waiting for the router's lock, of the 2 it waits for)", failAfter, err, released.Load(), f.router.waiting.Load())
	}
	readAll(t, resp)
	followUp() // the request has been served: the follow-up rebuild may start
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
	resp, err = f.postWithin(failAfter, "/v1/chat/completions", chatPlain, "x-session-id", session)
	if err != nil {
		t.Fatalf("no response within %s once the router's view held both: %v", failAfter, err)
	}
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
			released := releaseWhenReadersWait(f.router, 1, holds[0].open)
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
