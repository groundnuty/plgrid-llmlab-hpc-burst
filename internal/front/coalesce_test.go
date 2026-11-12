package front

// The router's rebuilds of a burst of registration changes are coalesced (#417): the Transports' subscriber marks the
// view stale and returns, a goroutine rebuilds it while it is stale, and a reader of the view waits for at most one
// rebuild. These tests hold chosen rebuilds at a seam (Router.afterBuild, right after the view is built) and wait on
// the router's own state, never on a time window: a deadline here only names a failure.

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/naming"
)

// failAfter is how long a test waits for a state before it fails naming it: far above the milliseconds the state
// takes, so it decides no pass.
const failAfter = 30 * time.Second

// rebuildHold holds one rebuild at the seam until it is released.
type rebuildHold struct {
	held, release chan struct{}
	once          sync.Once
}

func (h *rebuildHold) open() { h.once.Do(func() { close(h.release) }) }

// waitHeld waits until the rebuild is held at the seam.
func (h *rebuildHold) waitHeld(t *testing.T) {
	t.Helper()
	select {
	case <-h.held:
	case <-time.After(failAfter):
		t.Fatal("no rebuild came to the seam")
	}
}

// holdRebuilds holds the next k rebuilds of the router at the seam, the i-th at holds[i]; later ones run on. The test's
// cleanup releases them all, so none outlives it.
func holdRebuilds(t *testing.T, r *Router, k int) []*rebuildHold {
	t.Helper()
	holds := make([]*rebuildHold, k)
	for i := range holds {
		holds[i] = &rebuildHold{held: make(chan struct{}), release: make(chan struct{})}
	}
	var n atomic.Int32
	r.afterBuild = func() {
		i := int(n.Add(1)) - 1
		if i >= k {
			return
		}
		close(holds[i].held)
		<-holds[i].release
	}
	t.Cleanup(func() {
		for _, h := range holds {
			h.open()
		}
	})
	return holds
}

// changeWhileHeld makes registration changes while a rebuild is held, and fails unless every one returns while it is
// held: a subscriber that rebuilds the view itself would wait for the held rebuild.
func changeWhileHeld(t *testing.T, changes func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		changes()
	}()
	select {
	case <-done:
	case <-time.After(failAfter):
		t.Fatal("a registration change waited for the rebuild in progress: the subscriber rebuilds the view itself")
	}
}

// quiesce waits until no rebuild is due or running: every change marked so far has a rebuild after it.
func (r *Router) quiesce(t *testing.T) {
	t.Helper()
	waitRouterState(t, "the router's rebuilds to settle", func() bool { return !r.running.Load() && !r.dirty.Load() })
}

// waitRouterState waits until cond holds, failing after failAfter with what it waited for.
func waitRouterState(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for end := time.Now().Add(failAfter); !cond(); time.Sleep(time.Millisecond) {
		if time.Now().After(end) {
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

// newBurstFront is a harness with n served deployments of one ready Instance each, model m-<i>, on StaticTransports,
// and none of them registered yet: S16's fixture shape (#412), one tunnel per model.
func newBurstFront(t *testing.T, n int) (*tunFront, []types.UID) {
	t.Helper()
	h := newHarness(t, nil)
	h.profile("helios-gh200", v1alpha1.PlatformBatch, "", "")
	h.batchProvider("helios-a", "helios-gh200")
	uids := make([]types.UID, n)
	for i := range uids {
		md := fmt.Sprintf("md-%d", i)
		h.servedMD(md, fmt.Sprintf("m-%d", i), 1, "helios-a")
		uids[i] = h.instance(md, md+"-0", "helios-a", nil, "", instanceOpts{})
	}
	h.apply()
	return &tunFront{harness: h}, uids
}

const burstAddr = "127.0.0.1:9"

// TestABurstOfRegistrationChangesCostsARebuildOrTwo: 200 registrations while the first rebuild they cause is held:
// none of them waits for it, and once it is released one more rebuild brings the view to the last state, so the burst
// costs two rebuilds at most, not 200 in a row under the routing lock.
func TestABurstOfRegistrationChangesCostsARebuildOrTwo(t *testing.T) {
	const n = 200
	f, uids := newBurstFront(t, n)
	holds := holdRebuilds(t, f.router, 1)
	start := f.router.rebuilds.Load()
	go f.trans.Set(uids[0], burstAddr) // the change whose rebuild the seam holds
	holds[0].waitHeld(t)
	changeWhileHeld(t, func() {
		for _, u := range uids[1:] {
			f.trans.Set(u, burstAddr)
		}
	})
	holds[0].open()
	f.router.quiesce(t)
	if got := f.router.rebuilds.Load() - start; got < 1 || got > 2 {
		t.Fatalf("a burst of %d registrations cost %d rebuilds, want 1 or 2", n, got)
	}
	for i, u := range uids {
		if member, eligible, _ := f.routing(u); !member || !eligible {
			t.Fatalf("Instance %d after the burst: member %t, eligible %t; want the last state, registered", i, member, eligible)
		}
	}
}

// TestAReaderMidBurstWaitsAtMostOneRebuild: a request comes while a rebuild runs and more changes land: once that
// rebuild ends the request routes on its view at once. It neither rebuilds itself, nor waits for the next rebuild,
// which the seam holds until the request is done: either would leave the request waiting here.
func TestAReaderMidBurstWaitsAtMostOneRebuild(t *testing.T) {
	const n = 50
	f, uids := newBurstFront(t, n)
	for _, u := range uids[:n/2] {
		f.trans.Set(u, burstAddr)
	}
	f.router.quiesce(t)
	holds := holdRebuilds(t, f.router, 2)
	go f.trans.Set(uids[n/2], burstAddr) // the change whose rebuild the seam holds
	holds[0].waitHeld(t)
	before := f.router.rebuilds.Load()
	var during atomic.Int64
	f.router.beforePick = func() { during.Store(f.router.rebuilds.Load()) }
	type result struct {
		tg  *target
		err *routeError
	}
	done := make(chan result, 1)
	go func() {
		tg, err := f.router.choose(context.Background(), naming.Name{Model: "m-0", Level: naming.LevelModel}, "session-a", "")
		done <- result{tg, err}
	}()
	waitRouterState(t, "the request to wait for the lock", func() bool { return f.router.waiting.Load() == 1 })
	changeWhileHeld(t, func() {
		for _, u := range uids[n/2+1:] {
			f.trans.Set(u, burstAddr)
		}
	})
	holds[0].open()
	var res result
	select {
	case res = <-done:
	case <-time.After(failAfter):
		t.Fatal("the request waited for a second rebuild: it rebuilt the view itself, or a rebuild took the lock ahead of it")
	}
	if res.err != nil || res.tg == nil {
		t.Fatalf("the request mid-burst: %v, want its target", res.err)
	}
	res.tg.res.Release()
	if got := during.Load() - before; got != 1 {
		t.Fatalf("the request waited for %d rebuilds, want the 1 in progress when it came", got)
	}
	holds[1].open()
	f.router.quiesce(t)
	for i, u := range uids {
		if member, eligible, _ := f.routing(u); !member || !eligible {
			t.Fatalf("Instance %d after the burst: member %t, eligible %t", i, member, eligible)
		}
	}
}

// TestAChangeDuringARebuildIsNotLost: changes that land after a rebuild has read the registrations, while it is held,
// are in the view once the rebuilds settle: the last registration state wins. A rebuild that clears its mark when it
// ends, not when it begins, would drop them.
func TestAChangeDuringARebuildIsNotLost(t *testing.T) {
	f, uids := newBurstFront(t, 2)
	holds := holdRebuilds(t, f.router, 1)
	go f.trans.Set(uids[0], burstAddr) // the change whose rebuild the seam holds
	holds[0].waitHeld(t)
	changeWhileHeld(t, func() {
		f.trans.Set(uids[1], burstAddr)
		f.trans.Remove(uids[0])
	})
	holds[0].open()
	f.router.quiesce(t)
	if _, eligible, _ := f.routing(uids[0]); eligible {
		t.Fatal("an Instance unregistered during a rebuild is still eligible once the rebuilds settled")
	}
	if member, eligible, _ := f.routing(uids[1]); !member || !eligible {
		t.Fatalf("an Instance registered during a rebuild once the rebuilds settled: member %t, eligible %t", member, eligible)
	}
}

// TestAChangeAsTheRebuildsStopIsNotLost: a change that lands after the drain's last rebuild, as it is about to stop,
// finds it still running and starts none of its own; the drain looks again before it stops, and rebuilds for it.
func TestAChangeAsTheRebuildsStopIsNotLost(t *testing.T) {
	f, uids := newBurstFront(t, 2)
	at, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	f.router.beforeStop = func() {
		once.Do(func() {
			close(at)
			<-release
		})
	}
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	f.trans.Set(uids[0], burstAddr)
	select {
	case <-at:
	case <-time.After(failAfter):
		t.Fatal("the rebuilds never came to their stop")
	}
	f.trans.Set(uids[1], burstAddr)
	close(release)
	f.router.quiesce(t)
	for i, u := range uids {
		if member, eligible, _ := f.routing(u); !member || !eligible {
			t.Fatalf("Instance %d once the rebuilds settled: member %t, eligible %t", i, member, eligible)
		}
	}
}

// TestInterleavedRegistrationsEndInTheLastStateAndKeepTheFence: over the real Tunnel, four goroutines take registrations
// away (Bump, whose subscribers run in a goroutine of their own) and give them back (a probe's pass, whose subscribers
// run in its call) on random Instances, while two route requests and dial each target as the proxy does. Every dial
// either reaches the double, or fails at the front's own fence (an epoch that moved, no transport), never otherwise;
// and once the churn stops the view comes to the Tunnel's own registrations, Instance by Instance.
func TestInterleavedRegistrationsEndInTheLastStateAndKeepTheFence(t *testing.T) {
	const n, workers, ops = 40, 4, 400
	h := newHarness(t, nil)
	h.profile("helios-gh200", v1alpha1.PlatformBatch, "", "")
	h.batchProvider("helios-a", "helios-gh200")
	m := startMuxDouble(t)
	uids := make([]types.UID, n)
	for i := range uids {
		md := fmt.Sprintf("md-%d", i)
		h.servedMD(md, fmt.Sprintf("m-%d", i), 1, "helios-a")
		uids[i] = h.instance(md, md+"-0", "helios-a", nil, "", instanceOpts{})
		m.set(uids[i], http.StatusOK)
	}
	tun := NewTunnel(m.ln.Addr().String(), h.cfg.TunnelSetupTimeout.D())
	h.router = NewRouter(h.cfg, h.clk, tun, h.rec, discardLog(), h.metrics)
	h.apply()
	register := func(u types.UID) {
		e := tun.Epoch(u)
		tun.setRegisteredAt(u, true, &e)
	}
	for _, u := range uids {
		register(u)
	}
	f := &tunFront{harness: h}

	stop := make(chan struct{})
	var churn, routers sync.WaitGroup
	var dials, fenced atomic.Int64
	// The churn runs its ops, and on until the routers have dialed minDials targets during it, so that the two
	// interleave; past failAfter it stops, and the count below fails the test.
	const minDials = 100
	end := time.Now().Add(failAfter)
	for w := 0; w < workers; w++ {
		churn.Add(1)
		go func(seed int64) {
			defer churn.Done()
			rnd := rand.New(rand.NewSource(seed))
			for i := 0; (i < ops || dials.Load() < minDials) && time.Now().Before(end); i++ {
				u := uids[rnd.Intn(n)]
				if rnd.Intn(2) == 0 {
					tun.Bump(u)
				} else {
					register(u)
				}
				runtime.Gosched() // so that the routers run between the changes, not after them
			}
		}(int64(w + 1))
	}
	bad := make(chan error, 1)
	for c := 0; c < 2; c++ {
		routers.Add(1)
		go func(seed int64) {
			defer routers.Done()
			rnd := rand.New(rand.NewSource(seed))
			for {
				select {
				case <-stop:
					return
				default:
				}
				i := rnd.Intn(n)
				tg, rerr := h.router.choose(context.Background(), naming.Name{Model: fmt.Sprintf("m-%d", i), Level: naming.LevelModel}, "s", "")
				if rerr != nil || tg == nil {
					continue
				}
				dial, ok := tun.Dialer(tg.inst.UID)
				if !ok {
					fenced.Add(1) // no transport: the proxy's own fence
					tg.res.Release()
					continue
				}
				dials.Add(1)
				conn, err := dial(context.Background())
				switch {
				case err == nil:
					conn.Close()
				case isOwnFence(err):
					fenced.Add(1)
				default:
					select {
					case bad <- fmt.Errorf("a dial to an Instance the view named failed past the fence: %w", err):
					default:
					}
				}
				tg.res.Release()
			}
		}(int64(100 + c))
	}
	churn.Wait()
	close(stop)
	routers.Wait()
	select {
	case err := <-bad:
		t.Fatal(err)
	default:
	}
	if dials.Load() < minDials {
		t.Fatalf("only %d dials during the churn, want %d: the routing and the churn did not interleave", dials.Load(), minDials)
	}
	waitRouterState(t, "the view to come to the Tunnel's registrations", func() bool {
		for _, u := range uids {
			if _, eligible, _ := f.routing(u); eligible != tun.Registered(u) {
				return false
			}
		}
		return true
	})
	f.router.quiesce(t)
	for i, u := range uids {
		if _, eligible, _ := f.routing(u); eligible != tun.Registered(u) {
			t.Fatalf("Instance %d: eligible %t, registered %t once the rebuilds settled", i, eligible, tun.Registered(u))
		}
	}
	t.Logf("%d dials during the churn, %d stopped at the front's own fence", dials.Load(), fenced.Load())
}
