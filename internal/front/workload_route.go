package front

import (
	"context"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/balancer"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/naming"
)

// The Workloads' state at the front: each Workload's count of requests in flight on managed targets and its own FIFO of requests that no route can take
// (HBEP-5, Workloads: Route order, Waiting; HBEP-764, the science agent's rulings Q5, Q6, Q9, Q13 and Q15 on #764).
//
// LOCK ORDER (Q6). A Workload's mutex is taken BEFORE any group's mutex, never the reverse, and a group delivers its wake to the Workload FIFOs only
// AFTER it has released its own mutex (balancer.Group's OnFree and OnChange). In full, from the outermost:
//
//	passMu          one wake pass at a time, so the FIFOs are served in the order of their oldest request's arrival (Q5)
//	wlState.mu      one Workload's mutex: its managed count and its FIFO
//	Router.mu       the view, which a placement reads and a rebuild replaces
//	Group.mu        one ModelDeployment's balancer
//
// wlMu, which guards the registry of Workload states, is a leaf: nothing is taken while it is held. Nothing that holds Router.mu or a Group.mu takes
// a Workload's mutex or passMu or waits for one. The rebuild runs under Router.mu and calls Group.Update, whose wake would otherwise take a Workload's
// mutex, so the wake from a rebuild is handed off (wakeAsync), and the wake from a completion, which holds no lock, runs in the completing goroutine
// after the group's mutex is released (wakeNow). A give-back made under a Workload's mutex is quiet (Reservation.ReleaseQuiet), since a wake would
// take that mutex again.

// wlState is one Workload's state, kept by the router under the Workload's namespace and name: it outlives the router's rebuilds, and goes only when the
// Workload has left the view and nothing of it is in flight or waiting (Q15).
type wlState struct {
	r *Router
	// key is the state's key in the registry, wlKey: the Workload's namespace, name and UID. It is per object, so a Workload deleted and created again under
	// the same name has a new state, with none of the deleted one's managed count or FIFO.
	key  string
	ns   string
	name string
	uid  types.UID

	// mu is the Workload's mutex (LOCK ORDER above). It guards managed, queue and dead; a request's count is reserved and released under it, once, as
	// the balancer's reservations are under the group's.
	mu sync.Mutex
	// managed is the Workload's requests in flight on managed targets, all of its managed targets together.
	managed int
	// queue is the Workload's FIFO, oldest first. Only its head is placed (strict: a younger waiter never passes an older one).
	queue []*wlWaiter
	// dead says the state has been dropped from the registry (pruneWorkloads): a request that finds it set asks for a new one.
	dead bool

	// waiting is len(queue) and headSeq the arrival number of its head (0: empty), kept for readers that must not take mu: the series, the status and
	// the wake pass's order.
	waiting atomic.Int64
	headSeq atomic.Uint64
}

// lock takes the Workload's mutex. onWlContended, set by tests only, runs in a caller that finds it held, before it waits.
func (ws *wlState) lock() {
	if ws.mu.TryLock() {
		return
	}
	if h := ws.r.onWlContended; h != nil {
		h()
	}
	ws.mu.Lock()
}

// wlWaiter is a request waiting in a Workload's FIFO with what its every attempt needs. It carries the Resolution it arrived with, so an edit of the
// Workload changes neither its charge nor its route order nor its bound (Q15).
type wlWaiter struct {
	seq         uint64 // arrival number, over all the Workloads' FIFOs
	n           naming.Name
	res         *Resolution
	key         string
	exclude     []string
	withManaged bool
	ch          chan wlResult // buffered: an answer never blocks
}

// wlResult is what a pass tells a waiter: the target it was placed on, or the error that ends its wait (no route of its serving set can ever take it).
type wlResult struct {
	tg  *target
	err *routeError
}

// wlKey is the registry key of a Workload's state.
func wlKey(ns, name string, uid types.UID) string { return ns + "/" + name + "/" + string(uid) }

// enqueueLocked appends a request to the FIFO. The caller holds mu.
func (ws *wlState) enqueueLocked(p *plan) *wlWaiter {
	w := &wlWaiter{seq: ws.r.wlSeq.Add(1), n: p.n, res: p.res, key: p.key, exclude: slices.Clone(p.exclude), withManaged: p.withManaged, ch: make(chan wlResult, 1)}
	ws.queue = append(ws.queue, w)
	ws.syncQueueLocked()
	return w
}

// popHeadLocked takes the head out of the FIFO, which was placed.
func (ws *wlState) popHeadLocked() { ws.popAtLocked(0) }

// popAtLocked takes the waiter at index i out of the FIFO, which was placed or answered.
func (ws *wlState) popAtLocked(i int) {
	ws.queue = slices.Delete(ws.queue, i, i+1)
	ws.syncQueueLocked()
	ws.r.wlActive.Add(-1)
}

// pinnedWaitingLocked is the targets that hold a pinned waiter of the Workload.
func (ws *wlState) pinnedWaitingLocked() map[pinTarget]bool {
	var m map[pinTarget]bool
	for _, w := range ws.queue {
		if w.res.pin != nil {
			if m == nil {
				m = map[pinTarget]bool{}
			}
			m[*w.res.pin] = true
		}
	}
	return m
}

// competesLocked reports whether a request arriving now must queue behind a waiter instead of trying a route: the FIFO is strict, so a request behind a
// waiter that may take the same route queues. A waiter that holds no pin may take any route, so it competes with every request. A waiter that holds one
// may take only its pinned target, so it competes with a request that holds the same pin, and a pinned head that can't be placed blocks no other
// (HBEP-5, Sticky routes, Requests after the pin). pin is the arriving request's pin, nil for one that holds none. A Workload with no sticky request
// has no pinned waiter, so there a waiter always competes: the FIFO is strict as before.
func (ws *wlState) competesLocked(pin *pinTarget) bool {
	for _, w := range ws.queue {
		if w.res.pin == nil || (pin != nil && *w.res.pin == *pin) {
			return true
		}
	}
	return false
}

// removeLocked takes a waiter that gave up out of the FIFO. It reports whether it was there, and whether it was the head.
func (ws *wlState) removeLocked(w *wlWaiter) (removed, wasHead bool) {
	i := slices.Index(ws.queue, w)
	if i < 0 {
		return false, false
	}
	ws.queue = slices.Delete(ws.queue, i, i+1)
	ws.syncQueueLocked()
	ws.r.wlActive.Add(-1)
	return true, i == 0
}

func (ws *wlState) syncQueueLocked() {
	ws.waiting.Store(int64(len(ws.queue)))
	if len(ws.queue) == 0 {
		ws.headSeq.Store(0)
		ws.queue = nil
		return
	}
	ws.headSeq.Store(ws.queue[0].seq)
}

// managedRes is the reservation of a request on a managed target. For a request that names a Workload it holds one unit of the Workload's count, reserved
// under the Workload's mutex by takeManagedLocked and given back under it by release, exactly once however the request ends; every managed request is
// counted in the in-flight series.
type managedRes struct {
	r        *Router
	ws       *wlState // nil: the request names no Workload, so it has no count
	workload string
	provider string
	// deployment is the managed target's ModelDeployment.
	deployment string
	done       atomic.Bool
}

// release gives the reservation back. Only the first call counts. A completion on a managed target frees a route of its Workload, so its FIFO is woken.
func (m *managedRes) release() {
	if m == nil || !m.done.CompareAndSwap(false, true) {
		return
	}
	m.r.metrics.inFlightAdd(m.workload, m.provider, m.deployment, "", -1)
	if m.ws == nil {
		return
	}
	m.ws.lock()
	m.ws.managed--
	m.ws.mu.Unlock()
	m.r.wakeNow()
}

// wlAcquire returns the state of the Workload a Resolution names, creating it at the first request. A request keeps the definition it came with, so a retry
// may still route on a Workload that has been deleted since, and its state may have been pruned meanwhile: the state is then created again, but never
// live. It is made an orphan at once (wlOrphan), so that the pass that ends the retry's last use of it drops it, and it leaks no FIFO and no waiting series.
func (r *Router) wlAcquire(res *Resolution) *wlState {
	key := wlKey(res.Namespace, res.Name, res.uid)
	r.mu.Lock()
	known := r.wlKnown[key]
	r.mu.Unlock()
	r.wlMu.Lock()
	defer r.wlMu.Unlock()
	ws := r.wls[key]
	if ws == nil {
		ws = &wlState{r: r, key: key, ns: res.Namespace, name: res.Name, uid: res.uid}
		r.wls[key] = ws
		r.wlN.Add(1)
		if !known {
			r.wlOrphan.Store(true)
		}
	}
	return ws
}

// lockWorkload takes the mutex of the Workload's state, asking for a new state when the one it found was dropped meanwhile.
func (r *Router) lockWorkload(res *Resolution) *wlState {
	for {
		ws := r.wlAcquire(res)
		ws.lock()
		if !ws.dead {
			return ws
		}
		ws.mu.Unlock()
	}
}

// chooseNamed places a request that names a Workload, the attempt of its first choice or of a retry (res is its Resolution): the Workload's route order
// over our ModelDeployments and the managed targets, and else the Workload's own FIFO. While that FIFO holds a request that may take the route the new one
// would (competesLocked), a new one queues behind it without trying a route (strict: every waiter of a Workload with no sticky pin is such a request), and a
// request is tried and queued in one critical section. A request of a conversation with a sticky pin has one route, its target (Resolution.pin). A wake pass lists only the Workloads whose FIFO holds a
// request, so a wake that comes between the try and the enqueue finds none for this Workload and skips it; the arrival counts the wake requests that came
// meanwhile (wakeGen) and, when there were any, runs a pass of its own once it is queued (found by the tests of the wakes: the lost wake).
func (r *Router) chooseNamed(ctx context.Context, n naming.Name, res *Resolution, key string, exclude []string, withManaged bool, arrival time.Time) (*target, *routeError) {
	// The queue's deadline is taken here, at arrival, before the request is queued and before any wake pass it runs: a pass that waits for the pass lock
	// must not spend the request's queueTimeout before it starts. A request that waited for its conversation's key lock first carries the time it arrived at
	// (arrival), so the wait for the lock is part of its queueTimeout and not before it.
	if arrival.IsZero() {
		arrival = r.arrivalNow()
	}
	qctx, cancel := context.WithDeadline(ctx, arrival.Add(r.cfg.QueueTimeout.D()))
	defer cancel()
	// An arrival counts as active before it reads the view, so no wake pass skips on seeing none: a rebuild whose wake finds nothing active has changed the
	// view before this request reads it (wakeNow, wakeAsync).
	r.wlActive.Add(1)
	ws := r.lockWorkload(res)
	gen := r.wakeGen.Load() // before the view is read
	p, rerr := r.plan(n, res, key, exclude, withManaged)
	if rerr != nil {
		ws.mu.Unlock()
		r.wlActive.Add(-1)
		return nil, rerr
	}
	if r.beforePick != nil {
		r.beforePick()
	}
	if !ws.competesLocked(res.pin) {
		tg, canWait := r.tryNamedLocked(ws, p)
		if tg != nil {
			ws.mu.Unlock()
			r.wlActive.Add(-1)
			return tg, nil
		}
		if !canWait {
			ws.mu.Unlock()
			r.wlActive.Add(-1)
			return nil, errNoReadyTarget(n.Model).forWorkload(p.wname)
		}
	} else if !r.mayFreeLocked(p) {
		// Strict: a request behind a waiter tries no route. But one that no route may ever take, because nothing is eligible for it after its exclusions
		// and its level's filter, is not made to wait: it is answered as it would be alone, so a retry with no other Instance relays its first failure
		// and the FIFO is never held by a request that cannot be placed.
		ws.mu.Unlock()
		r.wlActive.Add(-1)
		return nil, errNoReadyTarget(n.Model).forWorkload(p.wname)
	}
	if h := r.beforeEnqueue; h != nil {
		h()
	}
	w := ws.enqueueLocked(p)
	ws.mu.Unlock()
	if r.wakeGen.Load() != gen {
		r.wakeNow()
	}
	return r.waitNamed(ctx, qctx, ws, w)
}

// arrivalNow is the time a request's queue deadline counts from. The tests of the deadline replace it.
func (r *Router) arrivalNow() time.Time {
	if f := r.arrivalClock; f != nil {
		return f()
	}
	return time.Now()
}

// mayFreeLocked reports whether some route of the request's serving set may free: an eligible managed target (at the Workload's bound it frees when one of
// its requests ends), or one of our Instances that is eligible for the request's level and not excluded (full, it frees when a request ends). It reserves
// nothing. With none, no wake can ever place the request.
func (r *Router) mayFreeLocked(p *plan) bool {
	if len(p.managed) > 0 {
		return true
	}
	if p.res.pin != nil {
		return p.pinWait || len(p.managed) > 0
	}
	match := levelMatch(p.n, p.exclude)
	for _, d := range p.serving {
		if g := p.groups[d.Key]; g != nil && g.HasEligible(match) {
			return true
		}
	}
	return false
}

// tryNamedLocked tries every route of the Workload once, in its route order, with the Workload's mutex held. It reserves nothing when it places nothing.
// canWait says a route exists that may free: one of our Instances that is eligible and full, or an eligible managed target at the Workload's bound
// (Q9: a full managed target with none of ours eligible waits in the Workload's FIFO, then 503 capacity).
func (r *Router) tryNamedLocked(ws *wlState, p *plan) (tg *target, canWait bool) {
	if h := r.holdAttempt; h != nil {
		h()
	}
	res := p.res
	avoided := false
	if res.sticky && res.pin == nil {
		// A first request of a sticky Workload leaves the targets that hold a pinned waiter to those waiters.
		if p.avoid = ws.pinnedWaitingLocked(); len(p.avoid) > 0 {
			avoided = p.avoidsATarget()
		}
	}
	if res.managedFirst {
		if tg = r.takeManagedLocked(ws, p); tg != nil {
			return tg, false
		}
	}
	tg, wait := r.chooseServed(p.serving, p.ring, p.groups, p.key, p.match(), res.load())
	if tg != nil {
		tg.workload, tg.wres = p.wname, res
		return tg, false
	}
	if !res.managedFirst {
		if tg = r.takeManagedLocked(ws, p); tg != nil {
			return tg, false
		}
	}
	return nil, wait != nil || len(p.managed) > 0 || p.pinWait || avoided
}

// takeManagedLocked reserves the Workload's count and returns a managed target when one is eligible and the Workload is below its managedSessions (no
// bound: no limit). The check and the reservation are one critical section under the Workload's mutex, so two requests at the last slot cannot both
// pass it.
func (r *Router) takeManagedLocked(ws *wlState, p *plan) *target {
	if len(p.managed) == 0 {
		return nil
	}
	if max := p.res.managedMax; max > 0 && ws.managed >= max {
		return nil
	}
	if h := r.afterManagedCheck; h != nil {
		h()
	}
	ws.managed++
	return r.managedTarget(ws, p)
}

// waitNamed holds a request in its Workload's FIFO for at most queueTimeout (HBEP-5, Workloads, Waiting). A wake places the head by tryHead and sends
// the target on w.ch.
func (r *Router) waitNamed(ctx, qctx context.Context, ws *wlState, w *wlWaiter) (*target, *routeError) {
	if h := r.onQueueDeadline; h != nil {
		d, _ := qctx.Deadline()
		h(d)
	}
	select {
	case res := <-w.ch:
		return res.tg, res.err
	case <-qctx.Done():
	}
	ws.lock()
	removed, wasHead := ws.removeLocked(w)
	ws.mu.Unlock()
	if !removed {
		// Answered just as it gave up: hand a reservation back.
		(<-w.ch).tg.release()
	}
	if wasHead || (w.res.sticky && w.res.pin == nil) {
		// The waiter behind it is the head now, and may fit a route that was free all along. For a sticky Workload a waiter that holds no pin, which holds
		// back every waiter behind it, may have been the one that did, wherever it queued (tryHead).
		r.wakeNow()
	}
	name := w.res.Name
	return nil, waitError(ctx, nil, "a route of the Workload "+name, "every route of the Workload "+name+" is at its limit").forWorkload(name)
}

// wakeNow runs a wake pass in the calling goroutine, which must hold no lock: a completion's goroutine, after the group's or the Workload's mutex is
// released. It skips only when no request is active, and an arrival counts as active before it reads the view.
func (r *Router) wakeNow() {
	r.wakeGen.Add(1)
	if r.wlActive.Load() == 0 && !r.wlOrphan.Load() {
		return
	}
	r.wakePass()
}

// recomputeOrphanLocked sets wlOrphan from the registry and the view's Workloads. The caller holds Router.mu, which both the rebuild and a pass take for
// it, so a flag set by one is never cleared by a pass that did not see its state.
func (r *Router) recomputeOrphanLocked() {
	r.wlMu.Lock()
	defer r.wlMu.Unlock()
	for key := range r.wls {
		if !r.wlKnown[key] {
			r.wlOrphan.Store(true)
			return
		}
	}
	r.wlOrphan.Store(false)
}

// wakeAsync hands a wake pass to a goroutine of its own: the rebuild's, which runs under Router.mu and so may not take a Workload's mutex. Wakes that
// come while a pass runs are folded into one more pass. It skips only when no Workload state exists, which is when there is nothing to wake or drop.
func (r *Router) wakeAsync() {
	r.wakeGen.Add(1)
	if r.wlActive.Load() == 0 && r.wlN.Load() == 0 {
		return
	}
	r.wakeAgain.Store(true)
	if !r.wakeRunning.CompareAndSwap(false, true) {
		return
	}
	go func() {
		for {
			for r.wakeAgain.Swap(false) {
				r.wakePass()
			}
			r.wakeRunning.Store(false)
			if !r.wakeAgain.Load() || !r.wakeRunning.CompareAndSwap(false, true) {
				return
			}
		}
	}()
}

// wakePass tries the head of each Workload's FIFO, the Workloads in the order of their oldest request's arrival, until none places. It runs after the
// group's own waiters were placed (Release and Update place them under the group's mutex, before the hooks that lead here), so a freed slot goes to a
// ModelDeployment's own waiters, which name no Workload, first, and then to the Workloads' FIFOs (Q5; HBEP-5, Workloads, Waiting). One pass at a time.
func (r *Router) wakePass() {
	if h := r.beforeWake; h != nil {
		h()
	}
	r.passMu.Lock()
	defer r.passMu.Unlock()
	for {
		progressed := false
		heads := r.wlByHead()
		if h := r.afterSort; h != nil {
			h()
		}
		for _, h := range heads {
			// A head answered, or a head that changed since the order was taken (its oldest request left): either way the order is taken again.
			if r.tryHead(h.ws, h.seq) != headNone {
				progressed = true
				break
			}
		}
		if !progressed {
			break
		}
	}
	r.pruneWorkloads()
}

// headed is a Workload's FIFO with the arrival number of its head when the order of a pass was taken.
type headed struct {
	ws  *wlState
	seq uint64
}

// wlByHead is the Workloads whose FIFO holds a request, the one with the oldest head first.
func (r *Router) wlByHead() []headed {
	r.wlMu.Lock()
	var hs []headed
	for _, ws := range r.wls {
		if seq := ws.headSeq.Load(); seq != 0 {
			hs = append(hs, headed{ws, seq})
		}
	}
	r.wlMu.Unlock()
	sort.Slice(hs, func(i, j int) bool { return hs[i].seq < hs[j].seq })
	return hs
}

// tryHead tries the head of one Workload's FIFO against the view as it is now, under the Workload's mutex, and delivers the target when it places. A
// head whose serving set has gone, or whose routes are all taken, keeps its place.
func (r *Router) tryHead(ws *wlState, seq uint64) headResult {
	ws.lock()
	defer ws.mu.Unlock()
	if len(ws.queue) == 0 {
		return headNone
	}
	if ws.queue[0].seq != seq {
		// The head the order was taken on left before this lock: the oldest request of this FIFO is another now, and the order must be taken again, or a
		// younger request of this FIFO could be placed ahead of an older one of another (Q5).
		return headStale
	}
	// The head is tried, and when it is a waiter that holds a pin and can't be placed, so are the waiters behind it, in order, except those that hold the
	// same pin: it blocks no request of another target (HBEP-5, Sticky routes). A waiter that holds none, which is every waiter of a Workload without
	// sessionAffinity, ends the scan when it can't be placed: it may take any route, so the FIFO is strict behind it.
	var blocked []pinTarget
	for i := 0; i < len(ws.queue); i++ {
		w := ws.queue[i]
		if i > 0 && r.olderHeadElsewhere(ws, w.seq) {
			// Behind a pinned head that can't be placed, a waiter takes a slot only when no other Workload's FIFO has an older head: the order across Workloads,
			// by the age of each head, is #786's, and a sticky Workload's scan past its blocked head does not change it.
			break
		}
		if w.res.pin != nil && slices.Contains(blocked, *w.res.pin) {
			continue
		}
		p, rerr := r.plan(w.n, w.res, w.key, w.exclude, w.withManaged)
		if rerr != nil {
			// Its serving set has gone, or its provider, or its pinned target: no route can take it. It leaves with the error, and the pass goes on to
			// the next waiter.
			ws.popAtLocked(i)
			w.ch <- wlResult{err: rerr.forWorkload(w.res.Name)}
			return headAnswered
		}
		tg, canWait := r.tryNamedLocked(ws, p)
		switch {
		case tg != nil:
			ws.popAtLocked(i)
			w.ch <- wlResult{tg: tg}
			return headAnswered
		case !canWait:
			// Nothing is eligible for it now, after its exclusions and its level's filter: no wake can place it, and it must not hold the waiters behind it.
			ws.popAtLocked(i)
			w.ch <- wlResult{err: errNoReadyTarget(w.n.Model).forWorkload(w.res.Name)}
			return headAnswered
		}
		if w.res.pin == nil {
			break
		}
		blocked = append(blocked, *w.res.pin)
	}
	return headNone
}

// olderHeadElsewhere reports whether the FIFO of another Workload holds a head older than the arrival number seq. It takes the registry's leaf lock under the
// Workload's mutex, as pruneWorkloads does.
func (r *Router) olderHeadElsewhere(ws *wlState, seq uint64) bool {
	r.wlMu.Lock()
	defer r.wlMu.Unlock()
	for _, o := range r.wls {
		if h := o.headSeq.Load(); o != ws && h != 0 && h < seq {
			return true
		}
	}
	return false
}

// headResult is what tryHead did with a FIFO's head.
type headResult int

const (
	headNone     headResult = iota // nothing placed or answered
	headAnswered                   // the head was placed or answered with an error
	headStale                      // the head was not the one the pass's order was taken on
)

// pruneWorkloads drops the state of each Workload that has left the view and has nothing in flight or waiting: its FIFO has drained (Q15).
func (r *Router) pruneWorkloads() {
	r.mu.Lock()
	known := r.wlKnown
	r.mu.Unlock()
	r.wlMu.Lock()
	all := make([]*wlState, 0, len(r.wls))
	for _, ws := range r.wls {
		if !known[ws.key] {
			all = append(all, ws)
		}
	}
	r.wlMu.Unlock()
	for _, ws := range all {
		ws.lock()
		if !ws.dead && len(ws.queue) == 0 && ws.managed == 0 {
			ws.dead = true
			r.wlMu.Lock()
			if r.wls[ws.key] == ws {
				delete(r.wls, ws.key)
				r.wlN.Add(-1)
			}
			r.wlMu.Unlock()
		}
		ws.mu.Unlock()
	}
	r.mu.Lock()
	r.recomputeOrphanLocked()
	r.mu.Unlock()
}

// wlWaitingTotal is the requests waiting in the Workloads' FIFOs.
func (r *Router) wlWaitingTotal() int {
	r.wlMu.Lock()
	defer r.wlMu.Unlock()
	n := 0
	for _, ws := range r.wls {
		n += int(ws.waiting.Load())
	}
	return n
}

// waitSample is one sample of the waiting series.
type waitSample struct {
	workload, deployment string
	n                    int
}

// waitingSamples is the requests waiting in each live group's queue and in each Workload's FIFO.
func (r *Router) waitingSamples() []waitSample {
	r.lockAsReader()
	groups := make(map[string]*balancer.Group, len(r.groups))
	for k, g := range r.groups {
		groups[k] = g
	}
	r.mu.Unlock()
	var out []waitSample
	for k, g := range groups {
		out = append(out, waitSample{deployment: k, n: g.Waiting()})
	}
	// The label is the Workload's name, as on every other series, and two Workloads of one name in different namespaces (of different models) share it.
	byName := map[string]int{}
	r.wlMu.Lock()
	for _, ws := range r.wls {
		byName[ws.name] += int(ws.waiting.Load())
	}
	r.wlMu.Unlock()
	for name, n := range byName {
		out = append(out, waitSample{workload: name, n: n})
	}
	return out
}
