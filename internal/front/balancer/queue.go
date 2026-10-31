package balancer

import "context"

// waiter is a request held at the front because every eligible member it
// may use is at its cap (HBEP-5, Load bound and cap: the FIFO).
type waiter struct {
	key   string
	match Match
	ch    chan *Reservation // buffered: placement never blocks
}

// PickOrWait is TryPick, except that when every eligible matching member is
// at its cap the request waits in the group's FIFO until a completion, a
// join, an Instance that is eligible again or the cap going off makes room,
// or until ctx ends. The caller bounds ctx with queueTimeout. A request that
// gives up while being placed has that reservation released.
func (g *Group) PickOrWait(ctx context.Context, key string, match Match) (*Reservation, error) {
	g.lock()
	r, err := g.pickLocked(key, match)
	if err != ErrAllAtCap {
		g.mu.Unlock()
		return r, err
	}
	w := &waiter{key: key, match: match, ch: make(chan *Reservation, 1)}
	g.queue = append(g.queue, w)
	g.mu.Unlock()

	select {
	case r := <-w.ch:
		return r, nil
	case <-ctx.Done():
	}
	g.lock()
	if g.removeWaiterLocked(w) {
		g.mu.Unlock()
		return nil, ctx.Err()
	}
	g.mu.Unlock()
	// Placed just as it gave up: hand the reservation back.
	(<-w.ch).Release()
	return nil, ctx.Err()
}

// Waiting is the number of requests in the FIFO.
func (g *Group) Waiting() int {
	g.lock()
	defer g.mu.Unlock()
	return len(g.queue)
}

func (g *Group) removeWaiterLocked(w *waiter) bool {
	for i, q := range g.queue {
		if q == w {
			g.queue = append(g.queue[:i], g.queue[i+1:]...)
			return true
		}
	}
	return false
}

// placeWaitersLocked places waiting requests oldest first. A waiter that
// still does not fit keeps its place; a younger one whose filter admits a
// member with room is placed past it.
func (g *Group) placeWaitersLocked() {
	if len(g.queue) == 0 {
		return
	}
	kept := g.queue[:0]
	for _, w := range g.queue {
		r, err := g.pickLocked(w.key, w.match)
		if err != nil {
			kept = append(kept, w)
			continue
		}
		w.ch <- r
	}
	for i := len(kept); i < len(g.queue); i++ {
		g.queue[i] = nil
	}
	g.queue = kept
}
