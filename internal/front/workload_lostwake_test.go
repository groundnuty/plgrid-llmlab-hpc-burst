package front

import "testing"

// A wake that comes between a request's failed try and its enqueue (found by the tests of the wakes): the request tried a-0, which was full, and holds
// the Workload's mutex, not yet in the FIFO. a-0's holder completes meanwhile and its wake pass finds no Workload with a waiter, so it places nothing.
// The request, once queued, must still be placed on the a-0 that freed, not wait for queueTimeout (set long, so that a lost wake hangs to the wait's
// own deadline). The seam holds the request at the instant, so the interleaving is forced and never a window.
func TestAWakeBetweenARequestsFailedTryAndItsEnqueueIsNotLost(t *testing.T) {
	f := newWrFix(t, wrOpts{cap: 1, holdOurs: true, noManaged: true, edit: wkQueueLong})
	_, cutHolder := f.start(f.alpha(), "lw-holder")
	wkSettledOurs(f, 1, 0)
	paused, gate := make(chan struct{}), make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-gate:
		default:
			close(gate)
		}
	})
	f.router.beforeEnqueue = func() { close(paused); <-gate }
	f.start(f.alpha(), "lw-second")
	<-paused // the second request tried a-0, found it full, and is about to queue
	cutHolder()
	errWaitFor(t, "the holder's completion to free a-0", func() bool { return f.openOurs() == 0 })
	close(gate) // the second request queues, a-0 being free
	errWaitFor(t, "the second request to be placed on the freed a-0", func() bool {
		return len(f.engines["ns/a-0"].Requests()) == 2 && f.waiting() == 0
	})
}
