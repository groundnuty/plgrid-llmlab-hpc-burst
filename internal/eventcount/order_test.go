package eventcount

// The order of one route's raises (the late review of #697's first delta, 17 Nov 2026): the notifier is one
// goroutine, and a closing that waits for it can be updated in place.

import (
	"strings"
	"testing"
	"time"
)

// Within one Lease read: run 1's loss job blocks in it, run 1 ends and its closing is queued, run 2 starts and its loss
// is queued behind the closing, and run 2 ends, so the waiting closing is updated in place to cover both runs. The log
// must not say, in the order the notifier emits them, that it takes events again and then that it is losing events
// for a run the closing covers.
func TestALossIsNeverEmittedAfterAClosingThatCoversItsRun(t *testing.T) {
	ctl := ControllerProvider("p1")
	l := newBlockedLease("provider-p1", 1) // the first read waits: run 1's loss job
	r := newRig(t, ctl, false, bigCaps, nil, l.read)
	r.pub.leaseTimeoutFor = time.Hour
	rt := r.route(ctl)
	rt.lost(lostBudget, "JobStarted", "ord-1", nil, nil) // run 1: its loss job takes the notifier and waits in the Lease read
	within(t, 20*time.Second, "run 1's Lease read", func() bool { return l.calls.Load() == 1 })
	rt.logged(&item{r: rt, id: "ok"})                    // run 1 ends: its closing is queued
	rt.lost(lostBudget, "JobStarted", "ord-2", nil, nil) // run 2 starts: its loss is queued behind the closing
	rt.logged(&item{r: rt, id: "ok"})                    // run 2 ends: the waiting closing is updated in place to cover both
	close(l.release)
	within(t, 20*time.Second, "the notifier to finish", func() bool { return r.pendingJobs() == 0 && rt.gateIdle() })
	var seq []string
	for _, c := range r.inner.byReason(ReasonEventsNotLogged) {
		if strings.Contains(c.note, "takes events again") {
			seq = append(seq, "closing")
		} else {
			seq = append(seq, "loss")
		}
	}
	closed := false
	for i, k := range seq {
		if k == "closing" {
			closed = true
		} else if closed {
			t.Errorf("raised in the order %v: the loss at %d comes after a closing that covers its run (the log says it takes events again, then that it is losing them)", seq, i)
			break
		}
	}
}

