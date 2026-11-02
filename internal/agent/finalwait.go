package agent

import (
	"context"
	"sync/atomic"
	"time"
)

// What the agent logs when the last report meets a periodic report still in
// flight (issue #220). The tests match on these.
const (
	// logWaitingForSend is logged (Info) when the last report waits for a
	// periodic send in flight.
	logWaitingForSend = "a periodic report is in flight; the last report waits for it"
	// logOpenSendAtLast is logged (Warn) when the wait reached its bound with
	// the send still open: that send can still reach the endpoint after the
	// last report (issue #221 tracks the full fix, a sequence number).
	logOpenSendAtLast = "sending the last report with a periodic send still open"
)

// startTimer starts a timer for d and returns its channel and the function
// that stops it. The waits at the agent's end go through it, so that a test
// can control the progress of time: production never sets timerHook.
func startTimer(d time.Duration) (<-chan time.Time, func()) {
	if f := timerHook.Load(); f != nil {
		return (*f)(d)
	}
	t := time.NewTimer(d)
	return t.C, func() { t.Stop() }
}

var timerHook atomic.Pointer[func(d time.Duration) (<-chan time.Time, func())]

// jobEndWait is how long an agent whose job ends waits for a periodic send in
// flight: half of jobEndReportBudget, so that the last report keeps the rest of
// the 5 s this path always had.
func jobEndWait() time.Duration { return min(finalReportTimeout, jobEndReportBudget/2) }

// awaitPeriodicSend waits up to wait for the periodic send in flight, if any,
// to finish (the periodic reports were closed when the stop began). A send
// still open at the bound is logged at Warn and cancelled, which ends the call
// here only: the last report goes anyway, and that send can still reach the
// endpoint after it (the known limit; issue #221 tracks the full fix).
//
// When jobCtx ends during the wait, a job cancellation has arrived: what is
// left of the wait is cut to jobEndWait, and the time of the cancellation is
// returned (the zero time if it did not come).
func (a *Agent) awaitPeriodicSend(jobCtx context.Context, cancelRep context.CancelFunc, wait time.Duration) (termAt time.Time) {
	if a.rep.sendOpen() {
		a.log.Info(logWaitingForSend, "wait", wait)
	}
	started := time.Now()
	timerC, stop := startTimer(wait)
	termC := jobCtx.Done()
	for {
		select {
		case <-a.rep.done:
			stop()
			return termAt
		case <-timerC:
			a.log.Warn(logOpenSendAtLast, "wait", wait)
			cancelRep()
			return termAt
		case <-termC:
			termC, termAt = nil, time.Now()
			stop()
			timerC, stop = startTimer(min(wait-time.Since(started), jobEndWait()))
		}
	}
}

// sendLastReport sends the last report after a stop instruction, with every
// worker stopped: it waits for the periodic send in flight (awaitPeriodicSend),
// then tries for up to finalReportTimeout. A job cancellation (jobCtx ending)
// during either cuts what is left to the job-end budget: jobEndReportBudget
// from the cancellation, which Slurm's KillWait follows.
func (a *Agent) sendLastReport(jobCtx context.Context, cancelRep context.CancelFunc) error {
	termAt := a.awaitPeriodicSend(jobCtx, cancelRep, finalReportTimeout)
	tries := finalReportTimeout
	if !termAt.IsZero() {
		tries = min(tries, jobEndReportBudget-time.Since(termAt))
	}
	ctx, cancel := context.WithTimeout(context.Background(), tries)
	defer cancel()
	if termAt.IsZero() {
		defer clampAtTerm(jobCtx, cancel)()
	}
	return a.rep.final(ctx, a.snapshot())
}

// clampAtTerm cancels the last report's tries jobEndReportBudget after jobCtx
// ends, if they are still going; the returned function ends the watch.
func clampAtTerm(jobCtx context.Context, cancel context.CancelFunc) (stop func()) {
	quit := make(chan struct{})
	go func() {
		select {
		case <-quit:
		case <-jobCtx.Done():
			c, stopTimer := startTimer(jobEndReportBudget)
			defer stopTimer()
			select {
			case <-quit:
			case <-c:
				cancel()
			}
		}
	}()
	return func() { close(quit) }
}

// sendLastReportAtJobEnd sends the last report of an agent whose job ended,
// with its workers stopped: the wait for a periodic send in flight and the one
// try share the 5 s of jobEndReportBudget from deadline's start.
func (a *Agent) sendLastReportAtJobEnd(cancelRep context.CancelFunc) error {
	deadline := time.Now().Add(jobEndReportBudget)
	a.awaitPeriodicSend(context.Background(), cancelRep, jobEndWait())
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	return a.rep.send(ctx, a.snapshot(), false)
}
