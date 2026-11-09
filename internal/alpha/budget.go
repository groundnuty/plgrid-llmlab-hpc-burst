package alpha

import "time"

// The waits of the live steps (steps_live_test.go, live_flows_test.go, guard_live_test.go), named here because each
// row's worst case is added up from them: the steps use these, and hack/livechecks checks the script's timeout against
// the sum (review of PR 284, B5 and B6), so a longer wait that outgrows a row's timeout fails a test rather than a run.
const (
	// StartupWait is the stack's start, before the first clock runs: hpcburst up (stack.UpTimeout), then the controller's and the
	// Provider's readiness (stack.WaitDeadline each).
	StartupWait = 11 * time.Minute
	// TunnelRunOver is what a tunnel row's run budget, and the script's timeout for it, keep over its lease: the startup, the job's
	// end and Close, and a margin.
	TunnelRunOver = 25 * time.Minute
	// CloseTimeout bounds Close: hpcburst down and the cancel of the run's jobs.
	CloseTimeout = 5 * time.Minute
	// ReadyMargin is what a job that started has beyond health.startTimeout to become ready.
	ReadyMargin = 5 * time.Minute
	// EndMargin is what a job has beyond the drain window to end.
	EndMargin = 5 * time.Minute
	// GuardEvery is how often the two-job guard looks at the cluster, and GuardResultLooks how many of its looks
	// the run gives it to set the successor's drain start.
	GuardEvery       = 10 * time.Second
	GuardResultLooks = 6
)

// SuccessorWait is how long a no-overlap row waits, from the predecessor's lease end, for its successor to be ready:
// the wait in the queue the job may take once the GPU is free, then the start it needs. A job not started when the wait
// in the queue ends goes no further (successorOf).
func SuccessorWait(tm Timing, pendingMax time.Duration) time.Duration {
	return pendingMax + tm.StartTimeout + ReadyMargin
}

// RunDeadline is the deadline of a row's whole run, its Close aside: one wait in the queue for each of its jobs, then
// its run budget. A no-overlap row's successor waits in the queue after its predecessor did, on top of the budget.
func (r Row) RunDeadline(pendingMax time.Duration) time.Duration {
	return time.Duration(r.MostJobs)*pendingMax + r.Budget
}

// WorstCase is the longest a stack row's test can take when every wait it has runs to its end, added up from the
// steps' own waits: the stack's start, the first job's wait in the queue (FirstClock), its lease, then what comes after, Close included.
// The script's timeout for the row must be above it, or the timeout fires before the row's bound can.
//
//   - A tunnel row: the job's drain start, a drain window and EndMargin for the job to end, Close.
//   - The handover: the drain start, a request's deadline for the streams that cross it, a drain window and EndMargin
//     for the primary to end, the guard's last looks, Close. Its successor must be ready by the drain start, so its
//     second job has no wait of its own.
//   - The no-overlap row: the predecessor's lease end, SuccessorWait, the guard's last looks, Close.
func (r Row) WorstCase(pendingMax time.Duration) time.Duration {
	tm := r.Clock()
	dw := DrainWindow(tm.MaxRequestDuration, tm.DrainMargin, 0)
	first, _ := FirstClock(r, pendingMax, r.Lease, dw)
	lastLooks := GuardResultLooks * GuardEvery
	switch r.Name {
	case "alpha-athena-handover":
		return StartupWait + first + (r.Lease - dw) + tm.MaxRequestDuration + dw + EndMargin + lastLooks + CloseTimeout
	case "alpha-helios-nooverlap":
		return StartupWait + first + r.Lease + SuccessorWait(tm, pendingMax) + lastLooks + CloseTimeout
	}
	return StartupWait + first + (r.Lease - dw) + dw + EndMargin + CloseTimeout
}
