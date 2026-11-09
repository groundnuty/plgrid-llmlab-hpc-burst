package alpha

import (
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
)

// The waits of the live steps (steps_live_test.go, live_flows_test.go, guard_live_test.go), named here because each
// row's worst case is added up from them: the steps use these, and hack/livechecks checks the script's timeout against
// the sum (review of PR 284, B5 and B6), so a longer wait that outgrows a row's timeout fails a test rather than a run.
//
// A wait of the stack's is named from the stack, never copied, so that a change there flows through: stack.CatchUpWorstCase, once for
// each catch-up on a row's path (liveWaits in budget_test.go lists them, so a new one fails until it is counted here).
const (
	// StartupWait is the stack's start, before the first clock runs: hpcburst up, then the controller's and the Provider's readiness.
	StartupWait = stack.UpTimeout + 2*stack.WaitDeadline
	// BudgetFactorPercent is the margin of every derived timeout, in the safe direction (the science agent, on PR 284): the waits the
	// code adds past a row's own bounds count this much of themselves, since a scan of the source can miss one. A timeout that fires
	// late costs only time; one that fires before its bounds is the bug. A row's bounds in its table (a start wait, the wait
	// in the queue, a time limit) are exact and cannot be missed, so they count once, and a long wait in the queue is not
	// inflated by a quarter for nothing.
	BudgetFactorPercent = 125
	// TunnelRunOver is what a tunnel row's run budget, and the script's timeout for it, keep over its lease: what follows the
	// lease in the worst case, with the factor (TestEachStackRowsWorstCaseIsItsWaitsAddedUp), and the two minutes before
	// the test's deadline at which its waits give up.
	TunnelRunOver = 29 * time.Minute
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
//   - A tunnel row: the job's drain start, a drain window and EndMargin for the job to end, one catch-up, Close.
//   - The handover: the drain start, a request's deadline for the streams that cross it, a drain window and EndMargin
//     for the primary to end, the guard's last looks, two catch-ups (the worker restart's and the result's), Close. Its
//     successor must be ready by the drain start, so its second job has no wait of its own.
//   - The no-overlap row: the predecessor's lease end, SuccessorWait, the guard's last looks, two catch-ups, Close.
//
// A catch-up on a path that then fails the row (whyGone) is not counted: nothing follows it.
func (r Row) WorstCase(pendingMax time.Duration) time.Duration {
	tm := r.Clock()
	dw := DrainWindow(tm.MaxRequestDuration, tm.DrainMargin, 0)
	first, _ := FirstClock(r, pendingMax, r.Lease, dw)
	lastLooks := GuardResultLooks * GuardEvery
	switch r.Name {
	case "alpha-athena-handover":
		return StartupWait + first + (r.Lease - dw) + tm.MaxRequestDuration + dw + EndMargin + lastLooks + 2*stack.CatchUpWorstCase + CloseTimeout
	case "alpha-helios-nooverlap":
		return StartupWait + first + r.Lease + SuccessorWait(tm, pendingMax) + lastLooks + 2*stack.CatchUpWorstCase + CloseTimeout
	}
	return StartupWait + first + (r.Lease - dw) + dw + EndMargin + stack.CatchUpWorstCase + CloseTimeout
}

// Scaled is the waits the code adds past a row's bounds, with the margin (BudgetFactorPercent).
func Scaled(waits time.Duration) time.Duration { return waits * BudgetFactorPercent / 100 }

// bounds is the part of WorstCase that is the row's own bounds in its table: its wait in the queue, one for each wait it has, and
// its lease.
func (r Row) bounds(pendingMax time.Duration) time.Duration {
	tm := r.Clock()
	first, _ := FirstClock(r, pendingMax, r.Lease, DrainWindow(tm.MaxRequestDuration, tm.DrainMargin, 0))
	if r.Name == "alpha-helios-nooverlap" {
		first += pendingMax
	}
	return first + r.Lease
}

// MinTimeout is the shortest a stack row's timeout may be: its bounds, and the rest of its worst case with the margin.
func (r Row) MinTimeout(pendingMax time.Duration) time.Duration {
	b := r.bounds(pendingMax)
	return b + Scaled(r.WorstCase(pendingMax)-b)
}
