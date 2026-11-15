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
// its run budget. A no-overlap row's successor waits in the queue after its predecessor did, on top of the budget. A row
// with a start wait has its budget alone: the budget holds each job's start wait, as the shape's timeout column does for
// every row with one (hack/live-checks.sh, execute_cluster).
func (r Row) RunDeadline(pendingMax time.Duration) time.Duration {
	if r.StartWait > 0 {
		return r.Budget
	}
	return time.Duration(r.MostJobs)*pendingMax + r.Budget
}

// StageAgentWait bounds the bulk row's staging of the shipped agent its test shim execs: an agent build of tens of MB written through the run's
// ssh to the Provider's staging root, before the deployment is created (issue #521, option (A)).
const StageAgentWait = 5 * time.Minute

// WorstCase is the longest a stack row's test can take when every wait it has runs to its end, added up from the
// steps' own waits: the stack's start, the first job's wait in the queue (FirstClock), its lease, then what comes after, Close included.
// The script's timeout for the row must be above it, or the timeout fires before the row's bound can.
//
//   - A tunnel row: the job's drain start, a drain window and EndMargin for the job to end, one catch-up, Close.
//   - The handover: the drain start, a request's deadline for the streams that cross it, a drain window and EndMargin
//     for the primary to end, the guard's last looks, two catch-ups (the worker restart's and the result's), Close. Its
//     successor must be ready by the drain start, so its second job has no wait of its own.
//   - The no-overlap row: the predecessor's lease end, SuccessorWait, the guard's last looks, two catch-ups, Close.
//   - Row B on plgrid-now (alpha-athena-now-nooverlap): its start wait, the predecessor's lease and EndMargin for its job
//     to be seen gone from the slot, the successor's Allocation (SuccessorAllocationWait) and its own start wait, then
//     health.startTimeout and ReadyMargin for it to be ready, the guard's last looks, a drain window and EndMargin for the
//     jobs to end once the deployment goes, two catch-ups (the result's and the deployment end's), Close.
//   - The keeper row on plgrid-now (beta-athena-now-keeper, issue #521) has the router row's sum: its stream, the controller's restart and every wait of
//     the keeperprobe package are steps before the drain start, which the lease covers.
//   - The router row on plgrid-now (beta-athena-now-router, issue #521): row A's, less the events read last, which it does not read: its
//     start wait; every step before the drain start (the glibc and Envoy probes, the response-level reading, the worker restart); then a
//     drain window and EndMargin for the job to end once the deployment goes, two catch-ups (the worker restart's and the deployment's
//     end's), Close.
//   - Row A on plgrid-now (alpha-athena-now-serve): its start wait; every step before the drain start; the events, read
//     last (stack.WaitDeadline); then a drain window and EndMargin for the job to end once the deployment goes, two
//     catch-ups (the worker restart's and the deployment's end's), Close.
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
	case BetaRouterName, BetaHeliosRouterName, BetaKeeperName:
		return StartupWait + first + (r.Lease - dw) + dw + EndMargin + 2*stack.CatchUpWorstCase + CloseTimeout
	case BetaBulkName:
		// The router row's sum, and the staging of the shipped agent the test shim execs, before the deployment (issue #521, option (A)).
		return StartupWait + StageAgentWait + first + (r.Lease - dw) + dw + EndMargin + 2*stack.CatchUpWorstCase + CloseTimeout
	case "alpha-athena-now-serve":
		return StartupWait + first + (r.Lease - dw) + stack.WaitDeadline + dw + EndMargin + 2*stack.CatchUpWorstCase + CloseTimeout
	case "alpha-athena-now-nooverlap":
		return StartupWait + first + r.Lease + EndMargin + SuccessorAllocationWait + r.StartWait + tm.StartTimeout + ReadyMargin + lastLooks +
			dw + EndMargin + 2*stack.CatchUpWorstCase + CloseTimeout
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
	switch r.Name {
	case "alpha-helios-nooverlap":
		first += pendingMax
	case "alpha-athena-now-nooverlap":
		// The successor's own start wait, from its Allocation's creation.
		first += r.StartWait
	}
	return first + r.Lease
}

// MinTimeout is the shortest a stack row's timeout may be: its bounds, and the rest of its worst case with the margin.
func (r Row) MinTimeout(pendingMax time.Duration) time.Duration {
	b := r.bounds(pendingMax)
	return b + Scaled(r.WorstCase(pendingMax)-b)
}
