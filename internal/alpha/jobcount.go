package alpha

// The job counts of the stack rows (issue #245). With no queue wait observed,
// the lifecycle controller creates a primary's replacement at the primary's
// start, and a successor's own replacement before the predecessor's drain
// start (HBEP-7, Handover): a tunnel row would hold two jobs and a handover
// row three, where the runbook promises one and two. A deployment that ends at
// or before an Instance's drain start gives it no replacement
// (internal/lifecycle/handover.go, replacements), so the live steps place the
// deployment's end with these functions. The choice awaits the science
// agent's ruling (the PR of #245).

import (
	"errors"
	"fmt"
	"time"
)

// FirstEndOfOne is the end a deployment of one job is created with: the
// earliest drain start its job can have, the lease less the drain window after
// the deployment's creation, so that no replacement is planned at the job's
// start, whenever it comes. API times are whole seconds.
func FirstEndOfOne(created time.Time, lease, drainWindow time.Duration) time.Time {
	return created.Add(lease - drainWindow).Truncate(time.Second)
}

// EndOfOne is the end of a deployment of one job once its Instance's drain
// start is known: a minute before it, so that no rounding puts it after.
func EndOfOne(drainStart time.Time) time.Time {
	return drainStart.Add(-time.Minute).Truncate(time.Second)
}

// EndOfTwo is the end of a deployment of two jobs once its successor's drain
// start is known: that drain start, where the successor's lease drains it
// anyway. An end at or before the predecessor's drain start would stop the
// handover, so that is an error.
func EndOfTwo(predDrainStart, succDrainStart time.Time) (time.Time, error) {
	end := succDrainStart.Truncate(time.Second)
	if !end.After(predDrainStart) {
		return time.Time{}, fmt.Errorf("the successor's drain start %s is not after the predecessor's %s: the deployment keeps no end",
			end.UTC().Format(time.RFC3339), predDrainStart.UTC().Format(time.RFC3339))
	}
	return end, nil
}

// ErrQueueWaitPastEnd is a row of one job whose allowed queue wait reaches its
// deployment's first end: the deployment would end before a job that waited
// that long could move it.
var ErrQueueWaitPastEnd = errors.New("a tunnel row's queue wait (HPCBURST_ALPHA_PENDING_MAX_SECONDS) must end a minute before its deployment's first end, the lease less the drain window: lower the wait, or choose a longer lease (LIVE_TUNNEL_LEASE_MINUTES)")

// CheckQueueWait refuses a row of one job whose queue wait does not end at
// least a minute before FirstEndOfOne: the job must start, and the run move
// the end, before the deployment's first end.
func CheckQueueWait(row Row, pendingMax, drainWindow time.Duration) error {
	if row.MostJobs == 1 && pendingMax+time.Minute > row.Lease-drainWindow {
		return fmt.Errorf("%w (wait %s, first end %s after the deployment)", ErrQueueWaitPastEnd, pendingMax, row.Lease-drainWindow)
	}
	return nil
}
