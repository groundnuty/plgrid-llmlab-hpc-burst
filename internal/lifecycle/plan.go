package lifecycle

import (
	"slices"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The replacement plan (HBEP-7, Handover; open question 2). Every input lives
// in status: the Provider's queue waits (HBEP-4), the ModelDeployment's
// start-to-ready time per class, the Allocation's start and hard end. So a
// restarted controller plans the same time.

// p90Wait is the planning wait of a Provider's observed queue waits (HBEP-7,
// open question 2): with MaxQueueWaits waits or more, their 90th percentile by
// the nearest-rank method, the ceil(0.9 * n)-th smallest (the 18th of 20);
// with fewer, the largest; with none, ok is false. HBEP-7 names the p90 but no
// method; nearest rank picks an observed wait and needs no interpolation.
func p90Wait(waits []v1alpha1.QueueWait) (time.Duration, bool) {
	if len(waits) == 0 {
		return 0, false
	}
	ds := make([]time.Duration, len(waits))
	for i, w := range waits {
		ds[i] = w.Wait()
	}
	slices.Sort(ds)
	if len(ds) < v1alpha1.MaxQueueWaits {
		return ds[len(ds)-1], true
	}
	rank := (9*len(ds) + 9) / 10 // ceil(0.9 * n)
	return ds[rank-1], true
}

// startToReady is the class's last measured start-to-ready time, or 30 min
// until one is measured (HBEP-7, Handover).
func startToReady(md *v1alpha1.ModelDeployment, class v1alpha1.AcceleratorClass) time.Duration {
	for _, s := range md.Status.StartToReady {
		if s.AcceleratorClass == class {
			if d, err := s.Duration.AsDuration(); err == nil {
				return d
			}
		}
	}
	return defaultStartToReady
}

// replacementTime is when a primary's replacement is created: its drain start
// minus (the planning wait + the class's start-to-ready time + the margin),
// never before the primary's Allocation started. With no wait observed it is
// the primary's start.
func replacementTime(drainStart, primaryStart time.Time, waits []v1alpha1.QueueWait, s2r, margin time.Duration) time.Time {
	wait, ok := p90Wait(waits)
	if !ok {
		return primaryStart
	}
	t := drainStart.Add(-(wait + s2r + margin))
	if t.Before(primaryStart) {
		return primaryStart
	}
	return t
}

// lateAt is when a replacement whose Allocation has not started is late: the
// predecessor's drain start minus the class's start-to-ready time (HBEP-7,
// Handover, lateness). A replacement that starts later cannot be ready before
// the drain starts.
func lateAt(drainStart time.Time, s2r time.Duration) time.Time {
	return drainStart.Add(-s2r)
}

// timeLimit is an Allocation's time limit: the partition's maxTime, or the
// deployment's leaseLimit when that is shorter, rounded down to whole minutes
// (HBEP-7, Provisioning; docs/development.md, Time limits). It returns false
// when neither is known or the result is below a minute.
func timeLimit(maxTime, leaseLimit *time.Duration) (time.Duration, bool) {
	var lim time.Duration
	switch {
	case maxTime != nil && leaseLimit != nil:
		lim = min(*maxTime, *leaseLimit)
	case maxTime != nil:
		lim = *maxTime
	case leaseLimit != nil:
		lim = *leaseLimit
	default:
		return 0, false
	}
	lim = lim.Truncate(time.Minute)
	return lim, lim >= time.Minute
}

// formatMinutes writes a whole-minute duration as the API keeps it, for
// example "4h0m0s".
func formatMinutes(d time.Duration) v1alpha1.Duration {
	return v1alpha1.Duration(d.Truncate(time.Minute).String())
}
