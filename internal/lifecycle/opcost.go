package lifecycle

// The release cost of a plan target (HBEP-10, The release cost; #589 plan, section 8). Every function here
// is pure: a figure is a function of what the pass read and of at, the plan's observation time, never of
// the clock. A figure that applies and cannot be known carries its reason in Unknown and never a zero
// (HBEP-8, Notes: unknown is a value).

import (
	"fmt"
	"slices"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The reasons of an unknown figure.
const (
	unknownHardEnd       = "the job's hard end is not observed yet"
	unknownSubmitTime    = "the scheduler has not accepted the job yet"
	unknownStartEstimate = "the platform gives no start estimate for the job"
	unknownQueueWaits    = "the Provider has recorded no queue waits yet"
	unknownNoProvider    = "the Provider is not read, so its queue waits are not known"
	unknownStartToReady  = "not measured yet; the controller plans with 30 min"
	unknownEstimateNow   = "the estimate operation is beta step 4 (HBEP-4 amendment pending)"
)

// costInput is what one target's cost reads.
type costInput struct {
	// at is the plan's observation time.
	at time.Time
	// alloc is the target's Allocation (an Instance target's own), nil when it has none: only the
	// figures that need none are then given.
	alloc *v1alpha1.Allocation
	// prov is the Allocation's Provider, nil when it is not read.
	prov *v1alpha1.Provider
	// class is the Provider's accelerator class, nil when the profile does not name its partition; the
	// ModelDeployment's start-to-ready list is read for it.
	class *v1alpha1.AcceleratorClass
	// startToReady is the ModelDeployment's status.startToReady.
	startToReady []v1alpha1.ClassStartToReady
	// lastReady marks the deployment's last ready Instance.
	lastReady bool
}

// releaseCost is the cost of one target that lowers capacity: what it gives up and what getting it back
// costs (HBEP-10, The release cost).
func releaseCost(in costInput) *v1alpha1.ReleaseCost {
	at := metav1.NewTime(in.at)
	c := &v1alpha1.ReleaseCost{LastReady: in.lastReady}
	if a := in.alloc; a != nil {
		obs := a.Status.AtProvider
		if obs.StartTime != nil {
			c.LeaseLeft, c.GPUHoursLeft = leaseFigures(obs.HardEnd, allocGPUs(a), in.at)
		} else {
			c.QueueAge = queueAgeFigure(obs.SubmitTime, in.at)
			c.StartEstimate = startEstimateFigure(obs.StartEstimate, in.at)
		}
	}
	c.QueueWaits = queueWaitsFigure(in.prov)
	c.StartToReady = startToReadyFigure(in.startToReady, in.class)
	// Always unknown in step 1: no sbatch --test-only is made, and nothing plans one.
	c.EstimateNow = &v1alpha1.TimeFigure{Unknown: unknownEstimateNow, ObservedAt: &at, Advisory: true}
	return c
}

func allocGPUs(a *v1alpha1.Allocation) int32 {
	if a.Spec.ForProvider.Batch == nil {
		return 0
	}
	return a.Spec.ForProvider.Batch.GPUs
}

// leaseFigures are a started Allocation's lease left, its hard end minus at, and the GPU-hours that
// leaves: the lease left times its GPUs, to one decimal. A hard end that has passed leaves none (zero is
// the value then, since the hard end is known).
func leaseFigures(hardEnd *metav1.Time, gpus int32, at time.Time) (*v1alpha1.DurationFigure, *v1alpha1.GPUHoursFigure) {
	obs := metav1.NewTime(at)
	if hardEnd == nil {
		return &v1alpha1.DurationFigure{Unknown: unknownHardEnd, ObservedAt: &obs},
			&v1alpha1.GPUHoursFigure{Unknown: unknownHardEnd, ObservedAt: &obs}
	}
	left := max(hardEnd.Sub(at), 0)
	return &v1alpha1.DurationFigure{Duration: &metav1.Duration{Duration: left}, ObservedAt: &obs},
		&v1alpha1.GPUHoursFigure{GPUHours: gpuHours(left, gpus), ObservedAt: &obs}
}

// gpuHours is d times gpus, in hours, as a decimal string with one decimal, rounded half up on the
// tenth: "36.0". Integer arithmetic only, so no float is rounded on the way.
func gpuHours(d time.Duration, gpus int32) string {
	secs := int64(d / time.Second)
	tenths := (secs*int64(gpus)*10 + 1800) / 3600
	return fmt.Sprintf("%d.%d", tenths/10, tenths%10)
}

// queueAgeFigure is a pending Allocation's age in the queue: at minus the scheduler's submit time.
func queueAgeFigure(submit *metav1.Time, at time.Time) *v1alpha1.DurationFigure {
	obs := metav1.NewTime(at)
	if submit == nil {
		return &v1alpha1.DurationFigure{Unknown: unknownSubmitTime, ObservedAt: &obs}
	}
	return &v1alpha1.DurationFigure{Duration: &metav1.Duration{Duration: max(at.Sub(submit.Time), 0)}, ObservedAt: &obs}
}

// startEstimateFigure is a pending Allocation's own estimate of its start, advisory.
func startEstimateFigure(est *metav1.Time, at time.Time) *v1alpha1.TimeFigure {
	obs := metav1.NewTime(at)
	if est == nil {
		return &v1alpha1.TimeFigure{Unknown: unknownStartEstimate, ObservedAt: &obs, Advisory: true}
	}
	t := *est
	return &v1alpha1.TimeFigure{Time: &t, ObservedAt: &obs, Advisory: true}
}

// queueWaitsFigure is what getting the capacity back costs: the median, the p90 and the largest of the
// Provider's recorded queue waits, each wait its start time minus its submit time, with their count and
// the Provider's last successful read as the observation time.
func queueWaitsFigure(prov *v1alpha1.Provider) *v1alpha1.QueueWaitsFigure {
	if prov == nil {
		return &v1alpha1.QueueWaitsFigure{Unknown: unknownNoProvider}
	}
	var obs *metav1.Time
	if t := prov.Status.LastSuccessfulStatusRead; t != nil {
		c := *t
		obs = &c
	}
	waits := make([]time.Duration, 0, len(prov.Status.QueueWaits))
	for _, w := range prov.Status.QueueWaits {
		waits = append(waits, w.Wait())
	}
	if len(waits) == 0 {
		return &v1alpha1.QueueWaitsFigure{Unknown: unknownQueueWaits, ObservedAt: obs}
	}
	slices.Sort(waits)
	n := len(waits)
	dur := func(d time.Duration) *metav1.Duration { return &metav1.Duration{Duration: d} }
	return &v1alpha1.QueueWaitsFigure{
		Median:     dur(median(waits)),
		P90:        dur(waits[nearestRank(n, 90)-1]),
		Max:        dur(waits[n-1]),
		Count:      int32(n),
		ObservedAt: obs,
	}
}

// median is the middle of sorted ds: the middle one for an odd count, the mean of the two middle ones for
// an even count.
func median(ds []time.Duration) time.Duration {
	n := len(ds)
	if n%2 == 1 {
		return ds[n/2]
	}
	return (ds[n/2-1] + ds[n/2]) / 2
}

// nearestRank is the 1-based rank of the pct-th percentile of n values by the nearest-rank method:
// ceil(pct/100 * n), at least 1.
func nearestRank(n, pct int) int {
	return max((pct*n+99)/100, 1)
}

// startToReadyFigure is the class's last measured start-to-ready time, so the time to serve again is about
// the queue wait plus this. With no entry for the class it is unknown; it is never the 30 minutes the
// controller plans with, which is a plan, not a measurement.
func startToReadyFigure(list []v1alpha1.ClassStartToReady, class *v1alpha1.AcceleratorClass) *v1alpha1.DurationFigure {
	if class != nil {
		for _, s := range list {
			if s.AcceleratorClass != *class {
				continue
			}
			if d, err := s.Duration.AsDuration(); err == nil {
				m := s.MeasuredTime
				return &v1alpha1.DurationFigure{Duration: &metav1.Duration{Duration: d}, ObservedAt: &m}
			}
		}
	}
	return &v1alpha1.DurationFigure{Unknown: unknownStartToReady}
}
