package slurm

// The checks of a request against the limits that apply (HBEP-4, Submission,
// Refused before any call; QOS limits). Each takes the QOS limits that
// v1alpha1.EffectiveQOSLimits worked out for the partition under the Provider's
// qos, so the precedence is written once, in the API package.

import (
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// checkGPUs refuses a GPU count above the most one Allocation may ask for:
// the smaller of the partition's gpusPerNode, the node's hardware, and the QOS
// limits' maxGpusPerUser, each when set (v1alpha1.MaxAllocationGPUs). The
// refusal names the bound that is the smaller. A count above it is refused,
// never capped. With neither set there is no bound (a CPU partition).
func checkGPUs(gpus int32, part v1alpha1.PartitionProfile, limits v1alpha1.QOSLimits) error {
	most, ok := v1alpha1.MaxAllocationGPUs(part, limits)
	if !ok || gpus <= most {
		return nil
	}
	if m := limits.MaxGPUsPerUser; m != nil && (part.GPUsPerNode == nil || *m < *part.GPUsPerNode) {
		return refuse("gpus %d is above maxGpusPerUser %d of qos %q", gpus, *m, limits.From[v1alpha1.LimitMaxGPUsPerUser])
	}
	return refuse("gpus %d is above gpusPerNode %d of partition %q", gpus, *part.GPUsPerNode, part.Name)
}

// checkTimeLimit refuses a time limit above the smallest of the partition's
// maxTime and the QOS limits' maxWall (v1alpha1.AllocationTimeLimit; the
// leaseLimit is the lifecycle controller's, already in the request). The
// refusal names the bound that set the limit.
func checkTimeLimit(limit time.Duration, text v1alpha1.Duration, part v1alpha1.PartitionProfile, limits v1alpha1.QOSLimits) error {
	var maxTime *time.Duration
	if part.MaxTime != nil {
		d, err := part.MaxTime.AsDuration()
		if err != nil {
			return refuse("the profile's maxTime of partition %q is not a duration", part.Name)
		}
		maxTime = &d
	}
	allowed, bound, ok := v1alpha1.AllocationTimeLimit(maxTime, limits.MaxWall, nil)
	if !ok {
		if maxTime == nil && limits.MaxWall == nil {
			return nil // no bound is set
		}
		return refuse("the time limit bound of partition %q is below one minute", part.Name)
	}
	if limit <= allowed {
		return nil
	}
	if bound == v1alpha1.TimeLimitMaxWall {
		return refuse("time limit %q is above maxWall %s of qos %q (maxTime of partition %q is longer)",
			text, allowed, limits.From[v1alpha1.LimitMaxWall], part.Name)
	}
	return refuse("time limit %q is above maxTime %s of partition %q", text, allowed, part.Name)
}
