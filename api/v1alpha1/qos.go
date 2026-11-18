package v1alpha1

import (
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
)

// Names of the QOS limits, as the profile's qos entries name them. QOSLimits.From
// is keyed by them, and messages that name a limit use them.
const (
	LimitMaxWall          = "maxWall"
	LimitMaxSubmitPerUser = "maxSubmitPerUser"
	LimitMaxGPUsPerUser   = "maxGpusPerUser"
	LimitMaxCPUsPerUser   = "maxCpusPerUser"
	LimitMaxMemPerUser    = "maxMemPerUser"
	LimitMaxGPUsPerJob    = "maxGpusPerJob"
	LimitMaxCPUsPerJob    = "maxCpusPerJob"
	LimitMaxMemPerJob     = "maxMemPerJob"
)

// QOSLimits are the QOS limits that apply to a job on one partition under one
// job QOS, each taken from the first QOS in Slurm's order that sets it
// (EffectiveQOSLimits). A nil limit is one that no QOS in the order sets: no
// limit, as far as the plan knows.
type QOSLimits struct {
	// MaxWall is the wall limit per job.
	MaxWall *time.Duration
	// MaxSubmitPerUser is the most jobs one user may have pending or running.
	MaxSubmitPerUser *int32
	// MaxGPUsPerUser, MaxCPUsPerUser and MaxMemPerUser bound the sum over one
	// user's running jobs.
	MaxGPUsPerUser *int32
	MaxCPUsPerUser *int32
	MaxMemPerUser  *resource.Quantity
	// MaxGPUsPerJob, MaxCPUsPerJob and MaxMemPerJob bound one job (the QOS's
	// MaxTRES), each taken from the first QOS in Slurm's order that sets it,
	// as the per-user limits are (HBEP-4, QOS limits; #745).
	MaxGPUsPerJob *int32
	MaxCPUsPerJob *int32
	MaxMemPerJob  *resource.Quantity
	// From names the QOS that set each limit, keyed by the limit's name
	// (LimitMaxWall and the rest). A limit that no QOS sets has no entry.
	From map[string]string
}

// FindQOS returns the profile's entry for the named QOS, and false when the
// profile does not list it.
func FindQOS(spec *PlatformProfileSpec, name string) (QOSProfile, bool) {
	for _, q := range spec.QOS {
		if q.Name == name {
			return q, true
		}
	}
	return QOSProfile{}, false
}

// EffectiveQOSLimits returns the QOS limits Slurm applies to a job on the
// partition under jobQOS: the Provider's qos, or "" when it sets none. With
// none, the job runs under the association's default QOS, which the profile
// cannot know, so only the partition QOS's typed limits apply; the default's
// limits are not planned for (`@hpc-burst-science-agent`, 1 Nov 2026, from PR
// #155's review). For each limit
// it takes the partition QOS's value if that sets one, else the job QOS's;
// when the job QOS has overPartQos, the order is reversed. That is Slurm's
// order: the partition QOS comes before the job's QOS, reversible only by the
// job QOS's flag OverPartQOS, and the first QOS in the order that sets a limit
// gives it (HBEP-4, QOS limits; resource_limits, Hierarchy; acct_policy.c). An
// overPartQos on the partition's own QOS changes nothing, since Slurm reads
// the flag on the job's QOS. A limit that neither sets stays nil.
//
// It returns an error when the partition's qos or jobQOS is not listed in the
// profile's qos, naming the field, or when a maxWall does not parse.
func EffectiveQOSLimits(spec *PlatformProfileSpec, partition PartitionProfile, jobQOS string) (QOSLimits, error) {
	var order []QOSProfile
	if partition.QOS != "" {
		q, ok := FindQOS(spec, partition.QOS)
		if !ok {
			return QOSLimits{}, fmt.Errorf("partitions[%s].qos %q is not listed in the profile's qos", partition.Name, partition.QOS)
		}
		order = append(order, q)
	}
	if jobQOS != "" {
		q, ok := FindQOS(spec, jobQOS)
		if !ok {
			return QOSLimits{}, fmt.Errorf("the Provider's qos %q is not listed in the profile's qos", jobQOS)
		}
		if q.OverPartQOS {
			order = append([]QOSProfile{q}, order...)
		} else {
			order = append(order, q)
		}
	}
	out := QOSLimits{From: map[string]string{}}
	for _, q := range order {
		if out.MaxWall == nil && q.MaxWall != nil {
			d, err := q.MaxWall.AsDuration()
			if err != nil {
				return QOSLimits{}, fmt.Errorf("qos[%s].maxWall %q: %w", q.Name, *q.MaxWall, err)
			}
			out.MaxWall, out.From[LimitMaxWall] = &d, q.Name
		}
		if out.MaxSubmitPerUser == nil && q.MaxSubmitPerUser != nil {
			v := *q.MaxSubmitPerUser
			out.MaxSubmitPerUser, out.From[LimitMaxSubmitPerUser] = &v, q.Name
		}
		if out.MaxGPUsPerUser == nil && q.MaxGPUsPerUser != nil {
			v := *q.MaxGPUsPerUser
			out.MaxGPUsPerUser, out.From[LimitMaxGPUsPerUser] = &v, q.Name
		}
		if out.MaxCPUsPerUser == nil && q.MaxCPUsPerUser != nil {
			v := *q.MaxCPUsPerUser
			out.MaxCPUsPerUser, out.From[LimitMaxCPUsPerUser] = &v, q.Name
		}
		if out.MaxMemPerUser == nil && q.MaxMemPerUser != nil {
			v := q.MaxMemPerUser.DeepCopy()
			out.MaxMemPerUser, out.From[LimitMaxMemPerUser] = &v, q.Name
		}
		if out.MaxGPUsPerJob == nil && q.MaxGPUsPerJob != nil {
			v := *q.MaxGPUsPerJob
			out.MaxGPUsPerJob, out.From[LimitMaxGPUsPerJob] = &v, q.Name
		}
		if out.MaxCPUsPerJob == nil && q.MaxCPUsPerJob != nil {
			v := *q.MaxCPUsPerJob
			out.MaxCPUsPerJob, out.From[LimitMaxCPUsPerJob] = &v, q.Name
		}
		if out.MaxMemPerJob == nil && q.MaxMemPerJob != nil {
			v := q.MaxMemPerJob.DeepCopy()
			out.MaxMemPerJob, out.From[LimitMaxMemPerJob] = &v, q.Name
		}
	}
	return out, nil
}

// QOSForGPUs returns the QOS a job of gpus GPUs on the partition runs under.
// Where the partition sets qosByGpus it is the QOS of the first entry whose
// gpus is at least the count; a count above the last entry has no QOS and is
// an error, never a QOS (HBEP-4, The Slurm dialect, The QOS by GPU count;
// #745). Without qosByGpus it is providerQOS, the Provider's qos, or "" when
// it sets none, as before. The plan and the Slurm plugin both choose through
// this one function, so the chosen QOS is the job's QOS for every limit
// (HBEP-4, QOS limits); a request below the first entry's gpus gets the first
// entry's QOS (#745 does not speak of a count of 0, which the plugin refuses
// on a GPU partition).
func QOSForGPUs(partition PartitionProfile, providerQOS string, gpus int32) (string, error) {
	if len(partition.QOSByGPUs) == 0 {
		return providerQOS, nil
	}
	for _, e := range partition.QOSByGPUs {
		if gpus <= e.GPUs {
			return e.QOS, nil
		}
	}
	last := partition.QOSByGPUs[len(partition.QOSByGPUs)-1]
	return "", fmt.Errorf("partition %s: %d GPUs is above the %d of its last qosByGpus entry (qos %s), so no QOS fits it", partition.Name, gpus, last.GPUs, last.QOS)
}

// TimeLimitBound names the bound that set an Allocation's time limit, which a
// PartitionTooShort message names (HBEP-7, Provisioning, A partition too
// short).
type TimeLimitBound string

// Time-limit bounds.
const (
	// TimeLimitMaxTime: the partition's maxTime.
	TimeLimitMaxTime TimeLimitBound = "maxTime"
	// TimeLimitMaxWall: the maxWall of the QOS limits that apply.
	TimeLimitMaxWall TimeLimitBound = "maxWall"
	// TimeLimitLeaseLimit: the ModelDeployment's leaseLimit.
	TimeLimitLeaseLimit TimeLimitBound = "leaseLimit"
)

// AllocationTimeLimit returns an Allocation's time limit: the smallest of the
// partition's maxTime, the maxWall of the QOS limits that apply and the
// ModelDeployment's leaseLimit, each nil when unset, rounded down to whole
// minutes, because Slurm counts limits in minutes (HBEP-4, QOS limits and open
// question 11; HBEP-7, Provisioning). A wall limit never lifts the partition's
// maxTime. It also returns the bound that set the limit; when two are equal,
// the first of maxTime, maxWall and leaseLimit is named. ok is false when none
// is set or the limit is below one minute.
func AllocationTimeLimit(maxTime, maxWall, leaseLimit *time.Duration) (limit time.Duration, bound TimeLimitBound, ok bool) {
	set := false
	for _, b := range []struct {
		d     *time.Duration
		bound TimeLimitBound
	}{{maxTime, TimeLimitMaxTime}, {maxWall, TimeLimitMaxWall}, {leaseLimit, TimeLimitLeaseLimit}} {
		if b.d != nil && (!set || *b.d < limit) {
			limit, bound, set = *b.d, b.bound, true
		}
	}
	if !set {
		return 0, "", false
	}
	limit = limit.Truncate(time.Minute)
	return limit, bound, limit >= time.Minute
}

// Names of the bounds on an Allocation's GPU count, as GPUBound.By holds them.
const (
	// GPUBoundPerNode is the partition's gpusPerNode, the node's hardware.
	GPUBoundPerNode = "gpusPerNode"
	// GPUBoundQOSByGPUs is the last entry's gpus of the partition's qosByGpus.
	GPUBoundQOSByGPUs = "qosByGpus"
)

// GPUBound is the most GPUs one Allocation may request on a partition, and the
// bound that sets it: GPUBoundPerNode, GPUBoundQOSByGPUs, LimitMaxGPUsPerUser
// or LimitMaxGPUsPerJob. A tie names the first of them in that order.
type GPUBound struct {
	Most int32
	By   string
}

// AllocationGPUBound returns the most GPUs one Allocation may request on the
// partition under the QOS limits that apply: the smallest of the partition's
// gpusPerNode, the node's hardware, the last entry's gpus of its qosByGpus
// (the QOS depends on the count, so the list's own end bounds it; HBEP-4,
// Submission, GPU sizing; #745), and the limits' maxGpusPerUser and
// maxGpusPerJob, each when set; the smallest one that is set. A larger count
// is refused, never capped, because it is the engine's tensor-parallel size
// times its pipeline-parallel size (HBEP-4, Submission, GPU sizing; HBEP-7,
// Provisioning). The QOS cap bounds the request even when the profile does not
// give gpusPerNode (review of PR #155, N4). ok is false only when none is set:
// no bound is known. The limits are those of the job's QOS (QOSForGPUs).
func AllocationGPUBound(partition PartitionProfile, limits QOSLimits) (GPUBound, bool) {
	var best GPUBound
	ok := false
	consider := func(n int32, by string) {
		if !ok || n < best.Most {
			best, ok = GPUBound{Most: n, By: by}, true
		}
	}
	if partition.GPUsPerNode != nil {
		consider(*partition.GPUsPerNode, GPUBoundPerNode)
	}
	if n := len(partition.QOSByGPUs); n > 0 {
		consider(partition.QOSByGPUs[n-1].GPUs, GPUBoundQOSByGPUs)
	}
	if limits.MaxGPUsPerUser != nil {
		consider(*limits.MaxGPUsPerUser, LimitMaxGPUsPerUser)
	}
	if limits.MaxGPUsPerJob != nil {
		consider(*limits.MaxGPUsPerJob, LimitMaxGPUsPerJob)
	}
	return best, ok
}

// MaxAllocationGPUs returns the most GPUs one Allocation may request on the
// partition under the QOS limits that apply (AllocationGPUBound's Most). ok is
// false only when no bound is set.
func MaxAllocationGPUs(partition PartitionProfile, limits QOSLimits) (int32, bool) {
	b, ok := AllocationGPUBound(partition, limits)
	return b.Most, ok
}
