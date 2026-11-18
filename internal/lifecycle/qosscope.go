package lifecycle

import (
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The QOS an Allocation runs under, and the limits counted per QOS (HBEP-4, QOS limits, Limits counted per QOS;
// HBEP-7, Per-user limits; #745).
//
// Where a Provider's partition sets qosByGpus, its Allocations run under several QOSes, each chosen by the
// Allocation's GPU count (v1alpha1.QOSForGPUs, the one choice the Slurm plugin makes too). A QOS's per-user limits
// (maxSubmitPerUser, maxGpusPerUser, maxCpusPerUser, maxMemPerUser) then count only the Allocations under that QOS:
// the listed ones, the expectations, the reserved slots, a hedge member's and an extra's checks and the extra's
// headroom. The partition QOS's limits and the host-dialed ceiling still count every Allocation on the Provider.
//
// A Provider without qosByGpus has no scope: its limits are counted as they always were, and every function here
// leaves it alone.

// qosScope is how one asking Allocation's per-QOS limits are counted: the QOS it would submit under, and the
// limits that belong to that QOS alone, which count only the Allocations under it. A limit the partition QOS sets
// is not here, since it counts every Allocation on the Provider (providerView.limits).
type qosScope struct {
	partition   v1alpha1.PartitionProfile
	providerQOS string
	// qos is the QOS the asking Allocation runs under.
	qos string
	// limits are the per-user limits of the job's QOS, each set only where that QOS gives it (From names it), and
	// no QOS of the partition's own. A limit that is nil here is counted Provider-wide, or is no limit.
	limits v1alpha1.QOSLimits
}

// under reports whether an Allocation of the GPU count runs under the scope's QOS. A count with no QOS (above the
// last entry) cannot be under any, but one that is unknown is counted, never missed: a limit's count must not fall
// short (the stricter reading, as capsFit takes it).
func (s *qosScope) under(gpus int32) bool {
	if gpus < 1 {
		return true // not known
	}
	q, err := v1alpha1.QOSForGPUs(s.partition, s.providerQOS, gpus)
	return err != nil || q == s.qos
}

// submitLimit is the submit limit of the scope's QOS, nil for no scope or none set.
func (s *qosScope) submitLimit() *int32 {
	if s == nil {
		return nil
	}
	return s.limits.MaxSubmitPerUser
}

// reservedUnder returns the reserved slots held for Allocations under the scope's QOS. A slot whose GPU count is
// not known is kept, so a count never falls short of it.
func (s *qosScope) reservedUnder(slots []reservedSlot) []reservedSlot {
	var out []reservedSlot
	for _, sl := range slots {
		if s.under(sl.gpus) {
			out = append(out, sl)
		}
	}
	return out
}

// countUnder is the Provider's count against the submit limit of the scope's QOS: the listed Allocations that hold
// a slot and the expectations the list does not show yet, each counted when it runs under that QOS, or when its QOS
// is not known (an Allocation with no batch parameters, an expectation made with no GPU count). It reads what
// observe has just left, and the caller holds t.mu.
func (t *slotTable) countUnder(provider string, rd slotReads, sc *qosScope) int {
	n := 0
	for k := range rd.listed {
		if a := rd.allocs[k]; a == nil || a.Spec.ForProvider.Batch == nil || sc.under(a.Spec.ForProvider.Batch.GPUs) {
			n++
		}
	}
	for k := range t.expect[provider] {
		if g, known := t.gpus[provider][k]; !known || sc.under(g) {
			n++
		}
	}
	return n
}

// keyUnder reports whether the Allocation key (namespace/name), as the count reads it, runs under the scope's QOS.
func (t *slotTable) keyUnder(provider string, rd slotReads, sc *qosScope, key string) bool {
	if a := rd.allocs[key]; a != nil && a.Spec.ForProvider.Batch != nil {
		return sc.under(a.Spec.ForProvider.Batch.GPUs)
	}
	if g, known := t.gpus[provider][key]; known {
		return sc.under(g)
	}
	return true
}

// hasLimits is whether the scope holds any per-QOS limit to count.
func (s *qosScope) hasLimits() bool {
	l := s.limits
	return l.MaxSubmitPerUser != nil || l.MaxGPUsPerUser != nil || l.MaxCPUsPerUser != nil || l.MaxMemPerUser != nil
}

// forGPUs returns the view of the Provider for an Allocation of gpus GPUs: for a partition with qosByGpus the
// QOS the list chooses, and the QOS limits that apply under it, in Slurm's order (v1alpha1.EffectiveQOSLimits,
// the partition QOS's before the job QOS's unless OverPartQOS); the view itself otherwise. The returned view's
// limits are the Provider-wide ones, the partition QOS's and the host-dialed ceiling, and its jobLimits and scope
// the job QOS's. It errors for a count above the list's last entry, which has no QOS (the caller refuses it).
// It returns a copy, and leaves pv alone.
func (pv *providerView) forGPUs(gpus int32) (*providerView, error) {
	if len(pv.partition.QOSByGPUs) == 0 || pv.profile == nil {
		return pv, nil
	}
	q, err := v1alpha1.QOSForGPUs(pv.partition, "", gpus)
	if err != nil {
		return nil, err
	}
	job, err := v1alpha1.EffectiveQOSLimits(&pv.profile.Spec, pv.partition, q)
	if err != nil {
		return nil, err
	}
	// Each per-user limit the job's own QOS gives (not the partition QOS) belongs to that QOS: it moves from the
	// Provider-wide limits into the scope. One the partition QOS gives stays Provider-wide.
	wide, own := cloneLimits(job), v1alpha1.QOSLimits{From: map[string]string{}}
	belongs := func(limit string) bool {
		from := job.From[limit]
		return from != "" && from == q && q != pv.partition.QOS
	}
	if belongs(v1alpha1.LimitMaxSubmitPerUser) {
		own.MaxSubmitPerUser, wide.MaxSubmitPerUser = wide.MaxSubmitPerUser, nil
		delete(wide.From, v1alpha1.LimitMaxSubmitPerUser)
	}
	if belongs(v1alpha1.LimitMaxGPUsPerUser) {
		own.MaxGPUsPerUser, wide.MaxGPUsPerUser = wide.MaxGPUsPerUser, nil
		delete(wide.From, v1alpha1.LimitMaxGPUsPerUser)
	}
	if belongs(v1alpha1.LimitMaxCPUsPerUser) {
		own.MaxCPUsPerUser, wide.MaxCPUsPerUser = wide.MaxCPUsPerUser, nil
		delete(wide.From, v1alpha1.LimitMaxCPUsPerUser)
	}
	if belongs(v1alpha1.LimitMaxMemPerUser) {
		own.MaxMemPerUser, wide.MaxMemPerUser = wide.MaxMemPerUser, nil
		delete(wide.From, v1alpha1.LimitMaxMemPerUser)
	}
	for k, v := range job.From {
		if _, kept := wide.From[k]; !kept {
			own.From[k] = v
		}
	}
	// The per-job caps and the wall bound one job, not a count of several: they stay in jobLimits, which sizes it.
	out := *pv
	out.jobLimits = &job
	out.limits = wide
	if pv.hostDialed {
		out.limits = hostDialedLimits(wide)
	}
	out.scope = &qosScope{partition: pv.partition, providerQOS: "", qos: q, limits: own}
	return &out, nil
}

// sizingLimits are the QOS limits an Allocation is sized by and held to: the job QOS's, in Slurm's order, for a
// view resolved by forGPUs; the Provider's own limits for every other.
func (pv *providerView) sizingLimits() v1alpha1.QOSLimits {
	if pv.jobLimits != nil {
		return *pv.jobLimits
	}
	return pv.limits
}

// cloneLimits returns limits with its own From map, so a copy can drop an entry.
func cloneLimits(l v1alpha1.QOSLimits) v1alpha1.QOSLimits {
	out := l
	out.From = make(map[string]string, len(l.From))
	for k, v := range l.From {
		out.From[k] = v
	}
	return out
}
