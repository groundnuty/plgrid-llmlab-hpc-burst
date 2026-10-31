package agent

import (
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// recordHealth turns one probe and one metrics read of a running worker into
// the worker's health sample (HBEP-7, Reports: for health). The probe's result
// is always fresh. The gauges come only from a successful metrics read: when a
// read fails they are left out, which decodes as unknown, never as zero, and
// MetricsReadAt keeps the time of the last read that succeeded, so the
// controller can tell stale metrics from fresh ones (agentapi.Health,
// MetricsFresh; `@hpc-burst-science-agent`, 31 Oct 2026; PR #87 review, Opus
// nit).
func (w *worker) recordHealth(now time.Time, perr error, h agentapi.Health, serr error) {
	if serr == nil {
		read := now
		w.metricsReadAt = &read
	} else {
		h = agentapi.Health{}
	}
	if perr == nil {
		w.failures = 0
	} else {
		w.failures++
	}
	h.Time = now
	h.ProbeOK = perr == nil
	h.ConsecutiveProbeFailures = w.failures
	if w.metricsReadAt != nil {
		read := *w.metricsReadAt
		h.MetricsReadAt = &read
	}
	w.health = &h
}

// cloneHealth deep-copies a health sample: its gauges are pointers and its
// counts a map.
func cloneHealth(h agentapi.Health) agentapi.Health {
	h.MetricsReadAt = clonePtr(h.MetricsReadAt)
	h.RunningRequests = clonePtr(h.RunningRequests)
	h.WaitingRequests = clonePtr(h.WaitingRequests)
	h.GeneratedTokens = clonePtr(h.GeneratedTokens)
	h.KVCacheUsage = clonePtr(h.KVCacheUsage)
	h.Preemptions = clonePtr(h.Preemptions)
	h.PrefixCacheQueriedTokens = clonePtr(h.PrefixCacheQueriedTokens)
	h.PrefixCacheHitTokens = clonePtr(h.PrefixCacheHitTokens)
	h.TimePerOutputTokenSecondsSum = clonePtr(h.TimePerOutputTokenSecondsSum)
	h.TimePerOutputTokenCount = clonePtr(h.TimePerOutputTokenCount)
	h.ResponsesByStatus = cloneCounts(h.ResponsesByStatus)
	return h
}

func clonePtr[T any](p *T) *T {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}
