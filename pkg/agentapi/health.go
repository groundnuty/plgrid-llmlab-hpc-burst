package agentapi

import "time"

// MetricsStaleProbeIntervals is how many probe intervals old the last
// successful /metrics read may be before the controller treats the sample's
// metrics as unknown (`@hpc-burst-science-agent`, 31 Oct 2026). The read is
// stale when it is older than this many intervals, so exactly this many is
// still fresh.
const MetricsStaleProbeIntervals = 2

// Int64 returns a pointer to v, for the pointer fields of Health that a
// sample sets.
func Int64(v int64) *int64 { return &v }

// Float64 returns a pointer to v, for Health.KVCacheUsage.
func Float64(v float64) *float64 { return &v }

// MetricsFresh reports whether the sample's metrics (RunningRequests,
// WaitingRequests, GeneratedTokens, KVCacheUsage and ResponsesByStatus) are
// current: MetricsReadAt is set and no older than MetricsStaleProbeIntervals
// times probeInterval, the interval the agent probes at
// (AgentSettings.ProbeInterval). When it is false the controller treats all of them as
// unknown and takes no gauge-based action.
//
// The age is the sample's Time minus MetricsReadAt. Both are the agent's own
// clock, so the compute node's clock skew against the control-plane host's
// does not enter. A report that stopped arriving is a different matter, for
// the AgentUnreachable condition. A sample with no Time, or a zero
// probeInterval, is never fresh.
func (h Health) MetricsFresh(probeInterval time.Duration) bool {
	if h.MetricsReadAt == nil || h.Time.IsZero() || probeInterval <= 0 {
		return false
	}
	return h.Time.Sub(*h.MetricsReadAt) <= MetricsStaleProbeIntervals*probeInterval
}
