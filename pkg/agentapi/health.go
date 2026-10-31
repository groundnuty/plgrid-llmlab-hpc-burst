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
// current. When it is false the controller treats all of them as unknown and
// takes no gauge-based action.
//
// The arguments are what the controller holds about the sample: reportTime is
// the Time of the Report that carried it (the agent's clock), receivedAt is
// when the controller received that Report (the controller's clock), now is the
// controller's clock at the judgement, and probeInterval is the interval the
// agent probes at (AgentSettings.ProbeInterval). The metrics are fresh when
// MetricsReadAt is set, no argument is zero, and all three ages are at most
// MetricsStaleProbeIntervals times probeInterval:
//
//  1. Time minus MetricsReadAt: how old the read was when the sample was
//     taken. Both are the agent's clock, so no clock skew enters.
//  2. reportTime minus Time: how old the sample was when the Report was made.
//     Both are the agent's clock again. This catches an agent that keeps
//     re-sending a cached sample in new Reports, which the first age cannot
//     see because Time and MetricsReadAt stay equal.
//  3. now minus receivedAt: how long ago the Report arrived, on the
//     controller's clock. This catches a sample that stays unchanged because no
//     newer Report came; the AgentUnreachable condition acts only after
//     health.agentTimeout (2 min by default). An agent reports at least every
//     ReportInterval (15 s), so this age exceeds two probe intervals between
//     Reports only for a probeInterval under 7.5 s, which leaves the gauges
//     unknown, the safe side.
//
// An age is stale only when it is strictly more than the bound, so exactly two
// intervals is still fresh. A read dated after the sample, or a Report dated
// before its sample, gives a negative age and counts as fresh; a zero time
// argument or a probeInterval of zero is never fresh.
func (h Health) MetricsFresh(reportTime, receivedAt, now time.Time, probeInterval time.Duration) bool {
	if h.MetricsReadAt == nil || h.Time.IsZero() || reportTime.IsZero() || receivedAt.IsZero() || now.IsZero() || probeInterval <= 0 {
		return false
	}
	bound := MetricsStaleProbeIntervals * probeInterval
	return h.Time.Sub(*h.MetricsReadAt) <= bound &&
		reportTime.Sub(h.Time) <= bound &&
		now.Sub(receivedAt) <= bound
}
