package fake

import (
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// metricsSample is a sample whose probe passed and whose /metrics read finished
// at now: MetricsReadAt is now, and every gauge and counter is set, a zero
// explicitly: the preemption counter, the two prefix-cache counters and the
// sum and count of the time per output token (HBEP-8) are 0 here, as a real
// vLLM exports them before any request. A double built on it reads as an
// engine whose metrics are current and known; the doubles that model an
// unreadable /metrics (HealthMetricsUnreadable, HealthMetricsStale) do not use
// it.
func metricsSample(now time.Time, running, waiting, tokens int64, kvCache float64) agentapi.Health {
	read := now
	return agentapi.Health{
		Time: now, ProbeOK: true, MetricsReadAt: &read,
		RunningRequests: agentapi.Int64(running), WaitingRequests: agentapi.Int64(waiting),
		GeneratedTokens: agentapi.Int64(tokens), KVCacheUsage: agentapi.Float64(kvCache),
		Preemptions:              agentapi.Int64(0),
		PrefixCacheQueriedTokens: agentapi.Int64(0), PrefixCacheHitTokens: agentapi.Int64(0),
		TimePerOutputTokenSecondsSum: agentapi.Float64(0), TimePerOutputTokenCount: agentapi.Int64(0),
	}
}

// HealthStalled is an engine that has stalled: eight requests running, none
// waiting, and a generated-token counter that never changes. Its probe passes,
// and its metrics are read at every sample (MetricsReadAt is the sample's
// time). The controller restarts it once the counter has been unchanged for
// noProgressTimeout, by the clock the test moves.
func HealthStalled(now time.Time, _ Process) agentapi.Health {
	return metricsSample(now, 8, 0, 12345, 0.5)
}

// HealthSlowPrefill is an engine in a long prefill: one request running, none
// waiting, and no token generated yet. The sample is the same at every
// call; the test's clock decides how long the prefill has lasted, 1,100 s
// (not restarted) or 30 min (restarted). Its metrics are read at every sample.
func HealthSlowPrefill(now time.Time, _ Process) agentapi.Health {
	return metricsSample(now, 1, 0, 0, 0.05)
}

// HealthMetricsUnreadable is an engine whose probe passes but whose /metrics
// cannot be read: no gauge, no counter, no responses by status, and no
// MetricsReadAt, because there has been no successful read. Every value is
// unknown, not zero, so the controller takes no gauge-based action on it.
func HealthMetricsUnreadable(now time.Time, _ Process) agentapi.Health {
	return agentapi.Health{Time: now, ProbeOK: true}
}

// HealthMetricsStale returns an engine that looks stalled, with the values of
// HealthStalled, but whose last successful /metrics read was age before each
// sample: MetricsReadAt is the sample's time minus age. With an age above twice
// the agent's probe interval (agentapi.Health.MetricsFresh, with the Report made
// and received at the sample's time) the controller
// treats the values as unknown, so it must not restart this engine for a stall
// it cannot see; with a younger age the values count and it does.
func HealthMetricsStale(age time.Duration) func(time.Time, Process) agentapi.Health {
	return func(now time.Time, p Process) agentapi.Health {
		h := HealthStalled(now, p)
		read := now.Add(-age)
		h.MetricsReadAt = &read
		return h
	}
}

// HealthCached returns an engine whose sample never changes after its first: the
// Time and the MetricsReadAt stay at the first sample's time, with HealthStalled's
// values, while the Agent's Reports go on, as an agent that re-sends a cached
// sample in new Reports would. Read against the Report's own time
// (agentapi.Health.MetricsFresh), its metrics are fresh in the first Reports and
// stale once the Report is made more than twice the probe interval after the
// sample, so the controller must not restart this engine for the stall it shows.
// The first sample is kept per process: a restarted worker starts again. Give
// each Agent its own engine.
func HealthCached() func(time.Time, Process) agentapi.Health {
	first := map[int]time.Time{}
	return func(now time.Time, p Process) agentapi.Health {
		if _, ok := first[p.PID]; !ok {
			first[p.PID] = now
		}
		return HealthStalled(first[p.PID], p)
	}
}

// perProcessCount counts the samples taken of each worker process, for the
// doubles whose counters rise at every sample whatever the clock says. A count is
// kept by PID, so a restarted worker is a new process whose counters start again
// from 1, as a real engine's start from zero. The Agent never reuses a PID within
// one Agent; two Agents have the same PIDs, so each takes its own double.
type perProcessCount map[int]int64

func (c perProcessCount) next(p Process) int64 {
	c[p.PID]++
	return c[p.PID]
}

// HealthPlainLoad returns an engine under plain load: 256 requests running, a
// queue of 1,000 waiting, a full KV cache, and a generated-token counter that
// rises by 1,000 at every sample. Nothing is wrong with it, and the controller
// restarts nothing. The counter is kept per process: it starts again after a
// restart. Give each Agent its own engine.
func HealthPlainLoad() func(time.Time, Process) agentapi.Health {
	samples := perProcessCount{}
	return func(now time.Time, p Process) agentapi.Health {
		return metricsSample(now, 256, 1000, 1000*samples.next(p), 1)
	}
}

// HealthFailingProbes returns an engine whose probe fails at every sample:
// ProbeOK is false, and ConsecutiveProbeFailures is 1 at the first sample, 2 at
// the second, and so on. The counter is kept per process: a restarted worker
// starts again at 1. Give each Agent its own engine.
func HealthFailingProbes() func(time.Time, Process) agentapi.Health {
	samples := perProcessCount{}
	return func(now time.Time, p Process) agentapi.Health {
		h := metricsSample(now, 0, 0, 0, 0.1)
		h.ProbeOK, h.ConsecutiveProbeFailures = false, int32(samples.next(p))
		return h
	}
}

// HealthErrorsOnly returns an engine that answers every request with a server
// error: its probe passes, and the responses by status are {"5xx": 10k} at the
// k-th sample, with no success among them. The key is a status class, as vLLM
// reports it (agentapi.Health.ResponsesByStatus). The counter is kept per process: a
// restarted worker starts again at k = 1. Give each Agent its own engine.
func HealthErrorsOnly() func(time.Time, Process) agentapi.Health {
	samples := perProcessCount{}
	return func(now time.Time, p Process) agentapi.Health {
		h := metricsSample(now, 0, 0, 0, 0.1)
		h.ResponsesByStatus = map[string]int64{"5xx": 10 * samples.next(p)}
		return h
	}
}

// HealthEngineCounters returns an engine under moderate load whose preemption,
// prefix-cache and time-per-output-token counters rise at every sample (HBEP-8,
// Engine gauges per Instance). At the k-th sample of a process it has 4
// requests running, none waiting, a KV cache half full, and cumulative
// counters of k preemptions, 1,000k queried and 800k hit prefix-cache tokens,
// and 10k requests observed with 0.5k seconds of time per output token between
// them (a mean of 50 ms). The counters are kept per process: they start again
// after a restart, as a real engine's do. Give each Agent its own engine.
func HealthEngineCounters() func(time.Time, Process) agentapi.Health {
	samples := perProcessCount{}
	return func(now time.Time, p Process) agentapi.Health {
		k := samples.next(p)
		h := metricsSample(now, 4, 0, 1000*k, 0.5)
		h.Preemptions = agentapi.Int64(k)
		h.PrefixCacheQueriedTokens, h.PrefixCacheHitTokens = agentapi.Int64(1000*k), agentapi.Int64(800*k)
		h.TimePerOutputTokenSecondsSum, h.TimePerOutputTokenCount = agentapi.Float64(0.5*float64(k)), agentapi.Int64(10*k)
		return h
	}
}

// HealthWithoutPrefixCache is an engine whose /metrics lacks the two
// prefix-cache counter lines: PrefixCacheQueriedTokens and PrefixCacheHitTokens
// are nil, unknown, and every other value is known, a zero explicitly. It is not
// an engine with prefix caching off: a real vLLM v0.32.0 registers both counters
// whether or not prefix caching is on, and exports them at 0, which is a known
// zero (HBEP-8, Test plan).
func HealthWithoutPrefixCache(now time.Time, _ Process) agentapi.Health {
	h := metricsSample(now, 0, 0, 0, 0.1)
	h.PrefixCacheQueriedTokens, h.PrefixCacheHitTokens = nil, nil
	return h
}
