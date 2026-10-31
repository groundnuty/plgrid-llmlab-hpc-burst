package fake

import (
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// HealthStalled is an engine that has stalled: eight requests running, none
// waiting, and a generated-token counter that never changes. Its probe passes.
// The controller restarts it once the counter has been unchanged for
// noProgressTimeout, by the clock the test moves.
func HealthStalled(now time.Time, _ string) agentapi.Health {
	return agentapi.Health{Time: now, ProbeOK: true, RunningRequests: 8, GeneratedTokens: 12345, KVCacheUsage: 0.5}
}

// HealthSlowPrefill is an engine in a long prefill: one request running, none
// waiting, and no token generated yet. The sample is the same at every
// call; the test's clock decides how long the prefill has lasted, 1,100 s
// (not restarted) or 30 min (restarted).
func HealthSlowPrefill(now time.Time, _ string) agentapi.Health {
	return agentapi.Health{Time: now, ProbeOK: true, RunningRequests: 1, KVCacheUsage: 0.05}
}

// perWorkerCount counts the samples taken of each worker, for the doubles whose
// counters rise at every sample whatever the clock says. A count is kept by
// worker name, not by process: it goes on rising after the worker restarts,
// where a real engine's counters start again from zero. Engine.Health is given
// no PID, so a double cannot tell. After a restart, a test of a ladder with such
// a double sees the new process over its threshold at its first sample.
type perWorkerCount map[string]int64

func (c perWorkerCount) next(worker string) int64 {
	c[worker]++
	return c[worker]
}

// HealthPlainLoad returns an engine under plain load: 256 requests running, a
// queue of 1,000 waiting, a full KV cache, and a generated-token counter that
// rises by 1,000 at every sample. Nothing is wrong with it, and the controller
// restarts nothing. The counter is kept per worker, so give each Agent its own
// engine.
func HealthPlainLoad() func(time.Time, string) agentapi.Health {
	samples := perWorkerCount{}
	return func(now time.Time, worker string) agentapi.Health {
		return agentapi.Health{
			Time: now, ProbeOK: true,
			RunningRequests: 256, WaitingRequests: 1000,
			GeneratedTokens: 1000 * samples.next(worker),
			KVCacheUsage:    1,
		}
	}
}

// HealthFailingProbes returns an engine whose probe fails at every sample:
// ProbeOK is false, and ConsecutiveProbeFailures is 1 at the first sample, 2 at
// the second, and so on. The counter is kept per worker, so give each Agent its
// own engine.
func HealthFailingProbes() func(time.Time, string) agentapi.Health {
	samples := perWorkerCount{}
	return func(now time.Time, worker string) agentapi.Health {
		return agentapi.Health{Time: now, ConsecutiveProbeFailures: int32(samples.next(worker)), KVCacheUsage: 0.1}
	}
}

// HealthErrorsOnly returns an engine that answers every request with a server
// error: its probe passes, and the responses by status are {"500": 10k} at the
// k-th sample, with no success among them. The counter is kept per worker, so
// give each Agent its own engine.
func HealthErrorsOnly() func(time.Time, string) agentapi.Health {
	samples := perWorkerCount{}
	return func(now time.Time, worker string) agentapi.Health {
		return agentapi.Health{
			Time: now, ProbeOK: true, KVCacheUsage: 0.1,
			ResponsesByStatus: map[string]int64{"500": 10 * samples.next(worker)},
		}
	}
}
