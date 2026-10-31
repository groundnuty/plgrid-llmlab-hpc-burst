package fake_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

// healthSamples runs the Agent for n Steps, moving the clock by gap after each,
// and returns the health samples of worker-0 in the reports that carry one: from
// the second Step on, since a worker is starting in the first.
func healthSamples(t *testing.T, h func(time.Time, string) agentapi.Health, n int, gap time.Duration) []agentapi.Health {
	t.Helper()
	r := newRig(t, fake.Engine{Health: h})
	r.set(run(instance()))
	var out []agentapi.Health
	for range n {
		r.step()
		if w := r.workerOf("worker-0"); w.Health != nil {
			out = append(out, *w.Health)
		}
		r.clk.Advance(gap)
	}
	return out
}

func TestHealthStalledHasRequestsRunningAndATokenCounterThatNeverMoves(t *testing.T) {
	samples := healthSamples(t, fake.HealthStalled, 4, 5*time.Minute)
	eq(t, "samples", len(samples), 3)
	for k, h := range samples {
		eq(t, fmt.Sprintf("sample %d, apart from its time", k), withoutTime(h), agentapi.Health{
			ProbeOK: true, RunningRequests: 8, WaitingRequests: 0, GeneratedTokens: 12345, KVCacheUsage: 0.5,
		})
	}
	// The clock moved 5 minutes between samples; the counter did not.
	eq(t, "time between samples", samples[1].Time.Sub(samples[0].Time), 5*time.Minute)
}

func withoutTime(h agentapi.Health) agentapi.Health {
	h.Time = time.Time{}
	return h
}

func TestHealthSlowPrefillHasOneRequestRunningAndNoTokenWhateverTheClockSays(t *testing.T) {
	for _, elapsed := range []time.Duration{1100 * time.Second, 30 * time.Minute} {
		samples := healthSamples(t, fake.HealthSlowPrefill, 3, elapsed/2)
		eq(t, fmt.Sprintf("%v: samples", elapsed), len(samples), 2)
		for _, h := range samples {
			eq(t, fmt.Sprintf("%v: sample", elapsed), withoutTime(h), agentapi.Health{
				ProbeOK: true, RunningRequests: 1, WaitingRequests: 0, GeneratedTokens: 0, KVCacheUsage: 0.05,
			})
		}
		eq(t, fmt.Sprintf("%v: the clock moved between the samples", elapsed), samples[1].Time.Sub(samples[0].Time), elapsed/2)
	}
}

func TestHealthPlainLoadHasALongQueueAndATokenCounterThatRisesWithEverySample(t *testing.T) {
	// The clock does not move: the counter rises per sample, so a frozen clock
	// never looks like a stall.
	samples := healthSamples(t, fake.HealthPlainLoad(), 4, 0)
	eq(t, "samples", len(samples), 3)
	for k, h := range samples {
		eq(t, fmt.Sprintf("sample %d", k), withoutTime(h), agentapi.Health{
			ProbeOK: true, RunningRequests: 256, WaitingRequests: 1000,
			GeneratedTokens: int64(1000 * (k + 1)), KVCacheUsage: 1,
		})
	}
}

func TestHealthCountersAreKeptPerWorker(t *testing.T) {
	r := newRig(t, fake.Engine{Health: fake.HealthPlainLoad()})
	r.set(run(instance(withWorkers(0, 0))))
	r.steps(3)
	for _, name := range []string{"worker-0", "worker-1"} {
		eq(t, name+" tokens at its second sample", r.workerOf(name).Health.GeneratedTokens, int64(2000))
	}
}

func TestHealthFailingProbesFailsAndCountsTheFailuresInARow(t *testing.T) {
	samples := healthSamples(t, fake.HealthFailingProbes(), 4, time.Second)
	eq(t, "samples", len(samples), 3)
	for k, h := range samples {
		eq(t, fmt.Sprintf("sample %d", k), withoutTime(h), agentapi.Health{
			ProbeOK: false, ConsecutiveProbeFailures: int32(k + 1), KVCacheUsage: 0.1,
		})
	}
}

func TestHealthErrorsOnlyAnswersEveryRequestWithAServerError(t *testing.T) {
	samples := healthSamples(t, fake.HealthErrorsOnly(), 4, time.Second)
	eq(t, "samples", len(samples), 3)
	for k, h := range samples {
		eq(t, fmt.Sprintf("sample %d", k), withoutTime(h), agentapi.Health{
			ProbeOK: true, KVCacheUsage: 0.1, ResponsesByStatus: map[string]int64{"500": int64(10 * (k + 1))},
		})
	}
}

func TestEachHealthDoubleDiffersFromTheHealthyDefaultOnTheSignalItTrips(t *testing.T) {
	now := t0
	def := fake.DefaultHealth(now, "worker-0")
	eq(t, "the default is healthy and idle", withoutTime(def), agentapi.Health{ProbeOK: true, KVCacheUsage: 0.1})

	stalled := fake.HealthStalled(now, "worker-0")
	if stalled.RunningRequests == 0 || stalled.GeneratedTokens == 0 {
		t.Errorf("a stall needs requests running and a counter that stays above zero: %+v", stalled)
	}
	failing := fake.HealthFailingProbes()(now, "worker-0")
	if failing.ProbeOK || failing.ConsecutiveProbeFailures == 0 {
		t.Errorf("failing probes: %+v", failing)
	}
	errs := fake.HealthErrorsOnly()(now, "worker-0")
	if !errs.ProbeOK || errs.ResponsesByStatus["500"] == 0 || len(errs.ResponsesByStatus) != 1 {
		t.Errorf("errors only: want a passing probe and 500s alone, got %+v", errs)
	}
}
