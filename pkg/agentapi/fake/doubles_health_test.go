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
func healthSamples(t *testing.T, h func(time.Time, fake.Process) agentapi.Health, n int, gap time.Duration) []agentapi.Health {
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
			ProbeOK: true, KVCacheUsage: 0.1, ResponsesByStatus: map[string]int64{"5xx": int64(10 * (k + 1))},
		})
	}
}

func TestEachHealthDoubleDiffersFromTheHealthyDefaultOnTheSignalItTrips(t *testing.T) {
	now := t0
	def := fake.DefaultHealth(now, fake.Process{Worker: "worker-0", PID: 4000})
	eq(t, "the default is healthy and idle", withoutTime(def), agentapi.Health{ProbeOK: true, KVCacheUsage: 0.1})

	stalled := fake.HealthStalled(now, fake.Process{Worker: "worker-0", PID: 4000})
	if stalled.RunningRequests == 0 || stalled.GeneratedTokens == 0 {
		t.Errorf("a stall needs requests running and a counter that stays above zero: %+v", stalled)
	}
	failing := fake.HealthFailingProbes()(now, fake.Process{Worker: "worker-0", PID: 4000})
	if failing.ProbeOK || failing.ConsecutiveProbeFailures == 0 {
		t.Errorf("failing probes: %+v", failing)
	}
	errs := fake.HealthErrorsOnly()(now, fake.Process{Worker: "worker-0", PID: 4000})
	if !errs.ProbeOK || agentapi.ServerErrors(errs.ResponsesByStatus) == 0 || len(errs.ResponsesByStatus) != 1 {
		t.Errorf("errors only: want a passing probe and server errors alone, got %+v", errs)
	}
}

func TestHealthCountersStartAgainForARestartedWorkerProcess(t *testing.T) {
	cases := []struct {
		name   string
		engine func() func(time.Time, fake.Process) agentapi.Health
		field  func(agentapi.Health) any // the counter the double raises
		before any                       // after three samples of the first process
		first  any                       // the first sample of the new process
		second any                       // its second
	}{
		{
			name:   "HealthFailingProbes",
			engine: fake.HealthFailingProbes,
			field:  func(h agentapi.Health) any { return h.ConsecutiveProbeFailures },
			before: int32(3), first: int32(1), second: int32(2),
		},
		{
			name:   "HealthPlainLoad",
			engine: fake.HealthPlainLoad,
			field:  func(h agentapi.Health) any { return h.GeneratedTokens },
			before: int64(3000), first: int64(1000), second: int64(2000),
		},
		{
			name:   "HealthErrorsOnly",
			engine: fake.HealthErrorsOnly,
			field:  func(h agentapi.Health) any { return h.ResponsesByStatus },
			before: map[string]int64{"5xx": 30}, first: map[string]int64{"5xx": 10}, second: map[string]int64{"5xx": 20},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := newRig(t, fake.Engine{Health: c.engine()})
			r.set(run(instance()))
			r.steps(4) // starting; then three samples
			eq(t, "the counter after three samples of the first process", c.field(*r.workerOf("worker-0").Health), c.before)

			r.set(run(instance(withWorkers(1)))) // a worker restart: a new process
			r.step()
			w := r.workerOf("worker-0")
			eq(t, "the new process is starting", [2]any{w.Phase, w.PID}, [2]any{agentapi.PartStarting, 4001})
			eq(t, "no sample while it starts", w.Health, (*agentapi.Health)(nil))

			r.step()
			eq(t, "the new process's first sample starts the counter again", c.field(*r.workerOf("worker-0").Health), c.first)
			r.step()
			eq(t, "its second sample", c.field(*r.workerOf("worker-0").Health), c.second)
		})
	}
}

func TestHealthCountersStartAgainForAnInstanceRestartAndAfterARequeue(t *testing.T) {
	r := newRig(t, fake.Engine{Health: fake.HealthFailingProbes()})
	r.set(run(instance()))
	r.steps(3) // starting; samples 1 and 2
	eq(t, "failures before", r.workerOf("worker-0").Health.ConsecutiveProbeFailures, int32(2))

	r.set(run(instance(withGeneration(2)))) // an Instance restart
	r.steps(2)
	eq(t, "failures at the first sample after an Instance restart", r.workerOf("worker-0").Health.ConsecutiveProbeFailures, int32(1))

	r.a.Requeue() // a second execution: its process is a new one
	r.steps(2)
	eq(t, "failures at the first sample after a requeue", r.workerOf("worker-0").Health.ConsecutiveProbeFailures, int32(1))
}

func TestEngineHealthIsToldWhichProcessItSamples(t *testing.T) {
	var got []fake.Process
	e := fake.Engine{Health: func(now time.Time, p fake.Process) agentapi.Health {
		got = append(got, p)
		return fake.DefaultHealth(now, p)
	}}
	r := newRig(t, e)
	r.set(run(instance(withWorkers(2))))
	r.steps(2)
	r.clk.Advance(7 * time.Second) // t0+7 s
	r.set(run(instance(withWorkers(5))))
	r.steps(2)

	eq(t, "the processes sampled", got, []fake.Process{
		{Worker: "worker-0", PID: 4000, Start: t0, Generation: 2},
		{Worker: "worker-0", PID: 4001, Start: t0.Add(7 * time.Second), Generation: 5},
	})
}
