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
		eq(t, fmt.Sprintf("sample %d, apart from its time", k), withoutTime(h), withZeroCounters(agentapi.Health{
			ProbeOK: true, RunningRequests: i64p(8), WaitingRequests: i64p(0), GeneratedTokens: i64p(12345), KVCacheUsage: f64p(0.5),
		}))
	}
	// The clock moved 5 minutes between samples; the counter did not.
	eq(t, "time between samples", samples[1].Time.Sub(samples[0].Time), 5*time.Minute)
}

// withoutTime returns h without its Time and its MetricsReadAt, which a test
// compares on its own (TestEveryHealthDoubleReadsItsMetricsAtTheSampleTime): a
// strip that hid a missing MetricsReadAt would pass a double that left it out.
func withoutTime(h agentapi.Health) agentapi.Health {
	h.Time = time.Time{}
	h.MetricsReadAt = nil
	return h
}

func i64p(v int64) *int64     { return &v }
func boolp(v bool) *bool      { return &v }
func f64p(v float64) *float64 { return &v }

// withZeroCounters returns h with the five counters HBEP-8 adds (preemptions,
// the two prefix-cache counters, and the sum and count of the time per output
// token) set to the explicit zeros every Health double starts from.
func withZeroCounters(h agentapi.Health) agentapi.Health {
	h.Preemptions = i64p(0)
	h.PrefixCacheQueriedTokens, h.PrefixCacheHitTokens = i64p(0), i64p(0)
	h.TimePerOutputTokenSecondsSum, h.TimePerOutputTokenCount = f64p(0), i64p(0)
	return h
}

func TestHealthSlowPrefillHasOneRequestRunningAndNoTokenWhateverTheClockSays(t *testing.T) {
	for _, elapsed := range []time.Duration{1100 * time.Second, 30 * time.Minute} {
		samples := healthSamples(t, fake.HealthSlowPrefill, 3, elapsed/2)
		eq(t, fmt.Sprintf("%v: samples", elapsed), len(samples), 2)
		for _, h := range samples {
			eq(t, fmt.Sprintf("%v: sample", elapsed), withoutTime(h), withZeroCounters(agentapi.Health{
				ProbeOK: true, RunningRequests: i64p(1), WaitingRequests: i64p(0), GeneratedTokens: i64p(0), KVCacheUsage: f64p(0.05),
			}))
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
		eq(t, fmt.Sprintf("sample %d", k), withoutTime(h), withZeroCounters(agentapi.Health{
			ProbeOK: true, RunningRequests: i64p(256), WaitingRequests: i64p(1000),
			GeneratedTokens: i64p(int64(1000 * (k + 1))), KVCacheUsage: f64p(1),
		}))
	}
}

func TestHealthCountersAreKeptPerWorker(t *testing.T) {
	r := newRig(t, fake.Engine{Health: fake.HealthPlainLoad()})
	r.set(run(instance(withWorkers(0, 0))))
	r.steps(3)
	for _, name := range []string{"worker-0", "worker-1"} {
		eq(t, name+" tokens at its second sample", *r.workerOf(name).Health.GeneratedTokens, int64(2000))
	}
}

func TestHealthFailingProbesFailsAndCountsTheFailuresInARow(t *testing.T) {
	samples := healthSamples(t, fake.HealthFailingProbes(), 4, time.Second)
	eq(t, "samples", len(samples), 3)
	for k, h := range samples {
		eq(t, fmt.Sprintf("sample %d", k), withoutTime(h), withZeroCounters(agentapi.Health{
			ProbeOK: false, ConsecutiveProbeFailures: int32(k + 1),
			RunningRequests: i64p(0), WaitingRequests: i64p(0), GeneratedTokens: i64p(0), KVCacheUsage: f64p(0.1),
		}))
	}
}

func TestHealthErrorsOnlyAnswersEveryRequestWithAServerError(t *testing.T) {
	samples := healthSamples(t, fake.HealthErrorsOnly(), 4, time.Second)
	eq(t, "samples", len(samples), 3)
	for k, h := range samples {
		eq(t, fmt.Sprintf("sample %d", k), withoutTime(h), withZeroCounters(agentapi.Health{
			ProbeOK: true, RunningRequests: i64p(0), WaitingRequests: i64p(0), GeneratedTokens: i64p(0), KVCacheUsage: f64p(0.1),
			ResponsesByStatus: map[string]int64{"5xx": int64(10 * (k + 1))},
		}))
	}
}

func TestEachHealthDoubleDiffersFromTheHealthyDefaultOnTheSignalItTrips(t *testing.T) {
	now := t0
	def := fake.DefaultHealth(now, fake.Process{Worker: "worker-0", PID: 4000})
	eq(t, "the default is healthy and idle", withoutTime(def), withZeroCounters(agentapi.Health{
		ProbeOK: true, RunningRequests: i64p(0), WaitingRequests: i64p(0), GeneratedTokens: i64p(0), KVCacheUsage: f64p(0.1),
	}))

	stalled := fake.HealthStalled(now, fake.Process{Worker: "worker-0", PID: 4000})
	if *stalled.RunningRequests == 0 || *stalled.GeneratedTokens == 0 {
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
			field:  func(h agentapi.Health) any { return *h.GeneratedTokens },
			before: int64(3000), first: int64(1000), second: int64(2000),
		},
		{
			name:   "HealthEngineCounters",
			engine: fake.HealthEngineCounters,
			field:  func(h agentapi.Health) any { return *h.Preemptions },
			before: int64(3), first: int64(1), second: int64(2),
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

// Every Health double that models a readable /metrics sets MetricsReadAt to the
// sample's own time and all its gauges and counters, a zero explicitly. A double that left
// them out would read as "unknown" and the controller's tests on it would
// trip for the wrong reason (`@hpc-burst-science-agent`, 31 Oct 2026).
func TestEveryHealthDoubleReadsItsMetricsAtTheSampleTime(t *testing.T) {
	proc := fake.Process{Worker: "worker-0", PID: 4000}
	for name, h := range map[string]func(time.Time, fake.Process) agentapi.Health{
		"DefaultHealth":        fake.DefaultHealth,
		"HealthStalled":        fake.HealthStalled,
		"HealthSlowPrefill":    fake.HealthSlowPrefill,
		"HealthPlainLoad":      fake.HealthPlainLoad(),
		"HealthFailingProbes":  fake.HealthFailingProbes(),
		"HealthErrorsOnly":     fake.HealthErrorsOnly(),
		"HealthEngineCounters": fake.HealthEngineCounters(),
	} {
		got := h(t0, proc)
		if got.MetricsReadAt == nil || !got.MetricsReadAt.Equal(got.Time) || got.Time.IsZero() {
			t.Errorf("%s: MetricsReadAt = %v, Time = %v; want a read at the sample's time", name, got.MetricsReadAt, got.Time)
		}
		if got.RunningRequests == nil || got.WaitingRequests == nil || got.GeneratedTokens == nil || got.KVCacheUsage == nil {
			t.Errorf("%s: a gauge is nil (unknown): %+v", name, got)
		}
		if got.Preemptions == nil || got.PrefixCacheQueriedTokens == nil || got.PrefixCacheHitTokens == nil ||
			got.TimePerOutputTokenSecondsSum == nil || got.TimePerOutputTokenCount == nil {
			t.Errorf("%s: an engine counter of HBEP-8 is nil (unknown): %+v", name, got)
		}
		if !got.MetricsFresh(t0, t0, t0, 10*time.Second) {
			t.Errorf("%s: its metrics are not fresh", name)
		}
	}
}

func TestHealthMetricsUnreadableHasNoValueAndNoRead(t *testing.T) {
	h := fake.HealthMetricsUnreadable(t0, fake.Process{Worker: "worker-0", PID: 4000})
	eq(t, "sample", h, agentapi.Health{Time: t0, ProbeOK: true})
	if h.MetricsFresh(t0, t0, t0, 10*time.Second) || h.MetricsFresh(t0, t0, t0, time.Hour) {
		t.Error("an engine that was never read has fresh metrics")
	}
}

func TestHealthMetricsStaleLooksStalledButIsReadLongAgo(t *testing.T) {
	proc := fake.Process{Worker: "worker-0", PID: 4000}
	for _, c := range []struct {
		age   time.Duration
		fresh bool
	}{{5 * time.Second, true}, {20 * time.Second, true}, {21 * time.Second, false}, {10 * time.Minute, false}} {
		h := fake.HealthMetricsStale(c.age)(t0, proc)
		read := t0.Add(-c.age)
		eq(t, fmt.Sprintf("age %v: the read", c.age), h.MetricsReadAt, &read)
		// The values are HealthStalled's, which a controller that ignored the
		// age would restart for a stall.
		eq(t, fmt.Sprintf("age %v: apart from its read", c.age), withoutTime(h), withoutTime(fake.HealthStalled(t0, proc)))
		if got := h.MetricsFresh(t0, t0, t0, 10*time.Second); got != c.fresh {
			t.Errorf("age %v: MetricsFresh(10s) = %v, want %v", c.age, got, c.fresh)
		}
	}
}

// HealthCached sends the sample of its first call again and again while the
// Agent's reports go on: the sample's Time and MetricsReadAt stay at the first
// time, the report's time moves, and the metrics turn stale by the report's age
// (MetricsFresh), which the sample alone cannot show.
func TestHealthCachedKeepsItsFirstSampleWhileTheReportsMoveOn(t *testing.T) {
	const interval = 10 * time.Second
	samples := healthSamples(t, fake.HealthCached(), 6, 5*time.Second)
	eq(t, "samples", len(samples), 5)
	first := samples[0].Time
	for k, h := range samples {
		eq(t, fmt.Sprintf("sample %d is the first one", k), h, samples[0])
		eq(t, fmt.Sprintf("sample %d reads at its own time", k), *h.MetricsReadAt, first)
		reportTime := first.Add(time.Duration(k) * 5 * time.Second) // the report of sample k
		// The report is received as it is made, so only its age after the sample counts.
		want := time.Duration(k)*5*time.Second <= 2*interval
		if got := h.MetricsFresh(reportTime, reportTime, reportTime, interval); got != want {
			t.Errorf("sample %d in a report made %v after it: MetricsFresh = %v, want %v", k, reportTime.Sub(first), got, want)
		}
	}
	// The last sample's report is 20 s after the sample, which is exactly two
	// intervals: still fresh. One report later it is stale.
	if !samples[4].MetricsFresh(first.Add(20*time.Second), first.Add(20*time.Second), first.Add(20*time.Second), interval) ||
		samples[4].MetricsFresh(first.Add(25*time.Second), first.Add(25*time.Second), first.Add(25*time.Second), interval) {
		t.Error("the boundary of two intervals between sample and report is not where it should be")
	}
}

// The two doubles of HBEP-8's counters: one whose counters rise, one that omits
// the prefix-cache lines. The expected values are written out from the
// doubles' documentation, not computed with their code.
func TestHealthEngineCountersRiseWithEverySample(t *testing.T) {
	samples := healthSamples(t, fake.HealthEngineCounters(), 4, 0)
	eq(t, "samples", len(samples), 3)
	for k, h := range samples {
		n := int64(k + 1)
		eq(t, fmt.Sprintf("sample %d", k), withoutTime(h), agentapi.Health{
			ProbeOK: true, RunningRequests: i64p(4), WaitingRequests: i64p(0), GeneratedTokens: i64p(1000 * n), KVCacheUsage: f64p(0.5),
			Preemptions:              i64p(n),
			PrefixCacheQueriedTokens: i64p(1000 * n), PrefixCacheHitTokens: i64p(800 * n),
			TimePerOutputTokenSecondsSum: f64p(0.5 * float64(n)), TimePerOutputTokenCount: i64p(10 * n),
		})
	}
}

func TestHealthWithoutPrefixCacheOmitsOnlyThePrefixCacheCounters(t *testing.T) {
	samples := healthSamples(t, fake.HealthWithoutPrefixCache, 3, time.Second)
	eq(t, "samples", len(samples), 2)
	for k, h := range samples {
		got := withoutTime(h)
		if got.PrefixCacheQueriedTokens != nil || got.PrefixCacheHitTokens != nil {
			t.Errorf("sample %d: the prefix-cache counters are %v %v, want nil (unknown)", k, got.PrefixCacheQueriedTokens, got.PrefixCacheHitTokens)
		}
		got.PrefixCacheQueriedTokens, got.PrefixCacheHitTokens = i64p(0), i64p(0)
		eq(t, fmt.Sprintf("sample %d: everything else is a known zero or default", k), got, withZeroCounters(agentapi.Health{
			ProbeOK: true, RunningRequests: i64p(0), WaitingRequests: i64p(0), GeneratedTokens: i64p(0), KVCacheUsage: f64p(0.1),
		}))
	}
}
