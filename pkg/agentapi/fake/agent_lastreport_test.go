package fake_test

import (
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

// Agent.LastReport is what the Backend accepted. An Engine callback returns
// pointers into its own variables (Health's gauges), and it may change those
// variables later; neither the Backend's copy nor the Agent's retained report
// may change with them (Astra's finding on PR 92).
func TestLastReportDoesNotFollowAnEngineCallbacksVariables(t *testing.T) {
	tokens, running, waiting, kv, read := int64(100), int64(3), int64(1), 0.25, t0
	preempt, queried, hit, tpotSum, tpotCount := int64(2), int64(300), int64(200), 1.5, int64(30)
	e := fake.Engine{Health: func(now time.Time, _ fake.Process) agentapi.Health {
		return agentapi.Health{
			ProbeOK: true, MetricsReadAt: &read, RunningRequests: &running, WaitingRequests: &waiting,
			GeneratedTokens: &tokens, KVCacheUsage: &kv,
			Preemptions: &preempt, PrefixCacheQueriedTokens: &queried, PrefixCacheHitTokens: &hit,
			TimePerOutputTokenSecondsSum: &tpotSum, TimePerOutputTokenCount: &tpotCount,
		}
	}}
	r := newRig(t, e)
	r.set(run(instance()))
	r.steps(2) // the worker starts; then it is sampled and reported

	before, ok := r.a.LastReport()
	if !ok || before.Instance == nil || len(before.Instance.Workers) == 0 || before.Instance.Workers[0].Health == nil {
		t.Fatalf("no report with a sample: %+v", before)
	}
	h := before.Instance.Workers[0].Health
	eq(t, "tokens in the retained report", *h.GeneratedTokens, int64(100))

	// The callback's variables change after the report was accepted.
	tokens, running, waiting, kv, read = 999, 99, 98, 0.99, t0.Add(time.Hour)
	preempt, queried, hit, tpotSum, tpotCount = 999, 998, 997, 99.5, 996

	after, _ := r.a.LastReport()
	got := after.Instance.Workers[0].Health
	eq(t, "tokens", *got.GeneratedTokens, int64(100))
	eq(t, "running", *got.RunningRequests, int64(3))
	eq(t, "waiting", *got.WaitingRequests, int64(1))
	eq(t, "KV cache", *got.KVCacheUsage, 0.25)
	eq(t, "metrics read at", *got.MetricsReadAt, t0)
	eq(t, "preemptions", *got.Preemptions, int64(2))
	eq(t, "prefix-cache queried tokens", *got.PrefixCacheQueriedTokens, int64(300))
	eq(t, "prefix-cache hit tokens", *got.PrefixCacheHitTokens, int64(200))
	eq(t, "time per output token, sum", *got.TimePerOutputTokenSecondsSum, 1.5)
	eq(t, "time per output token, count", *got.TimePerOutputTokenCount, int64(30))
	// And the Backend's own copy.
	eq(t, "the Backend's tokens", *r.workerOf("worker-0").Health.GeneratedTokens, int64(100))
	bh := r.workerOf("worker-0").Health
	eq(t, "the Backend's preemptions", *bh.Preemptions, int64(2))
	eq(t, "the Backend's prefix-cache queries", *bh.PrefixCacheQueriedTokens, int64(300))
	eq(t, "the Backend's prefix-cache hits", *bh.PrefixCacheHitTokens, int64(200))
	eq(t, "the Backend's time per output token sum", *bh.TimePerOutputTokenSecondsSum, 1.5)
	eq(t, "the Backend's time per output token count", *bh.TimePerOutputTokenCount, int64(30))
}

// A known-empty ResponsesByStatus and an unknown (nil) one stay what they are
// through the Agent and the Backend; only the wire's encoding can lose the
// difference (agentapi.Health.ResponsesByStatus).
func TestAnEmptyResponseCountSurvivesTheAgentAndTheBackend(t *testing.T) {
	for _, c := range []struct {
		name string
		in   map[string]int64
	}{{"empty", map[string]int64{}}, {"nil", nil}} {
		t.Run(c.name, func(t *testing.T) {
			e := fake.Engine{Health: func(now time.Time, p fake.Process) agentapi.Health {
				h := fake.DefaultHealth(now, p)
				h.ResponsesByStatus = c.in
				return h
			}}
			r := newRig(t, e)
			r.set(run(instance()))
			r.steps(2)
			last, _ := r.a.LastReport()
			for name, got := range map[string]map[string]int64{
				"the Backend's report": r.workerOf("worker-0").Health.ResponsesByStatus,
				"Agent.LastReport":     last.Instance.Workers[0].Health.ResponsesByStatus,
			} {
				if (got == nil) != (c.in == nil) || len(got) != 0 {
					t.Errorf("%s: ResponsesByStatus = %#v, want %#v", name, got, c.in)
				}
			}
		})
	}
}
