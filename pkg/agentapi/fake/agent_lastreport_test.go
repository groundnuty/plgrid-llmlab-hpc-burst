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
	e := fake.Engine{Health: func(now time.Time, _ fake.Process) agentapi.Health {
		return agentapi.Health{
			ProbeOK: true, MetricsReadAt: &read, RunningRequests: &running, WaitingRequests: &waiting,
			GeneratedTokens: &tokens, KVCacheUsage: &kv,
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

	after, _ := r.a.LastReport()
	got := after.Instance.Workers[0].Health
	eq(t, "tokens", *got.GeneratedTokens, int64(100))
	eq(t, "running", *got.RunningRequests, int64(3))
	eq(t, "waiting", *got.WaitingRequests, int64(1))
	eq(t, "KV cache", *got.KVCacheUsage, 0.25)
	eq(t, "metrics read at", *got.MetricsReadAt, t0)
	// And the Backend's own copy.
	eq(t, "the Backend's tokens", *r.workerOf("worker-0").Health.GeneratedTokens, int64(100))
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
