package lifecycle

// The throughput gate with several workers (HBEP-7, Several workers; Test plan,
// Over-allocation and the throughput gate, Several workers): each worker is
// judged on its own measurement at its share of the sessions, never on the mean
// the Instance's status.throughput holds. This version records only.

import (
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	afake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

// gateTwoWorkers runs an Instance of two workers behind the router to ready.
// worker-0 measures tpot0 and worker-1 tpot1 per output token, 300 ms to first
// token. The engine double is told the round and the request, not the worker,
// and the agent asks worker-0 then worker-1 in the same step for each round (a
// worker's checks run in desired order), so the first call of a round is
// worker-0's and the second worker-1's. The row's expected message names which
// worker measured what, so a swapped order would fail it.
func gateTwoWorkers(t *testing.T, tpot0, tpot1 time.Duration, target int32) (*rig, v1alpha1.Instance) {
	t.Helper()
	rg := newRig(t)
	rg.r.Config.InstanceRouter = v1alpha1.InstanceRouterEnvoy
	calls := map[int32]int{}
	rg.engine = func(string) afake.Engine {
		return afake.Engine{WarmUpRound: func(n int32, req agentapi.WarmUpRequest) agentapi.WarmUpRound {
			worker := calls[n] % 2
			calls[n]++
			return gateRound([]time.Duration{tpot0, tpot1}[worker], 300*time.Millisecond)(n, req)
		}}
	}
	gateEntries(rg, withWorkers(gateMD(func(md *v1alpha1.ModelDeployment) { md.Spec.Workload.TargetSessions = target }), 2), oneEntry())
	startUntil(rg, "md", 10*time.Minute, "ready, every worker admitted", func() bool {
		if rg.readyCount("md") != 1 {
			return false
		}
		in := rg.instances("md")[0]
		return routerAdmitted(&in, "worker-0") && routerAdmitted(&in, "worker-1")
	})
	return rg, rg.instances("md")[0]
}

// workersperinstance 2 and targetSessions 16: each worker runs 8 sessions and
// matches the entry. Worker-0 at 20 ms and worker-1 at 45 ms would miss, naming
// worker-1, though the mean, 32.5 ms, is under 40 ms: the Instance's
// status.throughput holds the mean, and the gate judged the workers. Both at
// 39 ms pass. Either way the Instance is ready with no restart.
func TestGateJudgesEachOfTwoWorkers(t *testing.T) {
	ms := time.Millisecond
	t.Run("20 ms and 45 ms would miss, by worker-1", func(t *testing.T) {
		rg, in := gateTwoWorkers(t, 20*ms, 45*ms, 16)
		msg := conditionMessage(&in, v1alpha1.ConditionQualified)
		for _, want := range []string{"worker-1 would miss the entry dated 2026-10-23", "worker-0: time per output token 20ms (limit 40ms)", "worker-1: time per output token 45ms (limit 40ms)"} {
			if !strings.Contains(msg, want) {
				t.Errorf("Qualified's message %q does not hold %q", msg, want)
			}
		}
		if strings.Contains(msg, "worker-0 would miss") {
			t.Errorf("Qualified's message %q blames worker-0", msg)
		}
		if tp := in.Status.Throughput; tp == nil || tp.TimePerOutputToken != "32.5ms" {
			t.Errorf("status.throughput %+v, want the workers' mean, 32.5ms: the gate must not have judged it", tp)
		}
		if in.Status.Gate != nil {
			t.Errorf("status.gate %+v after a would-miss, want none", in.Status.Gate)
		}
		gateRecordsOnly(t, in, rg)
	})
	t.Run("39 ms and 39 ms pass", func(t *testing.T) {
		rg, in := gateTwoWorkers(t, 39*ms, 39*ms, 16)
		msg := conditionMessage(&in, v1alpha1.ConditionQualified)
		for _, want := range []string{"passed the entry dated 2026-10-23", "worker-0: time per output token 39ms (limit 40ms)", "worker-1: time per output token 39ms (limit 40ms)"} {
			if !strings.Contains(msg, want) {
				t.Errorf("Qualified's message %q does not hold %q", msg, want)
			}
		}
		if g := in.Status.Gate; g == nil || g.Verdict != v1alpha1.GatePassed || g.ReferenceDate != "2026-10-23" {
			t.Errorf("status.gate %+v, want Passed against 2026-10-23", g)
		}
		gateRecordsOnly(t, in, rg)
	})
	t.Run("targetSessions 32: 16 sessions a worker, no match", func(t *testing.T) {
		rg, in := gateTwoWorkers(t, 41*ms, 41*ms, 32)
		msg := conditionMessage(&in, v1alpha1.ConditionQualified)
		if !strings.Contains(msg, "records only: no entry matches") || !strings.Contains(msg, "16 sessions") {
			t.Errorf("Qualified's message %q, want records only, no entry matches 16 sessions", msg)
		}
		if g := in.Status.Gate; g == nil || g.Verdict != v1alpha1.GateRecordOnly || g.ReferenceDate != "" {
			t.Errorf("status.gate %+v, want RecordOnly and no reference date", g)
		}
		gateRecordsOnly(t, in, rg)
	})
}
