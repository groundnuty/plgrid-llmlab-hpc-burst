package lifecycle

// The throughput gate with several workers (HBEP-7, Several workers; Test plan,
// Over-allocation and the throughput gate, Several workers): each worker is
// judged on its own measurement at its share of the sessions, never on the mean
// the Instance's status.throughput holds. A worker's first miss restarts that
// worker alone; a second miss of any worker fails the Instance.

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

// workersPerInstance 2 and targetSessions 16: each worker runs 8 sessions and
// matches the entry. Both at 39 ms pass, and the Instance is ready with no
// restart; at targetSessions 32 each runs 16 sessions, which no entry matches,
// so 41 ms records only. (Worker-0 at 20 ms and worker-1 at 45 ms is
// TestGateRestartsTheMissingWorkerAlone's row.)
func TestGateJudgesEachOfTwoWorkers(t *testing.T) {
	ms := time.Millisecond
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

// gateTwoWorkersMiss runs an Instance of two workers, targetSessions 16, behind the router: worker-0 measures 20 ms
// and worker-1 45 ms, which is over the 40 ms limit though the workers' mean, 32.5 ms, is under it. The engine double
// is told the round and the request, not the worker, and the agent asks worker-0 then worker-1 in the same step for
// each round, so before the first restart the first call of a round is worker-0's and the second worker-1's; after
// it only worker-1 runs rounds, since worker-0's warm-up is done. retry is what worker-1 measures then.
func gateTwoWorkersMiss(t *testing.T, retry time.Duration) (*rig, string) {
	t.Helper()
	ms := time.Millisecond
	rg := newRig(t)
	rg.r.Config.InstanceRouter = v1alpha1.InstanceRouterEnvoy
	rg.engine = func(string) afake.Engine {
		calls := map[int32]int{}
		return afake.Engine{WarmUpRound: func(n int32, req agentapi.WarmUpRequest) agentapi.WarmUpRound {
			if len(rg.rec.Find("failing", "WorkerRestart")) > 0 {
				return gateRound(retry, 300*ms)(n, req)
			}
			worker := calls[n] % 2
			calls[n]++
			return gateRound([]time.Duration{20 * ms, 45 * ms}[worker], 300*ms)(n, req)
		}}
	}
	name, _ := gateEntries(rg, withWorkers(gateMD(func(md *v1alpha1.ModelDeployment) { md.Spec.Workload.TargetSessions = 16 }), 2), oneEntry())
	startUntil(rg, "md", 10*time.Minute, "the gate's restart", func() bool { return len(rg.rec.Find("failing", "WorkerRestart")) > 0 })
	return rg, name
}

// Worker-0 at 20 ms and worker-1 at 45 ms: worker-1 alone restarts (its own rung, with the signal and the miss
// recorded), worker-0 is left running; the retry at 30 ms passes both and the Instance becomes ready, and at 45 ms
// again it fails the Instance with QualificationFailed, naming worker-1 and not worker-0, though the mean, 32.5 ms, is
// within 40 ms.
func TestGateRestartsTheMissingWorkerAlone(t *testing.T) {
	ms := time.Millisecond
	t.Run("the retry passes", func(t *testing.T) {
		rg, name := gateTwoWorkersMiss(t, 30*ms)
		in := rg.instance(name)
		if sig := gateRestartSignals(in); len(sig) != 1 || sig[0] != "worker-1/ThroughputMissed" {
			t.Fatalf("recorded restarts %v, want worker-1/ThroughputMissed alone", sig)
		}
		if g := in.Status.Gate; g == nil || g.Verdict != "" || len(g.Misses) != 1 || g.Misses[0].Worker != "worker-1" || g.Misses[0].Throughput.TimePerOutputToken != "45ms" {
			t.Fatalf("status.gate %+v, want worker-1's 45 ms miss alone and no verdict", g)
		}
		if in.Annotations[v1alpha1.RestartGenerationAnnotation("worker-0")] != "" || in.Annotations[v1alpha1.RestartGenerationAnnotation("worker-1")] != "1" {
			t.Errorf("restart generations %v, want worker-1's raised to 1 and worker-0's untouched", in.Annotations)
		}
		startUntil(rg, "md", 10*time.Minute, "ready, every worker admitted", func() bool {
			got := rg.instance(name)
			return got.Status.Phase == v1alpha1.InstanceReady && routerAdmitted(got, "worker-0") && routerAdmitted(got, "worker-1")
		})
		got := rg.instance(name)
		if g := got.Status.Gate; g == nil || g.Verdict != v1alpha1.GatePassed || len(g.Misses) != 1 {
			t.Errorf("status.gate %+v, want Passed with the first miss kept", g)
		}
		if len(gateRestartSignals(got)) != 1 || got.Status.EndReason != "" {
			t.Errorf("restarts %v, end reason %q, want worker-1's one restart and no end", gateRestartSignals(got), got.Status.EndReason)
		}
	})
	t.Run("the retry misses again", func(t *testing.T) {
		rg, name := gateTwoWorkersMiss(t, 45*ms)
		startUntil(rg, "md", 10*time.Minute, "the Instance failed", func() bool { return ended(rg.instance(name)) })
		got := rg.instance(name)
		msg := conditionMessage(got, v1alpha1.ConditionQualified)
		for _, want := range []string{"worker-1 missed twice", "the first measurement was time per output token 45ms", "the retry after a worker restart time per output token 45ms", "the entry dated 2026-10-23"} {
			if !strings.Contains(msg, want) {
				t.Errorf("Qualified's message %q does not hold %q", msg, want)
			}
		}
		if strings.Contains(msg, "worker-0 missed") {
			t.Errorf("Qualified's message %q blames worker-0", msg)
		}
		if got.Status.EndReason != v1alpha1.ReasonQualificationFailed || got.Status.ReadyTime != nil {
			t.Errorf("end reason %q, ready %v, want QualificationFailed and never ready", got.Status.EndReason, got.Status.ReadyTime)
		}
		if g := got.Status.Gate; g == nil || g.Verdict != v1alpha1.GateMissed || len(g.Misses) != 2 || g.Misses[0].Worker != "worker-1" || g.Misses[1].Worker != "worker-1" {
			t.Errorf("status.gate %+v, want Missed with worker-1's two misses", g)
		}
	})
}
