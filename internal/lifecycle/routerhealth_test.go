package lifecycle

// Worker health behind the router, on the controller's side (routerhealth.go; HBEP-7, Several workers;
// the science agent's rulings on #429). The rig's fake agent runs the router as the real agent does and
// reports Envoy's view of the worker; RouterView and CrashRouter make the faults happen. Expected values
// come from HBEP-7's rules and the rulings, never from the code under test.

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	afake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

// routerRig brings one Instance of "md", behind a router, to ready.
func routerRig(t *testing.T, opts ...rigOption) (*rig, v1alpha1.Instance, *afake.Agent) {
	t.Helper()
	rg := newRig(t, opts...)
	rg.r.Config.InstanceRouter = v1alpha1.InstanceRouterEnvoy
	rg.engine = func(string) afake.Engine { return afake.Engine{} }
	rg.create(mdSpec("md"))
	in, alloc := startReady(rg)
	return rg, in, rg.agentOf(alloc).agent
}

func healthyWorkersOf(rg *rig, name string) *int32 { return rg.instance(name).Status.HealthyWorkers }

// countOf prints a healthyWorkers value: its number, or "absent".
func countOf(h *int32) string {
	if h == nil {
		return "absent"
	}
	return fmt.Sprint(*h)
}

func lastRestart(rg *rig, name string) (v1alpha1.InstanceRestart, bool) {
	rs := rg.instance(name).Status.Restarts
	if len(rs) == 0 {
		return v1alpha1.InstanceRestart{}, false
	}
	return rs[len(rs)-1], true
}

func TestMaxAttemptsPerRungStaysEightWhileMaxRestartsKeptIs32(t *testing.T) {
	if maxRestartsKept != 32 || maxAttemptsPerRung != 8 {
		t.Errorf("maxRestartsKept %d, maxAttemptsPerRung %d; want 32 (the CRD's maxItems) and 8, its own constant", maxRestartsKept, maxAttemptsPerRung)
	}
	if 3*maxAttemptsPerRung > maxRestartsKept {
		t.Errorf("three rungs of %d attempts do not fit %d restarts", maxAttemptsPerRung, maxRestartsKept)
	}
}

// The desired worker carries verifiedGeneration only once the controller has verified and warmed up
// its generation: none before the Instance is first ready, its generation after.
func TestVerifiedGenerationIsDesiredOnlyOnceVerified(t *testing.T) {
	rg, in, ag := routerRig(t)
	sawNone, sawVerified := false, false
	for _, d := range ag.Seen() {
		if d.Instance == nil || len(d.Instance.Workers) == 0 {
			continue
		}
		switch v := d.Instance.Workers[0].VerifiedGeneration; {
		case v == nil:
			sawNone = true
		case *v == d.Instance.Workers[0].Generation:
			sawVerified = true
		default:
			t.Errorf("a desired state carries verifiedGeneration %d for worker generation %d", *v, d.Instance.Workers[0].Generation)
		}
	}
	if !sawNone || !sawVerified {
		t.Errorf("seen: none before verification %v, verified after %v; want both", sawNone, sawVerified)
	}
	if h := healthyWorkersOf(rg, in.Name); h == nil || *h != 1 {
		t.Errorf("a ready Instance behind a router has healthyWorkers %s, want 1", countOf(h))
	}
}

// healthyWorkers follows a current report: an ejected worker is not healthy, and it counts again once
// back. Neither is a restart signal.
func TestHealthyWorkersFollowTheReport(t *testing.T) {
	rg, in, ag := routerRig(t)
	ag.RouterView = func(rr *agentapi.RouterReport) {
		for i := range rr.Workers {
			rr.Workers[i].Healthy, rr.Workers[i].Ejected = false, true
		}
	}
	startUntil(rg, "md", 2*time.Minute, "healthyWorkers 0", func() bool { h := healthyWorkersOf(rg, in.Name); return h != nil && *h == 0 })
	ag.RouterView = nil
	startUntil(rg, "md", 2*time.Minute, "healthyWorkers 1", func() bool { h := healthyWorkersOf(rg, in.Name); return h != nil && *h == 1 })
	if got := healthFailing(rg, in.Name); len(got) != 0 {
		t.Errorf("an ejection raised %v, want no restart", got)
	}
	if p := rg.instance(in.Name).Status.Phase; p != v1alpha1.InstanceReady {
		t.Errorf("phase %s, want ready", p)
	}
}

// The router rung: the router's exit writes healthyWorkers 0, records the restart under part router
// with a RouterRestart event, and raises the router's generation; the phase stays ready, nothing is
// verified again, and healthyWorkers returns once the restarted router holds the worker healthy.
func TestTheRouterRungRestartsOnlyTheRouter(t *testing.T) {
	rg, in, ag := routerRig(t)
	workerGen := restartGeneration(rg.instance(in.Name), v1alpha1.WorkerName(0))
	ag.CrashRouter(1)
	healthUntilEvent(rg, in.Name, 0, time.Minute, startTick)
	got := rg.instance(in.Name)
	if h := got.Status.HealthyWorkers; h == nil || *h != 0 {
		t.Errorf("after the rung, healthyWorkers %s, want 0", countOf(h))
	}
	if r, ok := lastRestart(rg, in.Name); !ok || r.Part != v1alpha1.PartRouter || r.Generation != 1 || r.Signal != v1alpha1.ReasonPartExited {
		t.Errorf("the last restart is %+v, want the router's, generation 1, signal PartExited", r)
	}
	if g := restartGeneration(got, v1alpha1.PartRouter); g != 1 {
		t.Errorf("the router's generation is %d, want 1", g)
	}
	if got.Status.Phase != v1alpha1.InstanceReady || !isTrue(got.Status.Conditions, v1alpha1.ConditionVerified) {
		t.Errorf("phase %s, Verified %v: the router rung keeps the Instance ready and verified", got.Status.Phase, isTrue(got.Status.Conditions, v1alpha1.ConditionVerified))
	}
	if f := healthFailing(rg, in.Name); !slices.Equal(f, []string{v1alpha1.ReasonRouterRestart}) {
		t.Errorf("failing events %v, want [RouterRestart]", f)
	}
	startUntil(rg, "md", 2*time.Minute, "healthyWorkers back", func() bool { h := healthyWorkersOf(rg, in.Name); return h != nil && *h == 1 })
	if g := restartGeneration(rg.instance(in.Name), v1alpha1.WorkerName(0)); g != workerGen {
		t.Errorf("the router rung moved worker-0's generation from %d to %d", workerGen, g)
	}
}

// The echo rule: a report whose router part does not carry the desired router generation is stale,
// and its healthy count cannot undo the rung's 0.
func TestAStaleRouterReportCannotUndoTheRung(t *testing.T) {
	rg, in, ag := routerRig(t)
	ag.CrashRouter(1)
	healthUntilEvent(rg, in.Name, 0, time.Minute, startTick)
	// The router's report keeps generation 0, with the worker healthy: what an agent that has not yet
	// applied the raised generation, or a delayed report, would say.
	ag.RouterView = func(rr *agentapi.RouterReport) {
		rr.Generation, rr.Phase = 0, agentapi.PartRunning
		for i := range rr.Workers {
			rr.Workers[i].Healthy, rr.Workers[i].FailedActiveCheck = true, false
		}
	}
	for i := 0; i < 6; i++ {
		startStep(rg, "md", startTick)
		if h := healthyWorkersOf(rg, in.Name); h == nil || *h != 0 {
			t.Fatalf("step %d: healthyWorkers %s from a stale report, want the rung's 0", i, countOf(h))
		}
	}
	ag.RouterView = nil
	startUntil(rg, "md", 2*time.Minute, "healthyWorkers back from a current report", func() bool { h := healthyWorkersOf(rg, in.Name); return h != nil && *h == 1 })
}

// The router's probe failing failureThreshold reports in a row while it holds the worker healthy is
// the router's signal. Failing while no worker is healthy is not: that is the worker's.
func TestTheRouterProbeSignal(t *testing.T) {
	t.Run("a worker healthy", func(t *testing.T) {
		rg, in, ag := routerRig(t)
		ag.RouterView = func(rr *agentapi.RouterReport) { rr.Probe.OK, rr.Probe.ConsecutiveFailures = false, 3 }
		healthUntilEvent(rg, in.Name, 0, time.Minute, startTick)
		if r, ok := lastRestart(rg, in.Name); !ok || r.Part != v1alpha1.PartRouter || r.Signal != v1alpha1.ReasonProbeFailing {
			t.Errorf("the last restart is %+v, want the router's for ProbeFailing", r)
		}
		// The report that raised the signal holds the worker healthy, so only the rung's own write makes
		// the Instance unroutable: healthyWorkers 0 at the Step of the restart.
		if h := healthyWorkersOf(rg, in.Name); h == nil || *h != 0 {
			t.Errorf("at the router rung, healthyWorkers %s, want 0 though the stale report holds the worker healthy", countOf(h))
		}
	})
	t.Run("control: no worker healthy", func(t *testing.T) {
		rg, in, ag := routerRig(t)
		ag.RouterView = func(rr *agentapi.RouterReport) {
			rr.Probe.OK, rr.Probe.ConsecutiveFailures = false, 3
			for i := range rr.Workers {
				rr.Workers[i].Healthy, rr.Workers[i].Ejected = false, true
			}
		}
		for i := 0; i < 6; i++ {
			startStep(rg, "md", startTick)
		}
		if r, ok := lastRestart(rg, in.Name); ok && r.Part == v1alpha1.PartRouter {
			t.Errorf("a failing probe with no worker healthy restarted the router: %+v", r)
		}
	})
}

// After attemptsPerRung router restarts within the window, the ladder climbs to the whole-Instance
// rung, which leaves ready.
func TestTheRouterRungClimbsToTheWholeInstance(t *testing.T) {
	rg, in, ag := routerRig(t, withProfile(func(p *v1alpha1.PlatformProfileSpec) {
		p.Restart = &v1alpha1.RestartPolicy{AttemptsPerRung: ptrTo(int32(1)), Window: dur(4 * time.Hour)}
	}))
	ag.CrashRouter(1)
	healthUntilEvent(rg, in.Name, 0, time.Minute, startTick)
	startUntil(rg, "md", 5*time.Minute, "the router back", func() bool { h := healthyWorkersOf(rg, in.Name); return h != nil && *h == 1 })
	ag.CrashRouter(1)
	healthUntilEvent(rg, in.Name, 1, 5*time.Minute, startTick)
	if r, ok := lastRestart(rg, in.Name); !ok || r.Part != v1alpha1.PartInstance {
		t.Errorf("the second router failure's restart is %+v, want the whole-Instance rung", r)
	}
	if f := healthFailing(rg, in.Name); !slices.Equal(f, []string{v1alpha1.ReasonRouterRestart, v1alpha1.ReasonInstanceRestart}) {
		t.Errorf("failing events %v, want RouterRestart then InstanceRestart", f)
	}
}

// The router holding a verified worker failing its check for failureThreshold reports in a row enters
// the worker rung (RouterCheckFailing); one report short does not. A worker's exit and its failing
// check in the same report are one death: one restart.
func TestRouterCheckFailingEntersTheWorkerRung(t *testing.T) {
	fail := func(n int32) func(*agentapi.RouterReport) {
		return func(rr *agentapi.RouterReport) {
			for i := range rr.Workers {
				rr.Workers[i].Healthy, rr.Workers[i].FailedActiveCheck, rr.Workers[i].ConsecutiveFailedChecks = false, true, n
			}
		}
	}
	t.Run("at the threshold", func(t *testing.T) {
		rg, in, ag := routerRig(t)
		ag.RouterView = fail(3)
		healthUntilEvent(rg, in.Name, 0, time.Minute, startTick)
		if r, ok := lastRestart(rg, in.Name); !ok || r.Part != v1alpha1.WorkerName(0) || r.Signal != v1alpha1.ReasonRouterCheckFailing {
			t.Errorf("the last restart is %+v, want worker-0's for RouterCheckFailing", r)
		}
		if p := rg.instance(in.Name).Status.Phase; p != v1alpha1.InstanceVerifying {
			t.Errorf("phase %s: with one worker, a worker restart leaves ready for verifying", p)
		}
	})
	t.Run("one report short", func(t *testing.T) {
		rg, in, ag := routerRig(t)
		ag.RouterView = fail(2)
		for i := 0; i < 6; i++ {
			startStep(rg, "md", startTick)
		}
		if f := healthFailing(rg, in.Name); len(f) != 0 {
			t.Errorf("two failed checks in a row raised %v, want nothing below the threshold of 3", f)
		}
	})
	t.Run("one restart per death", func(t *testing.T) {
		// The worker exits and the router holds it failing, in the same report: one death. Once the
		// restart is seen the router's view is Envoy's again (the restarted worker failing until it is
		// verified, then healthy), and the Instance returns to ready with that one restart.
		rg, in, ag := routerRig(t)
		ag.RouterView = fail(3)
		ag.CrashWorker(healthWorker, 137)
		healthUntilEvent(rg, in.Name, 0, time.Minute, startTick)
		ag.RouterView = nil
		startUntil(rg, "md", 10*time.Minute, "ready again", func() bool { return rg.readyCount("md") == 1 })
		rs := rg.instance(in.Name).Status.Restarts
		if len(rs) != 1 || rs[0].Signal != v1alpha1.ReasonPartExited {
			t.Errorf("restarts %+v for one death, want one, for the exit", rs)
		}
		if f := healthFailing(rg, in.Name); !slices.Equal(f, []string{v1alpha1.ReasonWorkerRestart}) {
			t.Errorf("failing events %v, want one WorkerRestart", f)
		}
	})
}

// The first entry to ready waits for the router's probe: with the worker verified and healthy but the
// probe failing, the Instance stays verifying; once it passes, the Instance is ready.
func TestTheFirstEntryToReadyWaitsForTheRouterProbe(t *testing.T) {
	rg := newRig(t)
	rg.r.Config.InstanceRouter = v1alpha1.InstanceRouterEnvoy
	rg.engine = func(string) afake.Engine { return afake.Engine{} }
	var probe bool
	rg.create(mdSpec("md"))
	// The agent exists from its job's start, long before verification ends.
	startUntil(rg, "md", 10*time.Minute, "the agent", func() bool { return len(rg.agents) == 1 })
	for _, ra := range rg.agents {
		ra.agent.RouterView = func(rr *agentapi.RouterReport) {
			if !probe {
				rr.Probe.OK = false
			}
		}
	}
	if n := rg.readyCount("md"); n != 0 {
		t.Fatalf("ready before the agent's first report: %d", n)
	}
	startUntil(rg, "md", 10*time.Minute, "verified and warmed up", func() bool {
		ins := rg.instances("md")
		return len(ins) == 1 && verifiedWorker(&ins[0])
	})
	for i := 0; i < 6; i++ {
		startStep(rg, "md", startTick)
	}
	if n := rg.readyCount("md"); n != 0 {
		t.Fatalf("%d ready with the router's probe failing, want 0", n)
	}
	probe = true
	startUntil(rg, "md", 2*time.Minute, "ready once the probe passes", func() bool { return rg.readyCount("md") == 1 })
}

// An Instance without a router never gets healthyWorkers, a verifiedGeneration or a router restart.
func TestAlphaNeverWritesHealthyWorkers(t *testing.T) {
	rg, in, alloc := healthRig(t, func() afake.Engine { return afake.Engine{} })
	if h := rg.instance(in.Name).Status.HealthyWorkers; h != nil {
		t.Errorf("alpha's Instance has healthyWorkers %d", *h)
	}
	for _, d := range rg.agentOf(alloc).agent.Seen() {
		if d.Instance != nil && (d.Instance.Router != nil || d.Instance.Workers[0].VerifiedGeneration != nil) {
			t.Fatalf("alpha's desired state carries a router or a verifiedGeneration: %+v", d.Instance)
		}
	}
}

// A router restart is not a worker try: with attemptsPerRung 1, a worker fault after the router's
// restart still enters the worker rung, not the whole Instance's.
func TestARouterRestartDoesNotSpendTheWorkerRung(t *testing.T) {
	rg, in, ag := routerRig(t, withProfile(func(p *v1alpha1.PlatformProfileSpec) {
		p.Restart = &v1alpha1.RestartPolicy{AttemptsPerRung: ptrTo(int32(1)), Window: dur(4 * time.Hour)}
	}))
	ag.CrashRouter(1)
	healthUntilEvent(rg, in.Name, 0, time.Minute, startTick)
	startUntil(rg, "md", 5*time.Minute, "the router back", func() bool { h := healthyWorkersOf(rg, in.Name); return h != nil && *h == 1 })
	ag.CrashWorker(healthWorker, 137)
	healthUntilEvent(rg, in.Name, 1, 5*time.Minute, startTick)
	if r, ok := lastRestart(rg, in.Name); !ok || r.Part != v1alpha1.WorkerName(0) {
		t.Errorf("the worker's restart after the router's is %+v, want the worker rung", r)
	}
	if f := healthFailing(rg, in.Name); !slices.Equal(f, []string{v1alpha1.ReasonRouterRestart, v1alpha1.ReasonWorkerRestart}) {
		t.Errorf("failing events %v, want RouterRestart then WorkerRestart", f)
	}
}
