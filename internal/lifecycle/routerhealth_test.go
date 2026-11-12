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

// worker0View is worker-0 in the router part of an agent's last report, and whether there was one.
func worker0View(ag *afake.Agent) (agentapi.RouterWorker, bool) {
	rep, ok := ag.LastReport()
	if !ok || rep.Instance == nil || rep.Instance.Router == nil {
		return agentapi.RouterWorker{}, false
	}
	for _, w := range rep.Instance.Router.Workers {
		if w.Name == v1alpha1.WorkerName(0) {
			return w, true
		}
	}
	return agentapi.RouterWorker{}, false
}

// The failures Envoy counts while a restarted worker warms up are not judged against it once it is
// verified (HBEP-7, Several workers: "Envoy's check counts only for a worker whose current generation
// is verified"; #455's review). Behind the router a worker restart reverifies the worker while the
// readiness endpoint answers 503, so Envoy holds it failing and the count of failed checks grows; Envoy
// still holds it failing for a check or two after it is verified (RouterLag). The return to ready waits
// for the router's first pass, and the worker is not restarted again.
func TestFailuresCountedDuringReverificationRestartNothing(t *testing.T) {
	t.Run("a return after a worker restart", func(t *testing.T) {
		rg, in, ag := routerRig(t)
		ag.RouterLag = 2
		ag.CrashWorker(healthWorker, 137)
		healthUntilEvent(rg, in.Name, 0, time.Minute, startTick)
		var counted int32
		for end := rg.clock.Now().Add(10 * time.Minute); rg.readyCount("md") == 0; startStep(rg, "md", startTick) {
			if !rg.clock.Now().Before(end) {
				t.Fatal("the Instance did not return to ready within 10 minutes")
			}
			if w, ok := worker0View(ag); ok && w.FailedActiveCheck && w.ConsecutiveFailedChecks > counted {
				counted = w.ConsecutiveFailedChecks
			}
		}
		if counted < 3 {
			t.Fatalf("the router counted at most %d failed checks of the reverifying worker: the test needs at least 3, the threshold", counted)
		}
		for i := 0; i < 6; i++ {
			startStep(rg, "md", startTick)
		}
		got := rg.instance(in.Name)
		if rs := got.Status.Restarts; len(rs) != 1 || rs[0].Signal != v1alpha1.ReasonPartExited {
			t.Errorf("restarts %+v, want only the one for the exit: %d failed checks counted while unverified restarted it again", rs, counted)
		}
		if f := healthFailing(rg, in.Name); !slices.Equal(f, []string{v1alpha1.ReasonWorkerRestart}) {
			t.Errorf("failing events %v, want one WorkerRestart", f)
		}
		if got.Status.Phase != v1alpha1.InstanceReady {
			t.Errorf("phase %s, want ready", got.Status.Phase)
		}
		t.Logf("the router counted %d failed checks during the reverification", counted)
		// The control: failures after the verification still count. Envoy now holds the verified
		// worker failing, one more failed check at each report, and the worker rung restarts it.
		var n int32
		ag.RouterView = func(rr *agentapi.RouterReport) {
			n++
			for i := range rr.Workers {
				rr.Workers[i].Healthy, rr.Workers[i].FailedActiveCheck, rr.Workers[i].ConsecutiveFailedChecks = false, true, n
			}
		}
		healthUntilEvent(rg, in.Name, 1, 5*time.Minute, startTick)
		if r, ok := lastRestart(rg, in.Name); !ok || r.Part != v1alpha1.WorkerName(0) || r.Signal != v1alpha1.ReasonRouterCheckFailing {
			t.Errorf("after %d failed checks of the verified worker, the restart is %+v, want worker-0's for RouterCheckFailing", n, r)
		}
	})
	t.Run("the first entry", func(t *testing.T) {
		// The agent reads /clusters before it probes the router, so one report can show the probe
		// passing beside the worker still failing, with the failures of its warm-up counted.
		rg := newRig(t)
		rg.r.Config.InstanceRouter = v1alpha1.InstanceRouterEnvoy
		rg.engine = func(string) afake.Engine { return afake.Engine{} }
		rg.create(mdSpec("md"))
		startUntil(rg, "md", 10*time.Minute, "the agent", func() bool { return len(rg.agents) == 1 })
		stale := true
		for _, ra := range rg.agents {
			ra.agent.RouterView = func(rr *agentapi.RouterReport) {
				if !stale {
					return
				}
				rr.Probe.OK, rr.Probe.ConsecutiveFailures = true, 0
				for i := range rr.Workers {
					rr.Workers[i].Healthy, rr.Workers[i].FailedActiveCheck, rr.Workers[i].ConsecutiveFailedChecks = false, true, 3
				}
			}
		}
		startUntil(rg, "md", 10*time.Minute, "verified and warmed up", func() bool {
			ins := rg.instances("md")
			return len(ins) == 1 && verifiedWorker(&ins[0])
		})
		for i := 0; i < 6; i++ {
			startStep(rg, "md", startTick)
		}
		in := rg.instances("md")[0]
		if n := rg.readyCount("md"); n != 0 {
			t.Fatalf("%d ready with the router holding the worker failing beside a passing probe, want 0", n)
		}
		stale = false
		startUntil(rg, "md", 2*time.Minute, "ready once the router holds the worker healthy", func() bool { return rg.readyCount("md") == 1 })
		for i := 0; i < 6; i++ {
			startStep(rg, "md", startTick)
		}
		if rs := rg.instance(in.Name).Status.Restarts; len(rs) != 0 {
			t.Errorf("restarts %+v at the first entry, want none", rs)
		}
	})
}

// A router that exits while a restarted worker reverifies is restarted there, at the router rung, with
// the Instance left verifying; the return to ready then waits for the new router to hold the worker
// healthy. The gate admits no router that is not running, so without the rung before ready the
// Instance would wait out health.startTimeout.
func TestARouterThatExitsDuringReverificationIsRestartedThere(t *testing.T) {
	rg, in, ag := routerRig(t)
	ag.CrashWorker(healthWorker, 137)
	healthUntilEvent(rg, in.Name, 0, time.Minute, startTick)
	ag.CrashRouter(1)
	healthUntilEvent(rg, in.Name, 1, 10*time.Minute, startTick)
	if f := healthFailing(rg, in.Name); !slices.Equal(f, []string{v1alpha1.ReasonWorkerRestart, v1alpha1.ReasonRouterRestart}) {
		t.Errorf("failing events %v, want WorkerRestart, then RouterRestart", f)
	}
	if got := rg.instance(in.Name); got.Status.Phase != v1alpha1.InstanceVerifying {
		t.Errorf("at the router's restart the phase is %s, want verifying: the rung restarts it where the Instance is", got.Status.Phase)
	}
	startUntil(rg, "md", 5*time.Minute, "ready, with the new router holding the worker healthy", func() bool {
		h := healthyWorkersOf(rg, in.Name)
		return rg.readyCount("md") == 1 && h != nil && *h == 1
	})
	if rs := rg.instance(in.Name).Status.Restarts; len(rs) != 2 {
		t.Errorf("restarts %+v, want the worker's and the router's", rs)
	}
}

// holdBeforeReady keeps an Instance behind a router out of its first ready: its router's probe is
// reported failing, with no failure counted, so the first-entry gate holds it and no router signal
// fires. hold false lets it through.
func holdBeforeReady(rg *rig, hold *bool) {
	for _, ra := range rg.agents {
		ra.agent.RouterView = func(rr *agentapi.RouterReport) {
			if *hold {
				rr.Probe.OK, rr.Probe.ConsecutiveFailures = false, 0
			}
		}
	}
}

// routedVerifying brings one Instance of "md", behind a router, to verifying with its router running,
// held before its first ready.
func routedVerifying(t *testing.T, hold *bool, opts ...rigOption) (*rig, string, *afake.Agent) {
	t.Helper()
	rg := newRig(t, opts...)
	rg.r.Config.InstanceRouter = v1alpha1.InstanceRouterEnvoy
	rg.engine = func(string) afake.Engine { return afake.Engine{} }
	rg.create(mdSpec("md"))
	startUntil(rg, "md", 10*time.Minute, "the agent", func() bool { return len(rg.agents) == 1 })
	holdBeforeReady(rg, hold)
	var ag *afake.Agent
	for _, ra := range rg.agents {
		ag = ra.agent
	}
	startUntil(rg, "md", 10*time.Minute, "verifying with the router running", func() bool {
		ins := rg.instances("md")
		if len(ins) != 1 || ins[0].Status.Phase != v1alpha1.InstanceVerifying {
			return false
		}
		rep, ok := ag.LastReport()
		return ok && rep.Instance != nil && rep.Instance.Router != nil && rep.Instance.Router.Phase == agentapi.PartRunning
	})
	return rg, rg.instances("md")[0].Name, ag
}

// A router that exits before the first ready is restarted at the router rung, in place: the Instance
// stays verifying, nothing is verified again, and it reaches ready once the router serves (the
// science agent's ruling on #455: HBEP-7 gives the router's exit the rung in any phase).
func TestTheRouterRungRunsBeforeTheFirstReady(t *testing.T) {
	hold := true
	rg, name, ag := routedVerifying(t, &hold)
	before := rg.instance(name)
	workerGen, verGen := restartGeneration(before, v1alpha1.WorkerName(0)), verificationGeneration(before)
	ag.CrashRouter(1)
	healthUntilEvent(rg, name, 0, 5*time.Minute, startTick)
	got := rg.instance(name)
	if r, ok := lastRestart(rg, name); !ok || r.Part != v1alpha1.PartRouter || r.Signal != v1alpha1.ReasonPartExited {
		t.Errorf("the restart is %+v, want the router's for PartExited", r)
	}
	if f := healthFailing(rg, name); !slices.Equal(f, []string{v1alpha1.ReasonRouterRestart}) {
		t.Errorf("failing events %v, want [RouterRestart]", f)
	}
	if got.Status.Phase != v1alpha1.InstanceVerifying || got.Status.ReadyTime != nil {
		t.Errorf("phase %s, ready time %v: the router rung keeps the Instance where it is", got.Status.Phase, got.Status.ReadyTime)
	}
	if restartGeneration(got, v1alpha1.WorkerName(0)) != workerGen || verificationGeneration(got) != verGen {
		t.Errorf("the router rung moved worker-0's generation or the verification's: %d/%d, were %d/%d",
			restartGeneration(got, v1alpha1.WorkerName(0)), verificationGeneration(got), workerGen, verGen)
	}
	hold = false
	startUntil(rg, "md", 10*time.Minute, "ready after the router's restart", func() bool { return rg.readyCount("md") == 1 })
}

// Router exits before the first ready count toward attemptsPerRung like any rung, and past it the
// ladder climbs to the whole-Instance rung.
func TestRouterExitsBeforeTheFirstReadyClimb(t *testing.T) {
	hold := true
	rg, name, ag := routedVerifying(t, &hold, withProfile(func(p *v1alpha1.PlatformProfileSpec) {
		p.Restart = &v1alpha1.RestartPolicy{AttemptsPerRung: ptrTo(int32(1)), Window: dur(4 * time.Hour)}
	}))
	ag.CrashRouter(1)
	healthUntilEvent(rg, name, 0, 5*time.Minute, startTick)
	startUntil(rg, "md", 5*time.Minute, "the router running again", func() bool {
		rep, ok := ag.LastReport()
		return ok && rep.Instance != nil && rep.Instance.Router != nil && rep.Instance.Router.Phase == agentapi.PartRunning &&
			rep.Instance.Router.Generation == 1
	})
	ag.CrashRouter(1)
	healthUntilEvent(rg, name, 1, 5*time.Minute, startTick)
	if r, ok := lastRestart(rg, name); !ok || r.Part != v1alpha1.PartInstance {
		t.Errorf("the second router exit's restart is %+v, want the whole-Instance rung", r)
	}
	if f := healthFailing(rg, name); !slices.Equal(f, []string{v1alpha1.ReasonRouterRestart, v1alpha1.ReasonInstanceRestart}) {
		t.Errorf("failing events %v, want RouterRestart, then InstanceRestart", f)
	}
}
