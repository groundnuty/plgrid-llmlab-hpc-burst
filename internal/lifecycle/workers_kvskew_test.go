package lifecycle

// The KV-skew signal end to end (HBEP-7, Health and restarts, by #520; #518's Test plan): the lifecycle
// controller against the fake agent, its fake router, and each worker's gauges scripted by the test
// (afake.HealthGauges). A stuck worker, full with requests waiting and its token counter flat, beside a
// routable idle sibling, restarts at its own rung once kvSkewTimeout has passed, and only then, with the
// Instance ready throughout. A skew that clears first, a loaded worker still generating, both workers
// full, an unroutable idle sibling and one worker never raise it. Each run is checked on every step.

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	afake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

var (
	idleGauges    = afake.Gauges{KVCache: 0.05}
	stuckGauges   = afake.Gauges{Running: 8, Waiting: 3, KVCache: 0.97, Flat: true}
	loadedGauges  = afake.Gauges{Running: 8, Waiting: 3, KVCache: 0.97}
	drainedGauges = afake.Gauges{Running: 8, Waiting: 0, KVCache: 0.97, Flat: true}
)

// skewRig brings an Instance of n workers behind the router to ready, every worker admitted, each with
// the gauges g names for it (idle until a test changes them), and returns the rig and the Instance.
func skewRig(t *testing.T, n int32, g map[string]afake.Gauges, view func(*agentapi.RouterReport)) (*rig, string) {
	t.Helper()
	e := afake.Engine{Health: afake.HealthGauges(func(w string) afake.Gauges { return g[w] })}
	rg := newRig(t)
	rg.r.Config.InstanceRouter = v1alpha1.InstanceRouterEnvoy
	rg.engine = func(string) afake.Engine { return e }
	if view != nil {
		rg.onAgent = func(ra *rigAgent) { ra.agent.RouterView = view }
	}
	rg.create(withWorkers(mdSpec("md"), n))
	startUntil(rg, "md", 10*time.Minute, "ready, every worker admitted", func() bool {
		if rg.readyCount("md") != 1 {
			return false
		}
		in := rg.instances("md")[0]
		for _, w := range workerNames(&in) {
			if !routerAdmitted(&in, w) {
				return false
			}
		}
		return true
	})
	return rg, rg.instances("md")[0].Name
}

// stepFor steps the rig for d, checking on every step that the Instance is ready and has restarted only
// as allowed, and returns the restarts seen since restartsBefore.
func stepFor(t *testing.T, rg *rig, name string, d time.Duration, restartsBefore int) []v1alpha1.InstanceRestart {
	t.Helper()
	for end := rg.clock.Now().Add(d); rg.clock.Now().Before(end); {
		startStep(rg, "md", startTick)
		if in := rg.instance(name); in.Status.Phase != v1alpha1.InstanceReady {
			t.Fatalf("%s: phase %s, want ready throughout", rg.clock.Now().Format(time.TimeOnly), in.Status.Phase)
		}
	}
	return rg.instance(name).Status.Restarts[restartsBefore:]
}

func TestAStuckWorkerBesideAnIdleSiblingRestartsAtKVSkewTimeout(t *testing.T) {
	g := map[string]afake.Gauges{"worker-0": idleGauges, "worker-1": idleGauges}
	rg, name := skewRig(t, 2, g, nil)
	g["worker-1"] = stuckGauges
	stuckAt := rg.clock.Now()
	var restarted *v1alpha1.InstanceRestart
	for end := stuckAt.Add(5 * time.Minute); rg.clock.Now().Before(end) && restarted == nil; {
		startStep(rg, "md", startTick)
		in := rg.instance(name)
		if in.Status.Phase != v1alpha1.InstanceReady {
			t.Fatalf("%s: phase %s, want ready throughout", rg.clock.Now().Format(time.TimeOnly), in.Status.Phase)
		}
		if rs := in.Status.Restarts; len(rs) > 0 {
			restarted = &rs[0]
		}
	}
	if restarted == nil {
		t.Fatal("no restart within 5 min of worker-1 getting stuck beside an idle sibling")
	}
	g["worker-1"] = idleGauges // the new process drains
	if restarted.Part != "worker-1" || restarted.Signal != v1alpha1.ReasonKVSkew {
		t.Fatalf("the restart is %+v, want worker-1's for KVSkew", *restarted)
	}
	// Due kvSkewTimeout (2m) after the first report that showed it, which is at most one tick after the
	// change; the pass it's due in is at most one tick later.
	if at := restarted.Time.Sub(stuckAt); at < defaultKVSkewTimeout || at > defaultKVSkewTimeout+3*startTick {
		t.Errorf("KVSkew came %s after the skew began, want kvSkewTimeout (%s), within a tick or two", at, defaultKVSkewTimeout)
	}
	in := rg.instance(name)
	if restartGeneration(in, "worker-0") != 0 {
		t.Error("worker-0, the idle sibling, was restarted")
	}
	if !strings.Contains(conditionMessage(in, v1alpha1.ConditionHealthy), "token counter flat") && conditionReason(in, v1alpha1.ConditionHealthy) != v1alpha1.ReasonKVSkew {
		t.Errorf("Healthy: %s %q, want the KVSkew signal", conditionReason(in, v1alpha1.ConditionHealthy), conditionMessage(in, v1alpha1.ConditionHealthy))
	}
	if more := stepFor(t, rg, name, 5*time.Minute, 1); len(more) != 0 {
		t.Errorf("after the drained worker-1 rejoined: restarts %+v, want none", more)
	}
}

// A loaded worker, full with requests waiting but still generating tokens, is load, not a fault: it never
// raises KVSkew, however long (#520: the case #518's measurement exposed).
func TestALoadedWorkerStillGeneratingNeverRaisesKVSkew(t *testing.T) {
	g := map[string]afake.Gauges{"worker-0": idleGauges, "worker-1": idleGauges}
	rg, name := skewRig(t, 2, g, nil)
	g["worker-1"] = loadedGauges
	if rs := stepFor(t, rg, name, 3*defaultKVSkewTimeout, 0); len(rs) != 0 {
		t.Errorf("restarts %+v of a loaded worker still generating, want none", rs)
	}
}

// A skew that clears one tick before kvSkewTimeout restarts nothing, and a new skew needs a whole new
// kvSkewTimeout: the clock started again.
func TestASkewThatClearsBeforeKVSkewTimeoutRestartsNothing(t *testing.T) {
	g := map[string]afake.Gauges{"worker-0": idleGauges, "worker-1": idleGauges}
	rg, name := skewRig(t, 2, g, nil)
	g["worker-1"] = stuckGauges
	if rs := stepFor(t, rg, name, defaultKVSkewTimeout-2*startTick, 0); len(rs) != 0 {
		t.Fatalf("restarts %+v before kvSkewTimeout, want none", rs)
	}
	g["worker-1"] = drainedGauges // no request waiting: one report clears it
	stepFor(t, rg, name, startTick, 0)
	g["worker-1"] = stuckGauges
	againAt := rg.clock.Now()
	if rs := stepFor(t, rg, name, defaultKVSkewTimeout-2*startTick, 0); len(rs) != 0 {
		t.Fatalf("restarts %+v within kvSkewTimeout of the skew's return, want none: the clock must start again", rs)
	}
	var rs []v1alpha1.InstanceRestart
	for end := againAt.Add(defaultKVSkewTimeout + 3*startTick); rg.clock.Now().Before(end) && len(rs) == 0; {
		rs = stepFor(t, rg, name, startTick, 0)
	}
	if len(rs) != 1 || rs[0].Part != "worker-1" || rs[0].Signal != v1alpha1.ReasonKVSkew {
		t.Errorf("restarts %+v, want worker-1's KVSkew kvSkewTimeout after the skew's return", rs)
	}
}

// Both workers stuck and full: no sibling idles, so neither raises it.
func TestBothWorkersFullRaiseNoKVSkew(t *testing.T) {
	g := map[string]afake.Gauges{"worker-0": idleGauges, "worker-1": idleGauges}
	rg, name := skewRig(t, 2, g, nil)
	g["worker-0"], g["worker-1"] = stuckGauges, stuckGauges
	if rs := stepFor(t, rg, name, 3*defaultKVSkewTimeout, 0); len(rs) != 0 {
		t.Errorf("restarts %+v with both workers full, want none", rs)
	}
}

// The idle sibling ejected by the router's outlier detection is not one Envoy routes to: it idles because
// Envoy sends it nothing, so the full worker raises nothing.
func TestAnUnroutableIdleSiblingRaisesNoKVSkew(t *testing.T) {
	g := map[string]afake.Gauges{"worker-0": idleGauges, "worker-1": idleGauges}
	eject := false
	rg, name := skewRig(t, 2, g, func(rr *agentapi.RouterReport) {
		for i := range rr.Workers {
			if eject && rr.Workers[i].Name == "worker-0" {
				rr.Workers[i].Healthy, rr.Workers[i].Ejected = false, true
			}
		}
	})
	eject = true
	g["worker-1"] = stuckGauges
	if rs := stepFor(t, rg, name, 3*defaultKVSkewTimeout, 0); len(rs) != 0 {
		t.Errorf("restarts %+v with the idle sibling ejected, want none", rs)
	}
}

// One worker (alpha), full with requests waiting and its counter flat: plain load on the only worker,
// never KVSkew (HBEP-7). Over 3 × kvSkewTimeout, well within noProgressTimeout's 30m.
func TestOneWorkerFullRaisesNoKVSkew(t *testing.T) {
	g := map[string]afake.Gauges{"worker-0": idleGauges}
	e := afake.Engine{Health: afake.HealthGauges(func(w string) afake.Gauges { return g[w] })}
	rg := newRig(t)
	rg.engine = func(string) afake.Engine { return e }
	rg.create(mdSpec("md"))
	startUntil(rg, "md", 10*time.Minute, "ready", func() bool { return rg.readyCount("md") == 1 })
	name := rg.instances("md")[0].Name
	g["worker-0"] = stuckGauges
	if rs := stepFor(t, rg, name, 3*defaultKVSkewTimeout, 0); len(rs) != 0 {
		t.Errorf("restarts %+v of the only worker, want none", rs)
	}
}

// Three workers, two stuck beside one idle: each has its own clock, and one signal per pass in the
// workers' order. worker-0 restarts first, and worker-1, whose clock went on, restarts next, at most
// restart.backoff later.
func TestTwoStuckWorkersBesideAnIdleOneEachRestart(t *testing.T) {
	g := map[string]afake.Gauges{"worker-0": idleGauges, "worker-1": idleGauges, "worker-2": idleGauges}
	rg, name := skewRig(t, 3, g, nil)
	g["worker-0"], g["worker-1"] = stuckGauges, stuckGauges
	var parts []string
	for end := rg.clock.Now().Add(5 * time.Minute); rg.clock.Now().Before(end) && len(parts) < 2; {
		for _, r := range stepFor(t, rg, name, startTick, len(parts)) {
			parts = append(parts, r.Part+"/"+r.Signal)
			g[r.Part] = idleGauges
		}
	}
	if want := []string{"worker-0/" + v1alpha1.ReasonKVSkew, "worker-1/" + v1alpha1.ReasonKVSkew}; !slices.Equal(parts, want) {
		t.Errorf("restarts %v, want %v", parts, want)
	}
}

// A skew seen for two reports, then no report for 110 s (below agentTimeout's 2m, so the agent is not
// unreachable), then reports resuming skewed: the last report went stale in the silence, which starts the
// clock again, so the signal waits a whole kvSkewTimeout from the reports' return and never fires on
// snapshots 110 s apart (#525's review).
func TestASkewAcrossASilenceWaitsAWholeKVSkewTimeoutFromTheReturn(t *testing.T) {
	g := map[string]afake.Gauges{"worker-0": idleGauges, "worker-1": idleGauges}
	rg, name := skewRig(t, 2, g, nil)
	var ag *afake.Agent
	for _, ra := range rg.agents {
		ag = ra.agent
	}
	g["worker-1"] = stuckGauges
	if rs := stepFor(t, rg, name, 2*startTick, 0); len(rs) != 0 {
		t.Fatalf("restarts %+v in the skew's first two ticks", rs)
	}
	ag.Silence(true)
	if rs := stepFor(t, rg, name, 110*time.Second, 0); len(rs) != 0 {
		t.Fatalf("restarts %+v while the agent was silent: the skew was seen for two reports", rs)
	}
	ag.Silence(false)
	resumeAt := rg.clock.Now()
	if rs := stepFor(t, rg, name, defaultKVSkewTimeout-2*startTick, 0); len(rs) != 0 {
		t.Fatalf("restarts %+v within kvSkewTimeout of the reports' return (%s after it): the stale report must start the clock again", rs, rg.clock.Now().Sub(resumeAt))
	}
	var rs []v1alpha1.InstanceRestart
	for end := resumeAt.Add(defaultKVSkewTimeout + 3*startTick); rg.clock.Now().Before(end) && len(rs) == 0; {
		rs = stepFor(t, rg, name, startTick, 0)
	}
	if len(rs) != 1 || rs[0].Part != "worker-1" || rs[0].Signal != v1alpha1.ReasonKVSkew {
		t.Errorf("restarts %+v, want worker-1's KVSkew kvSkewTimeout after the reports' return", rs)
	}
}
