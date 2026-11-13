package lifecycle

// A ready Instance of two workers behind the router (HBEP-7, Several workers, Health and restarts with
// several workers; the science agent's rulings on #439): a worker killed while another serves restarts at
// its own rung alone, the Instance staying ready on the other; a rejoining worker's router checks count
// only from its router admission (Q2), which survives a controller restart; a controller restarted while
// every worker is verified keeps every worker taking traffic. Each test asserts on every step, not only at
// the end: a per-worker restart test whose other worker was never seen serving, or a controller-restart
// test that checks only the phase, proves nothing (HBEP-7, Test plan; #439).

import (
	"slices"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	afake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

// readyTwo brings an Instance of two workers to ready, with every worker admitted and healthyWorkers 2;
// view, when set, reshapes the fake router's reports from the start.
func readyTwo(t *testing.T, view func(*agentapi.RouterReport)) (*rig, *afake.Agent, string) {
	t.Helper()
	rg, ag := twoWorkersRig(t, func(rr *agentapi.RouterReport) {
		if view != nil {
			view(rr)
		}
	})
	startUntil(rg, "md", 10*time.Minute, "ready, every worker admitted, healthyWorkers 2", func() bool {
		if rg.readyCount("md") != 1 {
			return false
		}
		in := rg.instances("md")[0]
		h := in.Status.HealthyWorkers
		return routerAdmitted(&in, "worker-0") && routerAdmitted(&in, "worker-1") && h != nil && *h == 2
	})
	return rg, ag, rg.instances("md")[0].Name
}

// lastDesiredWorker is worker w in the last desired state the agent saw.
func lastDesiredWorker(ag *afake.Agent, w string) (agentapi.DesiredWorker, bool) {
	seen := ag.Seen()
	if len(seen) == 0 || seen[len(seen)-1].Instance == nil {
		return agentapi.DesiredWorker{}, false
	}
	for _, dw := range seen[len(seen)-1].Instance.Workers {
		if dw.Name == w {
			return dw, true
		}
	}
	return agentapi.DesiredWorker{}, false
}

// routerWorker is worker w in the agent's last report's router part.
func routerWorker(ag *afake.Agent, w string) (agentapi.RouterWorker, bool) {
	rep, ok := ag.LastReport()
	if !ok || rep.Instance == nil || rep.Instance.Router == nil {
		return agentapi.RouterWorker{}, false
	}
	for _, rw := range rep.Instance.Router.Workers {
		if rw.Name == w {
			return rw, true
		}
	}
	return agentapi.RouterWorker{}, false
}

// worker-1 is killed while ready. On every step until it is back: the Instance is ready, worker-0 runs at
// generation 0 and the router holds it healthy, worker-1's verifiedGeneration in the desired state is
// absent until it is warmed up again, and the only restart is worker-1's. healthyWorkers goes 2, 1, 2.
func TestAWorkerKilledWhileReadyRejoinsAlone(t *testing.T) {
	rg, ag, name := readyTwo(t, nil)
	ag.CrashWorker("worker-1", 1)
	var counts []int32
	back := false
	for end := rg.clock.Now().Add(15 * time.Minute); !back; {
		if !rg.clock.Now().Before(end) {
			t.Fatalf("worker-1 not back within 15 minutes; healthyWorkers went %v", counts)
		}
		startStep(rg, "md", startTick)
		in := rg.instance(name)
		if in.Status.Phase != v1alpha1.InstanceReady {
			t.Fatalf("%s: phase %s, want ready on worker-0 throughout", rg.clock.Now().Format(time.TimeOnly), in.Status.Phase)
		}
		if parts := restartParts(in); len(parts) > 0 && !slices.Equal(parts, []string{"worker-1"}) {
			t.Fatalf("restarts %v, want worker-1's alone", parts)
		}
		if !runningAt(ag, "worker-0", 0) {
			t.Fatalf("%s: worker-0 is not running at generation 0", rg.clock.Now().Format(time.TimeOnly))
		}
		if rw, ok := routerWorker(ag, "worker-0"); !ok || !rw.Healthy {
			t.Fatalf("%s: the router does not hold worker-0 healthy: %+v", rg.clock.Now().Format(time.TimeOnly), rw)
		}
		if dw, ok := lastDesiredWorker(ag, "worker-1"); ok && dw.Generation == 1 && dw.VerifiedGeneration != nil && !workerWarmed(in, "worker-1") {
			t.Fatalf("worker-1 is desired verified at generation %d before it warmed up again", *dw.VerifiedGeneration)
		}
		if !workerWarmed(in, "worker-1") && isTrue(in.Status.Conditions, v1alpha1.ConditionHealthy) {
			t.Fatalf("%s: Healthy is True while worker-1 rejoins", rg.clock.Now().Format(time.TimeOnly))
		}
		if h := in.Status.HealthyWorkers; h != nil && (len(counts) == 0 || counts[len(counts)-1] != *h) {
			counts = append(counts, *h)
		}
		back = restartGeneration(in, "worker-1") == 1 && workerWarmed(in, "worker-1") && routerAdmitted(in, "worker-1") &&
			in.Status.HealthyWorkers != nil && *in.Status.HealthyWorkers == 2
	}
	if !slices.Equal(counts, []int32{1, 2}) && !slices.Equal(counts, []int32{2, 1, 2}) {
		t.Errorf("healthyWorkers went %v, want 2, 1, 2", counts)
	}
	in := rg.instance(name)
	if r, _ := lastRestart(rg, name); r.Part != "worker-1" || r.Signal != v1alpha1.ReasonPartExited {
		t.Errorf("the restart is %+v, want worker-1's for PartExited", r)
	}
	if dw, _ := lastDesiredWorker(ag, "worker-1"); dw.VerifiedGeneration == nil || *dw.VerifiedGeneration != 1 {
		t.Errorf("worker-1 back: desired verifiedGeneration %v, want 1", dw.VerifiedGeneration)
	}
	if !isTrue(in.Status.Conditions, v1alpha1.ConditionHealthy) {
		t.Errorf("Healthy is %s once every worker is back, want True", conditionReason(in, v1alpha1.ConditionHealthy))
	}
}

// The Q2 gate, reproduced: the fake router keeps counting worker-1's failed checks through its
// reverification and then holds it failing for RouterLag more reports after it is verified, as Envoy's
// next check does. Those failures, at or above health.failureThreshold, never restart worker-1: they are
// counted before its admission. Once admitted, a failing check does restart it, at its own rung, with the
// Instance ready (the positive control).
func TestTheRoutersChecksCountOnlyFromAWorkersAdmission(t *testing.T) {
	failing := false
	var n int32
	rg, ag, name := readyTwo(t, func(rr *agentapi.RouterReport) {
		if !failing {
			return
		}
		for i := range rr.Workers {
			if rr.Workers[i].Name == "worker-1" {
				n++
				rr.Workers[i].Healthy, rr.Workers[i].FailedActiveCheck, rr.Workers[i].ConsecutiveFailedChecks = false, true, n
			}
		}
	})
	ag.RouterLag = 3
	ag.CrashWorker("worker-1", 1)
	var counted int32 // the most failures the router reported for worker-1 once it was warmed up, before its admission
	startUntil(rg, "md", 15*time.Minute, "worker-1 back and admitted", func() bool {
		in := rg.instance(name)
		if r, _ := lastRestart(rg, name); r.Signal == v1alpha1.ReasonRouterCheckFailing {
			t.Fatalf("worker-1 restarted for RouterCheckFailing on failures counted before its admission: %+v", r)
		}
		if rw, ok := routerWorker(ag, "worker-1"); ok && workerWarmed(in, "worker-1") && !routerAdmitted(in, "worker-1") && rw.ConsecutiveFailedChecks > counted {
			counted = rw.ConsecutiveFailedChecks
		}
		// At generation 1: right after the crash the status still shows worker-1 warmed and admitted at 0.
		return restartGeneration(in, "worker-1") == 1 && workerWarmed(in, "worker-1") && routerAdmitted(in, "worker-1")
	})
	if counted < defaultFailureThreshold {
		t.Fatalf("the router counted %d failures of worker-1 between its warm-up and its admission, under the threshold %d: the gate was not put to the test", counted, defaultFailureThreshold)
	}
	if parts := restartParts(rg.instance(name)); !slices.Equal(parts, []string{"worker-1"}) {
		t.Fatalf("restarts %v, want worker-1's exit alone", parts)
	}
	t.Logf("the router counted %d failures of worker-1 before its admission", counted)

	// The positive control: admitted, worker-1 failing its checks restarts at its own rung.
	failing = true
	startUntil(rg, "md", 5*time.Minute, "worker-1's restart for RouterCheckFailing", func() bool {
		r, _ := lastRestart(rg, name)
		return r.Signal == v1alpha1.ReasonRouterCheckFailing
	})
	in := rg.instance(name)
	if r, _ := lastRestart(rg, name); r.Part != "worker-1" || in.Status.Phase != v1alpha1.InstanceReady {
		t.Errorf("the restart is %+v with the Instance %s, want worker-1's, ready", r, in.Status.Phase)
	}
}

// A worker's router admission is kept on the Instance: after a controller restart, worker-1's failing
// checks restart it promptly, for RouterCheckFailing, not an hour later for its start timeout.
func TestARoutersAdmissionSurvivesAControllerRestart(t *testing.T) {
	failing := false
	var n int32
	rg, _, name := readyTwo(t, func(rr *agentapi.RouterReport) {
		if !failing {
			return
		}
		for i := range rr.Workers {
			if rr.Workers[i].Name == "worker-1" {
				n++
				rr.Workers[i].Healthy, rr.Workers[i].FailedActiveCheck, rr.Workers[i].ConsecutiveFailedChecks = false, true, n
			}
		}
	})
	if v := rg.instance(name).Annotations[v1alpha1.RouterAdmittedAnnotation("worker-1")]; v != "0" {
		t.Fatalf("worker-1's admission annotation is %q, want 0", v)
	}
	restartRouted(rg)
	failing = true
	startUntil(rg, "md", 5*time.Minute, "a restart of worker-1", func() bool { return len(rg.instance(name).Status.Restarts) > 0 })
	if r, _ := lastRestart(rg, name); r.Part != "worker-1" || r.Signal != v1alpha1.ReasonRouterCheckFailing {
		t.Errorf("the restart is %+v, want worker-1's for RouterCheckFailing", r)
	}
}

// A controller restarted while the Instance is ready with every worker verified keeps every worker
// taking traffic, from status.workers: the new controller's desired states carry each worker's
// verifiedGeneration and no checks, healthyWorkers stays 2 and nothing restarts. The phase alone would
// prove nothing.
func TestAControllerRestartKeepsEveryWorkerTakingTraffic(t *testing.T) {
	rg, ag, name := readyTwo(t, nil)
	restartRouted(rg)
	for i := range 12 {
		// What the new controller serves the agent, read from its own Backend before each step: every
		// worker with its verifiedGeneration and no checks. (The fake agent records a desired state only
		// when its version changes, and the new controller's is the old one's.)
		di, err := rg.backend.desiredInstance(rg.ctx, rg.instance(name))
		if err != nil {
			t.Fatal(err)
		}
		if len(di.Workers) != 2 {
			t.Fatalf("the new controller desires %d workers, want 2", len(di.Workers))
		}
		for _, dw := range di.Workers {
			if dw.VerifiedGeneration == nil || *dw.VerifiedGeneration != dw.Generation || dw.Checks != nil {
				t.Fatalf("pass %d: the new controller desires %s with verifiedGeneration %v and checks %+v; want its generation and none", i, dw.Name, dw.VerifiedGeneration, dw.Checks)
			}
		}
		startStep(rg, "md", startTick)
		in := rg.instance(name)
		if h := in.Status.HealthyWorkers; in.Status.Phase != v1alpha1.InstanceReady || h == nil || *h != 2 || len(in.Status.Restarts) != 0 {
			t.Fatalf("after the controller's restart: phase %s, healthyWorkers %s, restarts %v; want ready, 2, none", in.Status.Phase, countOf(h), restartParts(in))
		}
		// Every worker keeps taking traffic: the router holds both healthy on every report.
		for _, w := range []string{"worker-0", "worker-1"} {
			if rw, ok := routerWorker(ag, w); !ok || !rw.Healthy {
				t.Fatalf("pass %d: the router does not hold %s healthy after the controller's restart: %+v", i, w, rw)
			}
		}
	}
}

// worker-1 dies while the router also holds it failing at and above the threshold: one death is one
// restart, worker-1's, and nothing more within the next minutes while it rejoins.
func TestOneDeathIsOneRestart(t *testing.T) {
	failing := false
	var n int32
	rg, ag, name := readyTwo(t, func(rr *agentapi.RouterReport) {
		if !failing {
			return
		}
		for i := range rr.Workers {
			if rr.Workers[i].Name == "worker-1" {
				n += defaultFailureThreshold
				rr.Workers[i].Healthy, rr.Workers[i].FailedActiveCheck, rr.Workers[i].ConsecutiveFailedChecks = false, true, n
			}
		}
	})
	failing = true
	ag.CrashWorker("worker-1", 1)
	for range 30 {
		startStep(rg, "md", startTick)
	}
	if parts := restartParts(rg.instance(name)); !slices.Equal(parts, []string{"worker-1"}) {
		t.Errorf("restarts %v, want one, worker-1's", parts)
	}
}

// A rejoining worker that the router never holds healthy again is judged by health.startTimeout from its
// restart: it restarts again, for StartTimeout, with the Instance ready on worker-0.
func TestARejoiningWorkerTheRouterNeverAdmitsRestartsAtItsTimeout(t *testing.T) {
	failing := false
	rg, ag, name := readyTwo(t, func(rr *agentapi.RouterReport) {
		if !failing {
			return
		}
		for i := range rr.Workers {
			if rr.Workers[i].Name == "worker-1" {
				rr.Workers[i].Healthy, rr.Workers[i].FailedActiveCheck = false, true
			}
		}
	})
	failing = true
	ag.CrashWorker("worker-1", 1)
	startUntil(rg, "md", 2*time.Hour, "worker-1's restart at its start timeout", func() bool {
		in := rg.instance(name)
		if in.Status.Phase != v1alpha1.InstanceReady {
			t.Fatalf("phase %s while worker-1 rejoins, want ready", in.Status.Phase)
		}
		r, _ := lastRestart(rg, name)
		return r.Signal == v1alpha1.ReasonStartTimeout
	})
	if r, _ := lastRestart(rg, name); r.Part != "worker-1" {
		t.Errorf("the timeout's restart is %+v, want worker-1's", r)
	}
}

// restartRouted is a controller killed and started again with its flags: --instance-router envoy, which
// the rig's restartController does not carry over.
func restartRouted(rg *rig) {
	rg.restartController()
	rg.r.Config.InstanceRouter = v1alpha1.InstanceRouterEnvoy
}

// Every worker is admitted at the first ready, where the router held each healthy: a worker the router
// starts failing on the very next report is judged by its checks, promptly, not by its start timeout.
func TestAWorkerFailingRightAfterTheFirstReadyIsJudgedByItsChecks(t *testing.T) {
	failing := false
	var n int32
	rg, _ := twoWorkersRig(t, func(rr *agentapi.RouterReport) {
		if !failing {
			return
		}
		for i := range rr.Workers {
			if rr.Workers[i].Name == "worker-1" {
				n++
				rr.Workers[i].Healthy, rr.Workers[i].FailedActiveCheck, rr.Workers[i].ConsecutiveFailedChecks = false, true, n
			}
		}
	})
	startUntil(rg, "md", 10*time.Minute, "the first ready", func() bool { return rg.readyCount("md") == 1 })
	failing = true
	name := rg.instances("md")[0].Name
	startUntil(rg, "md", 5*time.Minute, "a restart", func() bool { return len(rg.instance(name).Status.Restarts) > 0 })
	if r, _ := lastRestart(rg, name); r.Part != "worker-1" || r.Signal != v1alpha1.ReasonRouterCheckFailing {
		t.Errorf("the restart is %+v, want worker-1's for RouterCheckFailing", r)
	}
}
