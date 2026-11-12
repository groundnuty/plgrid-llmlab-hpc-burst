package lifecycle

// An Instance of two workers behind the router starts (HBEP-7, Several workers; the science agent's
// rulings on #439): each worker is verified, qualified and warmed up on its own checks, with its own
// record ver-<uid>-<worker>-<generation> and its entry in status.workers, the warm-up at its share of the
// target sessions; the Instance enters ready only once every worker has passed, the router's probe
// passes and the router holds every worker healthy. A worker that exits before the first ready restarts
// at its own rung, alone. The rig is the lifecycle controller against the fake agent and its fake router.

import (
	"slices"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	afake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

// twoWorkersRig is the rig with the router flag and a deployment of two workers, run until its agent is
// up; f, when set, reshapes the fake router's view before the agent first reports.
func twoWorkersRig(t *testing.T, f func(*agentapi.RouterReport)) (*rig, *afake.Agent) {
	t.Helper()
	rg := newRig(t)
	rg.r.Config.InstanceRouter = v1alpha1.InstanceRouterEnvoy
	rg.engine = func(string) afake.Engine { return afake.Engine{} }
	rg.onAgent = func(ra *rigAgent) { ra.agent.RouterView = f }
	rg.create(withWorkers(mdSpec("md"), 2))
	startUntil(rg, "md", 10*time.Minute, "the agent", func() bool { return len(rg.agents) == 1 })
	var ag *afake.Agent
	for _, ra := range rg.agents {
		ag = ra.agent
	}
	return rg, ag
}

func TestTwoWorkersReachReadyEachVerifiedOnItsOwn(t *testing.T) {
	rg, ag := twoWorkersRig(t, nil)
	startUntil(rg, "md", 10*time.Minute, "the Instance ready", func() bool { return rg.readyCount("md") == 1 })
	in := rg.instances("md")[0]

	// Each worker's entry names its own record, of its first verification, which passed, and its warm-up.
	if len(in.Status.Workers) != 2 {
		t.Fatalf("status.workers %+v, want two entries", in.Status.Workers)
	}
	for i, e := range in.Status.Workers {
		w := v1alpha1.WorkerName(i)
		want := "ver-" + string(in.UID) + "-" + w + "-1"
		if e.Name != w || e.Record != want || e.Verdict != string(v1alpha1.VerdictPass) || !e.WarmedUp || e.VerifiedGeneration == nil || *e.VerifiedGeneration != 0 {
			t.Errorf("%s's entry %+v, want record %s passed, warmed up, verified at generation 0", w, e, want)
		}
		var rec appsv1.ControllerRevision
		if err := rg.c.Get(rg.ctx, client.ObjectKey{Namespace: rigNS, Name: want}, &rec); err != nil {
			t.Errorf("%s's record %s: %v", w, want, err)
		} else if rec.Labels[v1alpha1.LabelInstanceUID] != string(in.UID) || rec.Revision != 1 {
			t.Errorf("%s's record labels %v, revision %d", w, rec.Labels, rec.Revision)
		}
	}
	if h := in.Status.HealthyWorkers; h == nil || *h != 2 {
		t.Errorf("healthyWorkers %s once ready, want 2", countOf(h))
	}

	// The desired states asked each worker's checks on the worker, never on the Instance, each warm-up at
	// ceil(targetSessions / 2); and once ready, every worker carries its verified generation.
	md := rg.md("md")
	want := (resolveShapes(md).targetSessions + 1) / 2
	warmUps := 0
	for _, d := range ag.Seen() {
		if d.Instance == nil {
			continue
		}
		if d.Instance.Checks != nil {
			t.Fatalf("a desired state asked the Instance's own checks: %+v", d.Instance.Checks)
		}
		for _, dw := range d.Instance.Workers {
			if dw.Checks != nil && dw.Checks.WarmUp != nil {
				warmUps++
				if dw.Checks.WarmUp.TargetSessions != want {
					t.Errorf("%s's warm-up at %d sessions, want ceil(%d / 2) = %d", dw.Name, dw.Checks.WarmUp.TargetSessions, resolveShapes(md).targetSessions, want)
				}
			}
		}
	}
	if warmUps == 0 {
		t.Error("no desired state asked a worker's warm-up")
	}
	seen := ag.Seen()
	last := seen[len(seen)-1].Instance
	for _, dw := range last.Workers {
		if dw.VerifiedGeneration == nil || *dw.VerifiedGeneration != dw.Generation || dw.Checks != nil {
			t.Errorf("once ready, %s is desired with verifiedGeneration %v and checks %+v; want its generation and none", dw.Name, dw.VerifiedGeneration, dw.Checks)
		}
	}
}

// While the router holds worker-1 failing, the Instance waits, verifying, with both workers passed; it
// enters ready once the router holds both healthy. The router holding worker-0 alone is not enough.
func TestTheFirstReadyWaitsForTheRouterToHoldEveryWorker(t *testing.T) {
	hold := true
	rg, _ := twoWorkersRig(t, func(rr *agentapi.RouterReport) {
		if !hold {
			return
		}
		for i := range rr.Workers {
			if rr.Workers[i].Name == "worker-1" {
				rr.Workers[i].Healthy, rr.Workers[i].FailedActiveCheck = false, true
			}
		}
	})
	startUntil(rg, "md", 10*time.Minute, "both workers warmed up, the router holding worker-0", func() bool {
		ins := rg.instances("md")
		return len(ins) == 1 && workerWarmed(&ins[0], "worker-0") && workerWarmed(&ins[0], "worker-1")
	})
	for range 6 {
		startStep(rg, "md", startTick)
	}
	if in := rg.instances("md")[0]; in.Status.Phase == v1alpha1.InstanceReady {
		t.Fatal("ready while the router holds worker-1 failing")
	}
	hold = false
	startUntil(rg, "md", 5*time.Minute, "ready once the router holds both", func() bool { return rg.readyCount("md") == 1 })
}

// worker-1 exits before the first ready: its own rung restarts it, alone. worker-0's generation and
// entry stay as they are, worker-1 verifies again under ver-<uid>-worker-1-2, and the Instance reaches
// ready.
func TestAWorkerExitBeforeTheFirstReadyRestartsThatWorkerAlone(t *testing.T) {
	hold := true
	rg, ag := twoWorkersRig(t, func(rr *agentapi.RouterReport) {
		if hold && rr.Probe != nil {
			rr.Probe.OK, rr.Probe.ConsecutiveFailures = false, 0 // held, as holdBeforeReady holds it
		}
	})
	startUntil(rg, "md", 10*time.Minute, "both workers warmed up, held before ready", func() bool {
		ins := rg.instances("md")
		return len(ins) == 1 && workerWarmed(&ins[0], "worker-0") && workerWarmed(&ins[0], "worker-1")
	})
	name := rg.instances("md")[0].Name
	before := rg.instance(name)
	entry0 := workerEntry(before, "worker-0")
	ag.CrashWorker("worker-1", 1)
	startUntil(rg, "md", 5*time.Minute, "worker-1's restart", func() bool {
		_, ok := lastRestart(rg, name)
		return ok
	})
	got := rg.instance(name)
	if r, _ := lastRestart(rg, name); r.Part != "worker-1" || r.Signal != v1alpha1.ReasonPartExited {
		t.Fatalf("the restart is %+v, want worker-1's for PartExited", r)
	}
	if g := restartGeneration(got, "worker-0"); g != 0 {
		t.Errorf("worker-0's restart generation is %d, want 0: only worker-1 restarts", g)
	}
	if e := workerEntry(got, "worker-0"); e.Record != entry0.Record || !e.WarmedUp {
		t.Errorf("worker-0's entry after worker-1's restart: %+v, want %+v", e, entry0)
	}
	if workerWarmed(got, "worker-1") {
		t.Error("worker-1 still counts as warmed up after its restart")
	}
	hold = false
	startUntil(rg, "md", 10*time.Minute, "ready after worker-1's restart", func() bool { return rg.readyCount("md") == 1 })
	in := rg.instance(name)
	if e := workerEntry(in, "worker-1"); e.Record != "ver-"+string(in.UID)+"-worker-1-2" || e.VerifiedGeneration == nil || *e.VerifiedGeneration != 1 {
		t.Errorf("worker-1's entry once ready: %+v, want record ver-<uid>-worker-1-2, verified at generation 1", e)
	}
	if parts := restartParts(in); !slices.Equal(parts, []string{"worker-1"}) {
		t.Errorf("restarts %v, want worker-1's alone", parts)
	}
}

func restartParts(in *v1alpha1.Instance) []string {
	var out []string
	for _, r := range in.Status.Restarts {
		out = append(out, r.Part)
	}
	return out
}
