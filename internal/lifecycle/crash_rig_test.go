package lifecycle

// CrashCorrelated on the lifecycle rig: two ready Instances of one ModelDeployment, each its own
// Allocation and fake agent, whose workers the agents crash at times the test sets on the rig's clock.
// The pair raises the event; one Instance's crash loop, exits 31 s apart, two requested restarts at
// once, and silent agents raise none.

import (
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	afake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

// crashRig brings two Instances of "md" to ready and returns the rig and their agents, in the order
// of the Instances' names.
func crashRig(t *testing.T, engine func() afake.Engine) (*rig, []v1alpha1.Instance, []*afake.Agent) {
	t.Helper()
	rg := newRig(t)
	if engine != nil {
		rg.engine = func(string) afake.Engine { return engine() }
	}
	md := mdSpec("md")
	md.Spec.Instances = 2
	rg.create(md)
	startUntil(rg, "md", 15*time.Minute, "two ready Instances", func() bool { return rg.readyCount("md") == 2 })
	ins := rg.instances("md")
	var ags []*afake.Agent
	for _, in := range ins {
		ags = append(ags, rg.agentOf(in.Spec.AllocationRef.Name).agent)
	}
	return rg, ins, ags
}

func crashEvents(rg *rig) []RecordedEvent {
	return rg.rec.Find(eventFailing, v1alpha1.ReasonCrashCorrelated)
}

func TestTwoInstancesCrashing10sApartRaiseCrashCorrelated(t *testing.T) {
	rg, ins, ags := crashRig(t, nil)
	w := v1alpha1.WorkerName(0)
	ags[0].CrashWorker(w, 1)
	startStep(rg, "md", 10*time.Second)
	ags[1].CrashWorker(w, 1)
	for k := 0; k < 3; k++ {
		startStep(rg, "md", 10*time.Second)
	}
	evs := crashEvents(rg)
	if len(evs) != 1 || evs[0].Name != ins[1].Name || evs[0].RelatedName != ins[0].Name || !evs[0].Warning {
		t.Fatalf("CrashCorrelated events %+v, want one Warning regarding %s, related to %s", evs, ins[1].Name, ins[0].Name)
	}
	if n := len(healthFailing(rg, ins[0].Name)) + len(healthFailing(rg, ins[1].Name)); n < 2 {
		t.Errorf("%d failing events on the two Instances, want each one's own restart too: the event changes no ladder", n)
	}
}

func TestExitsThatAreNoPairRaiseNoCrashCorrelated(t *testing.T) {
	w := v1alpha1.WorkerName(0)
	t.Run("31 s apart, seen 7 s apart", func(t *testing.T) {
		// The first exit is seen 25 s after it, the second 1 s after it: by first sight 7 s apart, by the
		// agents' exit times 31 s. The window is judged on the exit times (Q1).
		rg, _, ags := crashRig(t, nil)
		ags[0].CrashWorker(w, 1)
		startStep(rg, "md", 25*time.Second)
		startStep(rg, "md", 6*time.Second)
		ags[1].CrashWorker(w, 1)
		startStep(rg, "md", time.Second)
		for k := 0; k < 3; k++ {
			startStep(rg, "md", 10*time.Second)
		}
		if evs := crashEvents(rg); len(evs) != 0 {
			t.Errorf("events %+v, want none: the exits were 31 s apart", evs)
		}
	})
	// The second crash comes while the Instance verifies again, so it is no serving worker's and is not
	// recorded; the same-Instance rule itself is TestRecordExitPairsExitsOfTwoInstancesWithin30s's.
	t.Run("one Instance's crash loop", func(t *testing.T) {
		rg, ins, ags := crashRig(t, nil)
		for k := 0; k < 2; k++ {
			starts := len(ags[0].Starts())
			ags[0].CrashWorker(w, 1)
			startUntil(rg, "md", 2*time.Minute, "the crashed worker started again", func() bool { return len(ags[0].Starts()) > starts })
		}
		if evs := crashEvents(rg); len(evs) != 0 {
			t.Errorf("events %+v, want none: only %s's worker crashed", evs, ins[0].Name)
		}
	})
	t.Run("two requested restarts at once", func(t *testing.T) {
		// Both engines stall with requests running: the controller restarts both workers at
		// noProgressTimeout, in one pass. Its stops are reported stopped, never exited.
		rg, ins, _ := crashRig(t, func() afake.Engine { return afake.Engine{Health: afake.HealthStalled} })
		startUntil(rg, "md", 45*time.Minute, "both workers restarted for no progress", func() bool {
			return len(healthFailing(rg, ins[0].Name)) > 0 && len(healthFailing(rg, ins[1].Name)) > 0
		})
		for k := 0; k < 6; k++ {
			startStep(rg, "md", 10*time.Second)
		}
		if evs := crashEvents(rg); len(evs) != 0 {
			t.Errorf("events %+v, want none: both stops were the controller's", evs)
		}
	})
	t.Run("silent agents, no exit", func(t *testing.T) {
		rg, _, ags := crashRig(t, nil)
		for _, ag := range ags {
			ag.Silence(true)
		}
		for k := 0; k < 12; k++ {
			startStep(rg, "md", 10*time.Second)
		}
		if evs := crashEvents(rg); len(evs) != 0 {
			t.Errorf("events %+v, want none: no worker exited", evs)
		}
	})
}

// restartEvents counts the failing WorkerRestart and InstanceRestart events by Instance.
func restartEvents(rg *rig) map[string]int {
	n := map[string]int{}
	for _, code := range []string{v1alpha1.ReasonWorkerRestart, v1alpha1.ReasonInstanceRestart} {
		for _, e := range rg.rec.Find(eventFailing, code) {
			n[e.Name]++
		}
	}
	return n
}

// Two engines failing to START on a bad spec raise nothing, at any attempt (#550): their exits are failed
// starts, never a serving worker's, though each pair of attempts comes within seconds. The exits are seen:
// the controller restarts both Instances, generation after generation.
func TestEnginesFailingToStartRaiseNoCrashCorrelatedAtAnyAttempt(t *testing.T) {
	rg := newRig(t)
	rg.engine = func(string) afake.Engine { return afake.Engine{StartFails: true} }
	md := mdSpec("md")
	md.Spec.Instances = 2
	rg.create(md)
	startUntil(rg, "md", 20*time.Minute, "both Instances restarted at least twice", func() bool {
		n := restartEvents(rg)
		twice := 0
		for _, k := range n {
			if k >= 2 {
				twice++
			}
		}
		return twice >= 2
	})
	if evs := crashEvents(rg); len(evs) != 0 {
		t.Fatalf("CrashCorrelated %+v, want none: every exit was a failed start (restarts by Instance %v)", evs, restartEvents(rg))
	}
	if rg.readyCount("md") != 0 {
		t.Fatalf("%d ready Instances: an engine that never starts can't serve, so the test proves nothing", rg.readyCount("md"))
	}
	t.Logf("restarts by Instance: %v; no CrashCorrelated", restartEvents(rg))
}

// A deployment deleted and recreated under the same name (#545's review): the old Instance, stale and
// draining, has its worker crash within seconds of the new deployment's Instance failing to start. Neither
// exit was a serving worker's of the new deployment, so no CrashCorrelated names it. (The serving rule
// alone covers this scenario, since a stale Instance is draining; the stale guard's own discriminator is
// TestCorrelateExitsCountsOnlyServingWorkers' stale case.)
func TestARecreatedDeploymentsStaleInstanceNeverPairs(t *testing.T) {
	rg := newRig(t)
	// The first Allocation's engine runs requests all along, so its Instance stays draining once its
	// deployment is gone; every later Allocation's engine fails to start.
	first := ""
	rg.engine = func(alloc string) afake.Engine {
		if first == "" {
			first = alloc
		}
		if alloc == first {
			return afake.Engine{Health: afake.HealthPlainLoad()}
		}
		return afake.Engine{StartFails: true}
	}
	rg.create(mdSpec("md"))
	rg.until(5*time.Minute, 10*time.Second, "ready", func() bool { return rg.readyCount("md") == 1 })
	old := rg.instances("md")[0]
	oldAlloc := old.Spec.AllocationRef.Name
	if oldAlloc != first {
		t.Fatalf("the ready Instance's Allocation %s is not the first one %s", oldAlloc, first)
	}
	if err := rg.c.Delete(rg.ctx, rg.md("md")); err != nil {
		t.Fatal(err)
	}
	rg.create(mdSpec("md"))
	var fresh *afake.Agent
	rg.until(10*time.Minute, 5*time.Second, "the new Instance's first start failed", func() bool {
		for _, a := range rg.allocations("md") {
			if a.Name != oldAlloc {
				if ra := rg.agents[a.Name]; ra != nil && len(ra.agent.Starts()) > 0 {
					fresh = ra.agent
					return true
				}
			}
		}
		return false
	})
	if ph := rg.instance(old.Name).Status.Phase; ph != v1alpha1.InstanceDraining {
		t.Fatalf("the old Instance is %s when the new one's start failed, want draining: the test proves nothing", ph)
	}
	failedAt := fresh.Starts()[0].Time
	rg.agentOf(oldAlloc).agent.CrashWorker(v1alpha1.WorkerName(0), 1)
	if gap := rg.clock.Now().Sub(failedAt); gap > crashWindow {
		t.Fatalf("the old worker crashed %s after the new start failed, past the window: the test proves nothing", gap)
	}
	for k := 0; k < 4; k++ {
		rg.Step()
		rg.clock.Advance(5 * time.Second)
	}
	if evs := crashEvents(rg); len(evs) != 0 {
		t.Fatalf("CrashCorrelated %+v, want none: a stale draining Instance and a failed start", evs)
	}
}
