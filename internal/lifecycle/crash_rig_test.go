package lifecycle

// CrashCorrelated on the lifecycle rig: two ready Instances of one ModelDeployment, each its own
// Allocation and fake agent, whose workers the agents crash at times the test sets on the rig's clock.
// The pair raises the event; one Instance's crash loop, exits 31 s apart, two requested restarts at
// once, and silent agents raise none.

import (
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
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
// draining with requests in flight, so serving by #550's rule, has its worker crash 10 s before the new
// deployment's ready Instance's worker does. The stale guard alone keeps them apart: no CrashCorrelated
// names the new deployment.
func TestARecreatedDeploymentsStaleInstanceNeverPairs(t *testing.T) {
	rg := newRig(t)
	// Every engine runs requests all along, so the old Instance stays draining once its deployment is
	// gone, and the new one still becomes ready.
	rg.engine = func(string) afake.Engine { return afake.Engine{Health: afake.HealthPlainLoad()} }
	rg.create(mdSpec("md"))
	rg.until(5*time.Minute, 10*time.Second, "ready", func() bool { return rg.readyCount("md") == 1 })
	old := rg.instances("md")[0]
	oldAlloc := old.Spec.AllocationRef.Name
	if err := rg.c.Delete(rg.ctx, rg.md("md")); err != nil {
		t.Fatal(err)
	}
	rg.create(mdSpec("md"))
	var fresh v1alpha1.Instance
	rg.until(15*time.Minute, 10*time.Second, "the new deployment's Instance ready", func() bool {
		for _, in := range rg.liveInstances("md") {
			if in.Name != old.Name && in.Status.Phase == v1alpha1.InstanceReady {
				fresh = in
				return true
			}
		}
		return false
	})
	if o := rg.instance(old.Name); o.Status.Phase != v1alpha1.InstanceDraining || !verifiedWorker(o) {
		t.Fatalf("the old Instance is %s (verified and warmed %v) when the new one is ready, want draining and serving: the test proves nothing", o.Status.Phase, verifiedWorker(o))
	}
	w := v1alpha1.WorkerName(0)
	rg.agentOf(oldAlloc).agent.CrashWorker(w, 1)
	startStep(rg, "md", 10*time.Second)
	rg.agentOf(fresh.Spec.AllocationRef.Name).agent.CrashWorker(w, 1)
	for k := 0; k < 3; k++ {
		startStep(rg, "md", 10*time.Second)
	}
	if evs := crashEvents(rg); len(evs) != 0 {
		t.Fatalf("CrashCorrelated %+v, want none: the crashed draining Instance is the old deployment's", evs)
	}
	if n := restartEvents(rg)[fresh.Name]; n == 0 {
		t.Fatalf("the new Instance's crash was never acted on (no restart): the test proves nothing")
	}
}

// drainRig brings two Instances of "md", on engines e, to ready, scales the deployment to one, and returns
// the rig, the Instance that drains and the one that stays.
func drainRig(t *testing.T, e func() afake.Engine) (*rig, v1alpha1.Instance, v1alpha1.Instance) {
	t.Helper()
	rg, _, _ := crashRig(t, e)
	rg.mutateMD("md", func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 1 })
	var drained, kept v1alpha1.Instance
	startUntil(rg, "md", 2*time.Minute, "one Instance draining or drained", func() bool {
		for _, in := range rg.instances("md") {
			if in.Status.Phase == v1alpha1.InstanceDraining || in.Status.Phase == v1alpha1.InstanceEnded {
				drained = in
			} else if in.Status.Phase == v1alpha1.InstanceReady {
				kept = in
			}
		}
		return drained.Name != "" && kept.Name != ""
	})
	return rg, drained, kept
}

// A draining Instance still serves its in-flight requests (#550 as revised): its worker killed, then the
// ready Instance's 10 s later, pair.
func TestADrainingEngineKilledThenAReadyOnePair(t *testing.T) {
	rg, draining, ready := drainRig(t, func() afake.Engine { return afake.Engine{Health: afake.HealthPlainLoad()} })
	if d := rg.instance(draining.Name); d.Status.Phase != v1alpha1.InstanceDraining {
		t.Fatalf("the surplus Instance is %s, want draining (its engine runs requests): the test proves nothing", d.Status.Phase)
	}
	w := v1alpha1.WorkerName(0)
	rg.agentOf(draining.Spec.AllocationRef.Name).agent.CrashWorker(w, 1)
	startStep(rg, "md", 10*time.Second)
	rg.agentOf(ready.Spec.AllocationRef.Name).agent.CrashWorker(w, 1)
	for k := 0; k < 3; k++ {
		startStep(rg, "md", 10*time.Second)
	}
	evs := crashEvents(rg)
	if len(evs) != 1 || evs[0].Name != ready.Name || evs[0].RelatedName != draining.Name {
		t.Fatalf("CrashCorrelated %+v, want one regarding %s, related to the draining %s", evs, ready.Name, draining.Name)
	}
}

// The drain's own stop of its worker is requested, reported stopped, never exited: the ready Instance's
// crash 10 s after it raises nothing.
func TestTheDrainsOwnStopDoesNotPair(t *testing.T) {
	rg, drained, ready := drainRig(t, nil)
	startUntil(rg, "md", 2*time.Minute, "the surplus Instance drained and its worker stopped", func() bool {
		rep, ok := rg.agentOf(drained.Spec.AllocationRef.Name).agent.LastReport()
		if !ok || rep.Instance == nil {
			return false
		}
		for _, w := range rep.Instance.Workers {
			if w.Phase == agentapi.PartStopped {
				return true
			}
		}
		return false
	})
	if d := rg.instance(drained.Name); d.Status.Phase != v1alpha1.InstanceEnded || d.Status.EndReason != v1alpha1.ReasonDrained {
		t.Fatalf("the surplus Instance is %s (%s), want ended Drained", d.Status.Phase, d.Status.EndReason)
	}
	startStep(rg, "md", 10*time.Second)
	rg.agentOf(ready.Spec.AllocationRef.Name).agent.CrashWorker(v1alpha1.WorkerName(0), 1)
	for k := 0; k < 3; k++ {
		startStep(rg, "md", 10*time.Second)
	}
	if evs := crashEvents(rg); len(evs) != 0 {
		t.Fatalf("CrashCorrelated %+v, want none: the drained worker was stopped as asked", evs)
	}
}
