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
