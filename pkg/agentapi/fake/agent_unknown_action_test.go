package fake_test

import (
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

// unknownAction is a desired state with an Action this protocol version does not
// have, as a newer controller could send it. It carries an Instance with raised
// generations and settings that differ from the ones in force, so a Step that
// acted on it would show.
func unknownAction() agentapi.DesiredState {
	i := instance(withGeneration(9), withWorkers(9))
	return agentapi.DesiredState{
		Action:   "drain-later",
		Instance: &i,
		Settings: agentapi.AgentSettings{StopGrace: agentapi.Duration(time.Hour), ImageFormat: agentapi.ImageFormatSandbox},
	}
}

func TestAnUnknownActionChangesNothingAndTheAgentKeepsPollingAndReporting(t *testing.T) {
	r := newRig(t, fake.Engine{})
	r.set(run(instance()))
	r.steps(2)
	eq(t, "worker phase before", r.workerOf("worker-0").Phase, agentapi.PartRunning)
	reports, seen, starts := len(r.reports()), len(r.a.Seen()), len(r.a.Starts())
	var hooked []agentapi.Action
	r.a.OnDesired = func(d agentapi.DesiredState) { hooked = append(hooked, d.Action) }

	r.set(unknownAction())
	if err := r.a.Step(bgndCtx); err != nil {
		t.Fatalf("Step with an unknown action: %v", err)
	}
	eq(t, "a report was sent", len(r.reports()), reports+1)
	in := r.lastInstance()
	w := in.Workers[0]
	eq(t, "the worker keeps running: phase, PID, generation", [3]any{w.Phase, w.PID, w.Generation}, [3]any{agentapi.PartRunning, 4000, int64(0)})
	eq(t, "the Instance generation is not changed", in.Generation, int64(1))
	eq(t, "no worker was started", len(r.a.Starts()), starts)
	eq(t, "the settings in it are not taken: ImageFormat", r.a.ImageFormat(), agentapi.ImageFormatSIF)
	eq(t, "the state was seen", len(r.a.Seen()), seen+1)
	eq(t, "OnDesired", hooked, []agentapi.Action{"drain-later"})
	if r.a.Exited() {
		t.Error("the Agent exited")
	}

	// It goes on polling and reporting, and takes the next state it knows.
	r.steps(2)
	eq(t, "reports after two more Steps", len(r.reports()), reports+3)
	eq(t, "the worker still runs", r.workerOf("worker-0").PID, 4000)
	r.set(run(instance(withGeneration(2))))
	r.step()
	w = r.workerOf("worker-0")
	eq(t, "a state it knows is applied again: PID", w.PID, 4001)
	eq(t, "a state it knows is applied again: Instance generation", r.lastInstance().Generation, int64(2))
}

func TestAnUnknownActionDoesNotClearAPendingStop(t *testing.T) {
	r := stubborn(t) // worker-0 runs, ignores TERM; t0+60 s
	r.set(stopGrace())
	r.step()
	eq(t, "the worker is stopping", r.workerOf("worker-0").Phase, agentapi.PartStopping)
	eq(t, "Exited while the worker is stopping", r.a.Exited(), false)

	r.set(unknownAction())
	r.clk.Advance(grace)
	if err := r.a.Step(bgndCtx); err != nil {
		t.Fatalf("Step with an unknown action: %v", err)
	}
	w := r.workerOf("worker-0")
	eq(t, "the kill that came due is carried out: phase", w.Phase, agentapi.PartStopped)
	eq(t, "the kill that came due is carried out: exit code", w.ExitCode, intp(-1))
	eq(t, "the stop is still pending: the Agent exits once the worker is gone", r.a.Exited(), true)
}

func TestAnUnknownActionStillLetsAStartingWorkerRunWhenItsDelayHasPassed(t *testing.T) {
	const delay = 10 * time.Second
	r := newRig(t, fake.Engine{StartDelay: delay})
	r.set(run(instance()))
	r.step()
	eq(t, "starting", r.workerOf("worker-0").Phase, agentapi.PartStarting)

	r.set(unknownAction())
	r.clk.Advance(delay)
	r.step()
	eq(t, "running once the delay has passed", r.workerOf("worker-0").Phase, agentapi.PartRunning)
	eq(t, "the same process", r.workerOf("worker-0").PID, 4000)
}
