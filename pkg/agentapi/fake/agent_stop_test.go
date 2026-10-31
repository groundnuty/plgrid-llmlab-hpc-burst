package fake_test

import (
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

// grace is the stop grace of these tests. Every time below is worked out by hand
// from it and from t0: the stop instruction comes at t0+60 s, so the kill is due
// at t0+90 s.
const grace = 30 * time.Second

func withGrace(d agentapi.DesiredState) agentapi.DesiredState {
	d.Settings.StopGrace = agentapi.Duration(grace)
	return d
}

func runGrace(i agentapi.DesiredInstance) agentapi.DesiredState { return withGrace(run(i)) }

func idleGrace() agentapi.DesiredState {
	return withGrace(agentapi.DesiredState{Action: agentapi.ActionIdle})
}

func stopGrace() agentapi.DesiredState {
	return withGrace(agentapi.DesiredState{Action: agentapi.ActionStop})
}

// stubborn returns a rig whose engine ignores TERM, with worker-0 running since
// t0 as PID 4000, and the clock at t0+60 s.
func stubborn(t *testing.T) *rig {
	t.Helper()
	r := newRig(t, fake.Engine{IgnoreTerm: true})
	r.set(runGrace(instance()))
	r.steps(2)
	eq(t, "phase before the instruction", r.workerOf("worker-0").Phase, agentapi.PartRunning)
	r.clk.Advance(60 * time.Second)
	return r
}

func TestARestartWaitsForAWorkerThatIgnoresTermUntilItIsKilled(t *testing.T) {
	cases := []struct {
		name       string
		next       agentapi.DesiredInstance
		workerGen  int64
		instanceGn int64
	}{
		{"raised worker generation", instance(withWorkers(1)), 1, 1},
		{"raised Instance generation", instance(withGeneration(2)), 0, 2},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := stubborn(t)
			r.set(runGrace(c.next))

			r.step() // t0+60 s: TERM sent
			w := r.workerOf("worker-0")
			eq(t, "phase after the instruction", w.Phase, agentapi.PartStopping)
			eq(t, "PID: still the old process", w.PID, 4000)
			eq(t, "Generation: still the old process's", w.Generation, int64(0))
			eq(t, "ExitCode", w.ExitCode, (*int)(nil))
			eq(t, "Health of a stopping worker", w.Health, (*agentapi.Health)(nil))
			eq(t, "ServingEndpoint", r.lastInstance().ServingEndpoint, "")
			eq(t, "Instance generation: not applied while the old process lives", r.lastInstance().Generation, int64(1))
			eq(t, "starts", len(r.a.Starts()), 1)

			r.clk.Advance(29 * time.Second)
			r.step() // t0+89 s
			eq(t, "phase one second before the kill", r.workerOf("worker-0").Phase, agentapi.PartStopping)

			r.clk.Advance(time.Second)
			r.step() // t0+90 s: killed
			w = r.workerOf("worker-0")
			eq(t, "phase at the kill", w.Phase, agentapi.PartStopped)
			eq(t, "ExitCode at the kill: killed by a signal", w.ExitCode, intp(-1))
			eq(t, "PID at the kill", w.PID, 4000)
			eq(t, "starts at the kill", len(r.a.Starts()), 1)

			r.step() // the Step after the kill: the new process
			w = r.workerOf("worker-0")
			eq(t, "phase of the new process", w.Phase, agentapi.PartStarting)
			eq(t, "PID of the new process", w.PID, 4001)
			eq(t, "Generation of the new process", w.Generation, c.workerGen)
			eq(t, "ExitCode of the new process", w.ExitCode, (*int)(nil))
			eq(t, "Instance generation once the new process runs", r.lastInstance().Generation, c.instanceGn)
			starts := r.a.Starts()
			eq(t, "starts after the restart", len(starts), 2)
			if !starts[1].Time.Equal(t0.Add(90 * time.Second)) {
				t.Errorf("the new process started at %v, want t0+90s", starts[1].Time)
			}
		})
	}
}

func TestAWorkerThatHonoursTermRestartsInTheStepOfTheInstruction(t *testing.T) {
	r := newRig(t, fake.Engine{})
	r.set(runGrace(instance()))
	r.steps(2)
	r.clk.Advance(60 * time.Second)
	r.set(runGrace(instance(withWorkers(1))))
	r.step()

	w := r.workerOf("worker-0")
	eq(t, "phase", w.Phase, agentapi.PartStarting)
	eq(t, "PID", w.PID, 4001)
	eq(t, "ExitCode", w.ExitCode, (*int)(nil))
	starts := r.a.Starts()
	eq(t, "starts", len(starts), 2)
	if !starts[1].Time.Equal(t0.Add(60 * time.Second)) {
		t.Errorf("the new process started at %v, want t0+60s", starts[1].Time)
	}
}

func TestIdleKillsAWorkerThatIgnoresTermAfterTheGraceAndLeavesItStopped(t *testing.T) {
	r := stubborn(t)
	r.set(idleGrace())
	r.step() // t0+60 s
	w := r.workerOf("worker-0")
	eq(t, "phase after idle", w.Phase, agentapi.PartStopping)
	eq(t, "ExitCode after idle", w.ExitCode, (*int)(nil))

	// The instruction repeats at every Step; it does not move the deadline.
	r.clk.Advance(29 * time.Second)
	r.step() // t0+89 s
	eq(t, "phase one second before the kill", r.workerOf("worker-0").Phase, agentapi.PartStopping)

	r.clk.Advance(time.Second)
	r.step() // t0+90 s
	w = r.workerOf("worker-0")
	eq(t, "phase at the deadline", w.Phase, agentapi.PartStopped)
	eq(t, "ExitCode at the deadline", w.ExitCode, intp(-1))

	r.clk.Advance(time.Hour)
	r.steps(2)
	w = r.workerOf("worker-0")
	eq(t, "phase much later", w.Phase, agentapi.PartStopped)
	eq(t, "PID much later: nothing restarted it", w.PID, 4000)
	eq(t, "starts", len(r.a.Starts()), 1)
	if r.a.Exited() {
		t.Error("an idle agent exited")
	}
}

func TestStopExitsOnlyOnceEveryWorkerIsGone(t *testing.T) {
	r := stubborn(t)
	r.set(stopGrace())
	r.step() // t0+60 s
	eq(t, "phase after stop", r.workerOf("worker-0").Phase, agentapi.PartStopping)
	eq(t, "Exited with a worker still stopping", r.a.Exited(), false)

	r.clk.Advance(29 * time.Second)
	r.step() // t0+89 s
	eq(t, "Exited one second before the kill", r.a.Exited(), false)
	reports := len(r.reports())

	r.clk.Advance(time.Second)
	r.step() // t0+90 s
	w := r.workerOf("worker-0")
	eq(t, "phase in the final report", w.Phase, agentapi.PartStopped)
	eq(t, "ExitCode in the final report", w.ExitCode, intp(-1))
	eq(t, "Exited after the final report", r.a.Exited(), true)
	eq(t, "reports: the final one was sent", len(r.reports()), reports+1)

	if err := r.a.Step(bgndCtx); err != nil {
		t.Fatalf("Step after Stop: %v", err)
	}
	eq(t, "reports after Exited", len(r.reports()), reports+1)
}

func TestANewInstanceWaitsForTheOldOnesProcessToBeKilled(t *testing.T) {
	r := stubborn(t)
	oldKey := r.lastInstance().Key
	second := func(i *agentapi.DesiredInstance) { i.UID = "inst-uid-2" }
	r.set(runGrace(instance(second)))

	r.step() // t0+60 s
	in := r.lastInstance()
	eq(t, "the report still shows the old Instance", in.UID, "inst-uid-1")
	eq(t, "its worker is stopping", in.Workers[0].Phase, agentapi.PartStopping)
	eq(t, "its key", in.Key, oldKey)
	eq(t, "starts: none for the new Instance yet", len(r.a.Starts()), 1)

	r.clk.Advance(30 * time.Second)
	r.step() // t0+90 s: killed
	in = r.lastInstance()
	eq(t, "the old Instance at the kill", in.UID, "inst-uid-1")
	eq(t, "its worker at the kill: phase", in.Workers[0].Phase, agentapi.PartStopped)
	eq(t, "its worker at the kill: exit code", in.Workers[0].ExitCode, intp(-1))

	r.step()
	in = r.lastInstance()
	eq(t, "the new Instance after the kill", in.UID, "inst-uid-2")
	eq(t, "its worker: phase", in.Workers[0].Phase, agentapi.PartStarting)
	eq(t, "its worker: PID", in.Workers[0].PID, 4001)
	if in.Key == oldKey || !hex64.MatchString(in.Key) {
		t.Errorf("Key = %q: want a new key, not %q", in.Key, oldKey)
	}
	starts := r.a.Starts()
	eq(t, "starts", len(starts), 2)
	eq(t, "the new start's Instance", starts[1].Instance, "inst-uid-2")
}

func TestWithoutAGraceTheKillComesInTheStepOfTheInstruction(t *testing.T) {
	r := newRig(t, fake.Engine{IgnoreTerm: true})
	r.set(run(instance())) // Settings.StopGrace is 0
	r.steps(2)
	r.set(run(instance(withWorkers(1))))

	r.step()
	w := r.workerOf("worker-0")
	eq(t, "phase", w.Phase, agentapi.PartStopped)
	eq(t, "ExitCode", w.ExitCode, intp(-1))
	eq(t, "PID", w.PID, 4000)
	r.step()
	eq(t, "PID of the new process", r.workerOf("worker-0").PID, 4001)
}

func TestAKillThatComesDueDuringASilenceShowsInTheFirstReportAfterIt(t *testing.T) {
	r := stubborn(t)
	r.set(idleGrace())
	r.step() // t0+60 s: stopping
	reports := len(r.reports())

	r.a.Silence(true)
	r.clk.Advance(5 * time.Minute)
	if err := r.a.Step(bgndCtx); err != nil {
		t.Fatalf("Step while silenced: %v", err)
	}
	eq(t, "reports while silenced", len(r.reports()), reports)
	eq(t, "the last report still says stopping", r.workerOf("worker-0").Phase, agentapi.PartStopping)

	r.a.Silence(false)
	r.step()
	w := r.workerOf("worker-0")
	eq(t, "phase after the outage", w.Phase, agentapi.PartStopped)
	eq(t, "ExitCode after the outage", w.ExitCode, intp(-1))
	if got := r.last().Time; !got.Equal(t0.Add(6 * time.Minute)) {
		t.Errorf("Report.Time = %v, want t0+6m", got)
	}
}

func TestAStartingWorkerThatIgnoresTermIsStoppedAndKilledToo(t *testing.T) {
	r := newRig(t, fake.Engine{IgnoreTerm: true, StartDelay: time.Hour})
	r.set(runGrace(instance()))
	r.steps(2)
	eq(t, "phase before", r.workerOf("worker-0").Phase, agentapi.PartStarting)

	r.clk.Advance(60 * time.Second)
	r.set(idleGrace())
	r.step()
	eq(t, "phase after idle", r.workerOf("worker-0").Phase, agentapi.PartStopping)
	r.clk.Advance(grace)
	r.step()
	w := r.workerOf("worker-0")
	eq(t, "phase after the grace", w.Phase, agentapi.PartStopped)
	eq(t, "ExitCode after the grace", w.ExitCode, intp(-1))
}

func TestTheGraceInForceAtTheInstructionSetsTheDeadline(t *testing.T) {
	r := stubborn(t)
	r.set(idleGrace()) // a 30 s grace
	r.step()           // t0+60 s: the deadline is t0+90 s

	long := idleGrace()
	long.Settings.StopGrace = agentapi.Duration(time.Hour)
	r.set(long)
	r.clk.Advance(30 * time.Second)
	r.step() // t0+90 s
	w := r.workerOf("worker-0")
	eq(t, "phase: a longer grace later does not move the deadline", w.Phase, agentapi.PartStopped)
	eq(t, "ExitCode", w.ExitCode, intp(-1))
}

func TestCrashWorkerRefusesAWorkerThatIsStopping(t *testing.T) {
	r := stubborn(t)
	r.set(idleGrace())
	r.step()
	defer func() {
		if recover() == nil {
			t.Error("CrashWorker on a stopping worker did not panic")
		}
	}()
	r.a.CrashWorker("worker-0", 1)
}
