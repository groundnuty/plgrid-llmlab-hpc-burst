package fake_test

import (
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

var (
	stagingInProgress = &agentapi.Staging{Phase: agentapi.StagingInProgress}
	stagingDone       = &agentapi.Staging{Phase: agentapi.StagingDone}
)

func stagingFailed(msg string) *agentapi.Staging {
	return &agentapi.Staging{Phase: agentapi.StagingFailed, Message: msg}
}

// noWorkerYet fails the test unless the last report shows no worker and the
// Agent has started none.
func noWorkerYet(t *testing.T, r *rig, when string) {
	t.Helper()
	eq(t, when+": workers in the report", len(r.lastInstance().Workers), 0)
	eq(t, when+": ServingEndpoint", r.lastInstance().ServingEndpoint, "")
	eq(t, when+": starts", len(r.a.Starts()), 0)
}

func TestStagingIsInProgressUntilTheDurationHasPassedAndNoWorkerStartsBefore(t *testing.T) {
	const dur = 30 * time.Second
	r := newRig(t, fake.Engine{StagingDuration: dur})
	r.set(run(instance()))

	r.step() // t0: the Instance's first apply
	eq(t, "Staging at the first apply", r.lastInstance().Staging, stagingInProgress)
	noWorkerYet(t, r, "first apply")

	r.clk.Advance(dur - time.Nanosecond)
	r.step()
	eq(t, "Staging 1ns before the duration", r.lastInstance().Staging, stagingInProgress)
	noWorkerYet(t, r, "1ns before the duration")

	r.clk.Advance(time.Nanosecond)
	r.step() // t0+30 s
	in := r.lastInstance()
	eq(t, "Staging at exactly the duration", in.Staging, stagingDone)
	w := in.Workers[0]
	eq(t, "the worker starts in the Step staging is done: phase", w.Phase, agentapi.PartStarting)
	eq(t, "PID", w.PID, 4000)
	if !w.StartTime.Equal(t0.Add(dur)) {
		t.Errorf("StartTime = %v, want t0+30s", w.StartTime)
	}
	r.step()
	eq(t, "the worker runs a Step later", r.workerOf("worker-0").Phase, agentapi.PartRunning)
}

func TestTheStagingClockStartsAtTheFirstApplyNotWhenTheStateWasSet(t *testing.T) {
	const dur = 30 * time.Second
	r := newRig(t, fake.Engine{StagingDuration: dur})
	r.set(run(instance()))
	r.clk.Advance(5 * time.Minute) // the Agent takes the Instance up late

	r.step() // t0+5m
	eq(t, "Staging at the first apply", r.lastInstance().Staging, stagingInProgress)
	r.clk.Advance(dur - time.Nanosecond)
	r.step()
	eq(t, "Staging 1ns before the duration", r.lastInstance().Staging, stagingInProgress)
	r.clk.Advance(time.Nanosecond)
	r.step()
	eq(t, "Staging at the duration", r.lastInstance().Staging, stagingDone)
}

func TestWithoutAStagingDurationStagingIsDoneInTheFirstStepAndTheWorkerStarts(t *testing.T) {
	r := newRig(t, fake.Engine{})
	r.set(run(instance()))
	r.step()
	eq(t, "Staging", r.lastInstance().Staging, stagingDone)
	eq(t, "the worker starts in the first Step", r.workerOf("worker-0").Phase, agentapi.PartStarting)
}

func TestFailedStagingReportsTheMessageAndNoWorkerEverStarts(t *testing.T) {
	const dur = 10 * time.Second
	r := newRig(t, fake.Engine{StagingDuration: dur, StagingError: "no space left on device"})
	r.set(run(instance()))

	r.step()
	eq(t, "Staging at the first apply", r.lastInstance().Staging, stagingInProgress)
	r.clk.Advance(dur - time.Nanosecond)
	r.step()
	eq(t, "Staging 1ns before the duration", r.lastInstance().Staging, stagingInProgress)
	noWorkerYet(t, r, "1ns before the duration")

	r.clk.Advance(time.Nanosecond)
	r.step()
	eq(t, "Staging at the duration", r.lastInstance().Staging, stagingFailed("no space left on device"))
	noWorkerYet(t, r, "failed")

	// Nothing restarts it on its own.
	r.clk.Advance(time.Hour)
	r.steps(3)
	eq(t, "Staging an hour later", r.lastInstance().Staging, stagingFailed("no space left on device"))
	noWorkerYet(t, r, "an hour later")
}

func TestStagingThatFailsAtOnceFailsInTheFirstStep(t *testing.T) {
	r := newRig(t, fake.Engine{StagingError: "disk full"})
	r.set(run(instance()))
	r.step()
	eq(t, "Staging", r.lastInstance().Staging, stagingFailed("disk full"))
	noWorkerYet(t, r, "failed at once")
}

func TestTheStagingMessageIsCutAtARuneBoundaryToMaxMessageBytes(t *testing.T) {
	// "€" is 3 bytes: 85 of them are 255 bytes, and the 86th would end at 258.
	r := newRig(t, fake.Engine{StagingError: strings.Repeat("€", 3400)})
	r.set(run(instance()))
	r.step()
	eq(t, "Staging", r.lastInstance().Staging, stagingFailed(strings.Repeat("€", 85)))

	// A message at the limit is kept whole.
	atLimit := strings.Repeat("e", 256)
	r = newRig(t, fake.Engine{StagingError: atLimit})
	r.set(run(instance()))
	r.step()
	eq(t, "Staging message of 256 bytes", r.lastInstance().Staging, stagingFailed(atLimit))
}

func TestARisingInstanceGenerationRestagesAFailedStagingWithTheSameSettings(t *testing.T) {
	const dur = 10 * time.Second
	r := newRig(t, fake.Engine{StagingDuration: dur, StagingError: "disk full"})
	r.set(run(instance()))
	r.step()
	r.clk.Advance(dur)
	r.step()
	eq(t, "Staging failed", r.lastInstance().Staging, stagingFailed("disk full"))

	r.clk.Advance(5 * time.Second)
	r.set(run(instance(withGeneration(2))))
	r.step() // t0+15 s: re-staged from now
	eq(t, "Staging after the generation rose", r.lastInstance().Staging, stagingInProgress)
	noWorkerYet(t, r, "re-staging")

	r.clk.Advance(dur - time.Nanosecond)
	r.step()
	eq(t, "Staging 1ns before the new duration", r.lastInstance().Staging, stagingInProgress)
	r.clk.Advance(time.Nanosecond)
	r.step()
	eq(t, "Staging failed again", r.lastInstance().Staging, stagingFailed("disk full"))
	noWorkerYet(t, r, "failed again")

	// The same generation again re-stages nothing.
	r.clk.Advance(time.Hour)
	r.step()
	eq(t, "Staging with the generation unchanged", r.lastInstance().Staging, stagingFailed("disk full"))
}

func TestAStagingThatSucceededIsNotRepeatedWhenTheInstanceGenerationRises(t *testing.T) {
	const dur = 10 * time.Second
	r := newRig(t, fake.Engine{StagingDuration: dur})
	r.set(run(instance()))
	r.step()
	r.clk.Advance(dur)
	r.steps(2) // done, starting, running
	eq(t, "worker phase", r.workerOf("worker-0").Phase, agentapi.PartRunning)

	r.set(run(instance(withGeneration(2))))
	r.step()
	in := r.lastInstance()
	eq(t, "Staging after the rise", in.Staging, stagingDone)
	eq(t, "the restart is not delayed: phase", in.Workers[0].Phase, agentapi.PartStarting)
	eq(t, "the restart is not delayed: PID", in.Workers[0].PID, 4001)
}

func TestANewInstanceStagesFromTheStepThatTakesItUpAfterTheOldOnesProcessIsKilled(t *testing.T) {
	const dur = 10 * time.Second
	r := newRig(t, fake.Engine{IgnoreTerm: true, StagingDuration: dur})
	r.set(runGrace(instance()))
	r.step() // t0: staging
	r.clk.Advance(dur)
	r.steps(2) // t0+10 s: starting, running
	eq(t, "the first Instance runs", r.workerOf("worker-0").Phase, agentapi.PartRunning)
	r.clk.Advance(60 * time.Second) // t0+70 s

	r.set(runGrace(instance(func(i *agentapi.DesiredInstance) { i.UID = "inst-uid-2" })))
	r.step() // t0+70 s: TERM, ignored
	eq(t, "the report still shows the old Instance", r.lastInstance().UID, "inst-uid-1")
	eq(t, "its worker is stopping", r.workerOf("worker-0").Phase, agentapi.PartStopping)

	r.clk.Advance(grace)
	r.step() // t0+100 s: killed
	eq(t, "the old Instance at the kill", r.lastInstance().UID, "inst-uid-1")

	r.clk.Advance(5 * time.Second)
	r.step() // t0+105 s: the new Instance is taken up
	in := r.lastInstance()
	eq(t, "the new Instance", in.UID, "inst-uid-2")
	eq(t, "its Staging at the first apply", in.Staging, stagingInProgress)
	eq(t, "its workers", len(in.Workers), 0)
	eq(t, "starts: only the old Instance's", len(r.a.Starts()), 1)

	r.clk.Advance(dur - time.Nanosecond)
	r.step() // t0+115 s, less 1 ns
	eq(t, "Staging 1ns before the duration", r.lastInstance().Staging, stagingInProgress)
	r.clk.Advance(time.Nanosecond)
	r.step() // t0+115 s
	in = r.lastInstance()
	eq(t, "Staging at the duration", in.Staging, stagingDone)
	eq(t, "the new worker starts", in.Workers[0].PID, 4001)
	eq(t, "starts", len(r.a.Starts()), 2)
}

func TestARequeuedExecutionStagesFromNothing(t *testing.T) {
	const dur = 10 * time.Second
	r := newRig(t, fake.Engine{StagingDuration: dur})
	r.set(run(instance()))
	r.step()
	r.clk.Advance(dur)
	r.steps(2)
	eq(t, "the first execution runs", r.workerOf("worker-0").Phase, agentapi.PartRunning)

	r.a.Requeue()
	r.step()
	in := r.lastInstance()
	eq(t, "Staging in the second execution", in.Staging, stagingInProgress)
	eq(t, "its workers", len(in.Workers), 0)
	eq(t, "starts: only the first execution's", len(r.a.Starts()), 1)
}
