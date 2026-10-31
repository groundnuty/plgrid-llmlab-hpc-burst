package fake_test

import (
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

// counted is an Engine whose scripted answers count how often the Agent asked
// them. A section that is absent because the gate held it back was never asked:
// the counts show that, where an absent result alone could also mean the double
// answered nothing.
type counted struct {
	verifies, qualifies, rounds int
	startsAtVerify              []int // how many processes had been started when Verify was asked
}

func (c *counted) engine(base fake.Engine, started func() int) fake.Engine {
	base.Verify = func(i agentapi.DesiredInstance, req agentapi.VerifyRequest) agentapi.VerifyResult {
		c.verifies++
		if started != nil {
			c.startsAtVerify = append(c.startsAtVerify, started())
		}
		return fake.DefaultVerify(i, req)
	}
	base.Qualify = func(i agentapi.DesiredInstance, req agentapi.QualifyRequest) agentapi.QualifyResult {
		c.qualifies++
		return fake.DefaultQualify(i, req)
	}
	base.WarmUpRound = func(n int32, req agentapi.WarmUpRequest) agentapi.WarmUpRound {
		c.rounds++
		return fake.DefaultWarmUpRound(n, req)
	}
	return base
}

func (c *counted) counts() [3]int { return [3]int{c.verifies, c.qualifies, c.rounds} }

// allChecks asks for verification, qualification and warm-up under generation g.
func allChecks(g int64) func(*agentapi.DesiredInstance) {
	return withChecks(agentapi.Checks{
		VerificationGeneration: g,
		Verify:                 &agentapi.VerifyRequest{},
		Qualify:                &agentapi.QualifyRequest{Shapes: shapes},
		WarmUp:                 &agentapi.WarmUpRequest{Shapes: shapes, TargetSessions: 1},
	})
}

func TestNoCheckRunsBeforeTheWorkerRunsAndAllStartInTheStepItDoes(t *testing.T) {
	const delay = 10 * time.Second
	var c counted
	r := newRig(t, c.engine(fake.Engine{StartDelay: delay}, nil))
	r.set(run(instance(allChecks(1))))

	r.step() // t0: the worker starts
	eq(t, "worker phase", r.workerOf("worker-0").Phase, agentapi.PartStarting)
	got := checksOf(t, r)
	eq(t, "the report carries the generation", got.VerificationGeneration, int64(1))
	eq(t, "Verify while starting", got.Verify, (*agentapi.VerifyResult)(nil))
	eq(t, "Qualify while starting", got.Qualify, (*agentapi.QualifyResult)(nil))
	eq(t, "WarmUp while starting", got.WarmUp, (*agentapi.WarmUpResult)(nil))

	r.clk.Advance(delay - time.Nanosecond)
	r.steps(2)
	eq(t, "worker phase 1ns before the delay", r.workerOf("worker-0").Phase, agentapi.PartStarting)
	got = checksOf(t, r)
	eq(t, "Verify 1ns before the delay", got.Verify, (*agentapi.VerifyResult)(nil))
	eq(t, "WarmUp 1ns before the delay", got.WarmUp, (*agentapi.WarmUpResult)(nil))
	eq(t, "the engine was not asked: (verify, qualify, round)", c.counts(), [3]int{})

	r.clk.Advance(time.Nanosecond)
	r.step() // t0+10 s: running
	eq(t, "worker phase at the delay", r.workerOf("worker-0").Phase, agentapi.PartRunning)
	got = checksOf(t, r)
	if got.Verify == nil || !got.Verify.Done || got.Qualify == nil || !got.Qualify.Done || got.WarmUp == nil || len(got.WarmUp.Rounds) != 1 {
		t.Errorf("in the Step the worker runs: checks = %+v, want Verify, Qualify and warm-up round 1", got)
	}
	eq(t, "the engine was asked once each: (verify, qualify, round)", c.counts(), [3]int{1, 1, 1})
}

func TestACrashedWorkerGetsNoNewResultsAndWarmUpRoundsStopUntilItRunsAgain(t *testing.T) {
	var c counted
	r := newRig(t, c.engine(fake.Engine{}, nil))
	r.set(run(instance(allChecks(1))))
	r.steps(3) // starts; then round 1; then round 2
	eq(t, "(verify, qualify, round) while running", c.counts(), [3]int{1, 1, 2})
	eq(t, "rounds while running", len(checksOf(t, r).WarmUp.Rounds), 2)

	r.a.CrashWorker("worker-0", 1)
	r.steps(3)
	eq(t, "worker phase", r.workerOf("worker-0").Phase, agentapi.PartExited)
	w := checksOf(t, r).WarmUp
	eq(t, "no round runs on an exited worker", len(w.Rounds), 2)
	eq(t, "warm-up is paused, not done", w.Done, false)
	eq(t, "the engine was not asked while the worker was exited: (verify, qualify, round)", c.counts(), [3]int{1, 1, 2})
	// What was complete stays as it was.
	eq(t, "Verify stays", checksOf(t, r).Verify != nil && checksOf(t, r).Verify.Done, true)
	eq(t, "Qualify stays", checksOf(t, r).Qualify != nil && checksOf(t, r).Qualify.Done, true)

	// The restart: rounds resume once the new process runs, not before.
	r.set(run(instance(allChecks(1), withWorkers(1))))
	r.step()
	eq(t, "new process starting", r.workerOf("worker-0").Phase, agentapi.PartStarting)
	eq(t, "rounds while the new process starts", len(checksOf(t, r).WarmUp.Rounds), 2)
	r.step()
	eq(t, "new process running", r.workerOf("worker-0").Phase, agentapi.PartRunning)
	eq(t, "rounds once the new process runs", len(checksOf(t, r).WarmUp.Rounds), 3)
	eq(t, "(verify, qualify, round) after the restart", c.counts(), [3]int{1, 1, 3})
}

func TestANewVerificationGenerationGetsNoResultFromACrashedWorker(t *testing.T) {
	var c counted
	r := newRig(t, c.engine(fake.Engine{}, nil))
	verify := func(g int64) func(*agentapi.DesiredInstance) {
		return withChecks(agentapi.Checks{VerificationGeneration: g, Verify: &agentapi.VerifyRequest{}})
	}
	r.set(run(instance(verify(1))))
	r.steps(2)
	eq(t, "generation 1 verified", checksOf(t, r).Verify != nil, true)

	r.a.CrashWorker("worker-0", 1)
	r.steps(2)
	got := checksOf(t, r)
	eq(t, "generation 1's Verify stays while the worker is exited", got.Verify != nil && got.Verify.Done, true)

	r.set(run(instance(verify(2))))
	r.steps(3)
	got = checksOf(t, r)
	eq(t, "the report moves to generation 2", got.VerificationGeneration, int64(2))
	eq(t, "generation 2 has no Verify: the worker is exited", got.Verify, (*agentapi.VerifyResult)(nil))
	eq(t, "the engine was asked for generation 1 only", c.verifies, 1)

	r.set(run(instance(verify(2), withWorkers(1))))
	r.step()
	eq(t, "no result while the new process starts", checksOf(t, r).Verify, (*agentapi.VerifyResult)(nil))
	eq(t, "still not asked", c.verifies, 1)
	r.step()
	eq(t, "the new process runs", r.workerOf("worker-0").Phase, agentapi.PartRunning)
	got = checksOf(t, r)
	eq(t, "generation 2's Verify once the new process runs", got.VerificationGeneration == 2 && got.Verify != nil && got.Verify.Done, true)
	eq(t, "the engine was asked for generation 2", c.verifies, 2)
}

func TestAResultForANewGenerationNeverComesFromTheProcessItReplaces(t *testing.T) {
	for _, tc := range []struct {
		name    string
		restart func(*agentapi.DesiredInstance)
	}{
		{"raised worker generation", withWorkers(1)},
		{"raised Instance generation", withGeneration(2)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var c counted
			var r *rig
			engine := c.engine(fake.Engine{IgnoreTerm: true}, func() int { return len(r.a.Starts()) })
			r = newRig(t, engine)
			verify := func(g int64) func(*agentapi.DesiredInstance) {
				return withChecks(agentapi.Checks{VerificationGeneration: g, Verify: &agentapi.VerifyRequest{}})
			}
			r.set(runGrace(instance(verify(1))))
			r.steps(2)
			eq(t, "generation 1 verified by process 1", c.startsAtVerify, []int{1})
			r.clk.Advance(60 * time.Second) // t0+60 s

			// The controller raises the generation to restart and asks for a new
			// verification generation in the same desired state.
			r.set(runGrace(instance(verify(2), tc.restart)))
			r.step() // t0+60 s: TERM, ignored
			eq(t, "the old process is stopping", r.workerOf("worker-0").Phase, agentapi.PartStopping)
			eq(t, "no generation 2 result while the old process stops", checksOf(t, r).Verify, (*agentapi.VerifyResult)(nil))

			r.clk.Advance(grace - time.Nanosecond)
			r.step()
			eq(t, "the old process is still stopping 1ns before the kill", r.workerOf("worker-0").Phase, agentapi.PartStopping)
			eq(t, "still no result", checksOf(t, r).Verify, (*agentapi.VerifyResult)(nil))

			r.clk.Advance(time.Nanosecond)
			r.step() // t0+90 s: killed
			eq(t, "the old process is killed", r.workerOf("worker-0").Phase, agentapi.PartStopped)
			eq(t, "no result from a stopped process", checksOf(t, r).Verify, (*agentapi.VerifyResult)(nil))

			r.step() // the replacement starts
			w := r.workerOf("worker-0")
			eq(t, "the replacement is starting", [2]any{w.Phase, w.PID}, [2]any{agentapi.PartStarting, 4001})
			eq(t, "no result before the replacement runs", checksOf(t, r).Verify, (*agentapi.VerifyResult)(nil))
			eq(t, "the engine was not asked about generation 2 yet", c.verifies, 1)

			r.step() // the replacement runs
			w = r.workerOf("worker-0")
			eq(t, "the replacement runs", [2]any{w.Phase, w.PID}, [2]any{agentapi.PartRunning, 4001})
			got := checksOf(t, r)
			eq(t, "generation 2's Verify", got.VerificationGeneration == 2 && got.Verify != nil && got.Verify.Done, true)
			eq(t, "generation 2 was verified by process 2", c.startsAtVerify, []int{1, 2})
		})
	}
}

func TestAnInstanceWithNoDesiredWorkerHasNoProcessToCheck(t *testing.T) {
	var c counted
	r := newRig(t, c.engine(fake.Engine{}, nil))
	r.set(run(instance(allChecks(1), withWorkers())))
	r.steps(3)
	got := checksOf(t, r)
	eq(t, "the report carries the generation", got.VerificationGeneration, int64(1))
	eq(t, "Verify", got.Verify, (*agentapi.VerifyResult)(nil))
	eq(t, "Qualify", got.Qualify, (*agentapi.QualifyResult)(nil))
	eq(t, "WarmUp", got.WarmUp, (*agentapi.WarmUpResult)(nil))
	eq(t, "the engine was not asked", c.counts(), [3]int{})
}
