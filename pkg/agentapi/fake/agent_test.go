package fake_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

func TestFirstStepRegistersPollsAndReportsIdle(t *testing.T) {
	r := newRig(t, fake.Engine{})
	r.step()

	regs := r.b.Registrations(allocUID)
	if len(regs) != 1 {
		t.Fatalf("registrations = %d, want 1", len(regs))
	}
	if !hex32.MatchString(regs[0].Caller.AgentInstanceID) || regs[0].Caller.AgentInstanceID != r.a.AgentInstanceID() {
		t.Errorf("registered agent instance ID %q, agent agent instance ID %q: want the same 32 lowercase hex characters", regs[0].Caller.AgentInstanceID, r.a.AgentInstanceID())
	}
	eq(t, "registered caller, other fields", func() agentapi.Caller { c := regs[0].Caller; c.AgentInstanceID = ""; return c }(), caller(""))
	req := regs[0].Request
	eq(t, "Node", req.Node, "node-example")
	eq(t, "CPUArch", req.CPUArch, "aarch64")
	if len(req.GPUs) != 1 || !strings.Contains(req.GPUs[0].Name, "GH200") {
		t.Errorf("GPUs = %+v, want one GH200", req.GPUs)
	}

	reg, des, rep := r.calls()
	eq(t, "calls (register, desired, report)", [3]int{reg, des, rep}, [3]int{1, 1, 1})
	reports := r.reports()
	if len(reports) != 1 {
		t.Fatalf("reports = %d, want 1", len(reports))
	}
	if !reports[0].Time.Equal(t0) {
		t.Errorf("Report.Time = %v, want %v", reports[0].Time, t0)
	}
	if reports[0].Instance != nil {
		t.Errorf("an idle agent that never ran an Instance reported one: %+v", reports[0].Instance)
	}
	seen := r.a.Seen()
	if len(seen) != 1 || seen[0].Action != agentapi.ActionIdle || seen[0].Version == "" {
		t.Errorf("Seen = %+v, want the one idle state", seen)
	}
	rep0, ok := r.a.LastReport()
	if !ok {
		t.Fatal("LastReport: none")
	}
	eq(t, "agent's LastReport equals what the Backend recorded", rep0, reports[0])

	// A second step polls and reports again but does not register again, and
	// sees no new desired state.
	r.step()
	reg, des, rep = r.calls()
	eq(t, "calls after a second Step", [3]int{reg, des, rep}, [3]int{1, 2, 2})
	eq(t, "len(Seen) after a second Step", len(r.a.Seen()), 1)
}

func TestNewAgentKeepsAGivenAgentInstanceIDAndRegistrationOverride(t *testing.T) {
	b := fake.NewBackend()
	a := fake.NewAgent(b, caller(agent1), (&clock{t: t0}).Now, fake.Engine{})
	eq(t, "AgentInstanceID", a.AgentInstanceID(), agent1)
	a.Registration = agentapi.RegisterRequest{Node: "node-other", CPUArch: "x86_64"}
	if err := a.Step(bgndCtx); err != nil {
		t.Fatal(err)
	}
	regs := b.Registrations(allocUID)
	if len(regs) != 1 || regs[0].Caller.AgentInstanceID != agent1 {
		t.Fatalf("registrations = %+v", regs)
	}
	eq(t, "Request", regs[0].Request, agentapi.RegisterRequest{Node: "node-other", CPUArch: "x86_64"})
}

func TestRunStartsWorkerThenRunsAfterTheStartDelay(t *testing.T) {
	const delay = 10 * time.Second
	r := newRig(t, fake.Engine{StartDelay: delay})
	r.set(run(instance(withGeneration(3), withWorkers(2))))

	r.step()
	in := r.lastInstance()
	eq(t, "UID", in.UID, "inst-uid-1")
	eq(t, "Generation", in.Generation, int64(3))
	eq(t, "Staging", in.Staging, &agentapi.Staging{Phase: agentapi.StagingDone})
	eq(t, "ServingEndpoint while starting", in.ServingEndpoint, "")
	if !hex64.MatchString(in.Key) {
		t.Errorf("Key = %q, want 64 lowercase hex characters", in.Key)
	}
	if in.Checks != nil {
		t.Errorf("Checks = %+v, want none", in.Checks)
	}
	if len(in.Workers) != 1 {
		t.Fatalf("workers = %+v, want one", in.Workers)
	}
	w := in.Workers[0]
	eq(t, "worker name", w.Name, "worker-0")
	eq(t, "worker phase", w.Phase, agentapi.PartStarting)
	eq(t, "worker PID", w.PID, 4000)
	if w.StartTime == nil || !w.StartTime.Equal(t0) {
		t.Errorf("StartTime = %v, want %v", w.StartTime, t0)
	}
	eq(t, "worker ExitCode", w.ExitCode, (*int)(nil))
	eq(t, "worker Generation", w.Generation, int64(2))
	eq(t, "worker EngineSocket", w.EngineSocket, socketOf0)
	eq(t, "worker Health while starting", w.Health, (*agentapi.Health)(nil))
	key := in.Key

	// One nanosecond short of the delay it is still starting.
	r.clk.Advance(delay - time.Nanosecond)
	r.step()
	eq(t, "phase 1ns before the delay", r.workerOf("worker-0").Phase, agentapi.PartStarting)
	eq(t, "ServingEndpoint 1ns before the delay", r.lastInstance().ServingEndpoint, "")

	// At exactly StartTime + delay it is running.
	r.clk.Advance(time.Nanosecond)
	r.step()
	in = r.lastInstance()
	w = in.Workers[0]
	eq(t, "phase at the delay", w.Phase, agentapi.PartRunning)
	eq(t, "PID after becoming running", w.PID, 4000)
	if !w.StartTime.Equal(t0) {
		t.Errorf("StartTime after becoming running = %v, want %v", w.StartTime, t0)
	}
	eq(t, "ServingEndpoint once running", in.ServingEndpoint, "unix:/tmp/fake-job/worker-0.sock")
	eq(t, "Key stays", in.Key, key)
	if w.Health == nil {
		t.Fatal("a running worker reports no Health")
	}
	eq(t, "Health", *w.Health, agentapi.Health{Time: t0.Add(delay), ProbeOK: true, KVCacheUsage: 0.1})
}

func TestAWorkerIsReportedStartingInTheStepThatStartsItEvenWithoutADelay(t *testing.T) {
	r := newRig(t, fake.Engine{})
	r.set(run(instance()))
	r.step()
	eq(t, "phase in the starting Step", r.workerOf("worker-0").Phase, agentapi.PartStarting)
	r.step()
	eq(t, "phase in the next Step", r.workerOf("worker-0").Phase, agentapi.PartRunning)
}

func TestAMissedDesiredStateIsCorrectedByTheNextPoll(t *testing.T) {
	r := newRig(t, fake.Engine{})
	r.step() // registered, idle, one report
	r.set(run(instance()))
	boom := errors.New("path down")
	r.b.FailNext(fake.MethodDesired, boom)

	err := r.a.Step(bgndCtx)
	if !errors.Is(err, boom) {
		t.Fatalf("Step: err = %v, want boom", err)
	}
	eq(t, "reports after the missed poll", len(r.reports()), 1)
	eq(t, "len(Seen) after the missed poll", len(r.a.Seen()), 1)

	r.step()
	eq(t, "phase after the next poll", r.workerOf("worker-0").Phase, agentapi.PartStarting)
	eq(t, "PID", r.workerOf("worker-0").PID, 4000)
}

func TestAFailedReportIsResentWithoutApplyingAgain(t *testing.T) {
	r := newRig(t, fake.Engine{})
	r.set(run(instance()))
	r.b.FailNext(fake.MethodReport, errors.New("report lost"))

	if err := r.a.Step(bgndCtx); err == nil {
		t.Fatal("Step: no error")
	}
	eq(t, "reports after the lost one", len(r.reports()), 0)
	r.step()

	w := r.workerOf("worker-0")
	eq(t, "PID: the worker was not started a second time", w.PID, 4000)
	eq(t, "phase", w.Phase, agentapi.PartRunning)
	eq(t, "len(Seen)", len(r.a.Seen()), 1)
}

func TestRaisingAWorkerGenerationRestartsThatWorkerOnly(t *testing.T) {
	r := newRig(t, fake.Engine{})
	r.set(run(instance(withWorkers(0, 0))))
	r.steps(2)
	before := r.lastInstance()
	eq(t, "PIDs before", []int{before.Workers[0].PID, before.Workers[1].PID}, []int{4000, 4001})

	r.clk.Advance(5 * time.Second)
	restartedAt := r.clk.Now()
	var hookCalls int
	var inHook agentapi.WorkerReport
	var seenInHook int64
	r.a.OnDesired = func(d agentapi.DesiredState) {
		hookCalls++
		seenInHook = d.Instance.Workers[0].Generation
		// What the controller holds at this moment: no report shows it yet.
		inHook = r.workerOf("worker-0")
	}
	r.set(run(instance(withWorkers(1, 0))))
	r.step()

	eq(t, "OnDesired calls", hookCalls, 1)
	eq(t, "generation OnDesired saw", seenInHook, int64(1))
	eq(t, "worker-0 generation in the last report before the hook", inHook.Generation, int64(0))
	eq(t, "worker-0 PID in the last report before the hook", inHook.PID, 4000)

	in := r.lastInstance()
	w0, w1 := in.Workers[0], in.Workers[1]
	eq(t, "worker-0 generation applied", w0.Generation, int64(1))
	eq(t, "worker-0 new PID", w0.PID, 4002)
	eq(t, "worker-0 phase", w0.Phase, agentapi.PartStarting)
	if !w0.StartTime.Equal(restartedAt) {
		t.Errorf("worker-0 StartTime = %v, want %v", w0.StartTime, restartedAt)
	}
	eq(t, "worker-1 PID", w1.PID, 4001)
	eq(t, "worker-1 phase", w1.Phase, agentapi.PartRunning)
	eq(t, "worker-1 generation", w1.Generation, int64(0))
	if !w1.StartTime.Equal(t0) {
		t.Errorf("worker-1 StartTime = %v, want %v", w1.StartTime, t0)
	}
	eq(t, "Instance generation", in.Generation, int64(1))
	eq(t, "ServingEndpoint while worker-0 restarts", in.ServingEndpoint, "")

	r.step()
	eq(t, "worker-0 phase a Step later", r.workerOf("worker-0").Phase, agentapi.PartRunning)
	eq(t, "worker-0 PID a Step later", r.workerOf("worker-0").PID, 4002)
}

func TestRaisingTheInstanceGenerationRestartsEveryWorker(t *testing.T) {
	r := newRig(t, fake.Engine{})
	r.set(run(instance(withWorkers(0, 0))))
	r.steps(2)
	key := r.lastInstance().Key

	r.set(run(instance(withGeneration(2), withWorkers(0, 0))))
	r.step()

	in := r.lastInstance()
	eq(t, "Instance generation", in.Generation, int64(2))
	eq(t, "PIDs", []int{in.Workers[0].PID, in.Workers[1].PID}, []int{4002, 4003})
	eq(t, "phases", []agentapi.PartPhase{in.Workers[0].Phase, in.Workers[1].Phase}, []agentapi.PartPhase{agentapi.PartStarting, agentapi.PartStarting})
	eq(t, "worker generations", []int64{in.Workers[0].Generation, in.Workers[1].Generation}, []int64{0, 0})
	eq(t, "Key: the same Instance keeps its key", in.Key, key)
}

func TestANewInstanceReplacesTheOldOne(t *testing.T) {
	r := newRig(t, fake.Engine{})
	r.set(run(instance(withChecks(agentapi.Checks{VerificationGeneration: 1, Verify: &agentapi.VerifyRequest{}}))))
	r.steps(2)
	old := r.lastInstance()
	if old.Checks == nil {
		t.Fatal("the first Instance reports no checks")
	}

	r.set(run(instance(func(i *agentapi.DesiredInstance) { i.UID = "inst-uid-2" }, withGeneration(7), withWorkers(4))))
	r.step()

	in := r.lastInstance()
	eq(t, "UID", in.UID, "inst-uid-2")
	eq(t, "Generation", in.Generation, int64(7))
	if len(in.Workers) != 1 || in.Workers[0].PID != 4001 || in.Workers[0].Generation != 4 || in.Workers[0].Phase != agentapi.PartStarting {
		t.Errorf("workers = %+v, want the new Instance's one worker, PID 4001, generation 4, starting", in.Workers)
	}
	if in.Key == old.Key || !hex64.MatchString(in.Key) {
		t.Errorf("Key = %q, old %q: want a new 64-hex key", in.Key, old.Key)
	}
	if in.Checks != nil {
		t.Errorf("the old Instance's checks carried over: %+v", in.Checks)
	}
}

func TestTheKeyIsSentUntilKeyStoredAndNeverAfter(t *testing.T) {
	r := newRig(t, fake.Engine{})
	r.set(run(instance()))
	r.steps(3)
	before := r.reports()
	key := before[0].Instance.Key
	if !hex64.MatchString(key) {
		t.Fatalf("Key = %q, want 64 hex characters", key)
	}
	for n, rep := range before {
		if rep.Instance.Key != key {
			t.Errorf("report %d: Key = %q, want %q", n, rep.Instance.Key, key)
		}
	}

	r.set(run(instance(withKeyStored)))
	r.steps(3)
	reports := r.reports()
	eq(t, "reports", len(reports), 6)
	for n, rep := range reports[3:] {
		if rep.Instance.Key != "" {
			t.Errorf("report %d after KeyStored carries a key", n+3)
		}
	}
	// And the reports sent before KeyStored still carry it.
	eq(t, "an earlier report", reports[2].Instance.Key, key)
}

func TestOnDesiredIsCalledOncePerVersion(t *testing.T) {
	r := newRig(t, fake.Engine{})
	var got []agentapi.DesiredState
	r.a.OnDesired = func(d agentapi.DesiredState) { got = append(got, d) }
	r.steps(3)
	eq(t, "calls for an unchanged state", len(got), 1)
	r.set(run(instance()))
	r.steps(2)
	r.set(run(instance(withGeneration(2))))
	r.steps(2)
	eq(t, "calls after two changes", len(got), 3)
	eq(t, "Seen versions match the hook's", func() []string {
		var v []string
		for _, d := range r.a.Seen() {
			v = append(v, d.Version)
		}
		return v
	}(), func() []string {
		var v []string
		for _, d := range got {
			v = append(v, d.Version)
		}
		return v
	}())
	eq(t, "Instance generation the hook saw last", got[2].Instance.Generation, int64(2))
}

func TestAMalformedDesiredStateChangesNothing(t *testing.T) {
	r := newRig(t, fake.Engine{})
	r.step()
	r.set(agentapi.DesiredState{Action: agentapi.ActionRun}) // run, no Instance
	if err := r.a.Step(bgndCtx); err == nil {
		t.Error("run without an Instance: no error")
	}
	eq(t, "reports", len(r.reports()), 1)
	eq(t, "len(Seen)", len(r.a.Seen()), 1)
}

// wrongProtocol answers every registration with another protocol version.
type wrongProtocol struct{ *fake.Backend }

func (w wrongProtocol) Register(ctx context.Context, c agentapi.Caller, r agentapi.RegisterRequest) (agentapi.RegisterResponse, error) {
	if _, err := w.Backend.Register(ctx, c, r); err != nil {
		return agentapi.RegisterResponse{}, err
	}
	return agentapi.RegisterResponse{ProtocolVersion: "v0"}, nil
}

func TestAnEndpointOfAnotherProtocolVersionIsAnError(t *testing.T) {
	r := newRigOn(t, fake.Engine{}, func(b *fake.Backend) agentapi.Backend { return wrongProtocol{b} })
	if err := r.a.Step(bgndCtx); err == nil {
		t.Fatal("Step: no error")
	}
	eq(t, "reports", len(r.reports()), 0)
}

func TestBackendErrorsKeepTheirCodeThroughStep(t *testing.T) {
	r := newRig(t, fake.Engine{})
	r.b.FailNext(fake.MethodRegister, agentapi.Errorf(agentapi.CodeJobMismatch, "wrong job"))
	err := r.a.Step(bgndCtx)
	wantCode(t, "Step", err, agentapi.CodeJobMismatch)
	r.step() // the next Step registers
	eq(t, "registrations", len(r.b.Registrations(allocUID)), 1)
}

func TestEngineOverridesApply(t *testing.T) {
	n := int64(0)
	e := fake.Engine{
		Health: func(now time.Time, p fake.Process) agentapi.Health {
			n++
			return agentapi.Health{ProbeOK: true, GeneratedTokens: n * 100} // no Time
		},
		Verify: func(i agentapi.DesiredInstance, _ agentapi.VerifyRequest) agentapi.VerifyResult {
			return agentapi.VerifyResult{Error: "engine would not start"} // Done is the agent's to set
		},
		WarmUpRound: func(n int32, _ agentapi.WarmUpRequest) agentapi.WarmUpRound {
			return agentapi.WarmUpRound{Round: 99}
		},
	}
	r := newRig(t, e)
	r.set(run(instance(withChecks(agentapi.Checks{
		VerificationGeneration: 1,
		Verify:                 &agentapi.VerifyRequest{},
		WarmUp:                 &agentapi.WarmUpRequest{TargetSessions: 1},
	}))))
	r.steps(2) // the worker starts; then it runs, is sampled and checked

	w := r.workerOf("worker-0")
	if w.Health == nil {
		t.Fatal("no Health")
	}
	eq(t, "GeneratedTokens from the engine", w.Health.GeneratedTokens, int64(100))
	if !w.Health.Time.Equal(t0) {
		t.Errorf("Health.Time = %v, want the Step's time %v", w.Health.Time, t0)
	}
	checks := r.lastInstance().Checks
	eq(t, "Verify", checks.Verify, &agentapi.VerifyResult{Done: true, Error: "engine would not start"})
	eq(t, "the agent numbers the warm-up rounds", checks.WarmUp.Rounds[0].Round, int32(1))
	r.step()
	eq(t, "second round", r.lastInstance().Checks.WarmUp.Rounds[1].Round, int32(2))
}
