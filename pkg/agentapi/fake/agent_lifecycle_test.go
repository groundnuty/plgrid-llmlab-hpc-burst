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

func TestACrashedWorkerStaysExitedUntilAGenerationRises(t *testing.T) {
	for _, code := range []int{0, 1} {
		for _, trigger := range []string{"worker", "instance"} {
			t.Run(strings.Join([]string{"exit", string(rune('0' + code)), trigger}, "-"), func(t *testing.T) {
				r := newRig(t, fake.Engine{})
				r.set(run(instance()))
				r.steps(2)

				r.a.CrashWorker("worker-0", code)
				r.step()
				w := r.workerOf("worker-0")
				eq(t, "phase", w.Phase, agentapi.PartExited)
				eq(t, "ExitCode", w.ExitCode, &code)
				eq(t, "PID", w.PID, 4000)
				eq(t, "Health of an exited worker", w.Health, (*agentapi.Health)(nil))
				eq(t, "ServingEndpoint", r.lastInstance().ServingEndpoint, "")

				// No restart on its own, however many Steps pass.
				r.steps(3)
				w = r.workerOf("worker-0")
				eq(t, "phase after more Steps", w.Phase, agentapi.PartExited)
				eq(t, "PID after more Steps", w.PID, 4000)

				// A rising generation restarts it.
				if trigger == "worker" {
					r.set(run(instance(withWorkers(1))))
				} else {
					r.set(run(instance(withGeneration(2))))
				}
				r.step()
				w = r.workerOf("worker-0")
				eq(t, "phase after the generation rises", w.Phase, agentapi.PartStarting)
				eq(t, "PID after the generation rises", w.PID, 4001)
				eq(t, "ExitCode after the restart", w.ExitCode, (*int)(nil))
			})
		}
	}
}

func TestCrashWorkerPanicsWithoutALiveWorker(t *testing.T) {
	r := newRig(t, fake.Engine{})
	mustPanic := func(what string, f func()) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Errorf("%s: no panic", what)
			}
		}()
		f()
	}
	mustPanic("no Instance yet", func() { r.a.CrashWorker("worker-0", 1) })
	r.set(run(instance()))
	r.step()
	mustPanic("unknown worker", func() { r.a.CrashWorker("worker-9", 1) })
	r.a.CrashWorker("worker-0", 1)
	mustPanic("worker already exited", func() { r.a.CrashWorker("worker-0", 1) })
}

func TestIdleStopsTheWorkersAndKeepsTheInstanceInTheReport(t *testing.T) {
	cases := map[string]struct {
		engine fake.Engine
		steps  int
		crash  bool
		want   agentapi.PartPhase
		code   *int
	}{
		"running":  {steps: 2, want: agentapi.PartStopped, code: intp(0)},
		"starting": {engine: fake.Engine{StartDelay: time.Hour}, steps: 2, want: agentapi.PartStopped, code: intp(0)},
		"exited":   {steps: 2, crash: true, want: agentapi.PartExited, code: intp(7)},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			r := newRig(t, c.engine)
			r.set(run(instance()))
			r.steps(c.steps)
			if c.crash {
				r.a.CrashWorker("worker-0", 7)
			}
			key := r.lastInstance().Key

			r.set(agentapi.DesiredState{Action: agentapi.ActionIdle})
			r.steps(2)

			in := r.lastInstance()
			eq(t, "UID: the Instance report remains", in.UID, "inst-uid-1")
			eq(t, "Key", in.Key, key)
			w := in.Workers[0]
			eq(t, "phase", w.Phase, c.want)
			eq(t, "ExitCode", w.ExitCode, c.code)
			eq(t, "Health", w.Health, (*agentapi.Health)(nil))
			eq(t, "ServingEndpoint", in.ServingEndpoint, "")
			eq(t, "PID", w.PID, 4000)
			if r.a.Exited() {
				t.Error("an idle agent exited")
			}
		})
	}
}

func TestStopStopsTheWorkersReportsOnceAndExits(t *testing.T) {
	r := newRig(t, fake.Engine{})
	r.set(run(instance()))
	r.steps(2)
	if r.a.Exited() {
		t.Fatal("Exited before Stop")
	}

	r.set(agentapi.DesiredState{Action: agentapi.ActionStop})
	r.step()
	eq(t, "Exited", r.a.Exited(), true)
	eq(t, "phase in the final report", r.workerOf("worker-0").Phase, agentapi.PartStopped)
	eq(t, "reports", len(r.reports()), 3)

	reg, des, rep := r.calls()
	for range 3 {
		if err := r.a.Step(bgndCtx); err != nil {
			t.Fatalf("Step after Stop: %v", err)
		}
	}
	reg2, des2, rep2 := r.calls()
	eq(t, "calls after Stop: (register, desired, report)", [3]int{reg2, des2, rep2}, [3]int{reg, des, rep})
	eq(t, "reports after Stop", len(r.reports()), 3)
}

func TestAReportLostAtStopIsSentAgainBeforeTheAgentExits(t *testing.T) {
	r := newRig(t, fake.Engine{})
	r.set(agentapi.DesiredState{Action: agentapi.ActionStop})
	r.b.FailNext(fake.MethodReport, errors.New("report lost"))
	if err := r.a.Step(bgndCtx); err == nil {
		t.Fatal("Step: no error")
	}
	eq(t, "Exited with the final report unsent", r.a.Exited(), false)
	r.step()
	eq(t, "Exited once the final report is sent", r.a.Exited(), true)
	eq(t, "reports", len(r.reports()), 1)
}

func TestSilenceMakesNoCallAtAll(t *testing.T) {
	r := newRig(t, fake.Engine{})
	r.set(run(instance()))
	r.steps(2)
	reg, des, rep := r.calls()
	nReports := len(r.reports())

	r.a.Silence(true)
	r.set(run(instance(withGeneration(2))))
	for range 3 {
		if err := r.a.Step(bgndCtx); err != nil {
			t.Fatalf("Step while silenced: %v", err)
		}
	}
	reg2, des2, rep2 := r.calls()
	eq(t, "calls while silenced: (register, desired, report)", [3]int{reg2, des2, rep2}, [3]int{reg, des, rep})
	eq(t, "reports while silenced", len(r.reports()), nReports)

	r.a.Silence(false)
	r.step()
	eq(t, "reports after the outage", len(r.reports()), nReports+1)
	eq(t, "the desired state set during the outage is applied", r.lastInstance().Generation, int64(2))
}

func TestForgetMakesTheAgentRegisterAgainWithTheSameAgentInstanceID(t *testing.T) {
	r := newRig(t, fake.Engine{})
	r.set(run(instance()))
	r.steps(2)
	boot := r.a.AgentInstanceID()

	r.b.Forget()
	r.step()

	regs := r.b.Registrations(allocUID)
	if len(regs) != 1 || regs[0].Caller.AgentInstanceID != boot {
		t.Fatalf("registrations after Forget = %+v, want one with agent instance ID %s", regs, boot)
	}
	reg, des, rep := r.calls()
	// Register: the first Step and the one after Forget. Desired: two Steps,
	// then the refused poll and its retry. Report: three.
	eq(t, "calls: (register, desired, report)", [3]int{reg, des, rep}, [3]int{2, 4, 3})
	w := r.workerOf("worker-0")
	eq(t, "the agent continues: PID", w.PID, 4000)
	eq(t, "the agent continues: phase", w.Phase, agentapi.PartRunning)
	eq(t, "AgentInstanceID unchanged", r.a.AgentInstanceID(), boot)
}

// forgetAfterDesired forgets every registration once, right after answering a
// poll, so the report that follows is the first call refused.
type forgetAfterDesired struct {
	*fake.Backend
	done bool
}

func (f *forgetAfterDesired) Desired(ctx context.Context, c agentapi.Caller) (agentapi.DesiredState, error) {
	d, err := f.Backend.Desired(ctx, c)
	if !f.done {
		f.done = true
		f.Backend.Forget()
	}
	return d, err
}

func TestAReportRefusedAsNotRegisteredRegistersAgainAndIsResent(t *testing.T) {
	r := newRigOn(t, fake.Engine{}, func(b *fake.Backend) agentapi.Backend { return &forgetAfterDesired{Backend: b} })
	r.step()

	eq(t, "reports", len(r.reports()), 1)
	regs := r.b.Registrations(allocUID)
	if len(regs) != 1 || regs[0].Caller.AgentInstanceID != r.a.AgentInstanceID() {
		t.Errorf("registrations = %+v, want one with the agent's agent instance ID", regs)
	}
	reg, des, rep := r.calls()
	eq(t, "calls: (register, desired, report)", [3]int{reg, des, rep}, [3]int{2, 1, 2})
}

func TestRequeueIsASecondExecutionOfTheJob(t *testing.T) {
	r := newRig(t, fake.Engine{})
	r.set(run(instance()))
	r.steps(2)
	first := r.a.AgentInstanceID()
	firstKey := r.lastInstance().Key

	r.a.Requeue()
	if r.a.AgentInstanceID() == first || !hex32.MatchString(r.a.AgentInstanceID()) {
		t.Errorf("AgentInstanceID after Requeue = %q, first %q: want a new 32-hex ID", r.a.AgentInstanceID(), first)
	}
	r.step()

	regs := r.b.Registrations(allocUID)
	if len(regs) != 2 || regs[0].Caller.AgentInstanceID != first || regs[1].Caller.AgentInstanceID != r.a.AgentInstanceID() {
		t.Fatalf("registrations = %+v, want the first agent instance ID then the new one", regs)
	}
	in := r.lastInstance()
	w := in.Workers[0]
	eq(t, "the Instance starts afresh: phase", w.Phase, agentapi.PartStarting)
	eq(t, "the Instance starts afresh: PID", w.PID, 4001)
	if in.Key == firstKey || !hex64.MatchString(in.Key) {
		t.Errorf("Key = %q, first execution's %q: want a new key", in.Key, firstKey)
	}
	eq(t, "Seen spans the requeue: the same state seen again", len(r.a.Seen()), 2)
}
