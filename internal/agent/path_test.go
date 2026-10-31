package agent_test

import (
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

// A missed desired-state message is corrected by the next poll: the answer
// that carries the run instruction is lost on the way, and the agent still
// runs the Instance, from the next answer (HBEP-7, Desired state, pulled).
func TestMissedDesiredStateIsCorrectedByTheNextPoll(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	h.api.mu.Lock()
	h.api.dropNextRun = true
	h.api.mu.Unlock()
	h.set(h.runState("", 1, 0, nil))
	h.waitReport("worker-0 running", 20*time.Second, running)
	h.api.mu.Lock()
	dropped := len(h.api.dropped)
	h.api.mu.Unlock()
	if dropped != 1 {
		t.Fatalf("%d answers dropped, want 1: the test did not lose the message", dropped)
	}
}

// States the agent never sees are skipped, and the agent converges on the
// newest: the path is down while the controller moves through two states, and
// once it returns the agent runs only the last.
func TestAgentConvergesOnTheNewestStateAfterMissingSome(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	h.set(agentapi.DesiredState{Action: agentapi.ActionIdle})
	h.waitReport("a first report", 10*time.Second, func(agentapi.Report) bool { return true })
	h.api.setCut(true)
	h.set(h.runState("", 1, 0, nil))
	time.Sleep(100 * time.Millisecond)
	h.set(h.runState("", 1, 2, nil))
	h.api.setCut(false)
	r := h.waitReport("worker-0 running at generation 2", 20*time.Second, func(r agentapi.Report) bool {
		w := worker0(r)
		return w != nil && w.Phase == agentapi.PartRunning && w.Generation == 2
	})
	if n := len(h.execs()); n != 1 {
		t.Fatalf("%d starts, want 1: the agent ran a state it never saw", n)
	}
	_ = r
}

// An agent whose path is down keeps its Instance running and stops
// reporting; nothing is restarted, and the reports resume with the same
// process when the path returns (HBEP-7, Unreachable; HBEP-6: a path that is
// down is not a dead Instance). The gap the Backend sees exceeds an agent
// timeout of 1 s, so a controller judging health.agentTimeout by the Backend's
// clock would mark the Instance unreachable; that judgement and the condition
// are the lifecycle controller's (issue 61).
func TestPathOutageKeepsTheWorkerAndReportsResume(t *testing.T) {
	const agentTimeout = time.Second
	h := newHarness(t, harnessOptions{})
	h.set(h.runState("", 1, 0, nil))
	pid := worker0(h.waitReport("worker-0 running", 20*time.Second, running)).PID

	h.api.setCut(true)
	cutAt := time.Now()
	time.Sleep(2 * agentTimeout)
	h.api.setCut(false)
	h.waitReport("a report after the outage", 10*time.Second, func(agentapi.Report) bool {
		ts := h.timed.reportTimes()
		return len(ts) > 0 && ts[len(ts)-1].After(cutAt.Add(2*agentTimeout))
	})

	var gap time.Duration
	ts := h.timed.reportTimes()
	for i := 1; i < len(ts); i++ {
		gap = max(gap, ts[i].Sub(ts[i-1]))
	}
	if gap <= agentTimeout {
		t.Fatalf("the largest gap between reports is %s, not above the %s agent timeout: the outage did not show", gap, agentTimeout)
	}
	r := mustLast(t, h)
	if w := worker0(r); w.Phase != agentapi.PartRunning || w.PID != pid {
		t.Fatalf("after the outage the worker is %+v, want the same running process %d", w, pid)
	}
	if n := len(h.execs()); n != 1 {
		t.Fatalf("%d starts: the outage restarted the worker", n)
	}
}

// After the controller restarts it no longer knows the agent: the agent's next
// call gets CodeNotRegistered, and it registers again at once with the same
// agent instance ID, without restarting anything (agentapi's documentation,
// registration is soft state).
func TestReRegistersAfterControllerRestartWithoutRestarting(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	h.set(h.runState("", 1, 0, nil))
	pid := worker0(h.waitReport("worker-0 running", 20*time.Second, running)).PID
	h.backend.Forget()
	deadline := time.Now().Add(10 * time.Second)
	for len(h.backend.Registrations(hAllocUID)) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the agent did not register again")
		}
		time.Sleep(10 * time.Millisecond)
	}
	regs := h.backend.Registrations(hAllocUID)
	if regs[0].Caller.AgentInstanceID != h.aid {
		t.Fatalf("re-registered as %q, want the same agent instance ID %q", regs[0].Caller.AgentInstanceID, h.aid)
	}
	time.Sleep(500 * time.Millisecond)
	if w := worker0(mustLast(t, h)); w.PID != pid || w.Phase != agentapi.PartRunning {
		t.Fatalf("the worker changed across the re-registration: %+v", w)
	}
	if n := h.backend.Calls(fake.MethodRegister); n < 2 {
		t.Fatalf("%d registrations in all, want at least 2", n)
	}
}
