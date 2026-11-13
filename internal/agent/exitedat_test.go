package agent_test

import (
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// An unrequested exit is reported with when it happened, by the agent's clock (WorkerReport.ExitedAt,
// #539: HBEP-7's crash correlation is judged on it); a stop the agent was told to make carries none.
func TestAnUnrequestedExitCarriesItsTimeAndAStopDoesNot(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	before := time.Now()
	h.set(h.runState("exit-after=700ms:3", 1, 0, nil))
	r := h.waitReport("worker-0 exited", 20*time.Second, func(r agentapi.Report) bool {
		w := worker0(r)
		return w != nil && w.Phase == agentapi.PartExited
	})
	after := time.Now()
	w := worker0(r)
	if w.ExitedAt == nil {
		t.Fatal("an unrequested exit's report has no ExitedAt")
	}
	if w.ExitedAt.Before(before) || w.ExitedAt.After(after) || (w.StartTime != nil && w.ExitedAt.Before(*w.StartTime)) {
		t.Errorf("ExitedAt %s, want after the start (%v) and between the run (%s) and the exit's report (%s)", w.ExitedAt, w.StartTime, before, after)
	}

	// Restarted by a raised generation, then stopped on instruction: stopped, with no ExitedAt.
	h.set(h.runState("", 1, 1, nil))
	h.waitReport("worker-0 running at generation 1", 20*time.Second, func(r agentapi.Report) bool {
		w := worker0(r)
		return w != nil && w.Phase == agentapi.PartRunning && w.Generation == 1
	})
	h.set(agentapi.DesiredState{Action: agentapi.ActionStop, Settings: agentapi.AgentSettings{StopGrace: agentapi.Duration(time.Second)}})
	select {
	case err := <-h.done:
		if err != nil {
			t.Fatalf("Run returned %v after a stop", err)
		}
		h.done <- nil // for the cleanup's wait
	case <-time.After(20 * time.Second):
		t.Fatal("the agent did not exit after a stop")
	}
	if w := worker0(mustLast(t, h)); w == nil || w.Phase != agentapi.PartStopped || w.ExitedAt != nil {
		t.Fatalf("the stopped worker's report: %+v, want stopped with no ExitedAt", w)
	}
}
