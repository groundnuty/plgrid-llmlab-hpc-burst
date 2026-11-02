package fake_test

import (
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

// The Backend keeps a report only if it is newer than the newest it holds for
// the same agent instance (HBEP-7, Report order; issue #221). The reports below
// differ in content, so that "which one is kept" shows in what the Backend
// holds, and none of the cases sends its reports in order only: a fixture that
// does proves nothing, since it passes with or without the rule.

func stoppedAt(seq uint64, phase agentapi.PartPhase) agentapi.Report {
	return agentapi.Report{Seq: seq, Time: t0, Instance: &agentapi.InstanceReport{
		UID: "inst-uid-1", Workers: []agentapi.WorkerReport{{Name: "worker-0", Phase: phase}},
	}}
}

func phaseOfLast(t *testing.T, b *fake.Backend) agentapi.PartPhase {
	t.Helper()
	r, ok := b.LastReport(allocUID)
	if !ok || r.Instance == nil || len(r.Instance.Workers) == 0 {
		t.Fatalf("the Backend holds no report of a worker: %+v", r)
	}
	return r.Instance.Workers[0].Phase
}

func registered(t *testing.T, ids ...string) *fake.Backend {
	t.Helper()
	b := fake.NewBackend()
	for _, id := range ids {
		if _, err := b.Register(bgndCtx, caller(id), agentapi.RegisterRequest{}); err != nil {
			t.Fatal(err)
		}
	}
	return b
}

// The last report is delivered, then an older periodic one: the Backend keeps
// the last, answers the older as accepted and counts it as dropped.
func TestBackendKeepsTheLastReportWhenAnOlderOneArrivesAfterIt(t *testing.T) {
	b := registered(t, agent1)
	if err := b.Report(bgndCtx, caller(agent1), stoppedAt(5, agentapi.PartStopped)); err != nil {
		t.Fatal(err)
	}
	if err := b.Report(bgndCtx, caller(agent1), stoppedAt(4, agentapi.PartStopping)); err != nil {
		t.Fatalf("the older report: %v, want it accepted", err)
	}
	eq(t, "the phase the Backend holds", phaseOfLast(t, b), agentapi.PartStopped)
	eq(t, "reports kept", len(b.Reports(allocUID)), 1)
	eq(t, "dropped", b.Dropped(allocUID), 1)
	eq(t, "Calls(Report)", b.Calls(fake.MethodReport), 2)
}

// A repeat of a number the Backend holds is not kept, whatever it holds: the
// answer to the first may have been lost, and the agent sends it again.
func TestBackendDropsARepeatOfTheHeldCount(t *testing.T) {
	b := registered(t, agent1)
	for _, phase := range []agentapi.PartPhase{agentapi.PartRunning, agentapi.PartStopped} {
		if err := b.Report(bgndCtx, caller(agent1), stoppedAt(3, phase)); err != nil {
			t.Fatal(err)
		}
	}
	eq(t, "the phase the Backend holds", phaseOfLast(t, b), agentapi.PartRunning)
	eq(t, "dropped", b.Dropped(allocUID), 1)
}

// The count is per agent instance: a restarted agent, with a new instance ID
// counting from 1, is kept, though the instance before it reached 100.
func TestBackendKeepsAnAgentInstanceThatCountsFromOneAgain(t *testing.T) {
	b := registered(t, agent1, agent2)
	if err := b.Report(bgndCtx, caller(agent1), stoppedAt(100, agentapi.PartStopped)); err != nil {
		t.Fatal(err)
	}
	if err := b.Report(bgndCtx, caller(agent2), stoppedAt(1, agentapi.PartRunning)); err != nil {
		t.Fatal(err)
	}
	eq(t, "the phase the Backend holds", phaseOfLast(t, b), agentapi.PartRunning)
	eq(t, "reports kept", len(b.Reports(allocUID)), 2)
	eq(t, "dropped", b.Dropped(allocUID), 0)
	// And the first instance's count is its own: 100 stays held for it.
	if err := b.Report(bgndCtx, caller(agent1), stoppedAt(99, agentapi.PartExited)); err != nil {
		t.Fatal(err)
	}
	eq(t, "dropped after an older report of the first instance", b.Dropped(allocUID), 1)
}

// The counts are in memory, like registrations: Forget, which stands for a
// controller restart, clears them with the registrations.
func TestBackendForgetClearsTheCounts(t *testing.T) {
	b := registered(t, agent1)
	if err := b.Report(bgndCtx, caller(agent1), stoppedAt(50, agentapi.PartRunning)); err != nil {
		t.Fatal(err)
	}
	b.Forget()
	if _, err := b.Register(bgndCtx, caller(agent1), agentapi.RegisterRequest{}); err != nil {
		t.Fatal(err)
	}
	if err := b.Report(bgndCtx, caller(agent1), stoppedAt(2, agentapi.PartStopped)); err != nil {
		t.Fatal(err)
	}
	eq(t, "the phase the Backend holds", phaseOfLast(t, b), agentapi.PartStopped)
	eq(t, "dropped", b.Dropped(allocUID), 0)
}

// The held count rises only when a report is kept: one the Backend refused,
// and the agent sends again with its number, is not taken for an old one.
func TestBackendDoesNotCountAReportItRefused(t *testing.T) {
	b := registered(t, agent1)
	bad := stoppedAt(7, agentapi.PartRunning)
	bad.Instance.Key = "not a key"
	wantCode(t, "Report with a bad key", b.Report(bgndCtx, caller(agent1), bad), agentapi.CodeBadRequest)
	good := stoppedAt(7, agentapi.PartStopped)
	if err := b.Report(bgndCtx, caller(agent1), good); err != nil {
		t.Fatal(err)
	}
	eq(t, "the phase the Backend holds", phaseOfLast(t, b), agentapi.PartStopped)
	eq(t, "dropped", b.Dropped(allocUID), 0)
}

// A report with no count is refused, and not kept.
func TestBackendRefusesAReportWithNoCount(t *testing.T) {
	b := registered(t, agent1)
	wantCode(t, "Report", b.Report(bgndCtx, caller(agent1), stoppedAt(0, agentapi.PartRunning)), agentapi.CodeBadRequest)
	eq(t, "reports kept", len(b.Reports(allocUID)), 0)
	eq(t, "dropped", b.Dropped(allocUID), 0)
}

// The simulated agent counts its reports from 1, one more for each, and a
// requeued execution, a new agent instance, counts from 1 again: the Backend
// keeps its first report though the instance before it reached a higher count.
func TestFakeAgentCountsItsReportsAndRestartsAtOneWhenRequeued(t *testing.T) {
	b := fake.NewBackend()
	a := fake.NewAgent(b, caller(agent1), func() time.Time { return t0 }, fake.Engine{})
	for range 4 {
		if err := a.Step(bgndCtx); err != nil {
			t.Fatal(err)
		}
	}
	reps := b.Reports(allocUID)
	eq(t, "reports", len(reps), 4)
	for i, r := range reps {
		eq(t, "the count of report "+string(rune('1'+i)), r.Seq, uint64(i+1))
	}

	a.Requeue()
	if err := a.Step(bgndCtx); err != nil {
		t.Fatal(err)
	}
	last, _ := b.LastReport(allocUID)
	eq(t, "the requeued agent's first report", last.Seq, uint64(1))
	eq(t, "reports kept", len(b.Reports(allocUID)), 5)
	eq(t, "dropped", b.Dropped(allocUID), 0)
}
