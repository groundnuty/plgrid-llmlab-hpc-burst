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

// A second execution registers and reports; the first execution's late report,
// with a much higher count, arrives. The Backend keeps the second execution's
// report, and answers the late one as accepted: not as not registered, which
// would make that agent register again and look like a third execution. The
// count is per agent instance: the second counts from 1 and is kept though the
// first reached 100.
func TestBackendDropsTheLateReportOfAFirstExecution(t *testing.T) {
	b := registered(t, agent1)
	if err := b.Report(bgndCtx, caller(agent1), stoppedAt(100, agentapi.PartRunning)); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Register(bgndCtx, caller(agent2), agentapi.RegisterRequest{RestartCount: 1}); err != nil {
		t.Fatal(err)
	}
	if err := b.Report(bgndCtx, caller(agent2), stoppedAt(1, agentapi.PartStopped)); err != nil {
		t.Fatal(err)
	}
	eq(t, "dropped before the late report", b.Dropped(allocUID), 0)

	for _, seq := range []uint64{500, 0} { // numbered, and unnumbered
		if err := b.Report(bgndCtx, caller(agent1), stoppedAt(seq, agentapi.PartExited)); err != nil {
			t.Fatalf("the first execution's late report counted %d: %v, want it accepted", seq, err)
		}
	}
	eq(t, "the phase the Backend holds", phaseOfLast(t, b), agentapi.PartStopped)
	eq(t, "reports kept", len(b.Reports(allocUID)), 2)
	eq(t, "dropped", b.Dropped(allocUID), 2)
	eq(t, "registrations: the late report registered nobody", len(b.Registrations(allocUID)), 2)
}

// A report from an agent instance that is not registered is answered not
// registered, before anything else is decided.
func TestBackendAnswersNotRegisteredBeforeItComparesCounts(t *testing.T) {
	b := registered(t, agent1)
	if err := b.Report(bgndCtx, caller(agent1), stoppedAt(5, agentapi.PartRunning)); err != nil {
		t.Fatal(err)
	}
	wantCode(t, "Report from an instance that never registered", b.Report(bgndCtx, caller(agent2), stoppedAt(1, agentapi.PartStopped)), agentapi.CodeNotRegistered)
	eq(t, "dropped", b.Dropped(allocUID), 0)
}

// The counts are in memory, like registrations: Forget, which stands for a
// controller restart, clears them with the registrations. A report delayed
// across the restart is answered not registered until the agent registers
// again, and its numbering goes on: the report sent again is kept, and an older
// one is dropped.
func TestBackendForgetClearsTheCountsAndNumberingGoesOn(t *testing.T) {
	b := registered(t, agent1)
	if err := b.Report(bgndCtx, caller(agent1), stoppedAt(50, agentapi.PartRunning)); err != nil {
		t.Fatal(err)
	}
	b.Forget()
	delayed := stoppedAt(51, agentapi.PartStopping)
	wantCode(t, "the delayed report", b.Report(bgndCtx, caller(agent1), delayed), agentapi.CodeNotRegistered)
	if _, err := b.Register(bgndCtx, caller(agent1), agentapi.RegisterRequest{}); err != nil {
		t.Fatal(err)
	}
	if err := b.Report(bgndCtx, caller(agent1), delayed); err != nil { // the agent sends it again
		t.Fatal(err)
	}
	if err := b.Report(bgndCtx, caller(agent1), stoppedAt(52, agentapi.PartStopped)); err != nil {
		t.Fatal(err)
	}
	if err := b.Report(bgndCtx, caller(agent1), stoppedAt(51, agentapi.PartExited)); err != nil {
		t.Fatal(err)
	}
	eq(t, "the phase the Backend holds", phaseOfLast(t, b), agentapi.PartStopped)
	eq(t, "dropped", b.Dropped(allocUID), 1)
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

// An agent built before the count existed sends no Seq (0): its reports are
// kept in arrival order, as before, and they leave the count held for the
// instance alone.
func TestBackendKeepsUnnumberedReportsInArrivalOrder(t *testing.T) {
	b := registered(t, agent1)
	for _, phase := range []agentapi.PartPhase{agentapi.PartRunning, agentapi.PartStopped} {
		if err := b.Report(bgndCtx, caller(agent1), stoppedAt(0, phase)); err != nil {
			t.Fatal(err)
		}
	}
	eq(t, "reports kept", len(b.Reports(allocUID)), 2)
	eq(t, "the phase the Backend holds", phaseOfLast(t, b), agentapi.PartStopped)
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
