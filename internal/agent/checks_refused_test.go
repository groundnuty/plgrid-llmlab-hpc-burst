package agent

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/procgroup"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// refusedEngine counts the calls that would send a check request.
type refusedEngine struct {
	Engine // nil: runChecks calls only the three methods below
	calls  *atomic.Int32
}

func (e refusedEngine) Verify(context.Context, agentapi.Sampling) agentapi.VerifyResult {
	e.calls.Add(1)
	return agentapi.VerifyResult{}
}

func (e refusedEngine) Qualify(context.Context, agentapi.QualifyRequest, agentapi.Sampling) agentapi.QualifyResult {
	e.calls.Add(1)
	return agentapi.QualifyResult{}
}

func (e refusedEngine) WarmUpRound(context.Context, int32, agentapi.WarmUpRequest, agentapi.Sampling) agentapi.WarmUpRound {
	e.calls.Add(1)
	return agentapi.WarmUpRound{}
}

// Once a verification generation's file is refused, the loop starts no check
// unit in it, though the desired state still asks for qualification and
// warm-up (runChecks' refused-generation gate; PR #169 review, Opus N1). The
// units would end at once with no result, since a refused generation sends
// nothing, and the loop would start the next, again and again: up to 599
// empty qualify units in the half second of the end-to-end test. Production
// does not reach it (the controller asks for qualification only after a
// verification that passed), so the gate is defensive and is pinned here with
// runChecks itself, the way the loop calls it, over a staged and running
// worker. The wait is on the unit's own completion event, never on a clock.
func TestARefusedGenerationStartsNoUnit(t *testing.T) {
	shapes := agentapi.Shapes{}
	for _, tc := range []struct {
		name   string
		checks agentapi.Checks
	}{
		{"verification, qualification and warm-up asked", agentapi.Checks{
			VerificationGeneration: 1, Verify: &agentapi.VerifyRequest{}, Qualify: &agentapi.QualifyRequest{Shapes: shapes},
			WarmUp: &agentapi.WarmUpRequest{Shapes: shapes, TargetSessions: 1}}},
		{"qualification and warm-up asked after a verification the controller no longer asks for", agentapi.Checks{
			VerificationGeneration: 1, Qualify: &agentapi.QualifyRequest{Shapes: shapes},
			WarmUp: &agentapi.WarmUpRequest{Shapes: shapes, TargetSessions: 1}}},
		{"warm-up alone asked", agentapi.Checks{
			VerificationGeneration: 1, WarmUp: &agentapi.WarmUpRequest{Shapes: shapes, TargetSessions: 1}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			writeGenerationConfig(t, dir, `{"temperature": 2.5}`)
			ctx, cancel := context.WithCancel(context.Background())
			var calls atomic.Int32
			a := &Agent{
				ctx: ctx, log: slog.New(slog.NewTextHandler(io.Discard, nil)), events: make(chan func(), 64),
				o: Options{Engines: func(string, string, agentapi.Launch) Engine { return refusedEngine{calls: &calls} }},
			}
			defer func() { cancel(); a.wg.Wait() }()
			in := &instance{
				launch: testLaunch(), staged: true, ckptDir: dir, generation: 1,
				workers: []*worker{{name: agentapi.WorkerName(0), phase: agentapi.PartRunning, proc: &procgroup.Group{}, generation: 1, instGen: 1}},
			}
			a.inst = in
			i := agentapi.DesiredInstance{
				Generation: 1, Workers: []agentapi.DesiredWorker{{Name: agentapi.WorkerName(0), Generation: 1}}, Checks: &tc.checks,
			}
			// Until the unit that reads the file has ended, a unit may start; each
			// ends by itself with the refusal, and the loop takes its event.
			for n := 0; !in.sampling.refused(); n++ {
				if n > 3 {
					t.Fatal("the generation's file was not refused after 3 units")
				}
				a.runChecks(in, i)
				if in.unit == nil {
					continue
				}
				select {
				case f := <-a.events:
					f()
				case <-time.After(20 * time.Second):
					t.Fatal("the unit that reads the file did not end")
				}
			}
			// From now on the refused generation starts nothing, however often
			// the loop converges.
			for n := range 20 {
				a.runChecks(in, i)
				if in.unit != nil {
					t.Fatalf("pass %d started a %s unit in a refused generation", n, in.unit.kind)
				}
			}
			if n := calls.Load(); n != 0 {
				t.Errorf("%d calls reached the engine, want none", n)
			}
		})
	}
}
