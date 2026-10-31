package fake_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

var shapes = agentapi.Shapes{
	ContextShort: 128, ContextTypical: 1024, ContextLongest: 4096,
	OutputTypical: 256, OutputLongest: 1024,
}

func checksOf(t *testing.T, r *rig) agentapi.CheckResults {
	t.Helper()
	c := r.lastInstance().Checks
	if c == nil {
		t.Fatal("the last report has no checks")
	}
	return *c
}

func TestVerifyResultsCarryTheVerificationGenerationAndTheEngineValues(t *testing.T) {
	r := newRig(t, fake.Engine{})
	r.set(run(instance(withChecks(agentapi.Checks{
		VerificationGeneration: 4,
		Verify: &agentapi.VerifyRequest{Files: []agentapi.FileDigest{
			{Path: "config.json", SHA256: "aa"},
			{Path: "model-00001.safetensors", SHA256: "bb"},
		}},
	}))))
	r.steps(2) // the worker starts, then runs and is checked

	c := checksOf(t, r)
	eq(t, "VerificationGeneration", c.VerificationGeneration, int64(4))
	eq(t, "Verify", c.Verify, &agentapi.VerifyResult{
		Done: true,
		Files: []agentapi.FileResult{
			{Path: "config.json", Outcome: agentapi.FileMatch},
			{Path: "model-00001.safetensors", Outcome: agentapi.FileMatch},
		},
		EngineVersion:    "0.32.0",
		ImageDigest:      imageSHA,
		Settings:         map[string]string{"max-model-len": "32768"},
		TemplateOverhead: 12,
		Fingerprint:      "fp-example",
	})
	eq(t, "Qualify, not asked for", c.Qualify, (*agentapi.QualifyResult)(nil))
	eq(t, "WarmUp, not asked for", c.WarmUp, (*agentapi.WarmUpResult)(nil))
}

func TestANewVerificationGenerationDropsTheOldResultsAndRunsAgain(t *testing.T) {
	verifies := 0
	qualifies := 0
	e := fake.Engine{
		Verify: func(i agentapi.DesiredInstance, req agentapi.VerifyRequest) agentapi.VerifyResult {
			verifies++
			v := fake.DefaultVerify(i, req)
			v.Fingerprint = fmt.Sprintf("fp-run-%d", verifies)
			return v
		},
		Qualify: func(i agentapi.DesiredInstance, req agentapi.QualifyRequest) agentapi.QualifyResult {
			qualifies++
			return fake.DefaultQualify(i, req)
		},
	}
	r := newRig(t, e)
	both := agentapi.Checks{VerificationGeneration: 1, Verify: &agentapi.VerifyRequest{}, Qualify: &agentapi.QualifyRequest{}}
	r.set(run(instance(withChecks(both))))
	r.steps(3)
	eq(t, "Verify runs once per generation, however many Steps", verifies, 1)
	eq(t, "Qualify runs once per generation", qualifies, 1)
	c := checksOf(t, r)
	eq(t, "generation 1: Fingerprint", c.Verify.Fingerprint, "fp-run-1")

	// Generation 2 asks for qualification only: generation 1's verification is
	// no longer reported.
	r.set(run(instance(withChecks(agentapi.Checks{VerificationGeneration: 2, Qualify: &agentapi.QualifyRequest{}}))))
	r.step()
	c = checksOf(t, r)
	eq(t, "generation 2: VerificationGeneration", c.VerificationGeneration, int64(2))
	eq(t, "generation 2: the old Verify is dropped", c.Verify, (*agentapi.VerifyResult)(nil))
	if c.Qualify == nil || !c.Qualify.Done {
		t.Errorf("generation 2: Qualify = %+v, want a fresh, done result", c.Qualify)
	}
	eq(t, "Qualify ran again for the new generation", qualifies, 2)

	// Generation 3 asks to verify again: a new run, not generation 1's result.
	r.set(run(instance(withChecks(agentapi.Checks{VerificationGeneration: 3, Verify: &agentapi.VerifyRequest{}}))))
	r.step()
	c = checksOf(t, r)
	eq(t, "generation 3: Fingerprint", c.Verify.Fingerprint, "fp-run-2")
	eq(t, "generation 3: the old Qualify is dropped", c.Qualify, (*agentapi.QualifyResult)(nil))

	// The earlier reports kept what they said.
	reports := r.reports()
	eq(t, "the first report, from the Step that started the worker, has no Verify", reports[0].Instance.Checks.Verify, (*agentapi.VerifyResult)(nil))
	eq(t, "the second report's Verify", reports[1].Instance.Checks.Verify.Fingerprint, "fp-run-1")
}

func TestQualifyPassesTheChecksTheAgentJudges(t *testing.T) {
	cases := []struct {
		name      string
		toolCalls bool
		want      []agentapi.CheckName
	}{
		{"without tool calls", false, []agentapi.CheckName{"health", "chat", "corruption", "behaviour"}},
		{"with tool calls", true, []agentapi.CheckName{"health", "chat", "toolCall", "corruption", "behaviour"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := newRig(t, fake.Engine{})
			r.set(run(instance(withChecks(agentapi.Checks{
				VerificationGeneration: 1,
				Qualify:                &agentapi.QualifyRequest{Shapes: shapes, ToolCalls: c.toolCalls},
			}))))
			r.steps(2) // the worker starts, then runs and is checked

			q := checksOf(t, r).Qualify
			if q == nil {
				t.Fatal("no Qualify result")
			}
			var names []agentapi.CheckName
			for _, o := range q.Checks {
				names = append(names, o.Name)
				if !o.Passed {
					t.Errorf("check %s did not pass", o.Name)
				}
			}
			eq(t, "checks", names, c.want)
			eq(t, "Done", q.Done, true)
			eq(t, "KVCapacityTokens", q.KVCapacityTokens, int64(1048576))
			eq(t, "ServedModelNames", q.ServedModelNames, []string{"example-model"})
		})
	}
}

func warmUpChecks(w agentapi.WarmUpRequest) func(*agentapi.DesiredInstance) {
	return withChecks(agentapi.Checks{VerificationGeneration: 1, WarmUp: &w})
}

func TestWarmUpRunsOneRoundPerStepAndStopsAtFiveWithDone(t *testing.T) {
	r := newRig(t, fake.Engine{})
	r.set(run(instance(warmUpChecks(agentapi.WarmUpRequest{Shapes: shapes, ToolCalls: true, TargetSessions: 3}))))

	r.steps(8)
	reports := r.reports()
	// Report 0 is the Step that starts the worker: no round runs before it is
	// running.
	eq(t, "report 0: checks carry the generation", reports[0].Instance.Checks.VerificationGeneration, int64(1))
	eq(t, "report 0: no warm-up result", reports[0].Instance.Checks.WarmUp, (*agentapi.WarmUpResult)(nil))
	for n := 1; n < len(reports); n++ {
		w := reports[n].Instance.Checks.WarmUp
		wantRounds := min(n, agentapi.MaxWarmUpRounds)
		if w == nil || len(w.Rounds) != wantRounds {
			t.Fatalf("report %d: warm-up = %+v, want %d rounds", n, w, wantRounds)
		}
		for i, round := range w.Rounds {
			eq(t, fmt.Sprintf("report %d round %d number", n, i), round.Round, int32(i+1))
		}
		eq(t, fmt.Sprintf("report %d Done", n), w.Done, n >= 5)
	}

	// Round 1, as HBEP-7 describes a round: one request each at short, typical
	// and longest context with the typical output, one at the typical context
	// with the longest output, one tool call, then the concurrent sessions.
	got := reports[1].Instance.Checks.WarmUp.Rounds[0].Requests
	type req struct {
		shape agentapi.Shape
		out   int32
	}
	want := []req{
		{"short", 256}, {"typical", 256}, {"longest", 256}, {"longOutput", 1024}, {"toolCall", 256},
		{"concurrent", 256}, {"concurrent", 256}, {"concurrent", 256},
	}
	if len(got) != len(want) {
		t.Fatalf("round 1 has %d requests, want %d", len(got), len(want))
	}
	for i, rq := range got {
		eq(t, fmt.Sprintf("request %d (shape, output)", i), req{rq.Shape, rq.OutputTokens}, want[i])
		eq(t, fmt.Sprintf("request %d TimeToFirstToken", i), time.Duration(rq.TimeToFirstToken), 100*time.Millisecond)
		eq(t, fmt.Sprintf("request %d Total", i), time.Duration(rq.Total), 2*time.Second)
	}
}

func TestWarmUpWithoutToolCallsSendsNoToolCallRequest(t *testing.T) {
	r := newRig(t, fake.Engine{})
	r.set(run(instance(warmUpChecks(agentapi.WarmUpRequest{Shapes: shapes, TargetSessions: 2}))))
	r.steps(2) // the worker starts, then runs and round 1 is run
	var shapesSent []agentapi.Shape
	for _, rq := range checksOf(t, r).WarmUp.Rounds[0].Requests {
		shapesSent = append(shapesSent, rq.Shape)
	}
	eq(t, "shapes", shapesSent, []agentapi.Shape{"short", "typical", "longest", "longOutput", "concurrent", "concurrent"})
}

func TestWarmUpStopsWhenTheDesiredStateNoLongerAsksForIt(t *testing.T) {
	for _, tc := range []struct {
		name  string
		after func(*agentapi.DesiredInstance)
	}{
		{"warm-up removed from the checks", withChecks(agentapi.Checks{VerificationGeneration: 1, Verify: &agentapi.VerifyRequest{}})},
		{"checks removed", func(*agentapi.DesiredInstance) {}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newRig(t, fake.Engine{})
			r.set(run(instance(warmUpChecks(agentapi.WarmUpRequest{Shapes: shapes, TargetSessions: 1}))))
			r.steps(3) // the worker starts; then rounds 1 and 2
			w := checksOf(t, r).WarmUp
			eq(t, "rounds before", len(w.Rounds), 2)
			eq(t, "Done before", w.Done, false)

			r.set(run(instance(tc.after)))
			r.steps(3)
			w = checksOf(t, r).WarmUp
			eq(t, "rounds after: no more are run", len(w.Rounds), 2)
			eq(t, "Done after", w.Done, true)
			eq(t, "VerificationGeneration kept", checksOf(t, r).VerificationGeneration, int64(1))
		})
	}
}

func TestNoWarmUpResultAppearsUntilARoundRuns(t *testing.T) {
	r := newRig(t, fake.Engine{})
	r.set(run(instance(withChecks(agentapi.Checks{VerificationGeneration: 1, Verify: &agentapi.VerifyRequest{}}))))
	r.steps(2)
	eq(t, "WarmUp", checksOf(t, r).WarmUp, (*agentapi.WarmUpResult)(nil))
}
