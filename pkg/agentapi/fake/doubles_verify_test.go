package fake_test

import (
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

var checkpointFiles = []agentapi.FileDigest{
	{Path: "config.json", SHA256: "aa"},
	{Path: "model-00001.safetensors", SHA256: "bb"},
	{Path: "tokenizer.json", SHA256: "cc"},
}

func verifyRig(t *testing.T, v func(agentapi.DesiredInstance, agentapi.VerifyRequest) agentapi.VerifyResult, files []agentapi.FileDigest) *rig {
	t.Helper()
	r := newRig(t, fake.Engine{Verify: v})
	r.set(run(instance(withChecks(agentapi.Checks{VerificationGeneration: 1, Verify: &agentapi.VerifyRequest{Files: files}}))))
	r.steps(2) // the worker starts, then runs and is checked
	return r
}

func TestVerifyChangedFileReportsAMismatchForThatPathOnly(t *testing.T) {
	r := verifyRig(t, fake.VerifyChangedFile("model-00001.safetensors"), checkpointFiles)
	v := checksOf(t, r).Verify
	if v == nil {
		t.Fatal("no Verify result")
	}
	eq(t, "files: path and outcome", []struct {
		P string
		O agentapi.FileOutcome
	}{{v.Files[0].Path, v.Files[0].Outcome}, {v.Files[1].Path, v.Files[1].Outcome}, {v.Files[2].Path, v.Files[2].Outcome}},
		[]struct {
			P string
			O agentapi.FileOutcome
		}{{"config.json", agentapi.FileMatch}, {"model-00001.safetensors", agentapi.FileMismatch}, {"tokenizer.json", agentapi.FileMatch}})
	if h := v.Files[1].SHA256; !hex64.MatchString(h) || h == "bb" {
		t.Errorf("the mismatch's hash %q: want a 64-hex hash that is not the record's", h)
	}
	eq(t, "the rehash is the only failure: EngineVersion", v.EngineVersion, "0.32.0")
	eq(t, "ImageDigest", v.ImageDigest, imageSHA)
	eq(t, "Settings", v.Settings, map[string]string{"max-model-len": "32768"})
	eq(t, "Done", v.Done, true)
}

func TestVerifyChangedFileMismatchesAgainAfterTheWholeInstanceRestartRestages(t *testing.T) {
	r := verifyRig(t, fake.VerifyChangedFile("model-00001.safetensors"), checkpointFiles)
	first := checksOf(t, r).Verify.Files[1]

	// The controller restarts the Instance (generation 2) and verifies again,
	// under verification generation 2: the file is still changed.
	r.set(run(instance(withGeneration(2), withChecks(agentapi.Checks{VerificationGeneration: 2, Verify: &agentapi.VerifyRequest{Files: checkpointFiles}}))))
	r.steps(2) // the restarted worker starts, then runs and is checked
	c := checksOf(t, r)
	eq(t, "VerificationGeneration", c.VerificationGeneration, int64(2))
	eq(t, "the same file mismatches again", c.Verify.Files[1], first)
	eq(t, "outcome", c.Verify.Files[1].Outcome, agentapi.FileMismatch)
}

func TestVerifyChangedFileFlagsNothingTheRequestDoesNotList(t *testing.T) {
	// A worker restart's request lists no files: settings only.
	r := verifyRig(t, fake.VerifyChangedFile("model-00001.safetensors"), nil)
	v := checksOf(t, r).Verify
	eq(t, "Files", v.Files, []agentapi.FileResult(nil))
	eq(t, "EngineVersion", v.EngineVersion, "0.32.0")

	r = verifyRig(t, fake.VerifyChangedFile("not-in-the-record.bin"), checkpointFiles)
	for _, f := range checksOf(t, r).Verify.Files {
		eq(t, f.Path+" outcome", f.Outcome, agentapi.FileMatch)
	}
}

func TestVerifyMaxModelLenReportsAnotherValueAndNothingElseChanges(t *testing.T) {
	r := verifyRig(t, fake.VerifyMaxModelLen("16384"), checkpointFiles)
	v := checksOf(t, r).Verify
	eq(t, "Settings", v.Settings, map[string]string{"max-model-len": "16384"})
	eq(t, "EngineVersion", v.EngineVersion, "0.32.0")
	eq(t, "ImageDigest", v.ImageDigest, imageSHA)
	for _, f := range v.Files {
		eq(t, f.Path+" outcome", f.Outcome, agentapi.FileMatch)
	}
	eq(t, "Done", v.Done, true)
}

func TestVerifySettingsAddsToTheDefaultsAndCopiesItsArgument(t *testing.T) {
	overrides := map[string]string{"tensor-parallel-size": "2", "max-model-len": "8192"}
	f := fake.VerifySettings(overrides)
	overrides["tensor-parallel-size"] = "tampered"

	r := verifyRig(t, f, nil)
	eq(t, "Settings", checksOf(t, r).Verify.Settings, map[string]string{"max-model-len": "8192", "tensor-parallel-size": "2"})
	eq(t, "DefaultVerify is untouched", fake.DefaultVerify(instance(), agentapi.VerifyRequest{}).Settings, map[string]string{"max-model-len": "32768"})
}

func TestVerifyReportsNothingReportsNoVersionDigestOrSettings(t *testing.T) {
	r := verifyRig(t, fake.VerifyReportsNothing, checkpointFiles)
	eq(t, "Verify", checksOf(t, r).Verify, &agentapi.VerifyResult{
		Done: true,
		Files: []agentapi.FileResult{
			{Path: "config.json", Outcome: agentapi.FileMatch},
			{Path: "model-00001.safetensors", Outcome: agentapi.FileMatch},
			{Path: "tokenizer.json", Outcome: agentapi.FileMatch},
		},
	})
}

func qualifyOutcomes(t *testing.T, e fake.Engine, toolCalls bool) agentapi.QualifyResult {
	t.Helper()
	r := newRig(t, e)
	r.set(run(instance(withChecks(agentapi.Checks{
		VerificationGeneration: 1,
		Qualify:                &agentapi.QualifyRequest{Shapes: shapes, ToolCalls: toolCalls},
	}))))
	r.steps(2) // the worker starts, then runs and is checked
	q := checksOf(t, r).Qualify
	if q == nil {
		t.Fatal("no Qualify result")
	}
	return *q
}

// passedByName maps each outcome's name to whether it passed.
func passedByName(q agentapi.QualifyResult) map[agentapi.CheckName]bool {
	out := map[agentapi.CheckName]bool{}
	for _, o := range q.Checks {
		out[o.Name] = o.Passed
	}
	return out
}

func TestQualifyKVCapacityReportsTheGivenCapacityAndPassesTheRest(t *testing.T) {
	q := qualifyOutcomes(t, fake.Engine{Qualify: fake.QualifyKVCapacity(1000)}, false)
	eq(t, "KVCapacityTokens", q.KVCapacityTokens, int64(1000))
	eq(t, "outcomes", passedByName(q), map[agentapi.CheckName]bool{"health": true, "chat": true, "corruption": true, "behaviour": true})
	eq(t, "ServedModelNames", q.ServedModelNames, []string{"example-model"})
}

func TestQualifyLoopAtLongContextFailsTheBehaviourCheckOnly(t *testing.T) {
	for _, toolCalls := range []bool{false, true} {
		q := qualifyOutcomes(t, fake.Engine{Qualify: fake.QualifyLoopAtLongContext}, toolCalls)
		for _, o := range q.Checks {
			if o.Name == agentapi.CheckBehaviour {
				eq(t, "behaviour passed", o.Passed, false)
				eq(t, "behaviour reason", o.Reason, "Loop")
				if o.Message == "" {
					t.Error("the failed behaviour check says nothing")
				}
			} else {
				eq(t, string(o.Name)+" passed", o.Passed, true)
				eq(t, string(o.Name)+" reason", o.Reason, "")
			}
		}
		eq(t, "KVCapacityTokens is the default's", q.KVCapacityTokens, int64(1<<20))
	}
}

func TestQualifyReasoningBudgetExhaustedFailsTheBehaviourCheckWithItsReason(t *testing.T) {
	q := qualifyOutcomes(t, fake.Engine{Qualify: fake.QualifyReasoningBudgetExhausted}, false)
	for _, o := range q.Checks {
		if o.Name == agentapi.CheckBehaviour {
			eq(t, "behaviour passed", o.Passed, false)
			eq(t, "behaviour reason", o.Reason, "ReasoningBudgetExhausted")
			if o.Message == "" || o.Message == o.Reason {
				t.Errorf("the message %q is not human text apart from the reason", o.Message)
			}
		} else {
			eq(t, string(o.Name)+" passed", o.Passed, true)
			eq(t, string(o.Name)+" reason", o.Reason, "")
		}
	}
}

// Only the behaviour check reports the length it ran at: by default the
// request's longest context (4096 in these shapes), and the length the double
// is given otherwise.
func TestBehaviourCheckReportsTheContextItRanAt(t *testing.T) {
	contexts := func(q agentapi.QualifyResult) map[agentapi.CheckName]int64 {
		out := map[agentapi.CheckName]int64{}
		for _, o := range q.Checks {
			out[o.Name] = o.ContextTokens
		}
		return out
	}
	eq(t, "default", contexts(qualifyOutcomes(t, fake.Engine{}, false)),
		map[agentapi.CheckName]int64{"health": 0, "chat": 0, "corruption": 0, "behaviour": 4096})
	eq(t, "shortened", contexts(qualifyOutcomes(t, fake.Engine{Qualify: fake.QualifyBehaviourContext(3060)}, false)),
		map[agentapi.CheckName]int64{"health": 0, "chat": 0, "corruption": 0, "behaviour": 3060})
	eq(t, "still passing", passedByName(qualifyOutcomes(t, fake.Engine{Qualify: fake.QualifyBehaviourContext(3060)}, false)),
		map[agentapi.CheckName]bool{"health": true, "chat": true, "corruption": true, "behaviour": true})
}

// Each corruption failure the Agent judges from the raw outputs carries its
// own reason, and the positive control carries none.
func TestTheAgentsCorruptionOutcomeCarriesTheFailuresReason(t *testing.T) {
	for _, c := range []struct {
		name string
		out  func(agentapi.DesiredInstance, agentapi.QualifyRequest) [][]int32
		want string
	}{
		{"outputs differ at the 11th token", fake.GreedyDivergingAt(10), "OutputsDiverge"},
		{"a single repeated token", fake.GreedySingleRepeatedToken, "RepeatedToken"},
		{"empty outputs", fake.GreedyEmpty, "EmptyOutput"},
		{"outputs differ only after the 32nd token (positive control)", fake.GreedyDivergingAt(32), ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			q := qualifyOutcomes(t, fake.Engine{GreedyOutputs: c.out}, false)
			for _, o := range q.Checks {
				if o.Name == agentapi.CheckCorruption {
					eq(t, "reason", o.Reason, c.want)
				} else {
					eq(t, string(o.Name)+" reason", o.Reason, "")
				}
			}
		})
	}
}

func TestTheAgentJudgesCorruptionFromTheEnginesRawOutputs(t *testing.T) {
	cases := []struct {
		name string
		out  func(agentapi.DesiredInstance, agentapi.QualifyRequest) [][]int32
		want bool
	}{
		{"outputs differ at the 11th token", fake.GreedyDivergingAt(10), false},
		{"outputs differ at the 1st token", fake.GreedyDivergingAt(0), false},
		{"outputs differ at the 32nd token", fake.GreedyDivergingAt(31), false},
		{"outputs differ only after the 32nd token (positive control)", fake.GreedyDivergingAt(32), true},
		{"outputs differ at the 41st token", fake.GreedyDivergingAt(40), true},
		{"outputs are identical", fake.DefaultGreedyOutputs, true},
		{"a single repeated token", fake.GreedySingleRepeatedToken, false},
		{"empty outputs", fake.GreedyEmpty, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			q := qualifyOutcomes(t, fake.Engine{GreedyOutputs: c.out}, true)
			var names []agentapi.CheckName
			for _, o := range q.Checks {
				names = append(names, o.Name)
				if o.Name == agentapi.CheckCorruption {
					eq(t, "corruption passed", o.Passed, c.want)
					if !c.want && o.Message == "" {
						t.Error("the failed corruption check says nothing")
					}
				} else {
					eq(t, string(o.Name)+" passed", o.Passed, true)
				}
			}
			eq(t, "the corruption outcome replaces Qualify's: checks", names,
				[]agentapi.CheckName{"health", "chat", "toolCall", "corruption", "behaviour"})
			eq(t, "KVCapacityTokens is Qualify's", q.KVCapacityTokens, int64(1<<20))
		})
	}
}

func TestGreedyDoublesReturnTheRawTokensTheirNamesPromise(t *testing.T) {
	// Where the three outputs first differ, found by hand here and not by the
	// Agent's rule.
	firstDifference := func(outs [][]int32) int {
		for k := range outs[0] {
			if outs[1][k] != outs[0][k] || outs[2][k] != outs[0][k] {
				return k
			}
		}
		return -1
	}
	req := agentapi.QualifyRequest{}
	for _, at := range []int{0, 10, 31, 32, 40} {
		outs := fake.GreedyDivergingAt(at)(instance(), req)
		eq(t, "number of outputs", len(outs), 3)
		eq(t, "length of output 1", len(outs[0]), 64)
		eq(t, "first difference of GreedyDivergingAt", firstDifference(outs), at)
	}
	eq(t, "DefaultGreedyOutputs never differ", firstDifference(fake.DefaultGreedyOutputs(instance(), req)), -1)
	rep := fake.GreedySingleRepeatedToken(instance(), req)
	eq(t, "single repeated token: outputs", len(rep), 3)
	for _, o := range rep {
		eq(t, "length", len(o), 64)
		for _, tok := range o {
			eq(t, "token", tok, int32(7))
		}
	}
	empty := fake.GreedyEmpty(instance(), req)
	eq(t, "empty: outputs", len(empty), 3)
	for _, o := range empty {
		eq(t, "length", len(o), 0)
	}
}

func TestTheAgentsCorruptionVerdictWinsOverAQualifyOutcomeAndOnlyWhenOutputsAreGiven(t *testing.T) {
	corrupt := func(passed bool) func(agentapi.DesiredInstance, agentapi.QualifyRequest) agentapi.QualifyResult {
		return func(i agentapi.DesiredInstance, req agentapi.QualifyRequest) agentapi.QualifyResult {
			q := fake.DefaultQualify(i, req)
			for n := range q.Checks {
				if q.Checks[n].Name == agentapi.CheckCorruption {
					q.Checks[n].Passed = passed
				}
			}
			return q
		}
	}
	t.Run("raw outputs override a passing outcome", func(t *testing.T) {
		q := qualifyOutcomes(t, fake.Engine{Qualify: corrupt(true), GreedyOutputs: fake.GreedySingleRepeatedToken}, false)
		eq(t, "corruption", passedByName(q)["corruption"], false)
	})
	t.Run("raw outputs override a failing outcome", func(t *testing.T) {
		q := qualifyOutcomes(t, fake.Engine{Qualify: corrupt(false), GreedyOutputs: fake.DefaultGreedyOutputs}, false)
		eq(t, "corruption", passedByName(q)["corruption"], true)
	})
	t.Run("without raw outputs Qualify's own outcome stands", func(t *testing.T) {
		q := qualifyOutcomes(t, fake.Engine{Qualify: corrupt(false)}, false)
		eq(t, "corruption", passedByName(q)["corruption"], false)
	})
	t.Run("an outcome Qualify left out is added", func(t *testing.T) {
		omit := func(agentapi.DesiredInstance, agentapi.QualifyRequest) agentapi.QualifyResult {
			return agentapi.QualifyResult{Checks: []agentapi.CheckOutcome{{Name: agentapi.CheckHealth, Passed: true}}}
		}
		q := qualifyOutcomes(t, fake.Engine{Qualify: omit, GreedyOutputs: fake.GreedyEmpty}, false)
		eq(t, "outcomes", passedByName(q), map[agentapi.CheckName]bool{"health": true, "corruption": false})
	})
	t.Run("the engine receives the request", func(t *testing.T) {
		var got []agentapi.QualifyRequest
		e := fake.Engine{GreedyOutputs: func(i agentapi.DesiredInstance, req agentapi.QualifyRequest) [][]int32 {
			got = append(got, req)
			return fake.DefaultGreedyOutputs(i, req)
		}}
		qualifyOutcomes(t, e, true)
		eq(t, "requests", got, []agentapi.QualifyRequest{{Shapes: shapes, ToolCalls: true}})
	})
}
