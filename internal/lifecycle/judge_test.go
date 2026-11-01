package lifecycle

import (
	"encoding/json"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/deploymentid"
)

// The tests in this file need no rig. The declared structs and the agent's
// reports are built by hand; every expected value is a literal.

const judgeDigest = "sha256:1111111111111111111111111111111111111111111111111111111111111111"

// judgeDeclared is what the identity record declares: vLLM 0.32.0 on a digest,
// max-model-len 32768, a JSON speculative config, and two checkpoint files.
func judgeDeclared() declared {
	return declared{
		engineVersion: "0.32.0",
		imageDigest:   judgeDigest,
		flags: map[string]string{
			"max-model-len":      "32768",
			"speculative-config": `{"method":"ngram","num_speculative_tokens":3}`,
		},
		files: []deploymentid.File{
			{Path: "config.json", SHA256: strings.Repeat("1", 64)},
			{Path: "model.safetensors", SHA256: strings.Repeat("2", 64)},
		},
	}
}

// judgeResult is a verification report that matches judgeDeclared.
func judgeResult() *agentapi.VerifyResult {
	return &agentapi.VerifyResult{
		Done: true,
		Files: []agentapi.FileResult{
			{Path: "config.json", Outcome: agentapi.FileMatch},
			{Path: "model.safetensors", Outcome: agentapi.FileMatch},
		},
		EngineVersion: "0.32.0",
		ImageDigest:   judgeDigest,
		Settings: map[string]string{
			"max-model-len":      "32768",
			"speculative-config": `{"method":"ngram","num_speculative_tokens":3}`,
		},
		Fingerprint:      "fp-1",
		TemplateOverhead: 12,
	}
}

// judgeShapes are the workload shapes of the rig's deployments with the
// defaults applied: short 512, typical 4096, typical output 512, longest
// output 4096, 4 target sessions.
func judgeShapes() shapes {
	return shapes{short: 512, typical: 4096, outputTypical: 512, outputLongest: 4096, targetSessions: 4}
}

// TestJudgeVerifyPasses covers HBEP-7, Instance start, Verification: every file
// present matches, the version, digest and max-model-len are reported and equal
// the declared ones, and every shape fits the reported limit with the overhead.
func TestJudgeVerifyPasses(t *testing.T) {
	got := judgeVerify(judgeDeclared(), judgeResult(), true, judgeShapes())
	if got.Verdict != v1alpha1.VerdictPass || got.Cause != "" {
		t.Fatalf("verdict %q cause %q: %s", got.Verdict, got.Cause, got.Message)
	}
	if got.Rehash.Checked != 2 || len(got.Rehash.Mismatch) != 0 || len(got.Rehash.Absent) != 0 || got.Rehash.Skipped {
		t.Errorf("rehash = %+v, want two files checked, none wrong", got.Rehash)
	}
	if got.MaxModelLen != 32768 || got.Overhead != 12 || got.Fingerprint != "fp-1" {
		t.Errorf("maxModelLen/overhead/fingerprint = %d/%d/%q", got.MaxModelLen, got.Overhead, got.Fingerprint)
	}
	// The default longest context is 32768 - 512 - 12 = 32244.
	if got.Shapes == nil || got.Shapes.ContextLongest != 32244 {
		t.Errorf("shapes = %+v, want ContextLongest 32244", got.Shapes)
	}
	if len(got.NotReported) != 0 || len(got.Contradictions) != 0 || len(got.Undeclared) != 0 {
		t.Errorf("notReported %v contradictions %v undeclared %v, want none", got.NotReported, got.Contradictions, got.Undeclared)
	}
}

// The verification record holds the model's generation_config defaults as the
// agent reported them (science-agent rulings, 1 Nov 2026, issue #125), on a
// pass and on a failure, and none when the agent reported none.
func TestJudgeVerifyRecordsTheGenerationConfig(t *testing.T) {
	r := judgeResult()
	r.GenerationConfig = map[string]string{"repetition_penalty": "1.1", "temperature": "0.7", "top_k": "20", "top_p": "0.8"}
	got := judgeVerify(judgeDeclared(), r, true, judgeShapes())
	if got.Verdict != v1alpha1.VerdictPass || !maps.Equal(got.GenerationConfig, r.GenerationConfig) {
		t.Errorf("verdict %q, generationConfig %v, want a pass recording %v", got.Verdict, got.GenerationConfig, r.GenerationConfig)
	}
	b, err := json.Marshal(got)
	if err != nil || !strings.Contains(string(b), `"generationConfig":{"repetition_penalty":"1.1"`) {
		t.Errorf("record data %s (%v) does not carry generationConfig", b, err)
	}
	r.Files[0].Outcome = agentapi.FileMismatch
	if got := judgeVerify(judgeDeclared(), r, true, judgeShapes()); got.Verdict != v1alpha1.VerdictFail || got.GenerationConfig["top_p"] != "0.8" {
		t.Errorf("on a failed rehash: verdict %q, generationConfig %v", got.Verdict, got.GenerationConfig)
	}
	if got := judgeVerify(judgeDeclared(), judgeResult(), true, judgeShapes()); got.GenerationConfig != nil {
		t.Errorf("none reported, recorded %v", got.GenerationConfig)
	}
}

// The record carries what the agent found at generation_config.json: absent, present, or unreadable, on a
// pass and on a failure, and nothing when the agent did not say (issue #175). A present file with no keys
// and a missing file differ in the record, and only there.
func TestJudgeVerifyRecordsWhetherTheGenerationConfigFileIsThere(t *testing.T) {
	for _, file := range []string{agentapi.GenerationConfigAbsent, agentapi.GenerationConfigPresent, agentapi.GenerationConfigUnreadable} {
		r := judgeResult()
		r.GenerationConfigFile = file
		got := judgeVerify(judgeDeclared(), r, true, judgeShapes())
		b, err := json.Marshal(got)
		if err != nil || got.GenerationConfigFile != file || !strings.Contains(string(b), `"generationConfigFile":"`+file+`"`) {
			t.Errorf("%s: record data %s (%v)", file, b, err)
		}
		if got.GenerationConfig != nil || strings.Contains(string(b), `"generationConfig":`) {
			t.Errorf("%s: keys recorded when none were reported: %s", file, b)
		}
		r.Files[0].Outcome = agentapi.FileMismatch
		if got := judgeVerify(judgeDeclared(), r, true, judgeShapes()); got.Verdict != v1alpha1.VerdictFail || got.GenerationConfigFile != file {
			t.Errorf("%s on a failed rehash: verdict %q, file %q", file, got.Verdict, got.GenerationConfigFile)
		}
	}
	got := judgeVerify(judgeDeclared(), judgeResult(), true, judgeShapes())
	if b, _ := json.Marshal(got); got.GenerationConfigFile != "" || strings.Contains(string(b), "generationConfigFile") {
		t.Errorf("the agent did not say, and the record has %s", b)
	}
}

// The verification record fixes the behaviour check's context (science-agent
// ruling, 31 Oct 2026, for #88; issue #125, item 9). Without a reasoning
// parser it is the longest context: 32768 - 512 - 12 = 32244. With one it is
// #94's hand figure for Qwen3-0.6B, 8192 - 8 - 128 - 4096 = 3960 (issue #94,
// comment 6550908687, the behaviour row). After a worker restart, which runs
// no qualification, there is none.
func TestJudgeVerifyRecordsTheBehaviourContext(t *testing.T) {
	if got := judgeVerify(judgeDeclared(), judgeResult(), true, judgeShapes()); got.BehaviourContextTokens != 32244 {
		t.Errorf("no reasoning parser: %d, want 32244", got.BehaviourContextTokens)
	}
	d := judgeDeclared()
	d.flags["max-model-len"], d.flags["reasoning-parser"] = "8192", "qwen3"
	r := judgeResult()
	r.Settings["max-model-len"], r.TemplateOverhead = "8192", 8
	longest := int32(4096)
	sh := shapes{short: 128, typical: 1024, longest: &longest, outputTypical: 128, outputLongest: 512, targetSessions: 8}
	got := judgeVerify(d, r, true, sh)
	if got.Verdict != v1alpha1.VerdictPass || got.BehaviourContextTokens != 3960 {
		t.Errorf("reasoning parser: verdict %q (%s), context %d, want a pass at 3960", got.Verdict, got.Message, got.BehaviourContextTokens)
	}
	b, _ := json.Marshal(got)
	if !strings.Contains(string(b), `"behaviourContextTokens":3960`) {
		t.Errorf("record data %s does not carry behaviourContextTokens", b)
	}
	if got := judgeVerify(d, r, false, sh); got.BehaviourContextTokens != 0 {
		t.Errorf("worker restart: %d, want none", got.BehaviourContextTokens)
	}
}

// TestJudgeVerifyFileMismatch: a changed checkpoint file fails the rehash
// (HBEP-7, Test plan, "Verification").
func TestJudgeVerifyFileMismatch(t *testing.T) {
	r := judgeResult()
	r.Files[1] = agentapi.FileResult{Path: "model.safetensors", Outcome: agentapi.FileMismatch, SHA256: strings.Repeat("f", 64)}
	got := judgeVerify(judgeDeclared(), r, true, judgeShapes())
	if got.Verdict != v1alpha1.VerdictFail || got.Cause != causeRehash {
		t.Fatalf("verdict %q cause %q, want fail/%s", got.Verdict, got.Cause, causeRehash)
	}
	if !slices.Equal(got.Rehash.Mismatch, []string{"model.safetensors"}) || got.Rehash.Checked != 2 {
		t.Errorf("rehash = %+v, want model.safetensors the only mismatch", got.Rehash)
	}
	if !strings.Contains(got.Message, "model.safetensors") {
		t.Errorf("message %q does not name the file", got.Message)
	}
}

// TestJudgeVerifyAbsentFileIsNotAFailure: "A listed file that was never staged,
// because the engine does not load it, is not a failure (HBEP-3)".
func TestJudgeVerifyAbsentFileIsNotAFailure(t *testing.T) {
	r := judgeResult()
	r.Files[1] = agentapi.FileResult{Path: "model.safetensors", Outcome: agentapi.FileAbsent}
	got := judgeVerify(judgeDeclared(), r, true, judgeShapes())
	if got.Verdict != v1alpha1.VerdictPass {
		t.Fatalf("verdict %q cause %q: %s", got.Verdict, got.Cause, got.Message)
	}
	if !slices.Equal(got.Rehash.Absent, []string{"model.safetensors"}) || got.Rehash.Checked != 1 {
		t.Errorf("rehash = %+v, want one file absent and one checked", got.Rehash)
	}
}

// TestJudgeVerifyWorkerRestartSkipsTheRehash: a worker restart compares settings
// only (HBEP-7, Health and restarts, rung 1: "the settings comparison and
// warm-up run again"), so a file result the report carries is not read, even a
// mismatch.
func TestJudgeVerifyWorkerRestartSkipsTheRehash(t *testing.T) {
	r := judgeResult()
	r.Files[1].Outcome = agentapi.FileMismatch
	got := judgeVerify(judgeDeclared(), r, false, judgeShapes())
	if got.Verdict != v1alpha1.VerdictPass || !got.Rehash.Skipped {
		t.Fatalf("verdict %q skipped %v: %s", got.Verdict, got.Rehash.Skipped, got.Message)
	}
}

// TestJudgeVerifyContradictions: the engine version, the image digest and a
// declared max-model-len must equal the declared ones (HBEP-7, Instance start,
// Settings comparison). Each contradiction fails at once.
func TestJudgeVerifyContradictions(t *testing.T) {
	tests := []struct {
		name   string
		change func(*agentapi.VerifyResult)
		names  string
	}{
		// Declared 32768, reported 16384.
		{"another max-model-len", func(r *agentapi.VerifyResult) { r.Settings["max-model-len"] = "16384" }, "max-model-len"},
		{"another image digest", func(r *agentapi.VerifyResult) {
			r.ImageDigest = "sha256:2222222222222222222222222222222222222222222222222222222222222222"
		}, "image digest"},
		{"another engine version", func(r *agentapi.VerifyResult) { r.EngineVersion = "0.31.0" }, "engine version"},
		// 3 speculative tokens declared, 4 reported: a real JSON difference.
		{"another JSON value", func(r *agentapi.VerifyResult) {
			r.Settings["speculative-config"] = `{"method":"ngram","num_speculative_tokens":4}`
		}, "speculative-config"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := judgeResult()
			tc.change(r)
			got := judgeVerify(judgeDeclared(), r, true, judgeShapes())
			if got.Verdict != v1alpha1.VerdictFail || got.Cause != causeContradiction {
				t.Fatalf("verdict %q cause %q, want fail/%s", got.Verdict, got.Cause, causeContradiction)
			}
			if len(got.Contradictions) != 1 || !strings.Contains(got.Contradictions[0], tc.names) {
				t.Errorf("contradictions = %v, want one naming %q", got.Contradictions, tc.names)
			}
		})
	}
}

// TestJudgeVerifyRequiresWhatTheEngineMustReport: "an engine that reports
// nothing fails, because the version, the image digest and the context length
// are required" (HBEP-7, Test plan, "Verification"). The files all match, so
// the rehash does not fail first.
func TestJudgeVerifyRequiresWhatTheEngineMustReport(t *testing.T) {
	t.Run("nothing", func(t *testing.T) {
		r := judgeResult()
		r.EngineVersion, r.ImageDigest, r.Settings = "", "", nil
		got := judgeVerify(judgeDeclared(), r, true, judgeShapes())
		if got.Verdict != v1alpha1.VerdictFail || got.Cause != causeMissingRequired {
			t.Fatalf("verdict %q cause %q, want fail/%s", got.Verdict, got.Cause, causeMissingRequired)
		}
		for _, want := range []string{"engine version", "image digest", "max-model-len"} {
			if !strings.Contains(got.Message, want) {
				t.Errorf("message %q does not name %q", got.Message, want)
			}
		}
	})
	for _, tc := range []struct {
		name   string
		change func(*agentapi.VerifyResult)
	}{
		{"no version", func(r *agentapi.VerifyResult) { r.EngineVersion = "" }},
		{"no digest", func(r *agentapi.VerifyResult) { r.ImageDigest = "" }},
		{"no max-model-len", func(r *agentapi.VerifyResult) { delete(r.Settings, "max-model-len") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := judgeResult()
			tc.change(r)
			got := judgeVerify(judgeDeclared(), r, false, judgeShapes())
			if got.Verdict != v1alpha1.VerdictFail || got.Cause != causeMissingRequired {
				t.Fatalf("verdict %q cause %q, want fail/%s", got.Verdict, got.Cause, causeMissingRequired)
			}
		})
	}
}

// TestJudgeVerifyDeclaredInputNotReported: "A declared input it does not report
// is recorded as not reported", and the Instance still passes. "A setting it
// reports that the spec does not declare is recorded."
func TestJudgeVerifyDeclaredInputNotReported(t *testing.T) {
	r := judgeResult()
	delete(r.Settings, "speculative-config")
	r.Settings["gpu-memory-utilization"] = "0.9"
	got := judgeVerify(judgeDeclared(), r, true, judgeShapes())
	if got.Verdict != v1alpha1.VerdictPass {
		t.Fatalf("verdict %q cause %q: %s", got.Verdict, got.Cause, got.Message)
	}
	if !slices.Equal(got.NotReported, []string{"speculative-config"}) {
		t.Errorf("notReported = %v, want [speculative-config]", got.NotReported)
	}
	if got.Undeclared["gpu-memory-utilization"] != "0.9" || len(got.Undeclared) != 1 {
		t.Errorf("undeclared = %v, want gpu-memory-utilization 0.9 only", got.Undeclared)
	}
}

// TestJudgeVerifyJSONFormatting: a JSON-valued setting reported with other
// spacing is another value, since the comparison is verbatim; the declared
// text itself matches.
func TestJudgeVerifyJSONFormatting(t *testing.T) {
	// Neither a boolean nor a number, so verbatim (science-agent ruling, 31
	// Oct 2026; PR #91): the same JSON object written with other white space
	// is another value, so a contradiction.
	r := judgeResult()
	r.Settings["speculative-config"] = `{ "num_speculative_tokens": 3,
	  "method": "ngram" }`
	got := judgeVerify(judgeDeclared(), r, true, judgeShapes())
	if got.Verdict != v1alpha1.VerdictFail || got.Cause != causeContradiction {
		t.Fatalf("verdict %q cause %q: %s; want a contradiction, the comparison is verbatim", got.Verdict, got.Cause, got.Message)
	}
	// The value exactly as declared matches.
	r = judgeResult()
	got = judgeVerify(judgeDeclared(), r, true, judgeShapes())
	if got.Verdict != v1alpha1.VerdictPass {
		t.Fatalf("the declared value verbatim: verdict %q: %s", got.Verdict, got.Message)
	}
}

// TestJudgeVerifyShapeThatDoesNotFit: "At verification each rendered request,
// overhead included, is checked against the context limit the engine reports,
// and one that does not fit fails verification" (HBEP-7, What the controller
// reads). The spec declares no max-model-len, the engine reports 4096, and the
// typical context 4096 + typical output 512 + overhead 12 = 4620 > 4096.
func TestJudgeVerifyShapeThatDoesNotFit(t *testing.T) {
	d := judgeDeclared()
	delete(d.flags, "max-model-len")
	r := judgeResult()
	r.Settings["max-model-len"] = "4096"
	got := judgeVerify(d, r, true, judgeShapes())
	if got.Verdict != v1alpha1.VerdictFail || got.Cause != causeShape {
		t.Fatalf("verdict %q cause %q, want fail/%s", got.Verdict, got.Cause, causeShape)
	}
}

// judgeChecks returns the four checks the agent always judges, all passed.
func judgeChecks() []agentapi.CheckOutcome {
	return []agentapi.CheckOutcome{
		{Name: agentapi.CheckHealth, Passed: true}, {Name: agentapi.CheckChat, Passed: true},
		{Name: agentapi.CheckCorruption, Passed: true}, {Name: agentapi.CheckBehaviour, Passed: true},
	}
}

func judgeQualifyResult(kv int64) *agentapi.QualifyResult {
	return &agentapi.QualifyResult{Done: true, Checks: judgeChecks(), KVCapacityTokens: kv, ServedModelNames: []string{"example-model"}}
}

// TestJudgeQualifyKVCapacity covers HBEP-7, Instance start, Qualification: KV
// capacity from start-up of at least targetSessions times contextTokens.typical
// tokens. 4 * 4096 = 16384: 16383 fails, 16384 passes.
func TestJudgeQualifyKVCapacity(t *testing.T) {
	if ok, msg := judgeQualify(judgeQualifyResult(16384), judgeShapes(), "example-model"); !ok {
		t.Fatalf("16384 tokens fails: %s", msg)
	}
	ok, msg := judgeQualify(judgeQualifyResult(16383), judgeShapes(), "example-model")
	if ok {
		t.Fatal("16383 tokens passes")
	}
	if !strings.Contains(msg, "KV capacity") || !strings.Contains(msg, "16384") {
		t.Errorf("message %q does not say the KV capacity is short of 16384", msg)
	}
}

// TestJudgeQualifyChecks: a failed corruption check fails (Test plan,
// "Qualification"); the tool-call smoke test is required when workload.toolCalls
// is set and not otherwise; the engine must serve the deployment's model name.
func TestJudgeQualifyChecks(t *testing.T) {
	t.Run("a failed corruption check", func(t *testing.T) {
		q := judgeQualifyResult(16384)
		q.Checks[2] = agentapi.CheckOutcome{Name: agentapi.CheckCorruption, Passed: false, Message: "three greedy outputs differ within 32 tokens"}
		ok, msg := judgeQualify(q, judgeShapes(), "example-model")
		if ok || !strings.Contains(msg, "corruption") || !strings.Contains(msg, "three greedy outputs") {
			t.Fatalf("ok %v message %q, want a failure naming the corruption check and its message", ok, msg)
		}
	})
	t.Run("a missing check", func(t *testing.T) {
		q := judgeQualifyResult(16384)
		q.Checks = q.Checks[:3]
		if ok, msg := judgeQualify(q, judgeShapes(), "example-model"); ok || !strings.Contains(msg, "behaviour") {
			t.Fatalf("ok %v message %q, want a failure naming the behaviour check", ok, msg)
		}
	})
	t.Run("tool calls on, no tool-call check", func(t *testing.T) {
		sh := judgeShapes()
		sh.toolCalls = true
		ok, msg := judgeQualify(judgeQualifyResult(16384), sh, "example-model")
		if ok || !strings.Contains(msg, "toolCall") {
			t.Fatalf("ok %v message %q, want a failure naming the toolCall check", ok, msg)
		}
	})
	t.Run("tool calls on, check passed", func(t *testing.T) {
		sh := judgeShapes()
		sh.toolCalls = true
		q := judgeQualifyResult(16384)
		q.Checks = append(q.Checks, agentapi.CheckOutcome{Name: agentapi.CheckToolCall, Passed: true})
		if ok, msg := judgeQualify(q, sh, "example-model"); !ok {
			t.Fatalf("fails: %s", msg)
		}
	})
	t.Run("tool calls off, a failed tool-call check is not read", func(t *testing.T) {
		q := judgeQualifyResult(16384)
		q.Checks = append(q.Checks, agentapi.CheckOutcome{Name: agentapi.CheckToolCall, Passed: false})
		if ok, msg := judgeQualify(q, judgeShapes(), "example-model"); !ok {
			t.Fatalf("fails: %s", msg)
		}
	})
	t.Run("served names without the model", func(t *testing.T) {
		q := judgeQualifyResult(16384)
		q.ServedModelNames = []string{"other-model"}
		ok, msg := judgeQualify(q, judgeShapes(), "example-model")
		if ok || !strings.Contains(msg, "example-model") {
			t.Fatalf("ok %v message %q, want a failure naming the model", ok, msg)
		}
	})
	t.Run("the model among other names", func(t *testing.T) {
		q := judgeQualifyResult(16384)
		q.ServedModelNames = []string{"alias", "example-model"}
		if ok, msg := judgeQualify(q, judgeShapes(), "example-model"); !ok {
			t.Fatalf("fails: %s", msg)
		}
	})
}

// judgeReq builds one warm-up request's timing.
func judgeReq(shape agentapi.Shape, ttft, total time.Duration, tokens int32) agentapi.RequestTiming {
	return agentapi.RequestTiming{Shape: shape, TimeToFirstToken: agentapi.Duration(ttft), Total: agentapi.Duration(total), OutputTokens: tokens}
}

// judgeRound is a round whose short, typical, longest and long-output requests
// take the given totals, and whose four concurrent requests take 2 s each.
func judgeRound(n int32, short, typical, longest, longOutput time.Duration) agentapi.WarmUpRound {
	r := agentapi.WarmUpRound{Round: n, Requests: []agentapi.RequestTiming{
		judgeReq(agentapi.ShapeShort, 100*time.Millisecond, short, 64),
		judgeReq(agentapi.ShapeTypical, 200*time.Millisecond, typical, 64),
		judgeReq(agentapi.ShapeLongest, 300*time.Millisecond, longest, 64),
		judgeReq(agentapi.ShapeLongOutput, 400*time.Millisecond, longOutput, 64),
	}}
	for range 4 {
		r.Requests = append(r.Requests, judgeReq(agentapi.ShapeConcurrent, 100*time.Millisecond, 2*time.Second, 64))
	}
	return r
}

// judgeEven is a round with every sequential request at one total time.
func judgeEven(n int32, total time.Duration) agentapi.WarmUpRound {
	return judgeRound(n, total, total, total, total)
}

// TestJudgeWarmUpSettles covers HBEP-7, Instance start, Warm-up: "done when no
// shape in a round is more than 10% slower than in the round before", and the
// Test plan item "an engine double that is slow on the first request of each
// shape settles in two rounds".
func TestJudgeWarmUpSettles(t *testing.T) {
	s := time.Second
	tests := []struct {
		name   string
		rounds []agentapi.WarmUpRound
		want   warmUpState
		msg    string
	}{
		{"no round yet", nil, warmUpRunning, ""},
		{"one round cannot be compared", []agentapi.WarmUpRound{judgeEven(1, 5*s)}, warmUpRunning, ""},
		// Round 1 slow (5 s), round 2 at 2 s: 2 s is not slower than 5 s.
		{"slow first round, then fast", []agentapi.WarmUpRound{judgeEven(1, 5*s), judgeEven(2, 2*s)}, warmUpSettled, "round 2"},
		// 2.2 s is exactly 10% above 2 s: not more than 10%, so settled.
		{"exactly 10% slower settles", []agentapi.WarmUpRound{judgeEven(1, 2*s), judgeEven(2, 2200*time.Millisecond)}, warmUpSettled, "round 2"},
		// 2.22 s is 11% above 2 s: more than 10%, not settled.
		{"11% slower does not settle", []agentapi.WarmUpRound{judgeEven(1, 2*s), judgeEven(2, 2220*time.Millisecond)}, warmUpRunning, ""},
		// One shape alone, the long-output request, is 11% slower: no shape may be.
		{"one shape 11% slower does not settle",
			[]agentapi.WarmUpRound{judgeEven(1, 2*s), judgeRound(2, 2*s, 2*s, 2*s, 2220*time.Millisecond)}, warmUpRunning, ""},
		// The same shape at 10% settles.
		{"one shape 10% slower settles",
			[]agentapi.WarmUpRound{judgeEven(1, 2*s), judgeRound(2, 2*s, 2*s, 2*s, 2200*time.Millisecond)}, warmUpSettled, "round 2"},
		// Rounds out of order are read by round number.
		{"read in round order", []agentapi.WarmUpRound{judgeEven(2, 2*s), judgeEven(1, 5*s)}, warmUpSettled, "round 2"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			state, msg, thr := judgeWarmUp(&agentapi.WarmUpResult{Rounds: tc.rounds}, 4)
			if state != tc.want {
				t.Fatalf("state = %d (%q), want %d", state, msg, tc.want)
			}
			if !strings.Contains(msg, tc.msg) {
				t.Errorf("message %q does not contain %q", msg, tc.msg)
			}
			if (state == warmUpSettled) != (thr != nil) {
				t.Errorf("settled %v but throughput %v", state == warmUpSettled, thr)
			}
		})
	}
}

// withToolCall appends one tool-call request to r, with the given total and
// error.
func withToolCall(r agentapi.WarmUpRound, total time.Duration, errText string) agentapi.WarmUpRound {
	q := judgeReq(agentapi.ShapeToolCall, 50*time.Millisecond, total, 40)
	q.Error = errText
	r.Requests = append(slices.Clone(r.Requests), q)
	return r
}

// TestJudgeWarmUpSettleFloor covers the settling rule of the science agent's
// ruling of 1 Nov 2026 (issue #125): a shape is settled when its mean Total
// rises by at most max(10% of the round before, 100 ms). Every round here also
// carries a tool-call request at a steady 1 s, so the judged set is the one an
// Instance with tool calls reports. The figures are #125's acceptance tests:
//
//	200 ms -> 244 ms: +22%, +44 ms, under the 100 ms floor: settled
//	2 s -> 2.24 s:    +12%, +240 ms, above max(200 ms, 100 ms): not settled
//
// and the floor's edges: 200 ms -> 300 ms (+100 ms) settles, 200 ms -> 301 ms
// does not; 2 s -> 2.2 s (+200 ms, the 10%) settles.
//
// HBEP-7's Test plan (Warm-up) names the other figures, worked out by hand: a
// 200 ms request may grow by max(10% = 20 ms, 100 ms) = 100 ms, so +90 ms
// settles and +110 ms does not; a 5 s request by max(10% = 500 ms, 100 ms) =
// 500 ms, so +450 ms settles and +550 ms does not.
func TestJudgeWarmUpSettleFloor(t *testing.T) {
	s, ms := time.Second, time.Millisecond
	round := func(n int32, short, typical time.Duration) agentapi.WarmUpRound {
		return withToolCall(judgeRound(n, short, typical, 2*s, 2*s), s, "")
	}
	tests := []struct {
		name        string
		prev, cur   [2]time.Duration // short, typical
		wantSettled bool
	}{
		{"200 ms shape +22% is under the 100 ms floor", [2]time.Duration{200 * ms, 2 * s}, [2]time.Duration{244 * ms, 2 * s}, true},
		{"2 s shape +12% is above 10%", [2]time.Duration{200 * ms, 2 * s}, [2]time.Duration{200 * ms, 2240 * ms}, false},
		{"200 ms shape +100 ms is at the floor", [2]time.Duration{200 * ms, 2 * s}, [2]time.Duration{300 * ms, 2 * s}, true},
		{"200 ms shape +101 ms is above the floor", [2]time.Duration{200 * ms, 2 * s}, [2]time.Duration{301 * ms, 2 * s}, false},
		{"2 s shape +10% is at the tolerance", [2]time.Duration{200 * ms, 2 * s}, [2]time.Duration{200 * ms, 2200 * ms}, true},
		{"200 ms shape +90 ms settles, the floor exceeding 10%", [2]time.Duration{200 * ms, 2 * s}, [2]time.Duration{290 * ms, 2 * s}, true},
		{"200 ms shape +110 ms does not settle", [2]time.Duration{200 * ms, 2 * s}, [2]time.Duration{310 * ms, 2 * s}, false},
		{"5 s shape +450 ms settles, under 10%", [2]time.Duration{200 * ms, 5 * s}, [2]time.Duration{200 * ms, 5450 * ms}, true},
		{"5 s shape +550 ms does not settle, over 10%", [2]time.Duration{200 * ms, 5 * s}, [2]time.Duration{200 * ms, 5550 * ms}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rounds := []agentapi.WarmUpRound{round(1, tc.prev[0], tc.prev[1]), round(2, tc.cur[0], tc.cur[1])}
			state, msg, _ := judgeWarmUp(&agentapi.WarmUpResult{Rounds: rounds}, 4)
			if got := state == warmUpSettled; got != tc.wantSettled || state == warmUpFailed {
				t.Fatalf("state = %d (%q), want settled %v", state, msg, tc.wantSettled)
			}
		})
	}
}

// TestJudgeWarmUpVaryingShapeIsNotJudged: the tool-call shape, whose output
// length varies, runs in every round but its timings are never judged
// (science-agent ruling, 1 Nov 2026, issue #125): a total ten times the round
// before's does not block settling. A control shows the same slowdown on a
// fixed-length shape does. A failed request is judged on every shape: an error
// on the tool call fails warm-up, as one on the typical shape does (the agent
// reports one only for an HTTP error or a cut stream, never for the answer).
func TestJudgeWarmUpVaryingShapeIsNotJudged(t *testing.T) {
	s := time.Second
	r1 := withToolCall(judgeEven(1, 2*s), 400*time.Millisecond, "")
	slow := withToolCall(judgeEven(2, 2*s), 4*s, "")
	if state, msg, thr := judgeWarmUp(&agentapi.WarmUpResult{Rounds: []agentapi.WarmUpRound{r1, slow}}, 4); state != warmUpSettled || thr == nil || !strings.Contains(msg, "round 2") {
		t.Errorf("tool call ten times slower: state = %d (%q), want settled in round 2", state, msg)
	}
	// Control: the same tenfold slowdown on the typical shape does not settle.
	ctl := withToolCall(judgeRound(2, 2*s, 20*s, 2*s, 2*s), 400*time.Millisecond, "")
	if state, msg, _ := judgeWarmUp(&agentapi.WarmUpResult{Rounds: []agentapi.WarmUpRound{r1, ctl}}, 4); state != warmUpRunning {
		t.Errorf("typical shape ten times slower: state = %d (%q), want running", state, msg)
	}
	// A failed request fails warm-up on the tool call and on a fixed shape.
	toolFailed := withToolCall(judgeEven(2, 2*s), 400*time.Millisecond, "stream: unexpected EOF")
	typFailed := withToolCall(judgeEven(2, 2*s), 400*time.Millisecond, "")
	typFailed.Requests[1].Error = "HTTP 500"
	for name, r2 := range map[string]agentapi.WarmUpRound{"tool call": toolFailed, "typical": typFailed} {
		if state, msg, _ := judgeWarmUp(&agentapi.WarmUpResult{Rounds: []agentapi.WarmUpRound{r1, r2}}, 4); state != warmUpFailed {
			t.Errorf("%s request failed: state = %d (%q), want failed", name, state, msg)
		}
	}
}

// TestJudgeWarmUpRun94ReasoningTrace replays the two rounds of #94's
// reasoning model (Qwen3-0.6B) that the old rule could not settle (issue #94,
// comment 6550908687, finding 3): rounds 3 and 4, one request per sequential
// shape. Round 3's tool call spent its whole budget reasoning (4,224 tokens),
// which the agent now logs and does not report as an error; short went
// 0.208 -> 0.253 s (+22%) and typical 0.214 -> 0.266 s (+24%). The longest,
// long-output and concurrent figures are not in the report; they are held
// steady here. Under the ruling the tool call's timing is not judged and both
// rises are under the 100 ms floor, so warm-up settles in round 4.
func TestJudgeWarmUpRun94ReasoningTrace(t *testing.T) {
	ms := time.Millisecond
	r3 := withToolCall(judgeRound(3, 208*ms, 214*ms, 300*ms, 1100*ms), 9*time.Second, "")
	r3.Requests[len(r3.Requests)-1].OutputTokens = 4224
	r4 := withToolCall(judgeRound(4, 253*ms, 266*ms, 300*ms, 1100*ms), 600*ms, "")
	state, msg, thr := judgeWarmUp(&agentapi.WarmUpResult{Rounds: []agentapi.WarmUpRound{r3, r4}}, 8)
	if state != warmUpSettled || thr == nil || !strings.Contains(msg, "round 4") {
		t.Fatalf("state = %d (%q), want settled in round 4", state, msg)
	}
}

// TestJudgeWarmUpFailsAfterFiveRounds: "A warm-up that has not settled after
// five rounds fails qualification" (HBEP-7, Instance start, Warm-up). Each round
// is 25% slower than the one before: 2.0, 2.5, 3.125, 3.90625 and 4.8828125 s
// (exact in nanoseconds: 2000000000, 2500000000, 3125000000, 3906250000,
// 4882812500).
func TestJudgeWarmUpFailsAfterFiveRounds(t *testing.T) {
	totals := []time.Duration{2000000000, 2500000000, 3125000000, 3906250000, 4882812500}
	var rounds []agentapi.WarmUpRound
	for i, d := range totals {
		rounds = append(rounds, judgeEven(int32(i+1), d))
	}
	for n := 1; n <= 4; n++ {
		if state, msg, _ := judgeWarmUp(&agentapi.WarmUpResult{Rounds: rounds[:n]}, 4); state != warmUpRunning {
			t.Fatalf("after %d rounds state = %d (%q), want running", n, state, msg)
		}
	}
	state, msg, thr := judgeWarmUp(&agentapi.WarmUpResult{Rounds: rounds, Done: true}, 4)
	if state != warmUpFailed || thr != nil {
		t.Fatalf("after 5 rounds state = %d throughput %v (%q), want failed with none", state, thr, msg)
	}
	if !strings.Contains(msg, "5 rounds") {
		t.Errorf("message %q does not say it was five rounds", msg)
	}
	// Five rounds not marked Done by the agent still fail.
	if state, _, _ := judgeWarmUp(&agentapi.WarmUpResult{Rounds: rounds}, 4); state != warmUpFailed {
		t.Errorf("five unsettled rounds without Done: state = %d, want failed", state)
	}
	// Settling in the fifth round is settling: 3.9 s is below 3.90625 s * 1.1.
	late := append(slices.Clone(rounds[:4]), judgeEven(5, 3900*time.Millisecond))
	if state, msg, _ := judgeWarmUp(&agentapi.WarmUpResult{Rounds: late, Done: true}, 4); state != warmUpSettled {
		t.Errorf("settling in round 5: state = %d (%q), want settled", state, msg)
	}
}

// TestJudgeWarmUpFailedRequest: a request with an error fails warm-up, even in
// a round that would have settled.
func TestJudgeWarmUpFailedRequest(t *testing.T) {
	r2 := judgeEven(2, 2*time.Second)
	r2.Requests[1].Error = "connection reset"
	state, msg, thr := judgeWarmUp(&agentapi.WarmUpResult{Rounds: []agentapi.WarmUpRound{judgeEven(1, 5*time.Second), r2}}, 4)
	if state != warmUpFailed || thr != nil {
		t.Fatalf("state = %d throughput %v, want failed with none", state, thr)
	}
	if !strings.Contains(msg, "connection reset") || !strings.Contains(msg, "round 2") {
		t.Errorf("message %q does not name the round and the error", msg)
	}
}

// TestJudgeWarmUpThroughput covers HBEP-7, Warm-up: "The last round's time per
// output token and time to first token are recorded as the Instance's
// throughput at the target sessions." It is taken from the settled round's
// concurrent requests (TPOT = mean of (total - ttft) / (outputTokens - 1) per
// request, TTFT = mean).
//
// Settled round 2, four concurrent requests:
//
//	ttft 100 ms, total 1100 ms, 101 tokens: (1100-100)/100 = 10 ms
//	ttft 200 ms, total 3200 ms,  51 tokens: (3200-200)/50  = 60 ms
//	ttft 300 ms, total 1300 ms, 101 tokens: (1300-300)/100 = 10 ms
//	ttft 400 ms, total 3400 ms,  51 tokens: (3400-400)/50  = 60 ms
//
// mean TTFT = (100+200+300+400)/4 = 250 ms; mean TPOT = (10+60+10+60)/4 = 35
// ms. (Pooling the tokens instead would give 8000 ms / 300 = 26.67 ms.) Round
// 1's concurrent requests and every non-concurrent request carry other values,
// which would show if the wrong ones were read.
func TestJudgeWarmUpThroughput(t *testing.T) {
	ms := time.Millisecond
	r1 := judgeEven(1, 5*time.Second)
	for i := range r1.Requests {
		if r1.Requests[i].Shape == agentapi.ShapeConcurrent {
			r1.Requests[i] = judgeReq(agentapi.ShapeConcurrent, 900*ms, 9*time.Second, 11)
		}
	}
	// Round 2: the four sequential shapes at 2 s, then the four concurrent
	// requests above (mean total (1100+3200+1300+3400)/4 = 2250 ms, below
	// round 1's 9 s, so the round settles).
	r2 := agentapi.WarmUpRound{Round: 2, Requests: []agentapi.RequestTiming{
		judgeReq(agentapi.ShapeShort, 9999*ms, 2*time.Second, 64),
		judgeReq(agentapi.ShapeTypical, 9999*ms, 2*time.Second, 64),
		judgeReq(agentapi.ShapeLongest, 9999*ms, 2*time.Second, 64),
		judgeReq(agentapi.ShapeLongOutput, 9999*ms, 2*time.Second, 64),
		judgeReq(agentapi.ShapeConcurrent, 100*ms, 1100*ms, 101),
		judgeReq(agentapi.ShapeConcurrent, 200*ms, 3200*ms, 51),
		judgeReq(agentapi.ShapeConcurrent, 300*ms, 1300*ms, 101),
		judgeReq(agentapi.ShapeConcurrent, 400*ms, 3400*ms, 51),
	}}
	state, msg, thr := judgeWarmUp(&agentapi.WarmUpResult{Rounds: []agentapi.WarmUpRound{r1, r2}}, 4)
	if state != warmUpSettled || thr == nil {
		t.Fatalf("state = %d throughput %v (%q), want settled in round 2", state, thr, msg)
	}
	if thr.TimeToFirstToken != "250ms" || thr.TimePerOutputToken != "35ms" || thr.TargetSessions != 4 {
		t.Fatalf("throughput = %+v, want TTFT 250ms, TPOT 35ms at 4 sessions", *thr)
	}
}

// TestErrorsOnly covers HBEP-7, Health and restarts, "only errors: over
// errorOnlyWindow, the engine answered requests and every answer was a server
// error". The samples hold the engine's response counters; the window is 1 min
// and now is 10:00:00.
func TestErrorsOnly(t *testing.T) {
	now := time.Date(2026, 11, 2, 10, 0, 0, 0, time.UTC)
	at := func(ago time.Duration, responses, errs int64) healthSample {
		return healthSample{at: now.Add(-ago), responses: responses, errors: errs}
	}
	const window = time.Minute
	tests := []struct {
		name    string
		samples []healthSample
		want    bool
	}{
		// 70 s ago the counters were 100/10, now 110/20: 10 answers in the
		// window, all 10 errors.
		{"every answer an error", []healthSample{at(70*time.Second, 100, 10), at(0, 110, 20)}, true},
		// Now 110/18: 10 answers, 8 errors; two 200s.
		{"a 200 in the window", []healthSample{at(70*time.Second, 100, 10), at(0, 110, 18)}, false},
		// A single 200 among 10 answers is enough: 9 errors of 10.
		{"one 200 in the window", []healthSample{at(70*time.Second, 100, 10), at(0, 110, 19)}, false},
		// The only earlier sample is 30 s old, younger than the window.
		{"no sample old enough", []healthSample{at(30*time.Second, 100, 10), at(0, 110, 20)}, false},
		// The earlier sample is exactly one window old: old enough.
		{"a sample exactly one window old", []healthSample{at(window, 100, 10), at(0, 110, 20)}, true},
		// 59 s old: not yet.
		{"a sample 59 s old", []healthSample{at(59*time.Second, 100, 10), at(0, 110, 20)}, false},
		// Nothing answered in the window: 0 answers, 0 errors.
		{"no answers", []healthSample{at(70*time.Second, 100, 10), at(0, 100, 10)}, false},
		{"one sample", []healthSample{at(0, 110, 20)}, false},
		{"no samples", nil, false},
		// The base is the newest sample at least a window old: 90 s ago
		// (100/0). Over the window 10 answers, 10 errors -> true. The 5 min old
		// sample (0/0) would give 110 answers, 10 errors and a false answer.
		{"the newest old-enough sample is the base",
			[]healthSample{at(5*time.Minute, 0, 0), at(90*time.Second, 100, 0), at(30*time.Second, 105, 5), at(0, 110, 10)}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := errorsOnly(tc.samples, now, window); got != tc.want {
				t.Fatalf("errorsOnly = %v, want %v", got, tc.want)
			}
		})
	}
}
