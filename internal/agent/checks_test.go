package agent_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// Shapes of the harness's workload, in tokens. The engine double's template
// overhead is 9 and its max-model-len 4096 (the launch's flag), so the
// longest context, 4096 - 64 - 9 = 4023, fits with the typical output.
var hShapes = agentapi.Shapes{ContextShort: 16, ContextTypical: 64, ContextLongest: 4023, OutputTypical: 64, OutputLongest: 128}

func verifyChecks(gen int64, files []agentapi.FileDigest) *agentapi.Checks {
	return &agentapi.Checks{VerificationGeneration: gen, Verify: &agentapi.VerifyRequest{Files: files}}
}

func qualifyChecks(gen int64, toolCalls bool) *agentapi.Checks {
	return &agentapi.Checks{VerificationGeneration: gen, Qualify: &agentapi.QualifyRequest{Shapes: hShapes, ToolCalls: toolCalls}}
}

func checksOf(r agentapi.Report) *agentapi.CheckResults {
	if r.Instance == nil {
		return nil
	}
	return r.Instance.Checks
}

func verified(gen int64) func(agentapi.Report) bool {
	return func(r agentapi.Report) bool {
		c := checksOf(r)
		return c != nil && c.VerificationGeneration == gen && c.Verify != nil && c.Verify.Done
	}
}

func qualified(gen int64) func(agentapi.Report) bool {
	return func(r agentapi.Report) bool {
		c := checksOf(r)
		return c != nil && c.VerificationGeneration == gen && c.Qualify != nil && c.Qualify.Done
	}
}

// Verification reports what the engine itself reports: its version, its
// resolved max-model-len and cache settings, its fingerprint and the
// template's overhead, with the digest of the image the agent ran. The
// expected values are the engine double's documented constants and the
// launch's flag, not values the agent computes.
func TestVerificationReportsWhatTheEngineReports(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	h.set(h.runState("", 1, 0, verifyChecks(1, nil)))
	r := h.waitReport("verification done", 30*time.Second, verified(1))
	v := r.Instance.Checks.Verify
	if v.EngineVersion != "0.32.0" {
		t.Errorf("engine version %q, want 0.32.0", v.EngineVersion)
	}
	if v.ImageDigest != hDigest {
		t.Errorf("image digest %q, want %q", v.ImageDigest, hDigest)
	}
	if got := v.Settings["max-model-len"]; got != "4096" {
		t.Errorf("max-model-len %q, want 4096 (the engine's /v1/models)", got)
	}
	if got := v.Settings["kv-cache-dtype"]; got != "auto" {
		t.Errorf("kv-cache-dtype %q, want auto (cache_config_info)", got)
	}
	if v.TemplateOverhead != 9 {
		t.Errorf("template overhead %d, want 9", v.TemplateOverhead)
	}
	if v.Fingerprint != "fp-enginedouble" {
		t.Errorf("fingerprint %q", v.Fingerprint)
	}
}

// An engine that resolves max-model-len differently from the flag reports its
// own value; the controller fails the Instance on it.
func TestVerificationReportsAContradictedMaxModelLen(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	h.set(h.runState("report-max-model-len=8192", 1, 0, verifyChecks(1, nil)))
	r := h.waitReport("verification done", 30*time.Second, verified(1))
	if got := r.Instance.Checks.Verify.Settings["max-model-len"]; got != "8192" {
		t.Fatalf("max-model-len %q, want the engine's 8192, never the declared 4096", got)
	}
}

// An engine that reports nothing leaves the version and max-model-len empty:
// the agent fills in nothing from the declared spec, so the controller fails
// it (HBEP-7, Risks: verification passes because it read nothing).
func TestVerificationOfAnEngineThatReportsNothing(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	h.set(h.runState("reports-nothing", 1, 0, verifyChecks(1, nil)))
	r := h.waitReport("verification done", 30*time.Second, verified(1))
	v := r.Instance.Checks.Verify
	if v.EngineVersion != "" {
		t.Errorf("engine version %q from an engine that reports none", v.EngineVersion)
	}
	if got, ok := v.Settings["max-model-len"]; ok {
		t.Errorf("max-model-len %q from an engine that reports none", got)
	}
	if v.Fingerprint != "" {
		t.Errorf("fingerprint %q from an engine that reports none", v.Fingerprint)
	}
}

// The file rehash: a staged file that matches the record is a match, a
// changed one a mismatch with the hash found, and a listed weight file the
// engine does not load (a .bin beside .safetensors) and that was never staged
// is absent. The expected hashes are sha256sum's of the literal contents.
func TestVerificationRehashesTheStagedFiles(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	dir := filepath.Join(h.stagingDir, agentapi.StagedCheckpointDir("org/model", launch("", "").Checkpoint.Revision))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	write := func(name, content string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("config.json", "{}\n")
	write("model.safetensors", "weights, changed\n")
	// printf '{}\n' | sha256sum, printf 'weights\n' | sha256sum and
	// printf 'weights, changed\n' | sha256sum.
	const (
		configSum  = "ca3d163bab055381827226140568f3bef7eaac187cebd76878e0b63e9e442356"
		weightsSum = "1b465fa6b6bcbc06a3199e3d2d8aec35d37494a712f888b6d5536684dd89d0f0"
	)
	// printf 'weights, changed\n' | sha256sum
	const changedSum = "6f61dcf402f3e61ac62a8e42135900454cb04e480f6cd100147a833c4ce142c0"
	files := []agentapi.FileDigest{
		{Path: "config.json", SHA256: configSum},
		{Path: "model.safetensors", SHA256: weightsSum},
		{Path: "pytorch_model.bin", SHA256: weightsSum},
	}
	h.set(h.runState("", 1, 0, verifyChecks(1, files)))
	r := h.waitReport("verification done", 30*time.Second, verified(1))
	got := map[string]agentapi.FileResult{}
	for _, f := range r.Instance.Checks.Verify.Files {
		got[f.Path] = f
	}
	if f := got["config.json"]; f.Outcome != agentapi.FileMatch {
		t.Errorf("config.json: %+v, want a match", f)
	}
	if f := got["model.safetensors"]; f.Outcome != agentapi.FileMismatch || f.SHA256 != changedSum {
		t.Errorf("model.safetensors: %+v, want a mismatch with the changed file's hash", f)
	}
	if f := got["pytorch_model.bin"]; f.Outcome != agentapi.FileAbsent {
		t.Errorf("pytorch_model.bin: %+v, want absent (not loaded beside safetensors, never staged)", f)
	}
}

// qualifyOutcome runs qualification against an engine double with the modes
// and returns the result.
func qualifyOutcome(t *testing.T, modes string, toolCalls bool) agentapi.QualifyResult {
	t.Helper()
	h := newHarness(t, harnessOptions{})
	h.set(h.runState(modes, 1, 0, qualifyChecks(1, toolCalls)))
	r := h.waitReport("qualification done", 60*time.Second, qualified(1))
	return *r.Instance.Checks.Qualify
}

func outcome(q agentapi.QualifyResult, name agentapi.CheckName) (agentapi.CheckOutcome, bool) {
	for _, c := range q.Checks {
		if c.Name == name {
			return c, true
		}
	}
	return agentapi.CheckOutcome{}, false
}

// Every check passes on the healthy double, so the failures below come from
// their doubles and not from a check that always fails.
func TestQualificationPassesAHealthyEngine(t *testing.T) {
	q := qualifyOutcome(t, "", true)
	for _, n := range []agentapi.CheckName{agentapi.CheckHealth, agentapi.CheckChat, agentapi.CheckToolCall, agentapi.CheckCorruption, agentapi.CheckBehaviour} {
		c, ok := outcome(q, n)
		if !ok || !c.Passed {
			t.Errorf("check %s: %+v (present %v), want passed", n, c, ok)
		}
	}
	if q.KVCapacityTokens != 1<<20 {
		t.Errorf("KV capacity %d, want the double's 1048576", q.KVCapacityTokens)
	}
	if len(q.ServedModelNames) != 1 || q.ServedModelNames[0] != hServed {
		t.Errorf("served names %q", q.ServedModelNames)
	}
}

// Each double fails exactly its check (HBEP-7, Test plan: qualification).
func TestQualificationDoublesFailTheirCheck(t *testing.T) {
	for _, tc := range []struct {
		modes     string
		toolCalls bool
		fails     agentapi.CheckName
	}{
		{"greedy-diverge=10", false, agentapi.CheckCorruption},
		{"greedy-repeat", false, agentapi.CheckCorruption},
		{"loop-above=1000", false, agentapi.CheckBehaviour},
		{"tool-malformed", true, agentapi.CheckToolCall},
		{"no-tool-call", true, agentapi.CheckToolCall},
	} {
		t.Run(tc.modes, func(t *testing.T) {
			q := qualifyOutcome(t, tc.modes, tc.toolCalls)
			for _, c := range q.Checks {
				if c.Name == tc.fails && c.Passed {
					t.Errorf("check %s passed against its failing double", c.Name)
				}
				if c.Name != tc.fails && !c.Passed && !(tc.fails == agentapi.CheckToolCall && c.Name == agentapi.CheckBehaviour) {
					t.Errorf("check %s failed too: %q", c.Name, c.Message)
				}
			}
			if _, ok := outcome(q, tc.fails); !ok {
				t.Errorf("no %s outcome", tc.fails)
			}
		})
	}
}

// Outputs that differ only after the 32nd token pass the corruption check: the
// positive control of the rule (HBEP-7: identical text is not required).
func TestCorruptionCheckPassesDivergenceAfterToken32(t *testing.T) {
	q := qualifyOutcome(t, "greedy-diverge=40", false)
	if c, ok := outcome(q, agentapi.CheckCorruption); !ok || !c.Passed {
		t.Fatalf("corruption: %+v, want passed for outputs that share their first 40 tokens", c)
	}
}

// Too little KV capacity is reported as the engine gives it; the controller
// compares it with targetSessions times contextTokens.typical.
func TestQualificationReportsLowKVCapacity(t *testing.T) {
	q := qualifyOutcome(t, "kv-low", false)
	if q.KVCapacityTokens != 1000 {
		t.Fatalf("KV capacity %d, want the double's 1000", q.KVCapacityTokens)
	}
}
