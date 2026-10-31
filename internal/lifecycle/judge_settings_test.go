package lifecycle

// The settings comparison (HBEP-7, Instance start; science-agent rulings, 31
// Oct 2026, PR #91): a declared flag the engine reports is compared as
// booleans, else as numbers, else verbatim, and a mismatch is a contradiction
// whose message names the setting and both values; a declared "auto" is not
// compared and the reported value is recorded as resolved; max-model-len is
// compared as an integer; a setting reported but not declared is recorded and
// passes; a declared flag the engine does not report is recorded as not
// reported, never as matching. A server error is any response class starting
// with 5, as vLLM labels them "5xx" (agentapi.ServerErrors).

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

func TestJudgeVerifySettingsComparison(t *testing.T) {
	mml := map[string]string{"max-model-len": "32768"}
	withKV := map[string]string{"max-model-len": "32768", "kv-cache-dtype": "fp8"}
	for _, tc := range []struct {
		name        string
		declared    map[string]string
		reported    map[string]string
		pass        bool
		notReported []string
		undeclared  map[string]string
	}{
		{"declared and matching", withKV, map[string]string{"max-model-len": "32768", "kv-cache-dtype": "fp8"}, true, nil, map[string]string{}},
		{"declared and mismatched (verbatim: fp8 is not fp8_e4m3)", withKV, map[string]string{"max-model-len": "32768", "kv-cache-dtype": "fp8_e4m3"}, false, nil, map[string]string{}},
		{"undeclared but reported", mml, map[string]string{"max-model-len": "32768", "kv-cache-dtype": "auto"}, true, nil, map[string]string{"kv-cache-dtype": "auto"}},
		{"declared but not reported", withKV, map[string]string{"max-model-len": "32768"}, true, []string{"kv-cache-dtype"}, map[string]string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := declared{engineVersion: "0.32.0", imageDigest: rigDigest, flags: tc.declared}
			v := &agentapi.VerifyResult{Done: true, EngineVersion: "0.32.0", ImageDigest: rigDigest, Settings: tc.reported, TemplateOverhead: 12}
			got := judgeVerify(d, v, false, shapes{short: 512, typical: 4096, outputTypical: 512, outputLongest: 4096, targetSessions: 4})
			if pass := got.Verdict == v1alpha1.VerdictPass; pass != tc.pass {
				t.Fatalf("verdict %s (cause %q: %s), want pass=%v", got.Verdict, got.Cause, got.Message, tc.pass)
			}
			if !tc.pass && (got.Cause != causeContradiction || !strings.Contains(got.Message, `kv-cache-dtype "fp8_e4m3", declared "fp8"`)) {
				t.Fatalf("cause %q message %q, want a contradiction naming the mismatch", got.Cause, got.Message)
			}
			if !slices.Equal(got.NotReported, tc.notReported) {
				t.Errorf("not reported %v, want %v", got.NotReported, tc.notReported)
			}
			if tc.pass && !maps.Equal(got.Undeclared, tc.undeclared) {
				t.Errorf("undeclared %v, want %v", got.Undeclared, tc.undeclared)
			}
		})
	}
}

// TestJudgeVerifySettingsRules covers HBEP-7, Instance start, Settings
// comparison, as amended by PR #91: one rule for every declared input the
// engine reports. vLLM reports str() of each parsed field, so a boolean reads
// True and a float 4.0. Every verdict below is written by hand from the rule,
// and each row is the case one branch of the rule gets wrong when it is
// missing: a mismatch is a contradiction whose message names the setting and
// both values.
func TestJudgeVerifySettingsRules(t *testing.T) {
	for _, tc := range []struct {
		name, flag, declared, reported string
		pass                           bool
		resolved                       string // the value recorded as resolved, for a declared auto
	}{
		// Booleans, in any case.
		{"boolean: true is True", "enable-prefix-caching", "true", "True", true, ""},
		{"boolean: false is False", "enable-prefix-caching", "false", "False", true, ""},
		{"boolean: any case", "enable-prefix-caching", "TRUE", "true", true, ""},
		{"boolean: true is not False", "enable-prefix-caching", "true", "False", false, ""},
		{"boolean: 1 is not a boolean", "enable-prefix-caching", "1", "True", false, ""},
		// Numbers.
		{"number: 4 is 4.0", "swap-space", "4", "4.0", true, ""},
		{"number: 0.9 is 0.90", "gpu-memory-utilization", "0.9", "0.90", true, ""},
		{"number: 0.00001 is 1e-05", "rope-theta", "0.00001", "1e-05", true, ""},
		{"number: 0.9 is not 0.95", "gpu-memory-utilization", "0.9", "0.95", false, ""},
		// 2^53 + 1 and 2^53 round to the same float, but are different integers.
		{"number: integers are exact above 2^53", "seed", "9007199254740993", "9007199254740992", false, ""},
		// Only decimal numbers: ParseFloat would read both of these as numbers.
		{"number: a hex float is not decimal", "swap-space", "16", "0x1p4", false, ""},
		{"number: inf is not decimal", "swap-space", "inf", "Inf", false, ""},
		// Everything else is verbatim.
		{"verbatim: fp8 is not fp8_e4m3", "kv-cache-dtype", "fp8", "fp8_e4m3", false, ""},
		{"verbatim: case counts for text", "kv-cache-dtype", "fp8", "FP8", false, ""},
		{"verbatim: equal text", "kv-cache-dtype", "fp8_e4m3", "fp8_e4m3", true, ""},
		// A declared auto is not compared; the reported value is the resolved one.
		{"auto: resolved to fp8_e4m3", "kv-cache-dtype", "auto", "fp8_e4m3", true, "fp8_e4m3"},
		{"auto: resolved to auto", "kv-cache-dtype", "auto", "auto", true, "auto"},
		// A model hook's rewrite (DeepseekV32ForCausalLM reports bfloat16 as auto) is
		// a contradiction: only a declared auto asks the engine to choose.
		{"auto: a rewrite to auto is a contradiction", "kv-cache-dtype", "bfloat16", "auto", false, ""},
		// max-model-len is an integer.
		{"max-model-len: equal", "max-model-len", "32768", "32768", true, ""},
		{"max-model-len: 32768.0 is not an integer", "max-model-len", "32768", "32768.0", false, ""},
		{"max-model-len: another value", "max-model-len", "32768", "16384", false, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			flags := map[string]string{"max-model-len": "32768"}
			settings := map[string]string{"max-model-len": "32768"}
			flags[tc.flag], settings[tc.flag] = tc.declared, tc.reported
			d := declared{engineVersion: "0.32.0", imageDigest: rigDigest, flags: flags}
			v := &agentapi.VerifyResult{Done: true, EngineVersion: "0.32.0", ImageDigest: rigDigest, Settings: settings, TemplateOverhead: 12}
			got := judgeVerify(d, v, false, shapes{short: 512, typical: 4096, outputTypical: 512, outputLongest: 4096, targetSessions: 4})
			if !tc.pass {
				if got.Verdict != v1alpha1.VerdictFail || got.Cause != causeContradiction || len(got.Contradictions) != 1 {
					t.Fatalf("verdict %s cause %q contradictions %v (%s), want one contradiction", got.Verdict, got.Cause, got.Contradictions, got.Message)
				}
				for _, part := range []string{tc.flag, fmt.Sprintf("%q", tc.reported), fmt.Sprintf("%q", tc.declared)} {
					if !strings.Contains(got.Message, part) {
						t.Errorf("message %q does not name %s", got.Message, part)
					}
				}
				return
			}
			if got.Verdict != v1alpha1.VerdictPass || len(got.Contradictions) != 0 {
				t.Fatalf("verdict %s cause %q contradictions %v (%s), want pass", got.Verdict, got.Cause, got.Contradictions, got.Message)
			}
			wantResolved := map[string]string{}
			if tc.resolved != "" {
				wantResolved[tc.flag] = tc.resolved
			}
			if !maps.Equal(got.Resolved, wantResolved) {
				t.Errorf("resolved %v, want %v", got.Resolved, wantResolved)
			}
			if len(got.Undeclared) != 0 || len(got.NotReported) != 0 {
				t.Errorf("undeclared %v and not reported %v, want none: the flag is declared and reported", got.Undeclared, got.NotReported)
			}
		})
	}
}

// TestJudgeVerifyRecordsResolvedAuto: a declared "auto" is recorded as
// resolved in the verification record's data (HBEP-7, Instance start), and
// survives the record's JSON form, which is what the Instance's later passes
// read. Declared kv-cache-dtype auto, reported fp8_e4m3 (a checkpoint whose KV
// cache is FP8).
func TestJudgeVerifyRecordsResolvedAuto(t *testing.T) {
	d := declared{engineVersion: "0.32.0", imageDigest: rigDigest, flags: map[string]string{"max-model-len": "32768", "kv-cache-dtype": "auto"}}
	v := &agentapi.VerifyResult{Done: true, EngineVersion: "0.32.0", ImageDigest: rigDigest, TemplateOverhead: 12,
		Settings: map[string]string{"max-model-len": "32768", "kv-cache-dtype": "fp8_e4m3"}}
	got := judgeVerify(d, v, false, shapes{short: 512, typical: 4096, outputTypical: 512, outputLongest: 4096, targetSessions: 4})
	raw, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"resolved":{"kv-cache-dtype":"fp8_e4m3"}`) {
		t.Fatalf("record data %s does not hold the resolved value", raw)
	}
	var back verificationData
	if err := json.Unmarshal(raw, &back); err != nil || back.Resolved["kv-cache-dtype"] != "fp8_e4m3" {
		t.Fatalf("record read back: %v, resolved %v", err, back.Resolved)
	}
}

// TestTrackerCountsServerErrorClasses: responses keyed by class, as vLLM
// v0.32.0 reports them, count "5xx" as server errors, so errors-only fires on
// a real engine; "500" still counts.
func TestTrackerCountsServerErrorClasses(t *testing.T) {
	s := &agentState{health: map[string]*tracker{}}
	t0 := rigStart
	w := agentapi.WorkerReport{Name: "worker-0", PID: 4000}
	for i, resp := range []map[string]int64{
		{"2xx": 10},
		{"2xx": 10, "5xx": 3},
		{"2xx": 10, "5xx": 9, "500": 1},
	} {
		at := t0.Add(time.Duration(i) * time.Minute)
		w.Health = &agentapi.Health{Time: at, ProbeOK: true, MetricsReadAt: &at, ResponsesByStatus: resp}
		s.track("uid", w, t0.Add(time.Duration(i)*time.Minute))
	}
	tr := s.health[trackerKey("uid", w)]
	last := tr.samples[len(tr.samples)-1]
	if last.responses != 20 || last.errors != 10 {
		t.Fatalf("last sample counts %d responses and %d errors, want 20 and 10 (5xx 9 + 500 1)", last.responses, last.errors)
	}
	// From minute 1 to minute 2 every new answer was a server error.
	if !errorsOnly(tr.samples, t0.Add(2*time.Minute), time.Minute) {
		t.Fatal("errors-only did not fire on 5xx-only answers over the window")
	}
}
