package lifecycle

// The settings comparison (science-agent ruling, 31 Oct 2026): a declared
// flag is compared verbatim, and a mismatch is a contradiction named in the
// message; a setting reported but not declared is recorded and passes; a
// declared flag the engine does not report is recorded as not reported, never
// as matching. A server error is any response class starting with 5, as vLLM
// labels them "5xx" (agentapi.ServerErrors).

import (
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
		w.Health = &agentapi.Health{Time: t0.Add(time.Duration(i) * time.Minute), ProbeOK: true, ResponsesByStatus: resp}
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
