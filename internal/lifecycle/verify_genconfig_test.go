package lifecycle

import (
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	afake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

// TestVerifyRefusedGenerationConfigStopsClass: a staged generation_config.json
// with a value vLLM refuses fails verification with VerificationFailed, naming
// the key and the value, and stops provisioning in the class by the settings
// contradiction's stop (keyed on the deployment ID, surviving a controller
// restart), for a value the controller sees in the numbers the agent recorded
// (temperature 2.5) and for one only the agent sees (temperature "warm", which
// reaches it as GenerationConfigRefused), and for a file refused as a whole. The Instance fails at once, with no
// restart (HBEP-7, Instance start; science-agent ruling, 1 Nov 2026, N7).
func TestVerifyRefusedGenerationConfigStopsClass(t *testing.T) {
	for _, tc := range []struct {
		name     string
		verify   func(agentapi.DesiredInstance, agentapi.VerifyRequest) agentapi.VerifyResult
		contains []string // in the verification message and the record
	}{
		{"a number out of range", func(i agentapi.DesiredInstance, r agentapi.VerifyRequest) agentapi.VerifyResult {
			v := afake.DefaultVerify(i, r)
			v.GenerationConfig = map[string]string{"temperature": "2.5", "top_p": "0.95"}
			return v
		}, []string{`"temperature"`, `"2.5"`}},
		// The reports below are the agent's real refused report: it never calls
		// the engine, so EngineVersion, Settings, Fingerprint and
		// TemplateOverhead are blank (asTheAgentReportsARefusal).
		{"a value that is not a number, reported by the agent", func(i agentapi.DesiredInstance, r agentapi.VerifyRequest) agentapi.VerifyResult {
			v := afake.DefaultVerify(i, r)
			return *asTheAgentReportsARefusal(&v, &agentapi.GenerationConfigError{Param: "temperature", Value: `"warm"`, Want: "a number"})
		}, []string{`"param":"temperature"`, `warm`}},
		{"a number out of range, reported by the agent with the file's numbers", func(i agentapi.DesiredInstance, r agentapi.VerifyRequest) agentapi.VerifyResult {
			v := afake.DefaultVerify(i, r)
			v.GenerationConfig = map[string]string{"temperature": "2.5", "top_p": "0.95"}
			return *asTheAgentReportsARefusal(&v, &agentapi.GenerationConfigError{Param: "temperature", Value: "2.5", Want: "[0, 2]"})
		}, []string{`"param":"temperature"`, `"2.5"`}},
		{"a file above 1 MiB, refused as a whole", func(i agentapi.DesiredInstance, r agentapi.VerifyRequest) agentapi.VerifyResult {
			v := afake.DefaultVerify(i, r)
			return *asTheAgentReportsARefusal(&v, &agentapi.GenerationConfigError{Value: "larger than 1 MiB", Want: "a JSON object of at most 1 MiB"})
		}, []string{`"param":""`, `larger than 1 MiB`}},
		{"a file the agent cannot read, refused as a whole and reported unreadable", func(i agentapi.DesiredInstance, r agentapi.VerifyRequest) agentapi.VerifyResult {
			v := afake.DefaultVerify(i, r)
			v.GenerationConfigFile = "unreadable"
			return *asTheAgentReportsARefusal(&v, &agentapi.GenerationConfigError{Value: "unreadable", Want: "a JSON object of at most 1 MiB"})
		}, []string{`"param":""`, `unreadable`, `"generationConfigFile":"unreadable"`}},
		{"a file reported unreadable by an agent that sends no refusal", func(i agentapi.DesiredInstance, r agentapi.VerifyRequest) agentapi.VerifyResult {
			v := afake.DefaultVerify(i, r)
			v.GenerationConfigFile = "unreadable"
			v.EngineVersion, v.Settings, v.Fingerprint, v.TemplateOverhead = "", nil, "", 0
			return v
		}, []string{`"param":""`, `unreadable`, `"generationConfigFile":"unreadable"`}},
		{"a file with NaN, refused as a whole", func(i agentapi.DesiredInstance, r agentapi.VerifyRequest) agentapi.VerifyResult {
			v := afake.DefaultVerify(i, r)
			return *asTheAgentReportsARefusal(&v, &agentapi.GenerationConfigError{Value: "not valid JSON", Want: "a JSON object of at most 1 MiB"})
		}, []string{`"param":""`, `not valid JSON`}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rg, in := verifRig(t, func() afake.Engine { return afake.Engine{Verify: tc.verify} })
			st, everReady := verifEnded(rg, in.Name)
			if everReady || st.EndReason != "VerificationFailed" {
				t.Fatalf("ever ready %v, end reason %q, want never ready and VerificationFailed", everReady, st.EndReason)
			}
			raw, verdict, ok := verifRecord(rg, verifRecordName(in, 1))
			if !ok || verdict != "fail" {
				t.Fatalf("record 1: exists %v verdict %q, want fail", ok, verdict)
			}
			for _, want := range append([]string{`"cause":"contradiction"`, `"generationConfigRefused":{`}, tc.contains...) {
				if !strings.Contains(string(raw), want) {
					t.Errorf("record %s does not contain %s", raw, want)
				}
			}
			if strings.Contains(string(raw), `"sampling"`) {
				t.Errorf("record %s holds a sampling, but no check request was sent", raw)
			}
			if n := len(rg.rec.Find("failing", "InstanceRestart")) + len(rg.rec.Find("failing", "WorkerRestart")); n != 0 {
				t.Errorf("%d restarts; a refused file fails the Instance at once", n)
			}
			names := map[string]bool{in.Spec.AllocationRef.Name: true}
			hold := func(d time.Duration) {
				t.Helper()
				for end := rg.clock.Now().Add(d); rg.clock.Now().Before(end); {
					startStep(rg, "md", startTick)
					for _, a := range rg.allocations("md") {
						names[a.Name] = true
					}
				}
			}
			hold(30 * time.Minute)
			c := rg.condition("md", v1alpha1.ConditionDegraded)
			if c.Status != "True" || c.Reason != v1alpha1.ReasonVerificationFailed {
				t.Errorf("Degraded = %s/%s (%s), want True/VerificationFailed", c.Status, c.Reason, c.Message)
			}
			rg.restartController()
			hold(30 * time.Minute)
			if len(names) != 1 || rg.submissions() != 1 {
				t.Errorf("Allocations seen %v, submissions %d; want only the failed one, and nothing after a controller restart", names, rg.submissions())
			}
		})
	}
}
