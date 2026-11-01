package fake_test

import (
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

// A fake engine can script the staged file's sampling keys and its do_sample,
// and the Agent reports them as it gets them (VerifyResult.GenerationConfig and
// DoSample; issue #151), so a controller test can drive the verification record
// with a sampled reasoning model, a greedy one and a model whose file says
// do_sample false.
func TestTheFakeAgentReportsTheScriptedDoSample(t *testing.T) {
	f, tr := false, true
	for name, want := range map[string]*bool{"false": &f, "true": &tr, "absent": nil} {
		t.Run(name, func(t *testing.T) {
			e := fake.Engine{Verify: func(i agentapi.DesiredInstance, req agentapi.VerifyRequest) agentapi.VerifyResult {
				v := fake.DefaultVerify(i, req)
				v.GenerationConfig = map[string]string{"temperature": "0.6", "top_k": "20", "top_p": "0.95"}
				if want != nil {
					d := *want
					v.DoSample = &d
				}
				return v
			}}
			r := newRig(t, e)
			r.set(run(instance(withChecks(agentapi.Checks{VerificationGeneration: 1, Verify: &agentapi.VerifyRequest{}}))))
			r.steps(2)
			v := checksOf(t, r).Verify
			eq(t, "GenerationConfig", v.GenerationConfig, map[string]string{"temperature": "0.6", "top_k": "20", "top_p": "0.95"})
			switch {
			case want == nil && v.DoSample != nil:
				t.Errorf("DoSample %v, want absent", *v.DoSample)
			case want != nil && (v.DoSample == nil || *v.DoSample != *want):
				t.Errorf("DoSample %v, want %v", v.DoSample, *want)
			}
		})
	}
}
