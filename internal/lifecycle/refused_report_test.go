package lifecycle

import (
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	afake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

// The refused report the agent really sends (PR #169 review, Opus B1; lifted
// from the reviewer's probe, which killed mutant M1). On a refused
// generation_config.json the agent never calls the engine
// (internal/agent/checks.go, startVerify): the report holds Done, the image
// digest, the rehash, the file's numbers and the refusal, and leaves
// EngineVersion, Settings, Fingerprint and TemplateOverhead empty. The
// controller stops the class on it only because judgeVerify judges the
// refusal before the missing-required check; a fixture that fills the engine
// fields tests a report the agent never sends, and cannot see that order
// (the case the review named: a population that cannot produce the failing
// input).

// asTheAgentReportsARefusal returns r as the agent reports a refused file:
// the engine fields blank, and the refusal set.
func asTheAgentReportsARefusal(r *agentapi.VerifyResult, refusal *agentapi.GenerationConfigError) *agentapi.VerifyResult {
	r.EngineVersion, r.Settings, r.Fingerprint, r.TemplateOverhead = "", nil, "", 0
	r.GenerationConfigRefused = refusal
	return r
}

// agentRefusedShape is judgeResult as the agent reports temperature "warm".
func agentRefusedShape() *agentapi.VerifyResult {
	return asTheAgentReportsARefusal(judgeResult(), &agentapi.GenerationConfigError{Param: "temperature", Value: `"warm"`, Want: "a number"})
}

// The agent's real refused report is the settings contradiction's cause, not
// the missing-required one: the cause decides whether the class stops
// (verify.go: causeContradiction and causeShape stop it; causeMissingRequired
// only fails the Instance, and the controller provisions another).
func TestTheAgentsRealRefusedReportIsTheSettingsContradiction(t *testing.T) {
	got := judgeVerify(judgeDeclared(), agentRefusedShape(), true, judgeShapes())
	if got.Cause != causeContradiction {
		t.Errorf("the agent's real refused report: cause %q (%s), want %s", got.Cause, got.Message, causeContradiction)
	}
}

// The same report through the controller, for 30 fake minutes: one Allocation,
// one submission, and the stop survives the time. With the refusal judged
// after the missing-required check the rig submitted 8 Allocations in the
// same half hour (mutant M1).
func TestTheAgentsRealRefusedReportStopsTheClassInTheRig(t *testing.T) {
	rg, in := verifRig(t, func() afake.Engine {
		return afake.Engine{Verify: func(i agentapi.DesiredInstance, r agentapi.VerifyRequest) agentapi.VerifyResult {
			v := afake.DefaultVerify(i, r)
			return *asTheAgentReportsARefusal(&v, &agentapi.GenerationConfigError{Param: "temperature", Value: `"warm"`, Want: "a number"})
		}}
	})
	st, everReady := verifEnded(rg, in.Name)
	if everReady || st.EndReason != "VerificationFailed" {
		t.Errorf("ever ready %v, end reason %q, want never ready and VerificationFailed", everReady, st.EndReason)
	}
	names := map[string]bool{in.Spec.AllocationRef.Name: true}
	for end := rg.clock.Now().Add(30 * time.Minute); rg.clock.Now().Before(end); {
		startStep(rg, "md", startTick)
		for _, a := range rg.allocations("md") {
			names[a.Name] = true
		}
	}
	if len(names) != 1 || rg.submissions() != 1 {
		t.Errorf("Allocations seen %v, submissions %d; want only the failed one", names, rg.submissions())
	}
}
