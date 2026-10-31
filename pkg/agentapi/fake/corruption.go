package fake

import (
	"slices"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// corruptionOutputs is how many outputs the corruption check judges, the
// Agent's own number (agentapi.CorruptionOutputs).
const corruptionOutputs = agentapi.CorruptionOutputs

// judgeCorruption applies the Agent's own verdict on the corruption check
// (agentapi.JudgeCorruption, the rule the real agent applies, with the failure's
// Reason) to a qualification result, when the engine returns raw outputs (GreedyOutputs).
// It returns a new result and leaves q's slices alone.
func (e Engine) judgeCorruption(i agentapi.DesiredInstance, req agentapi.QualifyRequest, q agentapi.QualifyResult) agentapi.QualifyResult {
	if e.GreedyOutputs == nil {
		return q
	}
	outcome := agentapi.JudgeCorruption(e.GreedyOutputs(i, req))
	q.Checks = slices.Clone(q.Checks)
	for n, c := range q.Checks {
		if c.Name == agentapi.CheckCorruption {
			q.Checks[n] = outcome
			return q
		}
	}
	q.Checks = append(q.Checks, outcome)
	return q
}
