package fake

import (
	"fmt"
	"slices"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

const (
	// corruptionOutputs is how many times the corruption check sends its greedy
	// request.
	corruptionOutputs = 3
	// corruptionPrefix is how many leading tokens the outputs must share.
	corruptionPrefix = 32
)

// judgeCorruption applies the Agent's own verdict on the corruption check to a
// qualification result, when the engine returns raw outputs (GreedyOutputs).
// It returns a new result and leaves q's slices alone.
func (e Engine) judgeCorruption(i agentapi.DesiredInstance, req agentapi.QualifyRequest, q agentapi.QualifyResult) agentapi.QualifyResult {
	if e.GreedyOutputs == nil {
		return q
	}
	passed, msg := corruptionVerdict(e.GreedyOutputs(i, req))
	outcome := agentapi.CheckOutcome{Name: agentapi.CheckCorruption, Passed: passed, Message: msg}
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

// corruptionVerdict judges the outputs of the corruption check (HBEP-7, Instance
// start). It passes when there are three outputs, every pair shares its first 32
// tokens (every token of the shorter, when one is shorter), and no output is
// empty or a single token repeated (two or more tokens, all the same). Identical
// outputs are not required. The message says why a failure failed, and is empty
// on a pass.
func corruptionVerdict(outputs [][]int32) (bool, string) {
	if len(outputs) != corruptionOutputs {
		return false, fmt.Sprintf("got %d outputs, want %d", len(outputs), corruptionOutputs)
	}
	for n, o := range outputs {
		if len(o) == 0 {
			return false, fmt.Sprintf("output %d is empty", n+1)
		}
		if repeatsOneToken(o) {
			return false, fmt.Sprintf("output %d is a single repeated token", n+1)
		}
	}
	for a := range outputs {
		for b := a + 1; b < len(outputs); b++ {
			if at := divergesWithin(outputs[a], outputs[b], corruptionPrefix); at >= 0 {
				return false, fmt.Sprintf("outputs %d and %d differ at token %d, within the first %d", a+1, b+1, at+1, corruptionPrefix)
			}
		}
	}
	return true, ""
}

// repeatsOneToken reports whether the output is two or more copies of one token.
func repeatsOneToken(o []int32) bool {
	if len(o) < 2 {
		return false
	}
	for _, t := range o[1:] {
		if t != o[0] {
			return false
		}
	}
	return true
}

// divergesWithin returns the index of the first token, within the first limit,
// at which a and b differ, comparing only as far as the shorter output goes. It
// returns -1 when they agree throughout.
func divergesWithin(a, b []int32, limit int) int {
	n := min(limit, len(a), len(b))
	for k := range n {
		if a[k] != b[k] {
			return k
		}
	}
	return -1
}
