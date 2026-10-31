package agentapi

import "fmt"

// The corruption check is the one check that needs the engine's raw token
// outputs, which never leave the job, so the agent judges it (doc.go, Who
// decides). The rule lives here, in the standard-library-only package both the
// real agent and the fake judge from, so they judge identically.

const (
	// CorruptionOutputs is how many times the corruption check sends its
	// greedy request, and so how many outputs CorruptionVerdict wants.
	CorruptionOutputs = 3
	// CorruptionPrefix is how many leading tokens the outputs must share.
	CorruptionPrefix = 32
)

// CorruptionVerdict judges the outputs of the corruption check (HBEP-7,
// Instance start). It passes when there are CorruptionOutputs outputs, every
// pair shares its first CorruptionPrefix tokens (every token of the shorter,
// when one is shorter), and no output is empty or a single token repeated (two
// or more tokens, all the same). Identical outputs are not required. The
// message says why a failure failed, and is empty on a pass. It does not change
// outputs.
func CorruptionVerdict(outputs [][]int32) (passed bool, message string) {
	if len(outputs) != CorruptionOutputs {
		return false, fmt.Sprintf("got %d outputs, want %d", len(outputs), CorruptionOutputs)
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
			if at := divergesWithin(outputs[a], outputs[b], CorruptionPrefix); at >= 0 {
				return false, fmt.Sprintf("outputs %d and %d differ at token %d, within the first %d", a+1, b+1, at+1, CorruptionPrefix)
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
