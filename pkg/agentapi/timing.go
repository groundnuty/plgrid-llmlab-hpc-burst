package agentapi

import "time"

// TimePerOutputToken returns the mean interval between the request's output
// tokens, (Total - TimeToFirstToken) / (OutputTokens - 1), and true for a
// request with no Error and at least two output tokens. It returns false for
// every other request: one with an Error counts for nothing, and one with fewer
// than two tokens has no interval between tokens (see RequestTiming).
func (t RequestTiming) TimePerOutputToken() (time.Duration, bool) {
	if t.Error != "" || t.OutputTokens < 2 {
		return 0, false
	}
	return (time.Duration(t.Total) - time.Duration(t.TimeToFirstToken)) / time.Duration(t.OutputTokens-1), true
}
