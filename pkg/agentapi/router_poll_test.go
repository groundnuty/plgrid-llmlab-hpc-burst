package agentapi

import (
	"testing"
	"time"
)

// RouterReadTimeout is the one number the agent's router reads and the controller's window share
// (internal/lifecycle routerPollWindow, `@hpc-burst-science-agent`, 18 Nov 2026, #764, for #784): the
// controller's tests type their windows by hand from it (a 10 s probe interval and two reads of 2 s make
// a 14 s poll and a 28 s window), so a change to it must change those on purpose.
func TestRouterReadTimeoutIsTwoSeconds(t *testing.T) {
	if RouterReadTimeout != 2*time.Second {
		t.Errorf("RouterReadTimeout = %v, want 2s (the controller's window is counted in polls of the probe interval plus two such reads)", RouterReadTimeout)
	}
}
