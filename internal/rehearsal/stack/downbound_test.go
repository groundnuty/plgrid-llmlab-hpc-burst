package stack

import (
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/launcher"
)

// down stops the supervisor's tree and then k3s, each within its grace and 10 s more for what it killed
// to be gone: the stack's bound on down must cover both at down's default grace, and the clean-up margin
// a down at that bound, or a slow run's clean-up would be cut by the test's deadline (#553 made the
// grace the front's and the rest's in a row).
func TestDownTimeoutCoversDownsGrace(t *testing.T) {
	if want := 2 * (launcher.DefaultGrace + 10*time.Second); DownTimeout < want {
		t.Errorf("DownTimeout %s, want at least %s: down's two groups at launcher.DefaultGrace (%s) and 10 s each", DownTimeout, want, launcher.DefaultGrace)
	}
	if CleanupMargin <= DownTimeout {
		t.Errorf("CleanupMargin %s is not longer than DownTimeout %s: a down at its bound would outlast the clean-up's time", CleanupMargin, DownTimeout)
	}
}
