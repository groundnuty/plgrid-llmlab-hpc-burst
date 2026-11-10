package conformance

import (
	"os"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal"
)

// TestMain makes this binary the fake login node's forced command (rehearsal.Dispatch) before the tests' flags are
// parsed: the reach-host-dialed rows' dry runs reach the fake Slurm through a user-level sshd whose forced command is
// this binary again (hostdialed_dry_test.go). It then removes the agent the dry runs built.
func TestMain(m *testing.M) {
	rehearsal.Dispatch()
	code := m.Run()
	if dryAgent.dir != "" {
		_ = os.RemoveAll(dryAgent.dir)
	}
	os.Exit(code)
}
