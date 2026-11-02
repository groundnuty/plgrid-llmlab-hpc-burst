package alpha

import (
	"os"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal"
)

// TestMain lets this test binary be the stand-in login node's forced command
// (internal/rehearsal, the relay to the fake cluster) as well as the tests.
func TestMain(m *testing.M) {
	rehearsal.Dispatch()
	os.Exit(m.Run())
}
