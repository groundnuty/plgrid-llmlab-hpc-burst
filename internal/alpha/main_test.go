package alpha

import (
	"os"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/enginedouble"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal"
)

// TestMain lets this test binary be the stand-in login node's forced command
// (internal/rehearsal, the relay to the fake cluster) and the engine double's
// fake apptainer and engine (internal/agent/enginedouble), as well as the tests.
func TestMain(m *testing.M) {
	enginedouble.Dispatch()
	rehearsal.Dispatch()
	os.Exit(rehearsal.ReportLeaks(m.Run()))
}
