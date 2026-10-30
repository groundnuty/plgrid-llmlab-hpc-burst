package tests

import (
	"os"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s8/harness"
)

func TestMain(m *testing.M) {
	if harness.RunRole() { // child roles never get here
		return
	}
	os.Exit(m.Run())
}
