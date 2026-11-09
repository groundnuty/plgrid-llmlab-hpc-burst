package alpha

import (
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
)

// In host-dialed mode (HBEP-6, Dial-home modes) the agent registers only once
// its control relay is up: the Provider's controller sees the job running at
// its next poll, then starts the relay within hostdial.RelayStartWorstCase
// (the job-is-ours read's bound and the longest backoff), named here from the
// package, never copied. The live steps add no wait for it: it falls inside
// ReadyMargin, what a started job has beyond health.startTimeout to become
// ready, with the budget's margin.
func TestHostDialedRelayStartFitsTheReadyMargin(t *testing.T) {
	if got := Scaled(slurm.DefaultPollInterval + hostdial.RelayStartWorstCase); got > ReadyMargin {
		t.Errorf("a host-dialed job's relay may start %s after the job, with the margin, above ReadyMargin %s: the live steps would need a wait for it", got, ReadyMargin)
	}
}
