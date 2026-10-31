package fake

import (
	"context"
	"fmt"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// Results of a lift, the second argument of the "scontrol-release" call log
// entry. Every call is logged, so a test counts the lifts that took effect from
// the log: the entries whose result is LiftApplied.
const (
	LiftApplied   = "applied"
	LiftNotHeld   = "not-held"
	LiftAdminHold = "admin-hold"
	LiftNotFound  = "not-found"
	LiftFailed    = "failed"
)

// LiftHold implements provider.Batch. The call log has op "scontrol-release"
// with the job ID and the result (LiftApplied and the others).
func (b *Batch) LiftHold(_ context.Context, jobID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	result, err := b.lift(jobID)
	b.log("scontrol-release", jobID, result)
	return err
}

// lift does the work of LiftHold. The caller holds b.mu.
func (b *Batch) lift(jobID string) (string, error) {
	if len(b.liftErrs) > 0 {
		err := b.liftErrs[0]
		b.liftErrs = b.liftErrs[1:]
		return LiftFailed, err
	}
	j, ok := b.jobs[jobID]
	if !ok {
		return LiftNotFound, fmt.Errorf("job %s: %w", jobID, provider.ErrNotFound)
	}
	now := b.clock.Now()
	switch b.record(j, now).Hold {
	case provider.HoldAdmin:
		return LiftAdminHold, fmt.Errorf("job %s: %w", jobID, provider.ErrAdminHold)
	case provider.HoldUser:
		j.hold, j.eligible = provider.HoldNone, now
		return LiftApplied, nil
	default:
		return LiftNotHeld, fmt.Errorf("job %s: %w", jobID, provider.ErrNotHeld)
	}
}
