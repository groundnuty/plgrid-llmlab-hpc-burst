package alpha

import (
	"context"
	"errors"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// ErrNotWired is what the live steps return until the one-host rehearsal's
// scenario code (PR #161) is wired in: the run fails, loudly, and has used no
// cluster.
var ErrNotWired = errors.New("the live scenario steps are not wired yet: they wait for #161 (internal/rehearsal/scenario)")

// Plan is what a scenario is given.
type Plan struct {
	Row Row
	// Rec is the run's recorder. The scenario writes every Allocation's tag
	// down with Rec.Tag before anything can submit it.
	Rec *Recorder
	// PendingMax is the first clock of the run: how long a job may wait in the
	// queue before the run gives up and cancels it. The second is Row.Budget.
	PendingMax time.Duration
	// Logf writes a line to the test log. It is never given a host, a
	// key, an account or a job ID.
	Logf func(format string, args ...any)
}

// Ledger is the cluster's own record of the run's jobs, read by tag. The live
// ledger reads sacct and squeue over the provider's ssh, through the plugin's
// batch client; the dry ledger reads the fake Slurm in run mode. The verdicts
// judge the cluster's times, not the controllers' account of them.
type Ledger interface {
	Jobs(ctx context.Context, tags []string) ([]provider.Job, error)
}

// Steps is the seam between the scenario and the judgement. A scenario starts
// the stack, drives it and returns what it saw; it never judges. The dry run's
// stand-in steps (dry_test.go) and the live steps (steps_live_test.go) are the
// two implementations.
type Steps interface {
	Handover(ctx context.Context, p Plan) (HandoverObs, error)
	NoOverlap(ctx context.Context, p Plan) (NoOverlapObs, error)
	TunnelHour(ctx context.Context, p Plan) (TunnelHourObs, error)
	// Ledger is the cluster's record of this run's jobs.
	Ledger() Ledger
	// Close stops the stack and ends the run's jobs. Run calls it on every
	// exit, a failed scenario included.
	Close(ctx context.Context) error
}
