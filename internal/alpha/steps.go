package alpha

import (
	"context"
	"errors"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// ErrNotStarted is what a scenario returns when a job of the run was still
// pending when the first clock (Plan.PendingMax) ran out, after it cancelled the
// run's jobs by their tags. That ends the run as NOT RUN (NotRun) on every row
// (the science agent's ruling of 8 Nov 2026, #272): the job never started, so
// the property the row tests was never put to the test; try again later.
var ErrNotStarted = errors.New("a job of the run was still pending when the wait in the queue ran out, and was cancelled by its tag")

// ErrSecondNotStarted is what a scenario of two jobs returns when its first job
// ran and its second never started within its bound, a job the scheduler still
// shows waiting. The first job's part of the run stands: Run judges it, and an
// earlier failure there wins. With none, the run is NOT RUN (NotRun, the ruling of
// 8 Nov 2026 on #272, review of PR 284, B3): the second job never started, so the
// property the row tests was never put to the test. The scenario returns what it
// saw beside it.
var ErrSecondNotStarted = errors.New("the first job of the run ran and the second never started, and was cancelled by its tag")

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
// stand-in steps (dry_test.go) and the live steps (steps_live_test.go and
// live_flows_test.go, over the real cluster or the stand-in one) are the two
// implementations.
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
