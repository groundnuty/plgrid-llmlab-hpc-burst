package alpha

import (
	"context"
	"errors"
	"fmt"
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

// What a run read of ReplacementLate for its deployment, in a second job's NOT RUN.
const (
	LateRaised    = "raised"
	LateNotRaised = "not raised"
	LateUnknown   = "unknown"
)

// SecondNotStartedError is ErrSecondNotStarted with whether the controller raised ReplacementLate for the run's
// deployment (the science agent, on PR 284). That is information for whoever reads the line, and no check: nothing
// decides on it, and an event that could not be read says so. In no-overlap mode the gap is planned, and the controller
// does not raise it (internal/lifecycle/handover.go), so "not raised" is what that row expects.
//
// StartWait is the bound that ran out on a row whose every job has a start wait (Row.StartWait; issue #376's row B): the
// second job was still waiting that long after its Allocation's creation. Zero on the rows whose bound is the wait in the
// queue from the predecessor's lease end.
type SecondNotStartedError struct {
	ReplacementLate string
	StartWait       time.Duration
}

func (e *SecondNotStartedError) Error() string {
	if e.StartWait > 0 {
		return fmt.Sprintf("%v (within its start wait of %s from its Allocation's creation); ReplacementLate for its deployment: %s", ErrSecondNotStarted, e.StartWait, e.ReplacementLate)
	}
	return fmt.Sprintf("%v; ReplacementLate for its deployment: %s", ErrSecondNotStarted, e.ReplacementLate)
}

// Is makes the error ErrSecondNotStarted, whatever it read.
func (e *SecondNotStartedError) Is(target error) bool { return target == ErrSecondNotStarted }

// notKnownBeside is a catch-up's failure on a two-job run. A run whose second job never started still judges its first job's part from
// the watch, so a catch-up that cannot tell fails it as on any run, and it is no NOT RUN: that would hide what may be a failure of the
// part that did happen (the science agent, on PR 284: a NOT RUN and #292's "not known" must not mask each other). What the run read of
// the second job stays in the message, as text: this error is not ErrSecondNotStarted.
func notKnownBeside(err, second error) error {
	if second == nil {
		return err
	}
	return fmt.Errorf("%w; %v", err, second)
}

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
	// NowServe is row A on plgrid-now (issue #376; nowserve.go). Beside what it saw it may return
	// ErrRequeueRefused, which leaves the rest of the run to judge.
	NowServe(ctx context.Context, p Plan) (NowServeObs, error)
	// NowNoOverlap is row B on plgrid-now (issue #376; nownooverlap.go). Beside what it saw it may return ErrSlotTaken
	// or ErrSecondNotStarted, which leave the predecessor's part to judge.
	NowNoOverlap(ctx context.Context, p Plan) (NowNoOverlapObs, error)
	// Ledger is the cluster's record of this run's jobs.
	Ledger() Ledger
	// WatchResets is how many times the run's watch started again from the
	// current versions (stack.Recorder.ResetCount), for the text to post: a
	// measured fact (the science agent's addition on PR 292). The verdicts do not
	// judge it; a reset fails the test by the watch's own rule.
	WatchResets() int
	// Close stops the stack and ends the run's jobs. Run calls it on every
	// exit, a failed scenario included.
	Close(ctx context.Context) error
}
