package alpha

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// NotRun reports whether err ends the row's run as NOT RUN rather than as a failure: a job of the
// run never started within a bound, whichever bound ran out and whichever row it is (the science
// agent's ruling of 8 Nov 2026 on #272, replacing #230's no-slot failure). The bounds are:
//   - the wait in the queue, the first clock (ErrNotStarted), HPCBURST_ALPHA_PENDING_MAX_SECONDS;
//   - a tunnel row's deployment end, a minute before its first end (ErrQueueWaitPastEnd; a
//     NotStartedError with PastEnd is both errors);
//   - a two-job row's second job, by the first job's drain start or the wait in the queue from its
//     lease end (ErrSecondNotStarted): its first job ran, and Run ends the run NOT RUN only when
//     that part did not fail.
//
// A FAIL means the property the row tests was absent, and a queue wait tests nothing. The row is a
// parameter so that no rule by row can come back unseen: TestEveryRowEndsNotRunWhenItsJobNeverStarts
// asks it of every row in Rows.
func NotRun(_ Row, err error) bool { return neverStarted(err) }

// neverStarted is whether err says a job of the run never started within a bound.
func neverStarted(err error) bool {
	return errors.Is(err, ErrNotStarted) || errors.Is(err, ErrQueueWaitPastEnd) || errors.Is(err, ErrSecondNotStarted)
}

// NotRunNote is the text of the marker (NotRunFile): the row, the wait and the fact, never a job
// ID or a tag. err is the run's error: a NotStartedError gives the bound that ran out, and says
// when it was the deployment's first end, which hack/live-checks.sh reads off the marker to name
// that bound in its line (the wait in the queue otherwise); any other gives pendingMax.
func NotRunNote(row Row, pendingMax time.Duration, err error) string {
	if errors.Is(err, ErrSecondNotStarted) {
		return fmt.Sprintf("%s: %s; ReplacementLate for its deployment: %s; this run's jobs were cancelled by tag\n", row.Name, secondNotStartedWords, lateOf(err))
	}
	var ns *NotStartedError
	if errors.As(err, &ns) {
		if ns.PastEnd {
			return fmt.Sprintf("%s: a job was still pending after %s, a minute before the deployment's first end; this run's jobs were cancelled by tag\n", row.Name, ns.Bound)
		}
		pendingMax = ns.Bound
	}
	return fmt.Sprintf("%s: a job was still pending after %s; this run's jobs were cancelled by tag\n", row.Name, pendingMax)
}

// lateOf is what a second job's error says was read of ReplacementLate.
func lateOf(err error) string {
	var s *SecondNotStartedError
	if errors.As(err, &s) {
		return s.ReplacementLate
	}
	return LateUnknown
}

// stopsTheRun is whether a scenario's error leaves nothing to judge. ErrSecondNotStarted leaves the first job's part.
func stopsTheRun(err error) bool { return err != nil && !errors.Is(err, ErrSecondNotStarted) }

// secondNotStartedWords say what happened to a run of two jobs, in the marker, which hack/live-checks.sh reads for
// them, and in the text to post of a run whose first job's part failed.
const secondNotStartedWords = "the first job ran and the second never started"

// Run is one row from its scenario to its verdict. It hands the scenario a
// Recorder, takes what the scenario saw, reads the cluster's own record of the
// run's jobs, judges both, writes the record and the text to post, and closes
// the steps on every exit. It returns the failed checks; an error means the
// run itself broke (a scenario error, a ledger it could not read, a record it
// could not write), which is a failure too but has no verdict.
func Run(ctx context.Context, cfg Config, st Steps, rec *Recorder, logf func(string, ...any)) (failures []string, err error) {
	defer func() {
		if cerr := st.Close(ctx); cerr != nil {
			if neverStarted(err) {
				// A close that failed may have left a job: that is a failure, never a NOT RUN.
				err = fmt.Errorf("closing the steps after a job that did not start: %v (%v)", cerr, err)
				return
			}
			err = errors.Join(err, fmt.Errorf("closing the steps: %w", cerr))
		}
	}()
	// The text to post names where the front ran; a run without a label is refused before anything is driven.
	if lerr := ValidFrontLabel(cfg.FrontLabel); lerr != nil {
		return nil, fmt.Errorf("the text to post cannot be written: %w", lerr)
	}
	plan := Plan{Row: cfg.Row, Rec: rec, PendingMax: cfg.PendingMax, Logf: logf}
	var (
		tags    []string
		obs     any
		judge   func([]provider.Job) []string
		summary func([]string, []provider.Job) string
		// second is the scenario's ErrSecondNotStarted: what it saw of the first job is judged, and decides the end.
		second error
	)
	switch {
	case strings.HasPrefix(cfg.Row.Name, "alpha-athena-handover"):
		o, serr := st.Handover(ctx, plan)
		if stopsTheRun(serr) {
			return nil, serr
		}
		second = serr
		tags, obs = o.Tags, o
		judge = func(j []provider.Job) []string {
			if second != nil {
				return handoverFirstJobChecks(cfg.Row, o, j)
			}
			return VerifyHandover(cfg.Row, o, j)
		}
		summary = func(f []string, _ []provider.Job) string { return handoverPost(cfg.Row, o, f) }
	case strings.HasPrefix(cfg.Row.Name, "alpha-helios-nooverlap"):
		o, serr := st.NoOverlap(ctx, plan)
		if stopsTheRun(serr) {
			return nil, serr
		}
		second = serr
		tags, obs = o.Tags, o
		judge = func(j []provider.Job) []string {
			if second != nil {
				return noOverlapFirstJobChecks(cfg.Row, o, j)
			}
			return VerifyNoOverlap(cfg.Row, o, j)
		}
		summary = func(f []string, j []provider.Job) string { return noOverlapPost(cfg.Row, o, f, j) }
	case strings.HasPrefix(cfg.Row.Name, "tunnel-hour-"):
		o, serr := st.TunnelHour(ctx, plan)
		if serr != nil {
			return nil, serr
		}
		tags, obs = o.Tags, o
		judge = func(j []provider.Job) []string { return VerifyTunnelHour(cfg.Row, o, j) }
		summary = func(f []string, _ []provider.Job) string { return tunnelHourPost(cfg.Row, o, f) }
	default:
		return nil, fmt.Errorf("row %q is not one this package runs", cfg.Row.Name)
	}
	var missing []string
	for _, t := range tags {
		if !rec.Tagged(t) {
			missing = append(missing, "a job tag was not written down before its submission, so the cleanup cannot cancel it")
			break
		}
	}
	jobs, lerr := st.Ledger().Jobs(ctx, tags)
	if lerr != nil {
		return nil, fmt.Errorf("the cluster's record of the run's jobs could not be read: %w", lerr)
	}
	failures = dedupe(append(judge(jobs), missing...))
	if second != nil && len(failures) == 0 {
		return nil, second // the job never started and nothing else failed: no record and no text to post
	}
	verdict := "pass"
	if len(failures) > 0 {
		verdict = "fail"
	}
	if serr := rec.SaveJSON(RecordFile, map[string]any{
		"row": cfg.Row.Name, "mode": cfg.Mode(), "frontLabel": cfg.FrontLabel, "verdict": verdict, "failures": failures,
		"observation": obs, "jobs": jobs, "finished": time.Now().UTC().Format(time.RFC3339),
	}); serr != nil {
		return failures, fmt.Errorf("the record could not be written: %w", serr)
	}
	post := summary(failures, jobs)
	if second != nil {
		post += "- " + secondNotStartedWords + "; the failed checks are the first job's\n- ReplacementLate for its deployment: " + lateOf(second) + " (information, not a check)\n"
	}
	if serr := rec.SaveText(PostsFile, InsertFrontLine(post, cfg.FrontLabel)); serr != nil {
		return failures, fmt.Errorf("the text to post could not be written: %w", serr)
	}
	return failures, nil
}
