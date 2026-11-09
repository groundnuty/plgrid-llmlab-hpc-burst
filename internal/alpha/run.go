package alpha

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// NotRun reports whether err ends the row's run as NOT RUN rather than as a failure:
//   - a job pended past the first clock (ErrNotStarted) on a row that holds QOS now's one-job
//     slot, where a busy queue is not the run's fault and the slot must not be held waiting;
//   - a tunnel row's job pended until a minute before its deployment's first end
//     (ErrQueueWaitPastEnd), on either cluster: the job did not start in time (the science
//     agent's ruling on PR 255).
//
// On any other row the same error is a failure.
func NotRun(row Row, err error) bool {
	return (row.SlotHold > 0 && errors.Is(err, ErrNotStarted)) || (row.MostJobs == 1 && errors.Is(err, ErrQueueWaitPastEnd))
}

// NotRunNote is the text of the marker (NotRunFile): the row, the wait and the fact, never a job
// ID or a tag. err is the run's error: a NotStartedError gives the bound that ran out, and says
// when it was the deployment's first end, which hack/live-checks.sh reads off the marker for its
// line; any other gives pendingMax.
func NotRunNote(row Row, pendingMax time.Duration, err error) string {
	var ns *NotStartedError
	if errors.As(err, &ns) {
		if ns.PastEnd {
			return fmt.Sprintf("%s: a job was still pending after %s, a minute before the deployment's first end; this run's jobs were cancelled by tag\n", row.Name, ns.Bound)
		}
		pendingMax = ns.Bound
	}
	return fmt.Sprintf("%s: a job was still pending after %s; this run's jobs were cancelled by tag\n", row.Name, pendingMax)
}

// Run is one row from its scenario to its verdict. It hands the scenario a
// Recorder, takes what the scenario saw, reads the cluster's own record of the
// run's jobs, judges both, writes the record and the text to post, and closes
// the steps on every exit. It returns the failed checks; an error means the
// run itself broke (a scenario error, a ledger it could not read, a record it
// could not write), which is a failure too but has no verdict.
func Run(ctx context.Context, cfg Config, st Steps, rec *Recorder, logf func(string, ...any)) (failures []string, err error) {
	defer func() {
		if cerr := st.Close(ctx); cerr != nil {
			if errors.Is(err, ErrNotStarted) {
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
	)
	switch {
	case strings.HasPrefix(cfg.Row.Name, "alpha-athena-handover"):
		o, serr := st.Handover(ctx, plan)
		if serr != nil {
			return nil, serr
		}
		tags, obs = o.Tags, o
		judge = func(j []provider.Job) []string { return VerifyHandover(cfg.Row, o, j) }
		summary = func(f []string, _ []provider.Job) string { return handoverPost(cfg.Row, o, f) }
	case strings.HasPrefix(cfg.Row.Name, "alpha-helios-nooverlap"):
		o, serr := st.NoOverlap(ctx, plan)
		if serr != nil {
			return nil, serr
		}
		tags, obs = o.Tags, o
		judge = func(j []provider.Job) []string { return VerifyNoOverlap(cfg.Row, o, j) }
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
	verdict := "pass"
	if len(failures) > 0 {
		verdict = "fail"
	}
	resets := st.WatchResets()
	if serr := rec.SaveJSON(RecordFile, map[string]any{
		"row": cfg.Row.Name, "mode": cfg.Mode(), "frontLabel": cfg.FrontLabel, "verdict": verdict, "failures": failures,
		"observation": obs, "jobs": jobs, "watchResets": resets, "finished": time.Now().UTC().Format(time.RFC3339),
	}); serr != nil {
		return failures, fmt.Errorf("the record could not be written: %w", serr)
	}
	if serr := rec.SaveText(PostsFile, InsertFrontLine(summary(failures, jobs)+watchResetsLine(resets), cfg.FrontLabel)); serr != nil {
		return failures, fmt.Errorf("the text to post could not be written: %w", serr)
	}
	return failures, nil
}
