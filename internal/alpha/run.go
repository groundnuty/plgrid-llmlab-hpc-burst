package alpha

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// Run is one row from its scenario to its verdict. It hands the scenario a
// Recorder, takes what the scenario saw, reads the cluster's own record of the
// run's jobs, judges both, writes the record and the text to post, and closes
// the steps on every exit. It returns the failed checks; an error means the
// run itself broke (a scenario error, a ledger it could not read, a record it
// could not write), which is a failure too but has no verdict.
func Run(ctx context.Context, cfg Config, st Steps, rec *Recorder, logf func(string, ...any)) (failures []string, err error) {
	defer func() {
		if cerr := st.Close(ctx); cerr != nil {
			err = errors.Join(err, fmt.Errorf("closing the steps: %w", cerr))
		}
	}()
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
	if serr := rec.SaveJSON(RecordFile, map[string]any{
		"row": cfg.Row.Name, "mode": cfg.Mode(), "verdict": verdict, "failures": failures,
		"observation": obs, "jobs": jobs, "finished": time.Now().UTC().Format(time.RFC3339),
	}); serr != nil {
		return failures, fmt.Errorf("the record could not be written: %w", serr)
	}
	if serr := rec.SaveText(PostsFile, summary(failures, jobs)); serr != nil {
		return failures, fmt.Errorf("the text to post could not be written: %w", serr)
	}
	return failures, nil
}
