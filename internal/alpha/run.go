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
//
// Two NOT RUNs are not a job that never started, both row A's (issue #376): its warm start that the agent's own log
// did not show (ErrWarmUnshown; the ruling 1(b)), and row B's (below). Row A's requeue, which the scheduler refuses by design for
// a job submitted --no-requeue, is not a step of the row any more (issue #503; the science agent's ruling of 13 Nov 2026), and so
// no NOT RUN. Run has judged the rest of that run first, and returns it only when nothing failed.
//
// A third is row B's (issue #376): a job that is not the run's held the one-job slot between the row's two jobs
// (ErrSlotTaken; the science agent's ruling Q7), the owner's own work, so the one-slot handover was never put to the
// test.
//
// A fourth is a handover row's before its streams open (issue #447; the science agent's ruling (a), 12 Nov 2026): the lead
// that fits the deployment's max-model-len at the engine's measured rate is under LeadFloor (ErrLeadTooShort), so no
// stream long enough to tell a cut from an end can cross the drain start. Nothing was driven past the rate stream, and
// the run's jobs are cancelled by their tags.
//
// A sixth is the router row's (issue #521; beta's tracker #522): the compute node's glibc is older than the 2.30 Envoy needs
// (ErrGlibcTooOld), read on the node or from the agent's own log of a refused start, so Envoy cannot run bare there and the router
// was never put to the test. The marker names the version alone, and the run's jobs are cancelled by their tags.
//
// A seventh is the keeper row's (issue #521): the stream that fits the deployment's max-model-len at the engine's measured rate is under the
// window that spans a controller restart's outcome (ErrKeeperStreamTooShort), so the restart was never put to the test. Nothing was sent but
// the rate stream, and the run's jobs are cancelled by their tags.
//
// An eighth is the keeper row's after its run (the science agent's ruling (b) on #543): nothing failed, but the stream did not outlast the keeper's
// outcome by the margin (ErrKeeperStreamEndedFirst), so the handover was not shown crossing a stream. The controller was killed: the run is judged,
// recorded and posted with the reason, as row A's warm start is, and with no failure it is NOT RUN.
//
// A fifth is the tunnel rows' (issue #451; the science agent's ruling (b), 12 Nov 2026): the stream that fits the deployment's
// max-model-len at the engine's measured rate is under the row's LongFloor (ErrLongTooShort), so the hour cannot hold the
// long stream its check 2 asks for. Nothing was sent but the rate stream.
func NotRun(_ Row, err error) bool {
	return neverStarted(err) || errors.Is(err, ErrWarmUnshown) || errors.Is(err, ErrSlotTaken) || errors.Is(err, ErrLeadTooShort) || errors.Is(err, ErrLongTooShort) ||
		errors.Is(err, ErrGlibcTooOld) || errors.Is(err, ErrKeeperStreamTooShort) || errors.Is(err, ErrKeeperStreamEndedFirst) || errors.Is(err, ErrNoStagedImage)
}

// ErrInvalid ends a run that ran and was judged with no failure, but whose reading measures nothing: the bulk row's run with no saturation witness
// (betabulk.go, BulkInvalid; the science agent's ruling on #521, 1(a)). It is neither a pass nor a failure, and no NOT RUN: the row was put to
// the test, and the test did not measure. Run judges the rest of the run first, writes the record (verdict "invalid") and the text to post (its
// head INVALID, with why), and returns it only when nothing failed; a failure wins. runRow writes invalidWords to the marker (NotRunFile) and
// skips once at the top level, and hack/live-checks.sh reads that, on the bulk row alone, as INVALID, exit 3 like a NOT RUN: not a failure,
// not a pass, run it again.
var ErrInvalid = errors.New(invalidWords)

// invalidWords say what an INVALID run is, in the marker, which hack/live-checks.sh reads (invalid_marker).
const invalidWords = "the run has no saturation witness, so its reading measures nothing"

// InvalidError is ErrInvalid with why: the witness's checks that did not hold, in their words.
type InvalidError struct{ Why []string }

func (e *InvalidError) Error() string { return invalidWords + ": " + strings.Join(e.Why, "; ") }
func (e *InvalidError) Unwrap() error { return ErrInvalid }

// Invalid reports whether err ends the row's run as INVALID (ErrInvalid). It is never also NotRun: the two are told apart by the marker's words.
func Invalid(_ Row, err error) bool { return errors.Is(err, ErrInvalid) }

// InvalidNote is the text of an INVALID run's marker (NotRunFile): the row and the fixed words, nothing the run read. The text to post says why.
func InvalidNote(row Row) string {
	return fmt.Sprintf("%s: %s; its job started, ran and spent the slot, so this is no NOT RUN; the run's other checks passed; the text to post says why; the run ended its deployment\n", row.Name, invalidWords)
}

// neverStarted is whether err says a job of the run never started within a bound.
func neverStarted(err error) bool {
	return errors.Is(err, ErrNotStarted) || errors.Is(err, ErrQueueWaitPastEnd) || errors.Is(err, ErrSecondNotStarted)
}

// NotRunNote is the text of the marker (NotRunFile): the row, the wait and the fact, never a job
// ID or a tag. err is the run's error: a NotStartedError gives the bound that ran out, and says
// when it was the deployment's first end, which hack/live-checks.sh reads off the marker to name
// that bound in its line (the wait in the queue otherwise); any other gives pendingMax.
func NotRunNote(row Row, pendingMax time.Duration, err error) string {
	if errors.Is(err, ErrNoStagedImage) {
		// Fixed words: the bulk row's image precondition (the science agent's rulings on #579) names no path and no digest.
		return fmt.Sprintf("%s: %s; stage it first with the stage row; no job was submitted\n", row.Name, noStagedImageWords)
	}
	if errors.Is(err, ErrKeeperStreamEndedFirst) {
		// The times, durations only: hack/live-checks.sh reads the reason in parentheses.
		reason := ""
		var ke *KeeperStreamEndedFirstError
		if errors.As(err, &ke) {
			reason = " (" + ke.counts() + ")"
		}
		return fmt.Sprintf("%s: %s%s; the controller was killed and the run's other checks passed; this run's jobs were ended with its deployment\n", row.Name, keeperStreamEndedFirstWords, reason)
	}
	if errors.Is(err, ErrKeeperStreamTooShort) {
		// The rate and the context, numbers only: hack/live-checks.sh reads the reason in parentheses.
		reason := ""
		var kt *KeeperStreamTooShortError
		if errors.As(err, &kt) {
			reason = " (" + kt.counts() + ")"
		}
		return fmt.Sprintf("%s: %s%s; the controller was not killed; this run's jobs were cancelled by tag\n", row.Name, keeperStreamTooShortWords, reason)
	}
	if errors.Is(err, ErrGlibcTooOld) {
		// The version only, numbers: hack/live-checks.sh reads the reason in parentheses.
		reason := ""
		var g *GlibcTooOldError
		if errors.As(err, &g) {
			reason = " (glibc " + g.Version.String() + ")"
		}
		return fmt.Sprintf("%s: %s%s; the run sent no request through the front; this run's jobs were cancelled by tag\n", row.Name, glibcTooOldWords, reason)
	}
	if errors.Is(err, ErrLeadTooShort) {
		// The rate and the context, numbers only: hack/live-checks.sh reads the reason in parentheses (row_a_reason).
		reason := ""
		var lt *LeadTooShortError
		if errors.As(err, &lt) {
			reason = " (" + lt.counts() + ")"
		}
		return fmt.Sprintf("%s: %s%s; the run opened no stream across the drain start; this run's jobs were cancelled by tag\n", row.Name, leadTooShortWords, reason)
	}
	if errors.Is(err, ErrLongTooShort) {
		reason := ""
		var lt *LongTooShortError
		if errors.As(err, &lt) {
			reason = " (" + lt.counts() + ")"
		}
		return fmt.Sprintf("%s: %s%s; the hour was not started; this run's jobs were cancelled by tag\n", row.Name, longTooShortWords, reason)
	}
	if errors.Is(err, ErrWarmUnshown) || errors.Is(err, ErrSlotTaken) {
		why := notRunWhy(err)
		if why == "" && errors.Is(err, ErrWarmUnshown) {
			why = warmUnshownWords
		}
		return fmt.Sprintf("%s: %s; the run's other checks passed; this run's jobs were cancelled by tag\n", row.Name, why)
	}
	if errors.Is(err, ErrSecondNotStarted) {
		var s *SecondNotStartedError
		if errors.As(err, &s) && s.StartWait > 0 {
			return fmt.Sprintf("%s: %s within its start wait of %s from its Allocation's creation; ReplacementLate for its deployment: %s; this run's jobs were cancelled by tag\n", row.Name, secondNotStartedWords, s.StartWait, lateOf(err))
		}
		return fmt.Sprintf("%s: %s; ReplacementLate for its deployment: %s; this run's jobs were cancelled by tag\n", row.Name, secondNotStartedWords, lateOf(err))
	}
	var ns *NotStartedError
	if errors.As(err, &ns) {
		if ns.PastEnd {
			return fmt.Sprintf("%s: a job was still pending after %s, a minute before the deployment's first end; this run's jobs were cancelled by tag\n", row.Name, ns.Bound)
		}
		if ns.StartWait {
			return fmt.Sprintf("%s: a job did not start within the start wait of %s from the deployment's creation; this run's jobs were cancelled by tag\n", row.Name, ns.Bound)
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

// stopsTheRun is whether a scenario's error leaves nothing to judge. ErrSecondNotStarted leaves the first job's part, and
// ErrSlotTaken the predecessor's.
func stopsTheRun(err error) bool {
	return err != nil && !errors.Is(err, ErrSecondNotStarted) && !errors.Is(err, ErrSlotTaken)
}

// secondNotStartedWords say what happened to a run of two jobs, in the marker, which hack/live-checks.sh reads for
// them, and in the text to post of a run whose first job's part failed.
const secondNotStartedWords = "the first job ran and the second never started"

// evidenceKeeper is a Steps that keeps what a failed run leaves to diagnose it from: the live steps keep the stack's logs and the agents'
// logs in the record directory (evidence.go). Run tells it when the run failed in a way the steps did not Fatalf on.
type evidenceKeeper interface{ KeepEvidence() }

// Run is one row from its scenario to its verdict. It hands the scenario a
// Recorder, takes what the scenario saw, reads the cluster's own record of the
// run's jobs, judges both, writes the record and the text to post, and closes
// the steps on every exit. It returns the failed checks; an error means the
// run itself broke (a scenario error, a ledger it could not read, a record it
// could not write), which is a failure too but has no verdict.
func Run(ctx context.Context, cfg Config, st Steps, rec *Recorder, logf func(string, ...any)) (failures []string, err error) {
	defer func() {
		// A run that failed keeps its evidence (issue #503): a failed check or a broken run, not a NOT RUN and not a pass.
		if k, ok := st.(evidenceKeeper); ok && (len(failures) > 0 || (err != nil && !NotRun(cfg.Row, err))) {
			k.KeepEvidence()
		}
		if cerr := st.Close(ctx); cerr != nil {
			if NotRun(cfg.Row, err) {
				// A close that failed may have left a job: that is a failure, never a NOT RUN.
				err = fmt.Errorf("closing the steps after a job that did not start: %v (%v)", cerr, err)
				return
			}
			if Invalid(cfg.Row, err) {
				// The same for an INVALID run: a close that failed is a failure, never an INVALID.
				err = fmt.Errorf("closing the steps after a run with no saturation witness: %v (%v)", cerr, err)
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
		// refused is row A's NOT RUN (nowServeNotRun: a warm start the agent's log did not show): the
		// rest of the run is judged, recorded and posted with the reason, and with no failure the run is NOT RUN.
		refused error
		// invalid is the bulk row's INVALID (ErrInvalid: no saturation witness), judged the same way: with no failure the run is INVALID.
		invalid error
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
	case strings.HasPrefix(cfg.Row.Name, "alpha-athena-now-nooverlap"):
		o, serr := st.NowNoOverlap(ctx, plan)
		if stopsTheRun(serr) {
			return nil, serr
		}
		if errors.Is(serr, ErrSecondNotStarted) {
			second = serr
		}
		refused = nowNoOverlapNotRun(o, serr)
		partial := second != nil || errors.Is(serr, ErrSlotTaken)
		tags, obs = o.Tags, o
		judge = func(j []provider.Job) []string {
			if partial {
				return dedupe(nowNoOverlapFirstJobChecks(cfg.Row, o, j))
			}
			return VerifyNowNoOverlap(cfg.Row, o, j)
		}
		summary = func(f []string, j []provider.Job) string { return nowNoOverlapPost(cfg.Row, o, f, j, refused) }
	case cfg.Row.Name == BetaBulkName:
		o, serr := st.BetaBulk(ctx, plan)
		if serr != nil {
			return nil, serr
		}
		tags, obs = o.Tags, o
		if why := BulkInvalid(o); len(why) > 0 {
			invalid = &InvalidError{Why: why}
		}
		judge = func(j []provider.Job) []string { return VerifyBetaBulk(cfg.Row, o, j) }
		summary = func(f []string, _ []provider.Job) string { return betaBulkPost(cfg.Row, o, f) }
	case cfg.Row.Name == BetaKeeperName:
		o, serr := st.BetaKeeper(ctx, plan)
		if serr != nil {
			return nil, serr
		}
		tags, obs = o.Tags, o
		refused = keeperEndedFirst(o)
		judge = func(j []provider.Job) []string { return VerifyBetaKeeper(cfg.Row, o, j) }
		summary = func(f []string, _ []provider.Job) string { return betaKeeperPost(cfg.Row, o, f) }
	case IsRouterRow(cfg.Row.Name):
		o, serr := st.BetaRouter(ctx, plan)
		if serr != nil {
			// The Helios router row posts its glibc on any outcome (issue #566): a node under the minimum is the reading it exists for.
			// The run is still NOT RUN, with no record: it sent no request, and nothing else of it is judged.
			var g *GlibcTooOldError
			if cfg.Row.PostsGlibcNotRun && errors.As(serr, &g) {
				if perr := rec.SaveText(PostsFile, InsertFrontLine(glibcNotRunPost(cfg.Row, g), cfg.FrontLabel)); perr != nil {
					return nil, fmt.Errorf("the text to post of a node whose glibc is %s could not be written: %w", g.Version, perr)
				}
			}
			return nil, serr
		}
		tags, obs = o.Tags, o
		judge = func(j []provider.Job) []string { return VerifyBetaRouter(cfg.Row, o, j) }
		summary = func(f []string, _ []provider.Job) string { return betaRouterPost(cfg.Row, o, f) }
	case strings.HasPrefix(cfg.Row.Name, "alpha-athena-now-serve"):
		o, serr := st.NowServe(ctx, plan)
		if serr != nil {
			return nil, serr
		}
		refused = nowServeNotRun(o)
		tags, obs = o.Tags, o
		judge = func(j []provider.Job) []string { return VerifyNowServe(cfg.Row, o, j) }
		summary = func(f []string, _ []provider.Job) string { return nowServePost(cfg.Row, o, f) }
	default:
		return nil, fmt.Errorf("row %q is not one this package runs", cfg.Row.Name)
	}
	// Every row's text to post ends with the count of the run's watch resets, whatever its row.
	resets := st.WatchResets()
	rowSummary := summary
	summary = func(f []string, j []provider.Job) string { return rowSummary(f, j) + watchResetsLine(resets) }
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
	switch {
	case len(failures) > 0:
		verdict = "fail"
	case refused != nil:
		verdict = "not run"
	case invalid != nil:
		verdict = "invalid"
	}
	if serr := rec.SaveJSON(RecordFile, map[string]any{
		"row": cfg.Row.Name, "mode": cfg.Mode(), "frontLabel": cfg.FrontLabel, "verdict": verdict, "failures": failures,
		"observation": obs, "jobs": jobs, "watchResets": resets, "finished": time.Now().UTC().Format(time.RFC3339),
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
	if refused != nil && len(failures) == 0 {
		return nil, refused // a warm start or a slot was never put to the test and nothing else failed: NOT RUN, its reason posted
	}
	if invalid != nil && len(failures) == 0 {
		return nil, invalid // the run measured nothing and nothing failed: INVALID, why posted
	}
	return failures, nil
}
