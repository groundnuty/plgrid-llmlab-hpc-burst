package alpha

// Issue #303: two lines of the live steps change only what the watch's own rule does at the end of a test, and with it the row's
// outcome: the run's word that its resets decide nothing, said by firstJobStarted for a first job that never started, and never by
// secondNeverFinal for a second one. Until now only tests that read the source held them (budget_test.go); these run the behaviour. Each
// drives the real waiter over a real stack.Recorder that follows watch_resets_test.go's fake API server, whose script resets one stream,
// through Run; then it runs the Recorder's clean-up itself (capturingT) and reads what that reported. The row's outcome is what runRow and
// hack/live-checks.sh make of the two together (rowOutcome).
//
// Three verifications would prove nothing here, and these tests use none of them: a liveRun with no watch (the word is a no-op on a nil
// Recorder, which is why the earlier tests of these waiters could not see either line); a Recorder with no reset (its clean-up is silent
// whoever said what); and a reading of the source for the call.

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// rowOutcome is how a row's test ends. From Run's result, as runRow (alpha_live_test.go) ends it: a skip for NOT RUN, a failure for an
// error or a failed check. Any error the test reported besides, here the watch's clean-up at its end, fails the test whatever Run said,
// and hack/live-checks.sh reads NOT RUN only from a skip with no failed test (not_run_verdict), PASS only from a pass.
func rowOutcome(row Row, failures []string, err error, testErrs []string) string {
	switch {
	case len(testErrs) > 0:
		return "FAIL"
	case NotRun(row, err):
		return "NOT RUN"
	case err != nil || len(failures) > 0:
		return "FAIL"
	}
	return "PASS"
}

// watchWithResets is a real Recorder following a fake API server that resets the streams plan names before the run, its clean-up held by
// the capturingT it returns; the Recorder's resets are all in when it returns, and the test's own clean-up ends the watch.
func watchWithResets(t *testing.T, plan resetPlan) (*stack.Recorder, *capturingT) {
	t.Helper()
	if sumOf(plan.late) != 0 {
		t.Fatal("watchWithResets has no gate: every reset of its plan comes before the run")
	}
	const ns = "alpha-rehearsal"
	ct := &capturingT{T: t}
	rec := stack.StartWatch(ct, fakeAPIServer(t, ns, plan, nil), ns, nil)
	// At once, whatever the test does next, as in watch_resets_test.go: a step that fails the test must still end the watch.
	t.Cleanup(func() { ct.finish() })
	waitForResets(t, rec, plan.total(), "before the run")
	return rec, ct
}

// firstClockSteps are each row's scenario up to its first wait, on a liveRun with no stack: firstReady, the real waiter, which begins with
// firstJobStarted, as every row's scenario begins once its stack is up (live_flows_test.go).
type firstClockSteps struct{ *liveRun }

func (s firstClockSteps) Handover(_ context.Context, p Plan) (HandoverObs, error) {
	_, err := s.firstReady(p)
	return HandoverObs{}, err
}

func (s firstClockSteps) NoOverlap(_ context.Context, p Plan) (NoOverlapObs, error) {
	_, err := s.firstReady(p)
	return NoOverlapObs{}, err
}

func (s firstClockSteps) TunnelHour(_ context.Context, p Plan) (TunnelHourObs, error) {
	_, err := s.firstReady(p)
	return TunnelHourObs{}, err
}

func (s firstClockSteps) NowServe(_ context.Context, p Plan) (NowServeObs, error) {
	_, err := s.firstReady(p)
	return NowServeObs{}, err
}

func (s firstClockSteps) NowNoOverlap(_ context.Context, p Plan) (NowNoOverlapObs, error) {
	_, err := s.firstReady(p)
	return NowNoOverlapObs{}, err
}

func (s firstClockSteps) BetaKeeper(_ context.Context, p Plan) (KeeperObs, error) {
	_, err := s.firstReady(p)
	return KeeperObs{}, err
}

func (s firstClockSteps) BetaRouter(_ context.Context, p Plan) (RouterObs, error) {
	_, err := s.firstReady(p)
	return RouterObs{}, err
}

// A run whose first job never started ends NOT RUN on the scheduler's read, and judges nothing it read from the watch: so a reset of the
// watch leaves it NOT RUN, and the watch's clean-up logs the reset and fails nothing. The reset here is of the Allocation stream, the one
// the first job's wait reads. The run's deployment was created an hour ago, so the first clock has run out at the wait's first look.
func TestAResetLeavesARunWhoseFirstJobNeverStartedNotRun(t *testing.T) {
	plan := resetPlan{now: map[string]int{"allocations": 1}}
	waiting := waitingJob("p", v1alpha1.AllocationPending)
	for _, name := range []string{"alpha-athena-handover", "alpha-helios-nooverlap", "tunnel-hour-athena"} {
		t.Run(name, func(t *testing.T) {
			row := rowOf(name)
			rec, ct := watchWithResets(t, plan)
			r := &liveRun{t: t, guardFail: make(chan struct{}), limit: row.Lease, dw: 15 * time.Minute, created: time.Now().Add(-time.Hour), watch: rec,
				lookFn: func(context.Context) (Look, error) { return Look{Clock: guardAt, Jobs: []provider.Job{waiting}}, nil }}
			cfg := Config{Row: row, Out: filepath.Join(t.TempDir(), "out"), PendingMax: 10 * time.Minute, FrontLabel: testFrontLabel}
			tags, err := NewRecorder(cfg.Out)
			if err != nil {
				t.Fatal(err)
			}
			failures, runErr := Run(context.Background(), cfg, firstClockSteps{r}, tags, t.Logf)
			var ns *NotStartedError
			if !errors.As(runErr, &ns) || len(failures) != 0 {
				t.Fatalf("Run = failures %v, err %v; want the first clock run out on a job the scheduler shows waiting", failures, runErr)
			}
			if n := rec.ResetCount(); n != plan.total() {
				t.Fatalf("the watch counts %d resets, want the plan's %d: with none, its clean-up is silent whatever the run said", n, plan.total())
			}
			errs := ct.finish()
			if got := rowOutcome(row, failures, runErr, errs); got != "NOT RUN" {
				t.Errorf("the row ends %s, want NOT RUN: Run ended %v, and the watch's clean-up reported %q", got, runErr, errs)
			}
		})
	}
}

// secondReadSteps are a two-job row whose first job's part is the dry run's, one that passes, and whose read of the second job is the real
// waiter's: guardResult, whose read at its bound is secondNeverFinal, on a liveRun whose watch is a real Recorder.
type secondReadSteps struct {
	*drySteps
	live *guardedRun
}

func (s secondReadSteps) Handover(ctx context.Context, p Plan) (HandoverObs, error) {
	o, err := s.drySteps.Handover(ctx, p)
	if !errors.Is(err, ErrSecondNotStarted) {
		s.t.Fatalf("the dry run's first job's part ended %v, want one beside a second job that never started", err)
	}
	s.live.startGuard()
	return o, s.live.guardResult()
}

// A run whose second job never started still judges its first job's part from the watch, so a reset of the watch fails the test at its end
// where the run alone is NOT RUN: the script reads FAIL. The reset is of the ModelDeployment stream, which the handover's result catch-up
// does not cover (it covers the Instances and the Allocations, and a reset of those ends the run there), so the watch's own rule is all
// that keeps such a reset from passing for NOT RUN. With no reset the same steps end NOT RUN, so the FAIL is the reset's.
func TestAResetFailsARunWhoseSecondJobNeverStarted(t *testing.T) {
	row := rowOf("alpha-athena-handover")
	first := guardJob("p", guardAt)
	for _, c := range []struct {
		name string
		plan resetPlan
		want string
	}{
		{"a reset of the ModelDeployment stream", resetPlan{now: map[string]int{"modeldeployments": 1}}, "FAIL"},
		{"no reset", resetPlan{}, "NOT RUN"},
	} {
		t.Run(c.name, func(t *testing.T) {
			rec, ct := watchWithResets(t, c.plan)
			g := newGuardedRun(t, func(int) (Look, error) {
				return Look{Clock: guardAt, Jobs: []provider.Job{first, waitingJob("s", v1alpha1.AllocationPending)}}, nil
			}, written)
			// The guard's goroutine never looks (an interval of an hour), and the wait for its final end is over at once: the one look is
			// secondNeverFinal's own.
			g.guardEvery, g.resultWait = time.Hour, time.Nanosecond
			g.c = crfake.NewClientBuilder().WithScheme(eventScheme(t)).Build()
			g.w.md = &v1alpha1.ModelDeployment{ObjectMeta: metav1.ObjectMeta{Name: "alpha-0a1b2c3d4e", Namespace: "default"}}
			g.watch = rec
			cfg := Config{Row: row, Dry: true, DryFault: "second-never-started", Out: filepath.Join(t.TempDir(), "out"), PendingMax: 10 * time.Minute, FrontLabel: testFrontLabel}
			tags, err := NewRecorder(cfg.Out)
			if err != nil {
				t.Fatal(err)
			}
			failures, runErr := Run(context.Background(), cfg, secondReadSteps{newDrySteps(t, cfg, familyOf(row.Name)), g}, tags, t.Logf)
			if !errors.Is(runErr, ErrSecondNotStarted) || len(failures) != 0 {
				t.Fatalf("Run = failures %v, err %v; want the NOT RUN of a second job that never started, the first job's part passing", failures, runErr)
			}
			if n := rec.ResetCount(); n != c.plan.total() {
				t.Fatalf("the watch counts %d resets, want the plan's %d", n, c.plan.total())
			}
			errs := ct.finish()
			if got := rowOutcome(row, failures, runErr, errs); got != c.want {
				t.Errorf("the row ends %s, want %s: Run ended %v, and the watch's clean-up reported %q", got, c.want, runErr, errs)
			}
			if c.want == "FAIL" && (len(errs) != 1 || !strings.Contains(errs[0], "not known to be complete")) {
				t.Errorf("the watch's clean-up reported %q, want the one error that what was read from it is not known to be complete", errs)
			}
		})
	}
}
