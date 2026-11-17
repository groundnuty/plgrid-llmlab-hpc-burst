package slurm_test

// Estimate over the fake Slurm (beta; HBEP-4, Core operations, Estimate; #661).
// The expected starts are literals worked out by hand, never read back from the
// plugin's own parser: 1796017690 is #661's probe of 16 Nov 2026, which is
// 2026-11-30T05:48:10Z, and 1800000000 is 2027-01-15T08:00:00Z (both cross-checked
// with date -u -d @N while writing). The fake is told the digits as a raw token,
// so the plugin reads them as it would read a real reply.

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

const estDigest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

// recordingRunner remembers the commands that reach the platform.
type recordingRunner struct {
	*fakeslurm.Session
	cmds []slurm.Command
}

func (r *recordingRunner) Run(ctx context.Context, cmd slurm.Command) (slurm.Result, error) {
	r.cmds = append(r.cmds, cmd)
	return r.Session.Run(ctx, cmd)
}

type estimateRig struct {
	batch  *slurm.Batch
	plat   *fakeslurm.Platform
	runner *recordingRunner
	// mayEffect lifts the no-effect check that every rig makes when its test ends:
	// the control that submits on purpose.
	mayEffect bool
}

// newEstimateRig is a Batch over the fake whose profile reads the estimate as
// epoch seconds when decided is true, and says nothing about it otherwise.
func newEstimateRig(t *testing.T, decided bool) *estimateRig {
	t.Helper()
	clock := fake.NewClock(fakeStart)
	plat := fakeslurm.New(clock, fakeslurm.Config{Partitions: []fakeslurm.Partition{{Name: fakePart}}})
	sess := plat.Login(fakeslurm.Login{User: "example-user", Env: map[string]string{"HOME": fakeHome, "SCRATCH": fakeScratch}})
	runner := &recordingRunner{Session: sess}
	profile := exampleProfile("$SCRATCH/stage")
	if decided {
		profile.Spec.StartEstimateTime = v1alpha1.StartEstimateEpochSeconds
	}
	settings := provider.Settings{Provider: "example", Account: "SEEDACCT", Partition: fakePart}
	r := &estimateRig{batch: slurm.NewBatch(runner, profile, settings, slurm.Options{Clock: clock}), plat: plat, runner: runner}
	// E8 holds for every test, whatever else it asserts and wherever it stops: a test that fails on a wrong
	// answer still reports a submission the fake recorded.
	t.Cleanup(func() {
		if !r.mayEffect {
			r.wantNoEffect(t)
		}
	})
	return r
}

func estimateRequest() provider.SubmitRequest {
	return provider.SubmitRequest{
		Tag: "hb-example-0123456789ab", Hold: true, Account: "SEEDACCT",
		Params: v1alpha1.BatchParameters{Partition: fakePart, TimeLimit: "30m", GPUs: 1, CPUsPerTask: 8,
			Memory: resource.MustParse("16Gi"), Nodes: 1, AgentDigest: estDigest},
	}
}

// wantNoEffect is E8: nothing was submitted, cancelled, released, written or
// held. The call log holds only the login-node reads that resolve the paths and
// the estimate's own commands; the control (a submission that does all of it)
// is TestAnEstimateLineWithoutTestOnlyWouldSubmit.
func (r *estimateRig) wantNoEffect(t *testing.T) {
	t.Helper()
	if n := len(r.plat.Jobs()); n != 0 {
		t.Errorf("the platform holds %d jobs after estimates, want none", n)
	}
	if n := r.plat.CountSubmits(); n != 0 {
		t.Errorf("%d submissions in the call log, want none", n)
	}
	for _, c := range r.plat.Calls() {
		if c.Command()[0] != "printenv" && !c.TestOnly() {
			t.Errorf("call %v is neither a path read nor an sbatch --test-only", c.Words)
		}
	}
	if n := len(r.plat.Files()); n != 0 {
		t.Errorf("the platform's file system holds %d files after estimates, want none", n)
	}
}

func TestEstimateReadsEpochSecondsAsUTC(t *testing.T) {
	for _, tc := range []struct {
		name  string
		token string
		want  time.Time
	}{
		{"E1, the probe's value", "1796017690", time.Date(2026, 11, 30, 5, 48, 10, 0, time.UTC)},
		{"E2, hand-chosen", "1800000000", time.Date(2027, 1, 15, 8, 0, 0, 0, time.UTC)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newEstimateRig(t, true)
			r.plat.SetTestOnlyRaw(tc.token)
			res, err := r.batch.Estimate(context.Background(), estimateRequest())
			if err != nil {
				t.Fatalf("Estimate: %v", err)
			}
			if res.Start == nil || !res.Start.Equal(tc.want) || res.Start.Location() != time.UTC || res.Unknown != "" {
				t.Fatalf("Estimate = %+v, want the start %v (UTC) and no reason", res, tc.want)
			}
			if !res.ReadAt.Equal(fakeStart) {
				t.Fatalf("ReadAt = %v, want the clock's %v", res.ReadAt, fakeStart)
			}
		})
	}
}

// The fake's own formatting reaches the same instant: the start is epoch
// seconds when the plugin sets SLURM_TIME_FORMAT=%s, and the text form (which the
// plugin must not read) would not be.
func TestEstimateAgainstTheFakesOwnEpochFormat(t *testing.T) {
	r := newEstimateRig(t, true)
	want := time.Date(2027, 1, 15, 8, 0, 0, 0, time.UTC)
	r.plat.SetTestOnlyStart(want)
	res, err := r.batch.Estimate(context.Background(), estimateRequest())
	if err != nil || res.Start == nil || !res.Start.Equal(want) {
		t.Fatalf("Estimate = (%+v, %v), want %v", res, err, want)
	}
}

func TestEstimateAnswerThatIsNotWholeEpochSecondsIsUnknown(t *testing.T) {
	for _, tc := range []struct{ name, token string }{
		{"E3, the local text form", "2026-11-30T06:48:10"},
		{"E4, a fraction", "1796017690.5"},
		{"E4, negative", "-1"},
		{"E4, empty", ""},
		{"E4, N/A", "N/A"},
		{"a plus sign", "+1796017690"},
		{"zero, the epoch itself", "0"},
		{"more digits than any date", "99999999999999999999"},
		{"past year 9999", "253402300800"},
		{"a unit", "1796017690s"},
		{"digits beyond ASCII", "١٧٩٦٠١٧٦٩٠"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newEstimateRig(t, true)
			r.plat.SetTestOnlyRaw(tc.token)
			res, err := r.batch.Estimate(context.Background(), estimateRequest())
			if err != nil {
				t.Fatalf("Estimate: %v (an answer that cannot be read is an unknown result, not an error)", err)
			}
			if res.Start != nil || res.Unknown != provider.EstimateUnreadable {
				t.Fatalf("Estimate = %+v, want no start and the unreadable reason: never a time, never zero", res)
			}
		})
	}
}

func TestEstimateReplyWithNoStartLineIsUnknown(t *testing.T) {
	r := newEstimateRig(t, true)
	r.plat.SetTestOnlyOutput("sbatch: Job 1001 is queued\n")
	res, err := r.batch.Estimate(context.Background(), estimateRequest())
	if err != nil || res.Start != nil || res.Unknown != provider.EstimateNoAnswer {
		t.Fatalf("Estimate = (%+v, %v), want no start and the no-answer reason", res, err)
	}
}

func TestEstimateFailureIsAnErrorNeverATime(t *testing.T) {
	r := newEstimateRig(t, true)
	r.plat.FailTestOnly("Unable to allocate resources: Invalid account or account/partition combination specified")
	res, err := r.batch.Estimate(context.Background(), estimateRequest())
	if err == nil || res.Start != nil {
		t.Fatalf("Estimate = (%+v, %v), want an error and no start (E5)", res, err)
	}
	if strings.Contains(err.Error(), "SEEDACCT") {
		t.Fatalf("the error %q names the account", err)
	}
}

// E6: a command that does not answer ends on the call's context as the
// runner's time-out, and the line carries provider.EstimateTimeout as its own
// bound for a real runner.
func TestEstimateHangIsATimeoutNeverATime(t *testing.T) {
	r := newEstimateRig(t, true)
	g := r.plat.HangTestOnly()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	res, err := r.batch.Estimate(ctx, estimateRequest())
	if !errors.Is(err, slurm.ErrTimeout) || res.Start != nil {
		t.Fatalf("Estimate = (%+v, %v), want slurm.ErrTimeout and no start (E6)", res, err)
	}
	select {
	case <-g.Blocked():
	default:
		t.Fatal("the command never waited at the hang")
	}
	var bound time.Duration
	for _, c := range r.runner.cmds {
		if strings.HasPrefix(c.Line, "env ") {
			bound = c.Timeout
		}
	}
	if bound != provider.EstimateTimeout || provider.EstimateTimeout != time.Minute {
		t.Fatalf("the estimate's command carries Timeout %v, want %v (HBEP-10's 60 s)", bound, time.Minute)
	}
}

// E7: a platform whose profile does not decide the reading is unknown, with the
// gate's reason, and NO command runs: not the sbatch, and not even a path read.
func TestEstimateOnAnUndecidedPlatformRunsNoCommand(t *testing.T) {
	r := newEstimateRig(t, false)
	r.plat.SetTestOnlyRaw("1796017690") // an answer that would be read, were the command run
	res, err := r.batch.Estimate(context.Background(), estimateRequest())
	if err != nil || res.Start != nil || res.Unknown != provider.EstimateUndecided {
		t.Fatalf("Estimate = (%+v, %v), want unknown with provider.EstimateUndecided", res, err)
	}
	if want := "this platform's probe has not decided how to read the estimate (HBEP-4, Estimate)"; res.Unknown != want {
		t.Fatalf("the reason is %q, want %q", res.Unknown, want)
	}
	if n := len(r.plat.Calls()); n != 0 {
		t.Fatalf("the platform's call log holds %d calls, want none (E7)", n)
	}
	if n := len(r.runner.cmds); n != 0 {
		t.Fatalf("%d commands reached the runner, want none (E7)", n)
	}
	if !res.ReadAt.Equal(fakeStart) {
		t.Fatalf("ReadAt = %v, want the clock's %v", res.ReadAt, fakeStart)
	}
}

// The line is Submit's, without --hold, with --test-only and the epoch format:
// each word is written out by hand.
func TestEstimateLineIsSubmitsWithoutHold(t *testing.T) {
	r := newEstimateRig(t, true)
	r.plat.SetTestOnlyRaw("1796017690")
	if _, err := r.batch.Estimate(context.Background(), estimateRequest()); err != nil { // the request has Hold set
		t.Fatalf("Estimate: %v", err)
	}
	var got []string
	for _, c := range r.plat.Calls() {
		if c.TestOnly() {
			got = c.Words
		}
	}
	wantHead := []string{
		"env", "SLURM_TIME_FORMAT=%s", "sbatch", "--test-only", "--parsable", "--job-name=hb-example-0123456789ab", "--no-requeue",
		"--partition=example-gpu", "--account=SEEDACCT", "--time=30", "--nodes=1", "--gres=gpu:1", "--cpus-per-task=8", "--mem=16384M",
	}
	if len(got) != len(wantHead)+3 || !slices.Equal(got[:len(wantHead)], wantHead) {
		t.Fatalf("the estimate's words\n got  %q\n want %q followed by --output, --error and --wrap", got, wantHead)
	}
	for i, prefix := range []string{"--output=" + fakeScratch + "/stage/", "--error=" + fakeScratch + "/stage/", "--wrap="} {
		if !strings.HasPrefix(got[len(wantHead)+i], prefix) {
			t.Errorf("word %d is %q, want it to start with %q", len(wantHead)+i, got[len(wantHead)+i], prefix)
		}
	}
	if slices.Contains(got, "--hold") {
		t.Fatal("the estimate carries --hold")
	}
}

// E8's control: the line with --test-only taken out IS a submission, and the
// fake records it as one, so the no-effect checks above can fail. This is what a
// regression that dropped --test-only from estimateLine would do.
func TestAnEstimateLineWithoutTestOnlyWouldSubmit(t *testing.T) {
	r := newEstimateRig(t, true)
	r.plat.SetTestOnlyRaw("1796017690")
	if _, err := r.batch.Estimate(context.Background(), estimateRequest()); err != nil {
		t.Fatalf("Estimate: %v", err)
	}
	var line string
	for _, c := range r.runner.cmds {
		if strings.HasPrefix(c.Line, "env ") {
			line = c.Line
		}
	}
	mutated := strings.Replace(line, " --test-only", "", 1)
	if mutated == line {
		t.Fatalf("the estimate's line %q has no --test-only", line)
	}
	other := newEstimateRig(t, true)
	other.mayEffect = true // it submits on purpose
	if _, err := other.runner.Session.Run(context.Background(), slurm.Command{Line: mutated}); err != nil {
		t.Fatal(err)
	}
	if n := len(other.plat.Jobs()); n != 1 {
		t.Fatalf("%d jobs after the mutated line, want 1: the fake must record a submission", n)
	}
	if n := other.plat.CountSubmits(); n != 1 {
		t.Fatalf("%d submissions after the mutated line, want 1", n)
	}
	other.batch.Estimate(context.Background(), estimateRequest()) // the real line, for the contrast
	if n := other.plat.CountTestOnly(); n != 1 {
		t.Fatalf("%d test-only calls, want 1", n)
	}
}

func TestEstimateRefusesAnInvalidRequestBeforeAnyCall(t *testing.T) {
	r := newEstimateRig(t, true)
	req := estimateRequest()
	req.Params.Partition = "example-other"
	res, err := r.batch.Estimate(context.Background(), req)
	if !errors.Is(err, provider.ErrInvalidRequest) || res.Start != nil {
		t.Fatalf("Estimate = (%+v, %v), want a refusal wrapping ErrInvalidRequest", res, err)
	}
	if n := len(r.plat.Calls()); n != 0 {
		t.Fatalf("%d calls reached the platform, want none", n)
	}
}

func TestEstimateWithUnresolvedPathsIsAnError(t *testing.T) {
	r := newEstimateRig(t, true)
	profile := exampleProfile("$NO_SUCH_DIR/stage")
	profile.Spec.StartEstimateTime = v1alpha1.StartEstimateEpochSeconds
	r.batch.Configure(profile, provider.Settings{Provider: "example", Account: "SEEDACCT", Partition: fakePart})
	_, err := r.batch.Estimate(context.Background(), estimateRequest())
	if !errors.Is(err, provider.ErrPathUnresolved) {
		t.Fatalf("Estimate: %v, want an error wrapping ErrPathUnresolved", err)
	}
	if n := r.plat.CountTestOnly(); n != 0 {
		t.Fatalf("%d estimates ran with the paths unresolved, want none", n)
	}
}

// Estimate is on provider.Batch, so the controller's Platform has it, and
// both implementers answer alike for the same fixtures.
func TestBothImplementersAnswerTheSameEstimate(t *testing.T) {
	r := newEstimateRig(t, true)
	r.plat.SetTestOnlyRaw("1800000000")
	real, err := r.batch.Estimate(context.Background(), estimateRequest())
	if err != nil {
		t.Fatal(err)
	}
	fb := fake.NewBatch(fake.NewClock(fakeStart), fake.Config{Partitions: []fake.Partition{
		{Name: fakePart, GPUModel: "example-gpu-model", GPUsPerNode: 4, MaxTime: time.Hour},
	}})
	fb.SetEstimate(time.Date(2027, 1, 15, 8, 0, 0, 0, time.UTC))
	var _ provider.Batch = fb
	fres, err := fb.Estimate(context.Background(), estimateRequest())
	if err != nil {
		t.Fatal(err)
	}
	if real.Start == nil || fres.Start == nil || !real.Start.Equal(*fres.Start) {
		t.Fatalf("the plugin read %+v, the fake answered %+v: they must agree", real, fres)
	}
}
