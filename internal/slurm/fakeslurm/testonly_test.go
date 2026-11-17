package fakeslurm

// New (beta; HBEP-4, Core operations, Estimate). The expected texts are worked
// by hand: 1796017690 is #661's probe of 16 Nov 2026, 2026-11-30T05:48:10Z, which
// Europe/Warsaw (CET, +01:00 on that date) prints as 2026-11-30T06:48:10; and
// 1800000000 is 2027-01-15T08:00:00Z.

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
)

const (
	epochLine = "env SLURM_TIME_FORMAT=%s sbatch --test-only --parsable --job-name=hb-0123456789ab --no-requeue --partition=example-gpu " +
		"--account=SEEDACCT --time=10 --nodes=1 --gres=gpu:1 --cpus-per-task=4 --mem=8192M --output=/s/job.out --error=/s/job.err --wrap=true"
	textLine = "sbatch --test-only --parsable --job-name=hb-0123456789ab --no-requeue --partition=example-gpu " +
		"--account=SEEDACCT --time=10 --nodes=1 --gres=gpu:1 --cpus-per-task=4 --mem=8192M --output=/s/job.out --error=/s/job.err --wrap=true"
	probeStart = 1796017690
)

var (
	probeTime = time.Date(2026, 11, 30, 5, 48, 10, 0, time.UTC)
	jan2027   = time.Date(2027, 1, 15, 8, 0, 0, 0, time.UTC)
)

func TestTestOnlyAnswersInEpochSecondsWhenTheVariableIsSet(t *testing.T) {
	for _, tc := range []struct {
		name  string
		start time.Time
		want  string
	}{
		{"the probe's value", probeTime, "to start at 1796017690 using 4 processors"},
		{"a hand-chosen value", jan2027, "to start at 1800000000 using 4 processors"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newRig(t, Config{})
			r.p.SetTestOnlyStart(tc.start)
			out, errOut, code := r.run(epochLine)
			if code != 0 || out != "" || !strings.Contains(errOut, tc.want) {
				t.Fatalf("exit %d, stdout %q, stderr %q, want exit 0, no stdout and %q on stderr", code, out, errOut, tc.want)
			}
			if !strings.HasPrefix(errOut, "sbatch: Job 1001 to start at ") {
				t.Fatalf("stderr %q does not start with sbatch's own prefix and the ID the next submission would get", errOut)
			}
		})
	}
}

func TestTestOnlyAnswersInLocalTextWithoutTheVariable(t *testing.T) {
	r := newRig(t, Config{TimeZone: "Europe/Warsaw"})
	r.p.SetTestOnlyStart(probeTime)
	_, errOut, code := r.run(textLine)
	if code != 0 || !strings.Contains(errOut, "to start at 2026-11-30T06:48:10 using") {
		t.Fatalf("exit %d, stderr %q, want the local text form 2026-11-30T06:48:10 (CET, no zone marker)", code, errOut)
	}
	// The same line with the variable gives the epoch, the same instant.
	if _, errOut, _ := r.run(epochLine); !strings.Contains(errOut, "to start at 1796017690 using") {
		t.Fatalf("with the variable: stderr %q, want the epoch of the same instant", errOut)
	}
}

func TestTestOnlyDefaultsToStartingNow(t *testing.T) {
	r := newRig(t, Config{})
	_, errOut, _ := r.run(epochLine)
	if want := "to start at 1793361600 using"; !strings.Contains(errOut, want) { // 2026-10-30T12:00:00Z, the rig's clock
		t.Fatalf("stderr %q, want %q", errOut, want)
	}
}

// A test submits nothing: no job, no ID taken, no scripted step consumed, no late
// job registered, and the call log tells it from a submission.
func TestTestOnlySubmitsNothingAndTheCallLogTellsItFromASubmit(t *testing.T) {
	r := newRig(t, Config{})
	r.p.Script(Reject("scripted for the next submission"))
	r.p.SetTestOnlyStart(jan2027)
	for i := 0; i < 3; i++ {
		r.must(epochLine)
	}
	if jobs := r.p.Jobs(); len(jobs) != 0 {
		t.Fatalf("%d jobs after three tests, want none", len(jobs))
	}
	if n, nt, ns := r.p.Count("sbatch"), r.p.CountTestOnly(), r.p.CountSubmits(); n != 3 || nt != 3 || ns != 0 {
		t.Fatalf("Count(sbatch) %d, CountTestOnly %d, CountSubmits %d, want 3, 3, 0", n, nt, ns)
	}
	for _, c := range r.p.Calls() {
		if !c.TestOnly() || c.Command()[0] != "sbatch" || c.Words[0] != "env" {
			t.Fatalf("call %v: TestOnly %v, Command %v, want a test-only sbatch run through env", c.Words, c.TestOnly(), c.Command())
		}
	}
	// The step queued before the tests is still the next submission's, and the ID it gets is 1001.
	_, errOut, code := r.run("sbatch --parsable --wrap=true")
	if code != 1 || !strings.Contains(errOut, "scripted for the next submission") {
		t.Fatalf("the first submission: exit %d, stderr %q, want the scripted rejection: a test consumed it", code, errOut)
	}
	if id := r.sbatch(); id != 1001 {
		t.Fatalf("the job ID after three tests is %d, want 1001: a test took IDs", id)
	}
}

// A job whose reply was lost registers when the next SUBMISSION arrives; a test
// is no submission and does not register it.
func TestTestOnlyRegistersNoLateJob(t *testing.T) {
	r := newRig(t, Config{})
	r.p.Script(LostReplyLate(slurm.ErrTimeout))
	if _, err := r.s.Run(context.Background(), slurm.Command{Line: "sbatch --parsable --wrap=true"}); !errors.Is(err, slurm.ErrTimeout) {
		t.Fatalf("the submission with a lost reply: %v, want slurm.ErrTimeout", err)
	}
	r.must(epochLine)
	if n, jobs := r.p.LateJobs(), len(r.p.Jobs()); n != 1 || jobs != 0 {
		t.Fatalf("after a test: LateJobs %d, jobs %d, want 1 and 0", n, jobs)
	}
	r.sbatch()
	if n, jobs := r.p.LateJobs(), len(r.p.Jobs()); n != 0 || jobs != 2 {
		t.Fatalf("after a submission: LateJobs %d, jobs %d, want 0 and 2", n, jobs)
	}
}

// The control of every no-submit check: the same line without --test-only IS a
// submission, so the checks above can fail. An env prefix does not hide it.
func TestAnEnvPrefixedSubmissionIsASubmission(t *testing.T) {
	r := newRig(t, Config{})
	line := strings.Replace(epochLine, "--test-only ", "", 1)
	if out := r.must(line); out != "1001\n" {
		t.Fatalf("the line without --test-only printed %q, want the job ID 1001", out)
	}
	if jobs := r.p.Jobs(); len(jobs) != 1 {
		t.Fatalf("%d jobs, want the one the submission registered", len(jobs))
	}
	if n, nt, ns := r.p.Count("sbatch"), r.p.CountTestOnly(), r.p.CountSubmits(); n != 1 || nt != 0 || ns != 1 {
		t.Fatalf("Count(sbatch) %d, CountTestOnly %d, CountSubmits %d, want 1, 0, 1", n, nt, ns)
	}
	if r.p.Calls()[0].TestOnly() {
		t.Fatal("the submission's call reads as a test")
	}
}

func TestTestOnlyRawAnswersAreVerbatim(t *testing.T) {
	for _, token := range []string{"2026-11-30T06:48:10", "1796017690.5", "-1", "", "N/A"} {
		r := newRig(t, Config{})
		r.p.SetTestOnlyRaw(token)
		_, errOut, code := r.run(epochLine)
		want := "sbatch: Job 1001 to start at " + token + " using 4 processors on nodes fakenode in partition example-gpu\n"
		if code != 0 || errOut != want {
			t.Errorf("token %q: exit %d, stderr %q, want %q", token, code, errOut, want)
		}
	}
}

func TestTestOnlyOutputAndFailure(t *testing.T) {
	r := newRig(t, Config{})
	r.p.SetTestOnlyOutput("sbatch: nothing to say\n")
	if out, errOut, code := r.run(epochLine); code != 0 || out != "" || errOut != "sbatch: nothing to say\n" {
		t.Fatalf("output control: exit %d, stdout %q, stderr %q", code, out, errOut)
	}
	r.p.FailTestOnly("the scheduler cannot answer")
	if out, errOut, code := r.run(epochLine); code != 1 || out != "" || errOut != "sbatch: error: the scheduler cannot answer\n" {
		t.Fatalf("failure control: exit %d, stdout %q, stderr %q, want exit 1", code, out, errOut)
	}
	r.p.SetTestOnlyStart(jan2027) // a later control replaces the failure
	if _, _, code := r.run(epochLine); code != 0 {
		t.Fatalf("after SetTestOnlyStart: exit %d, want 0", code)
	}
}

func TestTestOnlyHangEndsOnTheContextAndOnTheRelease(t *testing.T) {
	r := newRig(t, Config{})
	r.p.SetTestOnlyStart(jan2027)
	g := r.p.HangTestOnly()

	// A deadline that passes while the command waits is the runner's time-out.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	res, err := r.s.Run(ctx, slurm.Command{Line: epochLine})
	if !errors.Is(err, slurm.ErrTimeout) || len(res.Stdout)+len(res.Stderr) != 0 {
		t.Fatalf("hung command = (%+v, %v), want slurm.ErrTimeout and no output", res, err)
	}
	select {
	case <-g.Blocked():
	default:
		t.Fatal("the gate never saw the command wait")
	}
	if n := r.p.CountTestOnly(); n != 1 {
		t.Fatalf("the hung command is in the call log %d times, want 1: it began", n)
	}
	// The hung command held no lock: another command ran while it waited.
	hung := make(chan error, 1)
	go func() {
		_, err := r.s.Run(context.Background(), slurm.Command{Line: epochLine})
		hung <- err
	}()
	for deadline := time.Now().Add(30 * time.Second); r.p.CountTestOnly() < 2; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the second test never began")
		}
	}
	r.must("printenv HOME")
	g.Release()
	if err := <-hung; err != nil {
		t.Fatalf("released command: %v", err)
	}
}

func TestSbatchGateHoldsAnEnvPrefixedTest(t *testing.T) {
	r := newRig(t, Config{})
	g := r.p.Gate("sbatch")
	done := make(chan struct{})
	go func() {
		r.s.Run(context.Background(), slurm.Command{Line: epochLine})
		close(done)
	}()
	g.Wait() // the line starts with env, and the gate still holds it, as it holds its sbatch
	g.Release()
	<-done
}

func TestTestOnlyAppliesThePartitionPolicyAThrowawaySubmissionWould(t *testing.T) {
	r := newRig(t, Config{Partitions: []Partition{{Name: "example-gpu", GPUOnly: true}}})
	_, errOut, code := r.run(strings.Replace(epochLine, "--gres=gpu:1 ", "", 1))
	if code != 1 || !strings.Contains(errOut, reasonGPUOnly) {
		t.Fatalf("exit %d, stderr %q, want the GPU-only refusal", code, errOut)
	}
}

func TestEnvAcceptsOnlyWhatThePluginBuilds(t *testing.T) {
	r := newRig(t, Config{})
	for _, tc := range []struct {
		line string
		code int
		text string
	}{
		{"env", 2, "the fake supports only: env SLURM_TIME_FORMAT=%s sbatch"},
		{"env SLURM_TIME_FORMAT=%s", 2, "the fake supports only"},
		{"env SLURM_TIME_FORMAT=%s squeue --me --json", 2, "the fake supports only"},
		{"env OTHER=1 sbatch --wrap=true", 2, "the fake supports only"},
		{"env SLURM_TIME_FORMAT=%s OTHER=1 sbatch --wrap=true", 2, "the fake supports only"},
		{"env SLURM_TIME_FORMAT=standard sbatch --test-only --wrap=true", 1, `reads only SLURM_TIME_FORMAT=%s, not "standard"`},
	} {
		_, errOut, code := r.run(tc.line)
		if code != tc.code || !strings.Contains(errOut, tc.text) {
			t.Errorf("%q: exit %d, stderr %q, want exit %d with %q", tc.line, code, errOut, tc.code, tc.text)
		}
	}
	if n := len(r.p.Jobs()); n != 0 {
		t.Errorf("%d jobs after refused env lines", n)
	}
}
