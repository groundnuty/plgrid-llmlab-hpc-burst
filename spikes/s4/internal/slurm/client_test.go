package slurm

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"
)

// fakeRunner returns canned output and records every command it was given.
type fakeRunner struct {
	calls  [][]string // name followed by args
	stdout string
	stderr string
	err    error
}

func (f *fakeRunner) Run(_ context.Context, name string, args ...string) ([]byte, []byte, error) {
	f.calls = append(f.calls, append([]string{name}, args...))
	return []byte(f.stdout), []byte(f.stderr), f.err
}

// processErr returns the error a real process produces, so that the tests
// see a genuine *exec.ExitError. wantCode is the exit code it must have, or
// -1 for a process killed by a signal; the fixture checks itself.
func processErr(t *testing.T, script string, wantCode int) error {
	t.Helper()
	err := exec.Command("sh", "-c", script).Run()
	var ee *exec.ExitError
	if !errors.As(err, &ee) {
		t.Fatalf("fixture %q: want an *exec.ExitError, got %T %v", script, err, err)
	}
	if ee.ExitCode() != wantCode || (wantCode == -1) == ee.Exited() {
		t.Fatalf("fixture %q: exit code %d, exited %v; want code %d", script, ee.ExitCode(), ee.Exited(), wantCode)
	}
	return err
}

const (
	testTag      = "hb-alloc-a7656bce1293"
	stderrReject = "sbatch: error: Batch job submission failed: Invalid account or account/partition combination specified"
	stderrSocket = "sbatch: error: Batch job submission failed: Socket timed out on send/recv operation"
	stderrSSH    = "ssh: connect to host x port 22: Connection timed out"
)

func TestSubmitOutcome(t *testing.T) {
	live := func() (context.Context, context.CancelFunc) { return context.Background(), func() {} }
	cancelled := func() (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		return ctx, cancel
	}
	expired := func() (context.Context, context.CancelFunc) {
		return context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	}
	cases := []struct {
		name   string
		ctx    func() (context.Context, context.CancelFunc)
		stdout string
		stderr string
		err    func(t *testing.T) error
		want   Outcome
		wantID int64
	}{
		{name: "job id", stdout: "1234\n", want: Accepted, wantID: 1234},
		{name: "job id with cluster", stdout: "1234;cluster\n", want: Accepted, wantID: 1234},
		{name: "garbage on exit 0", stdout: "garbage", want: Unknown},
		{name: "nothing on exit 0", stdout: "", want: Unknown},
		{name: "job id zero", stdout: "0\n", want: Unknown},
		{name: "invalid account", stderr: stderrReject, err: exit(1), want: Rejected},
		{name: "invalid partition", stderr: "sbatch: error: Batch job submission failed: Invalid partition name specified", err: exit(1), want: Rejected},
		{name: "qos limit", stderr: "sbatch: error: Batch job submission failed: Job violates accounting/QOS policy (job submit limit, user's size and/or time limits)", err: exit(1), want: Rejected},
		{name: "socket timed out", stderr: stderrSocket, err: exit(1), want: Unknown},
		{name: "controller unreachable", stderr: "sbatch: error: Batch job submission failed: Unable to contact slurm controller (connect failure)", err: exit(1), want: Unknown},
		{name: "zero bytes", stderr: "sbatch: error: Batch job submission failed: Zero Bytes were transmitted or received", err: exit(1), want: Unknown},
		{name: "ssh transport error", stderr: stderrSSH, err: exit(255), want: Unknown},
		{name: "exit 1 without a reason", err: exit(1), want: Unknown},
		// The signal row carries a definite-rejection text, so only the
		// signal check can make it Unknown.
		{name: "killed by a signal", stderr: stderrReject, err: func(t *testing.T) error { return processErr(t, "kill -9 $$", -1) }, want: Unknown},
		// The context rows carry a definite-rejection text and an ExitError
		// that would be Rejected with a live context.
		{name: "cancelled context", ctx: cancelled, stderr: stderrReject, err: exit(1), want: Unknown},
		{name: "expired deadline", ctx: expired, stderr: stderrReject, err: exit(1), want: Unknown},
		{name: "expired deadline, plain error", ctx: expired, err: func(*testing.T) error { return context.DeadlineExceeded }, want: Unknown},
		{name: "command not found", err: func(*testing.T) error { return fmt.Errorf("exec: %w", exec.ErrNotFound) }, want: Rejected},
		{name: "real lookup failure", err: func(*testing.T) error { return exec.Command("hb-no-such-command-xyz").Run() }, want: Rejected},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ctxf := c.ctx
			if ctxf == nil {
				ctxf = live
			}
			ctx, cancel := ctxf()
			defer cancel()
			r := &fakeRunner{stdout: c.stdout, stderr: c.stderr}
			if c.err != nil {
				r.err = c.err(t)
			}
			id, got, err := (&Client{R: r}).Submit(ctx, SubmitRequest{Tag: testTag, Hold: true})
			if got != c.want || id != c.wantID {
				t.Errorf("got %v id %d, want %v id %d (err %v)", got, id, c.want, c.wantID, err)
			}
			if (got == Accepted) != (err == nil) {
				t.Errorf("outcome %v with error %v: an error must accompany every outcome except Accepted", got, err)
			}
			if len(r.calls) != 1 || r.calls[0][0] != "sbatch" {
				t.Errorf("runner calls %v, want exactly one sbatch", r.calls)
			}
		})
	}
}

// exit returns a fixture that yields a real *exec.ExitError with that code.
func exit(code int) func(*testing.T) error {
	return func(t *testing.T) error { return processErr(t, fmt.Sprintf("exit %d", code), code) }
}

func TestSubmitWithoutTagNeverRuns(t *testing.T) {
	r := &fakeRunner{stdout: "1234\n"}
	_, got, err := (&Client{R: r}).Submit(context.Background(), SubmitRequest{})
	if got != Rejected || err == nil || len(r.calls) != 0 {
		t.Errorf("got %v, err %v, calls %v; want Rejected, an error and no command", got, err, r.calls)
	}
}

func TestSubmitArgsHold(t *testing.T) {
	held := SubmitArgs(SubmitRequest{Tag: testTag, Hold: true})
	for _, want := range []string{"--hold", "--no-requeue", "--job-name=" + testTag, "--parsable"} {
		if !slices.Contains(held, want) {
			t.Errorf("Hold=true: %q missing from %v", want, held)
		}
	}
	free := SubmitArgs(SubmitRequest{Tag: testTag})
	if slices.Contains(free, "--hold") {
		t.Errorf("Hold=false: --hold present in %v", free)
	}
	if !slices.Contains(free, "--no-requeue") {
		t.Errorf("Hold=false: --no-requeue missing from %v", free)
	}
}

func TestSubmitPassesHoldToSbatch(t *testing.T) {
	r := &fakeRunner{stdout: "1234\n"}
	if _, _, err := (&Client{R: r}).Submit(context.Background(), SubmitRequest{Tag: testTag, Hold: true}); err != nil {
		t.Fatal(err)
	}
	call := r.calls[0]
	for _, want := range []string{"--hold", "--no-requeue", "--job-name=" + testTag} {
		if !slices.Contains(call[1:], want) {
			t.Errorf("sbatch argv %v lacks %q", call, want)
		}
	}
}

func TestReleaseRunsScontrolRelease(t *testing.T) {
	r := &fakeRunner{}
	if err := (&Client{R: r}).Release(context.Background(), 1234); err != nil {
		t.Fatal(err)
	}
	if want := []string{"scontrol", "release", "1234"}; len(r.calls) != 1 || !slices.Equal(r.calls[0], want) {
		t.Errorf("argv %v, want one call %v", r.calls, want)
	}
}

func TestReleaseReportsFailure(t *testing.T) {
	r := &fakeRunner{stderr: "scontrol: error: Access/permission denied", err: exit(1)(t)}
	err := (&Client{R: r}).Release(context.Background(), 1234)
	if err == nil || !strings.Contains(err.Error(), "1234") || !strings.Contains(err.Error(), "Access/permission denied") {
		t.Errorf("err %v, want it to name job 1234 and the stderr", err)
	}
}

// squeue prints 1003, 1002 and 1001 (descending) and a job of another name.
const squeueDescending = `{"jobs":[
{"job_id":1003,"name":"hb-alloc-a7656bce1293","job_state":["PENDING"]},
{"job_id":1500,"name":"hb-other-000000000000","job_state":["RUNNING"]},
{"job_id":1002,"name":"hb-alloc-a7656bce1293","job_state":["RUNNING"]},
{"job_id":1001,"name":"hb-alloc-a7656bce1293","job_state":["PENDING"]}],
"meta":{},"errors":[],"warnings":[]}`

func TestSqueueByTagSortsByIDAndFilters(t *testing.T) {
	r := &fakeRunner{stdout: squeueDescending}
	jobs, err := (&Client{R: r}).SqueueByTag(context.Background(), testTag)
	if err != nil {
		t.Fatal(err)
	}
	var ids []int64
	for _, j := range jobs {
		ids = append(ids, j.ID)
	}
	if want := []int64{1001, 1002, 1003}; !slices.Equal(ids, want) {
		t.Errorf("ids %v, want %v", ids, want)
	}
	if call := r.calls[0]; call[0] != "squeue" || !slices.Contains(call, "--json") || !slices.Contains(call, "--name="+testTag) {
		t.Errorf("argv %v, want squeue with --json and --name=%s", call, testTag)
	}
}

func TestSqueueByTagUnknownJobIsNone(t *testing.T) {
	r := &fakeRunner{stderr: "slurm_load_jobs error: Invalid job id specified", err: exit(1)(t)}
	jobs, err := (&Client{R: r}).SqueueByTag(context.Background(), testTag)
	if err != nil || len(jobs) != 0 {
		t.Errorf("got %v, %v; want no jobs and no error", jobs, err)
	}
	r = &fakeRunner{stderr: "squeue: error: Unable to contact slurm controller", err: exit(1)(t)}
	if _, err := (&Client{R: r}).SqueueByTag(context.Background(), testTag); err == nil {
		t.Error("a squeue failure other than an unknown job ID returned no error")
	}
}

// sacct v0.0.40 output (no restart_cnt): an earlier record of job 2001 and a
// later one, in the given states, plus a job of another name.
func sacctRecords(earlier, later string) string {
	return `{"jobs":[
{"job_id":2001,"name":"hb-alloc-a7656bce1293","state":{"current":` + earlier + `,"reason":"None"},
 "time":{"submission":1761800000,"start":1761800010,"end":1761800050}},
{"job_id":2001,"name":"hb-alloc-a7656bce1293","state":{"current":` + later + `,"reason":"None"},
 "time":{"submission":1761800050,"start":1761800100,"end":1761800200}},
{"job_id":2002,"name":"hb-other-000000000000","state":{"current":["COMPLETED"],"reason":"None"},
 "time":{"submission":1761800000,"start":1761800001,"end":1761800002}}],
"meta":{},"errors":[],"warnings":[]}`
}

func TestSacctByTagMergesRequeuedRecords(t *testing.T) {
	for _, earlier := range []string{`["PENDING","REQUEUED"]`, `["REQUEUED"]`} {
		t.Run(earlier, func(t *testing.T) {
			r := &fakeRunner{stdout: sacctRecords(earlier, `["COMPLETED"]`)}
			jobs, err := (&Client{R: r}).SacctByTag(context.Background(), testTag, 26*time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			if len(jobs) != 1 {
				t.Fatalf("got %d jobs, want 1: %+v", len(jobs), jobs)
			}
			j := jobs[0]
			if j.ID != 2001 || !j.Requeued || j.Records != 2 || !slices.Equal(j.State, []string{"COMPLETED"}) || j.RestartCount != 0 {
				t.Errorf("got %+v, want job 2001 Requeued, Records 2, state [COMPLETED]", j)
			}
			if !j.End.Equal(time.Unix(1761800200, 0)) {
				t.Errorf("End %v, want the later record's 1761800200", j.End)
			}
			call := r.calls[0]
			if call[0] != "sacct" || !slices.Contains(call, "--name="+testTag) || !slices.Contains(call, "--json") {
				t.Errorf("argv %v, want sacct --json --name=%s", call, testTag)
			}
			if i := slices.Index(call, "-S"); i < 0 || i+1 >= len(call) || call[i+1] != "now-93600" {
				t.Errorf("argv %v, want an explicit -S now-93600", call)
			}
		})
	}
}

// The latest record is the one with the later submission, not the later
// line of output.
func TestSacctByTagLatestRecordWinsWhateverTheOutputOrder(t *testing.T) {
	out := `{"jobs":[
{"job_id":2001,"name":"hb-alloc-a7656bce1293","state":{"current":["COMPLETED"],"reason":"None"},
 "time":{"submission":1761800050,"start":1761800100,"end":1761800200}},
{"job_id":2001,"name":"hb-alloc-a7656bce1293","state":{"current":["REQUEUED"],"reason":"None"},
 "time":{"submission":1761800000,"start":1761800010,"end":1761800050}}],"errors":[]}`
	jobs, err := (&Client{R: &fakeRunner{stdout: out}}).SacctByTag(context.Background(), testTag, time.Hour)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("err %v, %d jobs", err, len(jobs))
	}
	if !jobs[0].HasState("COMPLETED") || !jobs[0].Requeued || jobs[0].Records != 2 {
		t.Errorf("got %+v, want COMPLETED, Requeued, Records 2", jobs[0])
	}
}

func TestSacctByTagSingleRecordIsNotRequeued(t *testing.T) {
	out := `{"jobs":[
{"job_id":2001,"name":"hb-alloc-a7656bce1293","state":{"current":["COMPLETED"],"reason":"None"},
 "time":{"submission":1761800000,"start":1761800010,"end":1761800050}}],"errors":[]}`
	jobs, err := (&Client{R: &fakeRunner{stdout: out}}).SacctByTag(context.Background(), testTag, time.Hour)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("err %v, %d jobs", err, len(jobs))
	}
	if jobs[0].Requeued || jobs[0].Records != 1 || !jobs[0].HasState("COMPLETED") {
		t.Errorf("got %+v, want COMPLETED, not Requeued, Records 1", jobs[0])
	}
}
