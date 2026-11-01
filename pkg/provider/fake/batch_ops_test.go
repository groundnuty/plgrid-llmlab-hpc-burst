package fake

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

func TestPurgedJobIsReportedFromAccounting(t *testing.T) {
	b, clock := newTestBatch(t)
	b.Enqueue(Script{QueueWait: 5 * time.Minute})
	id := mustSubmit(t, b, gpuReq("example-tag", false)) // ends 12:35:00; MinJobAge 10s
	ctx := context.Background()

	clock.Set(at(12, 35, 9))
	j, found, err := b.Observe(ctx, id)
	if err != nil || !found || j.Source != provider.SourceQueue || j.Phase != v1alpha1.AllocationEnded {
		t.Fatalf("at +9s Observe = %+v found %v err %v, want the ended job from the queue", j, found, err)
	}
	if n := b.Count("sacct"); n != 0 {
		t.Fatalf("sacct read %d times for a job the listing shows, want 0", n)
	}

	clock.Set(at(12, 35, 10))
	j, found, err = b.Observe(ctx, id)
	if err != nil || !found {
		t.Fatalf("at +10s Observe found %v err %v: a purged job is never missing", found, err)
	}
	if j.Source != provider.SourceAccounting || j.Phase != v1alpha1.AllocationEnded {
		t.Fatalf("at +10s source %q phase %s, want accounting and Ended", j.Source, j.Phase)
	}
	wantTime(t, "EndTime", j.EndTime, at(12, 35, 0))
	if n := b.Count("sacct"); n != 1 {
		t.Fatalf("sacct read %d times, want 1", n)
	}

	clock.Set(at(18, 0, 0)) // hours later, still found
	if _, found, err = b.Observe(ctx, id); err != nil || !found {
		t.Fatalf("hours later Observe found %v err %v", found, err)
	}
}

func TestMinJobAgeFromConfig(t *testing.T) {
	clock := NewClock(at(12, 0, 0))
	b := NewBatch(clock, Config{
		Partitions: []Partition{{Name: gpuPart, GPUModel: "m", GPUsPerNode: 4, MaxTime: time.Hour}},
		MinJobAge:  2 * time.Minute, DataParser: "v0.0.40",
	})
	id := mustSubmit(t, b, gpuReq("example-tag", false)) // ends 12:30:00
	clock.Set(at(12, 31, 59))
	if j := mustJob(t, b, id); j.Source != provider.SourceQueue || j.DataParser != "v0.0.40" {
		t.Fatalf("source %q parser %q, want queue and v0.0.40", j.Source, j.DataParser)
	}
	clock.Set(at(12, 32, 0))
	if j := mustJob(t, b, id); j.Source != provider.SourceAccounting {
		t.Fatalf("source %q, want accounting", j.Source)
	}
}

func TestFindByTagSinceWindowOnlyBoundsAccounting(t *testing.T) {
	b, clock := newTestBatch(t)
	ctx := context.Background()
	old := mustSubmit(t, b, gpuReq("example-tag", false)) // submitted 12:00:00, ends 12:30:00
	clock.Set(at(13, 0, 0))
	recent := mustSubmit(t, b, gpuReq("example-tag", false)) // submitted 13:00:00, still running
	clock.Set(at(13, 5, 0))

	// since after the old job's submit: the purged old job is out, the live one in.
	jobs, err := b.FindByTag(ctx, "example-tag", at(12, 30, 0))
	if err != nil || len(jobs) != 1 || jobs[0].ID != recent {
		t.Fatalf("FindByTag(since 12:30) = %v err %v, want only job %s", ids(jobs), err, recent)
	}
	// since at the old job's submit time: accounting covers SubmitTime >= since.
	jobs, err = b.FindByTag(ctx, "example-tag", at(12, 0, 0))
	if err != nil || !reflect.DeepEqual(ids(jobs), []string{old, recent}) {
		t.Fatalf("FindByTag(since 12:00) = %v err %v, want [%s %s]", ids(jobs), err, old, recent)
	}
	// A job the queue listing still shows is found whatever since says.
	jobs, err = b.FindByTag(ctx, "example-tag", at(23, 0, 0))
	if err != nil || len(jobs) != 1 || jobs[0].ID != recent || jobs[0].Source != provider.SourceQueue {
		t.Fatalf("FindByTag(since 23:00) = %v err %v, want the queue-listed job", ids(jobs), err)
	}
}

func ids(jobs []provider.Job) []string {
	out := make([]string, len(jobs))
	for i, j := range jobs {
		out[i] = j.ID
	}
	return out
}

func TestFindByTagSortsNumerically(t *testing.T) {
	b, _ := newTestBatch(t)
	for range 3 {
		mustSubmit(t, b, gpuReq("example-tag", false))
	}
	mustSubmit(t, b, gpuReq("example-other", false)) // 1001: another tag
	jobs, err := b.FindByTag(context.Background(), "example-tag", at(0, 0, 0))
	if err != nil {
		t.Fatal(err)
	}
	// A string sort would give 1000, 998, 999.
	if got := ids(jobs); !reflect.DeepEqual(got, []string{"998", "999", "1000"}) {
		t.Fatalf("FindByTag IDs = %v, want [998 999 1000]", got)
	}
}

func TestVanishAndReappear(t *testing.T) {
	b, clock := newTestBatch(t)
	ctx := context.Background()
	id := mustSubmit(t, b, gpuReq("example-tag", false))
	b.Vanish(id)

	j, found, err := b.Observe(ctx, id)
	if err != nil || found {
		t.Fatalf("Observe of a vanished job = (%+v, %v, %v), want not found and no error", j, found, err)
	}
	jobs, err := b.FindByTag(ctx, "example-tag", at(0, 0, 0))
	if err != nil || len(jobs) != 0 {
		t.Fatalf("FindByTag of a vanished job = %v err %v, want none", ids(jobs), err)
	}
	if _, ok := b.Job(id); !ok {
		t.Fatal("Job(id) must ignore Vanish")
	}
	clock.Set(at(12, 10, 0))
	b.Reappear(id)
	if _, found, err = b.Observe(ctx, id); err != nil || !found {
		t.Fatalf("after Reappear Observe found %v err %v", found, err)
	}
	if jobs, _ = b.FindByTag(ctx, "example-tag", at(0, 0, 0)); len(jobs) != 1 {
		t.Fatalf("after Reappear FindByTag = %v, want the job", ids(jobs))
	}
}

func TestObserveOfUnknownJobIsNotFound(t *testing.T) {
	b, _ := newTestBatch(t)
	_, found, err := b.Observe(context.Background(), "4242")
	if err != nil || found {
		t.Fatalf("Observe(unknown) found %v err %v, want not found and no error", found, err)
	}
}

func TestFailReadsIsAnErrorNeverAbsence(t *testing.T) {
	b, _ := newTestBatch(t)
	ctx := context.Background()
	id := mustSubmit(t, b, gpuReq("example-tag", false))
	boom := errors.New("example transport failure")
	b.FailReads(boom)

	jobs, err := b.FindByTag(ctx, "example-tag", at(0, 0, 0))
	if !errors.Is(err, boom) || jobs != nil {
		t.Fatalf("FindByTag = (%v, %v), want (nil, the injected error)", jobs, err)
	}
	_, found, err := b.Observe(ctx, id)
	if !errors.Is(err, boom) || found {
		t.Fatalf("Observe = (found %v, %v), want the injected error", found, err)
	}
	// Writes are not reads: a cancel still reaches the scheduler.
	if res := b.Cancel(ctx, []string{id}); res[0].Outcome != provider.Cancelled {
		t.Fatalf("Cancel under FailReads = %+v, want Cancelled", res[0])
	}

	b.FailReads(nil)
	if _, found, err = b.Observe(ctx, id); err != nil || !found {
		t.Fatalf("after clearing: found %v err %v", found, err)
	}
}

func TestUnknownOutcomeRegisteredIsFoundByTag(t *testing.T) {
	b, _ := newTestBatch(t)
	b.Enqueue(Script{Outcome: provider.Unknown, Registered: true, Reason: "example: connection lost"})
	id, outcome, err := b.Submit(context.Background(), gpuReq("example-tag", true))
	if id != "" || outcome != provider.Unknown || err == nil || err.Error() != "example: connection lost" {
		t.Fatalf("Submit = (%q, %s, %v), want no ID, Unknown and the reason", id, outcome, err)
	}
	jobs, ferr := b.FindByTag(context.Background(), "example-tag", at(0, 0, 0))
	if ferr != nil || len(jobs) != 1 || jobs[0].ID != "998" || jobs[0].Phase != v1alpha1.AllocationHeld {
		t.Fatalf("FindByTag = %v err %v, want job 998 held", ids(jobs), ferr)
	}
	if n := b.Count("sbatch"); n != 1 {
		t.Fatalf("sbatch logged %d times, want 1", n)
	}
}

func TestUnknownOutcomeNotRegisteredLeavesNothingAndNoID(t *testing.T) {
	b, _ := newTestBatch(t)
	b.Enqueue(Script{Outcome: provider.Unknown})
	id, outcome, err := b.Submit(context.Background(), gpuReq("example-tag", false))
	if id != "" || outcome != provider.Unknown || err == nil {
		t.Fatalf("Submit = (%q, %s, %v), want no ID, Unknown and an error", id, outcome, err)
	}
	if jobs, _ := b.FindByTag(context.Background(), "example-tag", at(0, 0, 0)); len(jobs) != 0 {
		t.Fatalf("FindByTag = %v, want nothing", ids(jobs))
	}
	if got := mustSubmit(t, b, gpuReq("example-tag", false)); got != "998" {
		t.Fatalf("next job ID = %s, want 998: an unregistered submission takes none", got)
	}
}

func TestRejectedNeverRegisters(t *testing.T) {
	b, _ := newTestBatch(t)
	b.Enqueue(Script{Outcome: provider.Rejected, Registered: true, Reason: "Batch job submission failed: example reason"})
	id, outcome, err := b.Submit(context.Background(), gpuReq("example-tag", false))
	if id != "" || outcome != provider.Rejected || err == nil || err.Error() != "Batch job submission failed: example reason" {
		t.Fatalf("Submit = (%q, %s, %v), want no ID, Rejected and the reason", id, outcome, err)
	}
	if errors.Is(err, provider.ErrInvalidRequest) {
		t.Fatal("a scheduler's rejection is not a pre-call refusal")
	}
	if jobs, _ := b.FindByTag(context.Background(), "example-tag", at(0, 0, 0)); len(jobs) != 0 {
		t.Fatalf("FindByTag = %v, want nothing", ids(jobs))
	}
	if n := b.Count("sbatch"); n != 1 {
		t.Fatalf("sbatch logged %d times, want 1: a rejection reached the scheduler", n)
	}
}

func TestPreCallRefusalsLeaveNoSbatchAndConsumeNoScript(t *testing.T) {
	b, _ := newTestBatch(t)
	b.Enqueue(Script{Outcome: provider.Rejected, Reason: "queued script"})
	mod := func(f func(*provider.SubmitRequest)) provider.SubmitRequest {
		r := gpuReq("example-tag", true)
		f(&r)
		return r
	}
	for _, tc := range []struct {
		name string
		req  provider.SubmitRequest
	}{
		{"empty tag", mod(func(r *provider.SubmitRequest) { r.Tag = "" })},
		{"unsafe tag", mod(func(r *provider.SubmitRequest) { r.Tag = "example tag;rm" })},
		{"partition not allowed", mod(func(r *provider.SubmitRequest) { r.Params.Partition = "example-other" })},
		{"no GPU on a GPU partition", mod(func(r *provider.SubmitRequest) { r.Params.GPUs = 0 })},
		{"more GPUs than gpusPerNode", mod(func(r *provider.SubmitRequest) { r.Params.GPUs = 5 })},
		{"unparseable time limit", mod(func(r *provider.SubmitRequest) { r.Params.TimeLimit = "soon" })},
		{"time limit above maxTime", mod(func(r *provider.SubmitRequest) { r.Params.TimeLimit = "61m" })},
		{"time limit not in whole minutes", mod(func(r *provider.SubmitRequest) { r.Params.TimeLimit = "90s" })},
		{"empty time limit", mod(func(r *provider.SubmitRequest) { r.Params.TimeLimit = "" })},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id, outcome, err := b.Submit(context.Background(), tc.req)
			if id != "" || outcome != provider.Rejected || !errors.Is(err, provider.ErrInvalidRequest) {
				t.Fatalf("Submit = (%q, %s, %v), want no ID, Rejected and ErrInvalidRequest", id, outcome, err)
			}
			if n := b.Count("sbatch"); n != 0 {
				t.Fatalf("sbatch logged %d times, want 0: nothing was called", n)
			}
		})
	}
	if _, ok := b.Job("998"); ok {
		t.Fatal("a refused request registered a job")
	}
	// The queued script is still first in line: the refusals did not consume it.
	_, outcome, err := b.Submit(context.Background(), gpuReq("example-tag", false))
	if outcome != provider.Rejected || err == nil || err.Error() != "queued script" {
		t.Fatalf("next Submit = (%s, %v), want the queued rejection", outcome, err)
	}
	// A CPU partition takes no GPU.
	cpu := provider.SubmitRequest{Tag: "example-cpu-tag", Params: v1alpha1.BatchParameters{Partition: cpuPart, TimeLimit: "2h", GPUs: 0}}
	if id, outcome, err := b.Submit(context.Background(), cpu); id != "998" || outcome != provider.Accepted || err != nil {
		t.Fatalf("CPU Submit = (%q, %s, %v), want job 998 accepted", id, outcome, err)
	}
}

func TestSbatchLogHoldsTheRequestsValues(t *testing.T) {
	b, _ := newTestBatch(t)
	mustSubmit(t, b, gpuReq("example-tag", true))
	calls := b.Calls()
	if len(calls) != 1 || calls[0].Op != "sbatch" || !reflect.DeepEqual(calls[0].Args, []string{"example-tag", gpuPart, "30m", "1"}) || !calls[0].Time.Equal(at(12, 0, 0)) {
		t.Fatalf("calls = %+v, want one sbatch [example-tag %s 30m 1] at 12:00:00", calls, gpuPart)
	}
	calls[0].Args[0] = "changed"
	if b.Calls()[0].Args[0] != "example-tag" {
		t.Fatal("Calls returned the log itself, not a copy")
	}
}

func TestRenameAndAsTimeLimitAreTheReadBackCases(t *testing.T) {
	b, clock := newTestBatch(t)
	b.Enqueue(Script{Rename: "example-short"}, Script{AsTimeLimit: 20 * time.Minute})
	renamed := mustSubmit(t, b, gpuReq("example-tag", false))
	shorter := mustSubmit(t, b, gpuReq("example-tag", false))

	if j := mustJob(t, b, renamed); j.Name != "example-short" {
		t.Fatalf("Name = %q, want the scheduler's name", j.Name)
	}
	jobs, err := b.FindByTag(context.Background(), "example-tag", at(0, 0, 0))
	if err != nil || !reflect.DeepEqual(ids(jobs), []string{shorter}) {
		t.Fatalf("FindByTag = %v err %v, want only the job still named by the tag", ids(jobs), err)
	}
	j := mustJob(t, b, shorter)
	if j.TimeLimit != 20*time.Minute {
		t.Fatalf("TimeLimit = %v, want 20m", j.TimeLimit)
	}
	wantTime(t, "HardEnd", j.HardEnd, at(12, 20, 0))
	clock.Set(at(12, 20, 0))
	wantPhase(t, b, shorter, v1alpha1.AllocationEnded)
}

func TestAdminHoldLandsHeldWhateverTheRequest(t *testing.T) {
	b, _ := newTestBatch(t)
	b.Enqueue(Script{AdminHold: true})
	id := mustSubmit(t, b, gpuReq("example-tag", false)) // not submitted held
	j := wantPhase(t, b, id, v1alpha1.AllocationHeld)
	if j.Hold != provider.HoldAdmin || j.Reason != "JobHeldAdmin" {
		t.Fatalf("hold %q reason %q, want an admin hold with JobHeldAdmin", j.Hold, j.Reason)
	}
}

func TestLiftHoldErrors(t *testing.T) {
	b, _ := newTestBatch(t)
	ctx := context.Background()
	b.Enqueue(Script{AdminHold: true})
	admin := mustSubmit(t, b, gpuReq("example-tag", true))  // 998
	plain := mustSubmit(t, b, gpuReq("example-tag", false)) // 999: never held
	user := mustSubmit(t, b, gpuReq("example-tag", true))   // 1000

	if err := b.LiftHold(ctx, "4242"); !errors.Is(err, provider.ErrNotFound) {
		t.Fatalf("LiftHold(unknown) = %v, want ErrNotFound", err)
	}
	if err := b.LiftHold(ctx, plain); !errors.Is(err, provider.ErrNotHeld) {
		t.Fatalf("LiftHold(not held) = %v, want ErrNotHeld", err)
	}
	if err := b.LiftHold(ctx, admin); !errors.Is(err, provider.ErrAdminHold) {
		t.Fatalf("LiftHold(admin hold) = %v, want ErrAdminHold", err)
	}
	wantPhase(t, b, admin, v1alpha1.AllocationHeld) // the admin hold stands
	if err := b.LiftHold(ctx, user); err != nil {
		t.Fatalf("LiftHold(user hold) = %v", err)
	}
	if err := b.LiftHold(ctx, user); !errors.Is(err, provider.ErrNotHeld) {
		t.Fatalf("second LiftHold = %v, want ErrNotHeld", err)
	}
	if n := b.Count("scontrol-release"); n != 5 {
		t.Fatalf("scontrol-release logged %d times, want 5", n)
	}
}

func TestLiftHoldOnACancelledHeldJobIsNotHeld(t *testing.T) {
	b, _ := newTestBatch(t)
	id := mustSubmit(t, b, gpuReq("example-tag", true))
	b.Cancel(context.Background(), []string{id})
	if err := b.LiftHold(context.Background(), id); !errors.Is(err, provider.ErrNotHeld) {
		t.Fatalf("LiftHold = %v, want ErrNotHeld", err)
	}
}

func TestCancelGivesOneResultPerJobInOrder(t *testing.T) {
	b, clock := newTestBatch(t)
	ctx := context.Background()
	b.Enqueue(Script{QueueWait: time.Hour}) // 998 stays pending
	pending := mustSubmit(t, b, gpuReq("example-tag", false))
	done := mustSubmit(t, b, gpuReq("example-tag", false)) // 999
	clock.Set(at(12, 10, 0))
	b.CancelExternally(done)

	res := b.Cancel(ctx, []string{"4242", done, pending, pending})
	want := []struct {
		id      string
		outcome provider.CancelOutcome
	}{
		{"4242", provider.CancelError},
		{done, provider.AlreadyEnded},
		{pending, provider.Cancelled},
		{pending, provider.AlreadyEnded}, // idempotent
	}
	if len(res) != len(want) {
		t.Fatalf("got %d results, want %d", len(res), len(want))
	}
	for i, w := range want {
		if res[i].JobID != w.id || res[i].Outcome != w.outcome {
			t.Fatalf("result %d = %+v, want job %s %s", i, res[i], w.id, w.outcome)
		}
		if (res[i].Err != nil) != (w.outcome == provider.CancelError) {
			t.Fatalf("result %d: Err = %v for outcome %s", i, res[i].Err, res[i].Outcome)
		}
	}
	if !errors.Is(res[0].Err, provider.ErrNotFound) {
		t.Fatalf("unknown job Err = %v, want ErrNotFound", res[0].Err)
	}
	j := mustJob(t, b, pending)
	if j.State[0] != "CANCELLED" {
		t.Fatalf("state = %v, want CANCELLED", j.State)
	}
	wantTime(t, "EndTime", j.EndTime, at(12, 10, 0))
	if n := b.Count("scancel"); n != 4 {
		t.Fatalf("scancel logged %d times, want 4 (one per job)", n)
	}
}

func TestCancelOfATimedOutJobIsAlreadyEnded(t *testing.T) {
	b, clock := newTestBatch(t)
	id := mustSubmit(t, b, gpuReq("example-tag", false))
	clock.Set(at(13, 0, 0))
	if res := b.Cancel(context.Background(), []string{id}); res[0].Outcome != provider.AlreadyEnded {
		t.Fatalf("Cancel = %+v, want AlreadyEnded", res[0])
	}
	if j := mustJob(t, b, id); j.State[0] != "TIMEOUT" {
		t.Fatalf("state = %v: a cancel after the end must not rewrite it", j.State)
	}
}

// file returns a File whose digest is the content's SHA-256 from the standard
// library, an implementation independent of the fake's.
func file(path, content string, mode os.FileMode) provider.File {
	sum := sha256.Sum256([]byte(content))
	return provider.File{Path: path, SHA256: hex.EncodeToString(sum[:]), Mode: mode, Content: []byte(content)}
}

func TestEnsureFileWritesOnceAndNeverLogsContent(t *testing.T) {
	b, _ := newTestBatch(t)
	ctx := context.Background()
	const secret = "example-credential-token"
	f := file("alloc/hb-example-0123456789ab/credential", secret, 0o600)
	if err := b.EnsureFile(ctx, f); err != nil {
		t.Fatalf("EnsureFile: %v", err)
	}
	if err := b.EnsureFile(ctx, f); err != nil {
		t.Fatalf("second EnsureFile: %v", err)
	}
	if n := b.Count("ensure-file"); n != 1 {
		t.Fatalf("ensure-file logged %d times, want 1: the second call does nothing", n)
	}
	got := b.Files()[f.Path]
	if string(got.Content) != secret || got.Mode != 0o600 || got.SHA256 != f.SHA256 {
		t.Fatalf("stored file = %+v", got)
	}
	var logged *Call
	for _, c := range b.Calls() {
		if c.Op == "ensure-file" {
			c := c
			logged = &c
		}
		for _, a := range c.Args {
			if strings.Contains(a, secret) {
				t.Fatalf("call %s logs the file's content: %q", c.Op, a)
			}
		}
	}
	if logged == nil || !reflect.DeepEqual(logged.Args, []string{f.Path, f.SHA256, "600"}) {
		t.Fatalf("logged call = %+v, want args [path sha256 600]", logged)
	}

	// Another content at the same path replaces the file and logs again.
	f2 := file(f.Path, "example-rotated-token", 0o600)
	if err := b.EnsureFile(ctx, f2); err != nil {
		t.Fatal(err)
	}
	if n := b.Count("ensure-file"); n != 2 || string(b.Files()[f.Path].Content) != "example-rotated-token" {
		t.Fatalf("after a changed content: %d ensure-file calls, content %q", n, b.Files()[f.Path].Content)
	}
}

func TestEnsureFileKnownDigest(t *testing.T) {
	// sha256("hello"), computed outside this code: printf %s hello | sha256sum.
	const hello = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
	b, _ := newTestBatch(t)
	f := provider.File{Path: "agent/example/hb-agent", SHA256: hello, Mode: 0o755, Content: []byte("hello")}
	if err := b.EnsureFile(context.Background(), f); err != nil {
		t.Fatalf("EnsureFile: %v", err)
	}
	if got := b.Calls()[0].Args; !reflect.DeepEqual(got, []string{"agent/example/hb-agent", hello, "755"}) {
		t.Fatalf("args = %v, want the mode in octal", got)
	}
}

func TestEnsureFileRefusals(t *testing.T) {
	b, _ := newTestBatch(t)
	good := file("alloc/hb-x/credential", "example", 0o600)
	bad := func(f func(*provider.File)) provider.File {
		g := good
		f(&g)
		return g
	}
	for _, tc := range []struct {
		name string
		f    provider.File
	}{
		{"absolute path", bad(func(f *provider.File) { f.Path = "/etc/example" })},
		{"dot dot", bad(func(f *provider.File) { f.Path = "alloc/../../etc/example" })},
		{"unsafe characters", bad(func(f *provider.File) { f.Path = "alloc/hb x/credential" })},
		{"empty path", bad(func(f *provider.File) { f.Path = "" })},
		{"wrong SHA-256", bad(func(f *provider.File) { f.SHA256 = strings.Repeat("0", 64) })},
		{"upper-case SHA-256", bad(func(f *provider.File) { f.SHA256 = strings.ToUpper(f.SHA256) })},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := b.EnsureFile(context.Background(), tc.f); !errors.Is(err, provider.ErrInvalidRequest) {
				t.Fatalf("EnsureFile = %v, want ErrInvalidRequest", err)
			}
		})
	}
	if len(b.Files()) != 0 || b.Count("ensure-file") != 0 {
		t.Fatalf("a refused file left %d files and %d calls", len(b.Files()), b.Count("ensure-file"))
	}
}

func TestFilesReturnsACopy(t *testing.T) {
	b, _ := newTestBatch(t)
	f := file("alloc/hb-x/credential", "example", 0o600)
	if err := b.EnsureFile(context.Background(), f); err != nil {
		t.Fatal(err)
	}
	f.Content[0] = 'X' // the caller's buffer
	out := b.Files()
	out[f.Path].Content[0] = 'Y'
	delete(out, f.Path)
	if got := b.Files()[f.Path]; string(got.Content) != "example" {
		t.Fatalf("stored content = %q: the fake shares memory with its callers", got.Content)
	}
}

func TestRemoveAllocationFiles(t *testing.T) {
	b, _ := newTestBatch(t)
	ctx := context.Background()
	for _, p := range []string{
		"alloc/hb-x/credential", "alloc/hb-x/agent.json", "alloc/hb-x/nested/file",
		"alloc/hb-xy/credential", "alloc/hb-y/credential", "agent/example/hb-agent",
	} {
		if err := b.EnsureFile(ctx, file(p, "example", 0o600)); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.RemoveAllocationFiles(ctx, "hb-x"); err != nil {
		t.Fatal(err)
	}
	var left []string
	for p := range b.Files() {
		left = append(left, p)
	}
	want := map[string]bool{"alloc/hb-xy/credential": true, "alloc/hb-y/credential": true, "agent/example/hb-agent": true}
	if len(left) != len(want) {
		t.Fatalf("files left = %v, want only %v", left, want)
	}
	for _, p := range left {
		if !want[p] {
			t.Fatalf("file %s should be gone or kept: left %v", p, left)
		}
	}
	if n := b.Count("remove-allocation-files"); n != 1 {
		t.Fatalf("remove-allocation-files logged %d times, want 1", n)
	}
	if err := b.RemoveAllocationFiles(ctx, "hb-x"); err != nil { // idempotent
		t.Fatalf("second RemoveAllocationFiles: %v", err)
	}
	for _, tag := range []string{"", "../x", "a/b", "a b"} {
		if err := b.RemoveAllocationFiles(ctx, tag); !errors.Is(err, provider.ErrInvalidRequest) {
			t.Fatalf("RemoveAllocationFiles(%q) = %v, want ErrInvalidRequest", tag, err)
		}
	}
}

func TestTestControlsPanicOnUnknownJob(t *testing.T) {
	b, _ := newTestBatch(t)
	for name, f := range map[string]func(){
		"CancelExternally": func() { b.CancelExternally("1") },
		"Requeue":          func() { b.Requeue("1") },
		"RequeueHold":      func() { b.RequeueHold("1") },
		"Vanish":           func() { b.Vanish("1") },
		"Reappear":         func() { b.Reappear("1") },
	} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatalf("%s of an unknown job did not panic", name)
				}
			}()
			f()
		})
	}
}

func TestEnqueueRejectsUnknownOutcome(t *testing.T) {
	b, _ := newTestBatch(t)
	defer func() {
		if recover() == nil {
			t.Fatal("Enqueue of a script with an unknown outcome did not panic")
		}
	}()
	b.Enqueue(Script{Outcome: "Bogus"})
}

func TestConcurrentUse(t *testing.T) {
	b, _ := newTestBatch(t)
	var wg sync.WaitGroup
	seen := make(chan string, 20)
	for i := range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, _, err := b.Submit(context.Background(), gpuReq(fmt.Sprintf("example-%d", i), false))
			if err != nil {
				t.Error(err)
			}
			_, _, _ = b.Observe(context.Background(), id)
			seen <- id
		}()
	}
	wg.Wait()
	close(seen)
	got := map[string]bool{}
	for id := range seen {
		if got[id] {
			t.Fatalf("job ID %s given twice", id)
		}
		got[id] = true
	}
	if len(got) != 20 || b.Count("sbatch") != 20 {
		t.Fatalf("%d distinct IDs and %d sbatch calls, want 20 and 20", len(got), b.Count("sbatch"))
	}
}

// A scripted policy refusal returns a *provider.PolicyRefusal with the reason
// name and Slurm's two lines, the reason line first (HBEP-4, QOS limits), and
// registers nothing. A refusal for another limit has its own reason name.
func TestScriptedPolicyRefusal(t *testing.T) {
	b, _ := newTestBatch(t)
	b.Enqueue(Script{Outcome: provider.Rejected, Policy: "QOSMaxSubmitJobPerUserLimit"}, Script{Outcome: provider.Rejected, Policy: "QOSMaxWallDurationPerJobLimit"})
	const want = "sbatch: error: QOSMaxSubmitJobPerUserLimit\nsbatch: error: Batch job submission failed: Job violates accounting/QOS policy (job submit limit, user's size and/or time limits)"
	id, outcome, err := b.Submit(context.Background(), gpuReq("hb-limit-000000000001", true))
	var pr *provider.PolicyRefusal
	if id != "" || outcome != provider.Rejected || !errors.As(err, &pr) {
		t.Fatalf("Submit = (%q, %s, %v), want no ID, Rejected and a PolicyRefusal", id, outcome, err)
	}
	if pr.Reason != "QOSMaxSubmitJobPerUserLimit" || !pr.SubmitLimit() || err.Error() != want {
		t.Errorf("refusal = %+v, text %q; want the submit-limit reason and Slurm's two lines", pr, err.Error())
	}
	_, outcome, err = b.Submit(context.Background(), gpuReq("hb-limit-000000000002", true))
	if outcome != provider.Rejected || !errors.As(err, &pr) || pr.Reason != "QOSMaxWallDurationPerJobLimit" || pr.SubmitLimit() {
		t.Errorf("second Submit = (%s, %v), want a PolicyRefusal for QOSMaxWallDurationPerJobLimit that is not a submit limit", outcome, err)
	}
	if jobs, err := b.FindByTag(context.Background(), "hb-limit-000000000001", at(0, 0, 0)); err != nil || len(jobs) != 0 {
		t.Errorf("FindByTag after a refusal = %v, %v; want no job", jobs, err)
	}
}
