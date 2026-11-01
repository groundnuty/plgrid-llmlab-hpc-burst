package providerctl

// Provider accounting in status.atProvider.accounting (HBEP-8, Provider
// accounting; issue #113). Over the real slurm.Batch and the fake Slurm, so the
// sacct JSON is parsed and the call counts come from the fake Slurm's log.
//
// Every expected number is written out by hand from the times the test gives
// the fake: the clock starts at 12:00:00; a job started at 12:01:00 that is
// read at 12:01:40 has run 40 s; the fake Slurm lists the job's CPUs (8, the
// request's cpusPerTask) and GPUs (the request's) in tres.allocated once the
// job has started.

import (
	"context"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// jobNumber is a job ID as the fake Slurm numbers it.
func jobNumber(t *testing.T, id string) int {
	t.Helper()
	n, err := strconv.Atoi(id)
	if err != nil {
		t.Fatalf("job ID %q: %v", id, err)
	}
	return n
}

// endJob ends the job on the fake Slurm at the clock's time.
func endJob(t *testing.T, sl *fakeslurm.Platform, id, state string) {
	t.Helper()
	if err := sl.End(jobNumber(t, id), state); err != nil {
		t.Fatal(err)
	}
}

// accStatus is the Allocation's stored accounting block.
func (e *realEnv) accStatus(name string) *v1alpha1.AllocationAccounting {
	e.t.Helper()
	return e.get(name).Status.AtProvider.Accounting
}

// wantAcctStatus checks the block against literals; a negative count or
// elapsed time means unknown.
func wantAcctStatus(t *testing.T, what string, got *v1alpha1.AllocationAccounting, cpus, gpus int32, elapsed int64, read time.Time) {
	t.Helper()
	if got == nil {
		t.Fatalf("%s: no accounting, want cpus %d gpus %d elapsed %d", what, cpus, gpus, elapsed)
	}
	if (cpus < 0) != (got.AllocatedCPUs == nil) || (cpus >= 0 && *got.AllocatedCPUs != cpus) {
		t.Errorf("%s: allocatedCPUs %v, want %d", what, got.AllocatedCPUs, cpus)
	}
	if (gpus < 0) != (got.AllocatedGPUs == nil) || (gpus >= 0 && *got.AllocatedGPUs != gpus) {
		t.Errorf("%s: allocatedGPUs %v, want %d", what, got.AllocatedGPUs, gpus)
	}
	if (elapsed < 0) != (got.ElapsedSeconds == nil) || (elapsed >= 0 && *got.ElapsedSeconds != elapsed) {
		t.Errorf("%s: elapsedSeconds %v, want %d", what, got.ElapsedSeconds, elapsed)
	}
	if !got.ReadTime.Time.Equal(read) {
		t.Errorf("%s: readTime %s, want %s", what, got.ReadTime.Format(time.RFC3339), read.Format(time.RFC3339))
	}
}

// A pending Allocation has no accounting. A running one's elapsed time follows
// the scheduler's start time to the read time, and its CPUs and GPUs come from
// one accounting read, made once however many polls follow.
func TestAccountingStatusFollowsTheJob(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := newSlurm(clock, fakeslurm.Config{})
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a")
	e.newAlloc("alloc-a", 1)
	e.run("alloc-a", 2)
	if a := e.accStatus("alloc-a"); a != nil {
		t.Fatalf("a pending Allocation has accounting %+v, want none: its hours are unknown, never 0", a)
	}
	clock.Advance(60 * time.Second)
	e.start("alloc-a") // started at 12:01:00
	clock.Advance(40 * time.Second)
	e.run("alloc-a", 1)
	wantAcctStatus(t, "12:01:40", e.accStatus("alloc-a"), 8, 1, 40, at(12, 1, 40))
	id := e.get("alloc-a").Annotations[v1alpha1.AnnotationJobID]
	// Polls every 40 s; the block is rewritten once it is a minute old (the
	// status-write cadence), so 12:02:20 keeps 12:01:40's block.
	for _, want := range []struct {
		elapsed int64
		read    time.Time
	}{{40, at(12, 1, 40)}, {120, at(12, 3, 0)}, {120, at(12, 3, 0)}} {
		clock.Advance(40 * time.Second)
		e.run("alloc-a", 1)
		wantAcctStatus(t, "poll at "+clock.Now().Format("15:04:05"), e.accStatus("alloc-a"), 8, 1, want.elapsed, want.read)
	}
	if got := jobReads(sl, id); got != 1 {
		t.Errorf("%d accounting reads of the running job over 4 polls, want 1", got)
	}
	a := e.accStatus("alloc-a")
	if s, ok := a.GPUSeconds(); !ok || s != 120 {
		t.Errorf("GPU-seconds %d %v, want 120 (1 GPU, 120 s at 12:03:00)", s, ok)
	}
	if h, ok := a.CPUHours(); !ok || math.Abs(h-8*120.0/3600) > 1e-12 {
		t.Errorf("CPU-hours %v %v, want 8 x 120 / 3600", h, ok)
	}
}

// A job that ends on its own: while the listing no longer shows it the final
// numbers are accounting's, time.elapsed (120 s), and a later poll reads
// nothing again.
func TestAccountingStatusOfAJobThatEnds(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := newSlurm(clock, fakeslurm.Config{})
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a")
	e.newAlloc("alloc-a", 1)
	e.run("alloc-a", 2)
	clock.Advance(60 * time.Second)
	e.start("alloc-a") // 12:01:00
	e.poll("alloc-a", 3)
	clock.Set(at(12, 3, 0))
	id := e.get("alloc-a").Annotations[v1alpha1.AnnotationJobID]
	endJob(t, sl, id, fakeslurm.Completed) // ends at 12:03:00: 120 s
	e.poll("alloc-a", 1)
	got := e.accStatus("alloc-a")
	wantAcctStatus(t, "ended", got, 8, 1, 120, at(12, 3, 40))
	if s, ok := got.GPUSeconds(); !ok || s != 120 {
		t.Errorf("GPU-seconds %d %v, want 120", s, ok)
	}
	if s, ok := got.CPUSeconds(); !ok || s != 960 {
		t.Errorf("CPU-seconds %d %v, want 960", s, ok)
	}
	reads := jobReads(sl, id)
	e.poll("alloc-a", 3)
	if after := jobReads(sl, id); after != reads {
		t.Errorf("accounting reads of the ended job went from %d to %d, want none after the final one", reads, after)
	}
	wantAcctStatus(t, "later polls", e.accStatus("alloc-a"), 8, 1, 120, at(12, 3, 40))
}

// The final state shown by the queue listing: end minus start, the counts from
// the stored status. Athena lists an ended job until MinJobAge.
func TestAccountingStatusOfAnEndedJobInTheListing(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := newSlurm(clock, fakeslurm.Config{NotListed: fakeslurm.NotListedAthena, MinJobAge: 10 * time.Minute})
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a")
	e.newAlloc("alloc-a", 1)
	e.run("alloc-a", 2)
	clock.Advance(60 * time.Second)
	e.start("alloc-a") // 12:01:00
	e.poll("alloc-a", 1)
	clock.Set(at(12, 2, 30))
	id := e.get("alloc-a").Annotations[v1alpha1.AnnotationJobID]
	endJob(t, sl, id, fakeslurm.Completed) // 12:02:30: 90 s
	before := jobReads(sl, id)
	e.poll("alloc-a", 1)
	if e.get("alloc-a").Status.AtProvider.EndTime == nil {
		t.Fatal("precondition: the end is not in the status")
	}
	wantAcctStatus(t, "ended in the listing", e.accStatus("alloc-a"), 8, 1, 90, at(12, 3, 10))
	if got := jobReads(sl, id); got != before {
		t.Errorf("accounting was read (%d -> %d) although the listing showed the end", before, got)
	}
}

// A failed accounting read is not silent: the Synced condition says so, the
// elapsed time still follows the job, and the next poll reads again, once it
// succeeds, and no more.
func TestAccountingReadIsRetriedAfterAFailure(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := newSlurm(clock, fakeslurm.Config{})
	f := &lineFaults{}
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a", withLineFaults(f))
	e.newAlloc("alloc-a", 1)
	e.run("alloc-a", 2)
	clock.Advance(60 * time.Second)
	e.start("alloc-a") // 12:01:00
	f.fail("sacct", 1, slurm.ErrTransport)
	clock.Advance(40 * time.Second)
	e.run("alloc-a", 1)
	wantAcctStatus(t, "after the failed read", e.accStatus("alloc-a"), -1, -1, 40, at(12, 1, 40))
	if c := e.get("alloc-a").GetCondition(xpv2.TypeSynced); c.Status != corev1.ConditionFalse || !strings.Contains(c.Message, "read accounting of job") {
		t.Errorf("Synced %s %q, want False naming the accounting read", c.Status, c.Message)
	}
	id := e.get("alloc-a").Annotations[v1alpha1.AnnotationJobID]
	if got := jobReads(sl, id); got != 0 {
		t.Fatalf("precondition: %d accounting reads reached the fake Slurm, want 0 (the first failed before it)", got)
	}
	e.poll("alloc-a", 4)
	// The retry is due a minute after the failed read's 12:01:40: at 12:03:00,
	// which gives the counts (120 s); 12:04:20 is the next block a minute old.
	wantAcctStatus(t, "after the retry", e.accStatus("alloc-a"), 8, 1, 200, at(12, 4, 20))
	if got := jobReads(sl, id); got != 1 {
		t.Errorf("%d accounting reads that reached the fake Slurm, want 1: the retry, then none", got)
	}
}

// Accounting does not show the job (it has not got it yet): the read is
// retried once a minute, not on every poll (Astra nit 10), the CPUs and GPUs
// stay unknown, and the final read after the job ends gives them.
func TestAccountingReadIsRetriedOnceAMinuteWhenAccountingDoesNotShowTheJob(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := newSlurm(clock, fakeslurm.Config{})
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a")
	e.newAlloc("alloc-a", 1)
	e.run("alloc-a", 2)
	id := e.get("alloc-a").Annotations[v1alpha1.AnnotationJobID]
	n := jobNumber(t, id)
	if err := sl.AcctHide(n); err != nil {
		t.Fatal(err)
	}
	clock.Advance(60 * time.Second)
	e.start("alloc-a") // 12:01:00
	e.poll("alloc-a", 5)
	wantAcctStatus(t, "hidden", e.accStatus("alloc-a"), -1, -1, 200, at(12, 4, 20))
	// Polls at 12:01:40 (read), 12:02:20, 12:03:00 (read), 12:03:40 and
	// 12:04:20 (read).
	if got := jobReads(sl, id); got != 3 {
		t.Errorf("%d accounting reads over 5 polls 40 s apart while accounting does not show the job, want 3", got)
	}
	if a := e.accStatus("alloc-a"); a != nil {
		if _, ok := a.GPUHours(); ok {
			t.Error("GPU-hours are known without a GPU count")
		}
	}
	if err := sl.AcctShow(n); err != nil {
		t.Fatal(err)
	}
	clock.Set(at(12, 5, 0))
	endJob(t, sl, id, fakeslurm.Completed) // 240 s
	e.poll("alloc-a", 1)
	wantAcctStatus(t, "final", e.accStatus("alloc-a"), 8, 1, 240, at(12, 5, 40))
}

// A CPU job asked for no GPU, and accounting lists no gres entry: 0 GPUs, a
// known 0, so its GPU-hours are 0 and its CPU-hours are 8 x 100 s / 3600.
func TestAccountingOfACPUJobHasZeroGPUs(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := fakeslurm.New(clock, fakeslurm.Config{Partitions: []fakeslurm.Partition{{Name: gpuPart}}})
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a", func(e *realEnv) {
		e.profiles.set(func(v *slurm.Validated) {
			v.Profile.Partition.GPUModel = "none"
			v.Profile.Partition.GPUsPerNode = nil
		})
	})
	e.newAlloc("alloc-a", 0)
	e.run("alloc-a", 2)
	clock.Advance(60 * time.Second)
	e.start("alloc-a") // 12:01:00
	clock.Advance(100 * time.Second)
	e.run("alloc-a", 1)
	got := e.accStatus("alloc-a")
	wantAcctStatus(t, "CPU job", got, 8, 0, 100, at(12, 2, 40))
	if h, ok := got.GPUHours(); !ok || h != 0 {
		t.Errorf("GPU-hours %v %v, want a known 0", h, ok)
	}
	if h, ok := got.CPUHours(); !ok || math.Abs(h-800.0/3600) > 1e-12 {
		t.Errorf("CPU-hours %v %v, want 800/3600", h, ok)
	}
}

// gpuEntryHidden is a Platform whose accounting lists no gres entry for the GPU
// job, as if a platform changed that part of the shape.
type gpuEntryHidden struct{ Platform }

func hideGPUs(j provider.Job) provider.Job {
	if j.Accounting != nil {
		a := *j.Accounting
		a.AllocatedGPUs = nil
		j.Accounting = &a
	}
	return j
}

func (g gpuEntryHidden) ReadAccounting(ctx context.Context, id string) (provider.Job, bool, error) {
	j, found, err := g.Platform.ReadAccounting(ctx, id)
	return hideGPUs(j), found, err
}

func (g gpuEntryHidden) Observe(ctx context.Context, id string) (provider.Job, bool, error) {
	j, found, err := g.Platform.Observe(ctx, id)
	return hideGPUs(j), found, err
}

func (g gpuEntryHidden) FindByTag(ctx context.Context, tag string, since time.Time) ([]provider.Job, error) {
	jobs, err := g.Platform.FindByTag(ctx, tag, since)
	for i := range jobs {
		jobs[i] = hideGPUs(jobs[i])
	}
	return jobs, err
}

// A GPU job whose accounting lists no GPU entry: the GPU count is unknown, in
// the status and in the record, and never 0 GPU-hours. The CPUs and the elapsed
// time stay known. fake.Batch's job starts at 12:01:00 (a queue wait of 1 min
// from the hold lift at 12:00:00) and is cancelled at 12:03:00: 120 s.
func TestAccountingOfAGPUJobWithNoGPUEntryHasUnknownGPUs(t *testing.T) {
	e := newEnv(t, func(e *env) {
		e.opts.NewPlatform = func(slurm.Validated) (Platform, io.Closer, error) {
			return gpuEntryHidden{e.platform}, io.NopCloser(nil), nil
		}
	})
	e.batch.Enqueue(fake.Script{QueueWait: time.Minute})
	e.newAlloc("alloc-a")
	e.run("alloc-a", 3)
	id := e.get("alloc-a").Annotations[v1alpha1.AnnotationJobID]
	e.clock.Set(at(12, 2, 0))
	e.run("alloc-a", 1)
	got := e.get("alloc-a").Status.AtProvider.Accounting
	if got == nil || got.AllocatedCPUs == nil || *got.AllocatedCPUs != 8 || got.AllocatedGPUs != nil || got.ElapsedSeconds == nil || *got.ElapsedSeconds != 60 {
		t.Fatalf("running accounting %+v, want 8 CPUs, unknown GPUs, 60 s", got)
	}
	if _, ok := got.GPUHours(); ok {
		t.Error("GPU-hours are known without a GPU count")
	}
	e.clock.Set(at(12, 3, 0))
	e.batch.CancelExternally(id)
	e.clock.Set(at(12, 3, 30)) // past MinJobAge: accounting gives the final numbers
	e.run("alloc-a", 1)
	if err := e.c.Delete(e.ctx, e.get("alloc-a")); err != nil {
		t.Fatal(err)
	}
	e.clock.Advance(10 * time.Second)
	e.run("alloc-a", 3)
	if err := e.c.Get(e.ctx, clientKey("alloc-a"), &v1alpha1.Allocation{}); !apierrors.IsNotFound(err) {
		t.Fatalf("the Allocation is still there: %v", err)
	}
	var rec appsv1.ControllerRevision
	if err := e.c.Get(e.ctx, client.ObjectKey{Namespace: testNS, Name: "acct-uid-alloc-a"}, &rec); err != nil {
		t.Fatal(err)
	}
	tag := v1alpha1.JobTag(testNS, "alloc-a", "uid-alloc-a")
	want := fmt.Sprintf(`{"provider":"%s","partition":"%s","tag":"%s","jobIDs":["%s"],"allocatedCPUs":8,"elapsedSeconds":120,`+
		`"startTime":"2026-10-30T12:01:00Z","endTime":"2026-10-30T12:03:00Z","basis":"allocated","unknown":["allocatedGPUs"]}`,
		testProvider, gpuPart, tag, id)
	if string(rec.Data.Raw) != want {
		t.Errorf("record data\n got %s\nwant %s", rec.Data.Raw, want)
	}
}
