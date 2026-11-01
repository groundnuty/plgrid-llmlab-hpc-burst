package fake

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// Accounting in the fake Batch (HBEP-8, Provider accounting). A job submitted at
// 12:00:00 with QueueWait 5m starts at 12:05:00. Every elapsed time below is
// worked out from those times by hand.

func accReq(tag, part string, gpus int32) provider.SubmitRequest {
	r := gpuReq(tag, false)
	r.Params.Partition, r.Params.GPUs, r.Params.CPUsPerTask = part, gpus, 8
	return r
}

func readAcct(t *testing.T, b *Batch, id string) provider.Job {
	t.Helper()
	j, found, err := b.ReadAccounting(context.Background(), id)
	if err != nil || !found {
		t.Fatalf("ReadAccounting(%s) = found %v, %v", id, found, err)
	}
	if j.Source != provider.SourceAccounting || j.Accounting == nil {
		t.Fatalf("ReadAccounting(%s): source %s accounting %v, want accounting's record", id, j.Source, j.Accounting)
	}
	return j
}

func wantCount(t *testing.T, what string, got *int32, want int32) {
	t.Helper()
	if got == nil || *got != want {
		t.Fatalf("%s = %v, want %d", what, got, want)
	}
}

func wantElapsed(t *testing.T, a *provider.JobAccounting, secs int) {
	t.Helper()
	if a.Elapsed == nil || *a.Elapsed != time.Duration(secs)*time.Second {
		t.Fatalf("Elapsed = %v, want %ds", a.Elapsed, secs)
	}
}

func TestReadAccountingOfAJobThatHasNotStarted(t *testing.T) {
	b, clock := newTestBatch(t)
	b.Enqueue(Script{QueueWait: 5 * time.Minute})
	id := mustSubmit(t, b, accReq("example-tag", gpuPart, 1))
	clock.Set(at(12, 3, 0))
	j := readAcct(t, b, id)
	if j.StartTime != nil || j.Accounting.AllocatedCPUs != nil || j.Accounting.AllocatedGPUs != nil {
		t.Errorf("a pending job: start %v, CPUs %v, GPUs %v; want none allocated", j.StartTime, j.Accounting.AllocatedCPUs, j.Accounting.AllocatedGPUs)
	}
	wantElapsed(t, j.Accounting, 0)
}

func TestAccountingOfARunningAndAnEndedJob(t *testing.T) {
	b, clock := newTestBatch(t)
	b.Enqueue(Script{QueueWait: 5 * time.Minute})
	id := mustSubmit(t, b, accReq("example-tag", gpuPart, 1))
	clock.Set(at(12, 7, 30)) // running since 12:05:00, so 150 s
	j := readAcct(t, b, id)
	wantCount(t, "AllocatedCPUs", j.Accounting.AllocatedCPUs, 8)
	wantCount(t, "AllocatedGPUs", j.Accounting.AllocatedGPUs, 1)
	wantElapsed(t, j.Accounting, 150)
	if got, _, _ := b.Observe(context.Background(), id); got.Source != provider.SourceQueue || got.Accounting != nil {
		t.Errorf("Observe while listed: source %s accounting %v, want the queue's record with none", got.Source, got.Accounting)
	}

	clock.Set(at(12, 10, 0))
	b.Cancel(context.Background(), []string{id}) // ended at 12:10:00: 300 s
	clock.Set(at(12, 10, 5))                     // inside MinJobAge (10 s): still listed
	if got, _, _ := b.Observe(context.Background(), id); got.Source != provider.SourceQueue || got.Accounting != nil {
		t.Errorf("an ended job still listed: source %s accounting %v, want the queue's record with none", got.Source, got.Accounting)
	}
	clock.Set(at(12, 10, 10)) // purged from the listing
	got, found, err := b.Observe(context.Background(), id)
	if err != nil || !found || got.Source != provider.SourceAccounting || got.Accounting == nil {
		t.Fatalf("Observe after the purge = source %s accounting %v found %v, %v", got.Source, got.Accounting, found, err)
	}
	wantCount(t, "AllocatedCPUs", got.Accounting.AllocatedCPUs, 8)
	wantCount(t, "AllocatedGPUs", got.Accounting.AllocatedGPUs, 1)
	wantElapsed(t, got.Accounting, 300)
	if got.Accounting.Executions != 1 {
		t.Errorf("Executions = %d, want 1", got.Accounting.Executions)
	}
	// The read of accounting alone still shows the ended job while it is listed.
	clock.Set(at(12, 10, 5))
	wantElapsed(t, readAcct(t, b, id).Accounting, 300)
}

// A CPU job has no gres entry, as in sacct: the GPU count is not reported, and
// the controller reads that as 0 only because the job asked for none.
func TestAccountingOfACPUJobHasNoGPUEntry(t *testing.T) {
	b, clock := newTestBatch(t)
	b.Enqueue(Script{})
	id := mustSubmit(t, b, accReq("example-cpu-tag", cpuPart, 0))
	clock.Set(at(12, 2, 0))
	j := readAcct(t, b, id)
	wantCount(t, "AllocatedCPUs", j.Accounting.AllocatedCPUs, 8)
	if j.Accounting.AllocatedGPUs != nil {
		t.Errorf("AllocatedGPUs = %d, want none for a CPU job", *j.Accounting.AllocatedGPUs)
	}
	wantElapsed(t, j.Accounting, 120)
}

// Accounting sums a requeued job's executions: 12:05:00 to 12:10:00 is 300 s;
// the requeue at 12:10:00 restarts it at 12:15:00 (QueueWait 5m), and it is
// cancelled at 12:20:00 after another 300 s.
func TestAccountingSumsRequeuedExecutions(t *testing.T) {
	b, clock := newTestBatch(t)
	b.Enqueue(Script{QueueWait: 5 * time.Minute})
	id := mustSubmit(t, b, accReq("example-tag", gpuPart, 1))
	clock.Set(at(12, 10, 0))
	b.Requeue(id)
	clock.Set(at(12, 17, 0))
	wantElapsed(t, readAcct(t, b, id).Accounting, 300+120) // the second execution has run 12:15:00 to 12:17:00
	clock.Set(at(12, 20, 0))
	b.Cancel(context.Background(), []string{id})
	clock.Set(at(12, 30, 0))
	j := readAcct(t, b, id)
	wantElapsed(t, j.Accounting, 600)
	if j.Accounting.Executions != 2 {
		t.Errorf("Executions = %d, want 2", j.Accounting.Executions)
	}
}

func TestReadAccountingFailuresAndTheCallLog(t *testing.T) {
	b, clock := newTestBatch(t)
	id := mustSubmit(t, b, accReq("example-tag", gpuPart, 1))
	clock.Set(at(12, 1, 0))
	b.Vanish(id)
	if _, found, err := b.ReadAccounting(context.Background(), id); found || err != nil {
		t.Errorf("a vanished job: found %v, %v; want not found without an error", found, err)
	}
	b.Reappear(id)
	boom := errors.New("sacct is down")
	b.FailReads(boom)
	if _, found, err := b.ReadAccounting(context.Background(), id); found || !errors.Is(err, boom) {
		t.Errorf("a failed read: found %v, %v; want the error", found, err)
	}
	b.FailReads(nil)
	if _, found, err := b.ReadAccounting(context.Background(), "9999"); found || err != nil {
		t.Errorf("an unknown job: found %v, %v; want not found", found, err)
	}
	if n := b.Count("sacct-job"); n != 3 {
		t.Errorf("sacct-job logged %d times, want 3 (every read, failed ones included)", n)
	}
}
