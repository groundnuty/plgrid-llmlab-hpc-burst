package fake

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// The kept record when the final read does not come, and the record's other
// rules (HBEP-8, Provider accounting). See controller_accounting_test.go for
// the times: a job with QueueWait 5m starts at 12:05:00.

// vanishedEnv returns an Allocation whose job started at 12:05:00 and would
// have failed at 12:15:00. The last live pass is at 12:10:00 (300 s, 8 CPUs, 1
// GPU stored); then the job is hidden from both sources and the Allocation is
// deleted. The first deleting pass is at 12:20:00.
func vanishedEnv(t *testing.T) *env {
	t.Helper()
	e := newEnv(t)
	failAfter := 10 * time.Minute
	e.batch.Enqueue(Script{QueueWait: 5 * time.Minute, FailAfter: &failAfter})
	e.newAlloc("a1", testProvider)
	e.sync()
	e.syncAt(at(12, 5, 0))
	e.syncAt(at(12, 10, 0))
	e.wantLive("a1", 8, 1, 300, at(12, 10, 0))
	e.batch.Vanish("998") // after the last live pass: none of the later ones sets an anchor
	e.del("a1")
	return e
}

// waitsFor checks the pass at when keeps the Allocation, writes no record, and
// leaves the status as the last live pass wrote it except for the anchor.
func (e *env) waitsFor(when time.Time, anchor time.Time) {
	e.t.Helper()
	e.syncAt(when)
	if e.gone("a1") {
		e.t.Fatalf("at %s the Allocation is gone: it should wait for the final read", when.Format("15:04:05"))
	}
	e.wantNoRecord("a1")
	o := e.obs("a1")
	wantMT(e.t, "absentSince", o.AbsentSince, anchor)
	wantMT(e.t, "startTime", o.StartTime, at(12, 5, 0))
	e.wantLive("a1", 8, 1, 300, at(12, 10, 0))
	if n := e.batch.Count("remove-allocation-files"); n != 0 {
		e.t.Fatalf("remove-allocation-files logged %d times while waiting, want 0", n)
	}
}

// vanishedWant is the record of the job above once the wait is over: the counts
// and the start time of the stored status, the elapsed and end times unknown,
// and the recorded job's ID although no read showed it.
var vanishedWant = recWant{cpus: i32(8), gpus: i32(1), start: tp(at(12, 5, 0)),
	unknown: []string{"elapsedSeconds", "endTime"}, jobIDs: []string{"998"}}

// The job ended and accounting does not have it: the Allocation waits LostGrace
// from the first pass that missed it, a restarted Controller keeps the wait, and
// the record then has the elapsed time unknown.
func TestRecordWaitsForTheFinalReadThenIsWrittenUnknown(t *testing.T) {
	for _, tc := range []struct {
		name  string
		grace time.Duration // the Controller's LostGrace, 0 for the default 2m
		last  time.Time     // exactly LostGrace after the anchor: still waiting
	}{
		{"default grace 2m", 0, at(12, 22, 0)},
		{"custom grace 10s", 10 * time.Second, at(12, 20, 10)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := vanishedEnv(t)
			e.ctl.LostGrace = tc.grace
			restart := func() { // a new Controller value on the same client, with the same setting
				e.restart()
				e.ctl.LostGrace = tc.grace
			}
			// A failed read is not absence: no anchor, and the finalizer stays.
			e.batch.FailReads(errors.New("example transport failure"))
			e.syncAt(at(12, 19, 0))
			if e.gone("a1") || e.obs("a1").AbsentSince != nil {
				t.Fatalf("a failed read ended the wait or set an anchor: gone %v, absentSince %v", e.gone("a1"), e.obs("a1").AbsentSince)
			}
			e.batch.FailReads(nil)

			e.waitsFor(at(12, 20, 0), at(12, 20, 0)) // the first pass that misses the job: the anchor
			restart()
			e.waitsFor(at(12, 20, 5), at(12, 20, 0))
			restart()
			e.waitsFor(tc.last, at(12, 20, 0))

			restart()
			e.syncAt(tc.last.Add(time.Second)) // one second more
			if !e.gone("a1") {
				t.Fatal("the Allocation survived the grace")
			}
			e.wantRecord("a1", vanishedWant)
			if n := e.batch.Count("sacct-job"); n != 1 {
				t.Errorf("sacct-job logged %d times, want the 1 of the live pass: the wait makes no read of its own", n)
			}
			if n := e.batch.Count("remove-allocation-files"); n != 1 {
				t.Errorf("remove-allocation-files logged %d times, want 1", n)
			}
		})
	}
}

// The final read comes within the grace: the record has the real numbers.
func TestRecordUsesTheFinalReadWhenItComesWithinTheGrace(t *testing.T) {
	e := vanishedEnv(t)
	e.waitsFor(at(12, 20, 0), at(12, 20, 0))
	e.batch.Reappear("998") // ended 12:15:00: accounting's elapsed is 600 s
	e.syncAt(at(12, 21, 0))
	if !e.gone("a1") {
		t.Fatal("the Allocation survived although the final read came")
	}
	e.wantRecord("a1", recWant{cpus: i32(8), gpus: i32(1), secs: i64(600), start: tp(at(12, 5, 0)), end: tp(at(12, 15, 0)), jobIDs: []string{"998"}})
}

// A job that was never seen started has nothing to wait for.
func TestRecordOfAnUnseenNeverStartedJobIsWrittenAtOnce(t *testing.T) {
	e := newEnv(t)
	e.batch.Enqueue(Script{QueueWait: 10 * time.Minute})
	e.newAlloc("a1", testProvider)
	e.sync()
	e.batch.Vanish("998")
	e.del("a1")
	e.syncAt(at(12, 3, 0))
	if !e.gone("a1") {
		t.Fatal("the Allocation waited for a job that was never seen started")
	}
	e.wantRecord("a1", recWant{jobIDs: []string{"998"},
		unknown: []string{"allocatedCPUs", "allocatedGPUs", "elapsedSeconds", "startTime", "endTime"}})
}

// An anchor that a live pass set while the job was missing counts: the grace may
// already be over when the Allocation is deleted.
func TestRecordKeepsTheAnchorOfALivePass(t *testing.T) {
	e := newEnv(t)
	failAfter := 10 * time.Minute
	e.batch.Enqueue(Script{QueueWait: 5 * time.Minute, FailAfter: &failAfter})
	e.newAlloc("a1", testProvider)
	e.sync()
	e.syncAt(at(12, 5, 0))
	e.syncAt(at(12, 10, 0))
	e.batch.Vanish("998")
	e.syncAt(at(12, 12, 0)) // the live anchor
	e.syncAt(at(12, 14, 1)) // Lost
	e.wantPhase("a1", v1alpha1.AllocationLost)
	e.del("a1")
	e.syncAt(at(12, 20, 0)) // 8 minutes after the anchor
	if !e.gone("a1") {
		t.Fatal("the Allocation waited again after a Lost job's grace had passed")
	}
	e.wantRecord("a1", vanishedWant)
}

// The record is created if absent and never rewritten.
func TestAnExistingRecordIsNotRewritten(t *testing.T) {
	e := newEnv(t)
	e.batch.Enqueue(Script{QueueWait: 5 * time.Minute})
	e.newAlloc("a1", testProvider)
	e.sync()
	e.syncAt(at(12, 5, 0))
	e.syncAt(at(12, 10, 0))
	old, err := v1alpha1.NewAccountingRecord(testNS, "uid-a1", "", v1alpha1.AccountingRecordData{
		Provider: testProvider, Partition: "example-old-partition", Tag: "example-old-tag", JobIDs: []string{"1"},
		AllocatedCPUs: i32(2), AllocatedGPUs: i32(3), ElapsedSeconds: i64(99),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.c.Create(context.Background(), old); err != nil {
		t.Fatal(err)
	}
	before, _ := e.acct("a1")

	e.del("a1")
	e.sync()
	if !e.gone("a1") {
		t.Fatal("an existing record kept the Allocation")
	}
	after, ok := e.acct("a1")
	if !ok || !bytes.Equal(after.Data.Raw, before.Data.Raw) || after.ResourceVersion != before.ResourceVersion {
		t.Fatalf("the existing record changed: data %s -> %s, resourceVersion %s -> %s", before.Data.Raw, after.Data.Raw, before.ResourceVersion, after.ResourceVersion)
	}
	if !strings.Contains(string(after.Data.Raw), `"elapsedSeconds":99`) {
		t.Fatalf("record data = %s, want the pre-created 99 s", after.Data.Raw)
	}
	if n := e.batch.Count("scancel"); n != 1 {
		t.Errorf("scancel logged %d times, want 1: the deletion goes on as before", n)
	}
}

// A create that fails keeps the finalizer (and the files) and fails the pass;
// the next pass writes the record from the job the first pass cancelled.
func TestAFailedRecordCreateKeepsTheFinalizer(t *testing.T) {
	e := newEnv(t)
	fail := true
	e.ctl.Client = interceptor.NewClient(e.c.(client.WithWatch), interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if _, ok := obj.(*appsv1.ControllerRevision); ok && fail {
				return errors.New("example: the create failed")
			}
			return c.Create(ctx, obj, opts...)
		},
	})
	e.batch.Enqueue(Script{QueueWait: 5 * time.Minute})
	e.newAlloc("a1", testProvider)
	e.sync()
	e.syncAt(at(12, 5, 0))
	e.syncAt(at(12, 10, 0))
	e.del("a1")

	err := e.ctl.Sync(context.Background())
	if err == nil || !strings.Contains(err.Error(), "create accounting record acct-uid-a1") {
		t.Fatalf("Sync = %v, want the failed create of acct-uid-a1", err)
	}
	if e.gone("a1") {
		t.Fatal("the Allocation went although its record was not written")
	}
	e.wantNoRecord("a1")
	if n := e.batch.Count("remove-allocation-files"); n != 0 {
		t.Fatalf("remove-allocation-files logged %d times before the record, want 0", n)
	}

	fail = false
	e.sync()
	if !e.gone("a1") {
		t.Fatal("the Allocation survived once the create worked")
	}
	e.wantRecord("a1", recWant{cpus: i32(8), gpus: i32(1), secs: i64(300), start: tp(at(12, 5, 0)), end: tp(at(12, 10, 0)), jobIDs: []string{"998"}})
	if n := e.batch.Count("scancel"); n != 1 {
		t.Errorf("scancel logged %d times, want 1", n)
	}
}

// The record names the ModelDeployment the Allocation was assigned to at
// deletion, and lists every job with the tag, in numeric order (998, 999 and
// 1000 are not in string order), while its numbers are the recorded job's.
func TestRecordNamesTheModelDeploymentAndEveryJob(t *testing.T) {
	e := newEnv(t)
	e.batch.Enqueue(Script{QueueWait: 5 * time.Minute})
	e.newAlloc("a1", testProvider)
	e.mutateSpec("a1", func(s *v1alpha1.AllocationSpec) {
		s.ModelDeploymentRef = &v1alpha1.ObjectReference{Name: "example-model"}
	})
	e.sync()
	e.syncAt(at(12, 5, 0))
	for range 2 { // duplicates with the tag, held: they never start
		if _, outcome, err := e.batch.Submit(context.Background(), provider.SubmitRequest{
			Tag: v1alpha1.JobTag(testNS, "a1", "uid-a1"), Hold: true, Params: *e.alloc("a1").Spec.ForProvider.Batch,
		}); outcome != provider.Accepted || err != nil {
			t.Fatalf("setup Submit = %s %v", outcome, err)
		}
	}
	e.syncAt(at(12, 6, 0)) // job 998 has run 60 s
	e.del("a1")
	e.sync()
	if !e.gone("a1") {
		t.Fatal("Allocation survived its deletion")
	}
	e.wantRecord("a1", recWant{cpus: i32(8), gpus: i32(1), secs: i64(60), start: tp(at(12, 5, 0)), end: tp(at(12, 6, 0)),
		jobIDs: []string{"998", "999", "1000"}, md: "example-model"})
}

// An Allocation that never had a job has nothing to account: no record.
func TestAnAllocationThatNeverHadAJobWritesNoRecord(t *testing.T) {
	e := newEnv(t)
	e.batch.Enqueue(Script{Outcome: provider.Rejected, Reason: "example: refused"})
	e.newAlloc("a1", testProvider)
	e.sync()
	if _, ok := e.alloc("a1").Annotations[v1alpha1.AnnotationJobID]; ok {
		t.Fatal("setup: the Allocation has a job ID")
	}
	e.del("a1")
	e.sync()
	if !e.gone("a1") {
		t.Fatal("Allocation survived its deletion")
	}
	var list appsv1.ControllerRevisionList
	if err := e.c.List(context.Background(), &list); err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 0 {
		t.Fatalf("records = %d, want none: there was no job to account", len(list.Items))
	}
	if n := e.batch.Count("remove-allocation-files"); n != 1 {
		t.Errorf("remove-allocation-files logged %d times, want 1: the deletion goes on as before", n)
	}
}
