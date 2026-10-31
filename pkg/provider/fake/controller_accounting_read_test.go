package fake

import (
	"context"
	"errors"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The accounting read of a started job: made once per job (HBEP-8, Provider
// accounting), and only for a job the listing showed started. See
// controller_accounting_test.go for the times.

// The controller reads accounting once per job (a restarted one reads again
// only when the stored accounting still lacks the CPU count), and the call is
// made only for a job that has started.
func TestOneAccountingReadPerJob(t *testing.T) {
	e := newEnv(t)
	e.batch.Enqueue(Script{QueueWait: 5 * time.Minute})
	e.newAlloc("a1", testProvider)
	e.sync()
	e.syncAt(at(12, 3, 0))
	if n := e.batch.Count("sacct-job"); n != 0 {
		t.Fatalf("sacct-job logged %d times for a pending job, want 0", n)
	}
	for _, when := range []time.Time{at(12, 5, 0), at(12, 6, 0), at(12, 7, 0), at(12, 8, 0), at(12, 9, 0)} {
		e.syncAt(when)
	}
	if n := e.batch.Count("sacct-job"); n != 1 {
		t.Fatalf("sacct-job logged %d times across 5 passes of a running job, want 1", n)
	}
	for _, c := range e.batch.Calls() {
		if c.Op == "sacct-job" && (len(c.Args) != 1 || c.Args[0] != "998") {
			t.Fatalf("sacct-job call = %v, want job 998", c.Args)
		}
	}
	e.syncAt(at(12, 36, 0)) // ended at 12:35:00
	e.restart()             // the counts are in the stored status
	e.syncAt(at(12, 37, 0))
	if n := e.batch.Count("sacct-job"); n != 1 {
		t.Fatalf("sacct-job logged %d times after the job ended and a restart, want 1", n)
	}
	// The ended job's block was written at 12:36:00; 12:37:00 changes nothing
	// but the clock, so it is kept (the status-write cadence).
	e.wantLive("a1", 8, 1, 1800, at(12, 36, 0))
}

// Reads that fail are never counted as the one read. While the listing fails the
// job is not even observed, so no accounting read is made.
func TestAccountingReadWaitsForTheListing(t *testing.T) {
	e := newEnv(t)
	e.batch.Enqueue(Script{QueueWait: 5 * time.Minute})
	e.newAlloc("a1", testProvider)
	e.sync()
	e.batch.FailReads(errors.New("example transport failure"))
	e.syncAt(at(12, 5, 0))
	e.syncAt(at(12, 6, 0))
	if n := e.batch.Count("sacct-job"); n != 0 {
		t.Fatalf("sacct-job logged %d times while the listing failed, want 0", n)
	}
	if o := e.obs("a1"); o.Accounting != nil {
		t.Fatalf("accounting %+v appeared from failed reads", o.Accounting)
	}
	e.batch.FailReads(nil)
	e.syncAt(at(12, 7, 0))
	e.syncAt(at(12, 8, 0))
	if n := e.batch.Count("sacct-job"); n != 1 {
		t.Fatalf("sacct-job logged %d times once reads worked, want 1", n)
	}
	e.wantLive("a1", 8, 1, 180, at(12, 8, 0))
}

// A read of accounting alone that fails while the listing works (the fake's
// FailReads fails both, so the test turns it on between the two, at the
// queue-wait write that follows the listing) is retried at the next pass and
// then made once. Meanwhile the elapsed time is shown and the counts are
// unknown, and the Provider's last successful read does not advance.
func TestAFailedAccountingReadIsRetriedThenMadeOnce(t *testing.T) {
	e := newEnv(t)
	armed := false
	e.ctl.Client = interceptor.NewClient(e.c.(client.WithWatch), interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*v1alpha1.Provider); ok && armed {
				armed = false
				e.batch.FailReads(errors.New("example: sacct is down"))
			}
			return c.Get(ctx, key, obj, opts...)
		},
	})
	e.batch.Enqueue(Script{QueueWait: 5 * time.Minute})
	e.newAlloc("a1", testProvider)
	e.sync() // 12:00:00

	armed = true
	e.syncAt(at(12, 5, 0)) // started: the listing works, the accounting read fails
	e.batch.FailReads(nil)
	if n := e.batch.Count("sacct-job"); n != 1 {
		t.Fatalf("sacct-job logged %d times, want the one failed read", n)
	}
	a := e.obs("a1").Accounting
	if a == nil || a.AllocatedCPUs != nil || a.AllocatedGPUs != nil {
		t.Fatalf("accounting %+v after a failed read, want the counts unknown", a)
	}
	wantSecs(t, "elapsedSeconds", a.ElapsedSeconds, 0)
	wantMT(t, "lastSuccessfulStatusRead", e.provider().Status.LastSuccessfulStatusRead, at(12, 0, 0))

	e.syncAt(at(12, 6, 0)) // retried
	if n := e.batch.Count("sacct-job"); n != 2 {
		t.Fatalf("sacct-job logged %d times, want 2: the failed read is retried", n)
	}
	e.wantLive("a1", 8, 1, 60, at(12, 6, 0))
	e.syncAt(at(12, 7, 0))
	e.syncAt(at(12, 8, 0))
	if n := e.batch.Count("sacct-job"); n != 2 {
		t.Fatalf("sacct-job logged %d times, want 2: once the read succeeded it is not made again", n)
	}
	wantMT(t, "lastSuccessfulStatusRead", e.provider().Status.LastSuccessfulStatusRead, at(12, 8, 0))
}
