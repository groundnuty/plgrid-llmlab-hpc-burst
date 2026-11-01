package providerctl

// The status-write cadence of status.atProvider.accounting (science agent,
// 31 Oct 2026, on the review of PR 118), over the real slurm.Batch and the
// fake Slurm. The clock starts at 12:00:00 and the job starts at 12:01:00; the
// fake Slurm allocates 8 CPUs and the request's GPUs.

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// statusWrites counts the Allocation status updates that change the stored
// status: the ones the API server persists, since it drops an update that
// changes nothing. failNext makes the next such updates fail instead.
type statusWrites struct {
	mu       sync.Mutex
	n        int
	failNext int
}

func (w *statusWrites) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.n
}

func (w *statusWrites) fail(n int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.failNext = n
}

// take counts one changing write, or reports that it is scripted to fail.
func (w *statusWrites) take() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.failNext > 0 {
		w.failNext--
		return errors.New("scripted: the Allocation status write failed")
	}
	w.n++
	return nil
}

func newWriteCountingClient(t *testing.T, w *statusWrites, providers ...string) client.Client {
	t.Helper()
	return interceptor.NewClient(newFakeClient(t, providers...).(client.WithWatch), interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if a, ok := obj.(*v1alpha1.Allocation); ok && sub == "status" {
				var cur v1alpha1.Allocation
				if err := c.Get(ctx, client.ObjectKeyFromObject(a), &cur); err != nil {
					return err
				}
				if !apiequality.Semantic.DeepEqual(cur.Status, a.Status) {
					if err := w.take(); err != nil {
						return err
					}
				}
			}
			return c.SubResource(sub).Update(ctx, obj, opts...)
		},
	})
}

// A running job polled ten times within one minute gets one status write, the
// one that shows it running; a minute after its readTime the block is written
// again.
func TestAccountingOfARunningJobIsWrittenOnceAMinute(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := newSlurm(clock, fakeslurm.Config{})
	w := &statusWrites{}
	e := newRealEnv(t, clock, sl, newWriteCountingClient(t, w, "prov-a"), "prov-a", "user-a")
	e.newAlloc("alloc-a", 1)
	e.run("alloc-a", 2)
	clock.Advance(60 * time.Second)
	e.start("alloc-a") // 12:01:00
	before := w.count()
	for i := 0; i < 10; i++ { // 12:01:05 to 12:01:50
		clock.Advance(5 * time.Second)
		e.run("alloc-a", 1)
	}
	if got := w.count() - before; got != 1 {
		t.Errorf("%d status writes over ten polls within a minute of a running job, want 1", got)
	}
	wantAcctStatus(t, "after ten polls", e.accStatus("alloc-a"), 8, 1, 5, at(12, 1, 5))
	clock.Set(at(12, 2, 5))
	e.run("alloc-a", 1)
	if got := w.count() - before; got != 2 {
		t.Errorf("%d status writes once the block is a minute old, want 2", got)
	}
	wantAcctStatus(t, "a minute on", e.accStatus("alloc-a"), 8, 1, 65, at(12, 2, 5))
}

// Astra B5: the status write after a successful accounting read fails. The
// next pass still gets the counts, since it judges from the stored status that
// the read is still due, and reads again.
func TestAccountingReadSurvivesAFailedStatusWrite(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := newSlurm(clock, fakeslurm.Config{})
	w := &statusWrites{}
	e := newRealEnv(t, clock, sl, newWriteCountingClient(t, w, "prov-a"), "prov-a", "user-a")
	e.newAlloc("alloc-a", 1)
	e.run("alloc-a", 2)
	id := e.get("alloc-a").Annotations[v1alpha1.AnnotationJobID]
	clock.Advance(60 * time.Second)
	e.start("alloc-a") // 12:01:00
	w.fail(1)
	clock.Advance(40 * time.Second)
	e.run("alloc-a", 1) // 12:01:40: the read succeeds, the status write fails
	if got := jobReads(sl, id); got != 1 {
		t.Fatalf("precondition: %d accounting reads, want 1", got)
	}
	if a := e.accStatus("alloc-a"); a != nil {
		t.Fatalf("precondition: the failed write stored accounting %+v", a)
	}
	clock.Advance(5 * time.Second)
	e.run("alloc-a", 1) // 12:01:45
	wantAcctStatus(t, "the pass after the failed write", e.accStatus("alloc-a"), 8, 1, 45, at(12, 1, 45))
	if got := jobReads(sl, id); got != 2 {
		t.Errorf("%d accounting reads, want 2: the one whose write failed, and one more", got)
	}
}

// Astra nit 10: an empty first read (accounting lags the queue) is not a usable
// observation. It is retried at most once a minute until the first usable one,
// and never after it.
func TestAnEmptyAccountingReadIsRetriedOnceAMinuteUntilUsable(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := newSlurm(clock, fakeslurm.Config{})
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a")
	e.newAlloc("alloc-a", 1)
	e.run("alloc-a", 2)
	id := e.get("alloc-a").Annotations[v1alpha1.AnnotationJobID]
	if err := sl.AcctHide(jobNumber(t, id)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(60 * time.Second)
	e.start("alloc-a") // 12:01:00
	every20s := func(n int) {
		for i := 0; i < n; i++ {
			clock.Advance(20 * time.Second)
			e.run("alloc-a", 1)
		}
	}
	every20s(9) // 12:01:20 (read), 12:02:20 (read), 12:03:20 (read), ..., 12:04:00
	if got := jobReads(sl, id); got != 3 {
		t.Errorf("%d accounting reads over 9 polls 20 s apart while accounting does not show the job, want 3", got)
	}
	wantAcctStatus(t, "hidden", e.accStatus("alloc-a"), -1, -1, 140, at(12, 3, 20))
	if err := sl.AcctShow(jobNumber(t, id)); err != nil {
		t.Fatal(err)
	}
	every20s(1) // 12:04:20: a minute after 12:03:20, the read is usable
	wantAcctStatus(t, "usable", e.accStatus("alloc-a"), 8, 1, 200, at(12, 4, 20))
	every20s(9) // to 12:07:20
	if got := jobReads(sl, id); got != 4 {
		t.Errorf("%d accounting reads, want 4: none after the usable one", got)
	}
}

// Astra nit 10 for a job that ended while accounting still lags: the listing
// shows it ended (Athena keeps it until MinJobAge), so its block no longer
// changes, and each retry stores its own readTime: still at most one read a
// minute, not one per poll.
func TestAnEndedJobsEmptyAccountingReadIsRetriedOnceAMinute(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := newSlurm(clock, fakeslurm.Config{NotListed: fakeslurm.NotListedAthena, MinJobAge: time.Hour})
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a")
	e.newAlloc("alloc-a", 1)
	e.run("alloc-a", 2)
	id := e.get("alloc-a").Annotations[v1alpha1.AnnotationJobID]
	if err := sl.AcctHide(jobNumber(t, id)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(60 * time.Second)
	e.start("alloc-a") // 12:01:00
	clock.Advance(30 * time.Second)
	endJob(t, sl, id, fakeslurm.Completed) // 12:01:30: 30 s
	clock.Advance(10 * time.Second)
	e.run("alloc-a", 1) // 12:01:40: ended in the listing, read 1
	wantAcctStatus(t, "ended", e.accStatus("alloc-a"), -1, -1, 30, at(12, 1, 40))
	for i := 0; i < 9; i++ { // 12:02:00 to 12:04:40, 20 s apart: reads at 12:02:40, 12:03:40 and 12:04:40
		clock.Advance(20 * time.Second)
		e.run("alloc-a", 1)
	}
	if got := jobReads(sl, id); got != 4 {
		t.Errorf("%d accounting reads of an ended job over 10 polls 20 s apart, want 4", got)
	}
	wantAcctStatus(t, "after the retries", e.accStatus("alloc-a"), -1, -1, 30, at(12, 4, 40))
}
