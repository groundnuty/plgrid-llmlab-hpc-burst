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
