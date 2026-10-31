package fake

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The tests for the queue wait: it is written to the Provider's status as soon
// as the job is observed with a start time, and the Allocation's
// queueWaitRecorded is set only after that write succeeded (docs/development.md,
// Queue waits). A job submitted at 12:00:00 with Script.QueueWait 5m starts at
// 12:05:00, so its wait is 5m.

// failStatusUpdateOnce makes the controller's client fail one status update of
// kind (a %T string, such as "*v1alpha1.Provider"), the next one after the
// returned arm function is called. The env's own client, which the test reads
// with, is not affected.
func (e *env) failStatusUpdateOnce(kind string) (arm func()) {
	armed := false
	e.ctl.Client = interceptor.NewClient(e.c.(client.WithWatch), interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if armed && fmt.Sprintf("%T", obj) == kind {
				armed = false
				return errors.New("example: the status update failed")
			}
			return c.SubResource(sub).Update(ctx, obj, opts...)
		},
	})
	return func() { armed = true }
}

func (e *env) restart() {
	e.ctl = &Controller{Client: e.c, Provider: testProvider, Batch: e.batch, Clock: e.clock}
}

func (e *env) wantOneWait(jobID string, submit, start time.Time) {
	e.t.Helper()
	waits := e.provider().Status.QueueWaits
	if len(waits) != 1 || waits[0].JobID != jobID {
		e.t.Fatalf("queue waits = %+v, want exactly one, of job %s", waits, jobID)
	}
	wantMT(e.t, "wait submit", &waits[0].SubmitTime, submit)
	wantMT(e.t, "wait start", &waits[0].StartTime, start)
}

func TestAQueueWaitSurvivesAFailedProviderStatusWrite(t *testing.T) {
	for _, restart := range []bool{false, true} {
		t.Run(fmt.Sprintf("restart=%v", restart), func(t *testing.T) {
			e := newEnv(t)
			arm := e.failStatusUpdateOnce("*v1alpha1.Provider")
			e.batch.Enqueue(Script{QueueWait: 5 * time.Minute})
			e.newAlloc("a1", testProvider)
			e.sync() // 12:00:00: pending
			if e.obs("a1").QueueWaitRecorded {
				t.Fatal("queueWaitRecorded is set before the job started")
			}

			arm()
			e.clock.Set(at(12, 6, 0)) // the job started at 12:05:00
			err := e.ctl.Sync(context.Background())
			if err == nil || !strings.Contains(err.Error(), "queue wait of job 998") {
				t.Fatalf("Sync error = %v, want the failed queue-wait write of job 998", err)
			}
			// The Allocation's status was still written, with the job as it is.
			o := e.wantPhase("a1", v1alpha1.AllocationRunning)
			wantMT(t, "startTime", o.StartTime, at(12, 5, 0))
			if o.QueueWaitRecorded {
				t.Fatal("queueWaitRecorded is true although the Provider write failed")
			}
			if got := waitIDs(e.provider()); len(got) != 0 {
				t.Fatalf("queue waits = %v, want none: the write failed", got)
			}

			if restart {
				e.restart()
			}
			e.syncAt(at(12, 7, 0))
			e.wantOneWait("998", at(12, 0, 0), at(12, 5, 0))
			if !e.obs("a1").QueueWaitRecorded {
				t.Fatal("queueWaitRecorded is false after the retry succeeded")
			}
			// Once recorded it stays so, and adds nothing.
			e.syncAt(at(12, 8, 0))
			e.wantOneWait("998", at(12, 0, 0), at(12, 5, 0))
		})
	}
}

func TestAQueueWaitIsNotAddedTwiceWhenOnlyTheAllocationWriteFailed(t *testing.T) {
	for _, restart := range []bool{false, true} {
		t.Run(fmt.Sprintf("restart=%v", restart), func(t *testing.T) {
			e := newEnv(t)
			arm := e.failStatusUpdateOnce("*v1alpha1.Allocation")
			e.batch.Enqueue(Script{QueueWait: 5 * time.Minute})
			e.newAlloc("a1", testProvider)
			e.sync() // 12:00:00: pending

			arm()
			e.clock.Set(at(12, 6, 0))
			err := e.ctl.Sync(context.Background())
			if err == nil || !strings.Contains(err.Error(), "update status") {
				t.Fatalf("Sync error = %v, want the failed Allocation status write", err)
			}
			// The Provider write went through; the Allocation does not know.
			e.wantOneWait("998", at(12, 0, 0), at(12, 5, 0))
			o := e.wantPhase("a1", v1alpha1.AllocationPending)
			if o.QueueWaitRecorded {
				t.Fatal("setup: queueWaitRecorded reached the store although the write failed")
			}

			if restart {
				e.restart()
			}
			e.syncAt(at(12, 7, 0))
			e.wantOneWait("998", at(12, 0, 0), at(12, 5, 0)) // not added a second time
			if !e.obs("a1").QueueWaitRecorded {
				t.Fatal("queueWaitRecorded is false after the retry")
			}
		})
	}
}

func TestARecordedQueueWaitIsNotAddedAgainAfterItsEntryWasEvicted(t *testing.T) {
	e := newEnv(t)
	// 22 jobs, each submitted a minute after the last and started at once; the
	// Provider keeps the last 20, so jobs 998 and 999 (Allocations a00, a01) go.
	for i := range 22 {
		e.clock.Set(at(12, 0, 0).Add(time.Duration(i) * time.Minute))
		e.newAlloc(fmt.Sprintf("a%02d", i), testProvider)
		e.sync()
	}
	waits := e.provider().Status.QueueWaits
	if len(waits) != v1alpha1.MaxQueueWaits || waits[0].JobID != "1000" {
		t.Fatalf("setup: waits start with %q, have %d, want 20 starting with 1000", waits[0].JobID, len(waits))
	}
	for _, n := range []string{"a00", "a01"} {
		if !e.obs(n).QueueWaitRecorded {
			t.Fatalf("Allocation %s: queueWaitRecorded is false although its wait was recorded", n)
		}
	}
	want := waitIDs(e.provider())

	// The newer Allocations go, so that a00 and a01 alone are observed again.
	for i := 2; i < 22; i++ {
		e.del(fmt.Sprintf("a%02d", i))
	}
	e.syncAt(at(12, 22, 0)) // a00 and a01 run until 12:30 and 12:31
	e.wantPhase("a00", v1alpha1.AllocationRunning)
	e.wantPhase("a01", v1alpha1.AllocationRunning)
	for _, restart := range []bool{false, true} {
		if restart {
			e.restart()
		}
		e.syncAt(e.clock.Now().Add(time.Minute))
		if got := waitIDs(e.provider()); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("after the evicted jobs were observed again (restart %v) the waits are %v, want %v", restart, got, want)
		}
	}
}
