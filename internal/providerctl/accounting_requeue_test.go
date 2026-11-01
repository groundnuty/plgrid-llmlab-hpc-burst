package providerctl

// A requeued job's accounting (science agent, 31 Oct 2026, on the review of
// PR 118: Astra B1 to B4). A job counts as requeued when it has more than one
// execution, a restart count above 0, or Requeued already in the Allocation's
// status (sticky). Its allocated counts, elapsed time and hours are unknown;
// the record is still written, with the job IDs and those fields unknown. Over
// the real slurm.Batch and the fake Slurm, in both dialects. The clock starts
// at 12:00:00 and the first execution at 12:01:00 with 8 CPUs and 1 GPU; the
// queue listing is cached for 30 s.

import (
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

type requeueEnv struct {
	*realEnv
	sl *fakeslurm.Platform
	id string
	n  int
}

// newRequeueEnv starts alloc-a's job at 12:01:00 and observes it running at
// 12:01:40, its counts read once.
func newRequeueEnv(t *testing.T, version string) *requeueEnv {
	t.Helper()
	clock := fake.NewClock(at(12, 0, 0))
	sl := newSlurm(clock, fakeslurm.Config{DataParser: version})
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a", func(e *realEnv) {
		e.profiles.set(func(v *slurm.Validated) { v.Profile.Spec.JSONDataParser = version })
	})
	e.newAlloc("alloc-a", 1)
	e.run("alloc-a", 2)
	r := &requeueEnv{realEnv: e, sl: sl, id: e.get("alloc-a").Annotations[v1alpha1.AnnotationJobID]}
	r.n = jobNumber(t, r.id)
	clock.Advance(60 * time.Second)
	e.start("alloc-a")
	e.poll("alloc-a", 1)
	wantAcctStatus(t, "first execution", e.accStatus("alloc-a"), 8, 1, 40, at(12, 1, 40))
	if got := jobReads(sl, r.id); got != 1 {
		t.Fatalf("precondition: %d accounting reads, want 1", got)
	}
	return r
}

func (r *requeueEnv) at(h, m, s int) { r.clock.Set(at(h, m, s)) }

func (r *requeueEnv) do(what string, f func(int) error) {
	r.t.Helper()
	if err := f(r.n); err != nil {
		r.t.Fatalf("%s: %v", what, err)
	}
}

// unknown checks that the block is there, read at read, with nothing known.
func (r *requeueEnv) unknown(what string, read time.Time) {
	r.t.Helper()
	wantAcctStatus(r.t, what, r.accStatus("alloc-a"), -1, -1, -1, read)
}

// noAllocatedRead checks that no accounting read was made since the first
// execution's: none is made for a requeued job, whose counts are unknown.
func (r *requeueEnv) noAllocatedRead(what string) {
	r.t.Helper()
	if got := jobReads(r.sl, r.id); got != 1 {
		r.t.Errorf("%s: %d accounting reads, want still 1: none for a requeued job", what, got)
	}
}

// deleteAndWantRecord deletes the Allocation and checks its record: the job
// IDs and times, the counts and elapsed time unknown.
func (r *requeueEnv) deleteAndWantRecord(start, end string) {
	r.t.Helper()
	head := head(r.realEnv, "alloc-a", r.id)
	r.del("alloc-a")
	deleteUntilGone(r.realEnv, r.sl, "alloc-a", r.id)
	r.wantRecord("alloc-a", head+`,"startTime":"2026-10-30T`+start+`Z","endTime":"2026-10-30T`+end+`Z","basis":"allocated",`+
		`"unknown":["allocatedCPUs","allocatedGPUs","elapsedSeconds"]}`)
}

var dialects = []string{"v0.0.40", "v0.0.42"}

// Requeued at 12:02:00 and seen pending, started again at 12:03:00, ended at
// 12:03:45. Pending, the Allocation has no accounting; afterwards everything
// is unknown, and no read is made for the second execution's counts.
func TestARequeuedJobHasUnknownHours(t *testing.T) {
	for _, v := range dialects {
		t.Run(v, func(t *testing.T) {
			r := newRequeueEnv(t, v)
			r.at(12, 2, 0)
			r.do("requeue", r.sl.Requeue)
			r.poll("alloc-a", 1) // 12:02:40
			if a := r.accStatus("alloc-a"); a != nil {
				t.Errorf("a requeued job that waits again has accounting %+v, want none", a)
			}
			r.at(12, 3, 0)
			r.do("start", r.sl.Start)
			r.at(12, 3, 20)
			r.run("alloc-a", 1)
			r.unknown("second execution running", at(12, 3, 20))
			r.poll("alloc-a", 2) // 12:04:00, 12:04:40: still running
			r.noAllocatedRead("second execution running")
			r.at(12, 4, 45)
			endJob(t, r.sl, r.id, fakeslurm.Completed)
			r.poll("alloc-a", 1) // 12:05:25: accounting has both executions
			r.unknown("ended", at(12, 5, 25))
			r.deleteAndWantRecord("12:03:00", "12:04:45")
		})
	}
}

// Astra B4: the requeue and the restart both happen between two polls, so no
// pending state is ever observed. The listing's restart count tells; the
// stored counts of the first execution are dropped at once, 35 s after they
// were read, not at the minute.
func TestARequeueAndRestartBetweenPollsHasUnknownHours(t *testing.T) {
	for _, v := range dialects {
		t.Run(v, func(t *testing.T) {
			r := newRequeueEnv(t, v)
			r.at(12, 1, 50)
			r.do("requeue", r.sl.Requeue)
			r.do("start", r.sl.Start)
			r.at(12, 2, 15)
			r.run("alloc-a", 1)
			if o := r.get("alloc-a").Status.AtProvider; o.Phase != v1alpha1.AllocationFailed || o.Reason != v1alpha1.ReasonRequeued {
				t.Fatalf("precondition: phase %s reason %q, want Failed, Requeued (HBEP-4)", o.Phase, o.Reason)
			}
			r.unknown("restarted between polls", at(12, 2, 15))
			r.poll("alloc-a", 2)
			r.noAllocatedRead("restarted between polls")
			r.at(12, 3, 30)
			endJob(t, r.sl, r.id, fakeslurm.Completed)
			r.poll("alloc-a", 1)
			r.unknown("ended", at(12, 4, 10))
			r.deleteAndWantRecord("12:01:50", "12:03:30")
		})
	}
}

// Astra B3: the status already records the requeue, and every later reply
// shows one execution only, with a restart count of 0 (the fake Slurm forgets
// the earlier execution). The requeue seen is kept: the hours stay unknown,
// live and in the record.
func TestARequeueSeenOnceKeepsTheHoursUnknown(t *testing.T) {
	for _, v := range dialects {
		t.Run(v, func(t *testing.T) {
			r := newRequeueEnv(t, v)
			r.at(12, 2, 0)
			r.do("requeue", r.sl.Requeue)
			r.poll("alloc-a", 1) // 12:02:40: pending, Requeued in the status
			if !r.get("alloc-a").Status.AtProvider.Requeued {
				t.Fatal("precondition: the status does not record the requeue")
			}
			r.at(12, 3, 0)
			r.do("start", r.sl.Start)
			r.do("forget the earlier execution", r.sl.ForgetRequeue)
			r.at(12, 3, 20)
			r.run("alloc-a", 1)
			r.unknown("one execution shown, running", at(12, 3, 20))
			r.noAllocatedRead("one execution shown")
			r.at(12, 4, 0)
			endJob(t, r.sl, r.id, fakeslurm.Completed)
			r.poll("alloc-a", 1) // 12:04:40: accounting shows one 60 s execution
			r.unknown("one execution shown, ended", at(12, 4, 40))
			r.deleteAndWantRecord("12:03:00", "12:04:00")
		})
	}
}
