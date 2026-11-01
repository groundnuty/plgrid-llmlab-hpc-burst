package providerctl

// New (#199). Deletion raises JobCancelled only for a job the Batch found live
// when it cancelled it. A job that had ended or that Slurm no longer knows is a
// no-op: no event, and deletion still completes. These run the controller over
// the real slurm.Batch and the fake Slurm, whose scancel is quiet for both, as
// real Slurm 24.11's is: against a fake that still printed the error text, the
// old classification by stderr passes, which is how the fault stayed hidden.
//
// The ended and unknown cases need Observe to see the job live in the listing
// the Batch cached and the job to end after that read, so the platform's end is
// made between two reconciles without advancing the clock (the cached listing
// is not due) and without a Batch write (which would mark it stale).

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// jobCancelled counts the JobCancelled events the recorder holds.
func jobCancelled(rec *recorder) (n int) {
	for _, r := range rec.reasons() {
		if r == "JobCancelled" {
			n++
		}
	}
	return n
}

// squeueReads counts the single-job squeue reads of a job: what Batch.Cancel
// makes for each job it is given, and nothing else does for a job being deleted.
func squeueReads(sl *fakeslurm.Platform, id string) (n int) {
	for _, c := range sl.Calls() {
		if len(c.Words) > 0 && c.Words[0] == "squeue" && strings.Contains(strings.Join(c.Words, " "), "--jobs="+id) {
			n++
		}
	}
	return n
}

// runningAllocation is an Allocation whose job is running, with the Batch's
// listing cached and live.
func runningAllocation(t *testing.T, rec *recorder) (*realEnv, *fakeslurm.Platform, string) {
	t.Helper()
	clock := fake.NewClock(at(12, 0, 0))
	sl := newSlurm(clock, fakeslurm.Config{NotListed: fakeslurm.NotListedHelios})
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a", withRecorder(rec))
	e.newAlloc("alloc-a", 1)
	e.run("alloc-a", 2)
	id := e.get("alloc-a").Annotations[v1alpha1.AnnotationJobID]
	e.start("alloc-a")
	e.poll("alloc-a", 1)
	if got := slurmJob(t, sl, id).State; got != fakeslurm.Running {
		t.Fatalf("precondition: job %s is %s, want RUNNING", id, got)
	}
	return e, sl, id
}

func TestDeleteRaisesJobCancelledForALiveJob(t *testing.T) {
	rec := &recorder{}
	e, sl, id := runningAllocation(t, rec)
	e.del("alloc-a")
	e.run("alloc-a", 1)
	if got := slurmJob(t, sl, id).State; got != fakeslurm.Cancelled {
		t.Fatalf("job %s is %s after the delete, want CANCELLED", id, got)
	}
	if n := jobCancelled(rec); n != 1 {
		t.Errorf("%d JobCancelled events for a live job, want 1: %v", n, rec.reasons())
	}
	for _, ev := range rec.events {
		if string(ev.Reason) == "JobCancelled" && !strings.Contains(ev.Message, "job "+id+" ") {
			t.Errorf("JobCancelled message %q does not name job %s", ev.Message, id)
		}
	}
	deleteUntilGone(e, sl, "alloc-a", id)
	if n := jobCancelled(rec); n != 1 {
		t.Errorf("%d JobCancelled events by the end of the deletion, want 1: %v", n, rec.reasons())
	}
}

// The job ends on the platform after Observe's listing was read: Cancel finds
// it ended, reports AlreadyEnded, and no event says it was cancelled.
func TestDeleteRaisesNoJobCancelledForAJobThatHadEnded(t *testing.T) {
	rec := &recorder{}
	e, sl, id := runningAllocation(t, rec)
	n, _ := strconv.Atoi(id)
	if err := sl.End(n, fakeslurm.Completed); err != nil {
		t.Fatal(err)
	}
	e.del("alloc-a")
	e.run("alloc-a", 1)
	if got := squeueReads(sl, id); got != 1 {
		t.Fatalf("precondition: Cancel read job %s %d times, want 1: Observe did not see the job live in the cached listing, so this test proves nothing", id, got)
	}
	if got := slurmJob(t, sl, id).State; got != fakeslurm.Completed {
		t.Errorf("job %s is %s, want COMPLETED untouched", id, got)
	}
	if c := sl.Count("scancel"); c != 0 {
		t.Errorf("%d scancel calls for a job that had ended, want 0", c)
	}
	if got := jobCancelled(rec); got != 0 {
		t.Errorf("%d JobCancelled events for a job that had ended, want 0: %v", got, rec.reasons())
	}
	deleteUntilGone(e, sl, "alloc-a", id)
	if got := jobCancelled(rec); got != 0 {
		t.Errorf("%d JobCancelled events by the end of the deletion, want 0: %v", got, rec.reasons())
	}
}

// The job ended and neither the queue nor accounting shows it any more (the
// accounting record is hidden): Cancel reports ErrNotFound, which deletion
// treats as a job that has ended: no event, no error.
func TestDeleteRaisesNoJobCancelledForAJobSlurmNoLongerKnows(t *testing.T) {
	rec := &recorder{}
	e, sl, id := runningAllocation(t, rec)
	n, _ := strconv.Atoi(id)
	if err := sl.End(n, fakeslurm.Completed); err != nil {
		t.Fatal(err)
	}
	if err := sl.AcctHide(n); err != nil {
		t.Fatal(err)
	}
	e.del("alloc-a")
	e.run("alloc-a", 1)
	if got := squeueReads(sl, id); got != 1 {
		t.Fatalf("precondition: Cancel read job %s %d times, want 1: Observe did not see the job live in the cached listing, so this test proves nothing", id, got)
	}
	if c := sl.Count("scancel"); c != 0 {
		t.Errorf("%d scancel calls for an unknown job, want 0", c)
	}
	if got := jobCancelled(rec); got != 0 {
		t.Errorf("%d JobCancelled events for a job Slurm no longer knows, want 0: %v", got, rec.reasons())
	}
	// Deletion goes on: no error was returned for the unknown job, and the
	// Allocation goes once the job is read as ended or lost.
	for i := 0; i < 40 && !e.gone("alloc-a"); i++ {
		e.clock.Advance(30 * time.Second)
		e.run("alloc-a", 1)
	}
	if !e.gone("alloc-a") {
		t.Error("the Allocation is still there after the job became unknown")
	}
	if got := jobCancelled(rec); got != 0 {
		t.Errorf("%d JobCancelled events by the end of the deletion, want 0: %v", got, rec.reasons())
	}
}
