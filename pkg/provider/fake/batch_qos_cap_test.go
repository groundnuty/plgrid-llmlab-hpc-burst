package fake

import (
	"context"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// Tests of the cap on the user's GPUs (Config.MaxGPUsPerUser). Every expected
// time and reason is a literal worked out by hand. Each test that shows the cap
// has a job running beside the pending one: with nothing running, a job always
// fits and the test could not tell a cap from no cap. A request is 1 GPU for 30
// minutes (gpuReq) unless a test says otherwise.

func gpuReqN(tag string, gpus int32) provider.SubmitRequest {
	req := gpuReq(tag, false)
	req.Params.GPUs = gpus
	return req
}

// wantWaitingOnCap checks that the job is pending, and pending only on the cap:
// state PENDING, reason QOSMaxGRESPerUser, phase Pending, no hold, no start and
// no estimate.
func wantWaitingOnCap(t *testing.T, b *Batch, id string) provider.Job {
	t.Helper()
	j := wantPhase(t, b, id, v1alpha1.AllocationPending)
	if len(j.State) != 1 || j.State[0] != "PENDING" || j.Reason != "QOSMaxGRESPerUser" || j.Hold != provider.HoldNone || j.Held() {
		t.Fatalf("at %s job %s = state %v reason %q hold %q, want PENDING with reason QOSMaxGRESPerUser and no hold", b.clock.Now().Format("15:04:05"), id, j.State, j.Reason, j.Hold)
	}
	wantNilTime(t, "StartTime", j.StartTime)
	wantNilTime(t, "HardEnd", j.HardEnd)
	wantNilTime(t, "EndTime", j.EndTime)
	wantNilTime(t, "StartEstimate", j.StartEstimate)
	return j
}

// wantRunningFrom checks that the job runs, started at start and with its hard
// end at hardEnd.
func wantRunningFrom(t *testing.T, b *Batch, id string, start, hardEnd time.Time) provider.Job {
	t.Helper()
	j := wantPhase(t, b, id, v1alpha1.AllocationRunning)
	wantTime(t, "StartTime", j.StartTime, start)
	wantTime(t, "HardEnd", j.HardEnd, hardEnd)
	return j
}

// wantEnded checks that the job ended in state at end, with the phase Ended
// (TIMEOUT, CANCELLED).
func wantEnded(t *testing.T, b *Batch, id, state string, end time.Time) provider.Job {
	t.Helper()
	return wantFinished(t, b, id, v1alpha1.AllocationEnded, state, end)
}

// wantFinished checks that the job ended in state at end, with the given phase:
// a job that fails (FAILED, NODE_FAIL) has the phase Failed.
func wantFinished(t *testing.T, b *Batch, id string, phase v1alpha1.AllocationPhase, state string, end time.Time) provider.Job {
	t.Helper()
	j := wantPhase(t, b, id, phase)
	if len(j.State) != 1 || j.State[0] != state {
		t.Fatalf("job %s state = %v, want %s", id, j.State, state)
	}
	wantTime(t, "EndTime", j.EndTime, end)
	return j
}

func TestQOSGPUCapHoldsASecondJobUntilTheFirstIsCancelled(t *testing.T) {
	b, clock := newQOSBatch(Config{MaxGPUsPerUser: 1})
	a := mustSubmit(t, b, gpuReq("example-a", false)) // 12:00:00, hard end 12:30:00
	wantRunningFrom(t, b, a, at(12, 0, 0), at(12, 30, 0))

	clock.Set(at(12, 1, 0))
	bID := mustSubmit(t, b, gpuReq("example-b", false))
	wantWaitingOnCap(t, b, bID)
	clock.Set(at(12, 9, 59))
	wantWaitingOnCap(t, b, bID)

	clock.Set(at(12, 10, 0))
	b.CancelExternally(a)
	wantEnded(t, b, a, "CANCELLED", at(12, 10, 0))
	wantRunningFrom(t, b, bID, at(12, 10, 0), at(12, 40, 0))
}

func TestQOSGPUCapFreesAtTheHardEndOfTheFirstJob(t *testing.T) {
	b, clock := newQOSBatch(Config{MaxGPUsPerUser: 1})
	a := mustSubmit(t, b, gpuReq("example-a", false)) // hard end 12:30:00
	clock.Set(at(12, 1, 0))
	bID := mustSubmit(t, b, gpuReq("example-b", false))

	clock.Set(at(12, 29, 59))
	wantWaitingOnCap(t, b, bID)
	clock.Set(at(12, 30, 0))
	wantEnded(t, b, a, "TIMEOUT", at(12, 30, 0))
	wantRunningFrom(t, b, bID, at(12, 30, 0), at(13, 0, 0))
	// The start was reached and stays, however far the clock goes on.
	clock.Set(at(12, 45, 0))
	wantRunningFrom(t, b, bID, at(12, 30, 0), at(13, 0, 0))
}

func TestQOSGPUCapOfTwoStartsTheSecondJobAndHoldsTheThird(t *testing.T) {
	b, clock := newQOSBatch(Config{MaxGPUsPerUser: 2})
	a := mustSubmit(t, b, gpuReq("example-a", false)) // 12:00:00 to 12:30:00
	clock.Set(at(12, 0, 30))
	bID := mustSubmit(t, b, gpuReq("example-b", false)) // fits beside A: 12:00:30 to 12:30:30
	clock.Set(at(12, 1, 0))
	c := mustSubmit(t, b, gpuReq("example-c", false)) // 3 GPUs would run: waits

	wantRunningFrom(t, b, a, at(12, 0, 0), at(12, 30, 0))
	wantRunningFrom(t, b, bID, at(12, 0, 30), at(12, 30, 30))
	wantWaitingOnCap(t, b, c)

	// A's hard end frees one GPU; C takes it at once, and B keeps its start.
	clock.Set(at(12, 30, 0))
	wantRunningFrom(t, b, c, at(12, 30, 0), at(13, 0, 0))
	wantRunningFrom(t, b, bID, at(12, 0, 30), at(12, 30, 30))
}

// A job that fits the cap alone but not beside the running jobs waits, even
// when it became eligible before a smaller job that fits: the smaller one goes
// first, as in Slurm, where a job held back by a QOS limit blocks nobody.
func TestQOSGPUCapLetsASmallerJobPassABiggerOneThatDoesNotFit(t *testing.T) {
	b, clock := newQOSBatch(Config{MaxGPUsPerUser: 2})
	a := mustSubmit(t, b, gpuReq("example-a", false)) // 1 GPU, 12:00:00 to 12:30:00
	clock.Set(at(12, 1, 0))
	big := mustSubmit(t, b, gpuReqN("example-big", 2)) // fits alone, not beside A
	clock.Set(at(12, 2, 0))
	small := mustSubmit(t, b, gpuReq("example-small", false)) // fits beside A: 12:02:00 to 12:32:00

	wantRunningFrom(t, b, a, at(12, 0, 0), at(12, 30, 0))
	wantRunningFrom(t, b, small, at(12, 2, 0), at(12, 32, 0))
	wantWaitingOnCap(t, b, big)
	clock.Set(at(12, 31, 59)) // A is gone, the small job still runs: 1 GPU held, 2 needed
	wantWaitingOnCap(t, b, big)
	clock.Set(at(12, 32, 0)) // the small job's hard end: nothing runs
	wantRunningFrom(t, b, big, at(12, 32, 0), at(13, 2, 0))
	j := wantEnded(t, b, small, "TIMEOUT", at(12, 32, 0)) // its start stays
	wantTime(t, "StartTime", j.StartTime, at(12, 2, 0))
}

func TestQOSGPUCapKeepsAStartOnceReached(t *testing.T) {
	b, clock := newQOSBatch(Config{MaxGPUsPerUser: 1})
	a := mustSubmit(t, b, gpuReq("example-a", false)) // 12:00:00 to 12:30:00
	clock.Set(at(12, 1, 0))
	bID := mustSubmit(t, b, gpuReq("example-b", false))
	clock.Set(at(12, 10, 0))
	b.CancelExternally(a)
	wantRunningFrom(t, b, bID, at(12, 10, 0), at(12, 40, 0))

	// Later jobs come and go; B's start never moves.
	clock.Set(at(12, 15, 0))
	c := mustSubmit(t, b, gpuReq("example-c", false))
	wantRunningFrom(t, b, bID, at(12, 10, 0), at(12, 40, 0))
	wantWaitingOnCap(t, b, c)
	clock.Set(at(12, 20, 0))
	b.CancelExternally(c) // cancelled while pending: it holds nothing
	wantRunningFrom(t, b, bID, at(12, 10, 0), at(12, 40, 0))
	clock.Set(at(12, 25, 0))
	d := mustSubmit(t, b, gpuReq("example-d", false))
	b.CancelExternally(a) // A has ended: a no-op
	wantRunningFrom(t, b, bID, at(12, 10, 0), at(12, 40, 0))

	clock.Set(at(12, 39, 59))
	wantWaitingOnCap(t, b, d)
	clock.Set(at(12, 40, 0)) // B's hard end; C never started, so D is next
	wantEnded(t, b, bID, "TIMEOUT", at(12, 40, 0))
	wantRunningFrom(t, b, d, at(12, 40, 0), at(13, 10, 0))
	wantEnded(t, b, c, "CANCELLED", at(12, 20, 0))
}

func TestQOSGPUCapNeverStartsAJobThatCannotFitAlone(t *testing.T) {
	b, clock := newQOSBatch(Config{MaxGPUsPerUser: 1})
	big := mustSubmit(t, b, gpuReqN("example-big", 2)) // accepted: Slurm accepts it too
	wantWaitingOnCap(t, b, big)
	small := mustSubmit(t, b, gpuReq("example-small", false)) // not blocked by it
	wantRunningFrom(t, b, small, at(12, 0, 0), at(12, 30, 0))

	clock.Set(at(22, 0, 0)) // ten hours on: nothing runs, and the big job still pends
	wantEnded(t, b, small, "TIMEOUT", at(12, 30, 0))
	wantWaitingOnCap(t, b, big)
	res := b.Cancel(context.Background(), []string{big})
	if len(res) != 1 || res[0].Outcome != provider.Cancelled {
		t.Fatalf("Cancel = %+v, want Cancelled", res)
	}
	j := wantEnded(t, b, big, "CANCELLED", at(22, 0, 0))
	wantNilTime(t, "StartTime", j.StartTime)
}

func TestQOSGPUCapReasonIsPriorityUntilTheNominalStart(t *testing.T) {
	b, clock := newQOSBatch(Config{MaxGPUsPerUser: 1})
	a := mustSubmit(t, b, gpuReq("example-a", false)) // to 12:30:00
	clock.Set(at(12, 1, 0))
	b.Enqueue(Script{QueueWait: 5 * time.Minute, StartEstimate: 10 * time.Minute})
	bID := mustSubmit(t, b, gpuReq("example-b", false)) // nominal start 12:06:00, estimate 12:11:00

	clock.Set(at(12, 3, 0))
	j := wantPhase(t, b, bID, v1alpha1.AllocationPending)
	if j.Reason != "Priority" {
		t.Fatalf("at 12:03:00 reason = %q, want Priority: the queue wait is not over", j.Reason)
	}
	wantTime(t, "StartEstimate", j.StartEstimate, at(12, 11, 0))

	clock.Set(at(12, 6, 0)) // the queue wait is over and A still runs
	wantWaitingOnCap(t, b, bID)

	clock.Set(at(12, 30, 0))
	wantEnded(t, b, a, "TIMEOUT", at(12, 30, 0))
	wantRunningFrom(t, b, bID, at(12, 30, 0), at(13, 0, 0))
}

func TestQOSGPUCapStartsAtTheNominalStartWhenTheGPUFreedEarlier(t *testing.T) {
	b, clock := newQOSBatch(Config{MaxGPUsPerUser: 1})
	a := mustSubmit(t, b, gpuReq("example-a", false))
	clock.Set(at(12, 1, 0))
	b.Enqueue(Script{QueueWait: 5 * time.Minute})
	bID := mustSubmit(t, b, gpuReq("example-b", false)) // nominal start 12:06:00

	clock.Set(at(12, 4, 0))
	b.CancelExternally(a) // the GPU is free from 12:04:00, B is not eligible to start yet
	j := wantPhase(t, b, bID, v1alpha1.AllocationPending)
	if j.Reason != "Priority" {
		t.Fatalf("at 12:04:00 reason = %q, want Priority", j.Reason)
	}
	clock.Set(at(12, 5, 59))
	wantPhase(t, b, bID, v1alpha1.AllocationPending)
	clock.Set(at(12, 6, 0))
	wantRunningFrom(t, b, bID, at(12, 6, 0), at(12, 36, 0))
}

// A job cancelled while it waits holds no GPU: when the running job ends, the
// next waiting job starts at once, as if the cancelled one never existed.
func TestQOSGPUCapHoldsNothingForAJobCancelledBeforeItsStart(t *testing.T) {
	b, clock := newQOSBatch(Config{MaxGPUsPerUser: 1})
	a := mustSubmit(t, b, gpuReq("example-a", false)) // to 12:30:00
	clock.Set(at(12, 1, 0))
	bID := mustSubmit(t, b, gpuReq("example-b", false))
	clock.Set(at(12, 2, 0))
	c := mustSubmit(t, b, gpuReq("example-c", false))

	clock.Set(at(12, 5, 0))
	b.CancelExternally(bID)
	j := wantEnded(t, b, bID, "CANCELLED", at(12, 5, 0))
	wantNilTime(t, "StartTime", j.StartTime)
	wantWaitingOnCap(t, b, c)

	clock.Set(at(12, 30, 0))
	wantEnded(t, b, a, "TIMEOUT", at(12, 30, 0))
	wantRunningFrom(t, b, c, at(12, 30, 0), at(13, 0, 0))

	// The cancelled job never ran: accounting has no allocation and 0 elapsed.
	acct, found, err := b.ReadAccounting(context.Background(), bID)
	if err != nil || !found || acct.Accounting == nil {
		t.Fatalf("ReadAccounting = %+v, %v, %v; want the job", acct, found, err)
	}
	if acct.Accounting.Elapsed == nil || *acct.Accounting.Elapsed != 0 || acct.Accounting.AllocatedGPUs != nil || acct.StartTime != nil {
		t.Fatalf("accounting of the cancelled job = %+v, start %v; want 0 elapsed, no allocation, no start", acct.Accounting, acct.StartTime)
	}
}

// A job cancelled at the very moment it starts keeps that start, as without a
// cap, and ran for no time: it holds nothing, so the next job starts then too.
func TestQOSGPUCapJobCancelledAtItsStartHoldsNothing(t *testing.T) {
	b, clock := newQOSBatch(Config{MaxGPUsPerUser: 1})
	a := mustSubmit(t, b, gpuReq("example-a", false)) // to 12:30:00
	clock.Set(at(12, 1, 0))
	bID := mustSubmit(t, b, gpuReq("example-b", false))
	clock.Set(at(12, 2, 0))
	c := mustSubmit(t, b, gpuReq("example-c", false))

	clock.Set(at(12, 30, 0))
	wantEnded(t, b, a, "TIMEOUT", at(12, 30, 0))
	wantRunningFrom(t, b, bID, at(12, 30, 0), at(13, 0, 0))
	wantWaitingOnCap(t, b, c)
	b.CancelExternally(bID)
	j := wantEnded(t, b, bID, "CANCELLED", at(12, 30, 0))
	wantTime(t, "StartTime", j.StartTime, at(12, 30, 0))
	wantRunningFrom(t, b, c, at(12, 30, 0), at(13, 0, 0))
}

// Jobs go in order of nominal start, not of job ID.
func TestQOSGPUCapOrdersByNominalStartNotByJobID(t *testing.T) {
	b, clock := newQOSBatch(Config{MaxGPUsPerUser: 1})
	outside := b.AddOutsideJob("example-other-job", gpuPart, 1, time.Hour) // 998, to 13:00:00
	b.Enqueue(Script{QueueWait: 20 * time.Minute})
	late := mustSubmit(t, b, gpuReq("example-late", false)) // 999, nominal start 12:20:00
	clock.Set(at(12, 1, 0))
	early := mustSubmit(t, b, gpuReq("example-early", false)) // 1000, nominal start 12:01:00

	clock.Set(at(12, 30, 0))
	wantWaitingOnCap(t, b, late)
	wantWaitingOnCap(t, b, early)
	b.CancelExternally(outside)
	wantRunningFrom(t, b, early, at(12, 30, 0), at(13, 0, 0))
	wantWaitingOnCap(t, b, late)
	clock.Set(at(13, 0, 0))
	wantRunningFrom(t, b, late, at(13, 0, 0), at(13, 30, 0))
}

func TestQOSGPUCapCountsAnOutsideJob(t *testing.T) {
	b, clock := newQOSBatch(Config{MaxGPUsPerUser: 1})
	b.AddOutsideJob("example-other-job", gpuPart, 1, time.Hour) // to 13:00:00
	bID := mustSubmit(t, b, gpuReq("example-b", false))
	wantWaitingOnCap(t, b, bID)
	clock.Set(at(12, 59, 59))
	wantWaitingOnCap(t, b, bID)
	clock.Set(at(13, 0, 0))
	wantRunningFrom(t, b, bID, at(13, 0, 0), at(13, 30, 0))
}

func TestQOSGPUCapFreesWhenAJobFailsEarly(t *testing.T) {
	b, clock := newQOSBatch(Config{MaxGPUsPerUser: 1})
	instant := time.Duration(0)
	b.Enqueue(Script{FailAfter: &instant})
	failed := mustSubmit(t, b, gpuReq("example-failed", false)) // fails at its start: runs for no time
	bID := mustSubmit(t, b, gpuReq("example-b", false))
	wantFinished(t, b, failed, v1alpha1.AllocationFailed, "FAILED", at(12, 0, 0))
	wantRunningFrom(t, b, bID, at(12, 0, 0), at(12, 30, 0))

	after := 5 * time.Minute
	clock.Set(at(12, 1, 0))
	b.Enqueue(Script{FailAfter: &after, FailState: "NODE_FAIL"})
	// B holds the GPU to 12:30:00; the next job waits for it, not for its own turn.
	c := mustSubmit(t, b, gpuReq("example-c", false))
	d := mustSubmit(t, b, gpuReq("example-d", false))
	wantWaitingOnCap(t, b, c)
	wantWaitingOnCap(t, b, d)
	clock.Set(at(12, 30, 0)) // B ends; C starts, fails after 5 minutes
	wantRunningFrom(t, b, c, at(12, 30, 0), at(13, 0, 0))
	wantWaitingOnCap(t, b, d)
	clock.Set(at(12, 35, 0)) // C fails at 12:35:00 and D takes its GPU
	wantFinished(t, b, c, v1alpha1.AllocationFailed, "NODE_FAIL", at(12, 35, 0))
	wantRunningFrom(t, b, d, at(12, 35, 0), at(13, 5, 0))
}

// Every read shows the start the cap gives: Job, FindByTag, Observe and
// ReadAccounting agree, while the job waits and after it starts.
func TestQOSGPUCapEveryReadSeesTheSameStart(t *testing.T) {
	ctx := context.Background()
	b, clock := newQOSBatch(Config{MaxGPUsPerUser: 1})
	a := mustSubmit(t, b, gpuReq("example-a", false))
	clock.Set(at(12, 1, 0))
	bID := mustSubmit(t, b, gpuReq("example-b", false))

	reads := func(when string, check func(string, provider.Job)) {
		t.Helper()
		j, _ := b.Job(bID)
		check("Job", j)
		jobs, err := b.FindByTag(ctx, "example-b", at(0, 0, 0))
		if err != nil || len(jobs) != 1 {
			t.Fatalf("%s: FindByTag = %v, %v; want one job", when, jobs, err)
		}
		check("FindByTag", jobs[0])
		obs, found, err := b.Observe(ctx, bID)
		if err != nil || !found {
			t.Fatalf("%s: Observe = %v, %v", when, found, err)
		}
		check("Observe", obs)
		acct, found, err := b.ReadAccounting(ctx, bID)
		if err != nil || !found {
			t.Fatalf("%s: ReadAccounting = %v, %v", when, found, err)
		}
		check("ReadAccounting", acct)
	}
	waiting := func(read string, j provider.Job) {
		t.Helper()
		if j.Phase != v1alpha1.AllocationPending || j.Reason != "QOSMaxGRESPerUser" || j.StartTime != nil || j.HardEnd != nil {
			t.Fatalf("at 12:05:00 %s = phase %s reason %q start %v, want Pending on QOSMaxGRESPerUser with no start", read, j.Phase, j.Reason, j.StartTime)
		}
		if j.Accounting != nil && (j.Accounting.Elapsed == nil || *j.Accounting.Elapsed != 0) {
			t.Fatalf("%s: elapsed of a waiting job = %v, want 0", read, j.Accounting.Elapsed)
		}
	}
	clock.Set(at(12, 5, 0))
	reads("12:05:00", waiting)

	clock.Set(at(12, 10, 0))
	b.CancelExternally(a)
	clock.Set(at(12, 20, 0))
	reads("12:20:00", func(read string, j provider.Job) {
		t.Helper()
		if j.Phase != v1alpha1.AllocationRunning {
			t.Fatalf("at 12:20:00 %s phase = %s, want Running", read, j.Phase)
		}
		wantTime(t, read+" StartTime", j.StartTime, at(12, 10, 0))
		wantTime(t, read+" HardEnd", j.HardEnd, at(12, 40, 0))
		if j.Accounting != nil && (j.Accounting.Elapsed == nil || *j.Accounting.Elapsed != 10*time.Minute) {
			t.Fatalf("%s: elapsed = %v, want 10m (12:10:00 to 12:20:00)", read, j.Accounting.Elapsed)
		}
	})
}

// A held job waits on the hold, not the cap; once lifted it waits on the cap.
func TestQOSGPUCapHeldJobWaitsOnTheCapOnlyAfterTheLift(t *testing.T) {
	b, clock := newQOSBatch(Config{MaxGPUsPerUser: 1})
	a := mustSubmit(t, b, gpuReq("example-a", false)) // to 12:30:00
	h := mustSubmit(t, b, gpuReq("example-h", true))
	j := wantPhase(t, b, h, v1alpha1.AllocationHeld)
	if j.Reason != "JobHeldUser" {
		t.Fatalf("held job reason = %q, want JobHeldUser, not the cap's", j.Reason)
	}

	clock.Set(at(12, 10, 0))
	if err := b.LiftHold(context.Background(), h); err != nil {
		t.Fatal(err)
	}
	wantWaitingOnCap(t, b, h)
	clock.Set(at(12, 30, 0))
	wantEnded(t, b, a, "TIMEOUT", at(12, 30, 0))
	wantRunningFrom(t, b, h, at(12, 30, 0), at(13, 0, 0))
}

// At a tie of nominal starts the job that became eligible first goes first, so
// a job lifted at the very moment another starts cannot take its start: that
// start was reached already. By job ID alone the lifted job (the lower ID) would
// win and move the other job's start.
func TestQOSGPUCapALiftAtTheMomentOfAStartDoesNotMoveIt(t *testing.T) {
	b, clock := newQOSBatch(Config{MaxGPUsPerUser: 1})
	outside := b.AddOutsideJob("example-other-job", gpuPart, 1, time.Hour) // 998
	h := mustSubmit(t, b, gpuReq("example-h", true))                       // 999, held

	clock.Set(at(12, 9, 0))
	b.CancelExternally(outside)
	clock.Set(at(12, 10, 0))
	k := mustSubmit(t, b, gpuReq("example-k", false)) // 1000, nominal start 12:10:00, fits
	wantRunningFrom(t, b, k, at(12, 10, 0), at(12, 40, 0))
	if err := b.LiftHold(context.Background(), h); err != nil { // nominal start 12:10:00 too
		t.Fatal(err)
	}
	wantRunningFrom(t, b, k, at(12, 10, 0), at(12, 40, 0))
	wantWaitingOnCap(t, b, h)
	clock.Set(at(12, 40, 0))
	wantRunningFrom(t, b, h, at(12, 40, 0), at(13, 10, 0))
}

// A requeue clears the job's start but not the GPU it held until then: another
// job cannot start in the time the requeued job ran.
func TestQOSGPUCapCountsTheEarlierRunOfARequeuedJob(t *testing.T) {
	b, clock := newQOSBatch(Config{MaxGPUsPerUser: 1})
	a := mustSubmit(t, b, gpuReq("example-a", false)) // runs from 12:00:00
	clock.Set(at(12, 1, 0))
	bID := mustSubmit(t, b, gpuReq("example-b", false)) // waits on A

	clock.Set(at(12, 10, 0))
	b.Requeue(a) // A's first run is 12:00:00 to 12:10:00; it is eligible again from 12:10:00
	// B is ahead of the requeued A (nominal start 12:01:00 against 12:10:00) and
	// starts when A's run ends, never in the time A ran.
	wantRunningFrom(t, b, bID, at(12, 10, 0), at(12, 40, 0))
	ja := wantPhase(t, b, a, v1alpha1.AllocationFailed) // a requeued job is Failed
	if ja.Reason != "QOSMaxGRESPerUser" || !ja.Requeued || ja.StartTime != nil {
		t.Fatalf("requeued job = reason %q requeued %v start %v, want waiting on QOSMaxGRESPerUser, requeued, no start", ja.Reason, ja.Requeued, ja.StartTime)
	}
	clock.Set(at(12, 20, 0))
	wantRunningFrom(t, b, bID, at(12, 10, 0), at(12, 40, 0))
	clock.Set(at(12, 40, 0))
	// A requeued job is Failed in any state (HBEP-4, Phases): check its state.
	ja = mustJob(t, b, a)
	if len(ja.State) != 1 || ja.State[0] != "RUNNING" {
		t.Fatalf("requeued job state = %v, want RUNNING", ja.State)
	}
	wantTime(t, "StartTime", ja.StartTime, at(12, 40, 0))
	wantTime(t, "HardEnd", ja.HardEnd, at(13, 10, 0))
}

func TestQOSNoGPUCapIsToday(t *testing.T) {
	b, clock := newQOSBatch(Config{})
	ids := []string{
		mustSubmit(t, b, gpuReq("example-a", false)),
		mustSubmit(t, b, gpuReq("example-b", false)),
		mustSubmit(t, b, gpuReq("example-c", false)),
	}
	for _, id := range ids {
		wantRunningFrom(t, b, id, at(12, 0, 0), at(12, 30, 0))
	}
	b.Enqueue(Script{QueueWait: 5 * time.Minute})
	late := mustSubmit(t, b, gpuReq("example-late", false))
	if j := wantPhase(t, b, late, v1alpha1.AllocationPending); j.Reason != "Priority" {
		t.Fatalf("reason = %q, want Priority", j.Reason)
	}
	clock.Set(at(12, 5, 0))
	wantRunningFrom(t, b, late, at(12, 5, 0), at(12, 35, 0))
}

func TestQOSNegativeGPUCapPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("no panic for a negative MaxGPUsPerUser")
		}
	}()
	newQOSBatch(Config{MaxGPUsPerUser: -1})
}

// Through the stand-in Provider controller: an Allocation whose job waits on
// the cap is Pending with the cap's reason, and Running once the outside job
// ends, with its queue wait measured to the start the cap gave it.
func TestQOSGPUCapThroughTheController(t *testing.T) {
	e := newEnv(t)
	e.batch = NewBatch(e.clock, Config{Partitions: qosPartitions(), MaxGPUsPerUser: 1})
	e.ctl.Batch = e.batch
	outside := e.batch.AddOutsideJob("example-other-job", gpuPart, 1, time.Hour)
	e.newAlloc("a1", testProvider)

	e.sync() // 12:00:00
	o := e.wantPhase("a1", v1alpha1.AllocationPending)
	if o.Reason != "QOSMaxGRESPerUser" {
		t.Fatalf("reason = %q, want QOSMaxGRESPerUser", o.Reason)
	}
	wantNilMT(t, "startTime", o.StartTime)
	e.syncAt(at(12, 5, 0))
	e.wantPhase("a1", v1alpha1.AllocationPending)

	e.clock.Set(at(12, 10, 0))
	e.batch.CancelExternally(outside)
	e.sync()
	o = e.wantPhase("a1", v1alpha1.AllocationRunning)
	wantMT(t, "startTime", o.StartTime, at(12, 10, 0))
	wantMT(t, "hardEnd", o.HardEnd, at(12, 40, 0))
	waits := e.provider().Status.QueueWaits
	if len(waits) != 1 || waits[0].JobID != "999" {
		t.Fatalf("queue waits = %+v, want one, for job 999", waits)
	}
	wantMT(t, "queue wait submitTime", &waits[0].SubmitTime, at(12, 0, 0))
	wantMT(t, "queue wait startTime", &waits[0].StartTime, at(12, 10, 0))
}
