package fake

import (
	"context"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// Tests of Batch.Requeue and Batch.RequeueHold. A job submitted at 12:00:00
// with QueueWait 5m starts at 12:05:00; with the 30-minute limit of gpuReq its
// hard end is 12:35:00. A job requeued at 12:10:00 is eligible again from then
// and starts at 12:15:00, with its hard end at 12:45:00. All times are
// literals worked out from that.

func wantPanics(t *testing.T, what string, f func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatalf("%s did not panic", what)
		}
	}()
	f()
}

func TestRequeueReturnsARunningJobToPendingThenItStartsAgain(t *testing.T) {
	b, clock := newTestBatch(t)
	b.Enqueue(Script{QueueWait: 5 * time.Minute})
	id := mustSubmit(t, b, gpuReq("example-tag", false))
	clock.Set(at(12, 10, 0))
	j := wantPhase(t, b, id, v1alpha1.AllocationRunning)
	wantTime(t, "StartTime before the requeue", j.StartTime, at(12, 5, 0))

	b.Requeue(id)
	j = wantPhase(t, b, id, v1alpha1.AllocationFailed) // a requeued job is Failed by phase
	if j.State[0] != "PENDING" || j.Reason != "Priority" {
		t.Fatalf("requeued job = state %v reason %q, want PENDING and Priority", j.State, j.Reason)
	}
	if !j.Requeued || j.RestartCount != 1 || j.Held() || j.Hold != provider.HoldNone {
		t.Fatalf("requeued job = requeued %v restarts %d hold %q, want true, 1 and no hold", j.Requeued, j.RestartCount, j.Hold)
	}
	wantNilTime(t, "StartTime", j.StartTime)
	wantNilTime(t, "HardEnd", j.HardEnd)
	wantNilTime(t, "EndTime", j.EndTime)

	clock.Set(at(12, 14, 59)) // eligible from 12:10:00, and the queue wait is 5m
	j = wantPhase(t, b, id, v1alpha1.AllocationFailed)
	if j.State[0] != "PENDING" {
		t.Fatalf("state at 12:14:59 = %v, want PENDING", j.State)
	}
	wantNilTime(t, "StartTime at 12:14:59", j.StartTime)
	clock.Set(at(12, 15, 0))
	j = wantPhase(t, b, id, v1alpha1.AllocationFailed) // still Failed by phase while it runs again
	if j.State[0] != "RUNNING" || !j.Requeued || j.RestartCount != 1 {
		t.Fatalf("job at 12:15:00 = state %v requeued %v restarts %d, want RUNNING, true, 1", j.State, j.Requeued, j.RestartCount)
	}
	wantTime(t, "StartTime", j.StartTime, at(12, 15, 0))
	wantTime(t, "HardEnd", j.HardEnd, at(12, 45, 0))

	if n := b.Count("requeue"); n != 1 {
		t.Fatalf("requeue logged %d times, want 1", n)
	}
	if n := b.Count("requeuehold"); n != 0 {
		t.Fatalf("requeuehold logged %d times, want 0", n)
	}
	for _, c := range b.Calls() {
		if c.Op == "requeue" && (len(c.Args) != 1 || c.Args[0] != id) {
			t.Fatalf("requeue call = %+v, want the job ID %s", c, id)
		}
	}

	clock.Set(at(12, 20, 0))
	b.Requeue(id)
	if j = mustJob(t, b, id); j.RestartCount != 2 {
		t.Fatalf("restarts after the second requeue = %d, want 2", j.RestartCount)
	}
}

func TestRequeueHoldHoldsTheJobByItsUserUntilItIsLifted(t *testing.T) {
	b, clock := newTestBatch(t)
	b.Enqueue(Script{QueueWait: 5 * time.Minute})
	id := mustSubmit(t, b, gpuReq("example-tag", false))
	clock.Set(at(12, 10, 0))
	wantPhase(t, b, id, v1alpha1.AllocationRunning)

	b.RequeueHold(id)
	j := wantPhase(t, b, id, v1alpha1.AllocationFailed)
	if j.State[0] != "PENDING" || j.Reason != provider.ReasonHeldUser || j.Hold != provider.HoldUser || !j.Held() {
		t.Fatalf("job = state %v reason %q hold %q, want PENDING, JobHeldUser, a user hold", j.State, j.Reason, j.Hold)
	}
	if !j.Requeued || j.RestartCount != 1 {
		t.Fatalf("job = requeued %v restarts %d, want true and 1", j.Requeued, j.RestartCount)
	}
	wantNilTime(t, "StartTime", j.StartTime)
	wantNilTime(t, "HardEnd", j.HardEnd)

	clock.Set(at(12, 40, 0)) // a held job never starts, however long it waits
	j = mustJob(t, b, id)
	if j.State[0] != "PENDING" || j.Hold != provider.HoldUser {
		t.Fatalf("job at 12:40:00 = state %v hold %q, want it still held", j.State, j.Hold)
	}
	wantNilTime(t, "StartTime at 12:40:00", j.StartTime)

	if err := b.LiftHold(context.Background(), id); err != nil {
		t.Fatalf("LiftHold: %v", err)
	}
	clock.Set(at(12, 44, 59)) // eligible from the lift at 12:40:00, plus the 5m wait
	if j = mustJob(t, b, id); j.State[0] != "PENDING" {
		t.Fatalf("state at 12:44:59 = %v, want PENDING", j.State)
	}
	clock.Set(at(12, 45, 0))
	j = mustJob(t, b, id)
	if j.State[0] != "RUNNING" {
		t.Fatalf("state at 12:45:00 = %v, want RUNNING", j.State)
	}
	wantTime(t, "StartTime", j.StartTime, at(12, 45, 0))
	wantTime(t, "HardEnd", j.HardEnd, at(13, 15, 0))

	if b.Count("requeuehold") != 1 || b.Count("requeue") != 0 {
		t.Fatalf("requeuehold logged %d times and requeue %d, want 1 and 0", b.Count("requeuehold"), b.Count("requeue"))
	}
}

func TestRequeueKeepsAHoldAndRequeueHoldDoesNotOverrideAnAdminHold(t *testing.T) {
	b, _ := newTestBatch(t)
	b.Enqueue(Script{}, Script{AdminHold: true})
	userHeld := mustSubmit(t, b, gpuReq("example-tag", true))
	adminHeld := mustSubmit(t, b, gpuReq("example-tag", false))

	b.Requeue(userHeld) // a requeue does not lift a hold
	if j := mustJob(t, b, userHeld); j.Hold != provider.HoldUser || j.RestartCount != 1 {
		t.Fatalf("requeued held job = hold %q restarts %d, want a user hold and 1", j.Hold, j.RestartCount)
	}
	b.RequeueHold(adminHeld)
	if j := mustJob(t, b, adminHeld); j.Hold != provider.HoldAdmin || j.Reason != provider.ReasonHeldAdmin {
		t.Fatalf("job = hold %q reason %q, want the administrator's hold kept", j.Hold, j.Reason)
	}
}

func TestRequeueOfAnEndedJobPanics(t *testing.T) {
	b, clock := newTestBatch(t)
	b.Enqueue(Script{QueueWait: 5 * time.Minute}, Script{QueueWait: 5 * time.Minute})
	timedOut := mustSubmit(t, b, gpuReq("example-tag", false)) // ends 12:35:00
	cancelled := mustSubmit(t, b, gpuReq("example-tag", false))
	b.Cancel(context.Background(), []string{cancelled})
	clock.Set(at(12, 40, 0))
	for name, id := range map[string]string{"timed out": timedOut, "cancelled": cancelled} {
		if j := mustJob(t, b, id); j.EndTime == nil {
			t.Fatalf("setup: the %s job has not ended", name)
		}
		wantPanics(t, "Requeue of the "+name+" job", func() { b.Requeue(id) })
		wantPanics(t, "RequeueHold of the "+name+" job", func() { b.RequeueHold(id) })
	}
	if n := b.Count("requeue") + b.Count("requeuehold"); n != 0 {
		t.Fatalf("%d requeue calls logged for ended jobs, want 0", n)
	}
}
