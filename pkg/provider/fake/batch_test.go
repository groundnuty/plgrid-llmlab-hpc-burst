package fake

import (
	"context"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// The expected times in these tests are literals worked out from the script:
// a job submitted at 12:00:00 with QueueWait 5m starts at 12:05:00, and with a
// 30-minute limit its hard end is 12:35:00. They never come from the fake's own
// helpers.

func at(h, m, s int) time.Time { return time.Date(2026, 10, 30, h, m, s, 0, time.UTC) }

const (
	gpuPart = "example-gpu"
	cpuPart = "example-cpu"
)

func newTestBatch(t *testing.T) (*Batch, *Clock) {
	t.Helper()
	clock := NewClock(at(12, 0, 0))
	b := NewBatch(clock, Config{Partitions: []Partition{
		{Name: gpuPart, GPUModel: "example-gpu-model", GPUsPerNode: 4, MaxTime: time.Hour},
		{Name: cpuPart, GPUModel: "none", GPUsPerNode: 0, MaxTime: 2 * time.Hour},
	}})
	return b, clock
}

func gpuReq(tag string, hold bool) provider.SubmitRequest {
	return provider.SubmitRequest{
		Tag:     tag,
		Hold:    hold,
		Account: "example-account",
		Params:  v1alpha1.BatchParameters{Partition: gpuPart, TimeLimit: "30m", GPUs: 1},
	}
}

func mustSubmit(t *testing.T, b *Batch, req provider.SubmitRequest) string {
	t.Helper()
	id, outcome, err := b.Submit(context.Background(), req)
	if err != nil || outcome != provider.Accepted || id == "" {
		t.Fatalf("Submit = (%q, %s, %v), want an accepted job", id, outcome, err)
	}
	return id
}

func mustJob(t *testing.T, b *Batch, id string) provider.Job {
	t.Helper()
	j, ok := b.Job(id)
	if !ok {
		t.Fatalf("job %s is unknown", id)
	}
	return j
}

func wantPhase(t *testing.T, b *Batch, id string, want v1alpha1.AllocationPhase) provider.Job {
	t.Helper()
	j := mustJob(t, b, id)
	if j.Phase != want {
		t.Fatalf("at %s job %s phase = %s (state %v), want %s", b.clock.Now().Format("15:04:05"), id, j.Phase, j.State, want)
	}
	return j
}

func wantTime(t *testing.T, name string, got *time.Time, want time.Time) {
	t.Helper()
	if got == nil || !got.Equal(want) || got.Location() != time.UTC {
		t.Fatalf("%s = %v, want %v (UTC)", name, got, want)
	}
}

func wantNilTime(t *testing.T, name string, got *time.Time) {
	t.Helper()
	if got != nil {
		t.Fatalf("%s = %v, want nil", name, *got)
	}
}

func TestClock(t *testing.T) {
	c := NewClock(at(12, 0, 0))
	c.Advance(90 * time.Second)
	if got := c.Now(); !got.Equal(at(12, 1, 30)) {
		t.Fatalf("after Advance, Now = %v, want 12:01:30", got)
	}
	c.Set(at(9, 0, 0))
	if got := c.Now(); !got.Equal(at(9, 0, 0)) || got.Location() != time.UTC {
		t.Fatalf("after Set, Now = %v, want 09:00:00 UTC", got)
	}
}

func TestJobIDsAreDecimalFrom998(t *testing.T) {
	b, _ := newTestBatch(t)
	for _, want := range []string{"998", "999", "1000", "1001"} {
		if got := mustSubmit(t, b, gpuReq("example-tag", false)); got != want {
			t.Fatalf("job ID = %s, want %s", got, want)
		}
	}
}

func TestHeldThenLifted(t *testing.T) {
	b, clock := newTestBatch(t)
	b.Enqueue(Script{QueueWait: 5 * time.Minute})
	id := mustSubmit(t, b, gpuReq("example-tag", true))

	j := wantPhase(t, b, id, v1alpha1.AllocationHeld)
	if j.Hold != provider.HoldUser || j.Reason != "JobHeldUser" || !j.Held() {
		t.Fatalf("held job = hold %q reason %q, want a user hold with reason JobHeldUser", j.Hold, j.Reason)
	}
	wantTime(t, "SubmitTime", &j.SubmitTime, at(12, 0, 0))

	// A held job never starts, however long it waits.
	clock.Advance(10 * time.Minute) // 12:10:00
	wantPhase(t, b, id, v1alpha1.AllocationHeld)

	if err := b.LiftHold(context.Background(), id); err != nil {
		t.Fatalf("LiftHold: %v", err)
	}
	// Eligible from the lift (12:10:00), so the start is 12:15:00.
	j = wantPhase(t, b, id, v1alpha1.AllocationPending)
	if j.Held() || j.Reason != "Priority" {
		t.Fatalf("lifted job = hold %q reason %q, want unheld and Priority", j.Hold, j.Reason)
	}
	clock.Set(at(12, 14, 59))
	wantPhase(t, b, id, v1alpha1.AllocationPending)
	clock.Set(at(12, 15, 0))
	j = wantPhase(t, b, id, v1alpha1.AllocationRunning)
	wantTime(t, "StartTime", j.StartTime, at(12, 15, 0))
	wantTime(t, "HardEnd", j.HardEnd, at(12, 45, 0))
	if n := b.Count("scontrol-release"); n != 1 {
		t.Fatalf("scontrol-release logged %d times, want 1", n)
	}
}

func TestQueueWaitThenRunningWithHardEnd(t *testing.T) {
	b, clock := newTestBatch(t)
	b.Enqueue(Script{QueueWait: 5 * time.Minute, StartEstimate: 3 * time.Minute})
	id := mustSubmit(t, b, gpuReq("example-tag", false))

	j := wantPhase(t, b, id, v1alpha1.AllocationPending)
	wantTime(t, "StartEstimate", j.StartEstimate, at(12, 3, 0))
	wantNilTime(t, "StartTime", j.StartTime)
	wantNilTime(t, "HardEnd", j.HardEnd)
	wantNilTime(t, "EndTime", j.EndTime)
	if j.Name != "example-tag" || j.Partition != gpuPart || j.TimeLimit != 30*time.Minute || j.GPUs != 1 {
		t.Fatalf("read-back fields = %q %q %v %d", j.Name, j.Partition, j.TimeLimit, j.GPUs)
	}
	if j.Source != provider.SourceQueue || j.DataParser != "v0.0.42" || len(j.State) != 1 || j.State[0] != "PENDING" {
		t.Fatalf("source %q parser %q state %v", j.Source, j.DataParser, j.State)
	}

	clock.Set(at(12, 4, 59))
	wantPhase(t, b, id, v1alpha1.AllocationPending)
	clock.Set(at(12, 5, 0))
	j = wantPhase(t, b, id, v1alpha1.AllocationRunning)
	wantTime(t, "StartTime", j.StartTime, at(12, 5, 0))
	wantTime(t, "HardEnd", j.HardEnd, at(12, 35, 0))
	wantNilTime(t, "StartEstimate", j.StartEstimate) // pending only
	wantNilTime(t, "EndTime", j.EndTime)
	if j.State[0] != "RUNNING" {
		t.Fatalf("state = %v, want RUNNING", j.State)
	}
}

func TestDefaultScriptRunsAtOnce(t *testing.T) {
	b, _ := newTestBatch(t)
	id := mustSubmit(t, b, gpuReq("example-tag", false))
	j := wantPhase(t, b, id, v1alpha1.AllocationRunning)
	wantTime(t, "StartTime", j.StartTime, at(12, 0, 0))
}

func TestHardEndEndsTimeout(t *testing.T) {
	b, clock := newTestBatch(t)
	b.Enqueue(Script{QueueWait: 5 * time.Minute})
	id := mustSubmit(t, b, gpuReq("example-tag", false))
	clock.Set(at(12, 34, 59))
	wantPhase(t, b, id, v1alpha1.AllocationRunning)
	clock.Set(at(12, 35, 0))
	j := wantPhase(t, b, id, v1alpha1.AllocationEnded)
	if j.State[0] != "TIMEOUT" {
		t.Fatalf("state = %v, want TIMEOUT", j.State)
	}
	wantTime(t, "EndTime", j.EndTime, at(12, 35, 0))
	wantTime(t, "StartTime", j.StartTime, at(12, 5, 0))
	wantTime(t, "HardEnd", j.HardEnd, at(12, 35, 0))
}

func TestFailedStartIsBootFail(t *testing.T) {
	b, clock := newTestBatch(t)
	zero := time.Duration(0)
	b.Enqueue(Script{QueueWait: 5 * time.Minute, FailAfter: &zero, FailState: "BOOT_FAIL"})
	id := mustSubmit(t, b, gpuReq("example-tag", false))
	clock.Set(at(12, 4, 59))
	wantPhase(t, b, id, v1alpha1.AllocationPending)
	clock.Set(at(12, 5, 0))
	j := wantPhase(t, b, id, v1alpha1.AllocationFailed)
	if j.State[0] != "BOOT_FAIL" {
		t.Fatalf("state = %v, want BOOT_FAIL", j.State)
	}
	// The job failed as it started: it was never Running.
	wantTime(t, "EndTime", j.EndTime, at(12, 5, 0))
	wantTime(t, "StartTime", j.StartTime, at(12, 5, 0))
}

func TestFailAfter(t *testing.T) {
	for _, tc := range []struct{ name, state, want string }{
		{"default state", "", "FAILED"},
		{"node fail", "NODE_FAIL", "NODE_FAIL"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, clock := newTestBatch(t)
			after := 10 * time.Minute
			b.Enqueue(Script{QueueWait: 5 * time.Minute, FailAfter: &after, FailState: tc.state})
			id := mustSubmit(t, b, gpuReq("example-tag", false))
			clock.Set(at(12, 14, 59)) // started 12:05:00, fails at 12:15:00
			wantPhase(t, b, id, v1alpha1.AllocationRunning)
			clock.Set(at(12, 15, 0))
			j := wantPhase(t, b, id, v1alpha1.AllocationFailed)
			if j.State[0] != tc.want {
				t.Fatalf("state = %v, want %s", j.State, tc.want)
			}
			wantTime(t, "EndTime", j.EndTime, at(12, 15, 0))
		})
	}
}

func TestFailAfterBeyondTheLimitStillTimesOut(t *testing.T) {
	b, clock := newTestBatch(t)
	after := 45 * time.Minute // the limit is 30m
	b.Enqueue(Script{FailAfter: &after})
	id := mustSubmit(t, b, gpuReq("example-tag", false))
	clock.Set(at(12, 30, 0))
	j := wantPhase(t, b, id, v1alpha1.AllocationEnded)
	if j.State[0] != "TIMEOUT" {
		t.Fatalf("state = %v, want TIMEOUT", j.State)
	}
}

func TestExternalCancel(t *testing.T) {
	b, clock := newTestBatch(t)
	b.Enqueue(Script{QueueWait: 5 * time.Minute})
	id := mustSubmit(t, b, gpuReq("example-tag", false))
	clock.Set(at(12, 10, 0))
	wantPhase(t, b, id, v1alpha1.AllocationRunning)
	b.CancelExternally(id)
	clock.Set(at(12, 20, 0))

	j := wantPhase(t, b, id, v1alpha1.AllocationEnded)
	if j.State[0] != "CANCELLED" {
		t.Fatalf("state = %v, want CANCELLED", j.State)
	}
	wantTime(t, "EndTime", j.EndTime, at(12, 10, 0))
	wantTime(t, "StartTime", j.StartTime, at(12, 5, 0))
	if n := b.Count("external-cancel"); n != 1 {
		t.Fatalf("external-cancel logged %d times, want 1", n)
	}
	if n := b.Count("scancel"); n != 0 {
		t.Fatalf("an external cancel logged %d scancel calls, want 0: the framework did not cancel", n)
	}
}

func TestExternalCancelOfPendingJobNeverStarts(t *testing.T) {
	b, clock := newTestBatch(t)
	b.Enqueue(Script{QueueWait: 5 * time.Minute})
	id := mustSubmit(t, b, gpuReq("example-tag", false))
	clock.Set(at(12, 2, 0))
	b.CancelExternally(id)
	clock.Set(at(12, 30, 0))
	j := wantPhase(t, b, id, v1alpha1.AllocationEnded)
	wantTime(t, "EndTime", j.EndTime, at(12, 2, 0))
	wantNilTime(t, "StartTime", j.StartTime)
	wantNilTime(t, "HardEnd", j.HardEnd)
}

func TestExternalCancelOfEndedJobChangesNothing(t *testing.T) {
	b, clock := newTestBatch(t)
	id := mustSubmit(t, b, gpuReq("example-tag", false))
	clock.Set(at(12, 40, 0)) // timed out at 12:30:00
	b.CancelExternally(id)
	j := mustJob(t, b, id)
	if j.State[0] != "TIMEOUT" {
		t.Fatalf("state = %v, want TIMEOUT kept", j.State)
	}
	wantTime(t, "EndTime", j.EndTime, at(12, 30, 0))
}

func TestRequeueIsFailed(t *testing.T) {
	b, clock := newTestBatch(t)
	b.Enqueue(Script{QueueWait: 5 * time.Minute})
	id := mustSubmit(t, b, gpuReq("example-tag", false))
	clock.Set(at(12, 10, 0))
	j := wantPhase(t, b, id, v1alpha1.AllocationRunning)
	if j.Requeued || j.RestartCount != 0 {
		t.Fatalf("before the requeue: requeued %v restarts %d", j.Requeued, j.RestartCount)
	}
	b.Requeue(id)
	j = wantPhase(t, b, id, v1alpha1.AllocationFailed)
	if !j.Requeued || j.RestartCount != 1 {
		t.Fatalf("requeued %v restarts %d, want true and 1", j.Requeued, j.RestartCount)
	}
}
