package fake

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// Tests of the Batch controls added for the lifecycle controller's scenarios:
// SetTimeLimit and FailNextLiftHold, the result in the lift log, and cancelling
// a held job. Expected times are worked out by hand: a job submitted at 12:00:00
// with QueueWait 5m starts at 12:05:00.

func TestSetTimeLimitMovesTheHardEndOfARunningJob(t *testing.T) {
	b, clock := newTestBatch(t)
	b.Enqueue(Script{QueueWait: 5 * time.Minute})
	id := mustSubmit(t, b, gpuReq("example-tag", false)) // limit 30m: hard end 12:35:00
	clock.Set(at(12, 10, 0))
	j := wantPhase(t, b, id, v1alpha1.AllocationRunning)
	wantTime(t, "HardEnd", j.HardEnd, at(12, 35, 0))

	b.SetTimeLimit(id, 45*time.Minute)
	j = wantPhase(t, b, id, v1alpha1.AllocationRunning)
	wantTime(t, "HardEnd", j.HardEnd, at(12, 50, 0)) // start 12:05:00 + 45m
	wantTime(t, "StartTime", j.StartTime, at(12, 5, 0))
	if j.TimeLimit != 45*time.Minute {
		t.Fatalf("TimeLimit = %s, want 45m", j.TimeLimit)
	}
	clock.Set(at(12, 36, 0)) // past the old hard end
	wantPhase(t, b, id, v1alpha1.AllocationRunning)
	clock.Set(at(12, 50, 0))
	j = wantPhase(t, b, id, v1alpha1.AllocationEnded)
	wantTime(t, "EndTime", j.EndTime, at(12, 50, 0))
	if j.State[0] != "TIMEOUT" {
		t.Fatalf("state = %v, want TIMEOUT", j.State)
	}
	if n := b.Count("set-time-limit"); n != 1 {
		t.Fatalf("set-time-limit logged %d times, want 1", n)
	}
}

func TestSetTimeLimitOfAPendingJobAppliesAtItsStart(t *testing.T) {
	b, clock := newTestBatch(t)
	b.Enqueue(Script{QueueWait: 5 * time.Minute})
	id := mustSubmit(t, b, gpuReq("example-tag", false))
	b.SetTimeLimit(id, 10*time.Minute) // lowered while pending
	clock.Set(at(12, 5, 0))
	j := wantPhase(t, b, id, v1alpha1.AllocationRunning)
	wantTime(t, "HardEnd", j.HardEnd, at(12, 15, 0))
}

func TestSetTimeLimitBelowTheTimeRunEndsTheJob(t *testing.T) {
	b, clock := newTestBatch(t)
	b.Enqueue(Script{QueueWait: 5 * time.Minute})
	id := mustSubmit(t, b, gpuReq("example-tag", false))
	clock.Set(at(12, 20, 0)) // running for 15m
	b.SetTimeLimit(id, 10*time.Minute)
	j := wantPhase(t, b, id, v1alpha1.AllocationEnded)
	wantTime(t, "EndTime", j.EndTime, at(12, 15, 0)) // start + the new limit
}

func TestSetTimeLimitRefusesWhatCannotHappen(t *testing.T) {
	b, clock := newTestBatch(t)
	b.Enqueue(Script{QueueWait: 5 * time.Minute})
	id := mustSubmit(t, b, gpuReq("example-tag", false))
	clock.Set(at(12, 35, 0)) // the job ended at its hard end
	for name, fn := range map[string]func(){
		"an ended job":    func() { b.SetTimeLimit(id, time.Hour) },
		"an unknown job":  func() { b.SetTimeLimit("4242", time.Hour) },
		"a zero limit":    func() { b.SetTimeLimit(mustSubmit(t, b, gpuReq("example-other", false)), 0) },
		"a nil lift fail": func() { b.FailNextLiftHold(nil) },
	} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("no panic")
				}
			}()
			fn()
		})
	}
	if j := mustJob(t, b, id); j.State[0] != "TIMEOUT" {
		t.Fatalf("state = %v: a refused time limit must not revive an ended job", j.State)
	}
}

func TestFailNextLiftHoldFailsOnceAndChangesNothing(t *testing.T) {
	b, clock := newTestBatch(t)
	ctx := context.Background()
	b.Enqueue(Script{QueueWait: 5 * time.Minute})
	id := mustSubmit(t, b, gpuReq("example-tag", true))
	first, second := errors.New("example: first failure"), errors.New("example: second failure")
	b.FailNextLiftHold(first)
	b.FailNextLiftHold(second)

	for i, want := range []error{first, second} {
		if err := b.LiftHold(ctx, id); !errors.Is(err, want) {
			t.Fatalf("LiftHold %d = %v, want %v", i+1, err, want)
		}
		if j := mustJob(t, b, id); j.Hold != provider.HoldUser {
			t.Fatalf("after failure %d the hold = %q, want it kept", i+1, j.Hold)
		}
	}
	clock.Set(at(12, 1, 0))
	if err := b.LiftHold(ctx, id); err != nil { // the injected failures are spent
		t.Fatalf("third LiftHold = %v, want success", err)
	}
	j := wantPhase(t, b, id, v1alpha1.AllocationPending)
	if j.Hold != provider.HoldNone {
		t.Fatalf("hold = %q, want none", j.Hold)
	}
	// Eligible from the third call, 12:01:00, so it starts at 12:06:00.
	clock.Set(at(12, 5, 59))
	wantPhase(t, b, id, v1alpha1.AllocationPending)
	clock.Set(at(12, 6, 0))
	wantPhase(t, b, id, v1alpha1.AllocationRunning)
}

func TestLiftHoldLogsItsResult(t *testing.T) {
	b, _ := newTestBatch(t)
	ctx := context.Background()
	b.Enqueue(Script{}, Script{AdminHold: true})
	held := mustSubmit(t, b, gpuReq("example-tag", true))
	admin := mustSubmit(t, b, gpuReq("example-tag", false))
	running := mustSubmit(t, b, gpuReq("example-tag", false))
	b.FailNextLiftHold(errors.New("example failure"))

	for _, id := range []string{held, held, held, admin, running, "4242"} {
		_ = b.LiftHold(ctx, id)
	}
	var got []string
	for _, c := range b.Calls() {
		if c.Op == "scontrol-release" {
			got = append(got, c.Args[0]+" "+c.Args[1])
		}
	}
	want := []string{
		held + " " + LiftFailed, // the injected failure
		held + " " + LiftApplied,
		held + " " + LiftNotHeld, // a second lift of the same job
		admin + " " + LiftAdminHold,
		running + " " + LiftNotHeld,
		"4242 " + LiftNotFound,
	}
	if len(got) != len(want) {
		t.Fatalf("log = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("log entry %d = %q, want %q (log %v)", i, got[i], want[i], got)
		}
	}
}

func TestCancelOfAHeldJob(t *testing.T) {
	b, clock := newTestBatch(t)
	id := mustSubmit(t, b, gpuReq("example-tag", true))
	clock.Set(at(12, 10, 0))
	if j := mustJob(t, b, id); j.Phase != v1alpha1.AllocationHeld {
		t.Fatalf("setup: phase = %s, want Held", j.Phase)
	}
	res := b.Cancel(context.Background(), []string{id})
	if len(res) != 1 || res[0].Outcome != provider.Cancelled || res[0].Err != nil {
		t.Fatalf("Cancel = %+v, want one Cancelled", res)
	}
	j := wantPhase(t, b, id, v1alpha1.AllocationEnded)
	if j.State[0] != "CANCELLED" || j.Held() {
		t.Fatalf("state %v held %v, want CANCELLED and not held", j.State, j.Held())
	}
	wantTime(t, "EndTime", j.EndTime, at(12, 10, 0))
	wantNilTime(t, "StartTime", j.StartTime)
	if err := b.LiftHold(context.Background(), id); !errors.Is(err, provider.ErrNotHeld) {
		t.Fatalf("LiftHold of the cancelled job = %v, want ErrNotHeld", err)
	}
}
