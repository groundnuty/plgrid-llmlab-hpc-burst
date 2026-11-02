package fake

// Script.Epilog and the COMPLETING flag (HBEP-4, Phases, A completing job;
// science-agent ruling for #198, 1 Nov 2026). Expected times are literals: a
// job submitted at 12:00:00 with no queue wait starts at 12:00:00, a 30-minute
// limit makes it TIMEOUT at 12:30:00, FailAfter 5m makes it FAILED (or the
// FailState) at 12:05:00. MinJobAge is the fake's 10 s unless a test sets it.

import (
	"context"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

func wantState(t *testing.T, j provider.Job, want ...string) {
	t.Helper()
	if len(j.State) != len(want) {
		t.Fatalf("state %v, want %v", j.State, want)
	}
	for i := range want {
		if j.State[i] != want[i] {
			t.Fatalf("state %v, want %v", j.State, want)
		}
	}
}

// Each way a job can end, with an epilog of 10 minutes: at the end the record
// carries the flag beside the end state, its phase is the base state's, its
// source is the queue listing long past MinJobAge, and its EndTime and
// CompletingSince are the end. 10 minutes after the end the flag is gone and
// the job reads from accounting.
func TestEpilogHoldsTheFlagPastMinJobAgeThenClearsIt(t *testing.T) {
	five := 5 * time.Minute
	for _, c := range []struct {
		name   string
		script Script
		cancel bool
		end    time.Time
		state  string
		phase  v1alpha1.AllocationPhase
	}{
		{"FAILED", Script{FailAfter: &five}, false, at(12, 5, 0), "FAILED", v1alpha1.AllocationFailed},
		{"COMPLETED", Script{FailAfter: &five, FailState: "COMPLETED"}, false, at(12, 5, 0), "COMPLETED", v1alpha1.AllocationEnded},
		{"TIMEOUT", Script{}, false, at(12, 30, 0), "TIMEOUT", v1alpha1.AllocationEnded},
		{"CANCELLED", Script{}, true, at(12, 10, 0), "CANCELLED", v1alpha1.AllocationEnded},
	} {
		t.Run(c.name, func(t *testing.T) {
			b, clock := newTestBatch(t)
			c.script.Epilog = 10 * time.Minute
			b.Enqueue(c.script)
			id := mustSubmit(t, b, gpuReq("hb-a", false))
			if c.cancel {
				clock.Set(c.end)
				b.Cancel(context.Background(), []string{id})
			}
			clock.Set(c.end.Add(time.Second))
			j := wantPhase(t, b, id, c.phase)
			wantState(t, j, c.state, "COMPLETING")
			if !j.Completing || j.Source != provider.SourceQueue {
				t.Errorf("1 s after the end: Completing %v, source %s; want true, queue", j.Completing, j.Source)
			}
			wantTime(t, "EndTime", j.EndTime, c.end)
			wantTime(t, "CompletingSince", j.CompletingSince, c.end)
			clock.Set(c.end.Add(9*time.Minute + 59*time.Second)) // far past MinJobAge, 1 s short of the epilog's end
			j = wantPhase(t, b, id, c.phase)
			wantState(t, j, c.state, "COMPLETING")
			if !j.Completing || j.Source != provider.SourceQueue {
				t.Errorf("1 s short of the epilog's end: Completing %v, source %s; want true, queue (the listing keeps a completing job)", j.Completing, j.Source)
			}
			clock.Set(c.end.Add(10 * time.Minute))
			j = wantPhase(t, b, id, c.phase)
			wantState(t, j, c.state)
			if j.Completing || j.CompletingSince != nil || j.Source != provider.SourceAccounting {
				t.Errorf("at the epilog's end: Completing %v, CompletingSince %v, source %s; want false, nil, accounting (MinJobAge passed long ago)", j.Completing, j.CompletingSince, j.Source)
			}
			wantTime(t, "EndTime", j.EndTime, c.end)
		})
	}
}

// An epilog that ends before end + MinJobAge leaves the job in the queue
// listing in its end state, no flag, until MinJobAge: the Athena shape.
func TestEpilogEndingInsideMinJobAgeLeavesTheEndStateListed(t *testing.T) {
	clock := NewClock(at(12, 0, 0))
	b := NewBatch(clock, Config{MinJobAge: time.Hour, Partitions: []Partition{
		{Name: gpuPart, GPUModel: "example-gpu-model", GPUsPerNode: 4, MaxTime: time.Hour},
	}})
	five := 5 * time.Minute
	b.Enqueue(Script{FailAfter: &five, Epilog: 2 * time.Minute})
	id := mustSubmit(t, b, gpuReq("hb-a", false))
	clock.Set(at(12, 6, 0))
	j := wantPhase(t, b, id, v1alpha1.AllocationFailed)
	wantState(t, j, "FAILED", "COMPLETING")
	clock.Set(at(12, 7, 0))
	j = wantPhase(t, b, id, v1alpha1.AllocationFailed)
	wantState(t, j, "FAILED")
	if j.Completing || j.Source != provider.SourceQueue {
		t.Errorf("epilog over inside MinJobAge: Completing %v, source %s; want false, queue", j.Completing, j.Source)
	}
	clock.Set(at(13, 5, 0)) // end + MinJobAge
	if j = mustJob(t, b, id); j.Source != provider.SourceAccounting {
		t.Errorf("end + MinJobAge: source %s, want accounting", j.Source)
	}
}

// A job that never started has no epilog; no epilog is no flag; accounting
// never carries the flag, whatever the queue listing shows.
func TestEpilogNeedsAStartAndAccountingNeverCarriesTheFlag(t *testing.T) {
	b, clock := newTestBatch(t)
	b.Enqueue(Script{QueueWait: 10 * time.Minute, Epilog: time.Hour})
	never := mustSubmit(t, b, gpuReq("hb-a", false))
	clock.Set(at(12, 1, 0))
	b.Cancel(context.Background(), []string{never})
	j := mustJob(t, b, never)
	wantState(t, j, "CANCELLED")
	if j.Completing {
		t.Error("a job cancelled before it started is completing, want no epilog")
	}

	five := 5 * time.Minute
	b.Enqueue(Script{FailAfter: &five, Epilog: time.Hour}, Script{FailAfter: &five})
	withEpilog := mustSubmit(t, b, gpuReq("hb-b", false)) // starts 12:01:00, fails 12:06:00
	without := mustSubmit(t, b, gpuReq("hb-c", false))
	clock.Set(at(12, 7, 0))
	if j = wantPhase(t, b, withEpilog, v1alpha1.AllocationFailed); !j.Completing {
		t.Error("the job with an epilog is not completing after its end")
	}
	j = wantPhase(t, b, without, v1alpha1.AllocationFailed)
	wantState(t, j, "FAILED")
	if j.Completing {
		t.Error("the job with no epilog is completing")
	}
	acct, found, err := b.ReadAccounting(context.Background(), withEpilog)
	if err != nil || !found {
		t.Fatalf("ReadAccounting: %v, found %v", err, found)
	}
	wantState(t, acct, "FAILED")
	if acct.Completing || acct.CompletingSince != nil || acct.Source != provider.SourceAccounting {
		t.Errorf("accounting: Completing %v, CompletingSince %v, source %s; want false, nil, accounting", acct.Completing, acct.CompletingSince, acct.Source)
	}
}

// A completing job has ended as far as scancel goes: cancelling it is a
// harmless AlreadyEnded that changes nothing (HBEP-4, Cancel).
func TestCancelOfACompletingJobIsAlreadyEnded(t *testing.T) {
	b, clock := newTestBatch(t)
	five := 5 * time.Minute
	b.Enqueue(Script{FailAfter: &five, Epilog: time.Hour})
	id := mustSubmit(t, b, gpuReq("hb-a", false))
	clock.Set(at(12, 6, 0))
	res := b.Cancel(context.Background(), []string{id})
	if len(res) != 1 || res[0].Outcome != provider.AlreadyEnded {
		t.Fatalf("Cancel = %+v, want AlreadyEnded", res)
	}
	j := wantPhase(t, b, id, v1alpha1.AllocationFailed)
	wantState(t, j, "FAILED", "COMPLETING")
	wantTime(t, "EndTime", j.EndTime, at(12, 5, 0))
}

// Slurm frees the user's submit slot when it sets the flag: with one job
// allowed, a second Submit is refused while the first runs and accepted while
// it is completing.
func TestCompletingJobFreesItsSubmitSlotAtTheEnd(t *testing.T) {
	clock := NewClock(at(12, 0, 0))
	b := NewBatch(clock, Config{MaxSubmitPerUser: 1, Partitions: []Partition{
		{Name: gpuPart, GPUModel: "example-gpu-model", GPUsPerNode: 4, MaxTime: time.Hour},
	}})
	five := 5 * time.Minute
	b.Enqueue(Script{FailAfter: &five, Epilog: time.Hour})
	first := mustSubmit(t, b, gpuReq("hb-a", false))
	if _, outcome, err := b.Submit(context.Background(), gpuReq("hb-b", false)); outcome != provider.Rejected || err == nil {
		t.Fatalf("a second Submit beside the running job: %s, %v; want the submit-limit refusal", outcome, err)
	}
	clock.Set(at(12, 6, 0))
	if j := mustJob(t, b, first); !j.Completing {
		t.Fatal("the first job is not completing after its end")
	}
	mustSubmit(t, b, gpuReq("hb-b", false)) // accepted: the completing job holds no slot
}

// Slurm frees the user's GPUs when it sets the flag: with a cap of one GPU, a
// job that waits on it starts at the first job's end (12:05:00), an hour
// before its epilog is over.
func TestCompletingJobFreesItsGPUAtTheEnd(t *testing.T) {
	clock := NewClock(at(12, 0, 0))
	b := NewBatch(clock, Config{MaxGPUsPerUser: 1, Partitions: []Partition{
		{Name: gpuPart, GPUModel: "example-gpu-model", GPUsPerNode: 4, MaxTime: time.Hour},
	}})
	five := 5 * time.Minute
	b.Enqueue(Script{FailAfter: &five, Epilog: time.Hour})
	first := mustSubmit(t, b, gpuReq("hb-a", false))
	second := mustSubmit(t, b, gpuReq("hb-b", false))
	clock.Set(at(12, 1, 0))
	wantWaitingOnCap(t, b, second)
	clock.Set(at(12, 6, 0))
	if j := mustJob(t, b, first); !j.Completing {
		t.Fatal("the first job is not completing after its end")
	}
	j := wantPhase(t, b, second, v1alpha1.AllocationRunning)
	wantTime(t, "StartTime", j.StartTime, at(12, 5, 0))
}

func TestNegativeEpilogIsNotAnOption(t *testing.T) {
	// The zero value is no epilog, and Epilog is read only as "above 0": a
	// negative one behaves as none, never as an immediate or endless flag.
	b, clock := newTestBatch(t)
	five := 5 * time.Minute
	b.Enqueue(Script{FailAfter: &five, Epilog: -time.Minute})
	id := mustSubmit(t, b, gpuReq("hb-a", false))
	clock.Set(at(12, 5, 30))
	j := wantPhase(t, b, id, v1alpha1.AllocationFailed)
	wantState(t, j, "FAILED")
}
