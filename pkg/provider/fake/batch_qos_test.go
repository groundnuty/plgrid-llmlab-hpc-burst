package fake

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// Tests of the fake's per-user QOS model (HBEP-4, QOS limits). As elsewhere in
// this package, every expected time and reason is a literal worked out by hand
// from the script, never read back from the fake's own helpers. The names share
// the prefix TestQOS so one -test.run covers them all.

// qosPartitions is newTestBatch's platform and newEnv's, one place for the QOS
// tests that need a Config of their own.
func qosPartitions() []Partition {
	return []Partition{
		{Name: gpuPart, GPUModel: "example-gpu-model", GPUsPerNode: 4, MaxTime: time.Hour},
		{Name: cpuPart, GPUModel: "none", GPUsPerNode: 0, MaxTime: 2 * time.Hour},
	}
}

// newQOSBatch is a Batch on the standard platform with the given QOS limits.
func newQOSBatch(cfg Config) (*Batch, *Clock) {
	clock := NewClock(at(12, 0, 0))
	cfg.Partitions = qosPartitions()
	return NewBatch(clock, cfg), clock
}

// wantSubmitLimit checks that a Submit was refused for a full submit limit: no
// ID, the outcome Rejected and a *provider.PolicyRefusal whose reason is
// QOSMaxSubmitJobPerUserLimit and whose line is Slurm's reason line.
func wantSubmitLimit(t *testing.T, b *Batch, tag string) {
	t.Helper()
	id, outcome, err := b.Submit(context.Background(), gpuReq(tag, false))
	var pr *provider.PolicyRefusal
	if id != "" || outcome != provider.Rejected || !errors.As(err, &pr) {
		t.Fatalf("at %s Submit(%s) = (%q, %s, %v), want no ID, Rejected and a PolicyRefusal", b.clock.Now().Format("15:04:05"), tag, id, outcome, err)
	}
	if pr.Reason != "QOSMaxSubmitJobPerUserLimit" || !pr.SubmitLimit() || pr.Line != "sbatch: error: QOSMaxSubmitJobPerUserLimit" {
		t.Fatalf("refusal = %+v, want reason QOSMaxSubmitJobPerUserLimit, a submit limit, with Slurm's reason line", pr)
	}
}

func TestQOSSubmitLimitRefusesWhileAnOutsideJobHoldsTheSlot(t *testing.T) {
	b, clock := newQOSBatch(Config{MaxSubmitPerUser: 1})
	// The queued Script is the next accepted Submit's: a 5-minute queue wait.
	b.Enqueue(Script{QueueWait: 5 * time.Minute})
	outside := b.AddOutsideJob("example-other-job", gpuPart, 1, time.Hour)
	if outside != "998" {
		t.Fatalf("outside job ID = %s, want 998", outside)
	}
	if n := b.Count("sbatch"); n != 0 {
		t.Fatalf("sbatch count after AddOutsideJob = %d, want 0: it is not the framework's call", n)
	}

	wantSubmitLimit(t, b, "example-tag")
	if n := b.Count("sbatch"); n != 1 {
		t.Fatalf("sbatch count = %d, want 1: Slurm refused a real sbatch call", n)
	}
	if jobs, err := b.FindByTag(context.Background(), "example-tag", at(0, 0, 0)); err != nil || len(jobs) != 0 {
		t.Fatalf("FindByTag after the refusal = %v, %v; want no job", jobs, err)
	}

	// The outside job ends: the next Submit is accepted, takes the next ID (the
	// refusal registered nothing: 999, not 1000), and still has the Script the
	// refusal left in the queue.
	b.CancelExternally(outside)
	id := mustSubmit(t, b, gpuReq("example-tag", false))
	if id != "999" {
		t.Fatalf("accepted job ID = %s, want 999: the refusal registered a job", id)
	}
	if n := b.Count("sbatch"); n != 2 {
		t.Fatalf("sbatch count = %d, want 2", n)
	}
	wantPhase(t, b, id, v1alpha1.AllocationPending)
	clock.Advance(5 * time.Minute) // 12:05:00
	j := wantPhase(t, b, id, v1alpha1.AllocationRunning)
	wantTime(t, "StartTime", j.StartTime, at(12, 5, 0))
}

func TestQOSSubmitLimitFreesWhenTheOutsideJobEndsByItself(t *testing.T) {
	b, clock := newQOSBatch(Config{MaxSubmitPerUser: 1})
	b.AddOutsideJob("example-other-job", gpuPart, 1, 10*time.Minute) // ends 12:10:00

	clock.Set(at(12, 9, 59))
	wantSubmitLimit(t, b, "example-tag")
	clock.Set(at(12, 10, 0))
	mustSubmit(t, b, gpuReq("example-tag", false))
}

func TestQOSSubmitLimitDoesNotCountAnEndedFrameworkJob(t *testing.T) {
	b, clock := newQOSBatch(Config{MaxSubmitPerUser: 1})
	first := mustSubmit(t, b, gpuReq("example-first", false)) // 30m limit: ends 12:30:00
	clock.Set(at(12, 29, 59))
	wantSubmitLimit(t, b, "example-second")
	clock.Set(at(12, 30, 0))
	if j := wantPhase(t, b, first, v1alpha1.AllocationEnded); j.State[0] != "TIMEOUT" {
		t.Fatalf("first job state = %v, want TIMEOUT", j.State)
	}
	mustSubmit(t, b, gpuReq("example-second", false))
	wantSubmitLimit(t, b, "example-third") // the second job holds the slot now
}

func TestQOSSubmitLimitCountsAHeldJob(t *testing.T) {
	b, _ := newQOSBatch(Config{MaxSubmitPerUser: 1})
	held := mustSubmit(t, b, gpuReq("example-held", true))
	wantPhase(t, b, held, v1alpha1.AllocationHeld)
	wantSubmitLimit(t, b, "example-tag")

	// Lifting the hold changes nothing: the job is pending or running, and live.
	if err := b.LiftHold(context.Background(), held); err != nil {
		t.Fatal(err)
	}
	wantSubmitLimit(t, b, "example-tag")

	res := b.Cancel(context.Background(), []string{held})
	if len(res) != 1 || res[0].Outcome != provider.Cancelled {
		t.Fatalf("Cancel = %+v, want Cancelled", res)
	}
	mustSubmit(t, b, gpuReq("example-tag", false))
}

func TestQOSSubmitLimitCountsAVanishedJob(t *testing.T) {
	b, _ := newQOSBatch(Config{MaxSubmitPerUser: 1})
	id := mustSubmit(t, b, gpuReq("example-first", false))
	b.Vanish(id) // reads no longer show it; the scheduler still holds it
	wantSubmitLimit(t, b, "example-second")
}

func TestQOSSubmitLimitOfTwoCountsEveryLiveJob(t *testing.T) {
	b, _ := newQOSBatch(Config{MaxSubmitPerUser: 2})
	mustSubmit(t, b, gpuReq("example-first", false))
	b.AddOutsideJob("example-other-job", gpuPart, 1, time.Hour)
	wantSubmitLimit(t, b, "example-second") // 2 live jobs, the limit
}

func TestQOSUnsetSubmitLimitNeverRefuses(t *testing.T) {
	b, _ := newQOSBatch(Config{})
	for i := 0; i < 6; i++ {
		b.AddOutsideJob("example-other-job", gpuPart, 1, time.Hour)
	}
	for i := 0; i < 6; i++ {
		mustSubmit(t, b, gpuReq("example-tag", false))
	}
	if n := b.Count("sbatch"); n != 6 {
		t.Fatalf("sbatch count = %d, want 6", n)
	}
}

// The scripted outcomes still apply when the limit does not refuse; and when it
// does, it refuses before the Script is read: a scripted refusal stays queued.
func TestQOSSubmitLimitAndScriptedPolicyRefusal(t *testing.T) {
	b, _ := newQOSBatch(Config{MaxSubmitPerUser: 2})
	outside := b.AddOutsideJob("example-other-job", gpuPart, 1, time.Hour)
	wantScripted := func(when string) {
		t.Helper()
		_, outcome, err := b.Submit(context.Background(), gpuReq("example-tag", false))
		var pr *provider.PolicyRefusal
		if outcome != provider.Rejected || !errors.As(err, &pr) || pr.Reason != "QOSMaxWallDurationPerJobLimit" || pr.SubmitLimit() {
			t.Fatalf("%s: Submit = (%s, %v), want the scripted refusal for QOSMaxWallDurationPerJobLimit", when, outcome, err)
		}
	}

	// One live job, limit 2: the scripted refusal applies.
	b.Enqueue(Script{Outcome: provider.Rejected, Policy: "QOSMaxWallDurationPerJobLimit"})
	wantScripted("with room under the limit")

	// Two live jobs, the limit: the scripted refusal waits in the queue while the
	// limit refuses, and applies once the outside job has ended.
	mustSubmit(t, b, gpuReq("example-second", false))
	b.Enqueue(Script{Outcome: provider.Rejected, Policy: "QOSMaxWallDurationPerJobLimit"})
	wantSubmitLimit(t, b, "example-tag")
	wantSubmitLimit(t, b, "example-tag")
	b.CancelExternally(outside)
	wantScripted("after the limit freed")
	mustSubmit(t, b, gpuReq("example-tag", false)) // the queue is empty now
}

func TestQOSOutsideJobIsSetUpState(t *testing.T) {
	b, _ := newQOSBatch(Config{MaxSubmitPerUser: 1})
	first := b.AddOutsideJob("example-other-job", gpuPart, 2, time.Hour)
	second := b.AddOutsideJob("example-other-job", cpuPart, 0, 2*time.Hour) // past the limit: not refused
	if first != "998" || second != "999" {
		t.Fatalf("outside job IDs = %s, %s; want 998, 999", first, second)
	}
	if calls := b.Calls(); len(calls) != 0 {
		t.Fatalf("call log = %v, want empty: an outside job is set-up state", calls)
	}
	j := wantPhase(t, b, first, v1alpha1.AllocationRunning)
	if j.Name != "example-other-job" || j.Partition != gpuPart || j.GPUs != 2 || j.TimeLimit != time.Hour {
		t.Fatalf("outside job = %+v, want its name, partition, 2 GPUs and 1h limit", j)
	}
	wantTime(t, "StartTime", j.StartTime, at(12, 0, 0))
	wantTime(t, "HardEnd", j.HardEnd, at(13, 0, 0))
	// The framework's FindByTag for its own tag does not see it.
	if jobs, err := b.FindByTag(context.Background(), "example-tag", at(0, 0, 0)); err != nil || len(jobs) != 0 {
		t.Fatalf("FindByTag(example-tag) = %v, %v; want no job", jobs, err)
	}
	// It ends with the existing CancelExternally.
	b.CancelExternally(first)
	wantPhase(t, b, first, v1alpha1.AllocationEnded)
}

func TestQOSOutsideJobRefusesBadSetUp(t *testing.T) {
	b, _ := newQOSBatch(Config{})
	for name, f := range map[string]func(){
		"unsafe name":       func() { b.AddOutsideJob("bad name", gpuPart, 1, time.Hour) },
		"empty name":        func() { b.AddOutsideJob("", gpuPart, 1, time.Hour) },
		"unknown partition": func() { b.AddOutsideJob("example-other-job", "nowhere", 1, time.Hour) },
		"negative gpus":     func() { b.AddOutsideJob("example-other-job", gpuPart, -1, time.Hour) },
		"zero limit":        func() { b.AddOutsideJob("example-other-job", gpuPart, 1, 0) },
		"negative submit":   func() { newQOSBatch(Config{MaxSubmitPerUser: -1}) },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s: no panic", name)
				}
			}()
			f()
		}()
	}
}

// Through the stand-in Provider controller: the Allocation stays Submitting with
// a Rejected record whose code is SubmitLimitReached, for as long as the outside
// job holds the only slot; once it ends and the backoff has passed, the next
// attempt is accepted.
func TestQOSSubmitLimitThroughTheController(t *testing.T) {
	e := newEnv(t)
	e.batch = NewBatch(e.clock, Config{Partitions: qosPartitions(), MaxSubmitPerUser: 1})
	e.ctl.Batch = e.batch
	outside := e.batch.AddOutsideJob("example-other-job", gpuPart, 1, time.Hour)
	e.newAlloc("a1", testProvider)

	for i, when := range []time.Time{at(12, 0, 0), at(12, 1, 0)} { // RejectBackoff is 1m
		e.syncAt(when)
		o := e.wantPhase("a1", v1alpha1.AllocationSubmitting)
		rec := submission(t, e.alloc("a1"))
		if rec.Attempt != int32(i+1) || rec.State != v1alpha1.SubmissionRejected || rec.Code != v1alpha1.ReasonSubmitLimitReached ||
			!strings.Contains(rec.Reason, "QOSMaxSubmitJobPerUserLimit") {
			t.Fatalf("at %s record = %+v, want attempt %d Rejected with code SubmitLimitReached", when.Format("15:04:05"), rec, i+1)
		}
		if o.Reason != v1alpha1.ReasonSubmitLimitReached {
			t.Fatalf("at %s reason = %q, want SubmitLimitReached", when.Format("15:04:05"), o.Reason)
		}
		if _, ok := e.alloc("a1").Annotations[v1alpha1.AnnotationJobID]; ok {
			t.Fatal("a job ID was recorded for a refusal")
		}
	}
	if n := e.batch.Count("sbatch"); n != 2 {
		t.Fatalf("sbatch count = %d, want 2", n)
	}

	// The outside job ends at 12:01:30. The reject backoff of the second attempt
	// (12:01:00) passes at 12:02:00: before it, no new sbatch.
	e.clock.Set(at(12, 1, 30))
	e.batch.CancelExternally(outside)
	e.sync()
	e.wantPhase("a1", v1alpha1.AllocationSubmitting)
	if n := e.batch.Count("sbatch"); n != 2 {
		t.Fatalf("sbatch count = %d at 12:01:30, want 2: the backoff has not passed", n)
	}
	e.syncAt(at(12, 2, 0))
	e.wantPhase("a1", v1alpha1.AllocationRunning)
	rec := submission(t, e.alloc("a1"))
	if rec.Attempt != 3 || rec.State != v1alpha1.SubmissionDone {
		t.Fatalf("record = %+v, want attempt 3 Done", rec)
	}
	if n := e.batch.Count("sbatch"); n != 3 {
		t.Fatalf("sbatch count = %d, want 3", n)
	}
}
