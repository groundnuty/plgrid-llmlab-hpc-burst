package providerctl

// Terminal state versus complete accounting (review of PR 118, Astra B6 and
// B7), and the finalizer's bound (science agent, 31 Oct 2026). Over the real
// slurm.Batch and the fake Slurm. The clock starts at 12:00:00, the job starts
// at 12:01:00 with 8 CPUs and 1 GPU, and lostGrace is 2 min.

import (
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// elapsedHidden is a Platform whose accounting records lack time.elapsed while
// on is set, as accounting's record of a job reads before it has all of the
// job's executions: the job is ended, its accounting incomplete.
type elapsedHidden struct {
	Platform
	on *atomic.Bool
}

func (h elapsedHidden) strip(j provider.Job) provider.Job {
	if h.on.Load() && j.Accounting != nil {
		a := *j.Accounting
		a.Elapsed = nil
		j.Accounting = &a
	}
	return j
}

func (h elapsedHidden) ReadAccounting(ctx context.Context, id string) (provider.Job, bool, error) {
	j, found, err := h.Platform.ReadAccounting(ctx, id)
	return h.strip(j), found, err
}

func (h elapsedHidden) Observe(ctx context.Context, id string) (provider.Job, bool, error) {
	j, found, err := h.Platform.Observe(ctx, id)
	return h.strip(j), found, err
}

func (h elapsedHidden) FindByTag(ctx context.Context, tag string, since time.Time) ([]provider.Job, error) {
	jobs, err := h.Platform.FindByTag(ctx, tag, since)
	for i := range jobs {
		jobs[i] = h.strip(jobs[i])
	}
	return jobs, err
}

func withElapsedHidden(on *atomic.Bool) realOption {
	return func(e *realEnv) {
		inner := e.opts.NewPlatform
		e.opts.NewPlatform = func(v slurm.Validated) (Platform, io.Closer, error) {
			p, c, err := inner(v)
			return elapsedHidden{Platform: p, on: on}, c, err
		}
	}
}

// restart replaces the controller with a new one that remembers nothing.
func (e *realEnv) restart() {
	e.t.Helper()
	ctl, err := New(e.c, e.c, e.c.Scheme(), e.opts)
	if err != nil {
		e.t.Fatal(err)
	}
	e.ctl = ctl
}

// Astra B7: an end read from accounting whose record lacks the elapsed time is
// a terminal state, not complete accounting, so it does not stop the reads: the
// job is read again on every poll until the record is complete, then no more.
func TestAnIncompleteAccountingEndIsReadAgainUntilComplete(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := newSlurm(clock, fakeslurm.Config{})
	hidden := &atomic.Bool{}
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a", withElapsedHidden(hidden))
	e.newAlloc("alloc-a", 1)
	e.run("alloc-a", 2)
	id := e.get("alloc-a").Annotations[v1alpha1.AnnotationJobID]
	clock.Advance(60 * time.Second)
	e.start("alloc-a") // 12:01:00
	e.poll("alloc-a", 1)
	hidden.Store(true)
	clock.Set(at(12, 3, 0))
	endJob(t, sl, id, fakeslurm.Completed) // 120 s
	e.poll("alloc-a", 1)                   // 12:03:40
	wantAcctStatus(t, "incomplete end", e.accStatus("alloc-a"), 8, 1, -1, at(12, 3, 40))
	if e.get("alloc-a").Status.AtProvider.EndTime == nil {
		t.Fatal("precondition: the end is not in the status")
	}
	reads := jobReads(sl, id)
	e.poll("alloc-a", 1) // 12:04:20
	if got := jobReads(sl, id); got != reads+1 {
		t.Errorf("%d accounting reads after an incomplete end, want %d: read again", got, reads+1)
	}
	hidden.Store(false)
	e.poll("alloc-a", 1) // 12:05:00
	wantAcctStatus(t, "complete", e.accStatus("alloc-a"), 8, 1, 120, at(12, 5, 0))
	reads = jobReads(sl, id)
	e.poll("alloc-a", 3)
	if got := jobReads(sl, id); got != reads {
		t.Errorf("accounting reads went from %d to %d after the complete record, want none", reads, got)
	}
}

// The reads of a record that stays incomplete are bounded: once lostGrace has
// passed since the job's end (12:03:00 + 2 min), the next read is the last.
func TestAnIncompleteAccountingEndIsReadForAtMostLostGrace(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := newSlurm(clock, fakeslurm.Config{})
	hidden := &atomic.Bool{}
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a", withElapsedHidden(hidden))
	e.newAlloc("alloc-a", 1)
	e.run("alloc-a", 2)
	id := e.get("alloc-a").Annotations[v1alpha1.AnnotationJobID]
	clock.Advance(60 * time.Second)
	e.start("alloc-a")
	e.poll("alloc-a", 1)
	hidden.Store(true)
	clock.Set(at(12, 3, 0))
	endJob(t, sl, id, fakeslurm.Completed)
	before := jobReads(sl, id)
	e.poll("alloc-a", 6) // 12:03:40, 12:04:20, 12:05:00 (exactly 2 min), 12:05:40 (the last), 12:06:20, 12:07:00
	if got := jobReads(sl, id) - before; got != 4 {
		t.Errorf("%d accounting reads of an incomplete end over 6 polls, want 4: none after 12:05:40", got)
	}
	wantAcctStatus(t, "still incomplete", e.accStatus("alloc-a"), 8, 1, -1, at(12, 3, 40))
}

// Astra B6, live: the queue listing showed the end (Athena lists an ended job
// until MinJobAge), 120 s from 12:01:00 to 12:03:00. Once the listing drops the
// job, accounting's record of it is incomplete: the known final observation is
// kept, across a restart.
func TestAKnownFinalIsNotReplacedByALaterIncompleteRead(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := newSlurm(clock, fakeslurm.Config{NotListed: fakeslurm.NotListedAthena, MinJobAge: 10 * time.Minute})
	hidden := &atomic.Bool{}
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a", withElapsedHidden(hidden))
	e.newAlloc("alloc-a", 1)
	e.run("alloc-a", 2)
	id := e.get("alloc-a").Annotations[v1alpha1.AnnotationJobID]
	clock.Advance(60 * time.Second)
	e.start("alloc-a") // 12:01:00
	e.poll("alloc-a", 1)
	clock.Set(at(12, 3, 0))
	endJob(t, sl, id, fakeslurm.Completed)
	e.poll("alloc-a", 1) // 12:03:40, from the listing
	wantAcctStatus(t, "final from the listing", e.accStatus("alloc-a"), 8, 1, 120, at(12, 3, 40))
	hidden.Store(true)
	e.restart()
	clock.Set(at(12, 13, 30)) // past MinJobAge: accounting gives the job
	e.run("alloc-a", 1)
	e.poll("alloc-a", 1)
	wantAcctStatus(t, "after the incomplete read", e.accStatus("alloc-a"), 8, 1, 120, at(12, 3, 40))
}

// Astra B6, at deletion: the status holds the known final (120 s), and the
// deleting pass no longer finds the job anywhere. The record is written at once
// from the status, with the elapsed and end times, not after lostGrace with
// them unknown.
func TestDeletionWritesAKnownFinalFromTheStatus(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := newSlurm(clock, fakeslurm.Config{NotListed: fakeslurm.NotListedAthena, MinJobAge: 10 * time.Minute})
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a")
	e.newAlloc("alloc-a", 1)
	e.run("alloc-a", 2)
	id := e.get("alloc-a").Annotations[v1alpha1.AnnotationJobID]
	clock.Advance(60 * time.Second)
	e.start("alloc-a")
	e.poll("alloc-a", 1)
	clock.Set(at(12, 3, 0))
	endJob(t, sl, id, fakeslurm.Completed)
	e.poll("alloc-a", 1) // 12:03:40: 120 s, from the listing
	if err := sl.AcctHide(jobNumber(t, id)); err != nil {
		t.Fatal(err)
	}
	clock.Set(at(12, 13, 30)) // past MinJobAge: the job shows nowhere
	head := head(e, "alloc-a", "1001")
	e.del("alloc-a")
	deleteUntilGone(e, sl, "alloc-a", id) // 80 s at most, inside lostGrace
	e.wantRecord("alloc-a", head+`,"allocatedCPUs":8,"allocatedGPUs":1,"elapsedSeconds":120,`+
		`"startTime":"2026-10-30T12:01:00Z","endTime":"2026-10-30T12:03:00Z","basis":"allocated"}`)
}

// Astra B7 at deletion, and the bound (science agent, 31 Oct 2026): the tag
// lookup succeeds and shows no live job, but accounting's record of the
// recorded job stays incomplete. The finalizer waits lostGrace for the final
// read, counted across a restart, then the record is written with the elapsed
// time unknown and the finalizer goes. The job is cancelled at 12:02:00.
func TestAnIncompleteFinalReadAtDeletionWaitsLostGrace(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := newSlurm(clock, fakeslurm.Config{})
	hidden := &atomic.Bool{}
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a", withElapsedHidden(hidden))
	e.newAlloc("alloc-a", 1)
	e.run("alloc-a", 2)
	id := e.get("alloc-a").Annotations[v1alpha1.AnnotationJobID]
	clock.Advance(60 * time.Second)
	e.start("alloc-a") // 12:01:00
	e.poll("alloc-a", 1)
	hidden.Store(true)
	head := head(e, "alloc-a", "1001")
	clock.Set(at(12, 2, 0))
	e.del("alloc-a")
	e.run("alloc-a", 1) // 12:02:00: cancels the running job
	if j := slurmJob(t, sl, id); !ended(j) {
		t.Fatalf("precondition: job %s is %s after the first deleting pass, want cancelled", id, j.State)
	}
	clock.Set(at(12, 2, 10))
	e.run("alloc-a", 1) // the lookup shows the cancelled job, its record incomplete
	if since := e.get("alloc-a").Status.AtProvider.AbsentSince; since == nil || !since.Time.Equal(at(12, 2, 10)) {
		t.Fatalf("the wait starts at %v, want 12:02:10", since)
	}
	waiting := func(when time.Time) {
		t.Helper()
		clock.Set(when)
		e.run("alloc-a", 1)
		if e.gone("alloc-a") {
			t.Fatalf("the Allocation is gone at %s, before lostGrace has passed", when.Format("15:04:05"))
		}
		if _, ok := e.record("alloc-a"); ok {
			t.Fatalf("the record is written at %s, before lostGrace has passed", when.Format("15:04:05"))
		}
	}
	waiting(at(12, 3, 0))
	e.restart()
	waiting(at(12, 4, 10)) // exactly lostGrace after 12:02:10
	clock.Set(at(12, 4, 11))
	e.run("alloc-a", 1)
	if !e.gone("alloc-a") {
		t.Fatal("the Allocation is still there after lostGrace")
	}
	e.wantRecord("alloc-a", head+`,"allocatedCPUs":8,"allocatedGPUs":1,`+
		`"startTime":"2026-10-30T12:01:00Z","endTime":"2026-10-30T12:02:00Z","basis":"allocated","unknown":["elapsedSeconds"]}`)
}

// The bound covers only that wait (science agent, 31 Oct 2026): a tag lookup
// that fails, here because sacct fails on every call, is unknown, never
// absence, so the finalizer stays however long it lasts, and no record is
// written (HBEP-4; PR 76's Astra B3). Once accounting answers, the deletion
// completes with the final numbers.
func TestAFailedLookupAtDeletionKeepsTheFinalizer(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := newSlurm(clock, fakeslurm.Config{})
	f := &lineFaults{}
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a", withLineFaults(f))
	e.newAlloc("alloc-a", 1)
	e.run("alloc-a", 2)
	id := e.get("alloc-a").Annotations[v1alpha1.AnnotationJobID]
	clock.Advance(60 * time.Second)
	e.start("alloc-a") // 12:01:00
	e.poll("alloc-a", 1)
	clock.Set(at(12, 3, 0))
	endJob(t, sl, id, fakeslurm.Completed) // 120 s
	head := head(e, "alloc-a", "1001")
	f.fail("sacct", -1, slurm.ErrTransport)
	e.del("alloc-a")
	for m := 0; m <= 20; m++ { // to 12:23:00, ten times lostGrace
		clock.Set(at(12, 3+m, 0))
		e.run("alloc-a", 1)
		if e.gone("alloc-a") {
			t.Fatalf("the Allocation is gone at %s while every accounting read fails", clock.Now().Format("15:04:05"))
		}
		if _, ok := e.record("alloc-a"); ok {
			t.Fatalf("a record is written at %s while every accounting read fails", clock.Now().Format("15:04:05"))
		}
	}
	if f.failed() == 0 {
		t.Fatal("precondition: no sacct call was failed")
	}
	f.clear()
	deleteUntilGone(e, sl, "alloc-a", id)
	e.wantRecord("alloc-a", head+`,"allocatedCPUs":8,"allocatedGPUs":1,"elapsedSeconds":120,`+
		`"startTime":"2026-10-30T12:01:00Z","endTime":"2026-10-30T12:03:00Z","basis":"allocated"}`)
}
