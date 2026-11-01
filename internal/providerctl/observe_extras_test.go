package providerctl

// Extra jobs with the tag while the recorded job is observed: settle's cancel
// of held extras, the sticky duplicates list, and the ended recorded job
// (review of PR 76: Opus N9, N10; Astra B5). Over the real Batch and the fake
// Slurm; counts come from the fake Slurm's call log.

import (
	"strconv"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// extraSbatch submits a job with the tag straight to the fake Slurm, held or
// not: a late registration of an older attempt.
func (e *realEnv) extraSbatch(tag string, hold bool) {
	e.t.Helper()
	line := "sbatch --parsable --job-name=" + tag + " --no-requeue"
	if hold {
		line += " --hold"
	}
	line += " --partition=" + gpuPart + " --time=30 --nodes=1 --gres=gpu:1 --cpus-per-task=8 --mem=16384M --wrap=true"
	res, err := e.session.Run(e.ctx, slurm.Command{Line: line})
	if err != nil || res.ExitCode != 0 {
		e.t.Fatalf("extra sbatch: %v %d %s", err, res.ExitCode, res.Stderr)
	}
}

// Opus N9: settle cancels a held extra; Athena then lists it CANCELLED, not
// held, and it must not enter the duplicates list with a DuplicateJob warning.
func TestCancelledHeldExtraIsNoDuplicateOnAthena(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := newSlurm(clock, fakeslurm.Config{NotListed: fakeslurm.NotListedAthena})
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a")
	a := e.newAlloc("alloc-a", 1)
	e.run("alloc-a", 2)
	e.extraSbatch(tagOf(a), true)
	for i := 0; i < 3; i++ {
		clock.Advance(40 * time.Second)
		e.run("alloc-a", 1)
	}
	if n := sl.Count("scancel"); n != 1 {
		t.Fatalf("precondition: %d scancel calls, want settle's one for the held extra", n)
	}
	if d := e.get("alloc-a").Status.AtProvider.Duplicates; len(d) > 0 {
		t.Errorf("a held extra that settle cancelled is listed as a not-held duplicate: %v", d)
	}
}

// jobReads counts the accounting reads of one job by its ID (sacct -j <id>).
func jobReads(sl *fakeslurm.Platform, id string) int {
	n := 0
	for _, c := range sl.Calls() {
		if c.Words[0] != "sacct" {
			continue
		}
		for i, w := range c.Words {
			if w == "-j" && i+1 < len(c.Words) && c.Words[i+1] == id {
				n++
			}
		}
	}
	return n
}

// poll advances the clock by a poll interval and reconciles once, n times.
func (e *realEnv) poll(name string, n int) {
	for i := 0; i < n; i++ {
		e.clock.Advance(40 * time.Second)
		e.run(name, 1)
	}
}

// Astra B5: once the recorded job has ended for good, its accounting record is
// not read again, but the tag still is: a late held job of an older attempt
// that registers afterwards is cancelled while held. Hold mode, Helios.
func TestEndedJobIsNotReadAgainButALateHeldExtraIsCancelled(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := newSlurm(clock, fakeslurm.Config{NotListed: fakeslurm.NotListedHelios})
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a")
	a := e.newAlloc("alloc-a", 1)
	e.run("alloc-a", 2)
	e.start("alloc-a")
	e.poll("alloc-a", 1)
	id := e.get("alloc-a").Annotations[v1alpha1.AnnotationJobID]
	n, _ := strconv.Atoi(id)
	if err := sl.End(n, fakeslurm.Completed); err != nil {
		t.Fatal(err)
	}
	e.poll("alloc-a", 2)
	if o := e.get("alloc-a").Status.AtProvider; o.Phase != v1alpha1.AllocationEnded || !o.QueueWaitRecorded {
		t.Fatalf("precondition: phase %s, queue wait recorded %v; want Ended with the wait recorded", o.Phase, o.QueueWaitRecorded)
	}
	reads := jobReads(sl, id)
	e.poll("alloc-a", 5)
	if got := jobReads(sl, id); got != reads {
		t.Errorf("%d more accounting reads of the ended job %s over 5 polls, want none", got-reads, id)
	}
	cancels := sl.Count("scancel")
	e.extraSbatch(tagOf(a), true)
	e.poll("alloc-a", 1)
	if got := sl.Count("scancel"); got != cancels+1 {
		t.Fatalf("%d scancel calls after a late held extra appeared, want %d", got, cancels+1)
	}
	for _, j := range sl.Jobs() {
		if strconv.Itoa(j.ID) != id && j.Name == tagOf(a) && j.State != fakeslurm.Cancelled {
			t.Errorf("late extra %d is %s (held %v), want CANCELLED", j.ID, j.State, j.Held)
		}
	}
}

// Astra B5, fallback mode: a late extra that is not held still enters the
// sticky duplicates list after the recorded job has ended for good.
func TestEndedJobStillReportsALateDuplicate(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := newSlurm(clock, fakeslurm.Config{NotListed: fakeslurm.NotListedHelios})
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a", holdMode(false))
	a := e.newAlloc("alloc-a", 1)
	e.run("alloc-a", 2)
	e.start("alloc-a")
	e.poll("alloc-a", 1)
	id := e.get("alloc-a").Annotations[v1alpha1.AnnotationJobID]
	n, _ := strconv.Atoi(id)
	if err := sl.End(n, fakeslurm.Completed); err != nil {
		t.Fatal(err)
	}
	e.poll("alloc-a", 3)
	e.extraSbatch(tagOf(a), false)
	e.poll("alloc-a", 1)
	if d := e.get("alloc-a").Status.AtProvider.Duplicates; len(d) != 1 {
		t.Errorf("duplicates %v after a late extra that is not held, want it listed", d)
	}
}

// Opus N10: a job that ended without starting (cancelled while pending) has
// no queue wait to record; its accounting record is read once, not on every
// poll.
func TestEndedJobThatNeverStartedIsNotReadAgain(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := newSlurm(clock, fakeslurm.Config{NotListed: fakeslurm.NotListedHelios})
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a")
	e.newAlloc("alloc-a", 1)
	e.run("alloc-a", 2)
	id := e.get("alloc-a").Annotations[v1alpha1.AnnotationJobID]
	n, _ := strconv.Atoi(id)
	if err := sl.End(n, fakeslurm.Cancelled); err != nil {
		t.Fatal(err)
	}
	e.poll("alloc-a", 6)
	if o := e.get("alloc-a").Status.AtProvider; o.EndTime == nil || o.StartTime != nil {
		t.Fatalf("precondition: end %v, start %v; want an end and no start", o.EndTime, o.StartTime)
	}
	if got := jobReads(sl, id); got != 1 {
		t.Errorf("%d accounting reads of job %s over 6 polls after it ended, want 1", got, id)
	}
}

// Astra B5: a terminal state shown by the queue listing is not final. Athena
// lists an ended job in its end state until MinJobAge; the job is read from
// accounting once after the listing drops it, and never again. Besides that
// final read the job has the one accounting read of HBEP-4 (Poll rate), made
// after it was first seen started, for the CPUs and GPUs allocated (HBEP-8): so
// one sacct -j while it runs, none while the listing still shows it ended, and
// one more after the drop.
func TestEndedJobInTheListingIsReadFromAccountingOnce(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := newSlurm(clock, fakeslurm.Config{NotListed: fakeslurm.NotListedAthena, MinJobAge: 90 * time.Second})
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a")
	e.newAlloc("alloc-a", 1)
	e.run("alloc-a", 2)
	e.start("alloc-a")
	e.poll("alloc-a", 1)
	id := e.get("alloc-a").Annotations[v1alpha1.AnnotationJobID]
	n, _ := strconv.Atoi(id)
	if got := jobReads(sl, id); got != 1 {
		t.Fatalf("precondition: %d accounting reads of the running job, want the one for the allocated CPUs and GPUs", got)
	}
	if err := sl.End(n, fakeslurm.Completed); err != nil {
		t.Fatal(err)
	}
	e.poll("alloc-a", 1)
	if o := e.get("alloc-a").Status.AtProvider; o.Phase != v1alpha1.AllocationEnded || jobReads(sl, id) != 1 {
		t.Fatalf("precondition: phase %s after %d accounting reads; want Ended from the listing, with no read besides the allocated one", o.Phase, jobReads(sl, id))
	}
	e.poll("alloc-a", 6) // past MinJobAge: the listing drops it
	if got := jobReads(sl, id); got != 2 {
		t.Errorf("%d accounting reads of job %s, want exactly 2: the allocated one, and one once the listing dropped it", got, id)
	}
}
