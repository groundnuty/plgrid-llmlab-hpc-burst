package providerctl

// Deletion and the recorded job (review of PR 76: Opus R2 and Astra B3, a job
// the site renamed outlives the delete; Astra B4, an adopted job bypassed the
// unresolved-attempt guard). The renamed-job cases run over the real Batch and
// the fake Slurm, which renames at sbatch (fakeslurm.Rename); the guard cases
// run over fake.Batch. Job states come from the fakes' own records.

import (
	"strconv"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

func holdMode(hold bool) realOption {
	return func(e *realEnv) { e.profiles.set(func(v *slurm.Validated) { v.Profile.Spec.HoldRelease = hold }) }
}

func (e *realEnv) gone(name string) bool {
	e.t.Helper()
	var a v1alpha1.Allocation
	return e.c.Get(e.ctx, clientKey(name), &a) != nil
}

func (e *realEnv) del(name string) {
	e.t.Helper()
	if err := e.c.Delete(e.ctx, e.get(name)); err != nil {
		e.t.Fatal(err)
	}
}

// slurmJob returns the fake Slurm's own record of a job.
func slurmJob(t *testing.T, sl *fakeslurm.Platform, id string) fakeslurm.Job {
	t.Helper()
	n, err := strconv.Atoi(id)
	if err != nil {
		t.Fatalf("job ID %q: %v", id, err)
	}
	j, ok := sl.Job(n)
	if !ok {
		t.Fatalf("the fake Slurm has no job %s", id)
	}
	return j
}

func ended(j fakeslurm.Job) bool { return j.State != fakeslurm.Pending && j.State != fakeslurm.Running }

// deleteUntilGone reconciles a deleting Allocation until it is gone, failing
// if it goes while its job is still live.
func deleteUntilGone(e *realEnv, sl *fakeslurm.Platform, name, id string) {
	e.t.Helper()
	for i := 0; i < 8; i++ {
		e.clock.Advance(10 * time.Second)
		e.run(name, 1)
		if e.gone(name) {
			if j := slurmJob(e.t, sl, id); !ended(j) {
				e.t.Fatalf("the Allocation is gone while its job %s is still %s (held %v)", id, j.State, j.Held)
			}
			return
		}
	}
	e.t.Fatalf("the Allocation is still there after 8 reconciles; its job is %s", slurmJob(e.t, sl, id).State)
}

// Opus R2: the site stores the job under another name; the Allocation is
// deleted before any observe refuses and cancels it (in hold mode accepted()
// leaves the renamed job held). Deletion cancels the recorded job by its ID
// and keeps the finalizer until it has ended. Hold and fallback mode.
func TestDeleteCancelsARenamedRecordedJob(t *testing.T) {
	for _, hold := range []bool{true, false} {
		t.Run(map[bool]string{true: "hold", false: "fallback"}[hold], func(t *testing.T) {
			clock := fake.NewClock(at(12, 0, 0))
			sl := newSlurm(clock, fakeslurm.Config{})
			e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a", holdMode(hold))
			sl.Script(fakeslurm.Rename(20))
			e.newAlloc("alloc-a", 1)
			e.run("alloc-a", 1)
			id := e.get("alloc-a").Annotations[v1alpha1.AnnotationJobID]
			if id == "" {
				t.Fatal("precondition: no job recorded")
			}
			if j := slurmJob(t, sl, id); ended(j) || j.Name == tagOf(e.get("alloc-a")) {
				t.Fatalf("precondition: job %s is %s under name %q; want a live renamed job", id, j.State, j.Name)
			}
			e.del("alloc-a")
			deleteUntilGone(e, sl, "alloc-a", id)
			if j := slurmJob(t, sl, id); j.State != fakeslurm.Cancelled {
				t.Errorf("job %s is %s after the delete, want CANCELLED", id, j.State)
			}
		})
	}
}

// Astra B3: an observe refuses the renamed job, but its cancel fails; the
// Allocation is deleted before the retry. Deletion still cancels the job.
func TestDeleteAfterAFailedRenamedJobCancel(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := newSlurm(clock, fakeslurm.Config{})
	f := &lineFaults{}
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a", withLineFaults(f))
	sl.Script(fakeslurm.Rename(20))
	e.newAlloc("alloc-a", 1)
	e.run("alloc-a", 1)
	id := e.get("alloc-a").Annotations[v1alpha1.AnnotationJobID]
	f.fail("scancel", 1, slurm.ErrTransport)
	clock.Advance(40 * time.Second)
	e.run("alloc-a", 1)
	if f.failed() != 1 || e.get("alloc-a").Status.AtProvider.Phase != v1alpha1.AllocationRefused {
		t.Fatalf("precondition: %d scancel failed, phase %s; want the refusal's cancel to have failed",
			f.failed(), e.get("alloc-a").Status.AtProvider.Phase)
	}
	if j := slurmJob(t, sl, id); ended(j) {
		t.Fatalf("precondition: job %s is %s; want it live after the failed cancel", id, j.State)
	}
	e.del("alloc-a")
	deleteUntilGone(e, sl, "alloc-a", id)
}

// A read of the recorded job that fails is unknown, never absence: the
// finalizer stays. The renamed job ended unseen and the listing dropped it
// (Helios), so only its accounting record by ID can show it; that read fails.
func TestDeleteKeepsTheFinalizerWhileTheRecordedJobCannotBeRead(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := newSlurm(clock, fakeslurm.Config{NotListed: fakeslurm.NotListedHelios})
	f := &lineFaults{}
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a", withLineFaults(f), holdMode(false))
	sl.Script(fakeslurm.Rename(20))
	e.newAlloc("alloc-a", 1)
	e.run("alloc-a", 1)
	id := e.get("alloc-a").Annotations[v1alpha1.AnnotationJobID]
	n, _ := strconv.Atoi(id)
	if err := sl.End(n, fakeslurm.Completed); err != nil {
		t.Fatal(err)
	}
	f.fail("sacct --json -X -D -j", -1, slurm.ErrTransport)
	e.del("alloc-a")
	for i := 0; i < 5; i++ {
		clock.Advance(10 * time.Second)
		e.run("alloc-a", 1)
		if e.gone("alloc-a") {
			t.Fatalf("the finalizer went on reconcile %d while the recorded job's read was failing", i+1)
		}
	}
	if f.failed() == 0 {
		t.Fatal("precondition: the recorded job was never read by its ID")
	}
	f.clear()
	e.run("alloc-a", 2)
	if !e.gone("alloc-a") {
		t.Error("the Allocation is still there once the read succeeds and shows the job ended")
	}
}

// Astra B4, hold mode: attempt 2 is in flight inside its window when attempt
// 1's late job registers and is adopted. Deleting cancels the adopted job, but
// attempt 2 is still unresolved, so the finalizer stays until its window has
// passed, and attempt 2's own late job, registering inside the window, is
// cancelled too.
func TestDeleteKeepsAnUnresolvedAttemptAfterAnAdoption(t *testing.T) {
	e := newEnv(t)
	a := e.newAlloc("a")
	ptPlantRecord(e, "a", ptInFlight(e, 2))
	late1 := ptSubmit(e, a, true)
	e.run("a", 1)
	if got, r := ptJobID(e, "a"), ptRecordOf(e, "a"); got != late1 || r == nil || r.Attempt != 2 || r.State != v1alpha1.SubmissionInFlight {
		t.Fatalf("precondition: job %q, record %+v; want %s adopted and attempt 2 still InFlight", got, r, late1)
	}
	if !ptHasFinalizer(e.get("a")) {
		t.Fatal("precondition: no finalizer")
	}
	ptDelete(e, "a")
	for i := 0; i < 3; i++ {
		ptStep(e, "a", 10*time.Second)
		if ptGone(e, "a") {
			t.Fatalf("the finalizer went %d reconciles into the delete while attempt 2 was in flight inside its window", i+1)
		}
	}
	if j, _ := e.batch.Job(late1); j.EndTime == nil {
		t.Fatalf("the adopted job %s was not cancelled", late1)
	}
	late2 := ptSubmit(e, a, true) // attempt 2's sbatch returns inside the window
	ptStep(e, "a", 10*time.Second)
	ptStep(e, "a", 10*time.Second)
	if ptGone(e, "a") {
		t.Fatal("the Allocation went while attempt 2's late job was live")
	}
	e.clock.Advance(5 * time.Minute)
	e.run("a", 3)
	if !ptGone(e, "a") {
		t.Fatal("the Allocation is still there after attempt 2's window passed")
	}
	if live := e.live(tagOf(a)); len(live) != 0 {
		t.Errorf("live jobs %v carry the tag after the delete; attempt 2's job %s must have been cancelled", live, late2)
	}
}

// The fallback control of B4: the recorded job resolves an InFlight attempt
// once its window has passed, as observeRecorded does, so the deletion needs no
// operator abandon and raises no NeedsOperator condition.
func TestDeleteResolvesTheAttemptByTheRecordedJobInFallback(t *testing.T) {
	e := newEnv(t, fallback)
	a := e.newAlloc("a")
	ptPlantRecord(e, "a", ptInFlight(e, 2))
	late1 := ptSubmit(e, a, false)
	e.run("a", 1)
	if got := ptJobID(e, "a"); got != late1 {
		t.Fatalf("precondition: job %q, want %s adopted", got, late1)
	}
	ptDelete(e, "a")
	ptStep(e, "a", 10*time.Second)
	if a := ptStep(e, "a", 10*time.Second); ptGone(e, "a") {
		t.Fatal("the finalizer went while attempt 2 was in flight inside its window")
	} else if c := ptCondStatus(a, v1alpha1.ConditionSubmissionResolved); c == corev1.ConditionFalse {
		t.Fatalf("SubmissionResolved %s inside the window", c)
	}
	e.clock.Advance(5 * time.Minute)
	e.run("a", 3)
	if !ptGone(e, "a") {
		a := e.get("a")
		t.Fatalf("the Allocation is still there after the window passed; record %+v, SubmissionResolved %s",
			ptRecordOf(e, "a"), a.GetCondition(v1alpha1.ConditionSubmissionResolved).Status)
	}
}
