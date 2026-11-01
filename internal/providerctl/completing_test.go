package providerctl

// A COMPLETING job (HBEP-4, Phases, A completing job; science-agent ruling for
// #198, 1 Nov 2026), over the real slurm.Batch and the fake Slurm. The phase
// follows the base state at once, status.atProvider.jobCompleting and the
// JobCompleting condition (its "since" from the listing's end_time) are written
// in the same status write, and the flag clears on a listing without it, on an
// accounting read, or when no read lists the job; a failed read keeps the last
// evidence. Both listing shapes of the ruling are tested: Helios 24.11 drops
// the job from the listing the moment the flag clears (the job then reads from
// accounting), Athena 23.11 keeps it listed in its base state until MinJobAge.
//
// The clock starts at 12:00:00, the job starts at 12:01:00 and ends at 12:02:30.
// Expected values are written out by hand: the job ID is the fake Slurm's
// first, 1001, and every instant is a literal.

import (
	"context"
	"io"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// cgRig is a Provider's controller over the real Batch and a fake Slurm, with
// one Allocation whose job is Running.
type cgRig struct {
	*realEnv
	sl *fakeslurm.Platform
	id string
	n  int
}

// newCGRig starts the job at 12:01:00 and observes it Running.
func newCGRig(t *testing.T, mode fakeslurm.NotListedMode, minAge time.Duration, mods ...realOption) *cgRig {
	t.Helper()
	r := startCGRig(t, mode, minAge, mods...)
	r.poll("alloc-a", 1)
	if p := r.get("alloc-a").Status.AtProvider.Phase; p != v1alpha1.AllocationRunning {
		t.Fatalf("precondition: phase %s, want Running", p)
	}
	return r
}

// startCGRig starts the job at 12:01:00 and makes no observation after it, so
// the first read of the job finds it ended.
func startCGRig(t *testing.T, mode fakeslurm.NotListedMode, minAge time.Duration, mods ...realOption) *cgRig {
	t.Helper()
	clock := fake.NewClock(at(12, 0, 0))
	sl := newSlurm(clock, fakeslurm.Config{NotListed: mode, MinJobAge: minAge})
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a", mods...)
	e.newAlloc("alloc-a", 1)
	e.run("alloc-a", 2)
	id := e.get("alloc-a").Annotations[v1alpha1.AnnotationJobID]
	if id != "1001" {
		t.Fatalf("precondition: job ID %q, want 1001", id)
	}
	clock.Advance(60 * time.Second)
	e.start("alloc-a") // 12:01:00
	return &cgRig{realEnv: e, sl: sl, id: id, n: jobNumber(t, id)}
}

// endCompleting ends the job in state at when and holds it COMPLETING, then
// lets one poll observe it.
func (r *cgRig) endCompleting(state string, when time.Time) {
	r.t.Helper()
	r.clock.Set(when)
	endJob(r.t, r.sl, r.id, state)
	if err := r.sl.HoldCompleting(r.n, true); err != nil {
		r.t.Fatal(err)
	}
	r.poll("alloc-a", 1)
}

// wantCompleting checks the Allocation shows the job completing: the phase, the
// flag, the end time and the condition with its message.
func (r *cgRig) wantCompleting(what string, phase v1alpha1.AllocationPhase, state, since string) {
	r.t.Helper()
	a := r.get("alloc-a")
	o := a.Status.AtProvider
	if o.Phase != phase || !o.JobCompleting {
		r.t.Errorf("%s: phase %s, jobCompleting %v; want %s, true", what, o.Phase, o.JobCompleting, phase)
	}
	if o.EndTime == nil || o.EndTime.UTC().Format(time.RFC3339) != since {
		r.t.Errorf("%s: endTime %v, want %s", what, o.EndTime, since)
	}
	c := a.GetCondition(v1alpha1.ConditionJobCompleting)
	want := "job 1001 is completing in state " + state + " since " + since
	if c.Status != corev1.ConditionTrue || string(c.Reason) != "Completing" || c.Message != want {
		r.t.Errorf("%s: condition JobCompleting %s %q %q, want True Completing %q", what, c.Status, c.Reason, c.Message, want)
	}
}

// wantCleared checks the flag and its condition are cleared and the phase is
// still the base state's.
func (r *cgRig) wantCleared(what string, phase v1alpha1.AllocationPhase) {
	r.t.Helper()
	a := r.get("alloc-a")
	o := a.Status.AtProvider
	if o.Phase != phase || o.JobCompleting {
		r.t.Errorf("%s: phase %s, jobCompleting %v; want %s, false", what, o.Phase, o.JobCompleting, phase)
	}
	c := a.GetCondition(v1alpha1.ConditionJobCompleting)
	if c.Status != corev1.ConditionFalse || string(c.Reason) != "NotCompleting" || c.Message != "" {
		r.t.Errorf("%s: condition JobCompleting %s %q %q, want False NotCompleting with no message", what, c.Status, c.Reason, c.Message)
	}
}

var bothShapes = []struct {
	name string
	mode fakeslurm.NotListedMode
}{
	{"Helios shape", fakeslurm.NotListedHelios},
	{"Athena shape", fakeslurm.NotListedAthena},
}

// CANCELLED, FAILED, TIMEOUT and COMPLETED, each with COMPLETING, give their
// phase at once, with jobCompleting true and the JobCompleting condition whose
// "since" is the listing's end_time (12:02:30). While the flag stands the
// listing shows the job in both shapes; the shapes differ only once it clears.
func TestCompletingJobGivesItsPhaseAtOnceWithJobCompleting(t *testing.T) {
	for _, shape := range bothShapes {
		for _, c := range []struct {
			state string
			phase v1alpha1.AllocationPhase
		}{
			{fakeslurm.Cancelled, v1alpha1.AllocationEnded},
			{fakeslurm.Failed, v1alpha1.AllocationFailed},
			{fakeslurm.Timeout, v1alpha1.AllocationEnded},
			{fakeslurm.Completed, v1alpha1.AllocationEnded},
		} {
			t.Run(shape.name+"/"+c.state, func(t *testing.T) {
				r := newCGRig(t, shape.mode, 10*time.Minute)
				r.endCompleting(c.state, at(12, 2, 30))
				r.wantCompleting("after the end", c.phase, c.state, "2026-10-30T12:02:30Z")
				// Long past MinJobAge, the flag still stands and so does the evidence.
				r.clock.Advance(time.Hour)
				r.poll("alloc-a", 1)
				r.wantCompleting("an hour later", c.phase, c.state, "2026-10-30T12:02:30Z")
			})
		}
	}
}

// A requeued job whose state still carries the flag, ["PENDING","COMPLETING"],
// stays Failed by the requeue rule, with the flag recorded: completing is the
// flag, whatever the base state. The fake Slurm cannot list a pending job
// with the flag, so the platform in between adds it to a requeued job, as the
// parser test's fixture does.
func TestRequeuedCompletingJobStaysFailedAndIsCompleting(t *testing.T) {
	r := newCGRig(t, fakeslurm.NotListedHelios, 10*time.Minute, withCompletingFlagOnRequeued())
	if err := r.sl.Requeue(r.n); err != nil {
		t.Fatal(err)
	}
	r.poll("alloc-a", 1)
	a := r.get("alloc-a")
	o := a.Status.AtProvider
	if o.Phase != v1alpha1.AllocationFailed || o.Reason != v1alpha1.ReasonRequeued || !o.Requeued || !o.JobCompleting {
		t.Errorf("phase %s reason %q requeued %v jobCompleting %v; want Failed, Requeued, true, true", o.Phase, o.Reason, o.Requeued, o.JobCompleting)
	}
	if c := a.GetCondition(v1alpha1.ConditionJobCompleting); c.Status != corev1.ConditionTrue {
		t.Errorf("condition JobCompleting %s, want True", c.Status)
	}
}

// The flag clears when a successful listing shows the job without it. The
// Athena shape lists the ended job in its end state, no flag, until MinJobAge.
func TestFlagClearsWhenAListingShowsTheJobWithoutIt(t *testing.T) {
	r := newCGRig(t, fakeslurm.NotListedAthena, 10*time.Minute)
	r.endCompleting(fakeslurm.Cancelled, at(12, 2, 30))
	r.wantCompleting("holding", v1alpha1.AllocationEnded, "CANCELLED", "2026-10-30T12:02:30Z")
	if err := r.sl.HoldCompleting(r.n, false); err != nil {
		t.Fatal(err)
	}
	r.poll("alloc-a", 1) // 12:03:50, inside MinJobAge: the listing shows [CANCELLED]
	r.wantCleared("listing without the flag", v1alpha1.AllocationEnded)
}

// The flag clears when the job is read from accounting, which never carries
// it. The Helios shape drops the job from the listing the moment the flag
// clears, so the next read is accounting's.
func TestFlagClearsWhenTheJobIsReadFromAccounting(t *testing.T) {
	r := newCGRig(t, fakeslurm.NotListedHelios, 10*time.Minute)
	r.endCompleting(fakeslurm.Cancelled, at(12, 2, 30))
	r.wantCompleting("holding", v1alpha1.AllocationEnded, "CANCELLED", "2026-10-30T12:02:30Z")
	if err := r.sl.HoldCompleting(r.n, false); err != nil {
		t.Fatal(err)
	}
	r.poll("alloc-a", 1)
	r.wantCleared("read from accounting", v1alpha1.AllocationEnded)
	if r.get("alloc-a").Status.AtProvider.AbsentSince != nil {
		t.Error("absentSince is set: accounting showed the job, so it was not absent")
	}
}

// The flag clears when no read lists the job: the listing dropped it (Helios)
// and accounting has not got it. The phase is kept until lostGrace, as for any
// absent job.
func TestFlagClearsWhenNoReadListsTheJob(t *testing.T) {
	r := newCGRig(t, fakeslurm.NotListedHelios, 10*time.Minute)
	r.endCompleting(fakeslurm.Cancelled, at(12, 2, 30))
	if err := r.sl.HoldCompleting(r.n, false); err != nil {
		t.Fatal(err)
	}
	if err := r.sl.AcctHide(r.n); err != nil {
		t.Fatal(err)
	}
	r.poll("alloc-a", 1)
	r.wantCleared("listed by neither read", v1alpha1.AllocationEnded)
	if r.get("alloc-a").Status.AtProvider.AbsentSince == nil {
		t.Error("absentSince is unset: neither read showed the job")
	}
}

// A failed read keeps the last evidence: with the flag gone from the platform
// (a successful read would clear it), a failing listing leaves jobCompleting and
// its condition as they were; the next successful read clears them.
func TestFailedReadKeepsTheCompletingEvidence(t *testing.T) {
	f := &lineFaults{}
	r := newCGRig(t, fakeslurm.NotListedAthena, 10*time.Minute, withLineFaults(f))
	r.endCompleting(fakeslurm.Cancelled, at(12, 2, 30))
	if err := r.sl.HoldCompleting(r.n, false); err != nil {
		t.Fatal(err)
	}
	f.fail("squeue", -1, slurm.ErrTransport)
	r.poll("alloc-a", 2)
	if f.failed() == 0 {
		t.Fatal("no squeue was failed: the test proves nothing")
	}
	r.wantCompleting("listing failing", v1alpha1.AllocationEnded, "CANCELLED", "2026-10-30T12:02:30Z")
	f.clear()
	r.poll("alloc-a", 1)
	r.wantCleared("listing back", v1alpha1.AllocationEnded)
}

// A stored status that still shows the flag is read again, however final the
// controller thinks the job is: a status write that failed after an accounting
// read must not leave jobCompleting true for good.
func TestAJobWhoseStoredStatusIsCompletingIsNotFinal(t *testing.T) {
	e := &external{s: &shared{}}
	e.s.finals.Store("1001", true)
	end := at(12, 2, 30)
	stored := &v1alpha1.AllocationObservation{EndTime: mt(end), JobCompleting: true}
	if e.final("1001", stored) {
		t.Error("final with a stored status that still has jobCompleting, want false")
	}
	stored.JobCompleting = false
	if !e.final("1001", stored) {
		t.Error("control: not final with a stored status that has an end and no flag, so the test above proves nothing")
	}
}

// flagOnRequeued is a Platform that lists a requeued job with the COMPLETING
// flag after its state, as ["PENDING","COMPLETING"]: Slurm's listing of a job
// requeued while it completed, which the fake Slurm cannot produce.
type flagOnRequeued struct{ Platform }

func (f flagOnRequeued) flag(j provider.Job) provider.Job {
	if j.Requeued && j.Source == provider.SourceQueue {
		j.State = append(append([]string(nil), j.State...), provider.FlagCompleting)
		j.Completing = true
	}
	return j
}

func (f flagOnRequeued) Observe(ctx context.Context, id string) (provider.Job, bool, error) {
	j, found, err := f.Platform.Observe(ctx, id)
	return f.flag(j), found, err
}

func (f flagOnRequeued) ListByTag(ctx context.Context, tag string) ([]provider.Job, error) {
	jobs, err := f.Platform.ListByTag(ctx, tag)
	for i := range jobs {
		jobs[i] = f.flag(jobs[i])
	}
	return jobs, err
}

func (f flagOnRequeued) FindByTag(ctx context.Context, tag string, since time.Time) ([]provider.Job, error) {
	jobs, err := f.Platform.FindByTag(ctx, tag, since)
	for i := range jobs {
		jobs[i] = f.flag(jobs[i])
	}
	return jobs, err
}

func withCompletingFlagOnRequeued() realOption {
	return func(e *realEnv) {
		inner := e.opts.NewPlatform
		e.opts.NewPlatform = func(v slurm.Validated) (Platform, io.Closer, error) {
			p, c, err := inner(v)
			return flagOnRequeued{p}, c, err
		}
	}
}

// Clearance is a listing without the flag, or a listing that no longer shows the
// job; an accounting read alone is not (science agent, 1 Nov 2026, after spike
// S7: sacct said FAILED while squeue still showed COMPLETING). Accounting here
// says FAILED, with the elapsed time and the counts, for a job whose listing
// still shows ["FAILED","COMPLETING"], and the controller reads accounting for
// it (the job ended between two polls, so its first read finds it ended and
// takes its CPUs and GPUs from accounting). The job stays live: jobCompleting
// stays true, the finalizer stays, the files stay, no accounting record is
// written, however long and however often it is read; the flag's own clearing
// then releases everything, once.
func TestAccountingSayingTerminalDoesNotClearTheFlagWhileTheListingShowsIt(t *testing.T) {
	for _, shape := range bothShapes {
		t.Run(shape.name, func(t *testing.T) {
			r := startCGRig(t, shape.mode, 10*time.Minute)
			r.clock.Set(at(12, 2, 30))
			endJob(t, r.sl, r.id, fakeslurm.Failed)
			if err := r.sl.HoldCompleting(r.n, true); err != nil {
				t.Fatal(err)
			}
			// The precondition: accounting does say FAILED, ended, with its hours.
			acct, found, err := r.batch.ReadAccounting(r.ctx, r.id)
			if err != nil || !found || acct.Phase != v1alpha1.AllocationFailed || acct.EndTime == nil || acct.Completing || acct.Accounting == nil || acct.Accounting.Elapsed == nil {
				t.Fatalf("precondition: accounting gave %+v, found %v, %v; want FAILED with an end time and its elapsed time", acct, found, err)
			}
			reads := r.sl.Count("sacct")
			for i := 0; i < 3; i++ {
				r.clock.Advance(10 * time.Minute) // past the accounting refresh and MinJobAge
				r.run("alloc-a", 1)
				r.wantCompleting("accounting says FAILED", v1alpha1.AllocationFailed, "FAILED", "2026-10-30T12:02:30Z")
			}
			if r.sl.Count("sacct") == reads {
				t.Fatal("the controller read no accounting while the job completed: the test proves nothing")
			}
			head := head(r.realEnv, "alloc-a", "1001")
			r.del("alloc-a")
			for i := 0; i < 3; i++ {
				r.clock.Advance(10 * time.Minute)
				r.run("alloc-a", 1)
				if r.gone("alloc-a") {
					t.Fatalf("pass %d: the Allocation is gone while the listing still shows the flag", i)
				}
				if _, ok := r.record("alloc-a"); ok {
					t.Fatalf("pass %d: the accounting record was written while the listing shows the flag", i)
				}
				if n := r.sl.Count("rm"); n != 0 {
					t.Fatalf("pass %d: %d file removals while the listing shows the flag, want 0", i, n)
				}
			}
			// The listing stops showing the flag: everything follows, once.
			if err := r.sl.HoldCompleting(r.n, false); err != nil {
				t.Fatal(err)
			}
			r.clock.Advance(10 * time.Second)
			r.run("alloc-a", 3)
			if !r.gone("alloc-a") {
				t.Fatal("the Allocation is still there after the flag cleared")
			}
			if n := r.sl.Count("rm"); n != 1 {
				t.Errorf("%d file removals after the flag cleared, want 1", n)
			}
			r.wantRecord("alloc-a", head+`,"allocatedCPUs":8,"allocatedGPUs":1,"elapsedSeconds":90,`+
				`"startTime":"2026-10-30T12:01:00Z","endTime":"2026-10-30T12:02:30Z","basis":"allocated"}`)
		})
	}
}
