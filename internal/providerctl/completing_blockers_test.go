package providerctl

// Which job's COMPLETING flag the Allocation shows and obeys (HBEP-4, Phases,
// A completing job; Astra B2 and B4 on PR 217).
//
// B2: a deleting Allocation stays live while ANY job with its tag completes, not
// only the recorded one, and its status says so, naming that job, so the
// lifecycle controller's slot count (it reads status.atProvider.jobCompleting)
// keeps counting the Allocation as the finalizer keeps it.
//
// B4: accounting settles a job (its final state is cached, and the job is not
// read again), but accounting never carries the flag, so a successful listing
// that shows the flag again decides: jobCompleting turns true again and the
// slot is held again.

import (
	"strconv"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
)

// A duplicate with the tag, CANCELLED and COMPLETING, outlives the recorded job.
// The deleted Allocation keeps its finalizer, its files and its record, shows
// jobCompleting true with a JobCompleting condition that names the duplicate and
// the recorded job, and, as a job completing under the recorded one would, goes
// when the flag clears.
func TestACompletingDuplicateHoldsTheDeletedAllocationAndItsSlot(t *testing.T) {
	for _, shape := range bothShapes {
		t.Run(shape.name, func(t *testing.T) {
			r := newCGRig(t, shape.mode, minJobAgeFor(shape.mode))
			tag := jobTag(r.realEnv, "alloc-a")
			dup := r.sl.OutsideJob("user-a", tag, gpuPart, "", 1)
			if dup != 1002 {
				t.Fatalf("precondition: the duplicate is job %d, want 1002", dup)
			}
			if err := r.sl.Start(dup); err != nil {
				t.Fatal(err)
			}
			r.clock.Set(at(12, 2, 30))
			endJob(t, r.sl, r.id, fakeslurm.Cancelled) // the recorded job ends, flag-free
			if err := r.sl.End(dup, fakeslurm.Cancelled); err != nil {
				t.Fatal(err)
			}
			if err := r.sl.HoldCompleting(dup, true); err != nil {
				t.Fatal(err)
			}
			r.poll("alloc-a", 1)
			if o := r.get("alloc-a").Status.AtProvider; o.Phase != v1alpha1.AllocationEnded || o.JobCompleting {
				t.Fatalf("precondition: phase %s, jobCompleting %v; want Ended, false: the recorded job is not completing", o.Phase, o.JobCompleting)
			}
			r.del("alloc-a")
			const want = "job 1002 is completing in state CANCELLED since 2026-10-30T12:02:30Z; it carries the tag but is not the recorded job 1001"
			for i := 0; i < 3; i++ {
				r.clock.Advance(10 * time.Minute)
				r.run("alloc-a", 2)
				if r.gone("alloc-a") {
					t.Fatalf("pass %d: the Allocation is gone while a job with its tag is completing", i)
				}
				if _, ok := r.record("alloc-a"); ok {
					t.Fatalf("pass %d: the accounting record was written while a job with the tag is completing", i)
				}
				if n := r.sl.Count("rm"); n != 0 {
					t.Fatalf("pass %d: %d file removals while a job with the tag is completing, want 0", i, n)
				}
				a := r.get("alloc-a")
				c := a.GetCondition(v1alpha1.ConditionJobCompleting)
				if o := a.Status.AtProvider; o.Phase != v1alpha1.AllocationEnded || !o.JobCompleting {
					t.Fatalf("pass %d: phase %s, jobCompleting %v; want Ended, true: the slot count reads this", i, o.Phase, o.JobCompleting)
				}
				if c.Status != corev1.ConditionTrue || string(c.Reason) != "Completing" || c.Message != want {
					t.Fatalf("pass %d: condition JobCompleting %s %q %q, want True Completing %q", i, c.Status, c.Reason, c.Message, want)
				}
			}
			if err := r.sl.HoldCompleting(dup, false); err != nil {
				t.Fatal(err)
			}
			r.clock.Advance(10 * time.Second)
			r.run("alloc-a", 3)
			if !r.gone("alloc-a") {
				t.Fatal("the Allocation is still there after the duplicate's flag cleared")
			}
			if n := r.sl.Count("rm"); n != 1 {
				t.Errorf("%d file removals after the flag cleared, want 1", n)
			}
			// One record, from the recorded job's final state (8 CPUs, 1 GPU, 90 s
			// from 12:01:00 to 12:02:30), naming both jobs of the tag.
			r.wantRecord("alloc-a", head(r.realEnv, "alloc-a", "1001", "1002")+`,"allocatedCPUs":8,"allocatedGPUs":1,"elapsedSeconds":90,`+
				`"startTime":"2026-10-30T12:01:00Z","endTime":"2026-10-30T12:02:30Z","basis":"allocated"}`)
		})
	}
}

// A held duplicate on the live path is cancelled before its hold is lifted. It
// never ran, so it is not COMPLETING, and it neither holds the slot nor sets
// jobCompleting (science agent, 2 Nov 2026: the deleting pass is enough for
// alpha; a duplicate that ran needs fallback mode, which no alpha platform
// uses). The cancel is checked first, so the fixture proves it happened.
func TestAHeldDuplicateCancelledOnTheLivePathHoldsNoSlot(t *testing.T) {
	r := newCGRig(t, fakeslurm.NotListedHelios, 10*time.Minute)
	r.clock.Set(at(12, 2, 30))
	endJob(t, r.sl, r.id, fakeslurm.Cancelled)
	r.poll("alloc-a", 2) // Ended, read from accounting
	tag := jobTag(r.realEnv, "alloc-a")
	r.extraSbatch(tag, true) // a late job of an older attempt, held
	r.poll("alloc-a", 1)
	extra := 0
	for _, j := range r.sl.Jobs() {
		if strconv.Itoa(j.ID) != r.id && j.Name == tag {
			extra = j.ID
			if j.State != fakeslurm.Cancelled {
				t.Fatalf("precondition: the held duplicate %d is %s, want CANCELLED", j.ID, j.State)
			}
		}
	}
	if extra == 0 {
		t.Fatal("precondition: no duplicate job was made")
	}
	a := r.get("alloc-a")
	o := a.Status.AtProvider
	if o.Phase != v1alpha1.AllocationEnded || o.JobCompleting {
		t.Errorf("phase %s, jobCompleting %v; want Ended, false: a cancelled held duplicate holds no slot", o.Phase, o.JobCompleting)
	}
	// crossplane reads a condition that was never set as Unknown with no reason.
	if c := a.GetCondition(v1alpha1.ConditionJobCompleting); c.Status != corev1.ConditionUnknown || c.Reason != "" {
		t.Errorf("condition JobCompleting %s %q %q, want it never set: nothing completed", c.Status, c.Reason, c.Message)
	}
}

// A flag that comes back after accounting settled the job. The job's flag
// clears, accounting reads it as final and the controller stops reading it
// (the cached final state, trusted: no sacct call in the next pass). Then a
// successful listing shows the job with the flag again: jobCompleting is true
// again, the JobCompleting condition is True, and the slot count (phase Ended
// with the flag) holds the Allocation, until the flag clears once more.
func TestAFlagThatComesBackAfterAccountingClearedItIsSeenAgain(t *testing.T) {
	for _, shape := range bothShapes {
		t.Run(shape.name, func(t *testing.T) {
			r := newCGRig(t, shape.mode, 10*time.Minute)
			r.endCompleting(fakeslurm.Cancelled, at(12, 2, 30))
			if err := r.sl.HoldCompleting(r.n, false); err != nil {
				t.Fatal(err)
			}
			r.clock.Advance(20 * time.Minute) // past MinJobAge: the listing no longer shows the job
			r.run("alloc-a", 1)
			r.wantCleared("after accounting settled it", v1alpha1.AllocationEnded)
			// The preconditions: the final state is cached and trusted, so the next
			// pass reads no accounting. Without them the recheck proves nothing.
			if !(&external{s: r.ctl.shared}).final(r.id, &r.get("alloc-a").Status.AtProvider) {
				t.Fatal("precondition: the job is not final after accounting settled it")
			}
			reads := r.sl.Count("sacct")
			r.poll("alloc-a", 1)
			if n := r.sl.Count("sacct") - reads; n != 0 {
				t.Fatalf("precondition: %d accounting reads in a pass over a final job, want 0", n)
			}
			// The flag comes back: the listing shows the job with it again.
			if err := r.sl.HoldCompleting(r.n, true); err != nil {
				t.Fatal(err)
			}
			r.poll("alloc-a", 1)
			r.wantCompleting("the flag back", v1alpha1.AllocationEnded, "CANCELLED", "2026-10-30T12:02:30Z")
			// And it clears again, by accounting.
			if err := r.sl.HoldCompleting(r.n, false); err != nil {
				t.Fatal(err)
			}
			r.clock.Advance(20 * time.Minute)
			r.run("alloc-a", 1)
			r.wantCleared("cleared once more", v1alpha1.AllocationEnded)
		})
	}
}
