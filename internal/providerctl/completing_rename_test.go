package providerctl

// Queue evidence wins by job ID (HBEP-4, Phases, A completing job: "No other
// accounting read clears the flag"; Astra B1 on PR 217). A recorded job that
// the listing shows under a changed name, with COMPLETING, while accounting
// still has the tag and a terminal state, must stay live on deletion: the tag
// lookup skips the listing's row because its name differs, and accounting's
// record must not stand in for it.

import (
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
)

// minJobAgeFor is the MinJobAge of a deletion fixture: the Helios shape drops a
// job from the listing the moment the flag clears, so 10 minutes will do; the
// Athena shape keeps it listed, without the flag, until MinJobAge, so the age
// must outlast every pass of the test or the job is purged first and the Athena
// route is the Helios one (HBEP-4, Test plan: "A fixture that runs only one
// shape ... proves nothing"; Opus O1 on PR 217).
func minJobAgeFor(mode fakeslurm.NotListedMode) time.Duration {
	if mode == fakeslurm.NotListedAthena {
		return 2 * time.Hour
	}
	return 10 * time.Minute
}

func TestAccountingCannotHideTheFlagOfARenamedRecordedJob(t *testing.T) {
	const ended = `,"allocatedCPUs":8,"allocatedGPUs":1,"elapsedSeconds":90,` +
		`"startTime":"2026-10-30T12:01:00Z","endTime":"2026-10-30T12:02:30Z","basis":"allocated"}`
	for _, shape := range bothShapes {
		t.Run(shape.name, func(t *testing.T) {
			r := newCGRig(t, shape.mode, minJobAgeFor(shape.mode))
			r.endCompleting(fakeslurm.Cancelled, at(12, 2, 30))
			r.wantCompleting("before the rename", v1alpha1.AllocationEnded, "CANCELLED", "2026-10-30T12:02:30Z")
			tag := jobTag(r.realEnv, "alloc-a")
			head := head(r.realEnv, "alloc-a", "1001")
			// The listing now gives the job another name; accounting keeps the
			// tag. The preconditions read both through the real Batch: the
			// listing says COMPLETING under the new name, accounting says
			// CANCELLED under the tag and no flag, so FindByTag's accounting half
			// finds a job that its listing half does not.
			if err := r.sl.RenameListed(r.n, "ExampleRenamed"); err != nil {
				t.Fatal(err)
			}
			q, listed, err := r.batch.QueueJob(r.ctx, r.id)
			if err != nil || !listed || q.Name != "ExampleRenamed" || !q.Completing {
				t.Fatalf("precondition: the listing gives %+v, listed %v, %v; want ExampleRenamed with the flag", q, listed, err)
			}
			acct, found, err := r.batch.ReadAccounting(r.ctx, r.id)
			if err != nil || !found || acct.Name != tag || acct.EndTime == nil || acct.Completing {
				t.Fatalf("precondition: accounting gives %+v, found %v, %v; want the tag, an end time and no flag", acct, found, err)
			}
			r.del("alloc-a")
			for i := 0; i < 3; i++ {
				r.clock.Advance(10 * time.Minute)
				r.run("alloc-a", 2)
				if r.gone("alloc-a") {
					t.Fatalf("pass %d: the Allocation is gone while the listing shows its job completing under another name", i)
				}
				if _, ok := r.record("alloc-a"); ok {
					t.Fatalf("pass %d: the accounting record was written while the listing shows the flag", i)
				}
				if n := r.sl.Count("rm"); n != 0 {
					t.Fatalf("pass %d: %d file removals while the listing shows the flag, want 0", i, n)
				}
				if !r.get("alloc-a").Status.AtProvider.JobCompleting {
					t.Fatalf("pass %d: jobCompleting is false while the job completes", i)
				}
			}
			// The flag clears: the listing stops showing it (Helios drops the
			// job, Athena lists it without the flag), and everything follows once.
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
			r.wantRecord("alloc-a", head+ended)
		})
	}
}
