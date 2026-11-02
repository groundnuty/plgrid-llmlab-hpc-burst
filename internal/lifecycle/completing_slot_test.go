package lifecycle

// A COMPLETING job holds its Allocation's slot (HBEP-7, hbep/0007-lifecycle-
// controller/README.md on main, Provisioning, Per-user limits, lines 310 and
// 312; HBEP-4, Core operations, Phases, A completing job, lines 208-233;
// science-agent ruling for #198, 1 Nov 2026): the slot count lists an Allocation whose phase is Ended while its
// job's status says jobCompleting, until the flag clears, and a one-slot
// successor (no-overlap mode, Handover) is created only after that, once.

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// listProviderAllocations counts, for the Provider, an Allocation that has not
// ended and an Ended one whose job is still completing; an Ended one that is not
// completing, and another Provider's, are not counted. Expected sets are
// written by hand.
func TestListProviderAllocationsCountsACompletingEndedAllocation(t *testing.T) {
	rg := newRig(t)
	ctx := context.Background()
	mk := func(name string, phase v1alpha1.AllocationPhase, completing bool, provider string) {
		t.Helper()
		a := otherAllocation(name)
		a.Namespace = rigNS
		a.Spec.ProviderConfigReference = providerRef(provider)
		rg.create(a)
		a.Status.AtProvider.Phase, a.Status.AtProvider.JobCompleting = phase, completing
		if err := rg.c.Status().Update(ctx, a); err != nil {
			t.Fatal(err)
		}
	}
	mk("running", v1alpha1.AllocationRunning, false, rigProvider)
	mk("ended", v1alpha1.AllocationEnded, false, rigProvider)
	mk("ended-completing", v1alpha1.AllocationEnded, true, rigProvider)
	mk("failed-completing", v1alpha1.AllocationFailed, true, rigProvider)
	mk("other-providers", v1alpha1.AllocationEnded, true, "example-other-provider")
	seen, listed, err := listProviderAllocations(ctx, rg.c, rigProvider)
	if err != nil {
		t.Fatal(err)
	}
	keys := func(m map[string]bool) []string {
		var out []string
		for k := range m {
			out = append(out, k)
		}
		sort.Strings(out)
		return out
	}
	wantSeen := []string{rigNS + "/ended", rigNS + "/ended-completing", rigNS + "/failed-completing", rigNS + "/running"}
	wantListed := []string{rigNS + "/ended-completing", rigNS + "/failed-completing", rigNS + "/running"}
	if got := keys(seen); len(got) != len(wantSeen) || got[0] != wantSeen[0] || got[1] != wantSeen[1] || got[2] != wantSeen[2] || got[3] != wantSeen[3] {
		t.Errorf("seen %v, want %v", got, wantSeen)
	}
	if got := keys(listed); len(got) != len(wantListed) || got[0] != wantListed[0] || got[1] != wantListed[1] || got[2] != wantListed[2] {
		t.Errorf("counted %v, want %v: the Ended one that completes counts, the Ended one that does not is free", got, wantListed)
	}
}

// A completing predecessor holds the slot (HBEP-7, Test plan, line 593; and the
// one-slot path, line 592). The one-slot path (TestNoOverlapOneSlotPath's rig):
// the primary's job is cancelled from outside at 09:57:00, after its stream
// ended, and its epilog keeps COMPLETING for 20 minutes, so the flag clears at
// 10:17:00, past MinJobAge. Its Allocation is Ended with jobCompleting from
// then, and its idle release at about 10:01 deletes it, its finalizer staying.
// No successor Allocation exists before 10:17:00, however many passes run; once
// the flag has cleared, exactly one is created, and no second one in the hour
// after. It runs in HBEP-4's two listing shapes: Helios's, where the job leaves
// the listing when the flag clears (MinJobAge 10 s, so it reads from accounting),
// and Athena's, where it stays listed in its base state without the flag until
// MinJobAge (2 h here). The fake frees its own submit count when it sets the
// flag, as Slurm does, so only the controller's count can hold the successor.
func TestNoOverlapOneSlotSuccessorWaitsForTheFlagToClear(t *testing.T) {
	for _, shape := range []struct {
		name   string
		minAge time.Duration
	}{{"Helios shape", 10 * time.Second}, {"Athena shape", 2 * time.Hour}} {
		t.Run(shape.name, func(t *testing.T) { oneSlotSuccessorWaits(t, shape.minAge) })
	}
}

func oneSlotSuccessorWaits(t *testing.T, minAge time.Duration) {
	rg := noOverlapRig(t, oneSlotOpts(withBatchConfig(func(c *pfake.Config) {
		c.MaxSubmitPerUser, c.MaxGPUsPerUser, c.MinJobAge = 1, 1, minAge
	}))...)
	rg.batch.Enqueue(pfake.Script{Epilog: 20 * time.Minute}) // the primary's job; the successor's gets the default
	p := noOverlapStart(rg)
	pAlloc := p.Spec.AllocationRef.Name
	pa, ok := rg.allocation(pAlloc)
	if !ok {
		t.Fatal("no Allocation for the primary")
	}
	jobID := pa.Annotations[v1alpha1.AnnotationJobID]
	hoStepTo(rg, hoT(9, 57, 0), 10*time.Second, nil)
	rg.batch.CancelExternally(jobID)
	clears := hoT(10, 17, 0)

	var first string
	var firstAt time.Time
	sawFlag := false
	each := func() {
		now := rg.clock.Now()
		job, _ := rg.batch.Job(jobID)
		if job.Completing {
			sawFlag = true
			if now.After(hoT(9, 58, 0)) {
				if a, alive := rg.allocation(pAlloc); alive && !a.Status.AtProvider.JobCompleting && a.DeletionTimestamp.IsZero() {
					t.Fatalf("at %s the primary's job completes but its Allocation shows jobCompleting false", now.Format(time.TimeOnly))
				}
			}
		}
		var succ []string
		for _, a := range rg.allAllocations() {
			if a.Name != pAlloc {
				succ = append(succ, a.Name)
			}
		}
		switch {
		case len(succ) == 0:
		case first == "":
			first, firstAt = succ[0], now
			if job.Completing || job.EndTime == nil {
				t.Fatalf("at %s the successor %s was created while the primary's job is completing (flag %v, end %v)", now.Format(time.TimeOnly), first, job.Completing, job.EndTime)
			}
		case len(succ) != 1 || succ[0] != first:
			t.Fatalf("at %s the Allocations besides the primary's are %v, want only %s", now.Format(time.TimeOnly), succ, first)
		}
	}
	hoStepTo(rg, hoT(10, 40, 0), 10*time.Second, each)
	if !sawFlag {
		t.Fatal("the primary's job was never seen completing: the test proves nothing")
	}
	if first == "" {
		t.Fatal("no successor Allocation was created once the flag had cleared")
	}
	if firstAt.Before(clears) {
		t.Fatalf("the successor was created at %s, before the flag cleared at %s", firstAt.Format(time.TimeOnly), clears.Format(time.TimeOnly))
	}
	hoStepTo(rg, hoT(11, 30, 0), time.Minute, each)
	if n := rg.submissions(); n != 2 {
		t.Errorf("%d sbatch calls, want 2: the primary's and the successor's, once", n)
	}
}
