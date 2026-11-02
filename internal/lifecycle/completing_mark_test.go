package lifecycle

// The queued-successor mark and a COMPLETING primary (issue #195's mark, #198's
// flag; HBEP-7, Handover, No-overlap mode, README lines 464 to 466): an
// Allocation for a no-overlap successor is marked when the primary's Allocation
// on the same Provider still holds its slot as the slot count reads it
// (holdsSlot). An Ended primary whose job is still COMPLETING holds it, so the
// successor created then is marked, and does not record its own wait. The
// timeline is nooverlap_test.go's queued rig; the primary's job is cancelled from
// outside at 09:57:00, after its stream has ended, and its epilog keeps
// COMPLETING for 3 hours.

import (
	"errors"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

func TestAnEndedPrimaryWhoseJobIsCompletingStillMarksItsSuccessor(t *testing.T) {
	log := &creationLog{}
	rg := noOverlapQueuedRig(t, withInterceptors(log.funcs()))
	rg.batch.Enqueue(pfake.Script{Epilog: 3 * time.Hour}) // the primary's job; the successor's gets the default
	p := noOverlapStart(rg)
	pAlloc := p.Spec.AllocationRef.Name
	pa, ok := rg.allocation(pAlloc)
	if !ok {
		t.Fatal("no Allocation for the primary")
	}
	jobID := pa.Annotations[v1alpha1.AnnotationJobID]
	hoStepTo(rg, hoT(9, 19, 0), 5*time.Minute, nil)
	log.failAllocations(true)
	succ := cutShort(t, rg, log, p)

	// Creates keep failing while the primary's stream ends (09:56) and its job is cancelled from
	// outside at 09:57:00, which Ends its Allocation with the flag set.
	step := func(until func() bool, deadline time.Time, what string) {
		t.Helper()
		for !until() {
			if !rg.clock.Now().Before(deadline) {
				t.Fatalf("%s was not reached by %s", what, deadline.Format(time.TimeOnly))
			}
			rg.clock.Advance(10 * time.Second)
			if err := stepTolerant(rg); err != nil && !errors.Is(err, errHOKilled) {
				t.Fatalf("reconcile at %s: %v", rg.clock.Now().Format(time.TimeOnly), err)
			}
		}
	}
	step(func() bool { return !rg.clock.Now().Before(hoT(9, 57, 0)) }, hoT(9, 57, 0), "09:57:00")
	rg.batch.CancelExternally(jobID)
	step(func() bool {
		a, alive := rg.allocation(pAlloc)
		return alive && a.Status.AtProvider.Phase == v1alpha1.AllocationEnded && a.Status.AtProvider.JobCompleting
	}, hoT(10, 0, 0), "the primary's Allocation Ended with jobCompleting")
	if n := len(log.allocations()); n != 1 {
		t.Fatalf("%d Allocations created while creates were failing, want the primary's alone", n)
	}

	// Creates work again; the successor's Allocation is created while the primary is Ended and
	// completing, and it is marked.
	log.failAllocations(false)
	step(func() bool { return len(log.allocations()) == 2 }, hoT(10, 30, 0), "the successor's Allocation")
	if a, alive := rg.allocation(pAlloc); !alive || a.Status.AtProvider.Phase != v1alpha1.AllocationEnded || !a.Status.AtProvider.JobCompleting {
		t.Fatalf("the primary's Allocation %+v when the successor's was created, want it Ended and completing", a.Status.AtProvider)
	}
	made := log.allocations()[1]
	if made.name != succ.Spec.AllocationRef.Name {
		t.Fatalf("the Allocation created second is %s, want the successor's %s", made.name, succ.Spec.AllocationRef.Name)
	}
	if made.annotations[v1alpha1.AnnotationQueuedSuccessor] == "" {
		t.Errorf("the successor's Allocation %+v is not marked, although the primary's Ended Allocation still holds its slot while its job completes", made)
	}
}
