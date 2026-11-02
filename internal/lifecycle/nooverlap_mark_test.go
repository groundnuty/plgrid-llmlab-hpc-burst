package lifecycle

// The queued-successor mark is judged when an Allocation is created (issue #195; HBEP-7,
// Handover, No-overlap mode, README lines 464 to 466; Test plan, lines 601 and 602): an
// Allocation for a no-overlap successor of a primary, or a retry of one, is marked when that
// primary's Allocation on the same Provider still holds its slot, and not otherwise. These
// tests cover the creations that the first successor's does not: the Allocation made after a
// pass cut short between the successor's Instance and its Allocation, the one made after the
// primary has left, and step 7's retry of a successor that failed while the primary still holds
// the GPU. The timeline is nooverlap_test.go's (see nooverlap_waits_test.go).

import (
	"context"
	"errors"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// cutShort steps the deployment alone until an Allocation create fails (the log's failure
// armed or on), returning the successor's Instance, which exists without its Allocation.
func cutShort(t *testing.T, rg *rig, log *creationLog, p *v1alpha1.Instance) v1alpha1.Instance {
	t.Helper()
	for log.failedCount() == 0 {
		if !rg.clock.Now().Before(hoT(9, 25, 0)) {
			t.Fatal("the controller never reached the successor's Allocation by 09:25:00")
		}
		rg.clock.Advance(10 * time.Second)
		if err := stepTolerant(rg); err != nil && !errors.Is(err, errHOKilled) {
			t.Fatalf("reconcile at %s: %v", rg.clock.Now().Format(time.TimeOnly), err)
		}
	}
	reps := hoRepl(rg, p.Name)
	if len(reps) != 1 {
		t.Fatalf("after the cut-short pass the successors are %+v, want one Instance", reps)
	}
	if _, ok := rg.allocation(reps[0].Spec.AllocationRef.Name); ok {
		t.Fatalf("the successor's Allocation %s exists although its creation failed", reps[0].Spec.AllocationRef.Name)
	}
	if n := len(log.allocations()); n != 1 {
		t.Fatalf("%d Allocations created, want the primary's alone", n)
	}
	return reps[0]
}

// TestAQueuedSuccessorsAllocationIsMarkedWhenThePassWasCutShortBetweenTheTwoCreates: the Instance
// is the durable intent and is created before its Allocation. A pass that stops between the two
// leaves a pending successor, and the Allocation that a later pass creates (ensureAllocation) is
// marked in its create request, because at that moment the primary's Allocation still holds its
// slot (README line 601). A fixture that never re-creates an Allocation after a cut-short create
// would prove nothing here, so the test shows the first create failed and a second one came.
func TestAQueuedSuccessorsAllocationIsMarkedWhenThePassWasCutShortBetweenTheTwoCreates(t *testing.T) {
	log := &creationLog{}
	rg := noOverlapQueuedRig(t, withInterceptors(log.funcs()))
	p := noOverlapStart(rg)
	pAlloc := p.Spec.AllocationRef.Name
	hoStepTo(rg, hoT(9, 19, 0), 5*time.Minute, nil)
	log.armAllocFailure() // the successor's Allocation is the next one created
	succ := cutShort(t, rg, log, p)

	rg.restartController()
	rg.until(5*time.Minute, 10*time.Second, "the successor's Allocation is created", func() bool { return len(log.allocations()) == 2 })
	if pa, ok := rg.allocation(pAlloc); !ok || pa.Status.AtProvider.Phase != v1alpha1.AllocationRunning {
		t.Fatalf("the primary's Allocation %+v when the successor's was re-created, want it live", pa)
	}
	made := log.allocations()[1]
	if made.name != succ.Spec.AllocationRef.Name || made.annotations[v1alpha1.AnnotationQueuedSuccessor] == "" {
		t.Fatalf("the successor's Allocation %+v, want %s, marked in its create request", made, succ.Spec.AllocationRef.Name)
	}
	if m := log.instances()[1].annotations[v1alpha1.AnnotationQueuedSuccessor]; m != "" {
		t.Errorf("the successor's Instance carries the mark %q; the mark is on the Allocation alone", m)
	}
}

// TestAnAllocationCreatedAfterThePrimaryLeftTheSlotIsNotMarked: the mark is judged at the
// Allocation's creation, not at its Instance's. The pass is cut short and Allocation creates
// keep failing until the primary's Allocation has gone (its Instance ended at 09:56 and the
// deployment releases when idle after 5 min); the Allocation then created pends behind nothing,
// so it is not marked, and it records its own wait (README lines 464 to 466).
func TestAnAllocationCreatedAfterThePrimaryLeftTheSlotIsNotMarked(t *testing.T) {
	log := &creationLog{}
	rg := noOverlapQueuedRig(t, withInterceptors(log.funcs()))
	slurm := &slurmView{rg: rg}
	p := noOverlapStart(rg)
	pAlloc := p.Spec.AllocationRef.Name
	hoStepTo(rg, hoT(9, 19, 0), 5*time.Minute, nil)
	log.failAllocations(true)
	succ := cutShort(t, rg, log, p)

	// Every create keeps failing while the primary finishes and is released.
	for {
		if pa, ok := rg.allocation(pAlloc); !ok || pa.Status.AtProvider.Phase == v1alpha1.AllocationEnded {
			break
		}
		if !rg.clock.Now().Before(hoT(10, 30, 0)) {
			t.Fatal("the primary's Allocation did not leave by 10:30:00")
		}
		rg.clock.Advance(10 * time.Second)
		if err := stepTolerant(rg); err != nil && !errors.Is(err, errHOKilled) {
			t.Fatalf("reconcile at %s: %v", rg.clock.Now().Format(time.TimeOnly), err)
		}
	}
	if n := len(log.allocations()); n != 1 {
		t.Fatalf("%d Allocations created while creates were failing, want the primary's alone", n)
	}
	log.failAllocations(false)
	hoStepUntil(rg, hoT(10, 40, 0), 10*time.Second, func() bool { return successorReady(rg) }, nil)

	allocs := log.allocations()
	if len(allocs) != 2 || allocs[1].name != succ.Spec.AllocationRef.Name {
		t.Fatalf("Allocations created %+v, want the primary's and then the successor's %s", allocs, succ.Spec.AllocationRef.Name)
	}
	if m := allocs[1].annotations[v1alpha1.AnnotationQueuedSuccessor]; m != "" {
		t.Errorf("the Allocation created after the primary left the slot carries the mark %q", m)
	}
	jobs := slurm.jobs()
	waits := providerWaits(rg, rigProvider)
	if len(jobs) != 2 || len(waits) != 2 || waits[1].JobID != jobs[1].ID || waits[1].Wait() != jobs[1].StartTime.Sub(jobs[1].SubmitTime) {
		t.Fatalf("the Provider's waits %+v for jobs %+v, want the unmarked successor's own wait recorded next to the primary's", waits, jobs)
	}
}

// TestAStepSevenRetryWhileThePrimaryHoldsTheSlotIsMarked: under Keep the drained primary's
// Allocation holds the GPU until its hard end, 12:00:20. The queued successor is cancelled from
// outside while it pends behind it, at 10:30, and step 7 creates a retry, whose Allocation is
// marked in its create request because the primary's still holds its slot (README line 602).
// The retry pends until 12:00:20, and its wait is not recorded. A fixture that releases the
// primary before the retry would prove nothing here, so the test shows the primary's Allocation
// running when the retry's is created.
func TestAStepSevenRetryWhileThePrimaryHoldsTheSlotIsMarked(t *testing.T) {
	log := &creationLog{}
	rg := noOverlapQueuedRig(t, withInterceptors(log.funcs()))
	slurm := &slurmView{rg: rg}
	p := keepStart(rg, &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionKeep})
	pAlloc := p.Spec.AllocationRef.Name
	hoStepTo(rg, hoT(9, 19, 40), 5*time.Minute, nil) // the successor is created at 09:19:30 and submitted
	hoStepTo(rg, hoT(10, 30, 0), 5*time.Minute, nil)
	jobs := slurm.jobs()
	if len(jobs) != 2 || jobs[1].StartTime != nil {
		t.Fatalf("the fake Slurm holds %+v at 10:30:00, want the primary's job and the successor's, still pending", jobs)
	}
	first := hoRepl(rg, p.Name)
	if len(first) != 1 {
		t.Fatalf("%d successors at 10:30:00, want 1", len(first))
	}
	rg.batch.CancelExternally(jobs[1].ID)

	rg.until(15*time.Minute, 10*time.Second, "the retry's Allocation is created", func() bool { return len(log.allocations()) == 3 })
	if pa, ok := rg.allocation(pAlloc); !ok || pa.Status.AtProvider.Phase != v1alpha1.AllocationRunning {
		t.Fatalf("the primary's Allocation %+v when the retry's was created, want it running under Keep until 12:00:20", pa)
	}
	allocs := log.allocations()
	for i, what := range []string{"the first successor's Allocation", "the retry's Allocation"} {
		if m := allocs[i+1].annotations[v1alpha1.AnnotationQueuedSuccessor]; m == "" {
			t.Errorf("%s %s is not marked in its create request", what, allocs[i+1].name)
		}
	}
	var retry *v1alpha1.Instance
	for _, in := range rg.instances(hoMD) {
		if in.Annotations[v1alpha1.AnnotationPredecessor] == first[0].Name {
			retry = &in
		}
	}
	if retry == nil || retry.Annotations[annotationNoOverlapGap] != p.Name || retry.Spec.AllocationRef.Name != allocs[2].name {
		t.Fatalf("the retry %+v, want a successor of %s that names %s as the primary that drained, on Allocation %s", retry, first[0].Name, p.Name, allocs[2].name)
	}

	hoStepUntil(rg, hoT(12, 30, 0), 30*time.Second, func() bool { return successorReady(rg) }, nil)
	jobs = slurm.jobs()
	if len(jobs) != 3 || jobs[2].StartTime == nil || jobs[2].StartTime.Before(hoT(12, 0, 20)) {
		t.Fatalf("the fake Slurm holds %+v, want the retry's job started no sooner than the primary's hard end, 12:00:20", jobs)
	}
	if w := jobs[2].StartTime.Sub(jobs[2].SubmitTime); w < 80*time.Minute {
		t.Fatalf("the retry's job pended %v, want over 80 min behind the primary's job", w)
	}
	waits := providerWaits(rg, rigProvider)
	if len(waits) != 1 || waits[0].JobID != jobs[0].ID {
		t.Fatalf("the Provider's waits %+v, want the primary's job %s alone: neither marked successor records a wait", waits, jobs[0].ID)
	}
}

// TestTheMarkConditionReadsTheSlotCountsOwnList: the mark's condition is that the primary's
// Allocation "still holds its slot as the slot count reads it": its phase is not Ended, its
// deletion has not completed, and it is on the same Provider (README line 464). An Ended Allocation
// whose job is still completing holds its slot too (#198; holdsSlot). The mark is
// judged from the reservation's own list (slotTable.reserveFor), so it reads the Allocation the
// way the count does: by its phase now, and by its immutable spec.providerConfigRef, never by the
// mutable provider label (PR 214 review, Astra B1 and B2). The rig's primaries leave by deletion
// and always carry the right label, so no scenario above has an Allocation that is Ended and
// still listed, or whose label differs from its reference; this table is where those are judged.
func TestTheMarkConditionReadsTheSlotCountsOwnList(t *testing.T) {
	const prov, other = "example-provider", "example-other"
	for _, tc := range []struct {
		name    string
		phase   v1alpha1.AllocationPhase
		ref     string  // spec.providerConfigRef.name
		label   *string // the provider label: nil, none
		deleted bool    // its deletion completed: the Allocation is gone
		// completing is status.atProvider.jobCompleting: the job's state still carries COMPLETING.
		completing bool
		want       bool
	}{
		{"running", v1alpha1.AllocationRunning, prov, ptrTo(prov), false, false, true},
		{"pending", v1alpha1.AllocationPending, prov, ptrTo(prov), false, false, true},
		{"failed, not yet released: it holds its slot as the count reads it", v1alpha1.AllocationFailed, prov, ptrTo(prov), false, false, true},
		{"ended and still listed", v1alpha1.AllocationEnded, prov, ptrTo(prov), false, false, false},
		// #198: an Ended Allocation whose job is still COMPLETING holds its slot until the flag clears,
		// as the count reads it (holdsSlot), so the mark reads the same.
		{"ended, completing: it holds its slot until the flag clears", v1alpha1.AllocationEnded, prov, ptrTo(prov), false, true, true},
		{"failed, completing", v1alpha1.AllocationFailed, prov, ptrTo(prov), false, true, true},
		{"ended, completing, on another Provider", v1alpha1.AllocationEnded, other, ptrTo(other), false, true, false},
		{"gone: its deletion completed", v1alpha1.AllocationRunning, prov, ptrTo(prov), true, false, false},
		{"running on another Provider", v1alpha1.AllocationRunning, other, ptrTo(other), false, false, false},
		// B2: the reference decides, as it does for the count.
		{"running, its provider label is missing", v1alpha1.AllocationRunning, prov, nil, false, false, true},
		{"running, its provider label names another Provider", v1alpha1.AllocationRunning, prov, ptrTo(other), false, false, true},
		{"running on another Provider, its label names this one", v1alpha1.AllocationRunning, other, ptrTo(prov), false, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			alloc := &v1alpha1.Allocation{
				ObjectMeta: metav1.ObjectMeta{Name: "alloc", Namespace: rigNS},
				Spec:       v1alpha1.AllocationSpec{ProviderConfigReference: providerRef(tc.ref)},
			}
			if tc.label != nil {
				alloc.Labels = map[string]string{v1alpha1.LabelProvider: *tc.label}
			}
			scheme := runtime.NewScheme()
			if err := v1alpha1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			b := clientfake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.Allocation{})
			if !tc.deleted {
				b = b.WithObjects(alloc.DeepCopy())
			}
			c := b.Build()
			if !tc.deleted {
				stored := alloc.DeepCopy()
				if err := c.Get(context.Background(), client.ObjectKeyFromObject(alloc), stored); err != nil {
					t.Fatal(err)
				}
				stored.Status.AtProvider.Phase, stored.Status.AtProvider.JobCompleting = tc.phase, tc.completing
				if err := c.Status().Update(context.Background(), stored); err != nil {
					t.Fatal(err)
				}
			}
			limit := int32(5)
			ok, _, holds, err := newSlotTable().reserveFor(context.Background(), c, prov, rigNS+"/new", rigNS+"/alloc", &limit, time.Now)
			if err != nil || !ok {
				t.Fatalf("reserveFor: ok %v, %v; want a free slot", ok, err)
			}
			if holds != tc.want {
				t.Errorf("the reservation's read says the Allocation holds its slot: %v, want %v", holds, tc.want)
			}
			// With no limit, a holder is still read; with neither, nothing is.
			ok, _, holds, err = newSlotTable().reserveFor(context.Background(), c, prov, rigNS+"/new", rigNS+"/alloc", nil, time.Now)
			if err != nil || !ok || holds != tc.want {
				t.Errorf("with no submit limit: ok %v, holds %v, %v; want ok and holds %v", ok, holds, err, tc.want)
			}
		})
	}
}
