package lifecycle

// The queued-successor mark follows the reservation's own read of the Allocations, not the pass's first
// observation (PR 214 review, Astra B1), and judges the Provider by the immutable spec.providerConfigRef,
// not the mutable provider label (B2). HBEP-7, Handover, No-overlap mode, README lines 464 to 466: the
// mark is judged "when at its creation" the primary's Allocation "is on the same Provider and still holds
// its slot as the slot count reads it". The slot count's read is the one reserveSlotFor makes, so the
// mark takes its answer from it. A third test covers a warm Allocation (README line 466: "fixed at
// creation").

import (
	"context"
	"sync"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// readFlip moves the stored primary Allocation to Ended between the pass's first observation and the
// reservation's list. The observation lists the deployment's Allocations with options (a namespace and a
// label); the slot count lists them with none. From the time given, the first observation sets the flip
// pending, and the first list with no options after it makes the change, once, before that list answers.
type readFlip struct {
	mu                      sync.Mutex
	now                     func() time.Time
	from                    time.Time
	primary                 string
	armed, pending          bool
	flipped, noOptionsLists int
}

// arm makes the change on the pass that observes the Allocations at or after from, naming the primary's
// Allocation. Until then nothing happens, so the Provider's controller, which writes the Allocation's
// status from the fake Slurm at every step, cannot undo it before the pass.
func (f *readFlip) arm(primary string, from time.Time) {
	f.mu.Lock()
	f.primary, f.from, f.armed, f.pending = primary, from, true, false
	f.mu.Unlock()
}

func (f *readFlip) funcs() interceptor.Funcs {
	return interceptor.Funcs{List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
		if _, ok := list.(*v1alpha1.AllocationList); ok {
			f.mu.Lock()
			if len(opts) > 0 && f.armed && !f.now().Before(f.from) {
				f.pending = true
			}
			flip := len(opts) == 0 && f.armed && f.pending
			if len(opts) == 0 && f.pending {
				f.noOptionsLists++
			}
			if flip {
				f.armed, f.pending = false, false
				f.flipped++
			}
			f.mu.Unlock()
			if flip {
				var a v1alpha1.Allocation
				if err := c.Get(ctx, client.ObjectKey{Namespace: rigNS, Name: f.primary}, &a); err != nil {
					return err
				}
				a.Status.AtProvider.Phase = v1alpha1.AllocationEnded
				if err := c.Status().Update(ctx, &a); err != nil {
					return err
				}
			}
		}
		return c.List(ctx, list, opts...)
	}}
}

// withFlip merges the creation log's Create hook and the flip's List hook into one set of interceptors.
func withFlip(l *creationLog, f *readFlip) interceptor.Funcs {
	fn := l.funcs()
	fn.List = f.funcs().List
	return fn
}

// TestTheMarkFollowsTheReservationsReadWhenThePrimaryEndsAfterTheObservation: the pass observes the
// primary's Allocation Running; before the reservation lists, it has become Ended (the flip changes
// the stored Allocation). The reservation, reading the list, finds the primary gone from the slot and
// the successor, created now, pends behind nothing: it is not marked, and records its own wait. The
// control is the same pass with the primary still Running at the reservation: it is marked. A mark
// judged from the pass's first observation marks both.
func TestTheMarkFollowsTheReservationsReadWhenThePrimaryEndsAfterTheObservation(t *testing.T) {
	for _, tc := range []struct {
		name     string
		flip     bool
		wantMark bool
	}{
		{"queued path: the primary ends between the observation and the reservation", true, false},
		{"queued path: the primary still holds its slot at the reservation", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			log, f := &creationLog{}, &readFlip{}
			rg := noOverlapQueuedRig(t, withInterceptors(withFlip(log, f)))
			f.now = rg.clock.Now
			p := noOverlapStart(rg)
			if tc.flip {
				f.arm(p.Spec.AllocationRef.Name, hoT(9, 19, 30))
			}
			hoStepTo(rg, hoT(9, 19, 20), 5*time.Minute, nil)
			rg.clock.Advance(10 * time.Second) // 09:19:30: the successor's planned time
			_ = stepTolerant(rg)               // the flip makes the primary's next write conflict
			allocs := log.allocations()
			if len(allocs) != 2 {
				t.Fatalf("%d Allocations created at 09:19:30, want the primary's and the successor's", len(allocs))
			}
			if tc.flip && f.flipped != 1 {
				t.Fatalf("the primary was changed %d times between the reads, want once: the interleaving was not driven", f.flipped)
			}
			if got := allocs[1].annotations[v1alpha1.AnnotationQueuedSuccessor] != ""; got != tc.wantMark {
				t.Errorf("the successor's Allocation is marked: %v, want %v (annotations %v)", got, tc.wantMark, allocs[1].annotations)
			}
		})
	}
}

// TestTheMarkOfAReCreatedAllocationFollowsTheReservationsRead: the cut-short path (ensureAllocation) judges
// the mark from the reservation's read too. The successor's Allocation create fails after its Instance exists;
// at the pass that re-creates it the pass observes the primary Running and the stored one has ended by the
// reservation's list: the Allocation is not marked. The control, with the primary still running, is the
// cut-short test above.
func TestTheMarkOfAReCreatedAllocationFollowsTheReservationsRead(t *testing.T) {
	log, f := &creationLog{}, &readFlip{}
	rg := noOverlapQueuedRig(t, withInterceptors(withFlip(log, f)))
	f.now = rg.clock.Now
	p := noOverlapStart(rg)
	hoStepTo(rg, hoT(9, 19, 0), 5*time.Minute, nil)
	log.armAllocFailure()
	succ := cutShort(t, rg, log, p)
	f.arm(p.Spec.AllocationRef.Name, rg.clock.Now().Add(10*time.Second))
	rg.clock.Advance(10 * time.Second)
	_ = stepTolerant(rg)
	allocs := log.allocations()
	if f.flipped != 1 || len(allocs) != 2 || allocs[1].name != succ.Spec.AllocationRef.Name {
		t.Fatalf("the primary was changed %d times and the Allocations created are %+v; want once, and the successor's %s re-created", f.flipped, allocs, succ.Spec.AllocationRef.Name)
	}
	if m := allocs[1].annotations[v1alpha1.AnnotationQueuedSuccessor]; m != "" {
		t.Errorf("the re-created Allocation, made after the primary left the slot, carries the mark %q", m)
	}
}

// TestTheMarkOfAOneSlotSuccessorUnderKeepFollowsTheReservationsRead is the case the review describes: on
// the one-slot path a drained primary under Keep holds the slot until its hard end, so the reservation
// denies every successor. The pass at 10:30:00 observes the primary's Allocation Running; before the
// reservation lists, the stored one has ended. The reservation allows the successor, which is created
// after the primary left the slot, and must not be marked: a mark would suppress its genuine wait for
// good. Without the change of the stored Allocation the reservation denies it, so the change is what
// lets the successor be created, and the test shows no Allocation was created before it.
func TestTheMarkOfAOneSlotSuccessorUnderKeepFollowsTheReservationsRead(t *testing.T) {
	log, f := &creationLog{}, &readFlip{}
	rg := noOverlapRig(t, oneSlotOpts(withInterceptors(withFlip(log, f)), withBatchConfig(func(c *pfake.Config) { c.MaxSubmitPerUser, c.MaxGPUsPerUser = 1, 1 }))...)
	f.now = rg.clock.Now
	p := keepStart(rg, &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionKeep})
	pAlloc := p.Spec.AllocationRef.Name
	hoStepTo(rg, hoT(10, 29, 50), 5*time.Minute, nil)
	if pa, ok := rg.allocation(pAlloc); !ok || pa.Status.AtProvider.Phase != v1alpha1.AllocationRunning {
		t.Fatalf("the primary's Allocation %+v at 10:29:50, want it running under Keep", pa)
	}
	if n := len(log.allocations()); n != 1 {
		t.Fatalf("%d Allocations created while the primary holds the one slot, want the primary's alone", n)
	}
	f.arm(pAlloc, hoT(10, 30, 0))
	rg.clock.Advance(10 * time.Second)
	_ = stepTolerant(rg)
	allocs := log.allocations()
	if f.flipped != 1 || len(allocs) != 2 {
		t.Fatalf("the primary was changed %d times and %d Allocations exist; want once, and the successor's created", f.flipped, len(allocs))
	}
	if m := allocs[1].annotations[v1alpha1.AnnotationQueuedSuccessor]; m != "" {
		t.Errorf("the one-slot successor's Allocation, created after the primary left the slot, carries the mark %q", m)
	}
}

// TestTheMarkReadsTheProviderFromTheReferenceNotTheLabel: the primary's provider label is missing, or names
// another Provider, while its spec.providerConfigRef names this one, as the slot count reads it. The
// successor's Allocation is still marked: the primary holds the slot on this Provider.
func TestTheMarkReadsTheProviderFromTheReferenceNotTheLabel(t *testing.T) {
	for _, tc := range []struct {
		name  string
		label func(map[string]string)
	}{
		{"the label is missing", func(l map[string]string) { delete(l, v1alpha1.LabelProvider) }},
		{"the label names another Provider", func(l map[string]string) { l[v1alpha1.LabelProvider] = "example-other" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			log := &creationLog{}
			rg := noOverlapQueuedRig(t, withInterceptors(log.funcs()))
			p := noOverlapStart(rg)
			a, ok := rg.allocation(p.Spec.AllocationRef.Name)
			if !ok || a.Labels[v1alpha1.LabelProvider] != rigProvider {
				t.Fatalf("the primary's Allocation %+v, want it labelled with %s", a, rigProvider)
			}
			orig := a.DeepCopy()
			tc.label(a.Labels)
			if err := rg.c.Patch(rg.ctx, a, client.MergeFrom(orig)); err != nil {
				t.Fatal(err)
			}
			hoStepTo(rg, hoT(9, 19, 40), 5*time.Minute, nil)
			allocs := log.allocations()
			if len(allocs) != 2 {
				t.Fatalf("%d Allocations created, want the primary's and the successor's", len(allocs))
			}
			if allocs[1].annotations[v1alpha1.AnnotationQueuedSuccessor] == "" {
				t.Errorf("the successor's Allocation is not marked although the primary, by its reference, holds a slot on this Provider (annotations %v)", allocs[1].annotations)
			}
		})
	}
}

// TestAReplacementPlacedOnAWarmAllocationCreatesAndMarksNothing: a replacement that goes onto a retained
// warm Allocation creates no Allocation, so there is nothing to mark, and the existing ones are never
// marked later (README line 466). The setup is TestNoOverlapWarmAllocationTakesTheReplacement's: with
// maxSubmitPerUser 2 the primary's and a retained warm Allocation take both slots, so a new job is
// impossible, and the replacement goes onto the warm one at its planned time.
func TestAReplacementPlacedOnAWarmAllocationCreatesAndMarksNothing(t *testing.T) {
	log := &creationLog{}
	qos := []v1alpha1.QOSProfile{{Name: "example-s2", MaxSubmitPerUser: i32(2)}}
	rg := newRig(t, append(qosOpts(qos, "", "example-s2"), withMaxTime(4*time.Hour), withInterceptors(log.funcs()), withBatchConfig(func(c *pfake.Config) { c.MaxSubmitPerUser = 2 }))...)
	p := *hoStart(rg, nil)
	hoStepTo(rg, hoT(9, 0, 0), 5*time.Minute, nil)
	rg.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 2 })
	var warm string
	rg.until(5*time.Minute, 10*time.Second, "the second Allocation started", func() bool {
		for _, a := range rg.allocations(hoMD) {
			if a.Name != p.Spec.AllocationRef.Name && a.Status.AtProvider.StartTime != nil {
				warm = a.Name
				return true
			}
		}
		return false
	})
	rg.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 1 })
	rg.until(5*time.Minute, 10*time.Second, "one live Instance", func() bool { return len(rg.liveInstances(hoMD)) == 1 })
	before := len(log.allocations())
	if before != 2 {
		t.Fatalf("%d Allocations created before the replacement, want the primary's and the warm one", before)
	}
	hoStepTo(rg, hoT(9, 19, 20), 5*time.Minute, nil)
	hoStepTo(rg, hoT(9, 19, 40), 10*time.Second, nil)
	reps := hoRepl(rg, p.Name)
	if len(reps) != 1 || reps[0].Spec.AllocationRef.Name != warm {
		t.Fatalf("replacements %+v, want one, on the warm Allocation %s", reps, warm)
	}
	if n := len(log.allocations()); n != before {
		t.Errorf("%d Allocations created once the replacement was placed, want none beyond the %d", n-before, before)
	}
	for _, o := range log.allocations() {
		if m := o.annotations[v1alpha1.AnnotationQueuedSuccessor]; m != "" {
			t.Errorf("Allocation %s carries the mark %q", o.name, m)
		}
	}
	if a, ok := rg.allocation(warm); !ok || a.Annotations[v1alpha1.AnnotationQueuedSuccessor] != "" {
		t.Errorf("the warm Allocation %+v carries the mark after the replacement was placed on it", a)
	}
}
