package lifecycle

// Rows for the owner's COMMIT of a yielded extra's cancel (#708's delta reviews: Astra's three timing races, Opus's M1 and
// M2, Gemini's deleting-Allocation case; C1 to C5 of the fix round). The owner cancels only under the yield lock and its
// Provider's lock, after an uncached re-check: the extra's mark is still the one it saw, B exists and still allows the
// Provider, B's last pass is later than the mark and saw its current spec, B's shortage exceeds the yields in flight for it
// (Allocations being deleted whose release cause names B), and freeing the extra's slot leaves the count within the limit.
// Every expected value is worked by hand in the row.

import (
	"context"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// yieldFaults injects what the commit's rows need into the rig's store, on top of causeLog's:
//   - failClear: a Patch of an Instance that carries no yield mark (the clearing of one) fails;
//   - failStatus: the next status update of an Instance fails (a pass killed between two writes);
//   - holdDeletes: an Allocation's Delete waits at a barrier until a second delete arrives or the wait runs out;
//   - afterLoad: once, when the pass of the named deployment has listed its Instances, hook runs (another pass, nested),
//     and the list the pass keeps is the one it read BEFORE the hook, as a pass whose view went stale.
type yieldFaults struct {
	mu          sync.Mutex
	failClear   bool
	failStatus  bool
	barrier     bool
	arrivals    int
	barrierOpen chan struct{}
	wait        time.Duration
	loadOf      string
	hook        func()
}

func (f *yieldFaults) funcs(base interceptor.Funcs) interceptor.Funcs {
	baseDelete := base.Delete
	base.Patch = func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
		if in, ok := obj.(*v1alpha1.Instance); ok && in.Annotations[annotationYieldTo] == "" {
			f.mu.Lock()
			fail := f.failClear
			f.mu.Unlock()
			if fail {
				return errHOKilled
			}
		}
		return c.Patch(ctx, obj, patch, opts...)
	}
	base.SubResourceUpdate = func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
		if _, ok := obj.(*v1alpha1.Instance); ok {
			f.mu.Lock()
			fail := f.failStatus
			f.failStatus = false
			f.mu.Unlock()
			if fail {
				return errHOKilled
			}
		}
		return c.SubResource(sub).Update(ctx, obj, opts...)
	}
	base.Delete = func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
		if _, ok := obj.(*v1alpha1.Allocation); ok {
			f.mu.Lock()
			hold, wait := f.barrier, f.wait
			var open chan struct{}
			if hold {
				f.arrivals++
				if f.arrivals == 2 {
					close(f.barrierOpen)
				}
				open = f.barrierOpen
			}
			f.mu.Unlock()
			if hold {
				select {
				case <-open:
				case <-time.After(wait):
				}
			}
		}
		if baseDelete != nil {
			return baseDelete(ctx, c, obj, opts...)
		}
		return c.Delete(ctx, obj, opts...)
	}
	base.List = func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
		if err := c.List(ctx, list, opts...); err != nil {
			return err
		}
		if _, ok := list.(*v1alpha1.InstanceList); !ok {
			return nil
		}
		f.mu.Lock()
		of, hook := f.loadOf, f.hook
		f.mu.Unlock()
		if of == "" {
			return nil
		}
		lo := &client.ListOptions{}
		for _, o := range opts {
			o.ApplyToList(lo)
		}
		if lo.LabelSelector == nil || !lo.LabelSelector.Matches(labels.Set{v1alpha1.LabelModelDeployment: of}) {
			return nil
		}
		f.mu.Lock()
		f.loadOf = ""
		f.mu.Unlock()
		hook()
		return nil
	}
	return base
}

func (f *yieldFaults) holdDeletes(wait time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.barrier, f.arrivals, f.barrierOpen, f.wait = true, 0, make(chan struct{}), wait
}

func (f *yieldFaults) releaseDeletes() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.barrier = false
}

// afterLoadOf arms hook to run once, nested, when the pass of the deployment has listed its Instances.
func (f *yieldFaults) afterLoadOf(md string, hook func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.loadOf, f.hook = md, hook
}

func (f *yieldFaults) setFailClear(v bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failClear = v
}

func (f *yieldFaults) armFailStatus() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failStatus = true
}

// yieldRigFaulty is yieldRig with faults.
func yieldRigFaulty(t *testing.T, log *causeLog, f *yieldFaults) *rig {
	t.Helper()
	qos := []v1alpha1.QOSProfile{{Name: "example-s4", MaxSubmitPerUser: i32(4)}}
	return newRig(t, append(qosOpts(qos, "", "example-s4"), withMaxTime(12*time.Hour), withInterceptors(f.funcs(log.funcs())))...)
}

// syncProviders is the first half of rg.Step: the Providers' passes and the agents.
func syncProviders(rg *rig) {
	rg.t.Helper()
	if err := rg.prov.Sync(rg.ctx); err != nil {
		rg.t.Fatalf("provider sync: %v", err)
	}
	for _, x := range rg.extra {
		if err := x.prov.Sync(rg.ctx); err != nil {
			rg.t.Fatalf("provider %s sync: %v", x.name, err)
		}
	}
	rg.syncAgents()
}

// TestACancelWaitsWhileItCouldNotAdmitItsPrimary (Opus M1 = Astra 1, Astra 2 and Fable's path with no cut-short primary: the
// cancel's admission is re-checked at the commit). Limit 5 on one Provider. md-0 (k = 0, no end time) makes its primary P0 at
// 08:00:10, submitted 08:00:20 and starting after 50 s (08:01:10), so its handover reserves a slot once it runs; md-a (k = 1, end
// time 14:00) makes P1 and X1 at 08:00:10 and md-b (the same) P2 and X2 at 08:00:30, all four pending 10 h: 5 listed. md-c (k = 0,
// instances 2), created 08:00:40, is refused at 08:00:50 (5 listed + 0 reserved = 5 <= 5: a cancel admits it) and marks the
// newest pending extra, md-b's, and at 08:01:00 marks md-a's (still 5), so two marks for a shortage of 2, both admissible when
// marked; the extras are whichever of each pair the selection's tie leaves surplus, both pending, so they stay. At 08:01:10 P0
// starts and reserves one slot: 5 listed + 1 reserved = 6 > 5. The owner of the first mark (md-b), reconciled before md-c,
// finds md-c's last pass (08:01:00) later than its mark (08:00:50), the generation current and the shortage 2 above 0 yields
// in flight, but freeing its slot would leave 5, the limit, not below it: it WAITS, as md-a's owner does. Neither extra is
// cancelled while the count stays at 6 (through 08:02:00), and md-c marks no third. At 08:02:00 md-0 gets an end time of
// 08:30, before its drain start, so P0 reserves no slot: 5 listed + 0 = 5, and at 08:02:10 the owners commit one after the
// other, md-a's first (the names sort so): its extra (shortage 2 above 0 in flight; 5 <= 5), then md-b's (shortage 2 above 1 in
// flight, the first Allocation deleting and still listed; 5 <= 5). Expected: nothing deleted through 08:02:00 and both marks
// kept; two deleted by 08:03:00; md-c's two primaries made (3 listed + 0, then 4 + 0, within 5); the two cancelled Instances
// ended Drained (a clean end) and no Allocation made for them: 7 jobs, not 9.
func TestACancelWaitsWhileItCouldNotAdmitItsPrimary(t *testing.T) {
	log := newCauseLog()
	qos := []v1alpha1.QOSProfile{{Name: "example-s5", MaxSubmitPerUser: i32(5)}}
	rg := newRig(t, append(qosOpts(qos, "", "example-s5"), withMaxTime(12*time.Hour), withInterceptors(log.funcs()))...)
	rg.batch.Enqueue(append([]pfake.Script{{QueueWait: 50 * time.Second}}, pending10h(4)...)...)
	rg.create(mdSpec("md-0"))
	tick10(rg) // 08:00:10: P0 is made
	rg.create(yieldMD("md-a"))
	tick10(rg) // 08:00:20: P0's job is submitted (the first script: 50 s), md-a makes P1 and X1
	rg.create(yieldMD("md-b"))
	tick10(rg) // 08:00:30: md-a's jobs are submitted, md-b makes P2 and X2
	tick10(rg) // 08:00:40
	c := mdSpec("md-c")
	c.Spec.Instances = 2
	rg.create(c)
	tick10(rg) // 08:00:50: md-c marks the newest pending extra, md-b's
	tick10(rg) // 08:01:00: md-c marks md-a's
	if ms := marks(rg); len(ms) != 2 {
		t.Fatalf("the marks at 08:01:00 are %v, want two, md-b's extra and md-a's, both admissible when marked (5 listed, nothing reserved)", markNames(ms))
	}
	for range 6 { // 08:01:10 to 08:02:00
		tick10(rg)
		if n := cancelsOf(log); n != 0 {
			t.Fatalf("at %s %d Allocations were deleted, want none: P0 is Running and reserves a slot (5 listed + 1 reserved = 6), so a cancel would leave 5 and admit nothing", rg.clock.Now().Format(time.TimeOnly), n)
		}
	}
	if ms := marks(rg); len(ms) != 2 {
		t.Fatalf("the marks at 08:02:00 are %v, want both kept: the owners wait", markNames(ms))
	}
	rg.mutateMD("md-0", func(m *v1alpha1.ModelDeployment) { e := metav1.NewTime(hoT(8, 30, 0)); m.Spec.EndTime = &e })
	for range 6 { // 08:02:10 to 08:03:00
		tick10(rg)
	}
	if n := cancelsOf(log); n != 2 {
		t.Fatalf("%d Allocations were deleted by 08:03:00, want 2: the count fell back to 5 and the two marks committed", n)
	}
	if n := len(rg.allocations("md-c")); n != 2 {
		t.Fatalf("md-c has %d Allocations at 08:03:00, want its two primaries", n)
	}
	if a, b := len(rg.liveInstances("md-a")), len(rg.liveInstances("md-b")); a != 1 || b != 1 {
		t.Fatalf("md-a has %d live Instances and md-b %d, want 1 each: each extra yielded", a, b)
	}
	for _, md := range []string{"md-a", "md-b"} {
		for _, in := range rg.instances(md) {
			if ended(&in) && in.Status.EndReason != v1alpha1.ReasonDrained {
				t.Fatalf("%s's Instance %s ended with %q, want Drained: a yield is a clean end", md, in.Name, in.Status.EndReason)
			}
		}
	}
	if jobs := rg.submissions(); jobs != 7 {
		t.Fatalf("%d jobs in all, want 7: md-0's one, md-a's two, md-b's two and md-c's two primaries, none for a cancelled extra", jobs)
	}
}

// TestTwoOwnersCommitOnlyOneCancelForOneShortage (Astra's delta review, finding 3: a positive shortage does not bound
// concurrent cancellations). Three Providers: A (limit 2) holds md-a's primary and pending extra X_a, C (limit 2) md-b's and
// X_c, D (limit 1) md-0's pending primary. md-c (k = 0, instances 2; A, C, then D), created 08:00:40, is refused on all three
// at 08:00:50 (A and C: 2 <= 2) and marks both extras (shortage 2); at 08:01:00 it is refused again and its status is
// later than both marks. md-0 is scaled to zero after 08:00:50, so D is free at 08:01:10, where md-c places one primary on D
// (shortage 1) BEFORE it reaches the clearing of its marks, which a fault stops. X_a's owner (md-a) and X_c's owner (md-b) then
// reconcile AT THE SAME MOMENT, each held, after its re-check reads, at a barrier on the Allocation's delete until the other
// arrives (or 400 ms pass). Under the yield lock the second owner cannot read until the first has finished: it finds one
// yield in flight for md-c, equal to its shortage of 1, and clears its mark. Without the lock both read before either deletes.
// Expected: exactly ONE Allocation deleted by the two owners, the other extra alive with no mark.
func TestTwoOwnersCommitOnlyOneCancelForOneShortage(t *testing.T) {
	log := newCauseLog()
	faults := &yieldFaults{}
	qosA := []v1alpha1.QOSProfile{{Name: "example-a", MaxSubmitPerUser: i32(2)}}
	rg := newRig(t, append(qosOpts(qosA, "", "example-a"), withMaxTime(12*time.Hour), withInterceptors(faults.funcs(log.funcs())))...)
	c := rg.addProviderWithProfile(yieldC, yieldCProfile, yieldCPartition, []v1alpha1.QOSProfile{{Name: "example-c", MaxSubmitPerUser: i32(2)}}, nil)
	dProv := rg.addProviderWithProfile("example-d", "example-d-profile", "gpu-d", []v1alpha1.QOSProfile{{Name: "example-d", MaxSubmitPerUser: i32(1)}}, nil)
	rg.batch.Enqueue(pending10h(2)...)
	c.batch.Enqueue(pending10h(2)...)
	dProv.batch.Enqueue(pending10h(1)...)
	f := mdSpec("md-0")
	f.Spec.Providers.Names = []string{"example-d"}
	rg.create(f)
	rg.create(yieldMD("md-a"))
	tick10(rg) // 08:00:10
	tick10(rg) // 08:00:20
	d2 := yieldMD("md-b")
	d2.Spec.Providers.Names = []string{yieldC}
	rg.create(d2)
	tick10(rg) // 08:00:30
	tick10(rg) // 08:00:40
	b := mdSpec("md-c")
	b.Spec.Instances = 2
	b.Spec.Providers.Names = []string{rigProvider, yieldC, "example-d"}
	rg.create(b)
	tick10(rg) // 08:00:50
	if ms := marks(rg); len(ms) != 2 {
		t.Fatalf("the marks at 08:00:50 are %v, want two: md-a's extra on A and md-b's on C", markNames(ms))
	}
	rg.mutateMD("md-0", func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 0 })
	tick10(rg)                         // 08:01:00: md-0's pending Allocation is deleted; md-c keeps both marks, its status later than both
	rg.clock.Advance(10 * time.Second) // 08:01:10
	syncProviders(rg)                  // D's slot is free
	before := cancelsOf(log)
	faults.setFailClear(true)
	if err := rg.reconcileOf("md-c"); err == nil {
		t.Fatal("md-c's pass at 08:01:10 returned no error: the fault that stops its clearing did not fire, so the row proves nothing")
	}
	faults.setFailClear(false)
	if n := len(rg.onProvider("md-c", "example-d")); n != 1 {
		t.Fatalf("md-c has %d Instances on D, want its first primary, placed in the freed slot", n)
	}
	if ms := marks(rg); len(ms) != 2 {
		t.Fatalf("the marks are %v, want both still there: md-c's clearing was stopped", markNames(ms))
	}
	faults.holdDeletes(400 * time.Millisecond)
	for _, err := range rg.reconcileAtOnce(rg.r, "md-a", "md-b") {
		if err != nil {
			t.Fatalf("an owner's pass failed: %v", err)
		}
	}
	faults.releaseDeletes()
	if n := cancelsOf(log) - before; n != 1 {
		t.Fatalf("%d Allocations were deleted by the two owners, want exactly 1: md-c's shortage is 1", n)
	}
	for _, md := range []string{"md-a", "md-b"} {
		live := rg.liveInstances(md)
		for _, in := range live {
			if in.Annotations[annotationYieldTo] != "" {
				t.Fatalf("%s's live Instance %s still carries a mark: the owner that did not cancel must clear it", md, in.Name)
			}
		}
	}
	if a, bb := len(rg.liveInstances("md-a")), len(rg.liveInstances("md-b")); a+bb != 3 {
		t.Fatalf("md-a has %d live Instances and md-b %d, want 3 in all: two primaries and one extra", a, bb)
	}
}

// TestAMarkClearedByItsDeploymentBeforeTheOwnersCommitIsNotCancelled (Opus M2: the owner's mark is re-read at the commit).
// The fixture of the shortage that falls (TestAShortageThatFallsLeavesOneMarkNotTwo): A (limit 4) holds md-a's and md-b's
// primaries and pending extras X1 (md-a, made 08:00:10) and X2 (md-b, made 08:00:30); C (limit 1) is empty and not yet in md-c's
// list. md-c (instances 2) marks X2 at 08:00:50 and X1 at 08:01:00 (two marks, shortage 2) and gains C in its list. At 08:01:10
// md-a's owner pass has read its Instances, X1's mark among them, when md-c's WHOLE pass runs, nested: it places a primary on C
// (shortage 1), keeps the NEWEST mark, X2's, and clears the oldest, X1's, and writes its status (later than both marks, its
// generation current). Then md-a's owner, on its stale copy of X1's mark, commits: it re-reads X1 under the locks, finds no
// mark, and cancels NOTHING. md-b's owner then cancels X2 (shortage 1 above 0 in flight). On the stale copy alone md-a would
// cancel X1 (shortage 1, status later, generation current, 4 <= 4) and md-b would then clear X2's mark.
// Expected: exactly one Allocation deleted, and it is X2's (md-b has 1 live Instance, md-a 2, none marked).
func TestAMarkClearedByItsDeploymentBeforeTheOwnersCommitIsNotCancelled(t *testing.T) {
	log := newCauseLog()
	faults := &yieldFaults{}
	qosA := []v1alpha1.QOSProfile{{Name: "example-a", MaxSubmitPerUser: i32(4)}}
	rg := newRig(t, append(qosOpts(qosA, "", "example-a"), withMaxTime(12*time.Hour), withInterceptors(faults.funcs(log.funcs())))...)
	rg.addProviderWithProfile(yieldC, yieldCProfile, yieldCPartition, []v1alpha1.QOSProfile{{Name: "example-c", MaxSubmitPerUser: i32(1)}}, nil)
	rg.batch.Enqueue(pending10h(8)...)
	rg.create(yieldMD("md-a"))
	tick10(rg) // 08:00:10
	tick10(rg) // 08:00:20
	rg.create(yieldMD("md-b"))
	tick10(rg) // 08:00:30
	tick10(rg) // 08:00:40
	b := mdSpec("md-c")
	b.Spec.Instances = 2
	rg.create(b)
	tick10(rg) // 08:00:50: md-c marks X2
	tick10(rg) // 08:01:00: md-c marks X1
	if ms := marks(rg); len(ms) != 2 {
		t.Fatalf("the marks at 08:01:00 are %v, want two", markNames(ms))
	}
	rg.mutateMD("md-c", func(m *v1alpha1.ModelDeployment) { m.Spec.Providers.Names = []string{rigProvider, yieldC} })
	before := cancelsOf(log)
	rg.clock.Advance(10 * time.Second) // 08:01:10
	syncProviders(rg)
	faults.afterLoadOf("md-a", func() {
		if err := rg.reconcileOf("md-c"); err != nil {
			t.Errorf("md-c's nested pass: %v", err)
		}
	})
	if err := rg.reconcileOf("md-a"); err != nil {
		t.Fatalf("md-a's pass: %v", err)
	}
	if n := len(rg.onProvider("md-c", yieldC)); n != 1 {
		t.Fatalf("md-c has %d Instances on C, want its first primary: its nested pass did not run, so the row proves nothing", n)
	}
	if n := cancelsOf(log) - before; n != 0 {
		t.Fatalf("md-a's owner deleted %d Allocations, want none: md-c cleared X1's mark before the commit, and the owner must re-read it", n)
	}
	if err := rg.reconcileOf("md-b"); err != nil {
		t.Fatalf("md-b's pass: %v", err)
	}
	if n := cancelsOf(log) - before; n != 1 {
		t.Fatalf("%d Allocations were deleted, want 1: md-b's owner cancels X2", n)
	}
	if a, bb := len(rg.liveInstances("md-a")), len(rg.liveInstances("md-b")); a != 2 || bb != 1 {
		t.Fatalf("md-a has %d live Instances and md-b %d, want 2 and 1: the extra that yielded is X2, md-b's", a, bb)
	}
	if ms := marks(rg); len(ms) > 1 {
		t.Fatalf("%v are marked, want at most the cancelled extra's tombstone", markNames(ms))
	}
}

// TestACommitCutShortAfterItsCauseIsFinishedOrCleared: the commit writes the release cause, then ends the Instance's status, then
// deletes the Allocation. A pass killed between the cause and the status leaves the Allocation with its cause, not deleting,
// and the Instance live and marked. The yield rig at limit 4 (md-a, md-b, and md-c, short by one): md-c marks X2 at 08:00:50 and
// keeps the mark at 08:01:00; at 08:01:10 md-b's commit writes the cause and its status update fails once.
//   - the need persists: at 08:01:20 the next commit finds the same mark and cancels: the cause is the same, the Instance ends, the
//     Allocation is deleted with the cause "Instance <X2> ended: yielded its submit slot to example-ns/md-c";
//   - the need is gone (md-c is scaled to zero): at 08:01:20 the commit clears the mark AND the cause: X2 lives with its
//     Allocation, which carries no release cause, and nothing is deleted.
func TestACommitCutShortAfterItsCauseIsFinishedOrCleared(t *testing.T) {
	for _, c := range []struct {
		name  string
		scale bool
	}{{"the need persists: the next commit cancels", false}, {"the need is gone: the next commit clears the mark and the cause", true}} {
		t.Run(c.name, func(t *testing.T) {
			log := newCauseLog()
			faults := &yieldFaults{}
			rg := yieldRigFaulty(t, log, faults)
			rg.batch.Enqueue(pending10h(8)...)
			yieldStart(rg) // 08:00:50: md-c marks X2
			ms := marks(rg)
			if len(ms) != 1 || deploymentOfInstance(ms[0]) != "md-b" {
				t.Fatalf("the marks at 08:00:50 are %v, want one, md-b's X2", markNames(ms))
			}
			x2, x2Alloc := ms[0].Name, ms[0].Spec.AllocationRef.Name
			tick10(rg) // 08:01:00
			faults.armFailStatus()
			rg.clock.Advance(10 * time.Second) // 08:01:10
			stepTolerantAll(rg)
			al, ok := rg.allocation(x2Alloc)
			if !ok || !al.DeletionTimestamp.IsZero() || al.Annotations[v1alpha1.AnnotationReleaseCause] == "" {
				t.Fatalf("X2's Allocation %v (found %v) at 08:01:10, want it with its release cause written and not deleting: the commit was cut short after the cause", al, ok)
			}
			if in := instOrNil(rg, x2); in == nil || ended(in) {
				t.Fatalf("X2 %v, want it live: its status update failed", in)
			}
			if c.scale {
				rg.mutateMD("md-c", func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 0 })
			}
			rg.clock.Advance(10 * time.Second) // 08:01:20
			stepTolerantAll(rg)
			want := "Instance " + x2 + " ended: yielded its submit slot to " + yieldDeployment
			if !c.scale {
				if cause, deleted := log.deletedWith(x2Alloc); !deleted || cause != want {
					t.Fatalf("X2's Allocation: cause %q (deleted %v), want it cancelled with %q", cause, deleted, want)
				}
				return
			}
			al, ok = rg.allocation(x2Alloc)
			if !ok || !al.DeletionTimestamp.IsZero() || al.Annotations[v1alpha1.AnnotationReleaseCause] != "" {
				t.Fatalf("X2's Allocation %v (found %v) at 08:01:20, want it alive with the release cause cleared", al, ok)
			}
			if in := instOrNil(rg, x2); in == nil || ended(in) || in.Annotations[annotationYieldTo] != "" {
				t.Fatalf("X2 %v at 08:01:20, want it live and unmarked", in)
			}
			if n := cancelsOf(log); n != 0 {
				t.Fatalf("%d Allocations deleted, want none", n)
			}
		})
	}
}

// TestTwoRefusedDeploymentsMarkOneExtraOnce: the candidate excludes an extra that is already marked. Limit 3: md-a (k = 1, end time
// 14:00) holds its primary and pending extra X, md-0's pending primary the third slot. md-b and md-c (instances 1), created
// together at 08:00:30, are both refused at 08:00:40 for the limit alone (3 listed >= 3; 3 <= 3 admits a cancel): md-b, reconciled
// first, marks X; md-c finds X marked, so no pending extra is left to take and it marks nothing. Expected: one mark, naming md-b;
// X cancelled once by 08:01:20 and md-b's primary made in its slot, md-c with no Allocation and SubmitLimitReached.
func TestTwoRefusedDeploymentsMarkOneExtraOnce(t *testing.T) {
	log := newCauseLog()
	qos := []v1alpha1.QOSProfile{{Name: "example-s3", MaxSubmitPerUser: i32(3)}}
	rg := newRig(t, append(qosOpts(qos, "", "example-s3"), withMaxTime(12*time.Hour), withInterceptors(log.funcs()))...)
	rg.batch.Enqueue(pending10h(8)...)
	f := mdSpec("md-0")
	rg.create(f)
	rg.create(yieldMD("md-a"))
	tick10(rg) // 08:00:10
	tick10(rg) // 08:00:20
	tick10(rg) // 08:00:30
	rg.create(mdSpec("md-b"))
	rg.create(mdSpec("md-c"))
	tick10(rg) // 08:00:40
	if n := len(rg.allAllocations()); n != 3 {
		t.Fatalf("%d Allocations at 08:00:40, want 3: the limit is full", n)
	}
	ms := marks(rg)
	if len(ms) != 1 || ms[0].Annotations[annotationYieldTo] != rigNS+"/md-b 2026-11-02T08:00:40Z" {
		t.Fatalf("the marks at 08:00:40 are %v, want one, on md-a's extra, naming md-b: md-c finds it marked", markNames(ms))
	}
	for range 4 { // to 08:01:20
		tick10(rg)
	}
	if n := cancelsOf(log); n != 1 {
		t.Fatalf("%d Allocations deleted, want 1: the extra yielded once", n)
	}
	if b, c := len(rg.allocations("md-b")), len(rg.allocations("md-c")); b != 1 || c != 0 {
		t.Fatalf("md-b has %d Allocations and md-c %d, want 1 and 0: md-b's primary takes the freed slot", b, c)
	}
	if d := rg.condition("md-c", v1alpha1.ConditionDegraded); d.Reason != v1alpha1.ReasonSubmitLimitReached {
		t.Fatalf("md-c Degraded = %s/%s %q, want SubmitLimitReached", d.Status, d.Reason, d.Message)
	}
}
