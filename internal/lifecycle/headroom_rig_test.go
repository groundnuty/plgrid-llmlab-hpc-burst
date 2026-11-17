package lifecycle

// The extra's admission headroom (HBEP-7, Over-allocation and qualification (beta), Its checks; the science agent's ruling on
// #708's delta reviews, amendment #711; #616 PR 4): an extra is submitted only while, after it, at least H slots stay free on
// its Provider, H being the other deployments with a missing primary that the Provider allows, and at least 1. Nothing is
// cancelled to make room. Every count is worked by hand in the row.

import (
	"context"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

func limitOpts(limit int32) []rigOption {
	return append(qosOpts([]v1alpha1.QOSProfile{{Name: "example-lim", MaxSubmitPerUser: i32(limit)}}, "", "example-lim"), withMaxTime(12*time.Hour))
}

// TestHeadroomLeavesOneSlotPerOtherDeploymentWithAMissingPrimary: the rule at the reservation. The extras' deployment is
// md-e (k = 1, end time 14:00, so its handover reserves no slot); the others are k = 0 with one primary each. Pass order is by
// name: md-a, md-b, md-c place their primaries before md-e; md-y and md-z sort after it, so they still have none when md-e's
// extra is checked: each is a missing primary the Provider allows.
//   - limit 6, md-a and md-b placed, md-z missing: md-e's primary makes 3 listed, H = 1 (md-z), 3 + 0 + 1 + 1 = 5 <= 6: the extra
//     is admitted (it leaves 2 slots, one needed), md-e has 2 Allocations and md-z then submits its primary;
//   - limit 6, md-a, md-b and md-c placed, md-y and md-z missing: 4 listed, H = 2, 4 + 0 + 1 + 2 = 7 > 6: refused, silently
//     (md-e is not Degraded and names no limit); md-e has 1 Allocation, and md-y and md-z submit theirs: 6 = the limit;
//   - limit 3, md-a placed, nobody missing: 2 listed, H = max(0, 1) = 1, 2 + 0 + 1 + 1 = 4 > 3: refused. Without the floor of 1
//     it would be 3 <= 3 and the extra would take the last slot.
//
// Nothing is cancelled in any of them.
func TestHeadroomLeavesOneSlotPerOtherDeploymentWithAMissingPrimary(t *testing.T) {
	for _, c := range []struct {
		name       string
		limit      int32
		placed     []string // k = 0 deployments created first, each with a primary by the time md-e is reconciled
		missing    []string // k = 0 deployments created with md-e, sorting after it
		eAllocs    int
		wantInEnd  int // Allocations of every deployment at the end
		wantDegrad bool
	}{
		{"one other missing: H = 1, the extra is admitted", 6, []string{"md-a", "md-b"}, []string{"md-z"}, 2, 5, false},
		{"two others missing: H = 2, the extra is refused, silently", 6, []string{"md-a", "md-b", "md-c"}, []string{"md-y", "md-z"}, 1, 6, false},
		{"nobody missing: H is at least 1, the extra is refused", 3, []string{"md-a"}, nil, 1, 2, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			log := newCauseLog()
			rg := newRig(t, append(limitOpts(c.limit), withInterceptors(log.funcs()))...)
			rg.batch.Enqueue(pending10h(8)...)
			for _, n := range c.placed {
				rg.create(mdSpec(n))
			}
			tick10(rg) // 08:00:10: the placed ones submit their primaries
			rg.create(extraMD("md-e"))
			for _, n := range c.missing {
				rg.create(mdSpec(n))
			}
			tick10(rg) // 08:00:20: md-e's pass, then the missing ones'
			if n := len(rg.allocations("md-e")); n != c.eAllocs {
				t.Fatalf("md-e has %d Allocations after its pass, want %d", n, c.eAllocs)
			}
			if d := rg.condition("md-e", v1alpha1.ConditionDegraded); d.Reason == v1alpha1.ReasonSubmitLimitReached {
				t.Fatalf("md-e Degraded = %s/%s %q, want no SubmitLimitReached: a refused extra is silent", d.Status, d.Reason, d.Message)
			}
			for range 6 {
				tick10(rg)
			}
			for _, n := range append(append([]string{}, c.placed...), c.missing...) {
				if got := len(rg.allocations(n)); got != 1 {
					t.Fatalf("%s has %d Allocations, want its primary: headroom keeps the slot a primary needs", n, got)
				}
			}
			if n := len(rg.allAllocations()); n != c.wantInEnd {
				t.Fatalf("%d Allocations in all, want %d", n, c.wantInEnd)
			}
			if n := cancelsOf(log); n != 0 {
				t.Fatalf("%d Allocations were deleted, want none: nothing is cancelled to make room", n)
			}
		})
	}
}

// headroomFixture is a hand-built store for headroomOf: Provider prov-x asked, this deployment ns/this, and others.
type headroomFixture struct {
	objs []client.Object
}

func (f *headroomFixture) md(name string, instances int32, providers []string, edit func(*v1alpha1.ModelDeployment)) {
	md := mdSpec(name)
	md.Namespace = "ns"
	md.UID = types.UID("uid-" + name)
	md.Spec.Instances = instances
	md.Spec.Providers.Names = providers
	if edit != nil {
		edit(md)
	}
	f.objs = append(f.objs, md)
}

// served gives the deployment an Instance and its Allocation (a unit that has an Allocation).
func (f *headroomFixture) served(md string, withAlloc bool) {
	tru := true
	in := &v1alpha1.Instance{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: md + "-i", UID: types.UID("iuid-" + md),
		Labels:          map[string]string{v1alpha1.LabelModelDeployment: v1alpha1.LabelValue(md), v1alpha1.LabelProvider: "prov-x"},
		OwnerReferences: []metav1.OwnerReference{{Kind: "ModelDeployment", Name: md, UID: types.UID("uid-" + md), Controller: &tru}}}}
	in.Spec.AllocationRef.Name = md + "-a"
	f.objs = append(f.objs, in)
	if withAlloc {
		f.objs = append(f.objs, &v1alpha1.Allocation{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: md + "-a",
			Labels: map[string]string{v1alpha1.LabelModelDeployment: v1alpha1.LabelValue(md), v1alpha1.LabelProvider: "prov-x"}}})
	}
}

func (f *headroomFixture) h(t *testing.T) int {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c := clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(f.objs...).Build()
	p := &pass{r: &Reconciler{Client: c, Reader: c}, key: types.NamespacedName{Namespace: "ns", Name: "this"}, now: hoT(8, 0, 0)}
	h, err := p.headroomOf(&providerView{name: "prov-x"})(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return h
}

// TestHeadroomCountsOnlyTheOtherDeploymentsThatAllowTheProvider: H, read from a hand-built store through headroomOf, for the
// Provider prov-x asked by ns/this (which itself misses a primary: that never counts). Each row lists the others and the
// H it gives, counted by hand:
//   - none: H = max(0, 1) = 1;
//   - o1 allows prov-x and has no Instance: 1;  o1 and o7 (an Instance whose Allocation was never made, so a primary still
//     missing by Allocation presence): 2;  and o8, which allows prov-x by a selector on the Provider's labels: 3;
//   - o2 allows another Provider only, o3 has its Allocation, o4 is being deleted, o5 wants no Instance, o6 is past its end
//     time, and this deployment is missing a primary itself: none of them counts, H = max(0, 1) = 1, a count that took any of
//     them would be 2 or more; with o1 and o7 beside them, which do count, 2.
func TestHeadroomCountsOnlyTheOtherDeploymentsThatAllowTheProvider(t *testing.T) {
	past := metav1.NewTime(hoT(7, 0, 0))
	now := metav1.NewTime(hoT(8, 0, 0))
	build := func(names ...string) *headroomFixture {
		f := &headroomFixture{}
		f.md("this", 1, []string{"prov-x"}, nil) // missing a primary itself, allows prov-x: never counts
		f.objs = append(f.objs, &v1alpha1.Provider{ObjectMeta: metav1.ObjectMeta{Name: "prov-x", Labels: map[string]string{"site": "x"}}})
		for _, n := range names {
			switch n {
			case "o1":
				f.md("o1", 1, []string{"prov-x"}, nil)
			case "o2":
				f.md("o2", 1, []string{"prov-y"}, nil)
			case "o3":
				f.md("o3", 1, []string{"prov-x"}, nil)
				f.served("o3", true)
			case "o4":
				f.md("o4", 1, []string{"prov-x"}, func(m *v1alpha1.ModelDeployment) {
					m.DeletionTimestamp = &now
					m.Finalizers = []string{"example/keep"}
				})
			case "o5":
				f.md("o5", 0, []string{"prov-x"}, nil)
			case "o6":
				f.md("o6", 1, []string{"prov-x"}, func(m *v1alpha1.ModelDeployment) { m.Spec.EndTime = &past })
			case "o7":
				f.md("o7", 1, []string{"prov-x"}, nil)
				f.served("o7", false)
			case "o8":
				f.md("o8", 1, nil, func(m *v1alpha1.ModelDeployment) {
					m.Spec.Providers.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{"site": "x"}}
				})
			}
		}
		return f
	}
	for _, c := range []struct {
		name   string
		others []string
		want   int
	}{
		{"no other deployment: the floor of 1", nil, 1},
		{"o1 (allows prov-x, no Instance)", []string{"o1"}, 1},
		{"o1 and o7 (an Instance with no Allocation)", []string{"o1", "o7"}, 2},
		{"o1, o7 and o8 (allowed by a selector)", []string{"o1", "o7", "o8"}, 3},
		{"o2 (another Provider), o3 (has its Allocation), o4 (deleting), o5 (wants none), o6 (past its end time) and this one: none counts", []string{"o2", "o3", "o4", "o5", "o6"}, 1},
		{"those five with o1 and o7: only o1 and o7 count", []string{"o2", "o3", "o4", "o5", "o6", "o1", "o7"}, 2},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := build(c.others...).h(t); got != c.want {
				t.Fatalf("H = %d, want %d", got, c.want)
			}
		})
	}
}

// TestAnArrivingPrimaryNeverCostsAnExtraItsSlotAndNothingIsCancelled: the ruling's case, worked by hand. Limit 4 first: md-a
// (k = 1, end time 14:00) makes its primary and its extra at 08:00:10 (1 listed: 1 + 0 + 1 + H(1) = 3 <= 4: admitted; 2 listed).
// md-b (k = 1) and md-c (k = 0) are created together at 08:00:20: md-b's pass comes first, md-c has no Allocation, so H = 1
// for the other that is missing; md-b's primary makes 3 listed, and its extra needs 3 + 0 + 1 + 1 = 5 > 4: refused. md-c's
// primary then submits: 4 listed, the limit. Nothing is cancelled, and every deployment has its primary. Second: limit 5, md-a
// and md-b each with primary and extra (4 listed: md-b's extra, 3 + 0 + 1 + H(1) = 5 <= 5, is admitted, md-c not yet created);
// then md-c and md-d arrive TOGETHER: md-c's primary takes the 5th slot, md-d's is refused (5 listed): the stated limit, md-d
// waits, SubmitLimitReached, with no cancel; when md-a is scaled to zero at 08:01:10 its primary's pending Allocation is released,
// and md-d's primary submits once that completes.
func TestAnArrivingPrimaryNeverCostsAnExtraItsSlotAndNothingIsCancelled(t *testing.T) {
	t.Run("the arriving primary is already missing when the second extra is checked: the extra is refused, the primary submits", func(t *testing.T) {
		log := newCauseLog()
		rg := newRig(t, append(limitOpts(4), withInterceptors(log.funcs()))...)
		rg.batch.Enqueue(pending10h(8)...)
		rg.create(extraMD("md-a"))
		tick10(rg) // 08:00:10
		if n := len(rg.allocations("md-a")); n != 2 {
			t.Fatalf("md-a has %d Allocations at 08:00:10, want its primary and its extra", n)
		}
		rg.create(extraMD("md-b"))
		rg.create(mdSpec("md-c"))
		tick10(rg) // 08:00:20
		for range 6 {
			tick10(rg)
		}
		if a, b, c := len(rg.allocations("md-a")), len(rg.allocations("md-b")), len(rg.allocations("md-c")); a != 2 || b != 1 || c != 1 {
			t.Fatalf("md-a has %d Allocations, md-b %d and md-c %d, want 2, 1 and 1: md-b's extra is refused for md-c's missing primary, which submits", a, b, c)
		}
		if n := cancelsOf(log); n != 0 {
			t.Fatalf("%d Allocations were deleted, want none", n)
		}
	})
	t.Run("a primary that arrives after the extras waits, with no cancel, and submits once a slot frees", func(t *testing.T) {
		log := newCauseLog()
		rg := newRig(t, append(limitOpts(5), withInterceptors(log.funcs()))...)
		rg.batch.Enqueue(pending10h(10)...)
		rg.create(extraMD("md-a"))
		rg.create(extraMD("md-b"))
		tick10(rg) // 08:00:10: md-a: primary, extra; md-b: primary (3 listed), extra 3 + 0 + 1 + 1 = 5 <= 5
		if a, b := len(rg.allocations("md-a")), len(rg.allocations("md-b")); a != 2 || b != 2 {
			t.Fatalf("md-a has %d Allocations and md-b %d at 08:00:10, want 2 each", a, b)
		}
		rg.create(mdSpec("md-c"))
		rg.create(mdSpec("md-d"))
		tick10(rg) // 08:00:20: md-c's primary: 4 listed < 5, submits (5); md-d's: 5 >= 5, refused
		for range 12 {
			tick10(rg) // to 08:02:20
		}
		if c, d := len(rg.allocations("md-c")), len(rg.allocations("md-d")); c != 1 || d != 0 {
			t.Fatalf("md-c has %d Allocations and md-d %d, want 1 and 0: md-d arrives after the extras took the headroom and waits", c, d)
		}
		if dg := rg.condition("md-d", v1alpha1.ConditionDegraded); dg.Reason != v1alpha1.ReasonSubmitLimitReached {
			t.Fatalf("md-d Degraded = %s/%s %q, want SubmitLimitReached", dg.Status, dg.Reason, dg.Message)
		}
		if n := cancelsOf(log); n != 0 {
			t.Fatalf("%d Allocations were deleted, want none: the stated limit is a wait, never a cancel", n)
		}
		rg.mutateMD("md-a", func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 0 })
		for range 12 {
			tick10(rg) // to 08:04:20
		}
		if d := len(rg.allocations("md-d")); d != 1 {
			t.Fatalf("md-d has %d Allocations at 08:04:20, want its primary, submitted in the slot md-a's release freed", d)
		}
	})
}

// headroomBarrier makes the first two ModelDeployment Lists across every namespace (the headroom's own read, made once per
// extra's reservation; a pass reads its deployment by Get) wait for each other for up to 400 ms, then go on: held there, a
// second pass that is NOT serialised against the first reaches it too and both go on together; one serialised by the
// Provider's lock cannot arrive until the first has finished, so the first waits out the 400 ms and the row only costs
// that time.
type headroomBarrier struct {
	mu       sync.Mutex
	arrivals int
	open     chan struct{}
}

func (b *headroomBarrier) funcs(base interceptor.Funcs) interceptor.Funcs {
	b.open = make(chan struct{})
	base.List = func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
		if _, ok := list.(*v1alpha1.ModelDeploymentList); ok {
			b.mu.Lock()
			b.arrivals++
			n := b.arrivals
			if n == 2 {
				close(b.open)
			}
			b.mu.Unlock()
			if n <= 2 {
				select {
				case <-b.open:
				case <-time.After(400 * time.Millisecond):
				}
			}
		}
		return c.List(ctx, list, opts...)
	}
	return base
}

// TestTwoPassesCannotBothTakeTheLastHeadroomSlot: limit 4, md-a and md-b (k = 1, end time 14:00) reconciled AT THE SAME
// MOMENT, each placing its primary and then asking for its extra, the two extras held at a barrier after the headroom's own read
// (headroomBarrier). The count at a reservation is the listed Allocations plus the reserved ones, 2 once both primaries stand, and
// an extra needs 2 + 0 + 1 + H(1) = 4 <= 4 to be admitted and leaves exactly H = 1 free; after it, the other's needs 3 + 0 + 1 + 1 = 5.
// So exactly ONE extra is admitted: 3 Allocations in all, never 4, whichever pass is first. Checked outside the Provider's lock
// both would read 2 and both be admitted.
func TestTwoPassesCannotBothTakeTheLastHeadroomSlot(t *testing.T) {
	bar := &headroomBarrier{}
	log := newCauseLog()
	rg := newRig(t, append(limitOpts(4), withInterceptors(bar.funcs(log.funcs())))...)
	rg.batch.Enqueue(pending10h(6)...)
	rg.Step() // the Provider's first status
	rg.create(extraMD("md-a"))
	rg.create(extraMD("md-b"))
	rg.clock.Advance(10 * time.Second)
	syncProviders(rg)
	for _, err := range rg.reconcileAtOnce(rg.r, "md-a", "md-b") {
		if err != nil {
			t.Fatalf("a pass failed: %v", err)
		}
	}
	a, b := len(rg.allocations("md-a")), len(rg.allocations("md-b"))
	if a < 1 || b < 1 || a+b != 3 {
		t.Fatalf("md-a has %d Allocations and md-b %d, want each its primary and exactly one of them an extra: 3 in all, the count never above the limit less the headroom", a, b)
	}
}

// TestACutShortExtrasRetryKeepsTheHeadroom: the retry of an extra whose Allocation's create was cut short takes the headroom
// like its creation did (E's shared extraChecks). Limit 4. md-e (k = 1, end time 14:00) makes its primary and its extra X at
// 08:00:10: 1 + 0 + 1 + H(1) = 3 <= 4, admitted, and X's Allocation create is cut short; the controller restarts (its
// reservation is lost). md-b and md-c (k = 0), created then, take two slots at 08:00:20: 3 listed (md-e's primary and theirs).
// X's retry: 3 + 0 + 1 + H(1) = 5 > 4: refused, and X is ended through the trim path. Without the headroom at the retry it would
// be 3 + 0 + 1 = 4 <= 4 and X would take the last slot.
// Expected at 08:01:10: X ended with no Allocation; md-e, md-b and md-c one Allocation each; md-e not SubmitLimitReached.
func TestACutShortExtrasRetryKeepsTheHeadroom(t *testing.T) {
	log := newCauseLog()
	cut := &allocCutter{}
	rg := newRig(t, append(limitOpts(4), withInterceptors(cut.funcs(log.funcs())))...)
	rg.batch.Enqueue(pending10h(6)...)
	cut.arm("md-e", 1) // the primary's Allocation is made, the extra's is cut short
	rg.create(extraMD("md-e"))
	rg.clock.Advance(10 * time.Second)
	stepTolerantAll(rg) // 08:00:10
	if cut.failures() != 1 || len(rg.instances("md-e")) != 2 || len(rg.allocations("md-e")) != 1 {
		t.Fatalf("md-e: %d cut creates, %d Instances, %d Allocations, want 1, 2 and 1: the extra was admitted and its Allocation not made", cut.failures(), len(rg.instances("md-e")), len(rg.allocations("md-e")))
	}
	var xName string
	for _, in := range rg.instances("md-e") {
		if _, ok := rg.allocation(in.Spec.AllocationRef.Name); !ok {
			xName = in.Name
		}
	}
	cut.disarm()
	rg.restartController()
	rg.create(mdSpec("md-b"))
	rg.create(mdSpec("md-c"))
	for range 6 { // 08:00:20 to 08:01:10
		tick10(rg)
	}
	if got := instOrNil(rg, xName); got != nil && !ended(got) {
		t.Fatalf("X is %s at 08:01:10, want it ended: its retry leaves no headroom (3 listed + 1 + 1 > 4)", got.Status.Phase)
	}
	if e, b, c := len(rg.allocations("md-e")), len(rg.allocations("md-b")), len(rg.allocations("md-c")); e != 1 || b != 1 || c != 1 {
		t.Fatalf("md-e has %d Allocations, md-b %d and md-c %d, want 1 each", e, b, c)
	}
	if d := rg.condition("md-e", v1alpha1.ConditionDegraded); d.Reason == v1alpha1.ReasonSubmitLimitReached {
		t.Fatalf("md-e Degraded = %s/%s %q, want no SubmitLimitReached", d.Status, d.Reason, d.Message)
	}
	if n := cancelsOf(log); n != 0 {
		t.Fatalf("%d Allocations deleted, want none: X had no Allocation", n)
	}
}
