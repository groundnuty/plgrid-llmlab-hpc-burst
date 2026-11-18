package lifecycle

// The extra's admission headroom (HBEP-7, Over-allocation and qualification (beta), Its checks; the science agent's ruling on
// #708's delta reviews, amendment #711; #616 PR 4): an extra is submitted only while, after it, at least H slots stay free on
// its Provider, H being the missing primaries, counted as primaries, of the other deployments that the Provider allows, at the
// time of the check, and 0 when there are none. Nothing is cancelled to make room. Every count is worked by hand in the row.

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
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

func limitOpts(limit int32) []rigOption {
	return append(qosOpts([]v1alpha1.QOSProfile{{Name: "example-lim", MaxSubmitPerUser: i32(limit)}}, "", "example-lim"), withMaxTime(12*time.Hour))
}

// TestHeadroomLeavesASlotPerMissingPrimaryOfTheOtherDeployments: the rule at the reservation. The extras' deployment is
// md-e (k = 1, end time 14:00, so its handover reserves no slot); the others are k = 0 with one primary each. Pass order is by
// name: md-a, md-b, md-c place their primaries before md-e; md-y and md-z sort after it, so they still have none when md-e's
// extra is checked: each is a missing primary the Provider allows.
//   - limit 6, md-a and md-b placed, md-z missing: md-e's primary makes 3 listed, H = 1 (md-z), 3 + 0 + 1 + 1 = 5 <= 6: the extra
//     is admitted (it leaves 2 slots, one needed), md-e has 2 Allocations and md-z then submits its primary;
//   - limit 6, md-a, md-b and md-c placed, md-y and md-z missing: 4 listed, H = 2, 4 + 0 + 1 + 2 = 7 > 6: refused, silently
//     (md-e is not Degraded and names no limit); md-e has 1 Allocation, and md-y and md-z submit theirs: 6 = the limit;
//   - limit 3, md-a placed, nobody missing: 2 listed, H = 0, 2 + 0 + 1 + 0 = 3 <= 3: admitted, a lone deployment takes the last
//     slot (there is no floor), 3 Allocations; at limit 2 it is 2 + 0 + 1 + 0 = 3 > 2, refused.
//
// Nothing is cancelled in any of them. The placed deployments end at 14:00 (noHandover), before their primaries' drain
// starts, so their pending primaries will hold no slot for a replacement and the extra leaves none for them (#725's
// other-deployments term, which its own rows cover): this row is about H.
func TestHeadroomLeavesASlotPerMissingPrimaryOfTheOtherDeployments(t *testing.T) {
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
		{"nobody missing: H = 0, the extra takes the last slot", 3, []string{"md-a"}, nil, 2, 3, false},
		{"nobody missing, limit 2: 2 + 0 + 1 + 0 = 3 > 2, no extra", 2, []string{"md-a"}, nil, 1, 2, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			log := newCauseLog()
			rg := newRig(t, append(limitOpts(c.limit), withInterceptors(log.funcs()))...)
			rg.batch.Enqueue(pending10h(8)...)
			for _, n := range c.placed {
				rg.create(noHandover(mdSpec(n)))
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
	p := &pass{r: &Reconciler{Client: c, Reader: c, Clock: pfake.NewClock(hoT(8, 0, 0))}, key: types.NamespacedName{Namespace: "ns", Name: "this"}, now: hoT(8, 0, 0)}
	snap, err := readSlotSnapshot(context.Background(), c, "prov-x", true)
	if err != nil {
		t.Fatal(err)
	}
	h, err := p.headroomOf(&providerView{name: "prov-x"})(context.Background(), snap)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

// TestHeadroomCountsOnlyTheOtherDeploymentsThatAllowTheProvider: H, read from a hand-built store through headroomOf, for the
// Provider prov-x asked by ns/this (which itself misses a primary: that never counts). Each row lists the others and the
// H it gives, counted by hand:
//   - none: H = 0;
//   - o1 allows prov-x and has no Instance: 1;  o1 and o7 (an Instance whose Allocation was never made, so a primary still
//     missing by Allocation presence): 2;  and o8, which allows prov-x by a selector on the Provider's labels: 3;
//   - o9 wants 2 Instances and has none: its 2 missing primaries count, 2 (as primaries, not as one deployment);
//   - o2 allows another Provider only, o3 has its Allocation, o4 is being deleted, o5 wants no Instance, o6 is past its end
//     time, and this deployment is missing a primary itself: none of them counts, H = 0, a count that took any of
//     them would be 1 or more; with o1 and o7 beside them, which do count, 2.
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
			case "o9":
				f.md("o9", 2, []string{"prov-x"}, nil) // wants 2 Instances, has none: two missing primaries
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
		{"no other deployment: H = 0", nil, 0},
		{"o1 (allows prov-x, no Instance)", []string{"o1"}, 1},
		{"o1 and o7 (an Instance with no Allocation)", []string{"o1", "o7"}, 2},
		{"o1, o7 and o8 (allowed by a selector)", []string{"o1", "o7", "o8"}, 3},
		{"o2 (another Provider), o3 (has its Allocation), o4 (deleting), o5 (wants none), o6 (past its end time) and this one: none counts", []string{"o2", "o3", "o4", "o5", "o6"}, 0},
		{"those five with o1 and o7: only o1 and o7 count", []string{"o2", "o3", "o4", "o5", "o6", "o1", "o7"}, 2},
		{"o9 wants 2 Instances and has none: it counts for its 2 missing primaries, as primaries", []string{"o9"}, 2},
		{"o1 and o9: 1 + 2 missing primaries", []string{"o1", "o9"}, 3},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := build(c.others...).h(t); got != c.want {
				t.Fatalf("H = %d, want %d", got, c.want)
			}
		})
	}
}

// TestADeploymentMissingSeveralPrimariesLeavesASlotForEach: H counts missing primaries, as primaries (#711). Limit 3. md-e (k = 1,
// end time 14:00) and md-z (k = 0, instances 2) are created together; md-e's pass comes first, md-z has no Instance: 2 missing
// primaries, H = 2. md-e's primary makes 1 listed; its extra needs 1 + 0 + 1 + 2 = 4 > 3: refused. md-z then submits both of its
// primaries: 3 listed, the limit. Counting md-z as one deployment (H = 1), the extra would be admitted (1 + 0 + 1 + 1 = 3), md-z
// would submit one primary and its second would wait behind the extra for a slot it was missing when the extra was checked.
// Expected: md-e 1 Allocation, md-z 2.
func TestADeploymentMissingSeveralPrimariesLeavesASlotForEach(t *testing.T) {
	log := newCauseLog()
	rg := newRig(t, append(limitOpts(3), withInterceptors(log.funcs()))...)
	rg.batch.Enqueue(pending10h(6)...)
	rg.create(extraMD("md-e"))
	z := mdSpec("md-z")
	z.Spec.Instances = 2
	rg.create(z)
	tick10(rg) // 08:00:10: md-e's pass, then md-z's
	for range 3 {
		tick10(rg)
	}
	if e, zz := len(rg.allocations("md-e")), len(rg.allocations("md-z")); e != 1 || zz != 2 {
		t.Fatalf("md-e has %d Allocations and md-z %d, want 1 and 2: md-z misses two primaries, each leaves a slot, so md-e's extra (1 + 0 + 1 + 2 = 4 > 3) is refused and both of md-z's submit", e, zz)
	}
	if n := cancelsOf(log); n != 0 {
		t.Fatalf("%d Allocations were deleted, want none", n)
	}
}

// TestAnArrivingPrimaryNeverCostsAnExtraItsSlotAndNothingIsCancelled: the ruling's case, worked by hand. Limit 4 first: md-a
// (k = 1, end time 14:00) makes its primary and its extra at 08:00:10 (1 listed: 1 + 0 + 1 + H(0) = 2 <= 4: admitted; 2 listed).
// md-b (k = 1) and md-c (k = 0) are created together at 08:00:20: md-b's pass comes first, md-c has no Allocation, so H = 1
// for the other that is missing; md-b's primary makes 3 listed, and its extra needs 3 + 0 + 1 + 1 = 5 > 4: refused. md-c's
// primary then submits: 4 listed, the limit. Nothing is cancelled, and every deployment has its primary. Second: limit 5, md-a
// and md-b each with primary and extra (4 listed: md-a's extra, 1 + 0 + 1 + H(1: md-b is missing) = 3 <= 5, and md-b's,
// 3 + 0 + 1 + H(0) = 4 <= 5, are admitted, md-c not yet created);
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
		tick10(rg) // 08:00:10: md-a: primary, extra (1 + 0 + 1 + 1 = 3); md-b: primary (3 listed), extra 3 + 0 + 1 + 0 = 4 <= 5
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

// headroomProbe watches the headroom's own read: the List of ModelDeployments of every namespace, made once per extra's
// reservation (a pass reads its deployment by Get, and setup's Step is made before the probe is armed). On each such read
// it records whether the Provider's lock is held, by a TryLock that must fail (the caller holds it), and runs an optional
// hook before the read goes on.
type headroomProbe struct {
	mu       sync.Mutex
	armed    bool
	locks    *slotTable
	provider string
	reads    int
	unlocked int
	inside   int // reads of the headroom in progress now
	met      int // the most reads in progress at once: 2 would be two passes reading the headroom together
	hook     func()
}

func (b *headroomProbe) funcs(base interceptor.Funcs) interceptor.Funcs {
	base.List = func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
		if _, ok := list.(*v1alpha1.ModelDeploymentList); ok {
			b.mu.Lock()
			armed, hook := b.armed, b.hook
			if armed {
				b.reads++
				b.hook = nil
			}
			b.mu.Unlock()
			if armed {
				l := b.locks.slotLock(b.provider)
				if l.TryLock() {
					l.Unlock()
					b.mu.Lock()
					b.unlocked++
					b.mu.Unlock()
				}
				if hook != nil {
					hook()
				}
				b.mu.Lock()
				b.inside++
				b.met = max(b.met, b.inside)
				b.mu.Unlock()
				defer func() {
					b.mu.Lock()
					b.inside--
					b.mu.Unlock()
				}()
			}
		}
		return c.List(ctx, list, opts...)
	}
	return base
}

func (b *headroomProbe) arm(hook func()) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.armed, b.hook = true, hook
}

// TestTwoPassesCannotBothTakeTheLastHeadroomSlot: limit 5, md-a and md-b (k = 1, end time 14:00) reconciled AT THE SAME
// MOMENT, each placing its primary and then asking for its extra, with md-y and md-z (k = 0) created and never reconciled: two
// missing primaries the Provider allows, so H is 2 for each (3 while the other of the pair has no Allocation yet). An extra
// needs total + 0 + 1 + H <= 5. Whatever the interleaving, exactly one is admitted: the first to check sees 2 + 1 + 2 = 5 (or
// 1 + 1 + 3 = 5) and takes it, the other sees 3 + 1 + 2 = 6; both can be refused only if the second checks before the first's
// primary stands, which the order of a pass (its primary, then its extra) rules out. So 3 Allocations, never 4. The probe is
// armed after setup, so it engages on the headroom's own reads, and asserts from INSIDE each that the Provider's lock is held:
// checked outside it, the count and the headroom would be read by two passes at once.
func TestTwoPassesCannotBothTakeTheLastHeadroomSlot(t *testing.T) {
	probe := &headroomProbe{provider: rigProvider}
	log := newCauseLog()
	rg := newRig(t, append(limitOpts(5), withInterceptors(probe.funcs(log.funcs())))...)
	probe.locks = rg.r.slots()
	rg.batch.Enqueue(pending10h(6)...)
	rg.Step() // the Provider's first status
	rg.create(extraMD("md-a"))
	rg.create(extraMD("md-b"))
	rg.create(mdSpec("md-y"))
	rg.create(mdSpec("md-z"))
	rg.clock.Advance(10 * time.Second)
	syncProviders(rg)
	probe.arm(nil)
	for _, err := range rg.reconcileAtOnce(rg.r, "md-a", "md-b") {
		if err != nil {
			t.Fatalf("a pass failed: %v", err)
		}
	}
	probe.mu.Lock()
	reads, unlocked, met := probe.reads, probe.unlocked, probe.met
	probe.mu.Unlock()
	if reads < 2 || unlocked != 0 || met != 1 {
		t.Fatalf("%d headroom reads, %d of them with the Provider's lock free, %d in progress at once, want at least 2, none and 1: the headroom is read under the lock, so two passes' reads never meet", reads, unlocked, met)
	}
	a, b := len(rg.allocations("md-a")), len(rg.allocations("md-b"))
	if a < 1 || b < 1 || a+b != 3 {
		t.Fatalf("md-a has %d Allocations and md-b %d, want each its primary and exactly one of them an extra: 3 in all, the count never above the limit less the headroom", a, b)
	}
}

// TestAWarmPlacementCannotSlipBetweenTheCountAndTheHeadroom: the delta review's scenario. Limit 4. md-b ran and was scaled to
// zero, so its warm Allocation W holds a slot (1 listed); md-a (k = 1, end time 14:00) and md-c (k = 0) are created and md-b is
// scaled back to 1 at 08:02:00, so md-b and md-c each miss a primary and the Provider allows both. md-a's pass creates its
// primary (2 listed: W and P_a) and asks for its extra: it reads the count under the Provider's lock, 2 listed and no
// reserved slot, and then the headroom, H = 2 (md-b and md-c). 2 + 0 + 1 + 2 = 5 > 4: refused. While md-a is between the two
// reads, md-b's pass runs at once, held until it has either created its Instance or 2 s have passed: placing its primary on W
// makes a Running primary that reserves a handover slot (1) and ends md-b's missing primary (H = 1), so a placement that did not
// take the Provider's lock would let md-a combine the old count (2) with the new headroom (1): 2 + 0 + 1 + 1 = 4 <= 4, admitted,
// and md-c's primary then finds 3 listed + 1 reserved = 4. With the lock md-b's pass waits for md-a's reservation and places
// after it. Expected: md-a has 1 Allocation (no extra), md-b is on W, and md-c's primary is submitted (2 listed + 1 reserved = 3 < 4).
func TestAWarmPlacementCannotSlipBetweenTheCountAndTheHeadroom(t *testing.T) {
	probe := &headroomProbe{provider: rigProvider}
	bPlaced := make(chan struct{})
	var once sync.Once
	log := newCauseLog()
	hooks := interceptor.Funcs{Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
		if err := c.Create(ctx, obj, opts...); err != nil {
			return err
		}
		if in, ok := obj.(*v1alpha1.Instance); ok && in.Labels[v1alpha1.LabelModelDeployment] == "md-b" {
			once.Do(func() { close(bPlaced) })
		}
		return nil
	}}
	rg := newRig(t, append(limitOpts(4), withInterceptors(probe.funcs(mergeFuncs(log.funcs(), hooks))))...)
	probe.locks = rg.r.slots()
	rg.create(mdSpec("md-b"))
	rg.run(70*time.Second, 10*time.Second) // md-b is ready at 08:01:10
	if rg.readyCount("md-b") != 1 {
		t.Fatalf("md-b has %d ready Instances at 08:01:10, want 1", rg.readyCount("md-b"))
	}
	// md-b's first Instance, made above, is not the placement under test: the signal is renewed for the next one.
	bPlaced = make(chan struct{})
	once = sync.Once{}
	rg.mutateMD("md-b", func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 0 })
	rg.until(2*time.Minute, 10*time.Second, "md-b has no live Instance", func() bool { return len(rg.liveInstances("md-b")) == 0 })
	if n := len(rg.allocations("md-b")); n != 1 || allocReleased(rg, rg.allocations("md-b")[0].Name) {
		t.Fatalf("md-b has %d Allocations, want its warm one kept: the row proves nothing otherwise", n)
	}
	rg.run(30*time.Second, 10*time.Second)
	rg.create(extraMD("md-a"))
	rg.create(mdSpec("md-c"))
	rg.mutateMD("md-b", func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 1 })
	rg.clock.Advance(10 * time.Second)
	syncProviders(rg)
	bDone := make(chan error, 1)
	probe.arm(func() {
		go func() { bDone <- rg.reconcileOf("md-b") }()
		select {
		case <-bPlaced:
		case <-time.After(2 * time.Second):
		}
	})
	if err := rg.reconcileOf("md-a"); err != nil {
		t.Fatalf("md-a's pass: %v", err)
	}
	if err := <-bDone; err != nil {
		t.Fatalf("md-b's pass: %v", err)
	}
	if err := rg.reconcileOf("md-c"); err != nil {
		t.Fatalf("md-c's pass: %v", err)
	}
	probe.mu.Lock()
	reads := probe.reads
	probe.mu.Unlock()
	if reads != 1 {
		t.Fatalf("%d headroom reads, want md-a's one: the row's interleaving did not happen", reads)
	}
	if live := rg.liveInstances("md-b"); len(live) != 1 || live[0].Spec.AllocationRef.Name != rg.allocations("md-b")[0].Name {
		t.Fatalf("md-b's live Instances %+v, want one, on its warm Allocation", live)
	}
	if a := len(rg.allocations("md-a")); a != 1 {
		t.Fatalf("md-a has %d Allocations, want its primary alone: its extra was admitted into a slot md-b's handover and md-c's primary need (a count of 2 read before md-b's placement, a headroom of 1 read after it)", a)
	}
	if c := len(rg.allocations("md-c")); c != 1 {
		t.Fatalf("md-c has %d Allocations, want its primary, submitted in the slot the headroom kept", c)
	}
}

// mergeFuncs is a with b's Create after a's: only the Create of b and the Delete and Update of a are used by the rows that
// call it.
func mergeFuncs(a, b interceptor.Funcs) interceptor.Funcs {
	out := a
	out.Create = b.Create
	return out
}

// TestTheHeadroomsClockIsReadUnderTheLock: the end-time test of H uses the clock read when the headroom runs, with the
// Provider's lock held, not the pass's own p.now from before it waited for the lock. Limit 4. md-a (k = 0) is placed (1
// listed). md-e (k = 1, end time 14:00), md-y (k = 0) and md-z (k = 0, end time 08:00:30) are created, md-y and md-z never
// reconciled: two missing primaries the Provider allows. The Provider's lock is held by the row, and md-e's pass at 08:00:20
// starts and, once it has read its clock (p.now = 08:00:20) and gone on to the Providers, waits for the lock at its primary's
// reservation; the clock then moves to 08:00:35 and the lock is released. md-e's primary is reserved, and its extra's
// headroom, read at 08:00:35, finds md-z past its end time: H = 1 (md-y), 2 + 0 + 1 + 1 = 4 <= 4, admitted. With p.now H would
// be 2 and the extra refused for a deployment that makes no primary.
func TestTheHeadroomsClockIsReadUnderTheLock(t *testing.T) {
	var mu sync.Mutex
	var armed bool
	started := make(chan struct{})
	var once sync.Once
	hooks := interceptor.Funcs{List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
		if _, ok := list.(*v1alpha1.ProviderList); ok {
			mu.Lock()
			a := armed
			mu.Unlock()
			if a {
				once.Do(func() { close(started) })
			}
		}
		return c.List(ctx, list, opts...)
	}}
	rg := newRig(t, append(limitOpts(4), withInterceptors(hooks))...)
	rg.batch.Enqueue(pending10h(4)...) // pending jobs: no Running primary reserves a handover slot; md-a's ends before its drain start, so its pending one will hold none either
	rg.create(noHandover(mdSpec("md-a")))
	tick10(rg) // 08:00:10: md-a's primary
	end := metav1.NewTime(hoT(8, 0, 30))
	z := mdSpec("md-z")
	z.Spec.EndTime = &end
	rg.create(extraMD("md-e"))
	rg.create(mdSpec("md-y"))
	rg.create(z)
	rg.clock.Advance(10 * time.Second) // 08:00:20
	syncProviders(rg)
	lock := rg.r.slots().slotLock(rigProvider)
	lock.Lock()
	mu.Lock()
	armed = true
	mu.Unlock()
	done := make(chan error, 1)
	go func() { done <- rg.reconcileOf("md-e") }()
	select {
	case <-started: // md-e's pass has read its clock and is loading the Providers
	case <-time.After(30 * time.Second):
		lock.Unlock()
		t.Fatal("md-e's pass never reached the Providers")
	}
	rg.clock.Advance(15 * time.Second) // 08:00:35: md-z's end time has passed while the pass waits
	lock.Unlock()
	if err := <-done; err != nil {
		t.Fatalf("md-e's pass: %v", err)
	}
	if n := len(rg.allocations("md-e")); n != 2 {
		t.Fatalf("md-e has %d Allocations, want its primary and its extra: md-z passed its end time while the pass waited for the lock, so H is 1 (md-y), 2 + 0 + 1 + 1 = 4 <= 4", n)
	}
}

// TestAnExpectationThatLapsesDuringTheHeadroomScanStillCounts: the count's expectations are judged at the moment the Allocations
// were listed, before the headroom's reads, never after them. An expectation raised at 08:00:00 for Allocation ns/k, which the
// first list does not show; the headroom's read (a hook here) creates ns/k and moves the clock to 08:05:01, past the five
// minutes an expectation lives. Judged at the list, 08:00:00, it is live and counts: 0 listed + 1 expected = 1 >= the limit
// of 1, so the extra is refused. Judged after the scan, at 08:05:01, it would lapse while the list, taken before ns/k existed,
// misses the Allocation: a count of 0, and the extra takes a protected slot.
func TestAnExpectationThatLapsesDuringTheHeadroomScanStillCounts(t *testing.T) {
	now := hoT(8, 0, 0)
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c := clientfake.NewClientBuilder().WithScheme(scheme).Build()
	tab := newSlotTable()
	tab.expect["prov"] = map[string]time.Time{"ns/k": now}
	limit := int32(1)
	hook := func(ctx context.Context, _ slotSnapshot) (int, error) {
		k := &v1alpha1.Allocation{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "k"}}
		k.Spec.ProviderConfigReference = providerRef("prov")
		if err := c.Create(ctx, k); err != nil {
			return 0, err
		}
		now = now.Add(5*time.Minute + time.Second)
		return 0, nil
	}
	ans, err := tab.reserveAsk(context.Background(), c, slotAsk{provider: "prov", key: "ns/x", limit: &limit, headroom: hook}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if ans.ok {
		t.Fatalf("the extra was admitted (count %d): the expectation for ns/k lapsed during the headroom scan while the list missed the Allocation, and a protected slot went", ans.count)
	}
}

// deadlineReader records the deadline of the context of each List of Allocations.
type deadlineReader struct {
	client.Reader
	mu        sync.Mutex
	deadlines []time.Time
}

func (d *deadlineReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if _, ok := list.(*v1alpha1.AllocationList); ok {
		if dl, ok := ctx.Deadline(); ok {
			d.mu.Lock()
			d.deadlines = append(d.deadlines, dl)
			d.mu.Unlock()
		}
	}
	return d.Reader.List(ctx, list, opts...)
}

// TestTheCountAndTheHeadroomShareOneDeadline: a reservation's reads under the Provider's lock, the count's Lists and the
// headroom's, run under ONE deadline, listTimeout from the lock's acquisition, so a slow count leaves the headroom what remains,
// not a second listTimeout: a stalled headroom would otherwise hold every primary of the Provider for two timeouts. The
// deadline the headroom's context carries must be the very one the count's Lists carry.
func TestTheCountAndTheHeadroomShareOneDeadline(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	r := &deadlineReader{Reader: clientfake.NewClientBuilder().WithScheme(scheme).Build()}
	tab := newSlotTable()
	limit := int32(5)
	var headroomDeadline time.Time
	hook := func(ctx context.Context, _ slotSnapshot) (int, error) {
		headroomDeadline, _ = ctx.Deadline()
		return 1, nil
	}
	if _, err := tab.reserveAsk(context.Background(), r, slotAsk{provider: "prov", key: "ns/x", limit: &limit, headroom: hook}, func() time.Time { return hoT(8, 0, 0) }); err != nil {
		t.Fatal(err)
	}
	if headroomDeadline.IsZero() || len(r.deadlines) == 0 {
		t.Fatalf("headroom deadline %v and %d count Lists with a deadline, want both set", headroomDeadline, len(r.deadlines))
	}
	for _, d := range r.deadlines {
		if !d.Equal(headroomDeadline) {
			t.Fatalf("a count List has the deadline %v and the headroom %v, want one deadline for both", d, headroomDeadline)
		}
	}
}

// TestACutShortExtrasRetryKeepsTheHeadroom: the retry of an extra whose Allocation's create was cut short takes the headroom
// like its creation did (E's shared extraChecks). Limit 4. md-e (k = 1, end time 14:00) makes its primary and its extra X at
// 08:00:10: 1 + 0 + 1 + H(0: nobody else exists) = 2 <= 4, admitted, and X's Allocation create is cut short; the controller
// restarts (its reservation is lost). md-b and md-c (k = 0) and md-f (k = 0), created then, are reconciled in name order:
// md-b and md-c take two slots at 08:00:20 (3 listed with md-e's primary), and md-f, which sorts after md-e, is still missing
// when X's retry is checked in md-e's pass: H = 1. X's retry: 3 + 0 + 1 + 1 = 5 > 4: refused, and X is ended through the trim
// path; md-f then submits its primary in the slot (4 listed). Without the headroom at the retry it would be 3 + 0 + 1 = 4 <= 4,
// X would take the last slot and md-f's primary would wait.
// Expected at 08:01:10: X ended with no Allocation; md-e, md-b, md-c and md-f one Allocation each; md-e not SubmitLimitReached.
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
	rg.create(mdSpec("md-f"))
	for range 6 { // 08:00:20 to 08:01:10
		tick10(rg)
	}
	if got := instOrNil(rg, xName); got != nil && !ended(got) {
		t.Fatalf("X is %s at 08:01:10, want it ended: its retry leaves no headroom for md-f's missing primary (3 listed + 1 + 1 > 4)", got.Status.Phase)
	}
	for _, n := range []string{"md-e", "md-b", "md-c", "md-f"} {
		if got := len(rg.allocations(n)); got != 1 {
			t.Fatalf("%s has %d Allocations, want 1: md-f's primary takes the slot the headroom kept", n, got)
		}
	}
	if d := rg.condition("md-e", v1alpha1.ConditionDegraded); d.Reason == v1alpha1.ReasonSubmitLimitReached {
		t.Fatalf("md-e Degraded = %s/%s %q, want no SubmitLimitReached", d.Status, d.Reason, d.Message)
	}
	if n := cancelsOf(log); n != 0 {
		t.Fatalf("%d Allocations deleted, want none: X had no Allocation", n)
	}
}
