package lifecycle

// The extra's unit set (extra_units.go; #751's fix round for #725, the findings of Astra's review, A1 to A4, and Carbon's, C1
// to C3). One reader under the Provider's lock gives the extra's two terms, the slots it leaves beside itself and the sizes its
// per-user caps check adds, for every deployment's handovers. Each row works its count out by hand, at the table, on a fake
// client that holds the deployments, Instances and Allocations the lock's reads would list. One GPU per Allocation, a 4 h time
// limit, a drain window of 2 h 10 min, so an Instance created at 08:00:10 hands over from 09:50:10 and a deployment that ends
// at 09:30 never does.

import (
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// unitFx is the objects a Provider's reads would list: deployments, their Instances and their Allocations.
type unitFx struct{ objs []client.Object }

var unitProfile = &v1alpha1.PlatformProfileSpec{DrainWindow: dur(2*time.Hour + 10*time.Minute), DrainMargin: dur(10 * time.Minute)}

// oneGPU is what a new Allocation of these deployments requests: one GPU for 4 h.
func oneGPU() v1alpha1.BatchParameters {
	return v1alpha1.BatchParameters{GPUs: 1, CPUsPerTask: 16, Memory: resource.MustParse("120Gi"), TimeLimit: "4h", Nodes: 1}
}

func (f *unitFx) md(name string, end *time.Time) *v1alpha1.ModelDeployment {
	m := &v1alpha1.ModelDeployment{ObjectMeta: metav1.ObjectMeta{Namespace: rigNS, Name: name, UID: types.UID("uid-" + name)}}
	m.Spec.Instances = 1
	m.Spec.Source.Served = &v1alpha1.ServingSpec{}
	if end != nil {
		e := metav1.NewTime(*end)
		m.Spec.EndTime = &e
	}
	f.objs = append(f.objs, m)
	return m
}

// inst is an Instance of the deployment on the provider, created at the time, with the Allocation name alloc-<name>.
func (f *unitFx) inst(mdName, name, prov string, uid string, created time.Time, phase v1alpha1.InstancePhase, ann map[string]string) *v1alpha1.Instance {
	in := &v1alpha1.Instance{ObjectMeta: metav1.ObjectMeta{Namespace: rigNS, Name: name, UID: types.UID(uid), CreationTimestamp: metav1.NewTime(created), Annotations: ann,
		Labels:          map[string]string{v1alpha1.LabelProvider: prov, v1alpha1.LabelModelDeployment: mdName},
		OwnerReferences: []metav1.OwnerReference{{APIVersion: v1alpha1.GroupVersion.String(), Kind: "ModelDeployment", Name: mdName, UID: types.UID("uid-" + mdName), Controller: ptrTo(true)}}},
		Spec: v1alpha1.InstanceSpec{AllocationRef: v1alpha1.ObjectReference{Name: "alloc-" + name}, Launch: v1alpha1.Launch{GPUs: 1}}}
	in.Status.Phase = phase
	if phase == v1alpha1.InstanceReady {
		rt, le := metav1.NewTime(hoT(8, 1, 10)), metav1.NewTime(hoT(12, 0, 20))
		in.Status.ReadyTime, in.Status.LeaseEnd = &rt, &le
	}
	f.objs = append(f.objs, in)
	return in
}

// alloc is the Allocation of the Instance: Pending, or Running (job started at 08:00:20, ending 12:00:20).
func (f *unitFx) alloc(mdName, name, prov string, running bool) {
	a := &v1alpha1.Allocation{ObjectMeta: metav1.ObjectMeta{Namespace: rigNS, Name: "alloc-" + name, UID: types.UID("auid-" + name),
		Labels: map[string]string{v1alpha1.LabelProvider: prov, v1alpha1.LabelModelDeployment: mdName}}}
	a.Spec.ProviderConfigReference = providerRef(prov)
	b := oneGPU()
	a.Spec.ForProvider.Batch = &b
	if running {
		s, e := metav1.NewTime(hoT(8, 0, 20)), metav1.NewTime(hoT(12, 0, 20))
		a.Status.AtProvider.Phase, a.Status.AtProvider.StartTime, a.Status.AtProvider.HardEnd = v1alpha1.AllocationRunning, &s, &e
	} else {
		a.Status.AtProvider.Phase = v1alpha1.AllocationPending
	}
	f.objs = append(f.objs, a)
}

func (f *unitFx) reader(t *testing.T) client.Reader { return f.client(t) }

// client is the fixtures' fake client, which a hook may add objects to.
func (f *unitFx) client(t *testing.T) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(f.objs...).Build()
}

// hidden is a Reader whose Provider-wide list of Allocations (the one with no label selector) lacks the named Allocation, as
// the list of a read made before the Allocation was created does, while the lists by label, made after, show it.
type hidden struct {
	client.Reader
	name string
}

func (h hidden) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if err := h.Reader.List(ctx, list, opts...); err != nil {
		return err
	}
	if al, ok := list.(*v1alpha1.AllocationList); ok {
		lo := &client.ListOptions{}
		for _, o := range opts {
			o.ApplyToList(lo)
		}
		if lo.LabelSelector == nil {
			kept := al.Items[:0]
			for _, a := range al.Items {
				if a.Name != h.name {
					kept = append(kept, a)
				}
			}
			al.Items = kept
		}
	}
	return nil
}

// recordedSize is what the Allocation of an Instance that exists requests: its recorded launch's GPUs and workers, the rest as
// oneGPU (a fixture's Instance records one GPU a worker).
func recordedSize(_ *v1alpha1.ModelDeployment, in *v1alpha1.Instance) (v1alpha1.BatchParameters, error) {
	b := oneGPU()
	b.GPUs = in.Spec.Launch.GPUs * v1alpha1.LaunchWorkers(in.Spec.Launch)
	return b, nil
}

// stubSizing is the unit set's sizing in the table-level rows: a replacement that does not exist yet is one GPU (oneGPU), and
// an existing Instance's Allocation is sized by recorded.
func stubSizing(recorded func(*v1alpha1.ModelDeployment, *v1alpha1.Instance) (v1alpha1.BatchParameters, error)) func(context.Context) (unitSizing, error) {
	return func(context.Context) (unitSizing, error) {
		return unitSizing{
			sizeOf:       func(*v1alpha1.ModelDeployment) (v1alpha1.BatchParameters, error) { return oneGPU(), nil },
			sizeRecorded: recorded,
		}, nil
	}
}

// extraSlotAsk is the extra's reservation for a new Allocation of one GPU, on the Provider with a submit limit and a GPU cap
// (nil: none), asked by the deployment, leaving the Instance out of it. The sizing stubs size a new Allocation of any deployment
// at one GPU and an existing Instance's at its recorded launch's. THE ONLY CODE HERE THAT NAMES THE UNIT SET'S ASK FIELDS.
func extraSlotAsk(provider, key string, limit, gpuCap *int32, asker string, exclude types.UID) slotAsk {
	size := oneGPU()
	return slotAsk{provider: provider, key: rigNS + "/" + key, limit: limit, params: &size, caps: v1alpha1.QOSLimits{MaxGPUsPerUser: gpuCap}, profile: unitProfile,
		deployment: asker, unitExclude: exclude, unitSizing: stubSizing(recordedSize)}
}

func ask(t *testing.T, tab *slotTable, r client.Reader, q slotAsk) slotAnswer {
	t.Helper()
	a, err := tab.reserveAsk(context.Background(), r, q, fixedAt(hoT(8, 0, 30)))
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// TestAnExtraLeavesThePrimarysHandoverSlotWhoseAllocationAppearsBetweenTheReads (A1; re-anchored by #751's delta, D1): limit 3 on
// provider-a. md-e (k = 1, ends at 09:30: no handover of its own) has its primary pe, listed. md-o has no end time, so its
// primary po will hold a slot for its replacement once Running. po's reservation is in the table (an expectation: one slot),
// and its Allocation is made AFTER the snapshot's Allocation list, which does not show it, while its Instance is in the
// snapshot's Instance list (the hidden Reader hides the one Allocation from the one Allocation list; it once hid it from the
// Provider-wide list only, beside the by-label lists that showed it). An Instance with no Allocation in the snapshot is in
// flight, so its handover counts. The extra's check: listed 1 (pe) + expected 1 (po) + po's future slot 1 + the extra 1 = 4 >
// 3, refused; the first reader dropped po (its Allocation absent from the Provider's list) and counted 3 <= 3. With a limit of
// 4 the extra is admitted (3 <= 4), and when md-o ends at 09:30 (po never hands over) it is admitted at 3.
func TestAnExtraLeavesThePrimarysHandoverSlotWhoseAllocationAppearsBetweenTheReads(t *testing.T) {
	end := hoT(9, 30, 0)
	for _, c := range []struct {
		name  string
		limit int32
		oEnds bool
		ok    bool
	}{
		{"limit 3, po hands over: 4 > 3, refused", 3, false, false},
		{"limit 4, po hands over: 4 <= 4, admitted", 4, false, true},
		{"limit 3, md-o ends before its drain start: 3 <= 3, admitted", 3, true, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := &unitFx{}
			f.md("md-e", &end)
			var oEnd *time.Time
			if c.oEnds {
				oEnd = &end
			}
			f.md("md-o", oEnd)
			f.inst("md-e", "pe", provA, "uid-pe", hoT(8, 0, 10), v1alpha1.InstancePending, nil)
			f.alloc("md-e", "pe", provA, false)
			f.inst("md-o", "po", provA, "uid-po", hoT(8, 0, 10), v1alpha1.InstancePending, nil)
			f.alloc("md-o", "po", provA, false)
			tab := newSlotTable()
			lim := i32(c.limit)
			// po's own reservation, made before its Allocation: the expectation the extra's count reads.
			if a := ask(t, tab, hidden{f.reader(t), "alloc-po"}, slotAsk{provider: provA, key: rigNS + "/alloc-po", limit: lim}); !a.ok {
				t.Fatalf("po's reservation refused with a count of %d", a.count)
			}
			a := ask(t, tab, hidden{f.reader(t), "alloc-po"}, extraSlotAsk(provA, "alloc-x", lim, nil, rigNS+"/md-e", ""))
			if a.ok != c.ok {
				t.Fatalf("extra ok %v with a count of %d, want ok %v", a.ok, a.count, c.ok)
			}
		})
	}
}

// TestADeploymentCountsExactlyItsChosenUnitWhicheverInstanceTheUIDTiePicks (C1): md-a (k = 1, no end time) has pending
// primary p, with its Allocation listed, and extra x, in flight (its Instance made, its Allocation not yet), created in the
// same second, in both UID orders. The selection chooses one of them, and exactly ONE handover is left room for. md-e's extra
// on provider-a, with its primary listed (md-e ends at 09:30), counts by hand: listed p 1 + pe 1 = 2; at limit 3 one unit
// gives 2 + 1 >= 3, refused, and at limit 4 it gives 3 < 4, admitted (two units would be refused at 4). The old reader dropped
// both when x was chosen (an in-flight Instance was skipped and p was surplus): admitted at 3.
func TestADeploymentCountsExactlyItsChosenUnitWhicheverInstanceTheUIDTiePicks(t *testing.T) {
	end := hoT(9, 30, 0)
	for _, c := range []struct{ name, uidP, uidX string }{{"x has the lower UID", "uid-b", "uid-a"}, {"p has the lower UID", "uid-a", "uid-b"}} {
		t.Run(c.name, func(t *testing.T) {
			f := &unitFx{}
			f.md("md-e", &end)
			f.md("md-a", nil)
			f.inst("md-e", "pe", provA, "uid-pe", hoT(8, 0, 10), v1alpha1.InstancePending, nil)
			f.alloc("md-e", "pe", provA, false)
			f.inst("md-a", "p", provA, c.uidP, hoT(8, 0, 10), v1alpha1.InstancePending, nil)
			f.alloc("md-a", "p", provA, false)
			f.inst("md-a", "x", provA, c.uidX, hoT(8, 0, 10), v1alpha1.InstancePending, nil) // no Allocation: in flight
			if a := ask(t, newSlotTable(), f.reader(t), extraSlotAsk(provA, "alloc-ex", i32(3), nil, rigNS+"/md-e", "")); a.ok {
				t.Fatalf("limit 3: admitted with a count of %d, want refused: md-a's one unit is left room for", a.count)
			}
			if a := ask(t, newSlotTable(), f.reader(t), extraSlotAsk(provA, "alloc-ex", i32(4), nil, rigNS+"/md-e", "")); !a.ok {
				t.Fatalf("limit 4: refused with a count of %d, want admitted: md-a counts ONE unit, not two", a.count)
			}
		})
	}
}

// TestAnExtrasRetryDoesNotCountItselfBesideItsPendingPrimary (C1's retry; U2): md-a asks, and its extra x, made first by a
// pass cut short (its Instance exists, no Allocation), retries beside its pending primary p, in both UID orders. The asking
// Instance is left out of md-a's selection, so p is the chosen unit and counts alone: listed p 1 + p's handover slot 1 = 2, so
// x's reservation at limit 3 is admitted (2 < 3) and refused at limit 2 (2 >= 2). md-a's own count is the same as any
// deployment's.
func TestAnExtrasRetryDoesNotCountItselfBesideItsPendingPrimary(t *testing.T) {
	for _, c := range []struct{ name, uidP, uidX string }{{"x has the lower UID", "uid-b", "uid-a"}, {"p has the lower UID", "uid-a", "uid-b"}} {
		t.Run(c.name, func(t *testing.T) {
			f := &unitFx{}
			f.md("md-a", nil)
			f.inst("md-a", "p", provA, c.uidP, hoT(8, 0, 10), v1alpha1.InstancePending, nil)
			f.alloc("md-a", "p", provA, false)
			f.inst("md-a", "x", provA, c.uidX, hoT(8, 0, 10), v1alpha1.InstancePending, nil)
			if a := ask(t, newSlotTable(), f.reader(t), extraSlotAsk(provA, "alloc-x", i32(3), nil, rigNS+"/md-a", types.UID(c.uidX))); !a.ok {
				t.Fatalf("limit 3: refused with a count of %d, want admitted", a.count)
			}
			if a := ask(t, newSlotTable(), f.reader(t), extraSlotAsk(provA, "alloc-x", i32(2), nil, rigNS+"/md-a", types.UID(c.uidX))); a.ok {
				t.Fatalf("limit 2: admitted with a count of %d, want refused: p's handover slot is left", a.count)
			}
		})
	}
}

// TestAReplacementInFlightIsCountedAtItsOwnSizeOnceAndNotTwiceOnceListed (C3): md-a's primary p is Running, 1 GPU, and its
// replacement r, made by a pass, has no Allocation yet (in flight; the replacement's reservation asked no caps, so no size is
// recorded for it). A GPU cap of 4 and md-e's extra on provider-a, its primary pe listed (md-e ends at 09:30): by hand, listed
// p 1 + pe 1 + the extra 1 = 3, and r's own Allocation, 1, which nothing counts yet: 4 <= 4, admitted at cap 4 and refused at
// cap 3. p adds nothing for its replacement (it has one: r is it). Once r's Allocation is listed (Pending) it counts as listed,
// once: the same 4 <= 4 at cap 4, never 5, and 4 > 3 at cap 3. The old reader ignored r's size in flight: admitted at cap 3.
func TestAReplacementInFlightIsCountedAtItsOwnSizeOnceAndNotTwiceOnceListed(t *testing.T) {
	end := hoT(9, 30, 0)
	for _, listed := range []bool{false, true} {
		name := "r in flight"
		if listed {
			name = "r's Allocation listed"
		}
		t.Run(name, func(t *testing.T) {
			f := &unitFx{}
			f.md("md-e", &end)
			f.md("md-a", nil)
			f.inst("md-e", "pe", provA, "uid-pe", hoT(8, 0, 10), v1alpha1.InstancePending, nil)
			f.alloc("md-e", "pe", provA, false)
			f.inst("md-a", "p", provA, "uid-p", hoT(8, 0, 10), v1alpha1.InstanceReady, nil)
			f.alloc("md-a", "p", provA, true)
			f.inst("md-a", "r", provA, "uid-r", hoT(9, 19, 30), v1alpha1.InstancePending, map[string]string{v1alpha1.AnnotationPredecessor: "p"})
			if listed {
				f.alloc("md-a", "r", provA, false)
			}
			if a := ask(t, newSlotTable(), f.reader(t), extraSlotAsk(provA, "alloc-ex", nil, i32(4), rigNS+"/md-e", "")); !a.ok {
				t.Fatalf("cap 4: refused (%s), want admitted: 4 GPUs fit, r counted once", a.capsWhy)
			}
			if a := ask(t, newSlotTable(), f.reader(t), extraSlotAsk(provA, "alloc-ex", nil, i32(3), rigNS+"/md-e", "")); a.ok {
				t.Fatal("cap 3: admitted, want refused: r's Allocation needs the 4th GPU")
			}
		})
	}
}

// TestAnUnresolvedHedgesMembersCountByTheRule (U5; A4 and C2): the science agent ruled countEachMember true (#725, 18 Nov
// 2026): each live member of an unresolved hedge counts on its own Provider, so a hedge counts once on each Provider it has
// a member on, and a deployment's own sibling counts for its own extra. The hedge {m1 on provider-a, m2 on provider-b},
// both pending, md-h has no end time, so each member will hold a handover slot if it wins. On provider-b, limit 3, with
// md-e's primary pe listed there (md-e ends at 09:30) and m2's Allocation listed: listed 2, so by hand:
//   - A4, md-e asks for its extra on provider-b: true counts m2 (2 + 1 >= 3, refused: the ruled over-conservatism, a stated
//     limit); false counts the hedge at its representative's Provider, provider-a, so none on provider-b (2 < 3, admitted);
//   - C2, md-h itself asks for its extra on provider-b (its own sibling m2 there, with the extra's bound already met): true
//     counts m2 (refused), false counts none (admitted). The old reader looked at the representative's Provider alone and
//     counted none for the asker's own: admitted.
func TestAnUnresolvedHedgesMembersCountByTheRule(t *testing.T) {
	end := hoT(9, 30, 0)
	for _, c := range []struct {
		name string
		set  *bool // nil: the rule as shipped, which the science agent ruled true
		each bool
	}{{"the rule as shipped (ruled true)", nil, true}, {"countEachMember true", ptrTo(true), true}, {"countEachMember false", ptrTo(false), false}} {
		each := c.each
		t.Run(c.name, func(t *testing.T) {
			if c.set != nil {
				old := countEachMember
				countEachMember = *c.set
				t.Cleanup(func() { countEachMember = old })
			}
			f := &unitFx{}
			f.md("md-e", &end)
			f.md("md-h", nil)
			f.inst("md-e", "pe", provB, "uid-pe", hoT(8, 0, 10), v1alpha1.InstancePending, nil)
			f.alloc("md-e", "pe", provB, false)
			f.inst("md-h", "m1", provA, "uid-m1", hoT(8, 0, 10), v1alpha1.InstancePending, nil)
			f.alloc("md-h", "m1", provA, false)
			f.inst("md-h", "m2", provB, "uid-m2", hoT(8, 0, 20), v1alpha1.InstancePending, map[string]string{v1alpha1.AnnotationHedgeGroup: "m1"})
			f.alloc("md-h", "m2", provB, false)
			wantOK := !each
			if a := ask(t, newSlotTable(), f.reader(t), extraSlotAsk(provB, "alloc-ex", i32(3), nil, rigNS+"/md-e", "")); a.ok != wantOK {
				t.Fatalf("A4: another deployment's extra on provider-b: ok %v with a count of %d, want %v", a.ok, a.count, wantOK)
			}
			if a := ask(t, newSlotTable(), f.reader(t), extraSlotAsk(provB, "alloc-ex", i32(3), nil, rigNS+"/md-h", "")); a.ok != wantOK {
				t.Fatalf("C2: the hedge's own extra on provider-b: ok %v with a count of %d, want %v", a.ok, a.count, wantOK)
			}
		})
	}
}

// unitsOf is the unit set an extra of the asking deployment would read from the fixtures on the provider, as the reservation
// reads it (reservedAndReadsOn, then extraUnits), with the sizes of the stubs: a replacement that does not exist yet is one
// GPU (oneGPU), an existing Instance's Allocation its recorded launch's (recordedSize).
func unitsOf(t *testing.T, f *unitFx, prov, asker string) []extraUnit {
	t.Helper()
	r := f.reader(t)
	var al v1alpha1.AllocationList
	if err := r.List(context.Background(), &al); err != nil {
		t.Fatal(err)
	}
	allocs := map[string]*v1alpha1.Allocation{}
	for i := range al.Items {
		allocs[al.Items[i].Namespace+"/"+al.Items[i].Name] = &al.Items[i]
	}
	_, reads, err := reservedAndReadsOn(context.Background(), r, prov, unitProfile, allocs)
	if err != nil {
		t.Fatal(err)
	}
	us, _ := stubSizing(recordedSize)(context.Background())
	return extraUnits(reads, unitAsk{deployment: asker, unitSizing: us})
}

// TestAnInFlightReplacementIsCountedAtItsRecordedLaunchAndAFutureOneAtTheSpecNow (U4, k616's precision to #751's fix round,
// aligned with #756): md-a's primary p is Running, 1 GPU, and its replacement r, in flight (no Allocation yet), RECORDED a
// 2-GPU launch (as an Instance made before md-a's spec changed would have). md-b's primary q is Running too, on a 2-GPU
// launch and Allocation, with no replacement: its future one does not exist, so the current spec sizes it, 1 GPU, not q's 2.
// md-e's extra on provider-a, its primary pe listed (md-e ends at 09:30): by hand, listed p 1 + q 2 + pe 1 + the extra 1 = 5,
// plus r's own Allocation at its recorded 2 = 7, plus q's future replacement at the spec's 1 = 8; p adds nothing (r is its
// replacement). So 8 <= 8 admitted at cap 8 and refused at cap 7. Counted at the spec now (1 for r), the total is 7:
// admitted at cap 7 (what c88f8719 did); with q's future at q's own launch (2), 9: refused at cap 8. The units' GPU counts
// are the same sources: r's 2 (recorded), q's replacement's 1 (spec).
func TestAnInFlightReplacementIsCountedAtItsRecordedLaunchAndAFutureOneAtTheSpecNow(t *testing.T) {
	end := hoT(9, 30, 0)
	f := &unitFx{}
	f.md("md-e", &end)
	f.md("md-a", nil)
	f.md("md-b", nil)
	f.inst("md-e", "pe", provA, "uid-pe", hoT(8, 0, 10), v1alpha1.InstancePending, nil)
	f.alloc("md-e", "pe", provA, false)
	f.inst("md-a", "p", provA, "uid-p", hoT(8, 0, 10), v1alpha1.InstanceReady, nil)
	f.alloc("md-a", "p", provA, true)
	q := f.inst("md-b", "q", provA, "uid-q", hoT(8, 0, 10), v1alpha1.InstanceReady, nil)
	f.alloc("md-b", "q", provA, true)
	f.objs[len(f.objs)-1].(*v1alpha1.Allocation).Spec.ForProvider.Batch.GPUs = 2
	q.Spec.Launch.GPUs = 2
	r := f.inst("md-a", "r", provA, "uid-r", hoT(9, 19, 30), v1alpha1.InstancePending, map[string]string{v1alpha1.AnnotationPredecessor: "p"})
	r.Spec.Launch.GPUs = 2
	if a := ask(t, newSlotTable(), f.reader(t), extraSlotAsk(provA, "alloc-ex", nil, i32(8), rigNS+"/md-e", "")); !a.ok {
		t.Fatalf("cap 8: refused (%s), want admitted: 5 listed or asked + r's 2 + q's future 1 = 8", a.capsWhy)
	}
	if a := ask(t, newSlotTable(), f.reader(t), extraSlotAsk(provA, "alloc-ex", nil, i32(7), rigNS+"/md-e", "")); a.ok {
		t.Fatal("cap 7: admitted, want refused: r's Allocation is its recorded 2 GPUs, not the spec's 1 (8 > 7)")
	}
	got := map[string]int32{}
	for _, u := range unitsOf(t, f, provA, rigNS+"/md-e") {
		got[u.deployment+"/"+map[unitRole]string{roleHandover: "handover", roleOwn: "own"}[u.role]] = u.gpus
	}
	want := map[string]int32{rigNS + "/md-a/own": 2, rigNS + "/md-b/handover": 1}
	if len(got) != len(want) || got[rigNS+"/md-a/own"] != 2 || got[rigNS+"/md-b/handover"] != 1 {
		t.Fatalf("units' GPUs = %v, want %v", got, want)
	}
}

// TestAnInFlightPrimarysSlotIsJudgedByItsRecordedLaunchsTimeLimit (U4): md-q ends at 09:30 and its primary q is in flight
// (created 08:00:10, no Allocation yet). A 4 h limit puts q's drain start at 09:50:10, after the end: no handover, so none to
// leave room for; a 1 h limit puts it at 06:50:10, before the end: one. md-e's extra on provider-a (limit 2; pe listed and
// the extra, hand-counted 2): 2 <= 2 admitted without q's unit, 3 > 2 refused with it. The time limit comes from the Instance's recorded launch's
// sizing (sizeRecorded), the same one ensureAllocation will request.
func TestAnInFlightPrimarysSlotIsJudgedByItsRecordedLaunchsTimeLimit(t *testing.T) {
	end := hoT(9, 30, 0)
	for _, tc := range []struct {
		limit string
		ok    bool
	}{{"4h", true}, {"1h", false}} {
		t.Run(tc.limit, func(t *testing.T) {
			f := &unitFx{}
			f.md("md-e", &end)
			f.md("md-q", &end)
			f.inst("md-e", "pe", provA, "uid-pe", hoT(8, 0, 10), v1alpha1.InstancePending, nil)
			f.alloc("md-e", "pe", provA, false)
			f.inst("md-q", "q", provA, "uid-q", hoT(8, 0, 10), v1alpha1.InstancePending, nil)
			q := extraSlotAsk(provA, "alloc-ex", i32(2), nil, rigNS+"/md-e", "")
			q.unitSizing = stubSizing(func(md *v1alpha1.ModelDeployment, in *v1alpha1.Instance) (v1alpha1.BatchParameters, error) {
				b, err := recordedSize(md, in)
				b.TimeLimit = v1alpha1.Duration(tc.limit)
				return b, err
			})
			if a := ask(t, newSlotTable(), f.reader(t), q); a.ok != tc.ok {
				t.Fatalf("limit %s: ok %v with a count of %d, want %v", tc.limit, a.ok, a.count, tc.ok)
			}
		})
	}
}

// TestAnExtrasUnitsAreSummedProviderWideOrPerQOSByOneFilter (#756's meeting point): four units, hand-worked. u1 a future
// replacement's, not reserved, 1 GPU; u2 a Running primary's, reserved, 2; u3 an in-flight replacement's own Allocation,
// reserved, 4, whose Allocation no expectation sizes; u4 a pending primary's, not reserved, 4. Provider-wide: the slots left
// are u1 and u4, 2; the sizes every unit's, 4. Counting the units of 4 GPUs alone: 1 slot (u4), 2 sizes (u3, u4). With an
// expectation sizing u3: Provider-wide 3 sizes, per QOS 1.
func TestAnExtrasUnitsAreSummedProviderWideOrPerQOSByOneFilter(t *testing.T) {
	sz := func(g int32) v1alpha1.BatchParameters { b := oneGPU(); b.GPUs = g; return b }
	units := []extraUnit{
		{uid: "u1", role: roleHandover, size: sz(1), gpus: 1},
		{uid: "u2", role: roleHandover, reserved: true, size: sz(2), gpus: 2},
		{uid: "u3", role: roleOwn, key: "ns/k3", reserved: true, size: sz(4), gpus: 4},
		{uid: "u4", role: roleHandover, size: sz(4), gpus: 4},
	}
	four := func(u extraUnit) bool { return u.gpus == 4 }
	none := func(string) bool { return false }
	sizedK3 := func(k string) bool { return k == "ns/k3" }
	for _, tc := range []struct {
		name   string
		sized  func(string) bool
		in     func(extraUnit) bool
		leave  int
		counts int
	}{
		{"provider-wide", none, nil, 2, 4},
		{"per QOS, 4 GPUs", none, four, 1, 2},
		{"provider-wide, u3 sized by an expectation", sizedK3, nil, 2, 3},
		{"per QOS, u3 sized by an expectation", sizedK3, four, 1, 1},
	} {
		leave, sizes := unitTerms(units, tc.sized, tc.in)
		if leave != tc.leave || len(sizes) != tc.counts {
			t.Fatalf("%s: slots %d and %d sizes, want %d and %d", tc.name, leave, len(sizes), tc.leave, tc.counts)
		}
	}
}

// A deployment md-a that ends at 09:30 has a listed pending primary p, created at 08:00:10 on a 3 h Allocation (so its drain
// start is 08:50:10, before the end: it hands over), and an Instance x made in the same second with no Allocation, in flight or
// a cut-short extra's, whose launch is sized at the lease limit now, 4 h (its drain start would be 09:50:10, after the end: no
// handover) as after a lease increase. x has the lower UID, so the selection's UID tie chooses x and leaves p surplus.
func leaseIncreaseTwins(f *unitFx, end time.Time) {
	f.md("md-a", &end)
	f.inst("md-a", "p", provA, "uid-b", hoT(8, 0, 10), v1alpha1.InstancePending, nil)
	f.alloc("md-a", "p", provA, false)
	f.objs[len(f.objs)-1].(*v1alpha1.Allocation).Spec.ForProvider.Batch.TimeLimit = "3h"
	f.inst("md-a", "x", provA, "uid-a", hoT(8, 0, 10), v1alpha1.InstancePending, nil)
}

// TestAnExtrasRetryKeepsItsListedSiblingsHandoverAfterALeaseIncrease (D6a, M4): md-a asks for its extra x, a retry of the Instance
// x that a pass cut short, beside its listed p (see leaseIncreaseTwins). The asking Instance is left out of md-a's selection by its
// UID, so p is the chosen unit and its handover counts: by hand, listed p 1 + p's handover 1 + the extra 1 = 3, so refused at
// limit 2 and admitted at limit 3. Without the exclusion x is chosen, its drain start (09:50:10) is after the end, and md-a's
// handover is in no count: 2 <= 2, admitted at limit 2.
func TestAnExtrasRetryKeepsItsListedSiblingsHandoverAfterALeaseIncrease(t *testing.T) {
	f := &unitFx{}
	leaseIncreaseTwins(f, hoT(9, 30, 0))
	for _, c := range []struct {
		limit int32
		ok    bool
	}{{2, false}, {3, true}} {
		if a := ask(t, newSlotTable(), f.reader(t), extraSlotAsk(provA, "alloc-x", i32(c.limit), nil, rigNS+"/md-a", "uid-a")); a.ok != c.ok {
			t.Fatalf("limit %d: ok %v with a count of %d, want %v: p's handover is counted beside the retry", c.limit, a.ok, a.count, c.ok)
		}
	}
}

// TestAnotherDeploymentsListedPrimaryIsPreferredToItsInFlightTwin (D6b): md-e's extra on provider-a (md-e ends at 09:30; pe
// listed) beside md-a's p and x (see leaseIncreaseTwins). Another deployment's unit choice prefers the Instance with an
// Allocation to a same-tier, same-second one without, so x never displaces p: by hand, listed pe 1 + p 1 + p's handover 1 +
// the extra 1 = 4, so refused at limit 3 and admitted at limit 4. Left to the UID, x is chosen, no handover is counted and it
// is 3 <= 3: admitted at limit 3.
func TestAnotherDeploymentsListedPrimaryIsPreferredToItsInFlightTwin(t *testing.T) {
	end := hoT(9, 30, 0)
	f := &unitFx{}
	f.md("md-e", &end)
	f.inst("md-e", "pe", provA, "uid-pe", hoT(8, 0, 10), v1alpha1.InstancePending, nil)
	f.alloc("md-e", "pe", provA, false)
	leaseIncreaseTwins(f, end)
	for _, c := range []struct {
		limit int32
		ok    bool
	}{{3, false}, {4, true}} {
		if a := ask(t, newSlotTable(), f.reader(t), extraSlotAsk(provA, "alloc-ex", i32(c.limit), nil, rigNS+"/md-e", "")); a.ok != c.ok {
			t.Fatalf("limit %d: ok %v with a count of %d, want %v: md-a's listed p is the unit, not its in-flight twin", c.limit, a.ok, a.count, c.ok)
		}
	}
}

// TestAnAllocationOfTheProviderWhoseDeploymentViewDoesNotShowItCountsItsHandoverOnce (D5, M3): md-e's extra on provider-a (md-e
// ends at 09:30; pe listed) beside md-o (no end time), whose primary po is Running on an Allocation that names provider-a but
// carries no model-deployment label. The Provider's own list shows the Allocation, so po holds a reserved handover slot, and
// md-o's view by label does not, so po also reads as an Instance in flight: the SAME Instance and role twice. By hand, once:
// listed pe 1 + po 1 + po's reserved handover slot 1 + the extra 1 = 4, so refused at limit 3 and admitted at limit 4. Counted
// twice the unit's slot is left again, 5 > 4: refused at limit 4.
func TestAnAllocationOfTheProviderWhoseDeploymentViewDoesNotShowItCountsItsHandoverOnce(t *testing.T) {
	end := hoT(9, 30, 0)
	f := &unitFx{}
	f.md("md-e", &end)
	f.md("md-o", nil)
	f.inst("md-e", "pe", provA, "uid-pe", hoT(8, 0, 10), v1alpha1.InstancePending, nil)
	f.alloc("md-e", "pe", provA, false)
	f.inst("md-o", "po", provA, "uid-po", hoT(8, 0, 10), v1alpha1.InstanceReady, nil)
	f.alloc("md-o", "po", provA, true)
	f.objs[len(f.objs)-1].(*v1alpha1.Allocation).Labels = map[string]string{v1alpha1.LabelProvider: provA}
	for _, c := range []struct {
		limit int32
		ok    bool
	}{{3, false}, {4, true}} {
		if a := ask(t, newSlotTable(), f.reader(t), extraSlotAsk(provA, "alloc-ex", i32(c.limit), nil, rigNS+"/md-e", "")); a.ok != c.ok {
			t.Fatalf("limit %d: ok %v with a count of %d, want %v: po's handover is left room for once", c.limit, a.ok, a.count, c.ok)
		}
	}
}
