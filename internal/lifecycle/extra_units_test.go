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
		Spec: v1alpha1.InstanceSpec{AllocationRef: v1alpha1.ObjectReference{Name: "alloc-" + name}}}
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

func (f *unitFx) reader(t *testing.T) client.Reader {
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

// extraSlotAsk is the extra's reservation for a new Allocation of one GPU, on the Provider with a submit limit and a GPU cap
// (nil: none), asked by the deployment, leaving the Instance out of it. sizeOf sizes a new Allocation of any deployment at one
// GPU. THE ONLY CODE HERE THAT NAMES THE UNIT SET'S ASK FIELDS.
func extraSlotAsk(provider, key string, limit, gpuCap *int32, asker string, exclude types.UID) slotAsk {
	size := oneGPU()
	return slotAsk{provider: provider, key: rigNS + "/" + key, limit: limit, params: &size, caps: v1alpha1.QOSLimits{MaxGPUsPerUser: gpuCap}, profile: unitProfile,
		deployment: asker, unitExclude: exclude, unitSize: func(*v1alpha1.ModelDeployment) (v1alpha1.BatchParameters, error) { return oneGPU(), nil }}
}

func ask(t *testing.T, tab *slotTable, r client.Reader, q slotAsk) slotAnswer {
	t.Helper()
	a, err := tab.reserveAsk(context.Background(), r, q, fixedAt(hoT(8, 0, 30)))
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// TestAnExtraLeavesThePrimarysHandoverSlotWhoseAllocationAppearsBetweenTheReads (A1): limit 3 on provider-a. md-e (k = 1, ends
// at 09:30: no handover of its own) has its primary pe, listed. md-o has no end time, so its primary po will hold a slot for its
// replacement once Running. po's reservation is in the table (an expectation: one slot), and its Allocation appears between
// the lock's Provider-wide list, which does not show it, and the reads by label, which do. The extra's check: listed 1 (pe)
// + expected 1 (po) + po's future slot 1 + the extra 1 = 4 > 3, refused; the old reader dropped po (its Allocation absent
// from the Provider's list) and counted 3 <= 3. With a limit of 4 the extra is admitted (3 <= 4), and when md-o ends at 09:30
// (po never hands over) it is admitted at 3.
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
