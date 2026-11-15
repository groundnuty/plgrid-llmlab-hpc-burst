package lifecycle

// The science agent's rulings on #599's delta review (#598, comment 6901508274): a replacement waiting for an
// Allocation its Provider can never take ends, for the lasting causes only, so it doesn't hold its slot for ever;
// and of two waiting replacements on a Provider with no slot for both, the older takes it. The expectations are
// worked out by hand on the shared handover timeline (reserved_slot_test.go): md-a's replacement is planned at
// 09:19:30.

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

const stuckSecond = "example-second"

// cutShortReplacement is a host-dialed rig whose md-a may use the rig's Provider and stuckSecond, in that order.
// Its primary serves on the rig's Provider, and its replacement, made there at 09:19:30, is cut short: the
// replacement's Allocation create fails once. It returns the rig, the primary's name and the replacement.
func cutShortReplacement(t *testing.T, opts ...rigOption) (*rig, string, v1alpha1.Instance) {
	t.Helper()
	var armed, cut atomic.Bool
	gate := interceptor.Funcs{Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, o ...client.CreateOption) error {
		if a, ok := obj.(*v1alpha1.Allocation); ok && armed.Load() && a.Labels[v1alpha1.LabelModelDeployment] == "md-a" && cut.CompareAndSwap(false, true) {
			return apierrors.NewServiceUnavailable("the replacement's Allocation create fails once")
		}
		return cl.Create(ctx, obj, o...)
	}}
	rg := newRig(t, append([]rigOption{withHostDialed(), withMaxTime(4 * time.Hour), withInterceptors(gate)}, opts...)...)
	rg.addProvider(stuckSecond, "gpu-long", 4*time.Hour, nil, nil)
	reservedStart(rg, "md-a", func(md *v1alpha1.ModelDeployment) { md.Spec.Providers.Names = []string{rigProvider, stuckSecond} })
	p := rg.instances("md-a")[0]
	if p.Labels[v1alpha1.LabelProvider] != rigProvider {
		t.Fatalf("md-a's primary is on %s, want %s", p.Labels[v1alpha1.LabelProvider], rigProvider)
	}
	hoStepTo(rg, hoT(9, 19, 20), 5*time.Minute, nil)
	armed.Store(true)
	rg.clock.Advance(10 * time.Second) // 09:19:30
	if err := rg.reconcileOne("md-a"); err == nil || !cut.Load() {
		t.Fatalf("md-a's replacement was not cut short (err %v, cut %v): the test proves nothing", err, cut.Load())
	}
	for _, in := range rg.liveInstances("md-a") {
		if in.Annotations[v1alpha1.AnnotationPredecessor] == p.Name {
			if _, made := rg.allocation(in.Spec.AllocationRef.Name); made || in.Labels[v1alpha1.LabelProvider] != rigProvider {
				t.Fatalf("the replacement %s is on %s with its Allocation made %v, want it cut short on %s", in.Name, in.Labels[v1alpha1.LabelProvider], made, rigProvider)
			}
			return rg, p.Name, in
		}
	}
	t.Fatal("md-a's replacement Instance was not made")
	return nil, "", v1alpha1.Instance{}
}

// setPartitionMaxTime sets maxTime of the rig's partition in the PlatformProfile, as an operator's edit would.
func (rg *rig) setPartitionMaxTime(d time.Duration) {
	rg.t.Helper()
	var prof v1alpha1.PlatformProfile
	if err := rg.c.Get(rg.ctx, client.ObjectKey{Name: rigProfile}, &prof); err != nil {
		rg.t.Fatal(err)
	}
	for i := range prof.Spec.Partitions {
		if prof.Spec.Partitions[i].Name == rigPartition {
			prof.Spec.Partitions[i].MaxTime = dur(d)
		}
	}
	if err := rg.c.Update(rg.ctx, &prof); err != nil {
		rg.t.Fatal(err)
	}
}

// setGPUCap sets maxGpusPerUser of the named QOS in the rig's PlatformProfile.
func (rg *rig) setGPUCap(qos string, n int32) {
	rg.t.Helper()
	var prof v1alpha1.PlatformProfile
	if err := rg.c.Get(rg.ctx, client.ObjectKey{Name: rigProfile}, &prof); err != nil {
		rg.t.Fatal(err)
	}
	for i := range prof.Spec.QOS {
		if prof.Spec.QOS[i].Name == qos {
			prof.Spec.QOS[i].MaxGPUsPerUser = &n
		}
	}
	if err := rg.c.Update(rg.ctx, &prof); err != nil {
		rg.t.Fatal(err)
	}
}

// TestAStuckReplacementEndsForALastingCause: md-a's replacement, cut short on the rig's Provider at 09:19:30, can
// never get its Allocation there once that Provider is dropped from md-a's list, its partition is made too short for
// the lease (a maxTime of 1 h, under the lead of the 2h10m drain window, the 30 min handover margin and the start
// timeout), or its QOS's GPU cap is lowered to 0. md-a's next pass ends it, with AllocationLost for the first and
// SpecUnsupported for the other two, and in the same pass plans the primary's replacement again: the primary's
// Provider is tried first and can't take it, so the new replacement goes to stuckSecond.
func TestAStuckReplacementEndsForALastingCause(t *testing.T) {
	for _, c := range []struct {
		name   string
		opts   []rigOption
		cause  func(rg *rig)
		reason string
		words  string
	}{
		{"its Provider dropped from the deployment's list", nil,
			func(rg *rig) {
				rg.mutateMD("md-a", func(md *v1alpha1.ModelDeployment) { md.Spec.Providers.Names = []string{stuckSecond} })
			},
			v1alpha1.ReasonAllocationLost, "Provider " + rigProvider + " is no longer in the deployment's list"},
		{"its partition too short for the lease", nil,
			func(rg *rig) { rg.setPartitionMaxTime(time.Hour) },
			v1alpha1.ReasonSpecUnsupported, "partition " + rigPartition + " of Provider " + rigProvider},
		{"its sizing refused", qosOpts([]v1alpha1.QOSProfile{{Name: "example-q", MaxGPUsPerUser: i32(64)}}, "", "example-q"),
			func(rg *rig) { rg.setGPUCap("example-q", 0) },
			v1alpha1.ReasonSpecUnsupported, "maxGpusPerUser 0 of QOS example-q on Provider " + rigProvider},
	} {
		t.Run(c.name, func(t *testing.T) {
			rg, p, r := cutShortReplacement(t, c.opts...)
			c.cause(rg)
			rg.clock.Advance(10 * time.Second)
			rg.Step()
			in := rg.instance(r.Name)
			if in == nil {
				t.Fatalf("the replacement %s is gone; want it ended, its reason on record", r.Name)
			}
			if in.Status.Phase != v1alpha1.InstanceEnded || in.Status.EndReason != c.reason {
				t.Fatalf("the replacement is %s, ended for %q; want ended for %s", in.Status.Phase, in.Status.EndReason, c.reason)
			}
			if h := apimeta.FindStatusCondition(in.Status.Conditions, v1alpha1.ConditionHealthy); h == nil || !strings.Contains(h.Message, c.words) {
				t.Fatalf("the replacement's Healthy condition %+v does not say %q", h, c.words)
			}
			var again *v1alpha1.Instance
			for _, o := range rg.liveInstances("md-a") {
				if o.Name != r.Name && o.Annotations[v1alpha1.AnnotationPredecessor] == p {
					again = &o
				}
			}
			if again == nil || again.Labels[v1alpha1.LabelProvider] != stuckSecond {
				t.Fatalf("the primary's replacement planned again: %+v; want one on %s", again, stuckSecond)
			}
			if pi := rg.instance(p); pi == nil || !countable(pi) {
				t.Fatalf("the primary %s no longer serves: %+v", p, pi)
			}
		})
	}
}

// TestAStuckReplacementWaitsThroughAProviderProblem: a Provider that is not Ready may be again, so a replacement cut
// short there keeps waiting through it, its slot held: md-a's passes while the Provider reads not Ready end nothing
// and make no Allocation, and once the Provider's controller reads it Ready again the replacement's Allocation is made.
func TestAStuckReplacementWaitsThroughAProviderProblem(t *testing.T) {
	rg, _, r := cutShortReplacement(t)
	var prov v1alpha1.Provider
	if err := rg.c.Get(rg.ctx, client.ObjectKey{Name: rigProvider}, &prov); err != nil {
		t.Fatal(err)
	}
	now := metav1.NewTime(rg.clock.Now())
	apimeta.SetStatusCondition(&prov.Status.Conditions, metav1.Condition{
		Type: v1alpha1.ConditionProviderReady, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonLoginHostInUse,
		Message: "login host login.example.org is in use by another Provider", LastTransitionTime: now,
	})
	if err := rg.c.Status().Update(rg.ctx, &prov); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := rg.reconcileOne("md-a"); err != nil {
			t.Fatal(err)
		}
		in := rg.instance(r.Name)
		if in == nil || ended(in) || in.Status.EndReason != "" {
			t.Fatalf("the replacement %+v ended while its Provider was not Ready, a problem that can pass", in)
		}
		if _, made := rg.allocation(r.Spec.AllocationRef.Name); made {
			t.Fatal("the replacement's Allocation was made while its Provider was not Ready")
		}
	}
	rg.clock.Advance(10 * time.Second)
	rg.Step() // the Provider's controller reads it Ready again
	if _, made := rg.allocation(r.Spec.AllocationRef.Name); !made {
		t.Fatal("the replacement's Allocation was not made once its Provider was Ready again")
	}
}

// TestLastingCauses: which Provider states end a waiting replacement (lastingCause), written out by hand. A managed
// Provider is pinned here, since a rig's primary can't be moved onto a managed platform under it.
func TestLastingCauses(t *testing.T) {
	for _, c := range []struct {
		name   string
		pv     providerView
		reason string
	}{
		{"dropped from the deployment's list", providerView{name: "x", extra: true, hasClass: true}, v1alpha1.ReasonAllocationLost},
		{"managed", providerView{name: "x", managed: true}, v1alpha1.ReasonAllocationLost},
		{"its partition too short", providerView{name: "x", hasClass: true, tooShort: "partition p of Provider x is too short"}, v1alpha1.ReasonSpecUnsupported},
		{"a problem, which can pass", providerView{name: "x", hasClass: true, problem: "Provider x is not Ready"}, ""},
		{"usable", providerView{name: "x", hasClass: true}, ""},
	} {
		if got, _ := lastingCause(&c.pv); got != c.reason {
			t.Errorf("%s: lastingCause = %q, want %q", c.name, got, c.reason)
		}
	}
}

// TestTheOlderWaitingReplacementTakesTheSlot: two replacements wait for their Allocations on a Provider whose
// effective submit limit is 1, which only a leader hand-over race can leave (slots.go's header): each would count
// the other's own slot, and neither would go. The older, by creation time and then UID, takes the slot, and the
// younger waits. The race is forced as two leaders with expectations of their own (two slot tables) reading the same
// store; each reserves for one of the two. Written out by hand: the older counts nothing (0 < 1), the younger counts
// the older's own slot (1).
func TestTheOlderWaitingReplacementTakesTheSlot(t *testing.T) {
	md := &v1alpha1.ModelDeployment{ObjectMeta: metav1.ObjectMeta{Namespace: rigNS, Name: hoMD}}
	waiting := func(name string, created time.Time, uid string) v1alpha1.Instance {
		in := v1alpha1.Instance{
			ObjectMeta: metav1.ObjectMeta{Namespace: rigNS, Name: name, UID: types.UID(uid), CreationTimestamp: metav1.NewTime(created),
				Annotations: map[string]string{v1alpha1.AnnotationPredecessor: "primary-of-" + name},
				Labels:      map[string]string{v1alpha1.LabelProvider: rigProvider, v1alpha1.LabelModelDeployment: hoMD}},
			Spec: v1alpha1.InstanceSpec{AllocationRef: v1alpha1.ObjectReference{Name: "alloc-" + name}},
		}
		in.Status.Phase = v1alpha1.InstancePending
		return in
	}
	early, late := hoT(9, 19, 30), hoT(9, 19, 31)
	for _, c := range []struct {
		name           string
		older, younger v1alpha1.Instance
	}{
		// The UIDs run against the creation times, so the creation time must decide.
		{"by creation time", waiting("r-old", early, "uid-b"), waiting("r-new", late, "uid-a")},
		{"a tie on creation time, broken by UID", waiting("r-a", early, "uid-a"), waiting("r-b", early, "uid-b")},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := fixedInstances{insts: []v1alpha1.Instance{c.older, c.younger}, md: md}
			one := i32(1)
			for _, order := range [][2]v1alpha1.Instance{{c.older, c.younger}, {c.younger, c.older}} {
				leaderA, leaderB := newSlotTable(), newSlotTable()
				for i, in := range order {
					tab := leaderA
					if i == 1 {
						tab = leaderB
					}
					a, err := tab.reserveAsk(context.Background(), r, slotAsk{provider: rigProvider, key: rigNS + "/" + in.Spec.AllocationRef.Name, limit: one}, fixedAt(early))
					if err != nil {
						t.Fatal(err)
					}
					isOlder := in.Name == c.older.Name
					if a.ok != isOlder {
						t.Fatalf("%s (older %v) reserves: ok %v with a count of %d, want ok only for the older", in.Name, isOlder, a.ok, a.count)
					}
					if want := map[bool]int{true: 0, false: 1}[isOlder]; a.count != want {
						t.Fatalf("%s counts %d, want %d", in.Name, a.count, want)
					}
				}
			}
		})
	}
}
