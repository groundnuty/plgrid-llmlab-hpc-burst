package lifecycle

// The two holes #599's review found in the reserved count, and the parity of its gap carriers with step 7's. The
// expectations are worked out by hand, on the shared handover timeline (reserved_slot_test.go).

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// reconcileOne runs one deployment's pass, as a worker would.
func (rg *rig) reconcileOne(name string) error {
	_, err := rg.r.Reconcile(rg.ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: rigNS, Name: name}})
	return err
}

// TestReservedSlotKeepsACutShortReplacementsSlot: in host-dialed mode, md-a's ordinary replacement is cut short
// between its two creates at its planned time 09:19:30: its Instance is made, and its Allocation's create fails
// (the expectation is given back) or the controller restarts before it (the expectation is lost). md-b's pass runs
// next, before md-a's recovery pass: it counts md-a's primary's Allocation, 1, and the replacement's own slot, its
// Allocation not made yet, 1: 2, the limit, so it waits, naming md-a. The primary's replacement slot is not counted
// as well, since the replacement exists. md-a's next pass makes the replacement's Allocation in that slot: 1 + 0 < 2.
func TestReservedSlotKeepsACutShortReplacementsSlot(t *testing.T) {
	for _, c := range []struct {
		name    string
		restart bool
	}{
		{"its Allocation's create fails", false},
		{"the controller restarts before its Allocation", true},
	} {
		t.Run(c.name, func(t *testing.T) {
			var armed, cut atomic.Bool
			gate := interceptor.Funcs{Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if a, ok := obj.(*v1alpha1.Allocation); ok && armed.Load() && a.Labels[v1alpha1.LabelModelDeployment] == "md-a" && cut.CompareAndSwap(false, true) {
					if c.restart {
						a.UID = types.UID("00000000-0000-4000-8000-0000000000aa") // the create never reached the store
						return nil
					}
					return apierrors.NewServiceUnavailable("the replacement's Allocation create fails once")
				}
				return cl.Create(ctx, obj, opts...)
			}}
			rg := newRig(t, withHostDialed(), withMaxTime(4*time.Hour), withInterceptors(gate))
			reservedStart(rg, "md-a", nil)
			p := rg.instances("md-a")[0]
			hoStepTo(rg, hoT(9, 19, 20), 5*time.Minute, nil)
			rg.create(mdSpec("md-b"))
			armed.Store(true)
			rg.clock.Advance(10 * time.Second) // 09:19:30, the replacement's planned time
			err := rg.reconcileOne("md-a")
			if !cut.Load() {
				t.Fatal("md-a's replacement Allocation was never cut short: the test proves nothing")
			}
			if c.restart {
				if err != nil {
					t.Fatalf("md-a's pass: %v", err)
				}
				rg.restartController()
			} else if err == nil {
				t.Fatal("md-a's pass returned no error from the failed create")
			}
			var repl *v1alpha1.Instance
			for _, in := range rg.liveInstances("md-a") {
				if in.Annotations[v1alpha1.AnnotationPredecessor] == p.Name {
					repl = &in
				}
			}
			if repl == nil {
				t.Fatal("md-a's replacement Instance was not made")
			}
			if _, made := rg.allocation(repl.Spec.AllocationRef.Name); made {
				t.Fatalf("the replacement's Allocation %s exists: the pass was not cut short", repl.Spec.AllocationRef.Name)
			}
			// md-b's pass, before md-a's recovery: the replacement's own slot keeps it out.
			if err := rg.reconcileOne("md-b"); err != nil {
				t.Fatalf("md-b's pass: %v", err)
			}
			waitsNaming(t, rg, "md-b", "md-a")
			if n := len(rg.instances("md-b")); n != 0 {
				t.Fatalf("md-b has %d Instances, want none", n)
			}
			// md-a's next pass makes the replacement's Allocation in its slot; md-b still waits.
			rg.clock.Advance(10 * time.Second)
			rg.Step()
			if _, made := rg.allocation(repl.Spec.AllocationRef.Name); !made {
				t.Fatalf("the replacement's Allocation %s was not made on md-a's next pass", repl.Spec.AllocationRef.Name)
			}
			waitsNaming(t, rg, "md-b", "")
			if n := rg.slotHolders(); n != 2 {
				t.Fatalf("%d Allocations hold a slot, want md-a's two", n)
			}
		})
	}
}

// passProbe is a client.Reader that runs a hook at every list of one deployment's own Allocations (the first read of
// its pass, reconcile.go), before the list, so a test sees the store as that pass begins.
type passProbe struct {
	client.Reader
	md   string
	hook func()
}

func (pp *passProbe) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if _, ok := list.(*v1alpha1.AllocationList); ok {
		lo := &client.ListOptions{}
		for _, o := range opts {
			o.ApplyToList(lo)
		}
		if lo.LabelSelector != nil {
			if md, found := lo.LabelSelector.RequiresExactMatch(v1alpha1.LabelModelDeployment); found && md == pp.md {
				pp.hook()
			}
		}
	}
	return pp.Reader.List(ctx, list, opts...)
}

// TestReservedSlotKeepsAFailedSuccessorsSlotForItsRetry: in no-overlap mode, under an effective submit limit of 1,
// the primary's successor S is made once the primary's slot is free, and its job fails 10 s into its run before it
// is ready. S is then the gap's carrier, and step 7 makes S's retry once S's Allocation has left the slot. ma-b sorts
// first, so its pass meets the slot S's Allocation freed before the retry's pass does: it counts 0 Allocations, 0
// expectations and S's carrier slot, 1, the limit, and waits. The test checks that such a pass of ma-b's was seen (S
// ended without being ready, its Allocation gone, no retry yet), or it proves nothing: before that, S's Failed
// Allocation still holds the slot as listed.
func TestReservedSlotKeepsAFailedSuccessorsSlotForItsRetry(t *testing.T) {
	rg := noOverlapRig(t, oneSlotOpts(withBatchConfig(func(c *pfake.Config) { c.MaxSubmitPerUser, c.MaxGPUsPerUser = 1, 1 }))...)
	failAfter := 10 * time.Second
	// The primary's job, then the successor's, which fails 10 s into its run; the retry's is the default.
	rg.batch.Enqueue(pfake.Script{}, pfake.Script{FailAfter: &failAfter})
	p := noOverlapStart(rg)
	rg.create(mdSpec("ma-b"))
	successorOf := func(pred string) *v1alpha1.Instance {
		for _, in := range rg.instances(hoMD) {
			if in.Annotations[v1alpha1.AnnotationPredecessor] == pred {
				return &in
			}
		}
		return nil
	}
	var mu sync.Mutex
	sawWindow, sawFailed := false, false
	rg.r.Reader = &passProbe{Reader: rg.r.Reader, md: "ma-b", hook: func() {
		s := successorOf(p.Name)
		if s == nil || s.Status.Phase != v1alpha1.InstanceEnded || s.Status.ReadyTime != nil {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		sawFailed = true
		if _, there := rg.allocation(s.Spec.AllocationRef.Name); !there && successorOf(s.Name) == nil {
			sawWindow = true
		}
	}}
	bWaits := func() {
		if n := len(rg.allocations("ma-b")); n != 0 {
			t.Fatalf("at %s ma-b has %d Allocations, want none: the one slot is %s's handover's", rg.clock.Now().Format(time.TimeOnly), n, hoMD)
		}
	}
	retryReady := func() bool {
		s := successorOf(p.Name)
		if s == nil {
			return false
		}
		r := successorOf(s.Name)
		return r != nil && r.Status.Phase == v1alpha1.InstanceReady
	}
	hoStepUntil(rg, hoT(11, 30, 0), 10*time.Second, retryReady, bWaits)
	mu.Lock()
	defer mu.Unlock()
	if !sawFailed {
		t.Fatal("ma-b never ran a pass with the successor ended without being ready: the test proves nothing")
	}
	if !sawWindow {
		t.Fatal("ma-b never ran a pass with the failed successor's Allocation gone and no retry yet: the test proves nothing")
	}
	bWaits()
}

// fixedInstances is a client.Reader that lists a fixed set of Instances and reads one ModelDeployment.
type fixedInstances struct {
	client.Reader
	insts []v1alpha1.Instance
	md    *v1alpha1.ModelDeployment
}

func (f fixedInstances) Get(_ context.Context, key client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	if m, ok := obj.(*v1alpha1.ModelDeployment); ok && key.Name == f.md.Name && key.Namespace == f.md.Namespace {
		f.md.DeepCopyInto(m)
		return nil
	}
	return apierrors.NewNotFound(v1alpha1.GroupVersion.WithResource("modeldeployments").GroupResource(), key.Name)
}

func (f fixedInstances) List(_ context.Context, list client.ObjectList, _ ...client.ListOption) error {
	il, ok := list.(*v1alpha1.InstanceList)
	if !ok {
		return fmt.Errorf("fixedInstances lists only Instances, not %T", list)
	}
	il.Items = nil
	for _, in := range f.insts {
		il.Items = append(il.Items, *in.DeepCopy())
	}
	return nil
}

// TestReservedSlotCarriersAreStepSevensCarriers: the Instances whose reserved slot is held for a no-overlap successor
// are exactly those step 7 makes successors of (noOverlapCarriers), both read by outOfServiceCarriers; and a live
// Instance whose Allocation is not made yet holds its own slot. Both sets are written out by hand for one Provider's
// Instances in every shape that matters:
//
//	p1  drained in the mode, ended                       carrier
//	p2  drained in the mode, still draining              carrier
//	p3  drained in the mode, ended, s3 stamped for it    passed on
//	s3  p3's stamped successor, pending, Allocation made in service: nothing
//	p4  drained in the mode, ended, s4 stamped for it    passed on
//	s4  p4's stamped successor, failed before ready      carrier (its retry is next)
//	p5  drained in the mode, ended, s5 stamped for it    passed on
//	s5  p5's stamped successor, ready, then ended        no gap: ready ends it
//	p6  marked in the mode, drain not begun, ended       no gap
//	u6  an unstamped successor, ended                    no gap
//	p8  drained in the mode, ended, s8 stamped for it    passed on
//	s8  p8's stamped successor, pending, no Allocation   its own slot
func TestReservedSlotCarriersAreStepSevensCarriers(t *testing.T) {
	md := &v1alpha1.ModelDeployment{ObjectMeta: metav1.ObjectMeta{Namespace: rigNS, Name: hoMD}}
	began := hoT(9, 50, 20).Format(time.RFC3339)
	ready := metav1.NewTime(hoT(10, 5, 0))
	mk := func(name, phase string, ann map[string]string, readyTime *metav1.Time) v1alpha1.Instance {
		in := v1alpha1.Instance{
			ObjectMeta: metav1.ObjectMeta{Namespace: rigNS, Name: name, UID: types.UID("uid-" + name), Annotations: ann,
				Labels:          map[string]string{v1alpha1.LabelProvider: rigProvider, v1alpha1.LabelModelDeployment: hoMD},
				OwnerReferences: []metav1.OwnerReference{{APIVersion: v1alpha1.GroupVersion.String(), Kind: "ModelDeployment", Name: hoMD, Controller: ptrTo(true)}}},
			Spec: v1alpha1.InstanceSpec{AllocationRef: v1alpha1.ObjectReference{Name: "alloc-" + name}},
		}
		in.Status.Phase = v1alpha1.InstancePhase(phase)
		in.Status.ReadyTime = readyTime
		return in
	}
	drained := func() map[string]string {
		return map[string]string{annotationNoOverlap: "the one slot", annotationDrainStarted: began}
	}
	stamped := func(pred string) map[string]string {
		return map[string]string{v1alpha1.AnnotationPredecessor: pred, annotationNoOverlapSuccessor: "the one slot", annotationNoOverlapGap: pred}
	}
	ended, draining, pending := string(v1alpha1.InstanceEnded), string(v1alpha1.InstanceDraining), string(v1alpha1.InstancePending)
	insts := []v1alpha1.Instance{
		mk("p1", ended, drained(), nil),
		mk("p2", draining, drained(), nil),
		mk("p3", ended, drained(), nil),
		mk("s3", pending, stamped("p3"), nil),
		mk("p4", ended, drained(), nil),
		mk("s4", ended, stamped("p4"), nil),
		mk("p5", ended, drained(), nil),
		mk("s5", ended, stamped("p5"), &ready),
		mk("p6", ended, map[string]string{annotationNoOverlap: "the one slot"}, nil),
		mk("u6", ended, map[string]string{v1alpha1.AnnotationPredecessor: "p6", annotationNoOverlapSuccessor: "the one slot"}, nil),
		mk("p8", ended, drained(), nil),
		mk("s8", pending, stamped("p8"), nil),
	}
	// Every Instance's Allocation exists, ended, but s8's.
	allocs := map[string]*v1alpha1.Allocation{}
	for _, in := range insts {
		if in.Name != "s8" {
			allocs[rigNS+"/alloc-"+in.Name] = &v1alpha1.Allocation{}
		}
	}
	wantCarriers := []string{rigNS + "/alloc-p1", rigNS + "/alloc-p2", rigNS + "/alloc-s4"}
	wantOwn := []string{rigNS + "/alloc-s8"}

	slots, err := reservedOn(context.Background(), fixedInstances{insts: insts, md: md}, rigProvider, nil, allocs)
	if err != nil {
		t.Fatal(err)
	}
	var gotCarriers, gotOwn []string
	for _, s := range slots {
		switch s.kind {
		case slotForSuccessor:
			gotCarriers = append(gotCarriers, s.key)
		case slotForOwnAllocation:
			gotOwn = append(gotOwn, s.key)
		default:
			t.Errorf("a %v slot for %s, with no profile: no primary's slot is reserved", s.kind, s.key)
		}
		if s.deployment != rigNS+"/"+hoMD {
			t.Errorf("the slot for %s names %s, want %s/%s", s.key, s.deployment, rigNS, hoMD)
		}
	}
	sort.Strings(gotCarriers)
	if !slices.Equal(gotCarriers, wantCarriers) {
		t.Errorf("the reserved slots held for successors are %v, want %v", gotCarriers, wantCarriers)
	}
	if !slices.Equal(gotOwn, wantOwn) {
		t.Errorf("the reserved slots held for their own Allocations are %v, want %v", gotOwn, wantOwn)
	}
	ptrs := make([]*v1alpha1.Instance, len(insts))
	for i := range insts {
		ptrs[i] = &insts[i]
	}
	var stepSeven []string
	for _, g := range (&pass{insts: ptrs}).noOverlapCarriers() {
		stepSeven = append(stepSeven, rigNS+"/"+g.in.Spec.AllocationRef.Name)
	}
	sort.Strings(stepSeven)
	if !slices.Equal(stepSeven, wantCarriers) {
		t.Errorf("step 7's carriers are %v, want %v, the reserved slots' own", stepSeven, wantCarriers)
	}
}
