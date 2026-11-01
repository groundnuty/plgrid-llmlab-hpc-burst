package lifecycle

// HBEP-7's Test plan item "Per-user limits, reserved atomically" (Provisioning,
// Per-user limits and Slots are reserved atomically). The Provider's QOS sets
// maxSubmitPerUser 1 and the partition has no QOS, so the effective submit
// limit is 1 (worked out by hand: no partition QOS, so the job QOS's value).
//
// The reader the controller counts with is a stale cache that does not show
// Allocations created after its snapshot, so only the reservation can stop a
// second creation: a count of the listed Allocations alone would find none.
// Reconciles run one after another prove nothing here, so the deployments are
// reconciled at the same moment by one goroutine each.

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// staleReader is a client.Reader that shows only the Allocations its snapshot
// holds: a cache that has not yet seen the later creations. Everything else
// reads through.
type staleReader struct {
	client.Reader
	mu    sync.Mutex
	known map[string]bool
}

func newStaleReader(t *testing.T, r client.Reader) *staleReader {
	s := &staleReader{Reader: r}
	s.catchUp(t)
	return s
}

// catchUp takes a new snapshot: the cache has seen every Allocation now
// stored.
func (s *staleReader) catchUp(t *testing.T) {
	t.Helper()
	var al v1alpha1.AllocationList
	if err := s.Reader.List(context.Background(), &al); err != nil {
		t.Fatal(err)
	}
	known := map[string]bool{}
	for _, a := range al.Items {
		known[a.Namespace+"/"+a.Name] = true
	}
	s.mu.Lock()
	s.known = known
	s.mu.Unlock()
}

func (s *staleReader) shows(ns, name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.known[ns+"/"+name]
}

func (s *staleReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*v1alpha1.Allocation); ok && !s.shows(key.Namespace, key.Name) {
		return apierrors.NewNotFound(v1alpha1.GroupVersion.WithResource("allocations").GroupResource(), key.Name)
	}
	return s.Reader.Get(ctx, key, obj, opts...)
}

func (s *staleReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if err := s.Reader.List(ctx, list, opts...); err != nil {
		return err
	}
	al, ok := list.(*v1alpha1.AllocationList)
	if !ok {
		return nil
	}
	kept := al.Items[:0]
	for _, a := range al.Items {
		if s.shows(a.Namespace, a.Name) {
			kept = append(kept, a)
		}
	}
	al.Items = kept
	return nil
}

// oneSlotOpts: the Provider's QOS sets maxSubmitPerUser 1, the partition has
// no QOS, so the effective limit is 1.
func oneSlotOpts(extra ...rigOption) []rigOption {
	qos := []v1alpha1.QOSProfile{{Name: "example-now", MaxSubmitPerUser: i32(1), MaxGPUsPerUser: i32(1)}}
	return append(qosOpts(qos, "", "example-now"), extra...)
}

// allAllocations returns every Allocation in the store, whatever the
// controller's reader shows.
func (rg *rig) allAllocations() []v1alpha1.Allocation {
	rg.t.Helper()
	var al v1alpha1.AllocationList
	if err := rg.c.List(rg.ctx, &al); err != nil {
		rg.t.Fatal(err)
	}
	return al.Items
}

// reconcileAtOnce reconciles the named deployments at the same moment, one
// goroutine each, released together, and returns their errors.
func (rg *rig) reconcileAtOnce(r *Reconciler, names ...string) []error {
	start := make(chan struct{})
	errs := make([]error, len(names))
	var wg sync.WaitGroup
	for i, n := range names {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, errs[i] = r.Reconcile(rg.ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: rigNS, Name: n}})
		}()
	}
	close(start)
	wg.Wait()
	return errs
}

// TestSlotsReservedAtomicallyTwoWorkers: two deployments on a Provider whose
// effective submit limit is 1 race for the last slot, reconciled at the same
// moment by two goroutines, against a cache that never shows the first
// creation. The first Allocation's create is held until the other worker has
// finished its pass, or has itself come to create one, so the second worker
// counts while the first reservation is made and its Allocation not yet
// created. At most one Allocation is created; the other deployment has
// SubmitLimitReached naming the Provider and its limit.
func TestSlotsReservedAtomicallyTwoWorkers(t *testing.T) {
	var mu sync.Mutex
	creates := 0
	release := make(chan struct{})
	var once sync.Once
	open := func() { once.Do(func() { close(release) }) }
	gate := interceptor.Funcs{Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
		if _, ok := obj.(*v1alpha1.Allocation); ok {
			mu.Lock()
			creates++
			n := creates
			mu.Unlock()
			if n == 1 {
				select {
				case <-release:
				case <-time.After(30 * time.Second): // a safety net, not a window: the test is released by state
				}
			} else {
				open()
			}
		}
		return c.Create(ctx, obj, opts...)
	}}
	rg := newRig(t, oneSlotOpts(withInterceptors(gate))...)
	rg.Step() // the Provider's first status: Ready
	rg.create(mdSpec("md-a"))
	rg.create(mdSpec("md-b"))
	stale := newStaleReader(t, rg.c)
	rg.r.Reader = stale

	start := make(chan struct{})
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i, n := range []string{"md-a", "md-b"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, errs[i] = rg.r.Reconcile(rg.ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: rigNS, Name: n}})
			open() // this worker's pass is over: a held create may go on
		}()
	}
	close(start)
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("reconcile %d: %v", i, err)
		}
	}
	all := rg.allAllocations()
	if len(all) != 1 {
		t.Fatalf("%d Allocations on a Provider with a submit limit of 1, want exactly one", len(all))
	}
	winner := all[0].Spec.ModelDeploymentRef.Name
	loser := map[string]string{"md-a": "md-b", "md-b": "md-a"}[winner]
	if n := len(rg.instances(loser)); n != 0 {
		t.Fatalf("%s has %d Instances, want none: it got no slot", loser, n)
	}
	c := rg.condition(loser, v1alpha1.ConditionDegraded)
	if c.Status != metav1.ConditionTrue || c.Reason != v1alpha1.ReasonSubmitLimitReached ||
		!strings.Contains(c.Message, "Provider "+rigProvider) || !strings.Contains(c.Message, "maxSubmitPerUser 1 of QOS example-now") {
		t.Fatalf("%s Degraded = %s/%s %q, want SubmitLimitReached naming Provider %s and maxSubmitPerUser 1", loser, c.Status, c.Reason, c.Message, rigProvider)
	}
	// The winner's Allocation is counted as not ended, by its Provider's
	// controller and once the cache shows it: the loser still gets none.
	stale.catchUp(t)
	rg.clock.Advance(10 * time.Second)
	rg.Step()
	if n := len(rg.allAllocations()); n != 1 {
		t.Fatalf("after the cache caught up, %d Allocations, want still one", n)
	}
	if c := rg.condition(loser, v1alpha1.ConditionDegraded); c.Reason != v1alpha1.ReasonSubmitLimitReached {
		t.Fatalf("after the cache caught up, %s Degraded = %s/%s, want SubmitLimitReached", loser, c.Status, c.Reason)
	}
}

// TestSlotsReservedAtomicallyManyWorkers: six deployments reconciled at the
// same moment by six goroutines, against the same stale cache, on the limit of
// 1: exactly one Allocation, and five deployments with SubmitLimitReached.
func TestSlotsReservedAtomicallyManyWorkers(t *testing.T) {
	rg := newRig(t, oneSlotOpts()...)
	rg.Step()
	var names []string
	for i := range 6 {
		n := fmt.Sprintf("md-%d", i)
		names = append(names, n)
		rg.create(mdSpec(n))
	}
	rg.r.Reader = newStaleReader(t, rg.c)
	for i, err := range rg.reconcileAtOnce(rg.r, names...) {
		if err != nil {
			t.Fatalf("reconcile %s: %v", names[i], err)
		}
	}
	if n := len(rg.allAllocations()); n != 1 {
		t.Fatalf("%d Allocations, want exactly one under a submit limit of 1", n)
	}
	limited := 0
	for _, n := range names {
		if rg.condition(n, v1alpha1.ConditionDegraded).Reason == v1alpha1.ReasonSubmitLimitReached {
			limited++
		}
	}
	if limited != 5 {
		t.Fatalf("%d deployments with SubmitLimitReached, want 5", limited)
	}
}

// TestSlotsTwoDesiredInstancesUnderLimitOne: a deployment with two desired
// instances under the limit of 1 gets one Allocation, and SubmitLimitReached
// for the second.
func TestSlotsTwoDesiredInstancesUnderLimitOne(t *testing.T) {
	rg := newRig(t, oneSlotOpts()...)
	md := mdSpec("md")
	md.Spec.Instances = 2
	rg.create(md)
	for range 3 {
		rg.clock.Advance(10 * time.Second)
		rg.Step()
	}
	if n := len(rg.allocations("md")); n != 1 {
		t.Fatalf("%d Allocations, want one under a submit limit of 1", n)
	}
	c := rg.condition("md", v1alpha1.ConditionDegraded)
	if c.Status != metav1.ConditionTrue || c.Reason != v1alpha1.ReasonSubmitLimitReached || !strings.Contains(c.Message, "maxSubmitPerUser 1 of QOS example-now") {
		t.Fatalf("Degraded = %s/%s %q, want SubmitLimitReached naming the limit", c.Status, c.Reason, c.Message)
	}
}

// TestSlotsFailedCreationGivesItsSlotBack: a creation that fails gives its
// slot back, so the next pass, against the same stale cache, creates the
// Allocation; and a half-done pass (an Instance with no Allocation) is
// finished under the slot it already holds, never a second one.
func TestSlotsFailedCreationGivesItsSlotBack(t *testing.T) {
	var mu sync.Mutex
	fail := 1
	rg := newRig(t, oneSlotOpts(withInterceptors(interceptor.Funcs{Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
		if _, ok := obj.(*v1alpha1.Allocation); ok {
			mu.Lock()
			f := fail > 0
			fail--
			mu.Unlock()
			if f {
				return apierrors.NewServiceUnavailable("the API server is restarting")
			}
		}
		return c.Create(ctx, obj, opts...)
	}}))...)
	rg.Step()
	rg.create(mdSpec("md"))
	rg.r.Reader = newStaleReader(t, rg.c)
	if _, err := rg.r.Reconcile(rg.ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: rigNS, Name: "md"}}); err == nil {
		t.Fatal("the failed Allocation create was not reported")
	}
	ins := rg.instances("md")
	if len(ins) != 1 || len(rg.allAllocations()) != 0 {
		t.Fatalf("%d Instances and %d Allocations after the failed create, want 1 and 0", len(ins), len(rg.allAllocations()))
	}
	// The failed create gave its slot back; the pending Instance's
	// Allocation is created on the next pass under its own name.
	if _, err := rg.r.Reconcile(rg.ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: rigNS, Name: "md"}}); err != nil {
		t.Fatal(err)
	}
	all := rg.allAllocations()
	if len(all) != 1 || all[0].Name != ins[0].Spec.AllocationRef.Name {
		t.Fatalf("Allocations %+v, want the one Instance %s names", all, ins[0].Spec.AllocationRef.Name)
	}
	// Again, still unseen by the cache: the Instance's Allocation already
	// holds the slot, so the pass creates nothing more and degrades nothing.
	if _, err := rg.r.Reconcile(rg.ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: rigNS, Name: "md"}}); err != nil {
		t.Fatal(err)
	}
	if n := len(rg.allAllocations()); n != 1 {
		t.Fatalf("%d Allocations, want still one", n)
	}
	if c := rg.condition("md", v1alpha1.ConditionDegraded); c.Reason == v1alpha1.ReasonSubmitLimitReached {
		t.Fatalf("Degraded = %s %q: the Instance's own Allocation was counted twice", c.Reason, c.Message)
	}
}

// TestSlotTableExpectations covers the expectations' rules on the table
// alone, with a reader that shows what the test stores: a reserved creation
// counts until listed, then once; a release gives the slot back; a listed
// Allocation in phase Ended does not count; an expectation lapses after
// 5 min (ExpectationsTimeout, controller_utils.go at v1.35.0, line 72).
func TestSlotTableExpectations(t *testing.T) {
	rg := newRig(t)
	ctx := context.Background()
	tab := newSlotTable()
	two := i32(2)
	t0 := rigStart
	stale := newStaleReader(t, rg.c)
	reserve := func(key string, at time.Time) (bool, int) {
		t.Helper()
		ok, n, err := tab.reserve(ctx, stale, rigProvider, key, two, at)
		if err != nil {
			t.Fatal(err)
		}
		return ok, n
	}
	if ok, n := reserve(rigNS+"/a", t0); !ok || n != 0 {
		t.Fatalf("first reservation = %v with count %d, want true and 0", ok, n)
	}
	if ok, n := reserve(rigNS+"/a", t0); !ok || n != 1 {
		t.Fatalf("the same name again = %v with count %d, want true and 1: it holds its slot", ok, n)
	}
	if ok, n := reserve(rigNS+"/b", t0); !ok || n != 1 {
		t.Fatalf("second name = %v with count %d, want true and 1", ok, n)
	}
	if ok, n := reserve(rigNS+"/c", t0); ok || n != 2 {
		t.Fatalf("third name = %v with count %d, want false and 2", ok, n)
	}
	tab.release(rigProvider, rigNS+"/b")
	if ok, _ := reserve(rigNS+"/c", t0); !ok {
		t.Fatal("after a release the slot is not free")
	}
	// a and c are reserved. a is created and the cache shows it: counted
	// once, as listed.
	a := otherAllocation("a")
	a.Namespace = rigNS
	rg.create(a)
	stale.catchUp(t)
	if n, err := tab.count(ctx, stale, rigProvider, t0); err != nil || n != 2 {
		t.Fatalf("count = %d, %v; want 2 (a listed, c expected)", n, err)
	}
	// a ends: it no longer counts, though listed.
	a.Status.AtProvider.Phase = v1alpha1.AllocationEnded
	if err := rg.c.Status().Update(ctx, a); err != nil {
		t.Fatal(err)
	}
	if n, err := tab.count(ctx, stale, rigProvider, t0); err != nil || n != 1 {
		t.Fatalf("count = %d, %v; want 1 (c expected; a Ended)", n, err)
	}
	// A deleting Allocation counts until its deletion completes.
	gone := otherAllocation("deleting")
	gone.Namespace = rigNS
	gone.Finalizers = []string{"example.org/hold"}
	rg.create(gone)
	if err := rg.c.Delete(ctx, gone); err != nil {
		t.Fatal(err)
	}
	stale.catchUp(t)
	if n, err := tab.count(ctx, stale, rigProvider, t0); err != nil || n != 2 {
		t.Fatalf("count = %d, %v; want 2 (c expected, the deleting one listed)", n, err)
	}
	if err := rg.c.Get(ctx, client.ObjectKeyFromObject(gone), gone); err != nil {
		t.Fatal(err)
	}
	gone.Finalizers = nil
	if err := rg.c.Update(ctx, gone); err != nil {
		t.Fatal(err)
	}
	stale.catchUp(t)
	// c is never seen: its expectation lapses at 5 min, not before.
	if n, _ := tab.count(ctx, stale, rigProvider, t0.Add(5*time.Minute-time.Second)); n != 1 {
		t.Fatalf("count 1 s before the expiry = %d, want 1", n)
	}
	if n, _ := tab.count(ctx, stale, rigProvider, t0.Add(5*time.Minute)); n != 0 {
		t.Fatalf("count at the expiry = %d, want 0", n)
	}
	// No limit: nothing is listed or reserved.
	if ok, n, err := tab.reserve(ctx, stale, rigProvider, rigNS+"/d", nil, t0); !ok || n != 0 || err != nil {
		t.Fatalf("reserve with no limit = %v, %d, %v; want true, 0, nil", ok, n, err)
	}
	// A limit of 0 leaves no slot.
	if ok, _, _ := tab.reserve(ctx, stale, rigProvider, rigNS+"/e", i32(0), t0); ok {
		t.Fatal("a limit of 0 gave a slot")
	}
}

// TestSlotsHalfDonePassWaitsForASlot: a pass is cut short after md-a's
// Instance and before its Allocation, and the controller restarts, so its
// expectations are gone. md-b then takes the only slot. md-a's next pass
// finishes the half-done provisioning (ensureAllocation) only with a slot: it
// creates no Allocation, its Instance waits, and the deployment has
// SubmitLimitReached naming the Instance, the Provider and the limit.
func TestSlotsHalfDonePassWaitsForASlot(t *testing.T) {
	var mu sync.Mutex
	failA := true
	rg := newRig(t, oneSlotOpts(withInterceptors(interceptor.Funcs{Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
		if a, ok := obj.(*v1alpha1.Allocation); ok && a.Spec.ModelDeploymentRef != nil && a.Spec.ModelDeploymentRef.Name == "md-a" {
			mu.Lock()
			f := failA
			failA = false
			mu.Unlock()
			if f {
				return apierrors.NewServiceUnavailable("the controller stopped here")
			}
		}
		return c.Create(ctx, obj, opts...)
	}}))...)
	rg.Step()
	rg.create(mdSpec("md-a"))
	reconcile := func(md string) error {
		_, err := rg.r.Reconcile(rg.ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: rigNS, Name: md}})
		return err
	}
	if err := reconcile("md-a"); err == nil {
		t.Fatal("md-a's cut-short pass reported no error")
	}
	if ins, as := rg.instances("md-a"), rg.allocations("md-a"); len(ins) != 1 || len(as) != 0 {
		t.Fatalf("md-a has %d Instances and %d Allocations, want the half-done 1 and 0", len(ins), len(as))
	}
	rg.restartController()
	rg.create(mdSpec("md-b"))
	if err := reconcile("md-b"); err != nil {
		t.Fatal(err)
	}
	if n := len(rg.allocations("md-b")); n != 1 {
		t.Fatalf("md-b has %d Allocations, want the slot", n)
	}
	if err := reconcile("md-a"); err != nil {
		t.Fatal(err)
	}
	if n := len(rg.allocations("md-a")); n != 0 {
		t.Fatalf("md-a has %d Allocations, want none past the limit", n)
	}
	ins := rg.instances("md-a")
	if len(ins) != 1 || ins[0].Status.Phase != v1alpha1.InstancePending {
		t.Fatalf("md-a's Instances %+v, want one pending", ins)
	}
	c := rg.condition("md-a", v1alpha1.ConditionDegraded)
	if c.Reason != v1alpha1.ReasonSubmitLimitReached || !strings.Contains(c.Message, "Instance "+ins[0].Name) || !strings.Contains(c.Message, "Provider "+rigProvider) || !strings.Contains(c.Message, "maxSubmitPerUser 1 of QOS example-now") {
		t.Fatalf("md-a Degraded = %s/%s %q, want SubmitLimitReached naming the Instance, the Provider and the limit", c.Status, c.Reason, c.Message)
	}
}
