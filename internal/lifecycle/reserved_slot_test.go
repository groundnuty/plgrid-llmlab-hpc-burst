package lifecycle

// HBEP-7's Test plan row "The reserved replacement slot" (Provisioning, Per-user limits; the owner's decision of 15
// Nov 2026; issue #598). The expectations are worked out by hand on the shared handover timeline (handover_test.go):
// with the rig's 4 h limit, the first deployment's primary job starts at 08:00:20 and is ready at 08:01:10, its hard
// end is 12:00:20, the default drain window of 2h10m puts its drain start at 09:50:20, and the Provider's one observed
// wait (0 s) puts its replacement's planned time at 09:50:20 - (0 + 50 s + 30 min) = 09:19:30.
//
// In host-dialed mode the Provider has at most 2 Allocations (HBEP-6). While md-a's primary serves past its drain
// start with no replacement, it holds one slot and its reserved slot the other: 1 + 1 = 2, the limit, so md-b's first
// primary waits. md-a's replacement takes that reserved slot: 1 + 0 = 1 < 2. Then two Allocations hold both slots.

import (
	"context"
	"errors"
	"strings"
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

// reservedStart creates the deployment, with edit applied, and steps the rig to its primary's ready time on the
// shared timeline, 08:01:10, with lease end 12:00:20.
func reservedStart(rg *rig, name string, edit func(*v1alpha1.ModelDeployment)) {
	rg.t.Helper()
	md := mdSpec(name)
	if edit != nil {
		edit(md)
	}
	rg.create(md)
	for range 7 {
		rg.clock.Advance(10 * time.Second)
		rg.Step()
	}
	ins := rg.instances(name)
	if len(ins) != 1 || ins[0].Status.ReadyTime == nil || !ins[0].Status.ReadyTime.Time.Equal(hoT(8, 1, 10)) ||
		ins[0].Status.LeaseEnd == nil || !ins[0].Status.LeaseEnd.Time.Equal(hoT(12, 0, 20)) {
		rg.t.Fatalf("%s should have one primary, ready at 08:01:10 with leaseEnd 12:00:20; got %d Instances %+v", name, len(ins), ins)
	}
}

// endsAt sets the deployment's end time.
func endsAt(t time.Time) func(*v1alpha1.ModelDeployment) {
	return func(md *v1alpha1.ModelDeployment) { e := metav1.NewTime(t); md.Spec.EndTime = &e }
}

// slotHolders counts the Allocations that hold a slot on the rig's Provider, whatever the controller's reader shows.
func (rg *rig) slotHolders() int {
	n := 0
	for _, a := range rg.allAllocations() {
		if holdsSlot(&a) {
			n++
		}
	}
	return n
}

// setSubmitLimit sets maxSubmitPerUser of the named QOS in the rig's PlatformProfile, as an operator's edit would;
// the controller reads the profile on its next pass.
func (rg *rig) setSubmitLimit(qos string, n int32) {
	rg.t.Helper()
	var prof v1alpha1.PlatformProfile
	if err := rg.c.Get(rg.ctx, client.ObjectKey{Name: rigProfile}, &prof); err != nil {
		rg.t.Fatal(err)
	}
	found := false
	for i := range prof.Spec.QOS {
		if prof.Spec.QOS[i].Name == qos {
			prof.Spec.QOS[i].MaxSubmitPerUser, found = &n, true
		}
	}
	if !found {
		rg.t.Fatalf("PlatformProfile %s has no QOS %s", rigProfile, qos)
	}
	if err := rg.c.Update(rg.ctx, &prof); err != nil {
		rg.t.Fatal(err)
	}
}

// waitsNaming fails unless md has no Allocation and is SubmitLimitReached; with handover set, its message names
// the reserved slot of that deployment's handover, written out by hand.
func waitsNaming(t *testing.T, rg *rig, md, handover string) {
	t.Helper()
	now := rg.clock.Now().Format(time.TimeOnly)
	if n := len(rg.allocations(md)); n != 0 {
		t.Fatalf("at %s %s has %d Allocations, want none: the slot is held for a handover", now, md, n)
	}
	c := rg.condition(md, v1alpha1.ConditionDegraded)
	if c.Status != metav1.ConditionTrue || c.Reason != v1alpha1.ReasonSubmitLimitReached {
		t.Fatalf("at %s %s Degraded = %s/%s %q, want SubmitLimitReached", now, md, c.Status, c.Reason, c.Message)
	}
	if handover != "" && !strings.Contains(c.Message, "1 more slots are reserved for the handovers of "+rigNS+"/"+handover) {
		t.Fatalf("at %s %s Degraded %q does not name %s/%s, whose handover holds the slot", now, md, c.Message, rigNS, handover)
	}
}

// TestReservedSlotHoldsBUntilAEnds: in host-dialed mode, md-b's first Allocation waits while md-a's primary serves
// past its drain start, md-a's replacement is created at its planned time in its predecessor's reserved slot, and
// md-b's primary is created once md-a ends.
func TestReservedSlotHoldsBUntilAEnds(t *testing.T) {
	rg := newRig(t, withHostDialed(), withMaxTime(4*time.Hour))
	reservedStart(rg, "md-a", nil)
	rg.create(mdSpec("md-b"))
	atMostTwo := func() {
		if n := rg.slotHolders(); n > 2 {
			t.Fatalf("at %s %d Allocations hold a slot, above host-dialed mode's 2", rg.clock.Now().Format(time.TimeOnly), n)
		}
	}
	// Until the replacement's planned time: md-a's primary holds one slot, and its reserved slot the other.
	hoStepTo(rg, hoT(9, 19, 20), 5*time.Minute, func() { waitsNaming(t, rg, "md-b", "md-a"); atMostTwo() })
	// 09:19:30: the replacement takes the reserved slot; then two Allocations hold both slots, so md-b still waits.
	hoStepTo(rg, hoT(9, 20, 0), 10*time.Second, func() { waitsNaming(t, rg, "md-b", ""); atMostTwo() })
	if n := len(rg.allocations("md-a")); n != 2 {
		t.Fatalf("at 09:20:00 md-a has %d Allocations, want 2: its replacement is created at 09:19:30 in its reserved slot", n)
	}
	// md-a ends: its Instances end and its Allocations go, and then md-b's primary is created.
	rg.mutateMD("md-a", endsAt(rg.clock.Now()))
	hoStepUntil(rg, hoT(10, 0, 0), 10*time.Second, func() bool { return len(rg.allocations("md-b")) == 1 }, atMostTwo)
}

// TestReservedSlotFollowsTheReplacementRule: a slot is reserved whenever the controller would create the primary's
// replacement (the science agent's ruling on #598's plan, comment 6897017776; HBEP-7 as #601 amends it): its
// deployment ends after the drain start, 09:50:20 here, or not at all. A deployment ending before the drain start, or
// at it, gets no replacement, so no slot is reserved, and md-b's primary is created at once beside md-a's; one ending
// a second after it, or later but before the hard end, gets a replacement, so its slot is reserved. The reservation
// and replacements() share one test (servesPast, handover.go).
func TestReservedSlotFollowsTheReplacementRule(t *testing.T) {
	for _, c := range []struct {
		name  string
		end   time.Time
		waits bool
	}{
		{"md-a ends at 09:30:00, before its drain start 09:50:20 (and its hard end 12:00:20)", hoT(9, 30, 0), false},
		{"md-a ends at 09:50:19, a second before its drain start", hoT(9, 50, 19), false},
		{"md-a ends at 09:50:20, its drain start: no replacement", hoT(9, 50, 20), false},
		{"md-a ends at 09:50:21, past its drain start: a replacement, so a reserved slot", hoT(9, 50, 21), true},
		{"md-a ends at 11:00:00, before its hard end but past its drain start", hoT(11, 0, 0), true},
	} {
		t.Run(c.name, func(t *testing.T) {
			rg := newRig(t, withHostDialed(), withMaxTime(4*time.Hour))
			reservedStart(rg, "md-a", endsAt(c.end))
			rg.create(mdSpec("md-b"))
			rg.clock.Advance(10 * time.Second)
			rg.Step()
			if c.waits {
				waitsNaming(t, rg, "md-b", "md-a")
				return
			}
			if n := len(rg.allocations("md-b")); n != 1 {
				t.Fatalf("md-b has %d Allocations a pass after its creation, want 1: md-a needs no replacement, so nothing is reserved", n)
			}
			if c := rg.condition("md-b", v1alpha1.ConditionDegraded); c.Reason == v1alpha1.ReasonSubmitLimitReached {
				t.Fatalf("md-b Degraded = %s %q, want no SubmitLimitReached", c.Reason, c.Message)
			}
		})
	}
}

// TestReservedSlotGoesToTheNoOverlapSuccessorBeforeB: in no-overlap mode, under an effective submit limit of 1, the
// slot the primary frees goes to its successor before md-b's primary. The one-slot path's timeline
// (nooverlap_test.go): the primary drains at 09:50:20 with its stream in flight until 09:56:00, and its Allocation is
// released 5 min after its end, so the successor's Allocation comes no earlier than 10:01:00. The second deployment is
// named ma-b to sort before the primary's, md, so its pass runs first in every Step (rig.reconcileAll): in the Step
// whose Provider sync frees the slot, ma-b's pass meets the free slot before the successor's does, the order in which
// only the successor's reserved slot keeps it. ma-b waits throughout, and still waits once the successor's Allocation
// holds the one slot.
func TestReservedSlotGoesToTheNoOverlapSuccessorBeforeB(t *testing.T) {
	rg := noOverlapRig(t, oneSlotOpts(withBatchConfig(func(c *pfake.Config) { c.MaxSubmitPerUser, c.MaxGPUsPerUser = 1, 1 }))...)
	p := noOverlapStart(rg)
	rg.create(mdSpec("ma-b"))
	bWaits := func() {
		if n := len(rg.allocations("ma-b")); n != 0 {
			t.Fatalf("at %s ma-b has %d Allocations, want none: the one slot is %s's handover's", rg.clock.Now().Format(time.TimeOnly), n, hoMD)
		}
		if n := rg.slotHolders(); n > 1 {
			t.Fatalf("at %s %d Allocations hold a slot, above the limit of 1", rg.clock.Now().Format(time.TimeOnly), n)
		}
	}
	hoStepTo(rg, hoT(9, 50, 20), 5*time.Minute, bWaits)
	// The gap: the successor waits for the primary's slot, which is reserved for it, and ma-b's message names it.
	sawNamed := false
	gap := func() {
		bWaits()
		c := rg.condition("ma-b", v1alpha1.ConditionDegraded)
		if c.Reason == v1alpha1.ReasonSubmitLimitReached && strings.Contains(c.Message, "reserved for the handovers of "+rigNS+"/"+hoMD) {
			sawNamed = true
		}
	}
	hoStepUntil(rg, hoT(10, 30, 0), 10*time.Second, func() bool { return rg.readyCount(hoMD) > 0 }, gap)
	if !sawNamed {
		t.Fatal("ma-b's message never named the handover whose reserved slot held it")
	}
	var succ *v1alpha1.Instance
	for _, in := range rg.liveInstances(hoMD) {
		if in.Annotations[v1alpha1.AnnotationPredecessor] == p.Name && in.Annotations[annotationNoOverlapSuccessor] != "" {
			succ = &in
		}
	}
	if succ == nil || succ.Status.Phase != v1alpha1.InstanceReady {
		t.Fatalf("the primary's no-overlap successor %+v, want it ready in the freed slot", succ)
	}
	// Once the successor's Allocation holds the slot, ma-b still waits.
	hoStepTo(rg, rg.clock.Now().Add(time.Minute), 10*time.Second, bWaits)
}

// passKey is the context key under which a test names the deployment whose pass a context belongs to.
type passKey struct{}

// passGate is a client.Reader that holds a deployment's pass at its first list of its own Allocations (the
// first read of a pass, reconcile.go), once, until its hook returns. It forces one interleaving of two passes run at
// once, so that the order which matters is pinned, not left to the scheduler. It holds only a list made in that
// deployment's OWN pass, which the test marks in the pass's context (passKey): reservedOn lists every deployment's
// Allocations by its label when it counts the reserved slots (D8, #616 PR 4), so another deployment's reservation can
// list md-a's Allocations too, and a hold keyed on the label alone fired in that other pass, which then waited for itself.
type passGate struct {
	client.Reader
	mu   sync.Mutex
	hold map[string]func() error
	done map[string]bool
}

func (g *passGate) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if _, ok := list.(*v1alpha1.AllocationList); ok {
		lo := &client.ListOptions{}
		for _, o := range opts {
			o.ApplyToList(lo)
		}
		if lo.LabelSelector != nil {
			if md, found := lo.LabelSelector.RequiresExactMatch(v1alpha1.LabelModelDeployment); found && ctx.Value(passKey{}) == md {
				g.mu.Lock()
				h, first := g.hold[md], !g.done[md]
				g.done[md] = true
				g.mu.Unlock()
				if h != nil && first {
					if err := h(); err != nil {
						return err
					}
				}
			}
		}
	}
	return g.Reader.List(ctx, list, opts...)
}

// waitOn waits for ch, or fails the test after 30 s: a safety net, never a window the test races.
func waitOn(t *testing.T, ch <-chan struct{}, what string) error {
	select {
	case <-ch:
		return nil
	case <-time.After(30 * time.Second):
		t.Errorf("%s never happened in 30 s", what)
		return apierrors.NewServiceUnavailable(what + " never happened")
	}
}

// TestReservedSlotIsAtomicAgainstAStaleCache: md-b's first primary and md-a's replacement are reconciled at once, at
// the replacement's planned time 09:19:30, against a cache that does not show Allocations created after its snapshot,
// in both interleavings that matter. md-b's reservation first: 1 Allocation + md-a's reserved slot = 2, so md-b is
// refused, and md-a's replacement then takes its reserved slot, 1 + 0 = 1. md-a's first, its Allocation's create
// held until md-b's pass ends: md-b counts md-a's primary and md-a's raised expectation, 1 + 1 = 2, and is refused.
// Either way only md-a's replacement is created. Reconciles run one after another prove nothing here.
func TestReservedSlotIsAtomicAgainstAStaleCache(t *testing.T) {
	for _, first := range []string{"md-b", "md-a"} {
		t.Run(first+" reserves first", func(t *testing.T) {
			aCreating, bDone := make(chan struct{}), make(chan struct{})
			var once sync.Once
			// The gate is armed only for the passes run at once, not for md-a's primary at the rig's start.
			var armed atomic.Bool
			gate := interceptor.Funcs{Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if a, ok := obj.(*v1alpha1.Allocation); ok && armed.Load() && first == "md-a" && a.Labels[v1alpha1.LabelModelDeployment] == "md-a" {
					once.Do(func() { close(aCreating) })
					if err := waitOn(t, bDone, "md-b's pass ending"); err != nil {
						return err
					}
				}
				return c.Create(ctx, obj, opts...)
			}}
			rg := newRig(t, withHostDialed(), withMaxTime(4*time.Hour), withInterceptors(gate))
			reservedStart(rg, "md-a", nil)
			hoStepTo(rg, hoT(9, 19, 20), 5*time.Minute, nil)
			if n := len(rg.allocations("md-a")); n != 1 {
				t.Fatalf("at 09:19:20 md-a has %d Allocations, want only its primary's", n)
			}
			rg.create(mdSpec("md-b"))
			rg.clock.Advance(10 * time.Second) // 09:19:30, the replacement's planned time
			g := &passGate{Reader: newStaleReader(t, rg.c), hold: map[string]func() error{}, done: map[string]bool{}}
			if first == "md-b" {
				g.hold["md-a"] = func() error { return waitOn(t, bDone, "md-b's pass ending") }
			} else {
				g.hold["md-b"] = func() error { return waitOn(t, aCreating, "md-a's replacement Allocation being created") }
			}
			rg.r.Reader = g
			armed.Store(true)
			start := make(chan struct{})
			errs := map[string]error{}
			var mu sync.Mutex
			var wg sync.WaitGroup
			for _, n := range []string{"md-a", "md-b"} {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					_, err := rg.r.Reconcile(context.WithValue(rg.ctx, passKey{}, n), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: rigNS, Name: n}})
					mu.Lock()
					errs[n] = err
					mu.Unlock()
					if n == "md-b" {
						close(bDone)
					}
				}()
			}
			close(start)
			wg.Wait()
			for n, err := range errs {
				if err != nil {
					t.Fatalf("reconcile %s: %v", n, err)
				}
			}
			if n := len(rg.allocations("md-a")); n != 2 {
				t.Fatalf("md-a has %d Allocations, want 2: its primary's and its replacement's", n)
			}
			if n := len(rg.instances("md-b")); n != 0 {
				t.Fatalf("md-b has %d Instances, want none: md-a's handover holds the slot", n)
			}
			// md-b first counted md-a's reserved slot, and names it; md-a first, md-b counted its expectation.
			waitsNaming(t, rg, "md-b", map[string]string{"md-b": "md-a", "md-a": ""}[first])
			// The cache catches up; the next pass still finds no slot for md-b.
			g.Reader.(*staleReader).catchUp(t)
			rg.clock.Advance(10 * time.Second)
			rg.Step()
			if n := len(rg.allAllocations()); n != 2 {
				t.Fatalf("after the cache caught up, %d Allocations, want still 2", n)
			}
			waitsNaming(t, rg, "md-b", "")
		})
	}
}

// instanceListHang is a client.Reader whose Instance Lists never answer until their context ends.
type instanceListHang struct{ client.Reader }

func (h instanceListHang) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if _, ok := list.(*v1alpha1.InstanceList); ok {
		<-ctx.Done()
		return ctx.Err()
	}
	return h.Reader.List(ctx, list, opts...)
}

// TestReservedSlotReadsAreBoundedWithTheList: the reserved slots' reads run under the Provider's lock with the
// Allocations' List, under the one context the table's timeout bounds (slots.go, the lock order), so an Instance
// List that never answers gives up at that timeout, with an error naming the Provider, and the lock goes with it.
// The test waits on the call's return; the table's timeout is shortened for it.
func TestReservedSlotReadsAreBoundedWithTheList(t *testing.T) {
	rg := newRig(t)
	tab := newSlotTable()
	tab.listTimeout = 20 * time.Millisecond
	done := make(chan error, 1)
	go func() {
		_, _, err := tab.reserve(context.Background(), instanceListHang{rg.c}, rigProvider, rigNS+"/a", i32(1), fixedAt(rigStart))
		done <- err
	}()
	var err error
	select {
	case err = <-done:
	case <-time.After(time.Minute): // a safety net for a hang, not a window the test races
		t.Fatal("reserve never returned from an Instance List that never answers")
	}
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), rigProvider) {
		t.Fatalf("reserve error = %v, want the deadline, naming Provider %s", err, rigProvider)
	}
	l := tab.slotLock(rigProvider)
	if !l.TryLock() {
		t.Fatal("the Provider's lock is still held after the Instance List gave up")
	}
	l.Unlock()
}

// TestReservedSlotKeepsACutShortSuccessorsSlotAcrossARestart: on the one-slot path, a pass that creates the
// successor's Instance and stops before its Allocation (here the Allocation's create is dropped, with an invented UID
// so the pass's credential step completes), followed by a controller restart, which loses the expectation, leaves the
// successor holding its slot by its own reservation alone: ma-b, whose pass runs first in every Step, still waits, and
// the successor's Allocation is created on the next pass (ensureAllocation), in that slot.
func TestReservedSlotKeepsACutShortSuccessorsSlotAcrossARestart(t *testing.T) {
	var armed, dropped atomic.Bool
	drop := interceptor.Funcs{Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
		if a, ok := obj.(*v1alpha1.Allocation); ok && armed.Load() && a.Labels[v1alpha1.LabelModelDeployment] == hoMD && dropped.CompareAndSwap(false, true) {
			a.UID = types.UID("00000000-0000-4000-8000-0000000000dd") // the create never reached the store
			return nil
		}
		return c.Create(ctx, obj, opts...)
	}}
	rg := noOverlapRig(t, oneSlotOpts(withInterceptors(drop), withBatchConfig(func(c *pfake.Config) { c.MaxSubmitPerUser, c.MaxGPUsPerUser = 1, 1 }))...)
	p := noOverlapStart(rg)
	rg.create(mdSpec("ma-b"))
	armed.Store(true)
	restarted := false
	each := func() {
		if n := len(rg.allocations("ma-b")); n != 0 {
			t.Fatalf("at %s ma-b has %d Allocations, want none: the one slot is %s's successor's", rg.clock.Now().Format(time.TimeOnly), n, hoMD)
		}
		if dropped.Load() && !restarted {
			restarted = true
			rg.restartController()
		}
	}
	succReady := func() bool {
		for _, in := range rg.liveInstances(hoMD) {
			if in.Annotations[v1alpha1.AnnotationPredecessor] == p.Name && in.Status.Phase == v1alpha1.InstanceReady {
				return true
			}
		}
		return false
	}
	hoStepUntil(rg, hoT(10, 30, 0), 10*time.Second, succReady, each)
	if !dropped.Load() || !restarted {
		t.Fatal("the successor's Allocation was never dropped and the controller never restarted: the test proves nothing")
	}
	var succ *v1alpha1.Instance
	for _, in := range rg.liveInstances(hoMD) {
		if in.Annotations[v1alpha1.AnnotationPredecessor] == p.Name {
			succ = &in
		}
	}
	if succ == nil || succ.Status.Phase != v1alpha1.InstanceReady {
		t.Fatalf("the primary's successor %+v, want it ready in the freed slot", succ)
	}
	if _, ok := rg.allocation(succ.Spec.AllocationRef.Name); !ok {
		t.Fatalf("the successor's Allocation %s does not exist", succ.Spec.AllocationRef.Name)
	}
}
