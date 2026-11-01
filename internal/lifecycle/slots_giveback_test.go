package lifecycle

// A creation that fails gives its slot back (HBEP-7, Provisioning, Slots are
// reserved atomically), on each of the two creates a pass makes, and a pass cut
// short between them is finished or reported (review of PR 171, Opus B3, n1;
// Astra B4). The cache the controller reads shows no creation (staleReader), so
// only the table's expectations decide the count, and a slot not given back
// stays taken for the 5 minutes an expectation lives.

import (
	"context"
	"strings"
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
)

// failOnce makes the first create of an object for which match is true fail, as
// an API server that is restarting does.
func failOnce(match func(client.Object) bool) interceptor.Funcs {
	var failed atomic.Bool
	return interceptor.Funcs{Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
		if match(obj) && failed.CompareAndSwap(false, true) {
			return apierrors.NewServiceUnavailable("the API server is restarting")
		}
		return c.Create(ctx, obj, opts...)
	}}
}

// reconcileOf reconciles one deployment once and returns the error.
func (rg *rig) reconcileOf(md string) error {
	rg.t.Helper()
	_, err := rg.r.Reconcile(rg.ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: rigNS, Name: md}})
	return err
}

// TestSlotsFailedInstanceCreateGivesItsSlotBack: the Instance create fails
// after the slot was reserved under the name its Allocation would have. The
// next pass draws a NEW name, so the old reservation, never listed, would hold
// the only slot until it lapses at 5 minutes, and md would show
// SubmitLimitReached against itself. With the slot given back, the next pass,
// at the same moment against the same stale cache, creates the Allocation.
func TestSlotsFailedInstanceCreateGivesItsSlotBack(t *testing.T) {
	rg := newRig(t, oneSlotOpts(withInterceptors(failOnce(func(o client.Object) bool {
		_, ok := o.(*v1alpha1.Instance)
		return ok
	})))...)
	rg.Step() // the Provider's first status: Ready
	rg.create(mdSpec("md"))
	rg.r.Reader = newStaleReader(t, rg.c)
	if err := rg.reconcileOf("md"); err == nil {
		t.Fatal("the failed Instance create was not reported")
	}
	if n, m := len(rg.instances("md")), len(rg.allAllocations()); n != 0 || m != 0 {
		t.Fatalf("%d Instances and %d Allocations after the failed create, want none of either", n, m)
	}
	if err := rg.reconcileOf("md"); err != nil {
		t.Fatal(err)
	}
	if n := len(rg.allocations("md")); n != 1 {
		t.Fatalf("md has %d Allocations on the next pass, want 1: the failed create's slot was not given back", n)
	}
	if c := rg.condition("md", v1alpha1.ConditionDegraded); c.Reason == v1alpha1.ReasonSubmitLimitReached {
		t.Fatalf("Degraded = %s %q: md is refused a slot by its own earlier reservation", c.Reason, c.Message)
	}
}

// TestSlotsFailedAllocationCreateGivesItsSlotBack: md-a's pass creates its
// Instance, and its Allocation create fails: the slot is given back, so md-b,
// reconciled next against the same stale cache, takes the Provider's only slot.
// md-a's pending Instance then waits for a slot (its Allocation is created by
// ensureAllocation under the Instance's own name, which a same-deployment retry
// cannot tell from a slot still held), and the deployment shows
// SubmitLimitReached naming the Instance, the Provider and the limit. A
// reservation not given back would refuse md-b instead.
func TestSlotsFailedAllocationCreateGivesItsSlotBack(t *testing.T) {
	rg := newRig(t, oneSlotOpts(withInterceptors(failOnce(func(o client.Object) bool {
		a, ok := o.(*v1alpha1.Allocation)
		return ok && a.Spec.ModelDeploymentRef != nil && a.Spec.ModelDeploymentRef.Name == "md-a"
	})))...)
	rg.Step()
	rg.create(mdSpec("md-a"))
	rg.r.Reader = newStaleReader(t, rg.c)
	if err := rg.reconcileOf("md-a"); err == nil {
		t.Fatal("md-a's failed Allocation create was not reported")
	}
	if ins, as := rg.instances("md-a"), rg.allocations("md-a"); len(ins) != 1 || len(as) != 0 {
		t.Fatalf("md-a has %d Instances and %d Allocations, want the half-done 1 and 0", len(ins), len(as))
	}
	rg.create(mdSpec("md-b"))
	if err := rg.reconcileOf("md-b"); err != nil {
		t.Fatal(err)
	}
	if n := len(rg.allocations("md-b")); n != 1 {
		t.Fatalf("md-b has %d Allocations, want the slot md-a's failed create gave back", n)
	}
	if err := rg.reconcileOf("md-a"); err != nil {
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
	if c.Reason != v1alpha1.ReasonSubmitLimitReached || !strings.Contains(c.Message, "Instance "+ins[0].Name) || !strings.Contains(c.Message, "maxSubmitPerUser 1 of QOS example-now") {
		t.Fatalf("md-a Degraded = %s/%s %q, want SubmitLimitReached naming the Instance and the limit", c.Status, c.Reason, c.Message)
	}
}

// halfDone returns a rig with md's Instance created and its Allocation create
// cut short, under profile edits applied at the start.
func halfDone(t *testing.T, opts ...rigOption) *rig {
	t.Helper()
	rg := newRig(t, append(opts, withInterceptors(failOnce(func(o client.Object) bool {
		_, ok := o.(*v1alpha1.Allocation)
		return ok
	})))...)
	rg.Step()
	rg.create(mdSpec("md"))
	if err := rg.reconcileOf("md"); err == nil {
		t.Fatal("the cut-short Allocation create was not reported")
	}
	if ins, as := rg.instances("md"), rg.allAllocations(); len(ins) != 1 || len(as) != 0 {
		t.Fatalf("%d Instances and %d Allocations after the cut-short pass, want the half-done 1 and 0", len(ins), len(as))
	}
	return rg
}

// editProfile applies f to the rig's PlatformProfile in the store.
func (rg *rig) editProfile(f func(*v1alpha1.PlatformProfileSpec)) {
	rg.t.Helper()
	var prof v1alpha1.PlatformProfile
	if err := rg.c.Get(rg.ctx, client.ObjectKey{Name: rigProfile}, &prof); err != nil {
		rg.t.Fatal(err)
	}
	f(&prof.Spec)
	if err := rg.c.Update(rg.ctx, &prof); err != nil {
		rg.t.Fatal(err)
	}
}

// TestHalfDoneInstanceReportsASizingRefusal: md's Instance is created and its
// Allocation create is cut short. Before the next pass the Provider's QOS gets
// maxGpusPerUser 0, so no Allocation can be sized there. The pending Instance
// already counts as a primary, so provisioning never reports it: the recovery
// does, with SpecUnsupported naming the limit, the QOS and the Provider, not
// AsDesired. No Allocation is created (review of PR 171, Astra B4).
func TestHalfDoneInstanceReportsASizingRefusal(t *testing.T) {
	rg := halfDone(t, qosOpts([]v1alpha1.QOSProfile{{Name: "example-now", MaxGPUsPerUser: i32(4)}}, "", "example-now")...)
	rg.editProfile(func(s *v1alpha1.PlatformProfileSpec) { s.QOS[0].MaxGPUsPerUser = i32(0) })
	if err := rg.reconcileOf("md"); err != nil {
		t.Fatal(err)
	}
	if n := len(rg.allAllocations()); n != 0 {
		t.Fatalf("%d Allocations, want none: the Provider cannot host the Instance", n)
	}
	ins := rg.instances("md")
	if len(ins) != 1 || ins[0].Status.Phase != v1alpha1.InstancePending {
		t.Fatalf("Instances %+v, want the one pending", ins)
	}
	c := rg.condition("md", v1alpha1.ConditionDegraded)
	if c.Status != metav1.ConditionTrue || c.Reason != v1alpha1.ReasonSpecUnsupported ||
		!strings.Contains(c.Message, "maxGpusPerUser 0 of QOS example-now on Provider "+rigProvider) || !strings.Contains(c.Message, "Instance "+ins[0].Name) {
		t.Fatalf("Degraded = %s/%s %q, want SpecUnsupported naming the Instance and maxGpusPerUser 0 of QOS example-now on Provider %s", c.Status, c.Reason, c.Message, rigProvider)
	}
}

// TestHalfDoneInstanceOnAProviderThatBecameTooShort: the same cut-short pass,
// then the partition's maxTime drops to 1 h, under a lead of 3h40m (the same
// profile values as the PartitionTooShort tests). The recovery creates no
// Allocation there, and the deployment shows PartitionTooShort.
func TestHalfDoneInstanceOnAProviderThatBecameTooShort(t *testing.T) {
	rg := halfDone(t, withProfile(shortProfile))
	rg.editProfile(func(s *v1alpha1.PlatformProfileSpec) { s.Partitions[0].MaxTime = dur(time.Hour) })
	if err := rg.reconcileOf("md"); err != nil {
		t.Fatal(err)
	}
	if n := len(rg.allAllocations()); n != 0 {
		t.Fatalf("%d Allocations, want none on a partition too short", n)
	}
	c := rg.condition("md", v1alpha1.ConditionDegraded)
	if c.Status != metav1.ConditionTrue || c.Reason != v1alpha1.ReasonPartitionTooShort || !strings.Contains(c.Message, "time limit 1h0m0s") {
		t.Fatalf("Degraded = %s/%s %q, want PartitionTooShort naming the 1 h limit", c.Status, c.Reason, c.Message)
	}
}
