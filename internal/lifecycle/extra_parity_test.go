package lifecycle

// reservedOn applies the deployment's selection (D8; #616, PR 4), the Progressing count of a live extra (D10), and a
// qualification failure beside an extra at k = 1. Every expected set is written out by hand in its row, never computed by
// selectPrimaries, reservedOn or the pass.

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	afake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// parityReader serves a fixed deployment, its Instances and its Allocations, by namespace and label, as the API would.
type parityReader struct {
	client.Reader
	insts  []v1alpha1.Instance
	allocs []v1alpha1.Allocation
	md     *v1alpha1.ModelDeployment
}

func (r parityReader) Get(_ context.Context, key client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	if m, ok := obj.(*v1alpha1.ModelDeployment); ok && key.Name == r.md.Name && key.Namespace == r.md.Namespace {
		r.md.DeepCopyInto(m)
		return nil
	}
	return apierrors.NewNotFound(v1alpha1.GroupVersion.WithResource("modeldeployments").GroupResource(), key.Name)
}

func (r parityReader) List(_ context.Context, list client.ObjectList, opts ...client.ListOption) error {
	lo := &client.ListOptions{}
	for _, o := range opts {
		o.ApplyToList(lo)
	}
	keep := func(ns string, l map[string]string) bool {
		return (lo.Namespace == "" || ns == lo.Namespace) && (lo.LabelSelector == nil || lo.LabelSelector.Matches(labels.Set(l)))
	}
	switch l := list.(type) {
	case *v1alpha1.InstanceList:
		for _, in := range r.insts {
			if keep(in.Namespace, in.Labels) {
				l.Items = append(l.Items, *in.DeepCopy())
			}
		}
	case *v1alpha1.AllocationList:
		for _, a := range r.allocs {
			if keep(a.Namespace, a.Labels) {
				l.Items = append(l.Items, *a.DeepCopy())
			}
		}
	default:
		return apierrors.NewBadRequest("parityReader lists Instances and Allocations only")
	}
	return nil
}

// parityAlloc is the Allocation of the named Instance: Running with its job started at 08:00:20 and ending at
// 12:00:20, or, with running false, Pending and not started.
func parityAlloc(inst string, running bool) v1alpha1.Allocation {
	a := v1alpha1.Allocation{ObjectMeta: metav1.ObjectMeta{Namespace: rigNS, Name: "alloc-" + inst, UID: "auid-" + types.UID(inst),
		Labels: map[string]string{v1alpha1.LabelProvider: provA, v1alpha1.LabelModelDeployment: hoMD}}}
	a.Spec.ProviderConfigReference = providerRef(provA)
	if running {
		s, e := metav1.NewTime(hoT(8, 0, 20)), metav1.NewTime(hoT(12, 0, 20))
		a.Status.AtProvider.Phase, a.Status.AtProvider.StartTime, a.Status.AtProvider.HardEnd = v1alpha1.AllocationRunning, &s, &e
	} else {
		a.Status.AtProvider.Phase = v1alpha1.AllocationPending
	}
	return a
}

func passedAll(in *v1alpha1.Instance) {
	for _, c := range []string{v1alpha1.ConditionVerified, v1alpha1.ConditionQualified, v1alpha1.ConditionWarmedUp} {
		in.Status.Conditions = append(in.Status.Conditions, metav1.Condition{Type: c, Status: metav1.ConditionTrue})
	}
}

// TestReservedOnAppliesTheSelection: one hand-written table through the pass's selection and reservedOn, for a deployment
// of one Instance on Provider A with no end time and a drain window of 2 h 10 min. A chosen Running primary with no live
// replacement holds one slot for its replacement; a surplus Instance holds none, whatever its state, and a primary that
// has a live replacement holds none. The chosen set and the reserved slots are listed by hand, "replacement:Instance".
func TestReservedOnAppliesTheSelection(t *testing.T) {
	md := &v1alpha1.ModelDeployment{ObjectMeta: metav1.ObjectMeta{Namespace: rigNS, Name: hoMD}}
	md.Spec.Instances = 1
	profile := &v1alpha1.PlatformProfileSpec{DrainWindow: dur(2*time.Hour + 10*time.Minute), DrainMargin: dur(10 * time.Minute)}
	started := metav1.NewTime(hoT(8, 5, 20))
	for _, c := range []struct {
		name    string
		insts   []v1alpha1.Instance
		allocs  []v1alpha1.Allocation
		chosen  []string
		reserve []string
	}{
		{"chosen and ready",
			[]v1alpha1.Instance{crossInstance("p", provA, v1alpha1.InstanceReady, nil)},
			[]v1alpha1.Allocation{parityAlloc("p", true)},
			[]string{"p"}, []string{"replacement:p"}},
		{"chosen, Running and verifying",
			[]v1alpha1.Instance{crossInstance("p", provA, v1alpha1.InstanceVerifying, nil)},
			[]v1alpha1.Allocation{parityAlloc("p", true)},
			[]string{"p"}, []string{"replacement:p"}},
		{"a surplus that has started holds none",
			[]v1alpha1.Instance{crossInstance("p", provA, v1alpha1.InstanceReady, nil), crossInstance("x", provA, v1alpha1.InstanceVerifying, nil)},
			[]v1alpha1.Allocation{parityAlloc("p", true), parityAlloc("x", true)},
			[]string{"p"}, []string{"replacement:p"}},
		{"a surplus waiting at its ranking point holds none",
			func() []v1alpha1.Instance {
				p, x := crossInstance("p", provA, v1alpha1.InstanceReady, nil), crossInstance("x", provA, v1alpha1.InstanceVerifying, nil)
				passedAll(&x)
				x.Status.StartInstructionTime = &started
				return []v1alpha1.Instance{p, x}
			}(),
			[]v1alpha1.Allocation{parityAlloc("p", true), parityAlloc("x", true)},
			[]string{"p"}, []string{"replacement:p"}},
		{"a primary with a live replacement holds none, and the replacement is no primary",
			[]v1alpha1.Instance{crossInstance("p", provA, v1alpha1.InstanceReady, nil), crossInstance("r", provA, v1alpha1.InstancePending, map[string]string{v1alpha1.AnnotationPredecessor: "p"})},
			[]v1alpha1.Allocation{parityAlloc("p", true), parityAlloc("r", false)},
			[]string{"p"}, nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := parityReader{insts: c.insts, allocs: c.allocs, md: md}
			// the pass's selection
			ptrs := make([]*v1alpha1.Instance, len(c.insts))
			for i := range c.insts {
				ptrs[i] = &c.insts[i]
			}
			byName := map[string]*v1alpha1.Allocation{}
			for i := range c.allocs {
				byName[c.allocs[i].Name] = &c.allocs[i]
			}
			chosen, _ := selectPrimaries(selection{insts: ptrs, allocOf: func(in *v1alpha1.Instance) *v1alpha1.Allocation { return byName[in.Spec.AllocationRef.Name] }}, 1)
			var gotChosen []string
			for _, in := range chosen {
				gotChosen = append(gotChosen, in.Name)
			}
			if !slices.Equal(gotChosen, c.chosen) {
				t.Errorf("chosen %v, want %v", gotChosen, c.chosen)
			}
			// reservedOn
			allocs := map[string]*v1alpha1.Allocation{}
			for i := range c.allocs {
				allocs[rigNS+"/"+c.allocs[i].Name] = &c.allocs[i]
			}
			slots, err := reservedOn(context.Background(), r, provA, profile, allocs)
			if err != nil {
				t.Fatal(err)
			}
			var got []string
			for _, s := range slots {
				kind := map[slotKind]string{slotForReplacement: "replacement", slotForSuccessor: "successor", slotForOwnAllocation: "own"}[s.kind]
				got = append(got, kind+":"+strings.TrimPrefix(s.key, rigNS+"/alloc-"))
			}
			if !slices.Equal(got, c.reserve) {
				t.Errorf("reserved slots %v, want %v", got, c.reserve)
			}
		})
	}
}

// TestALiveExtraCountsInProgressingUntilItEnds (D10; HBEP-7, Events and conditions: Progressing is True/Provisioning
// "while the controller creates, starts or replaces Allocations and Instances", and a pending or waiting extra is being
// started). P starts at 00:10 and is ready at 00:40, measuring 20 ms; X starts at 00:15 and reaches its ranking point at
// 00:45 at 18.0 ms. At 00:41 both are live: "2 Instances live, 1 ready, 1 desired", Progressing True/Provisioning. X ends
// RankedOut at 00:45 (18.0 does not outrank 20), and at 00:46 the deployment is Stable, with one Instance live.
func TestALiveExtraCountsInProgressingUntilItEnds(t *testing.T) {
	h := newRkRig(t, false)
	p, x := h.start(nil, pfake.Script{QueueWait: 10 * time.Minute}, pfake.Script{QueueWait: rkXWait})
	h.engineOf(p, rk200, 0)
	h.engineOf(x, rk180, 0)
	h.stepTo(rkAt(0, 41, 0), 10*time.Second, nil)
	rkCalibration(t, h, p, x)
	c := h.condition(hoMD, v1alpha1.ConditionProgressing)
	if c.Status != metav1.ConditionTrue || c.Reason != v1alpha1.ReasonProvisioning || c.Message != "2 Instances live, 1 ready, 1 desired" {
		t.Fatalf("at 00:41 Progressing = %s/%s %q, want True/Provisioning \"2 Instances live, 1 ready, 1 desired\"", c.Status, c.Reason, c.Message)
	}
	if md := h.md(hoMD); md.Status.Instances != 2 || md.Status.ReadyInstances != 1 {
		t.Fatalf("status counts %d live and %d ready at 00:41, want 2 and 1", md.Status.Instances, md.Status.ReadyInstances)
	}
	h.stepTo(rkAt(0, 46, 0), 10*time.Second, nil)
	if in := h.mustInstance(x); in.Status.Phase != v1alpha1.InstanceEnded || in.Status.EndReason != v1alpha1.ReasonRankedOut {
		t.Fatalf("X is %s/%s at 00:46, want ended/RankedOut", in.Status.Phase, in.Status.EndReason)
	}
	c = h.condition(hoMD, v1alpha1.ConditionProgressing)
	if c.Status != metav1.ConditionFalse || c.Reason != v1alpha1.ReasonStable || h.md(hoMD).Status.Instances != 1 {
		t.Fatalf("at 00:46 Progressing = %s/%s with %d live, want False/Stable and 1", c.Status, c.Reason, h.md(hoMD).Status.Instances)
	}
}

// TestAQualificationFailedInstanceBesideAnExtraIsReleased (k = 1): the gate's second miss fails an Instance and releases its
// Allocation, whichever of the primary and the extra it is. The first Allocation by name measures 41 ms, twice (limit
// 40 ms), the second 20 ms. The failed Instance ends QualificationFailed and its Allocation is deleted with the cause
// written first. The other serves: if it was the extra it is chosen in the failed primary's place, with no Allocation made.
// Two jobs in all, one ready, one released.
func TestAQualificationFailedInstanceBesideAnExtraIsReleased(t *testing.T) {
	log := newCauseLog()
	rg := newRig(t, withInterceptors(log.funcs()))
	bad := ""
	rg.engine = func(alloc string) afake.Engine {
		if bad == "" {
			names := []string{}
			for _, a := range rg.allocations("md") {
				names = append(names, a.Name)
			}
			slices.Sort(names)
			bad = names[0]
		}
		tpot := 20 * time.Millisecond
		if alloc == bad {
			tpot = 41 * time.Millisecond
		}
		return afake.Engine{WarmUpRound: gateRound(tpot, 300*time.Millisecond)}
	}
	md := gateMD()
	md.Spec.OverAllocation = ptrTo(int32(1))
	rg.create(md)
	startUntil(rg, "md", 2*time.Minute, "the primary and the extra", func() bool { return len(rg.instances("md")) == 2 })
	id := rg.instances("md")[0].Spec.DeploymentID.Display()
	var prof v1alpha1.PlatformProfile
	if err := rg.c.Get(rg.ctx, client.ObjectKey{Name: rigProfile}, &prof); err != nil {
		t.Fatal(err)
	}
	prof.Spec.ExpectedPerformance = oneEntry()(id)
	if err := rg.c.Update(rg.ctx, &prof); err != nil {
		t.Fatal(err)
	}
	startUntil(rg, "md", 20*time.Minute, "one Instance failed and one ready", func() bool {
		failed := 0
		for _, in := range rg.instances("md") {
			if in.Status.EndReason == v1alpha1.ReasonQualificationFailed {
				failed++
			}
		}
		return failed == 1 && rg.readyCount("md") == 1
	})
	var failedAlloc, failedName string
	for _, in := range rg.instances("md") {
		if in.Status.EndReason == v1alpha1.ReasonQualificationFailed {
			failedAlloc, failedName = in.Spec.AllocationRef.Name, in.Name
		}
	}
	if failedAlloc != bad {
		t.Fatalf("the failed Instance is on %s, want the 41 ms one, %s", failedAlloc, bad)
	}
	startUntil(rg, "md", 5*time.Minute, "the failed Instance's Allocation deleted", func() bool { _, ok := log.deletedWith(failedAlloc); return ok })
	if cause, _ := log.deletedWith(failedAlloc); cause != "Instance "+failedName+" failed: QualificationFailed" {
		t.Fatalf("the Allocation was deleted with the cause %q, want it written first: Instance %s failed: QualificationFailed", cause, failedName)
	}
	rg.until(2*time.Minute, 10*time.Second, "the failed Instance's Allocation gone", func() bool { _, ok := rg.allocation(failedAlloc); return !ok })
	if s := rg.submissions(); s != 2 || len(rg.allAllocations()) != 1 {
		t.Fatalf("%d jobs and %d Allocations in all, want 2 and 1: the survivor serves, no Allocation was made for the failed one", s, len(rg.allAllocations()))
	}
}
