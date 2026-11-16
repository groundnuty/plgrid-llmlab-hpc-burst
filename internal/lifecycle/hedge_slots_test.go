package lifecycle

// The reserved count across Providers (#604 PR 2; #617) and the Provider-wide per-user caps (capsFit). Every
// expected slot, count and refusal is written out by hand.

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

const (
	provA = "provider-a"
	provB = "provider-b"
)

// crossInstance is an Instance of hoMD on prov, in phase phase, with its annotations.
func crossInstance(name, prov string, phase v1alpha1.InstancePhase, ann map[string]string) v1alpha1.Instance {
	in := v1alpha1.Instance{
		ObjectMeta: metav1.ObjectMeta{Namespace: rigNS, Name: name, UID: types.UID("uid-" + name), Annotations: ann,
			Labels:          map[string]string{v1alpha1.LabelProvider: prov, v1alpha1.LabelModelDeployment: hoMD},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: v1alpha1.GroupVersion.String(), Kind: "ModelDeployment", Name: hoMD, Controller: ptrTo(true)}}},
		Spec: v1alpha1.InstanceSpec{AllocationRef: v1alpha1.ObjectReference{Name: "alloc-" + name}},
	}
	in.Status.Phase = phase
	le := metav1.NewTime(hoT(12, 0, 20))
	in.Status.LeaseEnd = &le
	if phase == v1alpha1.InstanceReady {
		rt := metav1.NewTime(hoT(8, 1, 10))
		in.Status.ReadyTime = &rt
	}
	return in
}

// runningAlloc is a Running Allocation, keyed as the count keys it.
func runningAlloc(name string) (string, *v1alpha1.Allocation) {
	a := &v1alpha1.Allocation{}
	a.Status.AtProvider.Phase = v1alpha1.AllocationRunning
	return rigNS + "/alloc-" + name, a
}

// TestReservedSlotsReadTheDeploymentOnEveryProvider: the reserved slots on a Provider read the deployment's
// Instances on every Provider. With no end time, a drain window of 2h10m and every lease ending at 12:00:20:
//
//	P on A, ready, Running; R, P's replacement, on B, pending:
//	  A: nothing, since P is replaced (before #617 A held a slot for P); B: nothing, R isn't the winner.
//	P on A, ready; W, P's replacement, on B, Running but starting:
//	  B: nothing, a member that hasn't won holds no slot of its own.
//	P on A, ready; W, P's replacement, on B, ready and Running, its hedge's only member:
//	  B: one slot for W's replacement, the winner's from its win.
//	P on A, ready; W, P's replacement, on B, verifying in a restart after it won, Running:
//	  B: one slot for W's replacement, still: the hedge's one member that has served stays its winner.
//	P on A, ready; W on B ready since 08:01:10, and L, W's hedge member on C, ready since 08:01:00:
//	  B: nothing, L is the winner, ready first.
//	C, drained in no-overlap mode, ended, on A; S, its stamped successor, on B, pending:
//	  A: nothing, the gap has passed on to S (before, C's slot stayed on A).
func TestReservedSlotsReadTheDeploymentOnEveryProvider(t *testing.T) {
	md := &v1alpha1.ModelDeployment{ObjectMeta: metav1.ObjectMeta{Namespace: rigNS, Name: hoMD}}
	profile := &v1alpha1.PlatformProfileSpec{DrainWindow: dur(2*time.Hour + 10*time.Minute), DrainMargin: dur(10 * time.Minute)}
	pred := func(p string) map[string]string { return map[string]string{v1alpha1.AnnotationPredecessor: p} }
	earlier := metav1.NewTime(hoT(8, 1, 0))
	for _, c := range []struct {
		name   string
		insts  []v1alpha1.Instance
		allocs map[string][]string // Provider → Running Allocations' Instances
		want   map[string][]string // Provider → reserved slots, "kind:Instance"
	}{
		{"a replacement on another Provider, pending",
			[]v1alpha1.Instance{crossInstance("p", provA, v1alpha1.InstanceReady, nil), crossInstance("r", provB, v1alpha1.InstancePending, pred("p"))},
			map[string][]string{provA: {"p"}, provB: {"r"}},
			map[string][]string{provA: nil, provB: nil}},
		{"a member on another Provider, Running but starting",
			[]v1alpha1.Instance{crossInstance("p", provA, v1alpha1.InstanceReady, nil), crossInstance("w", provB, v1alpha1.InstanceStarting, pred("p"))},
			map[string][]string{provA: {"p"}, provB: {"w"}},
			map[string][]string{provA: nil, provB: nil}},
		{"the winner on another Provider",
			[]v1alpha1.Instance{crossInstance("p", provA, v1alpha1.InstanceReady, nil), crossInstance("w", provB, v1alpha1.InstanceReady, pred("p"))},
			map[string][]string{provA: {"p"}, provB: {"w"}},
			map[string][]string{provA: nil, provB: {"replacement:w"}}},
		{"the winner in a restart",
			func() []v1alpha1.Instance {
				w := crossInstance("w", provB, v1alpha1.InstanceVerifying, pred("p"))
				rt := metav1.NewTime(hoT(8, 1, 10))
				w.Status.ReadyTime = &rt
				return []v1alpha1.Instance{crossInstance("p", provA, v1alpha1.InstanceReady, nil), w}
			}(),
			map[string][]string{provA: {"p"}, provB: {"w"}},
			map[string][]string{provB: {"replacement:w"}}},
		{"a member that is ready but not the winner",
			func() []v1alpha1.Instance {
				l := crossInstance("l", "provider-c", v1alpha1.InstanceReady, map[string]string{v1alpha1.AnnotationPredecessor: "p", v1alpha1.AnnotationHedgeGroup: "w"})
				l.Status.ReadyTime = &earlier
				return []v1alpha1.Instance{crossInstance("p", provA, v1alpha1.InstanceReady, nil), crossInstance("w", provB, v1alpha1.InstanceReady, pred("p")), l}
			}(),
			map[string][]string{provA: {"p"}, provB: {"w"}},
			map[string][]string{provB: nil}},
		{"a gap's carrier whose successor is on another Provider",
			[]v1alpha1.Instance{
				crossInstance("c", provA, v1alpha1.InstanceEnded, map[string]string{annotationNoOverlap: "the one slot", annotationDrainStarted: hoT(9, 50, 20).Format(time.RFC3339)}),
				crossInstance("s", provB, v1alpha1.InstancePending, map[string]string{v1alpha1.AnnotationPredecessor: "c", annotationNoOverlapSuccessor: "the one slot", annotationNoOverlapGap: "c"}),
			},
			map[string][]string{provA: {"c"}, provB: {"s"}},
			map[string][]string{provA: nil}},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := fixedInstances{insts: c.insts, md: md}
			for prov, want := range c.want {
				allocs := map[string]*v1alpha1.Allocation{}
				for _, n := range c.allocs[prov] {
					k, a := runningAlloc(n)
					allocs[k] = a
				}
				slots, err := reservedOn(context.Background(), r, prov, profile, allocs)
				if err != nil {
					t.Fatal(err)
				}
				var got []string
				for _, s := range slots {
					kind := map[slotKind]string{slotForReplacement: "replacement", slotForSuccessor: "successor", slotForOwnAllocation: "own"}[s.kind]
					got = append(got, kind+":"+strings.TrimPrefix(s.key, rigNS+"/alloc-"))
				}
				if !slices.Equal(got, want) {
					t.Errorf("%s's reserved slots are %v, want %v", prov, got, want)
				}
			}
		})
	}
}

// allocsReader lists a fixed set of Allocations and no Instance.
type allocsReader struct {
	client.Reader
	allocs []v1alpha1.Allocation
}

func (a allocsReader) List(_ context.Context, list client.ObjectList, _ ...client.ListOption) error {
	switch l := list.(type) {
	case *v1alpha1.AllocationList:
		l.Items = append([]v1alpha1.Allocation(nil), a.allocs...)
	case *v1alpha1.InstanceList:
		l.Items = nil
	default:
		return fmt.Errorf("allocsReader lists only Allocations and Instances, not %T", list)
	}
	return nil
}

func (a allocsReader) Get(_ context.Context, key client.ObjectKey, _ client.Object, _ ...client.GetOption) error {
	return apierrors.NewNotFound(v1alpha1.GroupVersion.WithResource("modeldeployments").GroupResource(), key.Name)
}

// batch is an Allocation's size.
func batch(gpus, cpus int32, mem string) v1alpha1.BatchParameters {
	return v1alpha1.BatchParameters{GPUs: gpus, CPUsPerTask: cpus, Memory: resource.MustParse(mem)}
}

// TestCapsCountOurAllocationsAndTheExpectedOnes: Provider B's QOS caps us at 8 GPUs, 64 CPUs and 480Gi. One
// Running Allocation of ours there uses 4 GPUs, 32 CPUs and 240Gi. A reservation for 4 GPUs, 32 CPUs and 240Gi fits
// exactly (8, 64, 480Gi) and is made; a second for 1 GPU, 8 CPUs and 60Gi is refused, since the first is expected
// though not listed yet: 9 GPUs, 72 CPUs and 540Gi, each above its cap. Once the first's creation fails and its
// slot is given back, the second fits (5, 40, 300Gi). An ended Allocation counts nothing.
func TestCapsCountOurAllocationsAndTheExpectedOnes(t *testing.T) {
	running := v1alpha1.Allocation{ObjectMeta: metav1.ObjectMeta{Namespace: rigNS, Name: "ours"}}
	running.Spec.ProviderConfigReference = providerRef(provB)
	b := batch(4, 32, "240Gi")
	running.Spec.ForProvider.Batch = &b
	running.Status.AtProvider.Phase = v1alpha1.AllocationRunning
	done := v1alpha1.Allocation{ObjectMeta: metav1.ObjectMeta{Namespace: rigNS, Name: "done"}}
	done.Spec.ProviderConfigReference = providerRef(provB)
	big := batch(8, 64, "480Gi")
	done.Spec.ForProvider.Batch = &big
	done.Status.AtProvider.Phase = v1alpha1.AllocationEnded
	r := allocsReader{allocs: []v1alpha1.Allocation{running, done}}
	caps := v1alpha1.QOSLimits{MaxGPUsPerUser: ptrTo(int32(8)), MaxCPUsPerUser: ptrTo(int32(64)), MaxMemPerUser: memQ("480Gi"),
		From: map[string]string{v1alpha1.LimitMaxGPUsPerUser: "now", v1alpha1.LimitMaxCPUsPerUser: "now", v1alpha1.LimitMaxMemPerUser: "now"}}
	tab := newSlotTable()
	now := fixedAt(hoT(9, 0, 0))
	ask := func(key string, size v1alpha1.BatchParameters) slotAnswer {
		a, err := tab.reserveAsk(context.Background(), r, slotAsk{provider: provB, key: rigNS + "/" + key, params: &size, caps: caps}, now)
		if err != nil {
			t.Fatal(err)
		}
		return a
	}
	if a := ask("first", batch(4, 32, "240Gi")); !a.ok {
		t.Fatalf("the first reservation, filling the caps exactly, was refused: %s", a.capsWhy)
	}
	a := ask("second", batch(1, 8, "60Gi"))
	want := "our Allocations on Provider provider-b with a new one would need 9 GPUs, above the maxGpusPerUser 8 of QOS now and 72 CPUs, above the maxCpusPerUser 64 of QOS now and 540Gi of memory, above the maxMemPerUser 480Gi of QOS now"
	if a.ok || a.capsWhy != want {
		t.Fatalf("the second reservation: ok %v, %q; want refused with %q", a.ok, a.capsWhy, want)
	}
	tab.release(provB, rigNS+"/first")
	if a := ask("second", batch(1, 8, "60Gi")); !a.ok {
		t.Fatalf("the second reservation, once the first was given back, was refused: %s", a.capsWhy)
	}
	if n := len(tab.params[provB]); n != 1 {
		t.Fatalf("%d sizes kept for Provider B's expectations, want the second's alone", n)
	}
}

// TestAReplacementOnAnotherProviderFreesItsPredecessorsSlot (#617): P is Running on A under a submit limit of 2, A
// counting P and the slot P holds for its replacement. At P's replacement time A's limit is 1 for a while, so A has
// no room beside P and the replacement R goes to B. Back at a limit of 2, A counts P alone: R, on B, is P's live
// replacement, so P's reservation has lapsed, and a new deployment's primary is created on A. Before #617 A's count
// read only A's Instances, saw P unreplaced, and refused it.
func TestAReplacementOnAnotherProviderFreesItsPredecessorsSlot(t *testing.T) {
	qos := []v1alpha1.QOSProfile{{Name: "example-a2", MaxSubmitPerUser: ptrTo(int32(2))}}
	rg := newRig(t, append(qosOpts(qos, "", "example-a2"), withMaxTime(4*time.Hour))...)
	rg.addProvider(stuckSecond, "gpu-long", 4*time.Hour, nil, nil)
	reservedStart(rg, hoMD, func(md *v1alpha1.ModelDeployment) { md.Spec.Providers.Names = []string{rigProvider, stuckSecond} })
	p := rg.instances(hoMD)[0]
	hoStepTo(rg, hoT(9, 19, 20), 5*time.Minute, nil)
	rg.setSubmitLimit("example-a2", 1)
	hoStepTo(rg, hoT(9, 20, 0), 10*time.Second, nil)
	reps := hoRepl(rg, p.Name)
	if len(reps) != 1 || reps[0].Labels[v1alpha1.LabelProvider] != stuckSecond {
		t.Fatalf("P's replacements at 09:20:00: %+v, want one, on %s", reps, stuckSecond)
	}
	rg.setSubmitLimit("example-a2", 2)
	other := mdSpec("md-c")
	rg.create(other)
	rg.clock.Advance(10 * time.Second)
	rg.Step()
	var onA int
	for _, in := range rg.liveInstances("md-c") {
		if in.Labels[v1alpha1.LabelProvider] == rigProvider {
			onA++
		}
	}
	if onA != 1 {
		c := rg.condition("md-c", v1alpha1.ConditionDegraded)
		t.Fatalf("md-c has %d Instances on A, want its primary: Degraded %s %q", onA, c.Reason, c.Message)
	}
}
