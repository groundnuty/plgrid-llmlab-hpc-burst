package lifecycle

// An Operation's plan has no side effect (#589, PR 4, AC 5), on the scenario rig: the real Reconciler's passes
// over two Providers, each with its own fake Slurm and its call log. The world: R is ready on Provider A, P2
// pends on A behind a 10 h queue, and K is a STARTED Allocation on B that hosts no Instance (a kept one: the
// Instance that ran on it ended when the deployment was scaled down, and retention keeps the Allocation). One
// Operation of each action is created and never confirmed. The test shows, from the fake Slurm's own call log and
// not from the API alone, that no sbatch, scancel, hold release or --test-only was made; that the lifecycle
// controller's client wrote nothing but Operation status (every write it makes is recorded, and the Provider
// doubles, which keep rewriting their Allocations' status on their own, write through another client); and that each
// plan lists its targets by name.
//
// The venue is the lifecycle package's rig and not cmd/hpcburst-controller's envtest wiring: that harness has no
// path that provisions a ModelDeployment (it needs a Hub double and a sized partition), and an Allocation made by
// hand in it is cancelled by retention before it starts. The rig's Slurm doubles log every call, and its passes are
// Reconcile's, so the step under test is the one the binary runs.

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// slurmReads are the calls of the fake Slurm that only read: the call log of a plan holds these and no other.
var slurmReads = map[string]bool{"squeue": true, "squeue-me": true, "sacct": true, "sacct-job": true}

// writesOf counts the calls other than reads, by name, in a call log.
func writesOf(calls []pfake.Call) map[string]int {
	out := map[string]int{}
	for _, c := range calls {
		if !slurmReads[c.Op] {
			out[c.Op]++
		}
	}
	return out
}

// opWriteLog records every write of the lifecycle controller's client: its method, the object's type and name.
type opWriteLog struct {
	client.Client
	mu     sync.Mutex
	writes []string
}

func (w *opWriteLog) note(method string, obj client.Object) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.writes = append(w.writes, fmt.Sprintf("%s %T %s", method, obj, obj.GetName()))
}

func (w *opWriteLog) take() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := w.writes
	w.writes = nil
	return out
}

func (w *opWriteLog) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	w.note("Create", obj)
	return w.Client.Create(ctx, obj, opts...)
}

func (w *opWriteLog) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	w.note("Update", obj)
	return w.Client.Update(ctx, obj, opts...)
}

func (w *opWriteLog) Patch(ctx context.Context, obj client.Object, p client.Patch, opts ...client.PatchOption) error {
	w.note("Patch", obj)
	return w.Client.Patch(ctx, obj, p, opts...)
}

func (w *opWriteLog) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	w.note("Delete", obj)
	return w.Client.Delete(ctx, obj, opts...)
}

func (w *opWriteLog) DeleteAllOf(ctx context.Context, obj client.Object, opts ...client.DeleteAllOfOption) error {
	w.note("DeleteAllOf", obj)
	return w.Client.DeleteAllOf(ctx, obj, opts...)
}

func (w *opWriteLog) Status() client.SubResourceWriter {
	return opStatusLog{w: w, SubResourceWriter: w.Client.Status()}
}

// opStatusLog records the status writes.
type opStatusLog struct {
	client.SubResourceWriter
	w *opWriteLog
}

func (s opStatusLog) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	s.w.note("Status.Update", obj)
	return s.SubResourceWriter.Update(ctx, obj, opts...)
}

func (s opStatusLog) Patch(ctx context.Context, obj client.Object, p client.Patch, opts ...client.SubResourcePatchOption) error {
	s.w.note("Status.Patch", obj)
	return s.SubResourceWriter.Patch(ctx, obj, p, opts...)
}

func opRigOp(name string, spec v1alpha1.OperationSpec) *v1alpha1.Operation {
	spec.Owner = "agent-a"
	spec.ModelDeploymentRef = v1alpha1.ObjectReference{Name: hoMD}
	return &v1alpha1.Operation{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: rigNS}, Spec: spec}
}

func TestPlanningAnOperationOfEachActionHasNoSideEffect(t *testing.T) {
	h := newHedgeRig(t)
	// This test checks planning's side effects in a hand-seeded world (R, M1, the seeded M2, P2), so it pins hedging's
	// growth off. With growth on, a hedge member of its own wins (it is placed on a kept started Allocation and is
	// ready at once) and the seeded P2 is cancelled before the Operations are made. #627's own tests cover growth.
	h.r.hedgeFrozen = func(hedgeGroup) bool { return true }
	// A: R starts at once; M1 and P2 pend behind 10 h. B: the member M2 starts at once and its engine is slow, so
	// its Instance stays starting.
	h.batch.Enqueue(pfake.Script{}, pfake.Script{QueueWait: 10 * time.Hour}, pfake.Script{QueueWait: 10 * time.Hour})
	h.b.batch.Enqueue(pfake.Script{})
	h.Step()
	md := mdSpec(hoMD)
	onBoth(md)
	md.Spec.Instances = 2
	// No over-allocation: with k = 1 a Scale to one leaves P2, the pending extra, alone, and its plan is empty.
	md.Spec.OverAllocation = new(int32)
	h.create(md)
	h.until(5*time.Minute, 10*time.Second, "one ready Instance", func() bool { return h.readyCount(hoMD) == 1 })
	var r, m1 string
	for _, in := range h.onProvider(hoMD, rigProvider) {
		if in.Status.Phase == v1alpha1.InstanceReady {
			r = in.Name
		} else {
			m1 = in.Name
		}
	}
	m2 := h.seedHedgeMember(hoMD, m1, "", hedgeB)
	m2Alloc := h.mustInstance(m2).Spec.AllocationRef.Name
	h.slow[m2Alloc] = true
	h.until(time.Minute, 10*time.Second, "B's member started", func() bool {
		a, ok := h.allocation(m2Alloc)
		return ok && a.Status.AtProvider.StartTime != nil
	})
	// A person lowers the count: the hedge goes whole, M1's pending Allocation is cancelled, M2 ends and its started
	// Allocation K stays. Then the count is raised again, and a new primary P2 pends on A.
	m1Alloc := h.mustInstance(m1).Spec.AllocationRef.Name
	h.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 1 })
	h.until(time.Minute, 10*time.Second, "M1's Allocation released and M2 ended", func() bool {
		return h.released(m1Alloc) && ended(h.mustInstance(m2))
	})
	h.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 2 })
	var p2 string
	h.until(time.Minute, 10*time.Second, "P2 created", func() bool {
		for _, in := range h.onProvider(hoMD, rigProvider) {
			if in.Name != r && in.Name != m1 && !ended(&in) {
				p2 = in.Name
			}
		}
		return p2 != ""
	})
	p2Alloc := h.mustInstance(p2).Spec.AllocationRef.Name
	rAlloc := h.mustInstance(r).Spec.AllocationRef.Name
	if a, ok := h.allocation(m2Alloc); !ok || a.Status.AtProvider.StartTime == nil || !a.DeletionTimestamp.IsZero() {
		t.Fatalf("K, M2's Allocation, should be started and kept: %+v %v", a, ok)
	}

	// Let the world settle, then record from here: every write of the lifecycle controller's client, and the fake
	// Slurms' call logs. A control window with no Operation comes first: the controller writes the deployment's
	// status on its own every pass, so what planning must not do is write anything the control window did not. The
	// windows are ten minutes, well before R's lease nears its end, when the controller makes R's replacement on its
	// own (an hour of passes made one, and the test counted it as planning's).
	h.run(5*time.Minute, 20*time.Second)
	wl := &opWriteLog{Client: h.r.Client}
	h.r.Client = wl
	h.run(10*time.Minute, 30*time.Second)
	ordinary := map[string]bool{}
	for _, w := range wl.take() {
		ordinary[w] = true
	}
	if len(ordinary) == 0 {
		t.Fatal("the control window recorded no write: the log sees nothing")
	}
	callsA, callsB := writesOf(h.batch.Calls()), writesOf(h.b.batch.Calls())

	// One Operation of each action, never confirmed.
	h.create(opRigOp("scale", v1alpha1.OperationSpec{Action: v1alpha1.OperationActionScale, Scale: &v1alpha1.ScaleParameters{Instances: 1}}))
	h.create(opRigOp("end", v1alpha1.OperationSpec{Action: v1alpha1.OperationActionEnd}))
	h.create(opRigOp("release", v1alpha1.OperationSpec{Action: v1alpha1.OperationActionRelease, Release: &v1alpha1.ReleaseParameters{Allocations: []string{m2Alloc}}}))
	planned := func() bool {
		for _, n := range []string{"scale", "end", "release"} {
			var op v1alpha1.Operation
			if err := h.c.Get(h.ctx, client.ObjectKey{Namespace: rigNS, Name: n}, &op); err != nil || op.Status.Phase != v1alpha1.OperationPhasePlanned {
				return false
			}
		}
		return true
	}
	h.until(2*time.Minute, 10*time.Second, "every Operation Planned", planned)
	// And ten minutes more of passes: the plans are not written again.
	h.run(10*time.Minute, 30*time.Second)

	// No side effect: from the fake Slurms' own call logs, and from the API.
	if got := writesOf(h.batch.Calls()); !equalCounts(got, callsA) {
		t.Errorf("Provider A's Slurm saw writes during planning: %v before, %v after", callsA, got)
	}
	if got := writesOf(h.b.batch.Calls()); !equalCounts(got, callsB) {
		t.Errorf("Provider B's Slurm saw writes during planning: %v before, %v after", callsB, got)
	}
	for _, calls := range [][]pfake.Call{h.batch.Calls(), h.b.batch.Calls()} {
		for _, c := range calls {
			if !slurmReads[c.Op] && strings.Contains(c.Op, "test") {
				t.Errorf("a --test-only call was made: %s %v", c.Op, c.Args)
			}
		}
	}
	// The lifecycle controller wrote Operation status, beside what it writes in any window: no write to an object
	// the control window did not write, and no create, update, patch or delete at all.
	opWrites := 0
	for _, w := range wl.take() {
		switch {
		case strings.HasPrefix(w, "Status.Update *v1alpha1.Operation "):
			opWrites++
		case !strings.HasPrefix(w, "Status.Update ") && !strings.HasPrefix(w, "Status.Patch "):
			t.Errorf("planning made a write that is not a status write: %s", w)
		case !ordinary[w]:
			t.Errorf("planning made a write the control window did not make: %s", w)
		}
	}
	// The log sees the writes it is meant to see: each of the three Operations was planned by one status write, and
	// the later passes added none.
	if opWrites != 3 {
		t.Errorf("%d Operation status writes were recorded, want one for each of the three plans", opWrites)
	}

	// Each plan lists every target by name, with the effect the controller's own rules give it.
	want := map[string][]wantTarget{
		// Scale to 1 takes the pending primary P2 and its Allocation; R serves on.
		"scale": {
			{kind: tAlloc, name: p2Alloc, effect: v1alpha1.PlanEffectCancelPending},
			{kind: tInst, name: p2, effect: v1alpha1.PlanEffectCancelPending},
		},
		// End takes everything: R drains (the last ready one), P2 is cancelled, each Allocation goes, K included
		// (a started one: released at once, the end time being now), and the deployment gets its end time.
		"end": {
			{kind: tAlloc, name: m2Alloc, effect: v1alpha1.PlanEffectReleaseStarted},
			{kind: tAlloc, name: p2Alloc, effect: v1alpha1.PlanEffectCancelPending},
			{kind: tAlloc, name: rAlloc, effect: v1alpha1.PlanEffectReleaseStarted},
			{kind: tInst, name: p2, effect: v1alpha1.PlanEffectCancelPending},
			{kind: tInst, name: r, effect: v1alpha1.PlanEffectDrainLastReady},
			{kind: tMD, name: hoMD, effect: v1alpha1.PlanEffectSetEndTime},
		},
		// Release of the kept, empty K.
		"release": {{kind: tAlloc, name: m2Alloc, effect: v1alpha1.PlanEffectReleaseKept}},
	}
	for name, targets := range want {
		var op v1alpha1.Operation
		if err := h.c.Get(h.ctx, client.ObjectKey{Namespace: rigNS, Name: name}, &op); err != nil {
			t.Fatal(err)
		}
		plan := op.Status.Plan
		if plan == nil || len(plan.Targets) != len(targets) {
			t.Errorf("%s: plan %v, want the %d targets %v", name, plan, len(targets), targets)
			continue
		}
		// The plan's order is by kind and name, and these names are the rig's random ones: compare as a set.
		got := map[string]v1alpha1.PlanEffect{}
		for _, g := range plan.Targets {
			got[string(g.Kind)+" "+g.Name] = g.Effect
		}
		for _, w := range targets {
			if e, ok := got[string(w.kind)+" "+w.name]; !ok || e != w.effect {
				t.Errorf("%s: %s %s is %q in the plan (listed: %v), want %s", name, w.kind, w.name, e, ok, w.effect)
			}
		}
		if op.Spec.Confirm != nil || op.Status.Phase != v1alpha1.OperationPhasePlanned {
			t.Errorf("%s: phase %q, confirm %v: an Operation never confirmed stays Planned", name, op.Status.Phase, op.Spec.Confirm)
		}
	}
}

func equalCounts(a, b map[string]int) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// A deleted ModelDeployment with nothing left is not reconciled again, unless an UNFINISHED Operation names it.
// An Operation on a deployment that is gone is Refused in its first pass, which is terminal: the key then stops
// requeueing, though the Refused Operation stays (nothing removes a finished Operation until PR 5, and holding
// the key for it would requeue a deleted deployment every 30 s for as long as it exists).
func TestTheKeyOfADeletedDeploymentStopsWhenItsOperationsAreFinished(t *testing.T) {
	rg := newRig(t)
	key := client.ObjectKey{Namespace: rigNS, Name: "gone"}
	res, err := rg.r.Reconcile(rg.ctx, ctrl.Request{NamespacedName: key})
	if err != nil || res.RequeueAfter != 0 {
		t.Fatalf("with no Operation: result %+v, error %v, want no more passes", res, err)
	}
	op := opRigOp("scale", v1alpha1.OperationSpec{Action: v1alpha1.OperationActionScale, Scale: &v1alpha1.ScaleParameters{Instances: 0}})
	op.Spec.ModelDeploymentRef.Name = "gone"
	rg.create(op)
	res, err = rg.r.Reconcile(rg.ctx, ctrl.Request{NamespacedName: key})
	if err != nil || res.RequeueAfter != 0 {
		t.Fatalf("with a Refused Operation naming it: result %+v, error %v, want no more passes", res, err)
	}
	var got v1alpha1.Operation
	if err := rg.c.Get(rg.ctx, client.ObjectKeyFromObject(op), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != v1alpha1.OperationPhaseRefused {
		t.Errorf("an Operation on a ModelDeployment that is gone is %q, want Refused", got.Status.Phase)
	}
}

// The key stays alive while an Operation that is not finished names the deleted deployment: an Executing Release of
// one Allocation whose finalizer holds it, so the target is not done and the Operation stays Executing, requeues the
// key. Once the finalizer goes, the target is done, the Operation is Done, and the key stops.
func TestTheKeyOfADeletedDeploymentStaysAliveWhileAnUnfinishedOperationNamesIt(t *testing.T) {
	rg := newRig(t)
	key := client.ObjectKey{Namespace: rigNS, Name: "gone"}
	held := &v1alpha1.Allocation{ObjectMeta: metav1.ObjectMeta{
		Name: "held", Namespace: rigNS, Finalizers: []string{"example.test/hold"},
		Labels: map[string]string{v1alpha1.LabelModelDeployment: "gone"},
	}}
	held.Spec.ModelDeploymentRef = &v1alpha1.ObjectReference{Name: "gone"}
	rg.create(held)
	var stored v1alpha1.Allocation
	if err := rg.c.Get(rg.ctx, client.ObjectKeyFromObject(held), &stored); err != nil {
		t.Fatal(err)
	}
	op := opRigOp("release", v1alpha1.OperationSpec{Action: v1alpha1.OperationActionRelease, Release: &v1alpha1.ReleaseParameters{Allocations: []string{"held"}}})
	op.Spec.ModelDeploymentRef.Name = "gone"
	rg.create(op)
	var got v1alpha1.Operation
	if err := rg.c.Get(rg.ctx, client.ObjectKeyFromObject(op), &got); err != nil {
		t.Fatal(err)
	}
	// The admin commits it, as the controller would after approval: the target is the Allocation, by its UID.
	got.Status.Phase = v1alpha1.OperationPhaseExecuting
	got.Status.Committed = []v1alpha1.CommittedTarget{{Kind: v1alpha1.TargetKindAllocation, Name: "held", UID: stored.UID, Effect: v1alpha1.PlanEffectReleaseKept}}
	got.Status.Plan = &v1alpha1.OperationPlan{ModelDeployment: v1alpha1.PlannedDeployment{Name: "gone", UID: "uid-gone"}}
	if err := rg.c.Status().Update(rg.ctx, &got); err != nil {
		t.Fatal(err)
	}
	res, err := rg.r.Reconcile(rg.ctx, ctrl.Request{NamespacedName: key})
	if err != nil || res.RequeueAfter == 0 {
		t.Fatalf("with an Executing Operation naming it: result %+v, error %v, want the key requeued", res, err)
	}
	// The finalizer goes: the Allocation is gone, the target is done, the Operation is Done, and the key stops.
	var held2 v1alpha1.Allocation
	if err := rg.c.Get(rg.ctx, client.ObjectKeyFromObject(held), &held2); err != nil {
		t.Fatal(err)
	}
	if held2.DeletionTimestamp.IsZero() {
		t.Fatal("the Release did not delete the Allocation")
	}
	held2.Finalizers = nil
	if err := rg.c.Update(rg.ctx, &held2); err != nil {
		t.Fatal(err)
	}
	res, err = rg.r.Reconcile(rg.ctx, ctrl.Request{NamespacedName: key})
	if err != nil || res.RequeueAfter != 0 {
		t.Fatalf("with the Operation done: result %+v, error %v, want no more passes", res, err)
	}
	if err := rg.c.Get(rg.ctx, client.ObjectKeyFromObject(op), &got); err != nil || got.Status.Phase != v1alpha1.OperationPhaseDone {
		t.Fatalf("the Operation is %q (%v), want Done", got.Status.Phase, err)
	}
}
