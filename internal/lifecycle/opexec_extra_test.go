package lifecycle

// What an Executing Operation holds back, and what it leaves alone, beside the over-allocation extra (#589 PR 5 merged into #616
// PR 4; HBEP-7, Reconcile loop, steps 7 and 8, and Over-allocation and qualification (beta), When the extra is submitted;
// HBEP-10, Approval and execution). Two rules meet here, and each row keeps one of them:
//   - #707's window: from an Operation's commit until its spec write lands, NOTHING is created for the deployment, extras
//     included (capacity() runs only while creationHeld() is false, and an Instance with no Allocation is passed over before
//     ensureAllocation, the cut-short extra's retry);
//   - after the spec write, a missing primary placed beside an Executing Operation gets its extra when the checks admit it
//     (createExtras comes before holdsSelection's return in capacity()), and step 7 takes nothing of its own.
// Every count is worked by hand in the row, never read from warmOf, extraBound or the selection. The world is newThreeWorld's: three
// ready Instances R1, R2 and R3 on Provider A (overAllocation 0 there; the rows set k = 1 once it is built), jobs started five
// minutes apart, so a Scale to two takes R1, the nearest lease end.

import (
	"errors"
	"strings"
	"testing"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// newCutWorld is newThreeWorld's world of n ready Instances on Provider A, their jobs started five minutes apart, with the Allocation
// cutter on the store, so a row can cut an Allocation's create short. overAllocation is 0 until a row sets it.
func newCutWorld(t *testing.T, n int) (w *execWorld, cut *allocCutter, insts, allocs []string) {
	t.Helper()
	log := newCauseLog()
	cut = &allocCutter{}
	h := newHedgeRig(t, withInterceptors(cut.funcs(log.funcs())))
	for range n {
		h.batch.Enqueue(pfake.Script{})
	}
	h.Step()
	md := noOverAllocation(mdSpec(hoMD))
	md.Spec.Instances = 1
	h.create(md)
	pol := globalPolicy(person)
	h.create(&pol)
	for c := 1; c <= n; c++ {
		if c > 1 {
			h.run(5*time.Minute, 30*time.Second)
			h.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = int32(c) })
		}
		h.until(10*time.Minute, 10*time.Second, "ready", func() bool { return h.readyCount(hoMD) == c })
	}
	for _, in := range h.liveInstances(hoMD) {
		insts, allocs = append(insts, in.Name), append(allocs, in.Spec.AllocationRef.Name)
	}
	w = &execWorld{hedgeRig: h, log: log, fc: &faultClient{Client: h.r.Client}}
	h.r.Client = w.fc
	return w, cut, insts, allocs
}

// withK sets the world's overAllocation, after the world is built.
func (w *execWorld) withK(k int32) {
	w.t.Helper()
	w.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.OverAllocation = ptrTo(k) })
}

// stepTolerant is one round that tolerates the errors the rows cause on purpose: a failing write ("injected") and a cut-short create.
func (w *execWorld) stepTolerant() {
	w.t.Helper()
	if err := w.stepKeepingErrors(); err != nil && !strings.Contains(err.Error(), "injected") && !errors.Is(err, errHOKilled) && !errors.Is(err, errKilled) {
		w.t.Fatalf("a pass failed with %v", err)
	}
}

// passesTolerant runs rounds of 10 s for the total.
func (w *execWorld) passesTolerant(total time.Duration) {
	w.t.Helper()
	for end := w.clock.Now().Add(total); w.clock.Now().Before(end); {
		w.clock.Advance(10 * time.Second)
		w.stepTolerant()
	}
}

// untilTolerant runs rounds of 10 s until cond holds, and fails after limit.
func (w *execWorld) untilTolerant(limit time.Duration, what string, cond func() bool) {
	w.t.Helper()
	for end := w.clock.Now().Add(limit); !cond(); {
		if !w.clock.Now().Before(end) {
			w.t.Fatalf("%s: not reached within %s (now %s)", what, limit, w.clock.Now().Format(time.RFC3339))
		}
		w.clock.Advance(10 * time.Second)
		w.stepTolerant()
	}
}

// loseJob cancels the Allocation's job from outside, as an administrator would.
func (w *execWorld) loseJob(alloc string) {
	w.t.Helper()
	a := w.allocOrNil(alloc)
	if a == nil {
		w.t.Fatalf("no Allocation %s to lose", alloc)
	}
	w.batch.CancelExternally(a.Annotations[v1alpha1.AnnotationJobID])
}

// withoutAllocation lists the deployment's live Instances whose Allocation does not exist: made, and cut short before its Allocation.
func (w *execWorld) withoutAllocation() []string {
	w.t.Helper()
	var out []string
	for _, in := range w.instances(hoMD) {
		if ended(&in) {
			continue
		}
		if _, ok := w.allocation(in.Spec.AllocationRef.Name); !ok {
			out = append(out, in.Name)
		}
	}
	return out
}

// createdSince lists the objects of kind ("Instance " or "Allocation ") the deployment has now and the snapshot did not.
func (w *execWorld) createdSince(before map[string]bool, kind string) []string {
	var out []string
	for n := range w.objectNames() {
		if strings.HasPrefix(n, kind) && !before[n] {
			out = append(out, n)
		}
	}
	return out
}

// planScale plans a Scale to n of the world, through rounds that tolerate a cut-short create, and checks its targets.
func (w *execWorld) planScale(n int32, want map[string]v1alpha1.PlanEffect, why string) {
	w.t.Helper()
	w.create(opRigOp("scale", scaleTo(n)))
	w.untilTolerant(time.Minute, "the Scale planned", func() bool { return w.getOp("scale").Status.Phase == v1alpha1.OperationPhasePlanned })
	if got := planTargets(w.getOp("scale").Status.Plan); !sameTargets(got, want) {
		w.t.Fatalf("the plan is %v, want %v: %s", got, want, why)
	}
}

// #707's window. A Scale to two is committed, and its spec write fails, so it stays Executing with the spec still at three and
// SpecWritten not recorded. Before the commit the deployment holds a cut-short extra X: an Instance with no Allocation, made
// beside a missing primary at k = 1 with its Allocation's create cut short. In the window all three ready Instances are lost
// from outside, which leaves X alone: by the Scale's cap the deployment lacks min(3, 2) - 1 = 1 primary. Hand-worked
// expectation, for 3 minutes of rounds: nothing is created (no Instance, no Allocation, no job), X gets no Allocation and is not
// ended, and the Operation is still Executing with no SpecWritten. A pass that ran capacity() would place a primary for the
// missing one, and one that retried X would submit it.
func TestNothingIsCreatedBetweenAnOperationsCommitAndItsSpecWrite(t *testing.T) {
	w, cut, insts, allocs := newCutWorld(t, 3)
	w.withK(1)
	// R3 is lost with the cutter armed to let the first create through: the pass makes the missing primary P and then the extra X,
	// whose Allocation create is cut short (the cutter stays armed, so every retry of X is cut too, until the window).
	cut.arm(hoMD, 1)
	w.loseJob(allocs[2])
	w.untilTolerant(10*time.Minute, "a cut-short extra", func() bool { return len(w.withoutAllocation()) == 1 })
	x := w.withoutAllocation()[0]
	w.untilTolerant(10*time.Minute, "R1, R2 and the replacement ready", func() bool { return w.readyCount(hoMD) == 3 })
	if got := w.withoutAllocation(); len(got) != 1 || got[0] != x {
		t.Fatalf("Instances with no Allocation %v, want X (%s) alone: the row proves nothing without a cut-short extra", got, x)
	}
	var primary string // the replacement primary P, ready: neither R1, R2, R3 nor X
	for _, in := range w.liveInstances(hoMD) {
		if in.Name != insts[0] && in.Name != insts[1] && in.Name != x {
			primary = in.Spec.AllocationRef.Name
		}
	}
	w.planScale(2, map[string]v1alpha1.PlanEffect{"Instance " + insts[0]: v1alpha1.PlanEffectDrainInstance, "Allocation " + allocs[0]: v1alpha1.PlanEffectKeep},
		"a Scale to two of three ready takes the nearest lease end, R1, as a drain, and the Allocation it empties stays; X, a surplus extra within k, is left alone")
	stop := w.failSpecWrites()
	w.confirm("scale")
	w.untilTolerant(time.Minute, "the commit", func() bool { return len(w.getOp("scale").Status.Committed) > 0 })
	op := w.getOp("scale")
	if !isExecuting(op) || apimeta.IsStatusConditionTrue(op.Status.Conditions, v1alpha1.ConditionSpecWritten) || w.md(hoMD).Spec.Instances != 3 {
		t.Fatalf("phase %q, SpecWritten %v, spec.instances %d: the window is an Executing Scale whose spec write has not landed, at three",
			op.Status.Phase, apimeta.IsStatusConditionTrue(op.Status.Conditions, v1alpha1.ConditionSpecWritten), w.md(hoMD).Spec.Instances)
	}
	cut.disarm() // so a retry of X would succeed: only the window keeps it from running
	before, sbatches := w.objectNames(), w.submissions()
	w.loseJob(allocs[0])
	w.loseJob(allocs[1])
	w.loseJob(primary)
	w.passesTolerant(3 * time.Minute)
	for _, kind := range []string{"Instance ", "Allocation "} {
		if got := w.createdSince(before, kind); len(got) != 0 {
			t.Errorf("created in the window: %v: nothing is created for the deployment between the commit and the spec write", got)
		}
	}
	if w.submissions() != sbatches {
		t.Errorf("%d jobs submitted in the window, %d before", w.submissions(), sbatches)
	}
	if in := w.instanceOrNil(x); in == nil || ended(in) {
		t.Errorf("X is %v, want it pending: the window's retry neither submits nor ends it", in)
	}
	if got := w.withoutAllocation(); len(got) != 1 || got[0] != x {
		t.Errorf("Instances with no Allocation %v, want X alone: its retry must not make its Allocation in the window", got)
	}
	op = w.getOp("scale")
	if !isExecuting(op) || apimeta.IsStatusConditionTrue(op.Status.Conditions, v1alpha1.ConditionSpecWritten) {
		t.Errorf("phase %q, SpecWritten %v at the end: the row held the Operation in its window", op.Status.Phase, apimeta.IsStatusConditionTrue(op.Status.Conditions, v1alpha1.ConditionSpecWritten))
	}
	stop()
}

// The window's other opening: a commit whose reply is lost. An approved Release of the kept Allocation is committed, and the write
// is stored but the pass is told it failed (the Operation is read again and is Executing, with no spec write recorded). In that
// pass a primary is missing, since one serving Instance's job was cancelled from outside just before; creationHeld() holds the
// pass's creations (capacity() does not run), where opsHold, which a failed spec write sets, is not set, because executing() is not
// reached. At k = 1 hand-worked expectation for that pass: no new Instance, no new Allocation, no job. The next pass, which
// lifts the hold, places the primary on a new Allocation: the committed kept Allocation is not warm (warmOf) and gets none.
func TestNothingIsCreatedInThePassWhoseCommitsReplyWasLost(t *testing.T) {
	w, serving, kept := newKeptThreeWorld(t)
	w.withK(1)
	w.plan("release", releaseSpec(kept))
	w.confirm("release")
	w.untilPhase("release", v1alpha1.OperationPhaseAwaitingApproval)
	lostOnce := true
	w.fc.setFailAfter(func(method string, obj client.Object) error {
		if o, ok := obj.(*v1alpha1.Operation); ok && o.Name == "release" && o.Status.Phase == v1alpha1.OperationPhaseExecuting && lostOnce {
			lostOnce = false
			return errors.New("injected: the reply of the commit's write is lost")
		}
		return nil
	})
	w.loseJob(w.instance(serving[1]).Spec.AllocationRef.Name)
	before, sbatches := w.objectNames(), w.submissions()
	w.approve("release", "person", userPerson)
	w.clock.Advance(10 * time.Second)
	if err := w.stepKeepingErrors(); err == nil || !strings.Contains(err.Error(), "the reply of the commit's write is lost") {
		t.Fatalf("the pass that commits returned %v, want the lost reply's error: the row needs a commit stored and unknown to the pass", err)
	}
	if op := w.getOp("release"); !isExecuting(op) || len(op.Status.Committed) != 1 || apimeta.IsStatusConditionTrue(op.Status.Conditions, v1alpha1.ConditionSpecWritten) {
		t.Fatalf("phase %q, %d committed, SpecWritten %v: the commit is stored and its spec write not recorded", op.Status.Phase, len(op.Status.Committed), apimeta.IsStatusConditionTrue(op.Status.Conditions, v1alpha1.ConditionSpecWritten))
	}
	for _, kind := range []string{"Instance ", "Allocation "} {
		if got := w.createdSince(before, kind); len(got) != 0 {
			t.Errorf("created in the pass whose commit's reply was lost: %v", got)
		}
	}
	if w.submissions() != sbatches {
		t.Errorf("%d jobs submitted, %d before", w.submissions(), sbatches)
	}
	w.untilTolerant(5*time.Minute, "the missing primary placed once the hold lifts", func() bool { return w.submissions() > sbatches })
}

// After the spec write. The deployment has four ready Instances R1 to R4 on A, at k = 1. A Scale to three is planned (it takes R1, the
// nearest lease end, as a drain, and R1's Allocation stays), committed, and its spec write lands (spec.instances 3, SpecWritten
// recorded) while the write that drains R1 fails, so the Operation stays Executing and R1 stays ready, a committed target the
// Operation has not taken. R3 and R4 are then lost: by the Scale's cap and the spec the deployment keeps min(3, 3) = 3 primaries and
// has R1 and R2, so one is missing (R1 is not the last ready Instance, so its target does not fail for good, which would finish the
// Operation). The pass places it on a new Allocation and gives it its extra, on the same Provider (createExtras runs before
// capacity() returns for an Executing Operation; the bound finds no surplus Instance and no warm Allocation there, as R3's and R4's
// Allocations failed; no limit, no other deployment): two new Instances and two new Allocations, each with no mark, and nothing of
// its own taken by step 7 (R1 and R2 are ready and not draining; of the new Instances the surplus one may end RankedOut once both
// have passed their checks, which is the extra's own ranking and keeps its Allocation warm, and none drains).
func TestBesideAnExecutingScaleAfterItsSpecWriteAMissingPrimaryGetsItsExtra(t *testing.T) {
	w, _, insts, allocs := newCutWorld(t, 4)
	w.withK(1)
	r1 := insts[0]
	w.planScale(3, map[string]v1alpha1.PlanEffect{"Instance " + r1: v1alpha1.PlanEffectDrainInstance, "Allocation " + allocs[0]: v1alpha1.PlanEffectKeep},
		"a Scale to three of four ready takes the nearest lease end, R1, as a drain, and the Allocation it empties stays")
	w.fc.set(func(method string, obj client.Object) error {
		if in, ok := obj.(*v1alpha1.Instance); ok && in.Name == r1 && isDraining(in) {
			return errors.New("injected: the write that drains R1 fails")
		}
		return nil
	}, nil)
	w.confirm("scale")
	w.untilTolerant(time.Minute, "the spec write", func() bool {
		return apimeta.IsStatusConditionTrue(w.getOp("scale").Status.Conditions, v1alpha1.ConditionSpecWritten) && w.md(hoMD).Spec.Instances == 3
	})
	if op := w.getOp("scale"); !isExecuting(op) || len(op.Status.Committed) == 0 {
		t.Fatalf("phase %q with %d committed: R1's drain fails, so the Scale is still Executing after its spec write", op.Status.Phase, len(op.Status.Committed))
	}
	before, sbatches := w.objectNames(), w.submissions()
	w.loseJob(allocs[2])
	w.loseJob(allocs[3])
	w.untilTolerant(5*time.Minute, "the missing primary placed", func() bool { return w.submissions() > sbatches })
	if op := w.getOp("scale"); !isExecuting(op) {
		t.Fatalf("phase %q when the missing primary was placed: the row is about a pass beside an Executing Scale", op.Status.Phase)
	}
	w.passesTolerant(2 * time.Minute)
	newInsts, newAllocs := w.createdSince(before, "Instance "), w.createdSince(before, "Allocation ")
	if len(newInsts) != 2 || len(newAllocs) != 2 {
		t.Fatalf("created Instances %v and Allocations %v, want two of each: the missing primary and its extra", newInsts, newAllocs)
	}
	for _, in := range w.instances(hoMD) {
		isNew := !before["Instance "+in.Name]
		if isNew && (len(in.Annotations) != 0 || in.Labels[v1alpha1.LabelProvider] != rigProvider) {
			t.Errorf("Instance %s has annotations %v on Provider %q: neither the primary nor the extra carries a mark, and both are on %s", in.Name, in.Annotations, in.Labels[v1alpha1.LabelProvider], rigProvider)
		}
		// The extra's own ranking is not step 7: when the round's two Instances have passed their checks, the surplus one ends RankedOut
		// and its Allocation stays warm (rank.go). A drain, or an end for any other reason, would be step 7's own victim.
		if (isNew || in.Name == insts[0] || in.Name == insts[1]) && (isDraining(&in) || (ended(&in) && in.Status.EndReason != v1alpha1.ReasonRankedOut)) {
			t.Errorf("Instance %s (new %v, R1 %v, R2 %v) is %q, ended for %q: step 7 takes nothing of its own", in.Name, isNew, in.Name == insts[0], in.Name == insts[1], in.Status.Phase, in.Status.EndReason)
		}
	}
	if op := w.getOp("scale"); !isExecuting(op) {
		t.Errorf("phase %q at the end, want Executing", op.Status.Phase)
	}
}

// TestTheExtrasBoundDoesNotCountAWarmAllocationAnExecutingOperationCommittedForRelease: the extra's bound asks whether the
// deployment has a warm Allocation on the Provider (warmOf), and an Allocation an Executing Operation committed to release is
// not warm there, as one pending release (releasePending) is not: its release is made, so an extra may be admitted beside it.
// The hand-built population is a ready Instance R on prov-a and a warm Allocation "warm" (Running, no Instance) there. Without
// an Operation the warm Allocation bounds prov-a. With an Executing Operation that committed it with a releasing effect it does
// not. A committed target of another Allocation, or an Operation that is not Executing, leaves it warm.
func TestTheExtrasBoundDoesNotCountAWarmAllocationAnExecutingOperationCommittedForRelease(t *testing.T) {
	op := func(phase v1alpha1.OperationPhase, uid string) *v1alpha1.Operation {
		o := &v1alpha1.Operation{}
		o.Name, o.Namespace, o.UID = "op", "ns", "op-uid"
		o.Status.Phase = phase
		o.Status.Committed = []v1alpha1.CommittedTarget{{Kind: v1alpha1.TargetKindAllocation, Name: "warm", UID: types.UID(uid), Effect: v1alpha1.PlanEffectReleaseKept}}
		return o
	}
	for _, c := range []struct {
		name  string
		ops   []*v1alpha1.Operation
		bound bool // true: the extra is admitted on prov-a
	}{
		{"no Operation: the warm Allocation bounds prov-a", nil, false},
		{"an Executing Operation committed it to release: it is not warm", []*v1alpha1.Operation{op(v1alpha1.OperationPhaseExecuting, "auid-warm")}, true},
		{"an Executing Operation committed another Allocation: it stays warm", []*v1alpha1.Operation{op(v1alpha1.OperationPhaseExecuting, "auid-other")}, false},
		{"an Operation that is not Executing yet: it stays warm", []*v1alpha1.Operation{op(v1alpha1.OperationPhaseAwaitingApproval, "auid-warm")}, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := newSelFx()
			f.add("R", v1alpha1.InstanceReady, hardEnd(hoT(12, 10, 0)))
			f.warmAlloc("warm", "prov-a")
			f.allocs["warm"].UID = "auid-warm"
			p := f.pass(1, 1)
			p.ops = c.ops
			if got := p.extraBound(&providerView{name: "prov-a"}); got != c.bound {
				t.Fatalf("extraBound on prov-a = %v, want %v", got, c.bound)
			}
		})
	}
}
