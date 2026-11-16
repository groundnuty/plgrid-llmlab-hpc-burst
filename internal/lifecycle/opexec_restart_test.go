package lifecycle

// A controller killed or failing mid-execution (#589, PR 5): it resumes from the committed list and nothing else, the spec
// is written before any act, and a pass that cannot read the Operations takes no victim of its own. The kills are a
// wrapper on the lifecycle controller's client (faultClient, opexec_world_test.go).

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// restart is a controller killed and started again, with a faultClient on its client.
func (w *execWorld) restart(front bool) {
	w.t.Helper()
	w.startController(front)
	w.fc = &faultClient{Client: w.c}
	w.r.Client = w.fc
}

var errKilled = errors.New("the controller was killed")

// stepKeepingErrors is Step, for a test that expects the controller's pass to fail: the Provider and the agents run, then
// the deployment's pass, whose error is returned.
func (w *execWorld) stepKeepingErrors() error {
	w.t.Helper()
	if err := w.prov.Sync(w.ctx); err != nil {
		w.t.Fatalf("provider sync: %v", err)
	}
	w.syncAgents()
	_, err := w.r.Reconcile(w.ctx, ctrl.Request{NamespacedName: client.ObjectKey{Namespace: rigNS, Name: hoMD}})
	return err
}

// moveLeaseEnd is an administrator giving one Allocation's job a much later time limit: its hard end, and with it its
// Instance's lease end, moves later than every other's.
func (w *execWorld) moveLeaseEnd(alloc string) {
	w.t.Helper()
	a := w.allocOrNil(alloc)
	w.batch.SetTimeLimit(a.Annotations[v1alpha1.AnnotationJobID], 12*time.Hour)
	if err := w.prov.Sync(w.ctx); err != nil {
		w.t.Fatalf("provider sync: %v", err)
	}
}

func isDraining(in *v1alpha1.Instance) bool {
	_, began := drainBegan(in)
	return in.Status.Phase == v1alpha1.InstanceDraining || began
}

// The control of the restart test below: three ready Instances, R1's lease end then moved later than the others', and the
// person lowers the count to two. The controller's own selection takes R2, the nearest lease end now. So the Operation
// that committed R1 before the move, and is resumed after it, is tested against a population in which its own selection
// would take another victim, and a test that passed while the controller picked R1 anyway would prove nothing.
func TestAfterTheLeaseEndsMoveTheControllersOwnSelectionTakesAnotherVictim(t *testing.T) {
	w, insts, allocs := newThreeWorld(t)
	w.moveLeaseEnd(allocs[0])
	w.ticks(2) // the lifecycle controller reads the moved hard end into R1's lease end
	w.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 2 })
	w.until(2*time.Minute, 10*time.Second, "a drain", func() bool {
		for _, n := range insts {
			if isDraining(w.instance(n)) {
				return true
			}
		}
		return false
	})
	for i, n := range insts {
		if got, want := isDraining(w.instance(n)), i == 1; got != want {
			t.Errorf("R%d draining is %v, want %v: the controller's own selection takes R2", i+1, got, want)
		}
	}
}

// A controller killed between the commit and the first act resumes from the committed list. The kill: every write of the
// lifecycle controller's client fails once the Operation's status has been written with phase Executing, which is the
// commit; so the commit is the last write that landed, and neither the spec nor any target's act was made. Before the
// restart, the lease ends move, so that the controller's own selection would now take R2 and not the committed R1. On
// the restart the committed target's act fails at first and is retried: R2 and R3 are never touched, and once the fault
// is gone R1 drains and the Operation is Done. Nothing is released twice: no Allocation is released at all.
func TestAKilledControllerResumesFromItsCommittedTargetsAndNothingElse(t *testing.T) {
	w, insts, allocs := newThreeWorld(t)
	r1, r2, r3 := insts[0], insts[1], insts[2]
	planned := w.plan("scale", scaleTo(2))
	want := map[string]v1alpha1.PlanEffect{
		"Instance " + r1:          v1alpha1.PlanEffectDrainInstance, // the nearest lease end, while another stays ready
		"Allocation " + allocs[0]: v1alpha1.PlanEffectKeep,
	}
	if got := planTargets(planned.Status.Plan); !sameTargets(got, want) {
		t.Fatalf("the plan is %v, want %v", got, want)
	}
	if n := countNeeding(planned.Status.Plan); n != 0 {
		t.Fatalf("%d targets need approval; draining an Instance while two stay ready, its Allocation kept, needs none", n)
	}

	// The kill.
	var mu sync.Mutex
	var landed []string
	dead := false
	w.fc.set(
		func(method string, obj client.Object) error {
			mu.Lock()
			defer mu.Unlock()
			if dead {
				return errKilled
			}
			return nil
		},
		func(method string, obj client.Object) {
			mu.Lock()
			defer mu.Unlock()
			landed = append(landed, method+" "+obj.GetName())
			if op, ok := obj.(*v1alpha1.Operation); ok && method == "Status.Update" && op.Status.Phase == v1alpha1.OperationPhaseExecuting {
				dead = true
			}
		})
	w.confirm("scale")
	w.clock.Advance(10 * time.Second)
	if err := w.stepKeepingErrors(); !errors.Is(err, errKilled) {
		t.Fatalf("the pass failed with %v, want the kill", err)
	}
	op := w.getOp("scale")
	if op.Status.Phase != v1alpha1.OperationPhaseExecuting || !sameTargets(committedTargets(op.Status.Committed), want) {
		t.Fatalf("after the kill: phase %q, committed %v, want Executing with %v", op.Status.Phase, committedTargets(op.Status.Committed), want)
	}
	mu.Lock()
	if last := landed[len(landed)-1]; last != "Status.Update scale" {
		t.Errorf("the last write that landed is %q, want the commit's, Status.Update scale", last)
	}
	mu.Unlock()
	if got := w.md(hoMD).Spec.Instances; got != 3 {
		t.Errorf("spec.instances is %d after the kill, want 3: the spec write had not been made", got)
	}
	for i, n := range insts {
		if isDraining(w.instance(n)) {
			t.Errorf("R%d is draining after the kill: no act had been made", i+1)
		}
	}

	// The state changes while the controller is down: the control above shows the controller's own selection now takes R2.
	w.moveLeaseEnd(allocs[0])

	// The restart, with the committed target's act failing at first: it is retried, and nothing else is taken meanwhile.
	w.restart(true)
	failR1 := true
	// Only the write that starts R1's drain fails: its lease end is copied from the moved hard end all the same, so the
	// controller's own selection really reads R1 as the farthest lease end, and R2 as the one it would take.
	w.fc.set(func(method string, obj client.Object) error {
		if in, ok := obj.(*v1alpha1.Instance); ok && in.Name == r1 && failR1 && isDraining(in) {
			return errors.New("injected: the write that drains R1 fails")
		}
		return nil
	}, nil)
	for i := 0; i < 4; i++ {
		w.clock.Advance(10 * time.Second)
		if err := w.stepKeepingErrors(); err == nil || !strings.Contains(err.Error(), "injected") {
			t.Fatalf("restarted pass %d: %v, want R1's failed act", i+1, err)
		}
		for _, n := range []string{r2, r3} {
			if in := w.instance(n); isDraining(in) || in.Status.Phase != v1alpha1.InstanceReady {
				t.Fatalf("restarted pass %d: Instance %s is %q: only the committed R1 is taken, never another victim", i+1, n, in.Status.Phase)
			}
		}
		if isDraining(w.instance(r1)) {
			t.Fatalf("restarted pass %d: R1 is draining although its write fails", i+1)
		}
		// The population the test needs: the moved hard end reached R1's lease end, so R1 is the farthest.
		if i == 0 {
			l1, l2 := w.instance(r1).Status.LeaseEnd, w.instance(r2).Status.LeaseEnd
			if l1 == nil || l2 == nil || !l1.After(l2.Time) {
				t.Fatalf("R1's lease end %v is not after R2's %v: the controller's own selection would still take R1, and the test proves nothing", l1, l2)
			}
		}
	}
	if got := w.md(hoMD).Spec.Instances; got != 2 {
		t.Errorf("spec.instances is %d, want 2: the resumed pass writes the spec the killed one did not", got)
	}
	// R1's act fails on the controller's own write, so its target is Failed and the Operation PartlyFailed (HBEP-10, The
	// report), with the spec written.
	op = w.getOp("scale")
	if o, ok := outcomeOf(op, r1); op.Status.Phase != v1alpha1.OperationPhasePartlyFailed || !isTrue(op.Status.Conditions, v1alpha1.ConditionSpecWritten) ||
		!ok || o.State != v1alpha1.TargetStateFailed || o.Reason != reasonWriteFailed {
		t.Errorf("phase %q, SpecWritten %v, R1's outcome %+v: want PartlyFailed with the spec written and R1 Failed with %s", op.Status.Phase, isTrue(op.Status.Conditions, v1alpha1.ConditionSpecWritten), o, reasonWriteFailed)
	}

	// The fault is gone: R1 drains, the Operation is Done, and R2 and R3 were never touched.
	failR1 = false
	w.untilPhase("scale", v1alpha1.OperationPhaseDone)
	if in := w.instanceOrNil(r1); in != nil && !ended(in) {
		t.Errorf("R1 is %q, want drained and ended", in.Status.Phase)
	}
	for _, n := range []string{r2, r3} {
		if in := w.instance(n); isDraining(in) || in.Status.Phase != v1alpha1.InstanceReady {
			t.Errorf("Instance %s is %q at the end: it was never a target", n, in.Status.Phase)
		}
	}
	for i, a := range allocs {
		if _, ok := w.log.deletedWith(a); ok {
			t.Errorf("Allocation of R%d was deleted: the Operation releases nothing", i+1)
		}
	}
	if got := w.batch.Count("scancel"); got != 0 {
		t.Errorf("%d scancel calls: nothing is released", got)
	}
}

// The spec is written before any act. With the write of spec.instances failing, the committed Operation acts on nothing and
// the pass provisions nothing, however many passes it takes; once the write lands, the targets are acted on and the
// Operation is Done. A pass that acted first would cancel P's Allocation while the count still said two, and capacity
// would provision it back.
func TestAnApprovedScaleActsOnNothingBeforeItsSpecWriteLands(t *testing.T) {
	w := newExecWorld(t, 10*time.Hour)
	failSpec := true
	w.fc.set(func(method string, obj client.Object) error {
		if _, ok := obj.(*v1alpha1.ModelDeployment); ok && method == "Update" && failSpec {
			return errors.New("injected: the write of the spec fails")
		}
		return nil
	}, nil)
	w.plan("scale", scaleTo(1))
	w.confirm("scale")
	w.ticks(2)
	before, sbatches := w.objectNames(), w.submissions()
	w.approve("scale", "person", userPerson)
	for i := 1; i <= 3; i++ {
		w.clock.Advance(10 * time.Second)
		if err := w.stepKeepingErrors(); err == nil || !strings.Contains(err.Error(), "injected") {
			t.Fatalf("pass %d: %v, want the failed spec write", i, err)
		}
		op := w.getOp("scale")
		if op.Status.Phase != v1alpha1.OperationPhaseExecuting || len(op.Status.Committed) == 0 {
			t.Fatalf("pass %d: phase %q with %d committed targets, want Executing: the commit landed", i, op.Status.Phase, len(op.Status.Committed))
		}
		if isTrue(op.Status.Conditions, v1alpha1.ConditionSpecWritten) {
			t.Fatalf("pass %d: SpecWritten is true although the write failed", i)
		}
		if got := w.md(hoMD).Spec.Instances; got != 2 {
			t.Fatalf("pass %d: spec.instances is %d, want 2", i, got)
		}
		if in := w.instance(w.pending); ended(in) {
			t.Fatalf("pass %d: P's Instance was ended before the spec write landed", i)
		}
		if a, ok := w.allocation(w.pendingAlloc); !ok || !a.DeletionTimestamp.IsZero() {
			t.Fatalf("pass %d: P's Allocation was released before the spec write landed", i)
		}
		if _, ok := w.log.deletedWith(w.pendingAlloc); ok {
			t.Fatalf("pass %d: P's Allocation was deleted", i)
		}
	}
	if w.submissions() != sbatches || len(w.objectNames()) != len(before) {
		t.Errorf("a pass whose spec write failed provisioned: %d submissions, %d before", w.submissions(), sbatches)
	}
	failSpec = false
	w.untilPhase("scale", v1alpha1.OperationPhaseDone)
	if got := w.md(hoMD).Spec.Instances; got != 1 {
		t.Errorf("spec.instances is %d, want 1", got)
	}
	w.wantCause(w.pendingAlloc, w.getOp("scale"))
}

// A pass that cannot read the Operations makes no selection of its own beside an Executing Scale: it fails, as a failed
// read of the Allocations does, and the next pass resumes. The world is the restart test's: the spec has landed (two
// asked of three), R1's drain write fails, and the lease ends are moved so that the controller's own selection would take
// R2. One pass whose Operation list fails must leave R2 and R3 alone.
func TestAPassThatCannotReadTheOperationsTakesNoVictimOfItsOwn(t *testing.T) {
	w, insts, allocs := newThreeWorld(t)
	r1, r2, r3 := insts[0], insts[1], insts[2]
	w.plan("scale", scaleTo(2))
	failDrain, failList := true, false
	w.fc.set(func(method string, obj client.Object) error {
		if in, ok := obj.(*v1alpha1.Instance); ok && in.Name == r1 && failDrain && isDraining(in) {
			return errors.New("injected: the write that drains R1 fails")
		}
		return nil
	}, nil)
	w.r.Reader = opListFailer{Reader: w.c, fail: &failList}
	w.confirm("scale")
	pass := func(want string) {
		t.Helper()
		w.clock.Advance(10 * time.Second)
		if err := w.stepKeepingErrors(); err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("the pass: %v, want %q", err, want)
		}
	}
	pass("injected: the write that drains R1") // the commit, the spec write and R1's failed drain
	if got := w.md(hoMD).Spec.Instances; got != 2 {
		t.Fatalf("spec.instances is %d, want 2: the spec had landed", got)
	}
	// The state changes after the commit: the controller's own selection would now take R2.
	w.moveLeaseEnd(allocs[0])
	pass("injected: the write that drains R1")
	failList = true
	pass("list Operations")
	for _, n := range []string{r2, r3} {
		if in := w.instance(n); isDraining(in) || in.Status.Phase != v1alpha1.InstanceReady {
			t.Errorf("Instance %s is %q after a pass that could not read the Operations: the controller took a victim of its own", n, in.Status.Phase)
		}
	}
	// The fake Slurm's call log, not the API alone: no job was cancelled, and no Instance drains at all (R1's own drain
	// is the write that fails).
	if got := w.batch.Count("scancel"); got != 0 {
		t.Errorf("the fake Slurm saw %d scancel calls: a pass that could not read the Operations cancelled a job", got)
	}
	for i, n := range insts {
		if isDraining(w.instance(n)) {
			t.Errorf("Instance R%d is draining after the pass that could not read the Operations", i+1)
		}
	}
	failList, failDrain = false, false
	w.untilPhase("scale", v1alpha1.OperationPhaseDone)
	for _, n := range []string{r2, r3} {
		if in := w.instance(n); isDraining(in) || in.Status.Phase != v1alpha1.InstanceReady {
			t.Errorf("Instance %s is %q at the end", n, in.Status.Phase)
		}
	}
	if got := w.batch.Count("scancel"); got != 0 {
		t.Errorf("%d scancel calls at the end: a Scale that drains an Instance cancels no job", got)
	}
}

// opListFailer fails the list of Operations while fail is set.
type opListFailer struct {
	client.Reader
	fail *bool
}

func (l opListFailer) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if _, ok := list.(*v1alpha1.OperationList); ok && *l.fail {
		return errors.New("injected: the list of Operations fails")
	}
	return l.Reader.List(ctx, list, opts...)
}

// A controller killed after it wrote an Allocation's release cause and before it deleted the Allocation resumes and
// releases it once: the cause is the Operation's, written once, the delete is made on the resumed pass, and the fake
// Slurm sees one scancel for the job. The kill: every write of the lifecycle controller's client fails once the cause's
// write has landed, which is the last write of that Allocation's act.
func TestAControllerKilledBetweenTheCauseAndTheDeleteReleasesOnce(t *testing.T) {
	w := newExecWorld(t, 10*time.Hour)
	w.plan("scale", scaleTo(1))
	var mu sync.Mutex
	dead := false
	w.fc.set(func(string, client.Object) error {
		mu.Lock()
		defer mu.Unlock()
		if dead {
			return errKilled
		}
		return nil
	}, func(method string, obj client.Object) {
		if a, ok := obj.(*v1alpha1.Allocation); ok && method == "Update" && a.Annotations[v1alpha1.AnnotationReleaseCause] != "" {
			mu.Lock()
			dead = true
			mu.Unlock()
		}
	})
	w.confirm("scale")
	w.approve("scale", "person", userPerson)
	w.clock.Advance(10 * time.Second)
	if err := w.stepKeepingErrors(); !errors.Is(err, errKilled) {
		t.Fatalf("the pass failed with %v, want the kill", err)
	}
	op := w.getOp("scale")
	a, ok := w.allocation(w.pendingAlloc)
	if !ok || !a.DeletionTimestamp.IsZero() || a.Annotations[v1alpha1.AnnotationReleaseCause] != operationCauseOf(op) {
		t.Fatalf("after the kill, P's Allocation is %+v (found %v): it should carry the Operation's cause and not be deleted yet", a, ok)
	}
	if got := w.batch.Count("scancel"); got != 0 {
		t.Fatalf("%d scancel calls before the restart, want none", got)
	}
	w.restart(true)
	w.untilPhase("scale", v1alpha1.OperationPhaseDone)
	w.ticks(5)
	if got := w.batch.Count("scancel"); got != 1 {
		t.Errorf("%d scancel calls, want one: the Allocation is released once", got)
	}
	w.wantCause(w.pendingAlloc, w.getOp("scale"))
	if n := w.log.n[w.pendingAlloc]; n != 1 {
		t.Errorf("P's Allocation was deleted %d times, want once", n)
	}
	if got := w.md(hoMD).Spec.Instances; got != 1 {
		t.Errorf("spec.instances is %d, want 1", got)
	}
}
