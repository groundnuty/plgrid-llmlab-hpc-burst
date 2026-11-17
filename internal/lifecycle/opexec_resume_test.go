package lifecycle

// What a committed Operation does when the state moved between its commit and its act (#589, PR 5, the review's fixes): a
// target is acted on by its phase NOW, a started job is not cancelled under the approval to cancel a queued one, the spec is
// recorded as written right after it is, and an Executing Operation of any action holds back step 7's own selection, keeps
// a warm Allocation from being placed on what it releases, and gets its committed Instances no replacement. Each test
// moves the state with the controller's own spec write failing or its process killed, so the window between the commit and
// the act is open for as many rounds as the test needs, and every check waits on observable state.

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// failSpecWrites makes every update of the ModelDeployment's spec fail with an error that says "injected", until the returned
// function is called.
func (w *execWorld) failSpecWrites() (stop func()) {
	failing := true
	w.fc.set(func(method string, obj client.Object) error {
		if _, ok := obj.(*v1alpha1.ModelDeployment); ok && method == "Update" && failing {
			return errors.New("injected: the write of the spec fails")
		}
		return nil
	}, nil)
	return func() { failing = false }
}

// A Scale commits to cancel a pending Allocation and its Instance; the spec write fails, so nothing is acted on; the job
// starts and the Instance turns Ready. When the write lands, the Instance is DRAINED, not ended, its started Allocation is NOT
// cancelled (the approval was for a queued job, which is priced differently), and the target is reported Failed for good with
// JobStartedSinceCommit: the Operation, all else done, is PartlyFailed and finished.
func TestACommittedCancelPendingThatStartedAndTurnedReadyIsDrainedAndItsJobIsNotCancelled(t *testing.T) {
	w := newExecWorld(t, time.Hour) // R drains from 1 h 50 min into its 4 h lease: it is still ready when P turns ready
	pendingUID, pendingAllocUID := w.mustInstance(w.pending).UID, w.allocOrNil(w.pendingAlloc).UID
	stop := w.failSpecWrites()
	w.plan("scale", scaleTo(1))
	w.confirm("scale")
	w.untilPhase("scale", v1alpha1.OperationPhaseAwaitingApproval)
	w.approve("scale", "person", userPerson)
	w.stepsUntil(time.Minute, 10*time.Second, "the commit", func() bool { return len(w.getOp("scale").Status.Committed) == 2 })
	if got := w.md(hoMD).Spec.Instances; got != 2 {
		t.Fatalf("spec.instances is %d, want 2: the write fails", got)
	}

	// The queue wait ends and P's engine comes up, all while the spec write keeps failing and no act is made.
	w.stepsUntil(90*time.Minute, 2*time.Minute, "P ready", func() bool { return w.instance(w.pending).Status.Phase == v1alpha1.InstanceReady })
	if a := w.allocOrNil(w.pendingAlloc); a == nil || a.Status.AtProvider.StartTime == nil || !a.DeletionTimestamp.IsZero() {
		t.Fatalf("P's Allocation is %+v: its job should have started, and nothing should have released it", a)
	}
	stop()
	w.clock.Advance(10 * time.Second)
	if err := w.stepKeepingErrors(); err != nil {
		t.Fatalf("the pass after the spec write can land: %v", err)
	}

	// P is drained, not ended; its started Allocation is untouched.
	in := w.instance(w.pending)
	if in.UID != pendingUID || !isDraining(in) || ended(in) || in.Status.EndReason != "" {
		t.Errorf("P's Instance is %q with end reason %q (draining %v): a serving Instance is drained, never ended", in.Status.Phase, in.Status.EndReason, isDraining(in))
	}
	a, ok := w.allocation(w.pendingAlloc)
	if !ok || a.UID != pendingAllocUID || !a.DeletionTimestamp.IsZero() || a.Annotations[v1alpha1.AnnotationReleaseCause] != "" {
		t.Errorf("P's started Allocation is %+v (found %v): the approval to cancel a queued job does not release it", a, ok)
	}
	if _, deleted := w.log.deletedWith(w.pendingAlloc); deleted {
		t.Error("P's Allocation was deleted under the cancel approval")
	}
	if got := w.batch.Count("scancel"); got != 0 {
		t.Errorf("the fake Slurm saw %d scancel calls, want none: the job that started is not cancelled", got)
	}
	if got := w.md(hoMD).Spec.Instances; got != 1 {
		t.Errorf("spec.instances is %d, want 1", got)
	}

	// The drain ends; every target is Done or failed for good: PartlyFailed, finished, and the failure is the Allocation's.
	// Until then it is PartlyFailed already (the final failure is known), and not finished.
	w.until(30*time.Minute, 10*time.Second, "the Operation finished", func() bool { return w.getOp("scale").Status.FinishedAt != nil })
	op := w.getOp("scale")
	if op.Status.Phase != v1alpha1.OperationPhasePartlyFailed {
		t.Fatalf("the Operation is %q, want PartlyFailed", op.Status.Phase)
	}
	if op.Status.FinishedAt == nil {
		t.Error("the Operation is PartlyFailed with no finish time: every failure is final, so it is finished")
	}
	if o, ok := outcomeOf(op, w.pendingAlloc); !ok || o.State != v1alpha1.TargetStateFailed || o.Reason != reasonJobStartedSinceCommit ||
		o.UID != pendingAllocUID || !strings.Contains(o.Message, "started after the Operation was committed") {
		t.Errorf("P's Allocation outcome is %+v (%v), want Failed with %s", o, ok, reasonJobStartedSinceCommit)
	}
	if o, ok := outcomeOf(op, w.pending); !ok || o.State != v1alpha1.TargetStateDone || o.UID != pendingUID {
		t.Errorf("P's Instance outcome is %+v (%v), want Done: it was drained and has ended", o, ok)
	}
	if isExecuting(op) {
		t.Error("a finished PartlyFailed Operation still holds the deployment's slot")
	}
	// OperationPartlyFailed is raised for a final failure as for a retried one, regarding the Operation, naming the reason.
	if evs := w.partlyFailedEvents("scale"); len(evs) != 1 || !strings.Contains(evs[0].Message, reasonJobStartedSinceCommit) || evs[0].Type != "failing" {
		t.Errorf("the OperationPartlyFailed events are %+v, want one, failing, naming %s", evs, reasonJobStartedSinceCommit)
	}
	// It frees the deployment: the next Operation is planned and not held back by it, and the Allocation stays with retention.
	if a, ok := w.allocation(w.pendingAlloc); !ok || !a.DeletionTimestamp.IsZero() {
		t.Errorf("P's Allocation after the Operation: %+v (found %v), want it kept, hosting nothing", a, ok)
	}
	w.plan("next", endSpec(false))
	w.confirm("next")
	w.untilPhase("next", v1alpha1.OperationPhaseAwaitingApproval)
	w.approve("next", "person", userPerson)
	w.untilPhase("next", v1alpha1.OperationPhaseExecuting) // it commits: the finished Operation holds no slot
}

// A Scale commits to end an Instance that is still starting on a started job (EndStarting); the spec write fails; the
// Instance turns Ready. When the write lands it is DRAINED: a serving Instance is never ended.
func TestACommittedEndStartingInstanceThatTurnedReadyIsDrained(t *testing.T) {
	w, ready, starting := newStartingWorld(t)
	stop := w.failSpecWrites()
	planned := w.plan("scale", scaleTo(1))
	if got := planTargets(planned.Status.Plan)["Instance "+starting]; got != v1alpha1.PlanEffectEndStarting {
		t.Fatalf("the plan takes S as %q, want EndStarting: %v", got, planTargets(planned.Status.Plan))
	}
	w.confirm("scale") // nothing in it needs an approval: it commits at once, and its spec write fails
	w.stepsUntil(time.Minute, 10*time.Second, "the commit", func() bool { return len(w.getOp("scale").Status.Committed) > 0 })
	w.stepsUntil(10*time.Minute, 10*time.Second, "S ready", func() bool { return w.instance(starting).Status.Phase == v1alpha1.InstanceReady })
	if w.instance(ready).Status.Phase != v1alpha1.InstanceReady {
		t.Fatal("R is not ready")
	}
	stop()
	w.clock.Advance(10 * time.Second)
	if err := w.stepKeepingErrors(); err != nil {
		t.Fatalf("the pass after the spec write can land: %v", err)
	}
	if in := w.instance(starting); !isDraining(in) || ended(in) || in.Status.EndReason != "" {
		t.Errorf("S is %q with end reason %q: a committed EndStarting Instance that turned Ready is drained, not ended", in.Status.Phase, in.Status.EndReason)
	}
	w.untilPhase("scale", v1alpha1.OperationPhaseDone)
	if got := w.batch.Count("scancel"); got != 0 {
		t.Errorf("%d scancel calls: S's Allocation is kept, not cancelled", got)
	}
}

// An End whose spec write landed and whose process was killed before anything else. On the resumed pass step 6 sees the
// deployment shutting down and drains the Ready Instance itself, in the same pass as the Operations step, which must leave
// it: it is draining already, and is not ended. The pending one is cancelled by step 6, its Allocation with the Operation's
// cause.
func TestAnEndResumedAfterItsSpecWriteLeavesTheDrainingInstanceToStepSix(t *testing.T) {
	w := newExecWorld(t, 10*time.Hour)
	w.plan("end", endSpec(false))
	w.confirm("end")
	w.untilPhase("end", v1alpha1.OperationPhaseAwaitingApproval)
	dead := false
	w.fc.set(func(string, client.Object) error {
		if dead {
			return errKilled
		}
		return nil
	}, func(method string, obj client.Object) {
		if _, ok := obj.(*v1alpha1.ModelDeployment); ok && method == "Update" {
			dead = true // the spec write landed, and the process dies with it
		}
	})
	w.approve("end", "person", userPerson)
	w.clock.Advance(10 * time.Second)
	if err := w.stepKeepingErrors(); !errors.Is(err, errKilled) {
		t.Fatalf("the pass: %v, want the kill", err)
	}
	if md := w.md(hoMD); md.Spec.EndTime == nil {
		t.Fatal("the End's spec write did not land before the kill")
	}
	if in := w.instance(w.ready); in.Status.Phase != v1alpha1.InstanceReady {
		t.Fatalf("R is %q after the kill, want Ready: nothing was acted on", in.Status.Phase)
	}

	w.restart(true)
	w.clock.Advance(10 * time.Second)
	if err := w.stepKeepingErrors(); err != nil {
		t.Fatalf("the resumed pass: %v", err)
	}
	if in := w.instance(w.ready); !isDraining(in) || ended(in) || in.Status.EndReason != "" {
		t.Errorf("R is %q with end reason %q after the resumed pass, want draining: step 6 drains it, and the Operations step must not end it", in.Status.Phase, in.Status.EndReason)
	}
	w.untilPhase("end", v1alpha1.OperationPhaseDone)
	w.wantCause(w.pendingAlloc, w.getOp("end"))
	w.wantCause(w.readyAlloc, w.getOp("end"))
}

// The condition is in status right after the spec write, before any act: the process is killed at the first act of the
// Operation, and the record is already written. (With the record left to the report after the acts, a kill there would
// leave it unset, and a person's edit after it would be written over by the resumed pass.)
func TestTheSpecRecordIsWrittenBeforeAnyAct(t *testing.T) {
	w := newExecWorld(t, 10*time.Hour)
	w.plan("scale", scaleTo(1))
	w.confirm("scale")
	w.untilPhase("scale", v1alpha1.OperationPhaseAwaitingApproval)
	dead := false
	w.fc.set(func(method string, obj client.Object) error {
		switch obj.(type) {
		case *v1alpha1.Instance, *v1alpha1.Allocation:
			dead = true // the first act of the Operation: the process dies with it
		}
		if dead {
			return errKilled
		}
		return nil
	}, nil)
	w.approve("scale", "person", userPerson)
	w.clock.Advance(10 * time.Second)
	if err := w.stepKeepingErrors(); !errors.Is(err, errKilled) {
		t.Fatalf("the pass: %v, want the kill at the first act", err)
	}
	op := w.getOp("scale")
	if !isTrue(op.Status.Conditions, v1alpha1.ConditionSpecWritten) {
		t.Fatal("SpecWritten is not recorded when the process dies at the first act: the record is made right after the spec write")
	}
	if got := w.md(hoMD).Spec.Instances; got != 1 {
		t.Fatalf("spec.instances is %d, want 1", got)
	}
	if in := w.instance(w.pending); ended(in) {
		t.Fatal("P's Instance ended: the kill was to come at the first act")
	}
	// A person raises the count; the controller comes back, leaves it, and acts on nothing (opexec_spec_test.go).
	w.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 2 })
	w.restart(true)
	w.clock.Advance(10 * time.Second)
	if err := w.stepKeepingErrors(); err != nil {
		t.Fatalf("the resumed pass: %v", err)
	}
	if got := w.md(hoMD).Spec.Instances; got != 2 {
		t.Errorf("spec.instances is %d, want the person's 2", got)
	}
	w.wantNoTargetActedOn("scale")
}

// Two ready Operations on one deployment, and the first one's commit write is STORED yet returns an error (the reply is
// lost). The pass reads the Operation again, finds it Executing, and the second does not commit in this pass: one at a time.
func TestACommitWriteThatAppliedButReturnedAnErrorStopsTheNextOperationCommitting(t *testing.T) {
	w := newExecWorld(t, 10*time.Hour)
	w.plan("op-1", scaleTo(1))
	w.plan("op-2", scaleTo(1))
	w.confirm("op-1")
	w.confirm("op-2")
	w.untilPhase("op-1", v1alpha1.OperationPhaseAwaitingApproval)
	w.untilPhase("op-2", v1alpha1.OperationPhaseAwaitingApproval)
	lost := true
	w.fc.setFailAfter(func(method string, obj client.Object) error {
		if op, ok := obj.(*v1alpha1.Operation); ok && method == "Status.Update" && op.Name == "op-1" && op.Status.Phase == v1alpha1.OperationPhaseExecuting && lost {
			lost = false
			return errors.New("injected: the reply to the commit never came")
		}
		return nil
	})
	w.approve("op-1", "person", userPerson)
	w.approve("op-2", "person", userPerson)
	w.clock.Advance(10 * time.Second)
	if err := w.stepKeepingErrors(); err == nil || !strings.Contains(err.Error(), "injected") {
		t.Fatalf("the pass: %v, want the commit's lost reply", err)
	}
	one, two := w.getOp("op-1"), w.getOp("op-2")
	if one.Status.Phase != v1alpha1.OperationPhaseExecuting || len(one.Status.Committed) == 0 {
		t.Fatalf("op-1 is %q with %d committed targets: its commit was stored", one.Status.Phase, len(one.Status.Committed))
	}
	if two.Status.Phase == v1alpha1.OperationPhaseExecuting || len(two.Status.Committed) != 0 {
		t.Errorf("op-2 is %q with %d committed targets: it committed in the pass whose first commit's reply was lost", two.Status.Phase, len(two.Status.Committed))
	}
	// The next passes: op-1 goes on from its committed list, and op-2 is checked after it, and turns Stale.
	w.stepsUntil(time.Minute, 10*time.Second, "op-1 Done", func() bool { return w.getOp("op-1").Status.Phase == v1alpha1.OperationPhaseDone })
	w.stepsUntil(time.Minute, 10*time.Second, "op-2 Stale", func() bool { return w.getOp("op-2").Status.Phase == v1alpha1.OperationPhaseStale })
}

// The same, with the second read failing too: whether the commit landed is not known, so nothing else commits in the pass.
func TestACommitWriteThatReturnedAnErrorWhoseRereadFailsCommitsNothingElse(t *testing.T) {
	w := newExecWorld(t, 10*time.Hour)
	w.plan("op-1", scaleTo(1))
	w.plan("op-2", scaleTo(1))
	w.confirm("op-1")
	w.confirm("op-2")
	w.untilPhase("op-1", v1alpha1.OperationPhaseAwaitingApproval)
	w.untilPhase("op-2", v1alpha1.OperationPhaseAwaitingApproval)
	armed := false
	w.fc.setFailAfter(func(method string, obj client.Object) error {
		if op, ok := obj.(*v1alpha1.Operation); ok && method == "Status.Update" && op.Name == "op-1" && op.Status.Phase == v1alpha1.OperationPhaseExecuting {
			armed = true
			return errors.New("injected: the reply to the commit never came")
		}
		return nil
	})
	w.r.Reader = opGetFailer{Reader: w.c, fail: &armed}
	w.approve("op-1", "person", userPerson)
	w.approve("op-2", "person", userPerson)
	w.clock.Advance(10 * time.Second)
	if err := w.stepKeepingErrors(); err == nil || !strings.Contains(err.Error(), "injected") {
		t.Fatalf("the pass: %v, want the commit's lost reply", err)
	}
	if two := w.getOp("op-2"); two.Status.Phase == v1alpha1.OperationPhaseExecuting || len(two.Status.Committed) != 0 {
		t.Errorf("op-2 is %q with %d committed targets: nothing else commits in a pass that cannot tell whether a commit landed", two.Status.Phase, len(two.Status.Committed))
	}
}

// opGetFailer fails the read of an Operation while fail is set.
type opGetFailer struct {
	client.Reader
	fail *bool
}

func (f opGetFailer) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*v1alpha1.Operation); ok && *f.fail {
		return errors.New("injected: the read of the Operation fails")
	}
	return f.Reader.Get(ctx, key, obj, opts...)
}
