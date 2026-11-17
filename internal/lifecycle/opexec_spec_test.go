package lifecycle

// The spec effect an Operation committed to, checked by VALUE before it releases anything (HBEP-10, The report, as amended by
// #665 and #666; #589, PR 5, the delta review of 077a9408): a Scale's spec.instances is at most N; an End's spec.endTime is
// set and not after now; a delete leaves the deployment gone or deleting. On every pass, before any act, an Operation whose effect
// no longer holds acts on NOTHING more: every target not yet done fails for good with SpecChangedSinceCommit and the Operation
// is PartlyFailed and finished. A person who raised the count while a Scale drains, or moved an End's end time, would
// otherwise have the Operation release what the controller then provisions back. Beside it, a committed Instance of a Scale
// that is Ready and would be the last ready one is not drained, and the Allocation it keeps from emptying fails with it.
// Each test races a person's edit with the Operation by a Conflict on the controller's own write (conflictOnce), a kill, or a
// failing write, so every check waits on observable state.

import (
	"errors"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// wantNoTargetActedOn fails unless every target of the Operation is Failed with SpecChangedSinceCommit and the Operation is
// PartlyFailed and finished, and the fake Slurm saw no cancel.
func (w *execWorld) wantNoTargetActedOn(op string) *v1alpha1.Operation {
	w.t.Helper()
	got := w.getOp(op)
	if got.Status.Phase != v1alpha1.OperationPhasePartlyFailed || got.Status.FinishedAt == nil {
		w.t.Errorf("Operation %s is %q with finishedAt %v, want PartlyFailed and finished", op, got.Status.Phase, got.Status.FinishedAt)
	}
	if len(got.Status.Targets) != len(got.Status.Committed) || len(got.Status.Committed) == 0 {
		w.t.Errorf("%d outcomes for %d committed targets", len(got.Status.Targets), len(got.Status.Committed))
	}
	for _, o := range got.Status.Targets {
		if o.State != v1alpha1.TargetStateFailed || o.Reason != reasonSpecChangedSinceCommit || !strings.Contains(o.Message, "spec changed after the Operation was committed") {
			w.t.Errorf("target %s %s is %s (%s: %s), want Failed with %s", o.Kind, o.Name, o.State, o.Reason, o.Message, reasonSpecChangedSinceCommit)
		}
	}
	if isExecuting(got) {
		w.t.Errorf("Operation %s still holds the deployment's slot", op)
	}
	if n := w.batch.Count("scancel"); n != 0 {
		w.t.Errorf("the fake Slurm saw %d scancel calls: nothing is released", n)
	}
	return got
}

// wantNothingTouched fails if any Instance of the world is draining or ended, or any Allocation is being deleted.
func (w *execWorld) wantNothingTouched() {
	w.t.Helper()
	for _, in := range w.instances(hoMD) {
		if isDraining(&in) || ended(&in) {
			w.t.Errorf("Instance %s is %q: nothing was acted on", in.Name, in.Status.Phase)
		}
	}
	for _, a := range w.allocations(hoMD) {
		if !a.DeletionTimestamp.IsZero() {
			w.t.Errorf("Allocation %s is being deleted: nothing was acted on", a.Name)
		}
	}
}

// canCommitNext shows that the finished Operation holds no slot: another End is planned, confirmed, approved and commits.
func (w *execWorld) canCommitNext(name string) {
	w.t.Helper()
	w.plan(name, endSpec(false))
	w.confirm(name)
	w.untilPhase(name, v1alpha1.OperationPhaseAwaitingApproval)
	w.approve(name, "person", userPerson)
	w.untilPhase(name, v1alpha1.OperationPhaseExecuting)
}

// A person's edit to an UNRELATED spec field races the End's write: the End's update conflicts, and on the next pass the
// generation is past the plan's while the end time is still not set. The effect does not hold, so no target is released, and
// the Operation ends PartlyFailed and finished. (Without the check the pass recorded the spec as written, released the
// targets, and never ended the deployment: step 7 then provisioned them back, and the End reported Done.)
func TestAPersonsEditToAnUnrelatedFieldRacesTheEndsWriteAndNothingIsReleased(t *testing.T) {
	w := newExecWorld(t, 10*time.Hour)
	w.plan("end", endSpec(false))
	w.confirm("end")
	w.untilPhase("end", v1alpha1.OperationPhaseAwaitingApproval)
	w.conflictOnce("Update", func() {
		w.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Workload.TargetSessions = 9 })
	})
	w.approve("end", "person", userPerson)
	w.clock.Advance(10 * time.Second)
	if err := w.stepKeepingErrors(); err == nil || !strings.Contains(err.Error(), "has been modified") {
		t.Fatalf("the commit pass: %v, want the End's write to conflict", err)
	}
	if got := w.md(hoMD); got.Spec.EndTime != nil {
		t.Fatalf("the end time is %v: the write was not made", got.Spec.EndTime)
	}
	w.clock.Advance(10 * time.Second)
	if err := w.stepKeepingErrors(); err != nil {
		t.Fatalf("the next pass: %v", err)
	}
	w.wantNoTargetActedOn("end")
	w.wantNothingTouched()
	if md := w.md(hoMD); md.Spec.EndTime != nil || md.Spec.Workload.TargetSessions != 9 {
		t.Errorf("the deployment's spec is %+v: the person's edit stays and the End did not apply", md.Spec)
	}
	if in := w.instance(w.ready); in.Status.Phase != v1alpha1.InstanceReady {
		t.Errorf("R is %q, want Ready", in.Status.Phase)
	}
	w.canCommitNext("next")
}

// A person lowers spec.instances BELOW N while a Scale is committed: the effect holds (the count is at most N), the spec is
// not written again, and the Operation acts: R1, its target, drains.
func TestAPersonLowersTheCountBelowNDuringAScaleAndItActs(t *testing.T) {
	w, insts, _ := newThreeWorld(t)
	w.plan("scale", scaleTo(2))
	w.conflictOnce("Update", func() {
		w.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 1 })
	})
	w.confirm("scale") // nothing needs an approval: it commits at once, and its spec write conflicts
	w.clock.Advance(10 * time.Second)
	if err := w.stepKeepingErrors(); err == nil || !strings.Contains(err.Error(), "has been modified") {
		t.Fatalf("the commit pass: %v, want the Scale's write to conflict", err)
	}
	w.clock.Advance(10 * time.Second)
	if err := w.stepKeepingErrors(); err != nil {
		t.Fatalf("the next pass: %v", err)
	}
	if got := w.md(hoMD).Spec.Instances; got != 1 {
		t.Errorf("spec.instances is %d, want the person's 1: it is not written again", got)
	}
	if c := condOf(w.getOp("scale"), v1alpha1.ConditionSpecWritten); c == nil || c.Status != "True" || !strings.Contains(c.Message, "already holds the change") {
		t.Errorf("SpecWritten %+v, want True, saying the spec already holds the change", c)
	}
	if in := w.instance(insts[0]); !isDraining(in) {
		t.Errorf("R1 is %q: the effect holds, so the Operation drains its target", in.Status.Phase)
	}
	w.untilPhase("scale", v1alpha1.OperationPhaseDone)
}

// End with delete races a person's edit: the deployment is live and its generation moved, so the delete is not made, no
// target is released, and the Operation FINISHES instead of staying Executing for ever with the slot held.
func TestAnEndWithDeleteRacesAPersonsEditAndFinishesWithNothingReleased(t *testing.T) {
	w := newExecWorld(t, 10*time.Hour)
	w.plan("end", endSpec(true))
	w.confirm("end")
	w.untilPhase("end", v1alpha1.OperationPhaseAwaitingApproval)
	armed := true
	w.fc.set(func(method string, obj client.Object) error {
		if _, ok := obj.(*v1alpha1.ModelDeployment); ok && method == "Delete" && armed {
			armed = false
			w.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Workload.TargetSessions = 9 })
			return errors.New("injected: the delete does not reach the API server")
		}
		return nil
	}, nil)
	w.approve("end", "person", userPerson)
	w.clock.Advance(10 * time.Second)
	if err := w.stepKeepingErrors(); err == nil || !strings.Contains(err.Error(), "injected") {
		t.Fatalf("the commit pass: %v, want the delete not made", err)
	}
	w.clock.Advance(10 * time.Second)
	if err := w.stepKeepingErrors(); err != nil {
		t.Fatalf("the next pass: %v", err)
	}
	w.wantNoTargetActedOn("end")
	w.wantNothingTouched()
	if md := w.md(hoMD); !md.DeletionTimestamp.IsZero() {
		t.Error("the deployment is being deleted")
	}
	w.canCommitNext("next")
}

// A Scale's spec write lands, the process dies before its record, and a person raises the count: the effect no longer holds,
// so the resumed pass acts on nothing, and the count stays the person's. (HBEP-10's amendment #666 flips what an earlier
// version of this test pinned, a release of the targets that step 7 then provisioned back.)
func TestAScaleWhoseWriteLandedAndWhoseCountAPersonRaisedActsOnNothing(t *testing.T) {
	w := newExecWorld(t, 10*time.Hour)
	w.fc.bumpGeneration = true
	w.plan("scale", scaleTo(1))
	w.confirm("scale")
	w.untilPhase("scale", v1alpha1.OperationPhaseAwaitingApproval)
	dead := false
	w.fc.set(func(string, client.Object) error {
		if dead {
			return errKilled
		}
		return nil
	}, func(method string, obj client.Object) {
		if _, ok := obj.(*v1alpha1.ModelDeployment); ok && method == "Update" {
			dead = true
		}
	})
	w.approve("scale", "person", userPerson)
	w.clock.Advance(10 * time.Second)
	if err := w.stepKeepingErrors(); !errors.Is(err, errKilled) {
		t.Fatalf("the pass: %v, want the kill", err)
	}
	if got := w.md(hoMD).Spec.Instances; got != 1 {
		t.Fatalf("spec.instances is %d after the kill, want 1: the write landed", got)
	}
	w.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 3 })
	w.restart(true)
	w.clock.Advance(10 * time.Second)
	if err := w.stepKeepingErrors(); err != nil {
		t.Fatalf("the resumed pass: %v", err)
	}
	w.wantNoTargetActedOn("scale")
	w.wantNothingTouched()
	if got := w.md(hoMD).Spec.Instances; got != 3 {
		t.Errorf("spec.instances is %d, want the person's 3", got)
	}
	w.canCommitNext("next")
}

// A person raises the count right after a Scale's commit, so the Scale's update conflicts: nothing is acted on, the fake
// Slurm shows no cancel, no drain starts, every target is final, and the count stays the person's.
func TestAPersonRaisingTheCountAfterTheCommitOfAScaleReleasesNothing(t *testing.T) {
	w := newExecWorld(t, 10*time.Hour)
	w.plan("scale", scaleTo(1))
	w.confirm("scale")
	w.untilPhase("scale", v1alpha1.OperationPhaseAwaitingApproval)
	w.conflictOnce("Update", func() {
		w.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 3 })
	})
	w.approve("scale", "person", userPerson)
	w.clock.Advance(10 * time.Second)
	if err := w.stepKeepingErrors(); err == nil || !strings.Contains(err.Error(), "has been modified") {
		t.Fatalf("the commit pass: %v, want the Scale's write to conflict", err)
	}
	w.clock.Advance(10 * time.Second)
	if err := w.stepKeepingErrors(); err != nil {
		t.Fatalf("the next pass: %v", err)
	}
	w.wantNoTargetActedOn("scale")
	w.wantNothingTouched()
	if got := w.md(hoMD).Spec.Instances; got != 3 {
		t.Errorf("spec.instances is %d, want the person's 3", got)
	}
	w.canCommitNext("next")
}

// An unrelated edit in the window also fails the Operation, with the count still above N: the safe side, documented. The
// alternative, writing the count again, could override a person's deliberate edit (#666).
func TestAnUnrelatedEditInTheWindowFailsTheScaleOnTheSafeSide(t *testing.T) {
	w := newExecWorld(t, 10*time.Hour)
	w.plan("scale", scaleTo(1))
	w.confirm("scale")
	w.untilPhase("scale", v1alpha1.OperationPhaseAwaitingApproval)
	w.conflictOnce("Update", func() {
		w.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Workload.TargetSessions = 9 })
	})
	w.approve("scale", "person", userPerson)
	w.clock.Advance(10 * time.Second)
	if err := w.stepKeepingErrors(); err == nil || !strings.Contains(err.Error(), "has been modified") {
		t.Fatalf("the commit pass: %v, want the Scale's write to conflict", err)
	}
	w.clock.Advance(10 * time.Second)
	if err := w.stepKeepingErrors(); err != nil {
		t.Fatalf("the next pass: %v", err)
	}
	w.wantNoTargetActedOn("scale")
	if got := w.md(hoMD).Spec.Instances; got != 2 {
		t.Errorf("spec.instances is %d: the Operation does not write the count again", got)
	}
}

// A Scale whose write landed but whose record did not, with nobody else touching the spec: the generation moved by the
// Operation's own write, the count is N, so the effect holds and the Operation goes on.
func TestAScaleWhoseWriteLandedButWasNotRecordedActsOnResume(t *testing.T) {
	w := newExecWorld(t, 10*time.Hour)
	w.fc.bumpGeneration = true
	w.plan("scale", scaleTo(1))
	w.confirm("scale")
	w.untilPhase("scale", v1alpha1.OperationPhaseAwaitingApproval)
	dead := false
	w.fc.set(func(string, client.Object) error {
		if dead {
			return errKilled
		}
		return nil
	}, func(method string, obj client.Object) {
		if _, ok := obj.(*v1alpha1.ModelDeployment); ok && method == "Update" {
			dead = true
		}
	})
	w.approve("scale", "person", userPerson)
	w.clock.Advance(10 * time.Second)
	if err := w.stepKeepingErrors(); !errors.Is(err, errKilled) {
		t.Fatalf("the pass: %v, want the kill", err)
	}
	if md := w.md(hoMD); md.Spec.Instances != 1 || md.Generation < 2 {
		t.Fatalf("spec.instances %d, generation %d: the write landed and moved it", md.Spec.Instances, md.Generation)
	}
	w.restart(true)
	w.untilPhase("scale", v1alpha1.OperationPhaseDone)
	if c := condOf(w.getOp("scale"), v1alpha1.ConditionSpecWritten); c == nil || !strings.Contains(c.Message, "already holds the change") {
		t.Errorf("SpecWritten %+v, want it to say the spec already holds the change", c)
	}
	w.wantCause(w.pendingAlloc, w.getOp("scale"))
}

// An End whose end time a person clears, or moves later, after the commit: the effect does not hold, so nothing is acted on.
func TestAnEndWhoseEndTimeAPersonMovesLaterOrClearsActsOnNothing(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(m *v1alpha1.ModelDeployment, now time.Time)
	}{
		{"moved an hour later", func(m *v1alpha1.ModelDeployment, now time.Time) {
			later := metaTime(now.Add(time.Hour))
			m.Spec.EndTime = &later
		}},
		{"cleared", func(m *v1alpha1.ModelDeployment, _ time.Time) { m.Spec.EndTime = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newExecWorld(t, 10*time.Hour)
			w.fc.bumpGeneration = true
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
					dead = true // the End's write landed and the process died before its record
				}
			})
			w.approve("end", "person", userPerson)
			w.clock.Advance(10 * time.Second)
			if err := w.stepKeepingErrors(); !errors.Is(err, errKilled) {
				t.Fatalf("the pass: %v, want the kill", err)
			}
			if w.md(hoMD).Spec.EndTime == nil {
				t.Fatal("the End's write did not land")
			}
			now := w.clock.Now()
			w.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { tc.edit(m, now) })
			w.restart(true)
			w.clock.Advance(10 * time.Second)
			if err := w.stepKeepingErrors(); err != nil {
				t.Fatalf("the resumed pass: %v", err)
			}
			w.wantNoTargetActedOn("end")
			w.wantNothingTouched()
		})
	}
}

// An End whose end time a person sets EARLIER than now after the commit: the effect holds, and the Operation acts.
func TestAnEndWhoseEndTimeAPersonSetsEarlierActs(t *testing.T) {
	w := newExecWorld(t, 10*time.Hour)
	w.plan("end", endSpec(false))
	w.confirm("end")
	w.untilPhase("end", v1alpha1.OperationPhaseAwaitingApproval)
	w.conflictOnce("Update", func() {
		earlier := metaTime(w.clock.Now().Add(-time.Minute))
		w.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.EndTime = &earlier })
	})
	w.approve("end", "person", userPerson)
	w.clock.Advance(10 * time.Second)
	if err := w.stepKeepingErrors(); err == nil || !strings.Contains(err.Error(), "has been modified") {
		t.Fatalf("the commit pass: %v, want the End's write to conflict", err)
	}
	w.clock.Advance(10 * time.Second)
	if err := w.stepKeepingErrors(); err != nil {
		t.Fatalf("the next pass: %v", err)
	}
	if in := w.instance(w.ready); !isDraining(in) {
		t.Errorf("R is %q: the person's earlier end time holds the effect, so the Operation acts", in.Status.Phase)
	}
	w.untilPhase("end", v1alpha1.OperationPhaseDone)
}

// The same check runs on EVERY pass, not only before the first act: a Scale of three to one commits two drains, the first
// starts and ends, the second's write keeps failing; then a person raises the count. On the next pass nothing more is
// released: the second drain does not start, the first stays Done, and the rest are final.
func TestAPersonRaisingTheCountMidScaleStopsTheRestOfItAndTheFirstStaysDone(t *testing.T) {
	w, insts, _ := newThreeWorld(t)
	r1, r2 := insts[0], insts[1]
	planned := w.plan("scale", scaleTo(1))
	if planTargets(planned.Status.Plan)["Instance "+r1] != v1alpha1.PlanEffectDrainInstance || planTargets(planned.Status.Plan)["Instance "+r2] != v1alpha1.PlanEffectDrainInstance {
		t.Fatalf("the plan is %v, want R1 and R2 drained", planTargets(planned.Status.Plan))
	}
	failing := true
	w.fc.set(func(method string, obj client.Object) error {
		if in, ok := obj.(*v1alpha1.Instance); ok && in.Name == r2 && failing {
			return errors.New("injected: the write of R2 fails")
		}
		return nil
	}, nil)
	w.confirm("scale")
	w.stepsUntil(10*time.Minute, 10*time.Second, "R1 drained and ended, R2's drain still failing", func() bool {
		return w.endedOrGone(r1) && len(w.getOp("scale").Status.Committed) > 0
	})
	if in := w.instance(r2); isDraining(in) || in.Status.Phase != v1alpha1.InstanceReady {
		t.Fatalf("R2 is %q: its drain write fails", in.Status.Phase)
	}
	scancels := w.batch.Count("scancel")
	failing = false
	w.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 3 })
	w.clock.Advance(10 * time.Second)
	if err := w.stepKeepingErrors(); err != nil {
		t.Fatalf("the pass after the raise: %v", err)
	}
	op := w.getOp("scale")
	if op.Status.Phase != v1alpha1.OperationPhasePartlyFailed || op.Status.FinishedAt == nil {
		t.Fatalf("the Operation is %q with finishedAt %v, want PartlyFailed and finished", op.Status.Phase, op.Status.FinishedAt)
	}
	if o, ok := outcomeOf(op, r1); !ok || o.State != v1alpha1.TargetStateDone {
		t.Errorf("R1's outcome is %+v (%v), want Done: it stays Done", o, ok)
	}
	if o, ok := outcomeOf(op, r2); !ok || o.State != v1alpha1.TargetStateFailed || o.Reason != reasonSpecChangedSinceCommit {
		t.Errorf("R2's outcome is %+v (%v), want Failed with %s", o, ok, reasonSpecChangedSinceCommit)
	}
	if in := w.instance(r2); isDraining(in) || in.Status.Phase != v1alpha1.InstanceReady {
		t.Errorf("R2 is %q: no further target is released once the effect no longer holds", in.Status.Phase)
	}
	if got := w.batch.Count("scancel"); got != scancels {
		t.Errorf("%d scancel calls, %d before the raise", got, scancels)
	}
	if got := w.md(hoMD).Spec.Instances; got != 3 {
		t.Errorf("spec.instances is %d, want the person's 3", got)
	}
}

// The control: a person lowering the count FURTHER while a Scale is carried out does not stop it: the effect still holds.
func TestAPersonLoweringTheCountFurtherMidScaleDoesNotStopIt(t *testing.T) {
	w, insts, _ := newThreeWorld(t)
	r1 := insts[0]
	w.plan("scale", scaleTo(2))
	failing := true
	w.fc.set(func(method string, obj client.Object) error {
		if in, ok := obj.(*v1alpha1.Instance); ok && in.Name == r1 && failing && isDraining(in) {
			return errors.New("injected: the write that drains R1 fails")
		}
		return nil
	}, nil)
	w.confirm("scale")
	w.stepsUntil(time.Minute, 10*time.Second, "the commit and its failed drain", func() bool { return len(w.getOp("scale").Status.Committed) > 0 })
	w.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 1 })
	failing = false
	w.clock.Advance(10 * time.Second)
	if err := w.stepKeepingErrors(); err != nil {
		t.Fatalf("the pass after the lowering: %v", err)
	}
	if in := w.instance(r1); !isDraining(in) {
		t.Errorf("R1 is %q: lowering the count further leaves the effect holding, so the drain goes on", in.Status.Phase)
	}
	w.untilPhase("scale", v1alpha1.OperationPhaseDone)
}

// A Scale commits to cancel a queued Instance, X, newer than R, and both pend. The spec write fails; X's job starts and X turns
// Ready while R still pends: X is the LAST ready one. The Operation does not drain it, since that is a DrainLastReady nobody
// approved: X's target fails for good with LastReadySinceCommit. Its Allocation's job has started, which is its own final
// failure, and the reason it keeps. The Operation is PartlyFailed and finished, and X keeps serving.
func TestACommittedInstanceThatTurnedReadyAndIsTheLastReadyOneIsNotDrained(t *testing.T) {
	w, r, x := newTwoPendingWorld(t, 10*time.Hour, 30*time.Minute)
	stop := w.failSpecWrites()
	planned := w.plan("scale", scaleTo(1))
	if got := planTargets(planned.Status.Plan)["Instance "+x]; got != v1alpha1.PlanEffectCancelPending {
		t.Fatalf("the plan takes X as %q: %v", got, planTargets(planned.Status.Plan))
	}
	xAlloc := w.mustInstance(x).Spec.AllocationRef.Name
	w.confirm("scale")
	w.untilPhase("scale", v1alpha1.OperationPhaseAwaitingApproval)
	w.approve("scale", "person", userPerson)
	w.stepsUntil(time.Minute, 10*time.Second, "the commit", func() bool { return len(w.getOp("scale").Status.Committed) == 2 })
	w.clock.Advance(30 * time.Minute)
	w.stepsUntil(30*time.Minute, 10*time.Second, "X ready", func() bool { return w.instance(x).Status.Phase == v1alpha1.InstanceReady })
	if w.instance(r).Status.Phase == v1alpha1.InstanceReady {
		t.Fatal("R is ready: X would not be the last ready one")
	}
	stop()
	w.clock.Advance(10 * time.Second)
	if err := w.stepKeepingErrors(); err != nil {
		t.Fatalf("the pass after the spec write can land: %v", err)
	}
	if in := w.instance(x); isDraining(in) || ended(in) || in.Status.Phase != v1alpha1.InstanceReady {
		t.Errorf("X is %q: the last ready Instance is not drained under an approval that did not cover it", in.Status.Phase)
	}
	if got := w.batch.Count("scancel"); got != 0 {
		t.Errorf("%d scancel calls", got)
	}
	op := w.getOp("scale")
	if o, ok := outcomeOf(op, x); !ok || o.State != v1alpha1.TargetStateFailed || o.Reason != reasonLastReadySinceCommit {
		t.Errorf("X's Instance outcome is %+v (%v), want Failed with %s", o, ok, reasonLastReadySinceCommit)
	}
	if o, ok := outcomeOf(op, xAlloc); !ok || o.State != v1alpha1.TargetStateFailed || o.Reason != reasonJobStartedSinceCommit {
		t.Errorf("X's Allocation outcome is %+v (%v), want Failed, keeping its own reason %s", o, ok, reasonJobStartedSinceCommit)
	}
	if op.Status.Phase != v1alpha1.OperationPhasePartlyFailed || op.Status.FinishedAt == nil {
		t.Errorf("the Operation is %q with finishedAt %v, want PartlyFailed and finished", op.Status.Phase, op.Status.FinishedAt)
	}
	if a, ok := w.allocation(xAlloc); !ok || !a.DeletionTimestamp.IsZero() {
		t.Errorf("X's Allocation is %+v (%v): it is left alone", a, ok)
	}
}

// The knock-on: the Allocation a Scale committed to release once its Instance has been ended, ReleaseStarted, can never empty
// when that Instance fails for good and stays live. It fails for good with the same reason, or the Operation would wait for
// ever. R's engine never comes up and X's takes three minutes, so X turns Ready as the last ready one.
func TestTheAllocationOfAnInstanceThatFailedForGoodAsTheLastReadyOneFailsWithIt(t *testing.T) {
	w, r, x := newTwoStartingWorld(t)
	stop := w.failSpecWrites()
	planned := w.plan("scale", scaleTo(1))
	xAlloc := w.mustInstance(x).Spec.AllocationRef.Name
	if got := planTargets(planned.Status.Plan)["Instance "+x]; got != v1alpha1.PlanEffectEndStarting {
		t.Fatalf("the plan takes X as %q: %v", got, planTargets(planned.Status.Plan))
	}
	if got := planTargets(planned.Status.Plan)["Allocation "+xAlloc]; got != v1alpha1.PlanEffectReleaseStarted {
		t.Fatalf("the plan takes X's Allocation as %q, want ReleaseStarted: %v", got, planTargets(planned.Status.Plan))
	}
	w.confirm("scale")
	w.untilPhase("scale", v1alpha1.OperationPhaseAwaitingApproval)
	w.approve("scale", "person", userPerson)
	w.stepsUntil(time.Minute, 10*time.Second, "the commit", func() bool { return len(w.getOp("scale").Status.Committed) == 2 })
	w.stepsUntil(15*time.Minute, 10*time.Second, "X ready", func() bool { return w.instance(x).Status.Phase == v1alpha1.InstanceReady })
	stop()
	w.clock.Advance(10 * time.Second)
	if err := w.stepKeepingErrors(); err != nil {
		t.Fatalf("the pass after the spec write can land: %v", err)
	}
	op := w.getOp("scale")
	if o, ok := outcomeOf(op, x); !ok || o.State != v1alpha1.TargetStateFailed || o.Reason != reasonLastReadySinceCommit {
		t.Errorf("X's Instance outcome is %+v (%v), want Failed with %s", o, ok, reasonLastReadySinceCommit)
	}
	if o, ok := outcomeOf(op, xAlloc); !ok || o.State != v1alpha1.TargetStateFailed || o.Reason != reasonLastReadySinceCommit {
		t.Errorf("X's Allocation outcome is %+v (%v), want Failed with the same reason, %s: it can never empty", o, ok, reasonLastReadySinceCommit)
	}
	if op.Status.Phase != v1alpha1.OperationPhasePartlyFailed || op.Status.FinishedAt == nil {
		t.Errorf("the Operation is %q with finishedAt %v, want PartlyFailed and finished: it would wait for ever otherwise", op.Status.Phase, op.Status.FinishedAt)
	}
	if in := w.instance(x); isDraining(in) || in.Status.Phase != v1alpha1.InstanceReady {
		t.Errorf("X is %q, want Ready: not drained", in.Status.Phase)
	}
	_ = r
}

// metaTime is a metav1.Time.
func metaTime(t time.Time) metav1.Time { return metav1.NewTime(t) }
