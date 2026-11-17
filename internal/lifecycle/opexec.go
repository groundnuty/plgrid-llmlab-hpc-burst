package lifecycle

// Executing an Operation (HBEP-10, Approval and execution and The report; HBEP-7, Reconcile loop, the Operations
// step; #589 plan, 7.4 and 7.5). Three things happen here, in this order:
//
//   - the stale check, ONCE, and BEFORE the commit (readyOperation). The plan is recomputed from what this pass read.
//     Approvals are counted under today's policies, while the plan's ID covers each applied policy's generation, so
//     an edit to a policy since the plan is caught only here. A different ID, or a plan that can no longer be made,
//     turns the Operation Stale: terminal, nothing runs, its approvals no longer count;
//   - the commit (commitOperation): one status write, carrying the resourceVersion that was read, records every
//     target of the frozen plan and sets phase Executing. A conflict fails the pass and the next one reads again;
//     two controller copies that overlap commit once, and the other finds Executing and acts on the same list;
//   - the execution (executing), in the commit's own pass and in every pass after it, a restart included: the spec
//     write first, then each committed target by its effect, then the report. It never plans again and never reads
//     the controller's own selection: what it acts on is the committed list, and nothing else.
//
// The spec goes first so that a crash after a drain cannot leave a stale higher count for capacity() to provision
// back. The write is retried by every pass until it has landed, which status records (v1alpha1.ConditionSpecWritten), since
// the ModelDeployment's own generation cannot say it: a person's edit moves that too.

import (
	"errors"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// isExecuting reports whether the Operation is committed and not finished: Executing, or PartlyFailed with a failure
// that is still retried. A PartlyFailed Operation whose every failure is final has finishedAt set (phaseOf), and holds
// nothing: not the one-at-a-time slot, not step 7's selection, not an Allocation.
func isExecuting(op *v1alpha1.Operation) bool {
	switch op.Status.Phase {
	case v1alpha1.OperationPhaseExecuting:
		return true
	case v1alpha1.OperationPhasePartlyFailed:
		return op.Status.FinishedAt == nil
	}
	return false
}

// anotherExecuting returns an Operation of the pass, other than op, that is Executing or PartlyFailed: one at a
// time on a ModelDeployment, so none other is committed while it is (HBEP-10, Approval and execution).
func (p *pass) anotherExecuting(op *v1alpha1.Operation) *v1alpha1.Operation {
	for _, o := range p.ops {
		if o != op && isExecuting(o) {
			return o
		}
	}
	return nil
}

// readyOperation handles a confirmed Operation whose every target that needs approval has it, or that needs none:
// it waits while another Operation executes, else it is checked for staleness, once, and committed.
func (p *pass) readyOperation(op *v1alpha1.Operation, env *opEnv, plan *v1alpha1.OperationPlan, standings []targetStanding) error {
	if p.commitUnknown {
		return nil // an earlier commit of this pass may or may not have landed: nothing else commits until a pass reads it
	}
	if other := p.anotherExecuting(op); other != nil {
		return p.setOperationStatus(op, func(s *v1alpha1.OperationStatus) {
			s.ObservedGeneration = op.Generation
			if len(standings) > 0 {
				s.Phase = v1alpha1.OperationPhaseAwaitingApproval
			}
			p.setCondition(s, op, v1alpha1.ConditionExecutable, metav1.ConditionFalse, v1alpha1.ReasonAnotherOperationExecuting,
				fmt.Sprintf("Operation %s is executing on this ModelDeployment; this one is checked for staleness when it has finished", other.Name))
			p.setPolicyCondition(s, op, nil)
		})
	}
	// The stale check, before anything is written. A plan that cannot be made now (the deployment is gone, a Scale is
	// no longer below the count a Scale before it lowered, a Release's Allocation hosts an Instance) is as stale as a
	// plan with another ID.
	req, err := env.request(op, metav1.NewTime(p.now))
	if err != nil {
		return err
	}
	fresh, refusal := p.planOperation(req)
	switch {
	case refusal != "":
		return p.staleOperation(op, "the plan can no longer be made: "+refusal)
	case fresh.ID != plan.ID:
		return p.staleOperation(op, fmt.Sprintf("the state changed since the plan was approved: plan %s is now plan %s", plan.ID, fresh.ID))
	}
	return p.commitOperation(op, plan)
}

// staleOperation writes Stale, terminal, and raises PlanStale (HBEP-8, Reasons in alpha).
func (p *pass) staleOperation(op *v1alpha1.Operation, why string) error {
	if p.mem.once("plan-stale/" + string(op.UID)) {
		p.r.Recorder.Event(op, nil, eventDegraded, v1alpha1.ReasonPlanStale,
			fmt.Sprintf("Operation %s/%s turned stale before it was committed, so nothing ran and its approvals no longer count: %s", op.Namespace, op.Name, why))
	}
	return p.setOperationStatus(op, func(s *v1alpha1.OperationStatus) {
		s.Phase, s.ObservedGeneration = v1alpha1.OperationPhaseStale, op.Generation
		p.setCondition(s, op, v1alpha1.ConditionExecutable, metav1.ConditionFalse, v1alpha1.ReasonPlanStale, why)
		apimeta.RemoveStatusCondition(&s.Conditions, v1alpha1.ConditionNoApprovalPolicy)
		if s.FinishedAt == nil {
			t := metav1.NewTime(p.now)
			s.FinishedAt = &t
		}
	})
}

// commitOperation records every target of the frozen plan in one status write and sets phase Executing, then
// executes in this same pass.
func (p *pass) commitOperation(op *v1alpha1.Operation, plan *v1alpha1.OperationPlan) error {
	committed := make([]v1alpha1.CommittedTarget, 0, len(plan.Targets))
	for _, t := range plan.Targets {
		committed = append(committed, v1alpha1.CommittedTarget{Kind: t.Kind, Name: t.Name, UID: t.UID, Generation: t.Generation, Effect: t.Effect})
	}
	if err := p.setOperationStatus(op, func(s *v1alpha1.OperationStatus) {
		s.ObservedGeneration, s.Phase = op.Generation, v1alpha1.OperationPhaseExecuting
		s.Committed, s.Targets = committed, nil
		p.setCondition(s, op, v1alpha1.ConditionExecutable, metav1.ConditionTrue, v1alpha1.ReasonExecuting,
			fmt.Sprintf("committed to %d targets: the controller acts on this list and on nothing else", len(committed)))
		apimeta.RemoveStatusCondition(&s.Conditions, v1alpha1.ConditionNoApprovalPolicy)
	}); err != nil {
		p.afterCommitError(op)
		return err
	}
	return p.executing(op)
}

// afterCommitError settles what a commit write that returned an error means. The write may have been applied all the
// same (a lost reply, a timeout after the API server stored it), and then the Operation IS the executing one: no other
// Operation may commit in this pass, so the Operation is read again, through the uncached reader, and the pass's own copy
// becomes what the API server holds. When that read fails too, nothing is known, and no other Operation commits in this
// pass (commitUnknown); the next pass reads again. The pass fails with the commit's own error either way.
func (p *pass) afterCommitError(op *v1alpha1.Operation) {
	var cur v1alpha1.Operation
	if err := p.r.Reader.Get(p.ctx, client.ObjectKeyFromObject(op), &cur); err != nil {
		p.commitUnknown = true
		return
	}
	if isExecuting(&cur) {
		*op = cur
	}
}

// executing is one pass over a committed Operation: the spec write, then every committed target, then the report.
// A target's failure does not stop the others; the errors are returned joined, so the key is retried with backoff, and
// the report says which target failed (opreport.go).
func (p *pass) executing(op *v1alpha1.Operation) error {
	if finishedOperation(op) {
		return nil // every failure was final and the rest done: nothing is carried out again
	}
	var errs []error
	failed := map[types.UID]error{}
	// What was already being deleted when this pass began: only such an Allocation can be reported as failing to go (a
	// Synced=False it carried before its delete says nothing about the delete).
	deleting := p.deletingBeforeThePass(op)
	specOK := true
	if err := p.applySpec(op); err != nil {
		if errors.Is(err, errSpecChanged) {
			return p.failSpecChanged(op)
		}
		specOK = false
		// Nothing is acted on, created or replaced until the spec has landed and is recorded. An End's deployment target is
		// the one that failed; a Scale has no deployment target, and stays Executing until the write lands.
		p.opsHold = true
		errs = append(errs, err)
		for _, t := range op.Status.Committed {
			if t.Kind == v1alpha1.TargetKindModelDeployment {
				failed[t.UID] = err
			}
		}
	} else if !p.specEffectHolds(op, p.md) {
		// On EVERY pass, before any act: the effect the Operation committed to must still hold. A person who raises the count
		// while a Scale drains, or moves the end time of an End, would otherwise have the Operation release what the controller
		// then provisions back, which is churn at the cost of a queue position. When it no longer holds nothing more is acted
		// on: every target not yet done fails for good, and the Operation is finished (HBEP-10, The report, as amended by #665
		// and #666).
		return p.failSpecChanged(op)
	} else {
		var actErrs []error
		actErrs, failed = p.actOnCommitted(op)
		errs = append(errs, actErrs...)
	}
	if err := p.report(op, failed, deleting, specOK); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// finishedOperation reports whether the Operation has finished, whichever way: it has a finish time.
func finishedOperation(op *v1alpha1.Operation) bool { return op.Status.FinishedAt != nil }

// errSpecChanged is what applySpec returns when the spec changed after the plan and the effect the Operation committed to no
// longer holds.
var errSpecChanged = errors.New("the deployment's spec changed after the plan, and the committed effect no longer holds")

// specEffectHolds is the ONE check of whether the spec effect an Operation committed to holds now, by value, for the
// deployment as it is read (HBEP-10, The report, as amended by #665 and #666): a Scale, spec.instances is at most N; an End, spec.endTime is set and not after now, or the
// deployment is deleted (or is being, or is another one of the name now); a Release has no spec effect. Applied where the
// generation moved before the first act (applySpec) and on every pass before any act (executing): a person who undoes
// the effect while the Operation is carried out stops it.
func (p *pass) specEffectHolds(op *v1alpha1.Operation, md *v1alpha1.ModelDeployment) bool {
	switch op.Spec.Action {
	case v1alpha1.OperationActionScale:
		return md == nil || op.Spec.Scale == nil || md.Spec.Instances <= op.Spec.Scale.Instances
	case v1alpha1.OperationActionEnd:
		if op.Status.Plan != nil && (md == nil || md.UID != op.Status.Plan.ModelDeployment.UID || !md.DeletionTimestamp.IsZero()) {
			return true // gone, another deployment of the name, or being deleted: the End's job is done, or there is nothing to end
		}
		if op.Spec.End != nil && op.Spec.End.Delete {
			return false // a live deployment that is not being deleted
		}
		return md.Spec.EndTime != nil && !md.Spec.EndTime.After(p.now)
	}
	return true
}

// failSpecChanged ends the Operation when the effect it committed to no longer holds: nothing more is acted on, every
// target not yet done fails for good with SpecChangedSinceCommit, a target already done stays done, and the Operation is
// PartlyFailed and finished (phaseOf). A spec target (SetEndTime, DeleteDeployment) is not done when its effect no longer
// holds, whatever it did.
func (p *pass) failSpecChanged(op *v1alpha1.Operation) error {
	prior := map[types.UID]v1alpha1.TargetOutcome{}
	for _, o := range op.Status.Targets {
		prior[o.UID] = o
	}
	var out []v1alpha1.TargetOutcome
	for _, t := range op.Status.Committed {
		o := v1alpha1.TargetOutcome{Kind: t.Kind, Name: t.Name, UID: t.UID, State: v1alpha1.TargetStateNotStarted, LastTransitionTime: metav1.NewTime(p.now)}
		done := false
		if t.Kind != v1alpha1.TargetKindModelDeployment {
			var err error
			if done, err = p.targetDone(t, true); err != nil {
				return err
			}
		}
		if prev, ok := prior[t.UID]; ok && prev.State == v1alpha1.TargetStateFailed && failureIsFinal(prev.Reason) {
			o.State, o.Reason, o.Message = prev.State, prev.Reason, prev.Message // a final failure already recorded stays
		} else if done {
			o.State = v1alpha1.TargetStateDone
		} else {
			o.State, o.Reason, o.Message = v1alpha1.TargetStateFailed, reasonSpecChangedSinceCommit,
				"the deployment's spec changed after the Operation was committed, so that the effect it was approved for no longer holds: nothing more was acted on"
		}
		if prev, ok := prior[t.UID]; ok && prev.State == o.State {
			o.LastTransitionTime = prev.LastTransitionTime
		}
		out = append(out, o)
	}
	phase, finished := phaseOf(out)
	p.raisePartlyFailed(op, phase, out) // an Operation a person's edit stopped is PartlyFailed like any other: the event is raised here too
	return p.setOperationStatus(op, func(s *v1alpha1.OperationStatus) {
		s.Targets, s.Phase = out, phase
		p.setCondition(s, op, v1alpha1.ConditionExecutable, metav1.ConditionFalse, v1alpha1.ReasonDone,
			"the deployment's spec changed after the commit, so the committed effect no longer holds: nothing more was acted on, and the Operation is finished")
		if finished && s.FinishedAt == nil {
			t := metav1.NewTime(p.now)
			s.FinishedAt = &t
		}
	})
}

// lastReadyScaleTarget reports whether the committed Instance target is one that a Scale committed as CancelPending or
// EndStarting, that is Ready now, and would be the LAST ready one: no other Instance is Ready, not being deleted, not stale
// and not itself a committed target, which is the planner's own staysReady. Draining it would be a DrainLastReady nobody
// approved. An End drains every Ready Instance as the End itself, and so does a Scale to zero (the shutdown), so neither is
// asked: the rule applies to a Scale to a count above zero alone (HBEP-10, The report, as amended by #665, #666 and #668).
func (p *pass) lastReadyScaleTarget(op *v1alpha1.Operation, t v1alpha1.CommittedTarget, in *v1alpha1.Instance) bool {
	if op.Spec.Action != v1alpha1.OperationActionScale || in == nil || in.Status.Phase != v1alpha1.InstanceReady {
		return false
	}
	if op.Spec.Scale != nil && op.Spec.Scale.Instances == 0 {
		return false // a Scale to zero is the shutdown, as an End is: every Instance is drained, and the approval took it to zero
	}
	if t.Effect != v1alpha1.PlanEffectCancelPending && t.Effect != v1alpha1.PlanEffectEndStarting {
		return false
	}
	taken := map[types.UID]bool{}
	for _, c := range op.Status.Committed {
		if c.Kind == v1alpha1.TargetKindInstance {
			taken[c.UID] = true
		}
	}
	for _, o := range p.insts {
		if o.UID != in.UID && o.Status.Phase == v1alpha1.InstanceReady && o.DeletionTimestamp.IsZero() && !p.stale[o.UID] && !taken[o.UID] {
			return false
		}
	}
	return true
}

// applySpec writes the spec change an Operation commits to, once, before any target is acted on: Scale lowers
// spec.instances to N, End sets spec.endTime to now or deletes the ModelDeployment with a UID precondition. The write
// is an update that carries the resourceVersion read, under the lifecycle controller's field manager, and p.md
// becomes what was written, so the rest of the pass, step 7 included, works from it: after an End the pass shuts down.
// A deployment that is gone, being deleted, or another one of the same name now is never written.
//
// The write is made only while the deployment's generation is still the plan's. The stale check at the commit made them
// equal, and a spec change moves the generation, so a generation past the plan's means the spec changed since: by this
// Operation's own earlier write, whose record in status did not land (a kill between the two), or by a person or an agent.
// Nothing is written over it. The two need not be told apart: what decides is whether the committed effect holds now
// (specEffectHolds). If it does, the record is made and the Operation goes on; if it does not, errSpecChanged says nothing
// is acted on. The record that the spec is dealt with (v1alpha1.ConditionSpecWritten) is its own status write, made right
// after the spec write, so that the window in which a kill leaves it unwritten is that one write.
func (p *pass) applySpec(op *v1alpha1.Operation) error {
	if apimeta.IsStatusConditionTrue(op.Status.Conditions, v1alpha1.ConditionSpecWritten) {
		return nil
	}
	plan, md := op.Status.Plan, p.md
	switch {
	case plan == nil || md == nil || md.UID != plan.ModelDeployment.UID || !md.DeletionTimestamp.IsZero():
		return p.recordSpecWritten(op, "the deployment is gone, being deleted, or another one now: there is no spec to write")
	case md.Generation != plan.ModelDeployment.Generation:
		// The spec changed since the plan. Whether by this Operation's own write whose record did not land, or by someone else,
		// what matters is whether the effect the Operation committed to holds now (specEffectHolds): if it does the spec is
		// not written again and the Operation goes on; if it does not, nothing is acted on.
		if !p.specEffectHolds(op, md) {
			return errSpecChanged
		}
		return p.recordSpecWritten(op, fmt.Sprintf("the deployment's spec changed after the plan (generation %d, the plan's %d) and already holds the change: it is not written again", md.Generation, plan.ModelDeployment.Generation))
	}
	write, why := (*v1alpha1.ModelDeployment)(nil), ""
	switch op.Spec.Action {
	case v1alpha1.OperationActionScale:
		if op.Spec.Scale == nil {
			return errors.New("a Scale names its count in spec.scale")
		}
		if n := op.Spec.Scale.Instances; md.Spec.Instances > n {
			write, why = md.DeepCopy(), fmt.Sprintf("set spec.instances to %d", n)
			write.Spec.Instances = n
		}
	case v1alpha1.OperationActionEnd:
		if op.Spec.End != nil && op.Spec.End.Delete {
			if err := p.deleteDeployment(md); err != nil {
				return err
			}
			return p.recordSpecWritten(op, "the deployment's delete was asked for")
		}
		if md.Spec.EndTime == nil || md.Spec.EndTime.After(p.now) {
			t := metav1.NewTime(p.now)
			write, why = md.DeepCopy(), "set spec.endTime to now"
			write.Spec.EndTime = &t
		}
	}
	if write != nil {
		if err := p.writeSpec(write, why); err != nil {
			return err
		}
	}
	return p.recordSpecWritten(op, "the spec change this Operation commits to has landed, or there was none to make")
}

// recordSpecWritten writes the condition that the spec is dealt with, in its own status write.
func (p *pass) recordSpecWritten(op *v1alpha1.Operation, why string) error {
	return p.setOperationStatus(op, func(s *v1alpha1.OperationStatus) {
		p.setCondition(s, op, v1alpha1.ConditionSpecWritten, metav1.ConditionTrue, "Written", why)
	})
}

// writeSpec updates the ModelDeployment with the resourceVersion read and makes the pass work from the stored object.
func (p *pass) writeSpec(next *v1alpha1.ModelDeployment, what string) error {
	if err := p.r.Client.Update(p.ctx, next, owner); err != nil {
		return fmt.Errorf("%s of ModelDeployment %s: %w", what, p.key, err)
	}
	p.md = next
	p.shutdown = p.shutdown || p.shutsDown()
	return nil
}

// deleteDeployment deletes the ModelDeployment with a UID precondition, then reads it again: with a finalizer it stays,
// being deleted, and without one it is gone. Either way the pass shuts down.
func (p *pass) deleteDeployment(md *v1alpha1.ModelDeployment) error {
	uid := md.UID
	if err := p.r.Client.Delete(p.ctx, md, client.Preconditions{UID: &uid}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete ModelDeployment %s: %w", p.key, err)
	}
	var cur v1alpha1.ModelDeployment
	switch err := p.r.Reader.Get(p.ctx, p.key, &cur); {
	case apierrors.IsNotFound(err) || (err == nil && cur.UID != uid):
		p.md = nil
	case err != nil:
		return fmt.Errorf("get ModelDeployment %s after its delete: %w", p.key, err)
	default:
		p.md = &cur
	}
	p.shutdown = true
	return nil
}

// actOnCommitted acts on every committed target by its effect: the Instances first, then the Allocations, so an
// Allocation is released after what ran on it has ended. A target already done is skipped by its own act. It returns
// the errors, and the failed targets by UID with their error, which the report says.
func (p *pass) actOnCommitted(op *v1alpha1.Operation) ([]error, map[types.UID]error) {
	var errs []error
	failed := map[types.UID]error{}
	for _, kind := range []v1alpha1.TargetKind{v1alpha1.TargetKindInstance, v1alpha1.TargetKindAllocation} {
		for _, t := range op.Status.Committed {
			if t.Kind != kind {
				continue
			}
			if err := p.actOnTarget(op, t); err != nil {
				errs = append(errs, fmt.Errorf("%s %s (%s): %w", t.Kind, t.Name, t.Effect, err))
				failed[t.UID] = err
			}
		}
	}
	return errs, failed
}

// releasingEffect reports whether an Allocation target's effect releases the Allocation.
func releasingEffect(e v1alpha1.PlanEffect) bool {
	switch e {
	case v1alpha1.PlanEffectCancelPending, v1alpha1.PlanEffectReleaseKept, v1alpha1.PlanEffectReleaseStarted:
		return true
	}
	return false
}

// actOnTarget carries out one committed target, idempotently: an Instance that has ended or is gone, and an
// Allocation that is gone, are done; a drain already begun is not begun again. The Instance's act is the one the
// controller's own scale-down makes (startDrain, end), and an Allocation is released by deleteAllocation, with the
// cause deleteAllocation reads from the committed list (operationCause).
func (p *pass) actOnTarget(op *v1alpha1.Operation, t v1alpha1.CommittedTarget) error {
	if recordedFinal(op, t.UID) {
		return nil // failed for good, and recorded: no later pass acts on it (HBEP-10, The report, as amended by #665, #666 and #668)
	}
	switch t.Kind {
	case v1alpha1.TargetKindInstance:
		in := p.instanceByUID(t.UID)
		if in == nil || ended(in) {
			return nil
		}
		alloc := p.allocs[in.Spec.AllocationRef.Name]
		// By the Instance's phase NOW, not as committed: a target committed as pending or still starting may have turned
		// Ready since, and a serving Instance is drained (HBEP-7, Handover, Draining), never ended: its requests are in flight.
		switch in.Status.Phase {
		case v1alpha1.InstanceReady:
			if p.lastReadyScaleTarget(op, t, in) {
				return nil // it would be the last ready Instance: a drain nobody approved, so it is a final failure (finalFailure)
			}
			return p.startDrain(in)
		case v1alpha1.InstanceDraining:
			return nil // step 6 carries the drain on (and, in a shutdown, step 6 began it in this very pass)
		}
		// Neither serving nor draining: it ends, whatever it was committed as, with its Allocation left to the
		// Allocation's own target (CancelPending or a release), or to retention.
		return p.end(in, alloc, v1alpha1.ReasonDrained, opMessage(op, "ended"), false)
	case v1alpha1.TargetKindAllocation:
		if !releasingEffect(t.Effect) {
			return nil // Keep: no act
		}
		a, err := p.committedAllocation(t)
		if err != nil || a == nil {
			return err
		}
		if startedSinceCommit(t, a) {
			// The approval was to cancel a QUEUED job, which costs a queue position; its job has started since, and releasing a
			// running one is another price (ReleaseStarted). It is left to retention, and reported Failed for good.
			return nil
		}
		if len(p.liveOn(a.Name)) > 0 {
			// An Instance still runs on it (a drain that has not finished): its release waits until it has ended,
			// as retention's would.
			return nil
		}
		return p.deleteAllocation(a, v1alpha1.ReleaseCauseOperation(op.Namespace, op.Name, op.UID))
	}
	return nil
}

// recordedFinal reports whether the Operation's report already records the target as failed for good (failureIsFinal): such a
// target is never acted on by any later pass, and no later release of it carries the Operation's cause (HBEP-10, The
// report, as amended by #665, #666 and #668). A final outcome is recorded only by a pass that acts (finalFailure), so
// what is recorded was true at an act.
func recordedFinal(op *v1alpha1.Operation, uid types.UID) bool {
	for _, o := range op.Status.Targets {
		if o.UID == uid && o.State == v1alpha1.TargetStateFailed && failureIsFinal(o.Reason) {
			return true
		}
	}
	return false
}

// startedSinceCommit reports whether a committed CancelPending target's job has started since the commit: it was queued
// when the plan was made, since the plan's ID covers the effect and the stale check passed, so a start time now is a start
// after it.
func startedSinceCommit(t v1alpha1.CommittedTarget, a *v1alpha1.Allocation) bool {
	return t.Effect == v1alpha1.PlanEffectCancelPending && a != nil && a.Status.AtProvider.StartTime != nil
}

// opMessage names the Operation in a stop's message.
func opMessage(op *v1alpha1.Operation, what string) string {
	return fmt.Sprintf("%s by Operation %s/%s", what, op.Namespace, op.Name)
}

// instanceByUID returns the pass's Instance with the UID, or nil: gone, or never this deployment's.
func (p *pass) instanceByUID(uid types.UID) *v1alpha1.Instance {
	for _, in := range p.insts {
		if in.UID == uid {
			return in
		}
	}
	return nil
}

// committedAllocation returns the Allocation a committed target names, by its UID, or nil when it is gone: not
// found, or found with another UID. The pass's own list is used where it holds it; any other is read by name, since
// a Release names its Allocations by spec.modelDeploymentRef.name, which a long deployment name's shortened label
// need not reach, and an Allocation the pass did not list is not gone for that.
func (p *pass) committedAllocation(t v1alpha1.CommittedTarget) (*v1alpha1.Allocation, error) {
	if a, ok := p.opAllocs[t.UID]; ok {
		return a, nil
	}
	var found *v1alpha1.Allocation
	for _, a := range p.allocs {
		if a.UID == t.UID {
			found = a
		}
	}
	if found == nil {
		var a v1alpha1.Allocation
		err := p.r.Reader.Get(p.ctx, client.ObjectKey{Namespace: p.key.Namespace, Name: t.Name}, &a)
		switch {
		case apierrors.IsNotFound(err):
		case err != nil:
			return nil, fmt.Errorf("get Allocation %s: %w", t.Name, err)
		case a.UID == t.UID:
			found = &a
		}
	}
	if p.opAllocs == nil {
		p.opAllocs = map[types.UID]*v1alpha1.Allocation{}
	}
	p.opAllocs[t.UID] = found
	return found, nil
}

// operationCause is the release cause of an Allocation that an Executing Operation committed to release: the
// Operation's, whichever path of the pass deletes it (the Operation's own act, step 6's shutdown, retention).
// Only a releasing effect counts. A Keep stays retention's: when retention releases it later, its cause is its own. So does a
// target recorded as failed for good (recordedFinal): the Operation did not release it, and a later release of it is
// retention's or the shutdown's, never the Operation's.
func (p *pass) operationCause(a *v1alpha1.Allocation) (string, bool) {
	for _, op := range p.ops {
		if !isExecuting(op) {
			continue
		}
		for _, t := range op.Status.Committed {
			if t.Kind == v1alpha1.TargetKindAllocation && t.UID == a.UID && releasingEffect(t.Effect) && !startedSinceCommit(t, a) && !recordedFinal(op, t.UID) {
				return v1alpha1.ReleaseCauseOperation(op.Namespace, op.Name, op.UID), true
			}
		}
	}
	return "", false
}

// committedForRelease reports whether an Executing Operation committed the Allocation for release: nothing is placed on it
// (warmAllocation).
func (p *pass) committedForRelease(a *v1alpha1.Allocation) bool {
	_, ok := p.operationCause(a)
	return ok
}

// committedInstance reports whether an Executing Operation committed the Instance as a target: step 8 makes it no
// replacement, since the Operation takes it (a drain that failed to start is retried, not replaced).
func (p *pass) committedInstance(in *v1alpha1.Instance) bool {
	for _, op := range p.ops {
		if !isExecuting(op) {
			continue
		}
		for _, t := range op.Status.Committed {
			if t.Kind == v1alpha1.TargetKindInstance && t.UID == in.UID {
				return true
			}
		}
	}
	return false
}

// holdsSelection reports whether an Executing Operation of any action holds back step 7's own scale-down selection (HBEP-7,
// Reconcile loop, step 7: while an Operation is Executing, the scale-down takes its committed targets instead of choosing
// its own). Provisioning of missing primaries is not held.
func (p *pass) holdsSelection() bool {
	for _, op := range p.ops {
		if isExecuting(op) {
			return true
		}
	}
	return false
}

// executingScale returns the Executing Scale Operation of the pass, or nil. While it is, step 7 counts min(spec.instances,
// N) as the desired number of primaries and never makes its own selection of what to take (capacity).
func (p *pass) executingScale() *v1alpha1.Operation {
	for _, op := range p.ops {
		if isExecuting(op) && op.Spec.Action == v1alpha1.OperationActionScale && op.Spec.Scale != nil {
			return op
		}
	}
	return nil
}
