package lifecycle

// Operations in the reconcile pass (HBEP-10, Operations; HBEP-7, Reconcile loop, the Operations step;
// #589 plan, 7.1 to 7.3). This is the PLANNING half: the pass plans every Operation that names its
// ModelDeployment, with no side effect but the Operation's own status, counts the Approvals of a confirmed
// one, and sets its phase. It executes nothing: a confirmed Operation whose approvals are all in waits
// with Executable=False, reason ExecutionNotBuilt, until the execution lands (PR 5).
//
// The step sits between step 6 (the Instances' phases) and step 7 (capacity), so the planner sees the
// phases step 6 just advanced. It runs whether or not the ModelDeployment exists: an Operation that names
// a deployment that is gone is Refused. Everything is read through the uncached reader (r.Reader), as the
// rest of the pass is.
//
// The phases it writes:
//   - a new Operation: Planned with its plan, or Refused with a condition;
//   - Planned (unconfirmed): planned again on every pass, and written only when the plan's ID, or an input
//     of a cost, changed. Costs are snapshots at the plan's observedAt, so time passing writes nothing;
//   - confirmed: the plan is frozen and never rewritten. A confirm whose plan ID is not the one this
//     controller wrote and still holds in status is Refused: the backstop of HBEP-10's invariant, that a
//     confirm exists only by an update that follows a written plan. Otherwise the Approvals are counted:
//     AwaitingApproval while a target that needs one lacks it, and Executable=False/ExecutionNotBuilt when
//     none does.
//
// Done, Stale, Refused, Executing and PartlyFailed are not this step's to move.

import (
	"errors"
	"fmt"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// maxConditionMessage bounds a condition's message, in bytes.
const maxConditionMessage = 1024

// errUnwrittenPlan is the Refused message of a confirm the backstop does not accept.
const errUnwrittenPlan = "confirmed before a plan was written, or with an ID this controller never wrote"

// opEnv holds what the Operations of one pass read besides the pass's own state, each list read once and
// only when an Operation needs it.
type opEnv struct {
	p         *pass
	policies  []v1alpha1.ApprovalPolicy
	approvals []v1alpha1.Approval
	allocs    []*v1alpha1.Allocation
	read      struct{ policies, approvals, allocs bool }
}

func (e *opEnv) policyList() ([]v1alpha1.ApprovalPolicy, error) {
	if !e.read.policies {
		var l v1alpha1.ApprovalPolicyList
		if err := e.p.r.Reader.List(e.p.ctx, &l); err != nil {
			return nil, fmt.Errorf("list ApprovalPolicies: %w", err)
		}
		e.policies, e.read.policies = l.Items, true
	}
	return e.policies, nil
}

func (e *opEnv) approvalList() ([]v1alpha1.Approval, error) {
	if !e.read.approvals {
		var l v1alpha1.ApprovalList
		if err := e.p.r.Reader.List(e.p.ctx, &l, client.InNamespace(e.p.key.Namespace)); err != nil {
			return nil, fmt.Errorf("list Approvals: %w", err)
		}
		e.approvals, e.read.approvals = l.Items, true
	}
	return e.approvals, nil
}

// namespaceAllocations are a Release's candidates: every Allocation of the namespace, which the planner
// filters by spec.modelDeploymentRef.name, never by the model-deployment label.
func (e *opEnv) namespaceAllocations() ([]*v1alpha1.Allocation, error) {
	if !e.read.allocs {
		var l v1alpha1.AllocationList
		if err := e.p.r.Reader.List(e.p.ctx, &l, client.InNamespace(e.p.key.Namespace)); err != nil {
			return nil, fmt.Errorf("list Allocations: %w", err)
		}
		for i := range l.Items {
			e.allocs = append(e.allocs, &l.Items[i])
		}
		e.read.allocs = true
	}
	return e.allocs, nil
}

// request builds a planning request at time at.
func (e *opEnv) request(op *v1alpha1.Operation, at metav1.Time) (planRequest, error) {
	pol, err := e.policyList()
	if err != nil {
		return planRequest{}, err
	}
	req := planRequest{op: op, at: at.Time, policies: pol}
	if op.Spec.Action == v1alpha1.OperationActionRelease {
		if req.allocs, err = e.namespaceAllocations(); err != nil {
			return planRequest{}, err
		}
	}
	return req, nil
}

// operations is the pass's Operations step. It lists the namespace's Operations through the uncached
// reader and keeps those that name this ModelDeployment, in p.ops: Reconcile keeps the key alive while
// there is one. A failure of one Operation does not stop the others; they are returned joined.
func (p *pass) operations() error {
	var list v1alpha1.OperationList
	if err := p.r.Reader.List(p.ctx, &list, client.InNamespace(p.key.Namespace)); err != nil {
		return fmt.Errorf("list Operations: %w", err)
	}
	p.ops = nil
	for i := range list.Items {
		if op := &list.Items[i]; op.Spec.ModelDeploymentRef.Name == p.key.Name {
			p.ops = append(p.ops, op)
		}
	}
	env := &opEnv{p: p}
	var errs []error
	for _, op := range p.ops {
		if err := p.operation(op, env); err != nil {
			errs = append(errs, fmt.Errorf("Operation %s: %w", op.Name, err))
		}
	}
	return errors.Join(errs...)
}

// operation advances one Operation.
func (p *pass) operation(op *v1alpha1.Operation, env *opEnv) error {
	switch op.Status.Phase {
	case v1alpha1.OperationPhaseRefused, v1alpha1.OperationPhaseDone, v1alpha1.OperationPhaseStale,
		v1alpha1.OperationPhaseExecuting, v1alpha1.OperationPhasePartlyFailed:
		return nil
	}
	if op.Spec.Confirm != nil {
		return p.confirmedOperation(op, env)
	}
	return p.plannedOperation(op, env)
}

// plannedOperation plans an unconfirmed Operation: a new one gets its plan, a planned one is planned again
// and written only when its ID or a cost input changed.
func (p *pass) plannedOperation(op *v1alpha1.Operation, env *opEnv) error {
	req, err := env.request(op, metav1.NewTime(p.now))
	if err != nil {
		return err
	}
	plan, refusal := p.planOperation(req)
	if refusal != "" {
		return p.refuseOperation(op, refusal)
	}
	if stored := op.Status.Plan; stored != nil && stored.ID == plan.ID {
		// The ID is the same. Plan again at the stored plan's own time: the figures are snapshots at it, so
		// they differ from the stored ones only where an input differs, a new queue wait or a moved hard
		// end, never because time passed.
		req.at = stored.ObservedAt.Time
		if again, refusal := p.planOperation(req); refusal == "" && sameFigures(stored, again) {
			plan = stored
		}
	}
	policies, err := env.policyList()
	if err != nil {
		return err
	}
	return p.setOperationStatus(op, func(s *v1alpha1.OperationStatus) {
		s.Phase, s.ObservedGeneration, s.Plan = v1alpha1.OperationPhasePlanned, op.Generation, plan
		p.setCondition(s, op, v1alpha1.ConditionExecutable, metav1.ConditionFalse, v1alpha1.ReasonNotConfirmed,
			"planned: waiting for the owner to confirm plan "+plan.ID)
		p.setPolicyCondition(s, op, standingsOf(op, plan, policies, nil))
	})
}

// sameFigures reports whether two plans of one ID differ in no figure that a cost input moves. The
// Provider's read time on the queue waits and a pending Allocation's own start estimate are left out: the
// first moves with every read of the Provider's status, the second with every scheduler pass, and a write
// for either would move the Operation's resourceVersion under an owner who is about to confirm.
func sameFigures(a, b *v1alpha1.OperationPlan) bool {
	return apiequality.Semantic.DeepEqual(figuresOnly(a), figuresOnly(b))
}

// figuresOnly is a copy of the plan without the figures that age on their own.
func figuresOnly(p *v1alpha1.OperationPlan) *v1alpha1.OperationPlan {
	c := p.DeepCopy()
	for i := range c.Targets {
		if cost := c.Targets[i].Cost; cost != nil {
			cost.StartEstimate = nil
			if cost.QueueWaits != nil {
				cost.QueueWaits.ObservedAt = nil
			}
		}
	}
	return c
}

// confirmedOperation handles an Operation whose owner confirmed a plan: the plan is frozen, the backstop
// checks the confirm, and the Approvals are counted.
func (p *pass) confirmedOperation(op *v1alpha1.Operation, env *opEnv) error {
	plan := op.Status.Plan
	if plan == nil || plan.ID != op.Spec.Confirm.PlanID || planID(op.Spec.Action, plan.ModelDeployment, plan.Targets) != plan.ID {
		// No plan in status, an ID that is not the confirmed one, or an ID that is not the hash of the plan
		// it sits on: this controller never wrote it (the plan was written by another identity, or
		// edited). It never runs.
		return p.refuseOperation(op, errUnwrittenPlan)
	}
	policies, err := env.policyList()
	if err != nil {
		return err
	}
	approvals, err := env.approvalList()
	if err != nil {
		return err
	}
	var counted []*v1alpha1.Approval
	for i := range approvals {
		if countsFor(&approvals[i], op.Namespace, op.UID, plan.ID) {
			counted = append(counted, &approvals[i])
		}
	}
	standings := standingsOf(op, plan, policies, counted)
	var waiting []targetStanding
	for _, s := range standings {
		if !s.approved() {
			waiting = append(waiting, s)
		}
	}
	return p.setOperationStatus(op, func(s *v1alpha1.OperationStatus) {
		s.ObservedGeneration = op.Generation
		switch {
		case len(standings) == 0:
			// Nothing needs approval: the Operation stays as it was, and goes to the commit when
			// execution exists.
			p.setCondition(s, op, v1alpha1.ConditionExecutable, metav1.ConditionFalse, v1alpha1.ReasonExecutionNotBuilt,
				"confirmed, and no target needs approval; this release plans Operations and does not execute them")
		case len(waiting) == 0:
			s.Phase = v1alpha1.OperationPhaseAwaitingApproval
			p.setCondition(s, op, v1alpha1.ConditionExecutable, metav1.ConditionFalse, v1alpha1.ReasonExecutionNotBuilt,
				"confirmed, and every target that needs approval has it; this release plans Operations and does not execute them")
		default:
			s.Phase = v1alpha1.OperationPhaseAwaitingApproval
			p.setCondition(s, op, v1alpha1.ConditionExecutable, metav1.ConditionFalse, v1alpha1.ReasonAwaitingApproval, waitMessage(waiting))
		}
		p.setPolicyCondition(s, op, waiting)
	})
}

// standingsOf resolves the policies of every target that needs approval and counts the Approvals toward
// them. The policies are read as they are now: a change since the plan turns the plan stale at the check
// that execution brings, and until then a policy that no longer names an approver no longer counts him.
func standingsOf(op *v1alpha1.Operation, plan *v1alpha1.OperationPlan, policies []v1alpha1.ApprovalPolicy, counted []*v1alpha1.Approval) []targetStanding {
	var out []targetStanding
	for _, t := range plan.Targets {
		if !t.NeedsApproval {
			continue
		}
		r := resolvePolicies(op.Namespace, op.Spec.ModelDeploymentRef.Name, t.Provider, policies)
		out = append(out, standingOf(t, r, counted))
	}
	return out
}

// refuseOperation writes Refused, terminal, with the reason in the Executable condition.
func (p *pass) refuseOperation(op *v1alpha1.Operation, why string) error {
	return p.setOperationStatus(op, func(s *v1alpha1.OperationStatus) {
		s.Phase, s.ObservedGeneration = v1alpha1.OperationPhaseRefused, op.Generation
		p.setCondition(s, op, v1alpha1.ConditionExecutable, metav1.ConditionFalse, v1alpha1.ReasonRefused, why)
		apimeta.RemoveStatusCondition(&s.Conditions, v1alpha1.ConditionNoApprovalPolicy)
		if s.FinishedAt == nil {
			t := metav1.NewTime(p.now)
			s.FinishedAt = &t
		}
	})
}

// setCondition sets one of the Operation's conditions, keeping its transition time while its status holds.
func (p *pass) setCondition(s *v1alpha1.OperationStatus, op *v1alpha1.Operation, typ string, status metav1.ConditionStatus, reason, msg string) {
	if len(msg) > maxConditionMessage {
		msg = msg[:maxConditionMessage-len("...")] + "..."
	}
	cond(&s.Conditions, typ, status, reason, msg, op.Generation, metav1.NewTime(p.now))
}

// setPolicyCondition sets NoApprovalPolicy for the standings that cannot be approved at all: a target with
// no policy that applies, or two policies of one scope. With none, the condition is removed.
func (p *pass) setPolicyCondition(s *v1alpha1.OperationStatus, op *v1alpha1.Operation, standings []targetStanding) {
	var stuck []targetStanding
	reason := v1alpha1.ReasonNoApprovalPolicy
	for _, st := range standings {
		if st.none || len(st.duplicate) > 0 {
			stuck = append(stuck, st)
			if len(st.duplicate) > 0 {
				reason = v1alpha1.ReasonDuplicateApprovalPolicy
			}
		}
	}
	if len(stuck) == 0 {
		apimeta.RemoveStatusCondition(&s.Conditions, v1alpha1.ConditionNoApprovalPolicy)
		return
	}
	p.setCondition(s, op, v1alpha1.ConditionNoApprovalPolicy, metav1.ConditionTrue, reason, waitMessage(stuck))
}

// setOperationStatus applies mutate to a copy of the Operation's status and writes it when it changed, with
// the resourceVersion the pass read and the lifecycle controller's field manager. A conflict fails the
// pass, and the next one reads again (HBEP-7, open question 11).
func (p *pass) setOperationStatus(op *v1alpha1.Operation, mutate func(*v1alpha1.OperationStatus)) error {
	next := op.DeepCopy()
	mutate(&next.Status)
	if apiequality.Semantic.DeepEqual(op.Status, next.Status) {
		return nil
	}
	if err := p.r.Client.Status().Update(p.ctx, next, owner); err != nil {
		return fmt.Errorf("update Operation %s status: %w", op.Name, err)
	}
	*op = *next
	return nil
}

// opsNamingKey is whether a NON-terminal Operation names the pass's ModelDeployment: Reconcile keeps requeueing
// the key of a deleted deployment while one does, so it is still planned, and later executed and reported. A
// terminal one (Done, Stale, Refused) holds the key no longer: nothing removes a finished Operation until PR 5, so
// holding it would requeue a deleted deployment every 30 s for as long as its Operations exist. Not terminal is no
// phase yet, Planned, AwaitingApproval, Executing and PartlyFailed.
func (p *pass) opsNamingKey() bool {
	for _, op := range p.ops {
		switch op.Status.Phase {
		case v1alpha1.OperationPhaseRefused, v1alpha1.OperationPhaseDone, v1alpha1.OperationPhaseStale:
		default:
			return true
		}
	}
	return false
}
