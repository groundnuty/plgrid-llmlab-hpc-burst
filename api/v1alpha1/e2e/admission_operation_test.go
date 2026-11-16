package e2e

// The owner pin, the approver pin and the capacity guard's Operation rules, as agents A and B with real
// tokens and as the person (issue #589, PR 3; HBEP-10, Owner and purpose, Confirm, Approvals; plan
// section 10). Each refusal names its policy. The confirm rows write the Operation's plan as the admin,
// who stands in for the lifecycle controller: no controller runs in this suite, and a confirm needs a plan
// in status, which only the controller writes.

import (
	"slices"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// agentOperation is a Scale Operation owned by the named identity.
func agentOperation(ns, name, owner string) *v1alpha1.Operation {
	op := newOperation(ns, name)
	op.Spec.Owner = owner
	return op
}

// writePlan writes a plan into the Operation's status as the admin, as the lifecycle controller would.
func writePlan(t *testing.T, op *v1alpha1.Operation, planID string) {
	t.Helper()
	cur := &v1alpha1.Operation{}
	must(t, c.Get(bg(), client.ObjectKeyFromObject(op), cur), "read the Operation")
	cur.Status.Phase = v1alpha1.OperationPhasePlanned
	cur.Status.Plan = &v1alpha1.OperationPlan{
		ID: planID, ObservedAt: metav1.Now(),
		ModelDeployment: v1alpha1.PlannedDeployment{Name: opDeployment, UID: "00000000-0000-0000-0000-000000000009", Generation: 1},
	}
	must(t, c.Status().Update(bg(), cur), "write the plan into the Operation's status as the admin")
}

func confirmAs(t *testing.T, w writer, op *v1alpha1.Operation, planID string) error {
	t.Helper()
	cur := &v1alpha1.Operation{}
	must(t, c.Get(bg(), client.ObjectKeyFromObject(op), cur), "read the Operation")
	cur.Spec.Confirm = &v1alpha1.OperationConfirm{PlanID: planID}
	if w.dry {
		return w.c.Update(bg(), cur, client.DryRunAll)
	}
	return w.c.Update(bg(), cur)
}

// Create: an Operation names its creator as its owner, and carries no owner reference or finalizer, no
// confirm, and no status that is kept. The person is bound by the owner rule too.
func TestOperationCreateRules(t *testing.T) {
	agents := newAgents(t, "agent-a", "agent-b")
	a, b := agents[0], agents[1]
	requireEnforced(t)
	ns := newNamespace(t)
	person := thePerson(t)
	personW := personWriter(t)

	requireAgentIdentity(t, a)
	foreign := agentOperation(ns, uniq("op"), b.username())
	requireDenied(t, "A's Operation owned by B", a.c.Create(bg(), foreign), ownerPin, "spec.owner is the identity that creates it")
	asPerson := agentOperation(ns, uniq("op"), person)
	requireDenied(t, "A's Operation owned by the person", a.c.Create(bg(), asPerson), ownerPin, "spec.owner is the identity that creates it")
	requireDenied(t, "the person's Operation owned by A", personW.c.Create(bg(), foreign.DeepCopy(), client.DryRunAll), ownerPin, "spec.owner is the identity that creates it")
	for _, o := range []*v1alpha1.Operation{foreign, asPerson} {
		if err := c.Get(bg(), client.ObjectKeyFromObject(o), &v1alpha1.Operation{}); !apierrors.IsNotFound(err) {
			t.Errorf("after the refused create the Operation %s exists: %v", o.Name, err)
		}
	}

	// ownerReferences and finalizers at create (Q5), each beside the same create from the person.
	withRefs := agentOperation(ns, uniq("op"), a.username())
	withRefs.OwnerReferences = []metav1.OwnerReference{ownerRefToAConfigMap()}
	withFin := agentOperation(ns, uniq("op"), a.username())
	withFin.Finalizers = []string{"example.invalid/hold"}
	for name, op := range map[string]*v1alpha1.Operation{"ownerReferences": withRefs, "a finalizer": withFin} {
		requireDenied(t, "A's Operation with "+name, a.c.Create(bg(), op), capacityGuard, "cannot create an Operation with ownerReferences or finalizers")
		personOp := op.DeepCopy()
		personOp.Spec.Owner = person
		requireAccepted(t, "the person's Operation with "+name+" (a dry run)", c.Create(bg(), personOp, client.DryRunAll))
	}

	// A create with the agent's own owner is accepted.
	mine := agentOperation(ns, uniq("op"), a.username())
	must(t, a.c.Create(bg(), mine), "A's Operation with its own spec.owner")
	// And the person's, with the person as owner.
	own := agentOperation(ns, uniq("op"), person)
	requireAccepted(t, "the person's Operation with the person as owner", c.Create(bg(), own))

	// A create with a status body: the status subresource drops it, so no agent forges a phase or a
	// committed list that the controller would act on (plan section 10).
	forged := agentOperation(ns, uniq("op"), a.username())
	forged.Status.Phase = v1alpha1.OperationPhaseExecuting
	forged.Status.Committed = []v1alpha1.CommittedTarget{{Kind: v1alpha1.TargetKindModelDeployment, Name: opDeployment, UID: "u", Effect: v1alpha1.PlanEffectDeleteDeployment}}
	must(t, a.c.Create(bg(), forged), "A's Operation with a status body")
	got := &v1alpha1.Operation{}
	must(t, c.Get(bg(), client.ObjectKeyFromObject(forged), got), "read it back as the admin")
	if got.Status.Phase != "" || len(got.Status.Committed) != 0 {
		t.Errorf("the stored Operation has the phase %q and %d committed targets from the agent's create, want none", got.Status.Phase, len(got.Status.Committed))
	}
}

// Update: an agent cannot add or change an Operation's ownerReferences or finalizers (Q5), and the person
// can.
func TestAgentCannotChangeAnOperationsOwnerReferencesOrFinalizers(t *testing.T) {
	a := newAgents(t, "agent-a", "agent-b")[0]
	requireEnforced(t)
	ns := newNamespace(t)
	op := agentOperation(ns, uniq("op"), a.username())
	must(t, a.c.Create(bg(), op), "A's Operation")
	requireAgentIdentity(t, a)
	for _, tc := range []struct {
		name string
		edit func(*v1alpha1.Operation)
	}{
		{"an owner reference added", func(o *v1alpha1.Operation) { o.OwnerReferences = []metav1.OwnerReference{ownerRefToAConfigMap()} }},
		{"a finalizer added", func(o *v1alpha1.Operation) { o.Finalizers = []string{"example.invalid/hold"} }},
	} {
		cur := &v1alpha1.Operation{}
		must(t, a.c.Get(bg(), client.ObjectKeyFromObject(op), cur), "A's read")
		before := cur.ResourceVersion
		tc.edit(cur)
		requireDenied(t, "A: "+tc.name, a.c.Update(bg(), cur), capacityGuard, "cannot change an Operation's ownerReferences or finalizers")
		personCur := &v1alpha1.Operation{}
		must(t, c.Get(bg(), client.ObjectKeyFromObject(op), personCur), "the person's read")
		tc.edit(personCur)
		requireAccepted(t, "the person: "+tc.name+" (a dry run)", c.Update(bg(), personCur, client.DryRunAll))
		after := &v1alpha1.Operation{}
		must(t, c.Get(bg(), client.ObjectKeyFromObject(op), after), "read after")
		if after.ResourceVersion != before {
			t.Errorf("%s: the refused write changed the Operation (resourceVersion %s, then %s)", tc.name, before, after.ResourceVersion)
		}
	}
}

// Confirm (HBEP-10, Confirm): only the owner, only after a plan is written, only by the current plan's id.
func TestConfirmIsBoundToTheOwnerAndTheCurrentPlan(t *testing.T) {
	agents := newAgents(t, "agent-a", "agent-b")
	a, b := agents[0], agents[1]
	requireEnforced(t)
	ns := newNamespace(t)
	person := personWriter(t)
	op := agentOperation(ns, uniq("op"), a.username())
	must(t, a.c.Create(bg(), op), "A's Operation")

	// No plan in status yet: the owner's confirm is refused, with the pin's message about the plan.
	requireAgentIdentity(t, a)
	requireDenied(t, "A's confirm with no plan in status", confirmAs(t, agentWriter(a), op, opPlanID), ownerPin, "only after the controller wrote a plan")

	writePlan(t, op, opPlanID)

	requireAgentIdentity(t, b)
	requireDenied(t, "B's confirm of A's Operation", confirmAs(t, agentWriter(b), op, opPlanID), ownerPin, "only an Operation's owner confirms")
	requireDenied(t, "the person's confirm of A's Operation", confirmAs(t, person, op, opPlanID), ownerPin, "only an Operation's owner confirms")
	requireAgentIdentity(t, a)
	requireDenied(t, "A's confirm of a plan that is not the current one", confirmAs(t, agentWriter(a), op, opAnotherPlan), ownerPin, "id of the Operation's current plan")
	// The CRD refuses an empty plan ID (it is an Invalid, not a policy's Forbidden).
	err := confirmAs(t, agentWriter(a), op, "")
	if !apierrors.IsInvalid(err) {
		t.Errorf("A's confirm with an empty plan ID: %v, want the CRD's Invalid refusal", err)
	}
	got := &v1alpha1.Operation{}
	must(t, c.Get(bg(), client.ObjectKeyFromObject(op), got), "read")
	if got.Spec.Confirm != nil {
		t.Errorf("a refused confirm was stored: %+v", got.Spec.Confirm)
	}

	// The owner, with the current plan's id: accepted, and stored.
	must(t, confirmAs(t, agentWriter(a), op, opPlanID), "A's confirm of its own Operation with the current plan's id")
	must(t, c.Get(bg(), client.ObjectKeyFromObject(op), got), "read")
	if got.Spec.Confirm == nil || got.Spec.Confirm.PlanID != opPlanID {
		t.Errorf("the stored confirm is %+v, want the plan %s", got.Spec.Confirm, opPlanID)
	}

	// The person confirms the person's own Operation: the same rule, with the person as owner.
	mine := agentOperation(ns, uniq("op"), thePerson(t))
	must(t, c.Create(bg(), mine), "the person's Operation")
	writePlan(t, mine, opPlanID)
	requireDenied(t, "the person's confirm with a wrong id", confirmAs(t, person, mine, opAnotherPlan), ownerPin, "id of the Operation's current plan")
	requireAccepted(t, "the person's confirm of its own Operation (a dry run)", confirmAs(t, person, mine, opPlanID))
}

func approvalOf(ns, name, user string, groups []string) *v1alpha1.Approval {
	return &v1alpha1.Approval{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: v1alpha1.ApprovalSpec{
			Operation: v1alpha1.OperationReference{Name: "op-x", UID: "00000000-0000-0000-0000-000000000007"},
			PlanID:    opPlanID,
			Approver:  v1alpha1.ApproverIdentity{Username: user, Groups: groups},
		},
	}
}

// An Approval is accepted only from its approver's own identity: the same user name, and the same set of
// groups in any order. The person is bound by it too.
func TestApproverPinAcceptsAnApprovalOnlyFromItsApprover(t *testing.T) {
	agents := newAgents(t, "agent-a", "agent-b")
	a, b := agents[0], agents[1]
	requireEnforced(t)
	ns := newNamespace(t)
	requireAgentIdentity(t, a)
	// A's groups, as the API server reports them, never written by hand.
	groups := whoAmI(t, a.cs).Groups
	if len(groups) < 3 {
		t.Fatalf("agent A's groups are %v, want at least three", groups)
	}
	reversed := slices.Clone(groups)
	slices.Reverse(reversed)
	personCS, _ := clientsFromConfig(t, cfg)
	personInfo := whoAmI(t, personCS)

	const name = "requester's own user name"
	const grp = "requester's own groups"
	for _, tc := range []struct {
		what   string
		ap     *v1alpha1.Approval
		wantIn string
	}{
		{"A names the person", approvalOf(ns, uniq("ap"), personInfo.Username, personInfo.Groups), name},
		{"A names B", approvalOf(ns, uniq("ap"), b.username(), whoAmI(t, b.cs).Groups), name},
		{"A's user name with other groups", approvalOf(ns, uniq("ap"), a.username(), []string{"system:authenticated"}), grp},
		{"A's user name with an extra group", approvalOf(ns, uniq("ap"), a.username(), append(slices.Clone(groups), "example-group")), grp},
		{"A's user name with no groups", approvalOf(ns, uniq("ap"), a.username(), nil), grp},
	} {
		requireDenied(t, tc.what, a.c.Create(bg(), tc.ap), approverPin, tc.wantIn)
		if err := c.Get(bg(), client.ObjectKeyFromObject(tc.ap), &v1alpha1.Approval{}); !apierrors.IsNotFound(err) {
			t.Errorf("%s: after the refused create the Approval exists: %v", tc.what, err)
		}
	}
	// A names itself, with its groups in another order: accepted, and stored with the groups as given.
	ok := approvalOf(ns, uniq("ap"), a.username(), reversed)
	must(t, a.c.Create(bg(), ok), "A's Approval naming itself with its groups in another order")
	got := &v1alpha1.Approval{}
	must(t, c.Get(bg(), client.ObjectKeyFromObject(ok), got), "read the Approval")
	if got.Spec.Approver.Username != a.username() || !slices.Equal(got.Spec.Approver.Groups, reversed) {
		t.Errorf("the stored approver is %+v, want A with its groups reversed", got.Spec.Approver)
	}

	// The person: bound by the same rule.
	requireDenied(t, "the person naming A", c.Create(bg(), approvalOf(ns, uniq("ap"), a.username(), groups), client.DryRunAll), approverPin, name)
	requireAccepted(t, "the person naming the person", c.Create(bg(), approvalOf(ns, uniq("ap"), personInfo.Username, personInfo.Groups)))
}
