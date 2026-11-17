package agentcli

import (
	"context"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// waitingOnA is an Operation of agent B that waits for an approval under the Provider's policy, which
// names agent A, and the global policy, which names the person.
func waitingOnA(name string) *v1alpha1.Operation {
	spec := scaleSpec("m", 1)
	spec.Confirm = &v1alpha1.OperationConfirm{PlanID: planAB}
	plan := simplePlan(planAB)
	plan.Targets[0].Policies = policies()
	return opObject(name, userB, spec, v1alpha1.OperationStatus{Phase: v1alpha1.OperationPhaseAwaitingApproval, Plan: plan})
}

func TestApproveCreatesTheApproval(t *testing.T) {
	w := newWorld(t, []client.Object{waitingOnA("scale-m")})
	if code := w.run("", "approve", "scale-m", "--plan", planAB); code != exitOK {
		t.Fatalf("exit %d, stderr %q", code, w.errs.String())
	}
	// The name's digest is sha256(user + "\n" + plan ID), worked out in the shell: 711ca51436.
	var ap v1alpha1.Approval
	if err := w.store.Get(context.Background(), types.NamespacedName{Namespace: testNS, Name: "scale-m-711ca51436"}, &ap); err != nil {
		t.Fatalf("the Approval is not there under the hand-computed name: %v", err)
	}
	if ap.Spec.Operation.Name != "scale-m" || ap.Spec.Operation.UID != "uid-scale-m" || ap.Spec.PlanID != planAB ||
		ap.Spec.Approver.Username != userA || strings.Join(ap.Spec.Approver.Groups, ",") != strings.Join(groupsOfAgent, ",") {
		t.Errorf("Approval spec %+v, want the Operation, the plan, and the caller's user and groups exactly", ap.Spec)
	}
	if got := w.writes(); len(got) != 1 || got[0] != "create *v1alpha1.Approval" {
		t.Errorf("writes %v, want one Approval create", got)
	}
	contains(t, "stdout", w.out.String(), "approved plan "+planAB+" of Operation scale-m as "+userA)
}

// Q6: an identity outside every policy that applies is refused by the CLI, with the policies and their
// approvers named, and nothing is created. The controller's count stays the authority.
func TestApproveRefusesAnIdentityOutsideTheApplicablePolicies(t *testing.T) {
	w := newWorld(t, []client.Object{waitingOnA("scale-m")})
	w.user = userB
	if code := w.run("", "approve", "scale-m", "--plan", planAB); code != exitFail {
		t.Fatalf("exit %d, want 1", code)
	}
	contains(t, "stderr", w.errs.String(), userB+" is an approver of no policy",
		"provider.sano (approvers: serviceaccount hpcburst-agents/agent-a)", "global (approvers: user admin, group operators)")
	if got := w.writes(); len(got) != 0 {
		t.Errorf("writes %v, want none", got)
	}
	var l v1alpha1.ApprovalList
	if err := w.store.List(context.Background(), &l); err != nil || len(l.Items) != 0 {
		t.Errorf("Approvals %v (%v), want none", l.Items, err)
	}
}

func TestApproveRefusals(t *testing.T) {
	wrongPhase := waitingOnA("op-phase")
	wrongPhase.Status.Phase = v1alpha1.OperationPhaseExecuting
	noTarget := waitingOnA("op-none")
	noTarget.Status.Plan.Targets[0].NeedsApproval = false
	w := newWorld(t, []client.Object{waitingOnA("op"), wrongPhase, noTarget})
	rows := []struct {
		args []string
		want string
	}{
		{[]string{"approve", "op", "--plan", planCD}, "the current plan is " + planAB},
		{[]string{"approve", "op-phase", "--plan", planAB}, "is in phase Executing"},
		{[]string{"approve", "op-none", "--plan", planAB}, "no target of plan " + planAB + " needs approval"},
		{[]string{"approve", "absent", "--plan", planAB}, "Operation absent not found"},
	}
	for _, r := range rows {
		if code := w.run("", r.args...); code != exitFail {
			t.Errorf("%v: exit %d, want 1", r.args, code)
		}
		contains(t, strings.Join(r.args, " "), w.errs.String(), r.want)
	}
	if got := w.writes(); len(got) != 0 {
		t.Errorf("writes %v, want none", got)
	}
}

func TestApproveTwiceIsIdempotent(t *testing.T) {
	w := newWorld(t, []client.Object{waitingOnA("scale-m")})
	for i := 1; i <= 2; i++ {
		if code := w.run("", "approve", "scale-m", "--plan", planAB); code != exitOK {
			t.Fatalf("call %d: exit %d, stderr %q", i, code, w.errs.String())
		}
	}
	contains(t, "stdout of the second call", w.out.String(), "already approved")
	var l v1alpha1.ApprovalList
	if err := w.store.List(context.Background(), &l); err != nil || len(l.Items) != 1 {
		t.Errorf("Approvals %d (%v), want one", len(l.Items), err)
	}
	// An Approval of that name for another Operation (a deleted and re-created one has a new UID) is refused.
	op := w.operation("scale-m")
	op.UID = "uid-new"
	if err := w.store.Update(context.Background(), op); err != nil {
		t.Fatal(err)
	}
	if code := w.run("", "approve", "scale-m", "--plan", planAB); code != exitFail {
		t.Errorf("an Approval of another Operation UID under the name: exit %d, want 1", code)
	}
	contains(t, "stderr", w.errs.String(), "exists with a different request")
}

func TestApproveJSONGolden(t *testing.T) {
	w := newWorld(t, []client.Object{waitingOnA("scale-m")})
	if code := w.run("", "approve", "scale-m", "--plan", planAB, "--json", "-n", testNS); code != exitOK {
		t.Fatalf("exit %d, stderr %q", code, w.errs.String())
	}
	sameJSON(t, w.out.Bytes(), readGolden(t, "approve.json"))
}

// approvals: which Operations the caller may approve.
func TestApprovalsListsWhatTheCallerMayApprove(t *testing.T) {
	approved := waitingOnA("op-approved") // A approved it
	oldPlan := waitingOnA("op-old-plan")  // A approved an older plan: this one still needs A
	notTheirs := waitingOnA("op-other")   // its policies name nobody A is
	for i := range notTheirs.Status.Plan.Targets[0].Policies {
		notTheirs.Status.Plan.Targets[0].Policies[i].Approvers = []v1alpha1.Approver{{User: "someone-else"}}
	}
	planned := waitingOnA("op-planned")
	planned.Status.Phase = v1alpha1.OperationPhasePlanned
	noNeed := waitingOnA("op-no-need")
	noNeed.Status.Plan.Targets[0].NeedsApproval = false
	byGroup := waitingOnA("op-group")
	byGroup.Status.Plan.Targets[0].Policies = []v1alpha1.AppliedPolicy{{Name: "global", Approvers: []v1alpha1.Approver{{Group: "system:authenticated"}}}}
	elsewhere := waitingOnA("op-elsewhere")
	elsewhere.Namespace = "other"
	ap := func(name string, op *v1alpha1.Operation, plan string) *v1alpha1.Approval {
		return &v1alpha1.Approval{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: op.Namespace},
			Spec: v1alpha1.ApprovalSpec{Operation: v1alpha1.OperationReference{Name: op.Name, UID: op.UID}, PlanID: plan,
				Approver: v1alpha1.ApproverIdentity{Username: userA, Groups: groupsOfAgent}}}
	}
	objs := []client.Object{waitingOnA("op-plain"), approved, oldPlan, notTheirs, planned, noNeed, byGroup, elsewhere,
		ap("ap-1", approved, planAB), ap("ap-2", oldPlan, "0000000000000000")}
	w := newWorld(t, objs)

	if code := w.run("", "approvals"); code != exitOK {
		t.Fatalf("exit %d, stderr %q", code, w.errs.String())
	}
	out := w.out.String()
	contains(t, "approvals", out, "team/op-plain:", "team/op-old-plan:", "team/op-group:", "approve with: hpcburst approve op-plain --plan "+planAB+" -n team")
	lacks(t, "approvals", out, "op-approved", "op-other", "op-planned", "op-no-need", "op-elsewhere")

	if code := w.run("", "approvals", "-A"); code != exitOK {
		t.Fatalf("-A: exit %d, stderr %q", code, w.errs.String())
	}
	contains(t, "approvals -A", w.out.String(), "other/op-elsewhere:", "team/op-plain:")
}

func TestApprovalsJSONGolden(t *testing.T) {
	scale := scaleOperation()
	scale.Status.Conditions = nil
	w := newWorld(t, []client.Object{scale})
	if code := w.run("", "approvals", "--json"); code != exitOK {
		t.Fatalf("exit %d, stderr %q", code, w.errs.String())
	}
	sameJSON(t, w.out.Bytes(), readGolden(t, "approvals.json"))

	// Nothing waits: an empty list, never null.
	w = newWorld(t, nil)
	if code := w.run("", "approvals", "--json"); code != exitOK {
		t.Fatalf("exit %d", code)
	}
	sameJSON(t, w.out.Bytes(), []byte(`{"schemaVersion": 1, "operations": []}`))
	w.run("", "approvals")
	contains(t, "stdout", w.out.String(), "no Operation waits for your approval")
}

// The final approver's Create lands, the reply is lost, the controller runs the Operation on, and the retry meets an Operation
// that is no longer in AwaitingApproval: it finds its own Approval first, whatever the phase, and succeeds without a write. A
// test that runs no controller between the two calls cannot see it.
func TestApproveRetryAfterTheOperationMovedOn(t *testing.T) {
	for _, phase := range []v1alpha1.OperationPhase{v1alpha1.OperationPhaseExecuting, v1alpha1.OperationPhaseDone, v1alpha1.OperationPhasePartlyFailed, v1alpha1.OperationPhaseAwaitingApproval} {
		t.Run(string(phase), func(t *testing.T) {
			op := waitingOnA("scale-m")
			op.Status.Phase = phase
			own := &v1alpha1.Approval{ObjectMeta: metav1.ObjectMeta{Name: "scale-m-711ca51436", Namespace: testNS},
				Spec: v1alpha1.ApprovalSpec{Operation: v1alpha1.OperationReference{Name: "scale-m", UID: "uid-scale-m"}, PlanID: planAB,
					Approver: v1alpha1.ApproverIdentity{Username: userA, Groups: groupsOfAgent}}}
			w := newWorld(t, []client.Object{op, own})
			if code := w.run("", "approve", "scale-m", "--plan", planAB); code != exitOK {
				t.Fatalf("exit %d, want 0; stderr %q", code, w.errs.String())
			}
			contains(t, "stdout", w.out.String(), "already approved plan "+planAB)
			if got := w.writes(); len(got) != 0 {
				t.Errorf("writes %v, want none: the retry created an Approval", got)
			}
		})
	}
	// Another approver's Approval of that plan is not the caller's: the phase check still refuses.
	op := waitingOnA("scale-m")
	op.Status.Phase = v1alpha1.OperationPhaseExecuting
	other := &v1alpha1.Approval{ObjectMeta: metav1.ObjectMeta{Name: "scale-m-other", Namespace: testNS},
		Spec: v1alpha1.ApprovalSpec{Operation: v1alpha1.OperationReference{Name: "scale-m", UID: "uid-scale-m"}, PlanID: planAB,
			Approver: v1alpha1.ApproverIdentity{Username: userB, Groups: groupsOfAgent}}}
	w := newWorld(t, []client.Object{op, other})
	if code := w.run("", "approve", "scale-m", "--plan", planAB); code != exitFail {
		t.Errorf("an Operation in phase Executing with no Approval of the caller's: exit %d, want 1", code)
	}
	contains(t, "stderr", w.errs.String(), "only a plan in AwaitingApproval takes an approval")
}
