package agentcli

import (
	"context"
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// namesCaller reports whether the approver names the caller: the same user, a group among the caller's,
// or a ServiceAccount whose user name, system:serviceaccount:<namespace>:<name>, is the caller's. It is
// the rule the controller counts by (it cannot be imported here, so it is written again); the controller's
// count stays the authority.
func namesCaller(a v1alpha1.Approver, user string, groups []string) bool {
	switch {
	case a.User != "":
		return a.User == user
	case a.Group != "":
		return slices.Contains(groups, a.Group)
	case a.ServiceAccount != nil:
		return user == "system:serviceaccount:"+a.ServiceAccount.Namespace+":"+a.ServiceAccount.Name
	}
	return false
}

// policiesNamingCaller are the policies that apply to the targets of the plan that need approval and
// name the caller as an approver, in the order they appear, each once.
func policiesNamingCaller(p *v1alpha1.OperationPlan, user string, groups []string) []string {
	var out []string
	for _, t := range p.Targets {
		if !t.NeedsApproval {
			continue
		}
		for _, pol := range t.Policies {
			if slices.ContainsFunc(pol.Approvers, func(a v1alpha1.Approver) bool { return namesCaller(a, user, groups) }) && !slices.Contains(out, pol.Name) {
				out = append(out, pol.Name)
			}
		}
	}
	return out
}

// approvalPolicies are the policies of the targets of the plan that need approval, with their approvers.
func approvalPolicies(p *v1alpha1.OperationPlan) []policyView {
	var out []policyView
	seen := map[string]bool{}
	for _, t := range p.Targets {
		if !t.NeedsApproval {
			continue
		}
		for _, pol := range t.Policies {
			if !seen[pol.Name] {
				seen[pol.Name] = true
				out = append(out, policyView{Name: pol.Name, Approvers: approverViews(pol.Approvers)})
			}
		}
	}
	return out
}

// cmdApprove is hpcburst approve <operation> --plan <id>.
func (rt *cli) cmdApprove(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs, o := newFlags("approve", stderr, false)
	var planID string
	fs.StringVar(&planID, "plan", "", "the ID of the plan to approve (required)")
	name, code, done := oneArg(fs, args, "Operation name")
	if done {
		return code
	}
	if planID == "" {
		return failf(stderr, exitUsage, "approve: --plan <id> is required: the plan the caller has seen")
	}
	cl, ns, code := rt.open(ctx, o, stderr)
	if code != exitOK {
		return code
	}
	op, err := getOperation(ctx, cl.c, ns, name)
	if err != nil {
		return failf(stderr, exitFail, "%v", err)
	}
	if code := checkApprovable(op, planID, cl, stderr); code != exitOK {
		return code
	}
	ap := &v1alpha1.Approval{
		ObjectMeta: metav1.ObjectMeta{Name: approvalName(op.Name, cl.user, planID), Namespace: ns},
		Spec: v1alpha1.ApprovalSpec{
			Operation: v1alpha1.OperationReference{Name: op.Name, UID: op.UID}, PlanID: planID,
			Approver: v1alpha1.ApproverIdentity{Username: cl.user, Groups: slices.Clone(cl.groups)},
		},
	}
	result := "approved"
	switch err := cl.c.Create(ctx, ap); {
	case err == nil:
	case apierrors.IsAlreadyExists(err):
		have := &v1alpha1.Approval{}
		if err := cl.c.Get(ctx, client.ObjectKeyFromObject(ap), have); err != nil {
			return failf(stderr, exitFail, "read Approval %s: %v", ap.Name, err)
		}
		if !sameApproval(&have.Spec, &ap.Spec) {
			return failf(stderr, exitFail, "Approval %s exists with a different request: it is for another Operation or other groups", ap.Name)
		}
		result = "alreadyApproved"
	default:
		return failf(stderr, exitFail, "approve Operation %s: %v", name, err)
	}
	return rt.reportApproval(stdout, stderr, ap, result, o.asJSON)
}

// checkApprovable refuses an approval that cannot count: another plan, a phase that takes none, or a
// caller that no policy of the plan names (Q6). The refusal is a convenience; the controller's count
// is the authority, and there is no override.
func checkApprovable(op *v1alpha1.Operation, planID string, cl *caller, stderr io.Writer) int {
	plan := op.Status.Plan
	switch {
	case plan == nil:
		return failf(stderr, exitFail, "Operation %s has no plan yet (phase %s)", op.Name, phaseOf(op))
	case plan.ID != planID:
		return failf(stderr, exitFail, "plan %s is not the current plan of Operation %s: the current plan is %s", planID, op.Name, plan.ID)
	case op.Status.Phase != v1alpha1.OperationPhaseAwaitingApproval:
		return failf(stderr, exitFail, "Operation %s is in phase %s: only a plan in AwaitingApproval takes an approval", op.Name, phaseOf(op))
	case len(policiesNamingCaller(plan, cl.user, cl.groups)) > 0:
		return exitOK
	}
	var parts []string
	for _, p := range approvalPolicies(plan) {
		parts = append(parts, fmt.Sprintf("%s (approvers: %s)", p.Name, approversText(p.Approvers)))
	}
	if len(parts) == 0 {
		return failf(stderr, exitFail, "%s is an approver of no policy: no target of plan %s needs approval", cl.user, planID)
	}
	return failf(stderr, exitFail, "%s is an approver of no policy that applies to a target of plan %s that needs approval; the policies and their approvers: %s", cl.user, planID, strings.Join(parts, "; "))
}

// sameApproval reports whether an Approval that exists is the one asked for: the same Operation by name
// and UID, the same plan, the same approver, whose groups are compared as a set.
func sameApproval(a, b *v1alpha1.ApprovalSpec) bool {
	ga, gb := slices.Clone(a.Approver.Groups), slices.Clone(b.Approver.Groups)
	sort.Strings(ga)
	sort.Strings(gb)
	return a.Operation == b.Operation && a.PlanID == b.PlanID && a.Approver.Username == b.Approver.Username && slices.Equal(ga, gb)
}

// approvalView is the Approval as --json shows it.
type approvalView struct {
	Name      string     `json:"name"`
	Namespace string     `json:"namespace"`
	Operation string     `json:"operation"`
	PlanID    string     `json:"planID"`
	Approver  approverID `json:"approver"`
}

type approverID struct {
	User   string   `json:"user"`
	Groups []string `json:"groups,omitempty"`
}

func (rt *cli) reportApproval(stdout, stderr io.Writer, ap *v1alpha1.Approval, result string, asJSON bool) int {
	if asJSON {
		v := approvalView{Name: ap.Name, Namespace: ap.Namespace, Operation: ap.Spec.Operation.Name, PlanID: ap.Spec.PlanID,
			Approver: approverID{User: ap.Spec.Approver.Username, Groups: ap.Spec.Approver.Groups}}
		return emitCode(stderr, emitJSON(stdout, map[string]any{"schemaVersion": schemaVersion, "result": result, "approval": v}))
	}
	verb := "approved"
	if result == "alreadyApproved" {
		verb = "already approved"
	}
	fmt.Fprintf(stdout, "%s plan %s of Operation %s as %s (Approval %s)\n", verb, ap.Spec.PlanID, ap.Spec.Operation.Name, ap.Spec.Approver.Username, ap.Name)
	return exitOK
}

// approvable is one Operation the caller may approve, as --json shows it.
type approvable struct {
	Namespace       string             `json:"namespace"`
	Name            string             `json:"name"`
	Action          string             `json:"action"`
	ModelDeployment string             `json:"modelDeployment"`
	Owner           string             `json:"owner"`
	PlanID          string             `json:"planID"`
	Targets         []approvableTarget `json:"targets"`
	ApproveWith     string             `json:"approveWith"`
}

type approvableTarget struct {
	Kind     string   `json:"kind"`
	Name     string   `json:"name"`
	Effect   string   `json:"effect"`
	Policies []string `json:"policies"`
}

// cmdApprovals is hpcburst approvals [-A].
func (rt *cli) cmdApprovals(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs, o := newFlags("approvals", stderr, false)
	all := fs.Bool("A", false, "every namespace")
	fs.BoolVar(all, "all-namespaces", false, "every namespace (same as -A)")
	pos, code, done := parseArgs(fs, args)
	if done {
		return code
	}
	if len(pos) > 0 {
		return failf(stderr, exitUsage, "approvals: unexpected argument %q", pos[0])
	}
	cl, ns, code := rt.open(ctx, o, stderr)
	if code != exitOK {
		return code
	}
	scope := []client.ListOption{client.InNamespace(ns)}
	if *all {
		scope = nil
	}
	var ops v1alpha1.OperationList
	var aps v1alpha1.ApprovalList
	if err := cl.c.List(ctx, &ops, scope...); err != nil {
		return failf(stderr, exitFail, "list Operations: %v", err)
	}
	if err := cl.c.List(ctx, &aps, scope...); err != nil {
		return failf(stderr, exitFail, "list Approvals: %v", err)
	}
	list := approvableBy(ops.Items, aps.Items, cl.user, cl.groups)
	if o.asJSON {
		return emitCode(stderr, emitJSON(stdout, map[string]any{"schemaVersion": schemaVersion, "operations": list}))
	}
	printApprovable(stdout, list)
	return exitOK
}

// approvableBy are the Operations in AwaitingApproval with a target that needs approval under a policy
// that names the caller, and that the caller has not approved for the current plan.
func approvableBy(ops []v1alpha1.Operation, aps []v1alpha1.Approval, user string, groups []string) []approvable {
	out := []approvable{}
	for i := range ops {
		op := &ops[i]
		plan := op.Status.Plan
		if op.Status.Phase != v1alpha1.OperationPhaseAwaitingApproval || plan == nil || approvedBy(aps, op, plan.ID, user) {
			continue
		}
		a := approvable{Namespace: op.Namespace, Name: op.Name, Action: string(op.Spec.Action), ModelDeployment: op.Spec.ModelDeploymentRef.Name,
			Owner: op.Spec.Owner, PlanID: plan.ID, Targets: []approvableTarget{},
			ApproveWith: "hpcburst approve " + op.Name + " --plan " + plan.ID + inNamespace(op.Namespace)}
		for _, t := range plan.Targets {
			if !t.NeedsApproval {
				continue
			}
			var named []string
			for _, pol := range t.Policies {
				if slices.ContainsFunc(pol.Approvers, func(x v1alpha1.Approver) bool { return namesCaller(x, user, groups) }) {
					named = append(named, pol.Name)
				}
			}
			if len(named) > 0 {
				a.Targets = append(a.Targets, approvableTarget{Kind: string(t.Kind), Name: t.Name, Effect: string(t.Effect), Policies: named})
			}
		}
		if len(a.Targets) > 0 {
			out = append(out, a)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Namespace+"/"+out[i].Name < out[j].Namespace+"/"+out[j].Name
	})
	return out
}

// approvedBy reports whether the user already has an Approval for this Operation (by UID) and plan.
func approvedBy(aps []v1alpha1.Approval, op *v1alpha1.Operation, planID, user string) bool {
	return slices.ContainsFunc(aps, func(a v1alpha1.Approval) bool {
		return a.Namespace == op.Namespace && a.Spec.Operation.UID == types.UID(op.UID) && a.Spec.PlanID == planID && a.Spec.Approver.Username == user
	})
}

func printApprovable(w io.Writer, list []approvable) {
	if len(list) == 0 {
		fmt.Fprintln(w, "no Operation waits for your approval")
		return
	}
	for _, a := range list {
		fmt.Fprintf(w, "%s/%s: %s of ModelDeployment %s, owner %s, plan %s\n", a.Namespace, a.Name, a.Action, a.ModelDeployment, a.Owner, a.PlanID)
		for _, t := range a.Targets {
			fmt.Fprintf(w, "  %s %s: %s, under %s\n", t.Kind, t.Name, t.Effect, strings.Join(t.Policies, ", "))
		}
		fmt.Fprintf(w, "  approve with: %s\n", a.ApproveWith)
	}
}
