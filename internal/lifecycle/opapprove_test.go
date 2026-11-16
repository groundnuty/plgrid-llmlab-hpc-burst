package lifecycle

// The approval policy matrix and the counting of Approvals (HBEP-10, Approval policy and Approvals; #589,
// PR 4, AC 3 and AC 7's counting half). The policies, the expected resolutions and the approvers are worked
// out by hand from the owner's rules of 15 Nov 2026; every approval test has a requester (agent A), a second
// identity outside every policy (agent B) and the person.

import (
	"slices"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

const (
	userPerson = "person"
	userAgentA = "system:serviceaccount:hpcburst-agents:agent-a"
	userAgentB = "system:serviceaccount:hpcburst-agents:agent-b"
)

func policyOf(name string, scope v1alpha1.ApprovalScope, approvers ...v1alpha1.Approver) v1alpha1.ApprovalPolicy {
	return v1alpha1.ApprovalPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID("uid-" + name), Generation: 1},
		Spec:       v1alpha1.ApprovalPolicySpec{Scope: scope, Approvers: approvers},
	}
}

func globalPolicy(a ...v1alpha1.Approver) v1alpha1.ApprovalPolicy {
	g := true
	return policyOf("global", v1alpha1.ApprovalScope{Global: &g}, a...)
}

func providerPolicy(p string, a ...v1alpha1.Approver) v1alpha1.ApprovalPolicy {
	return policyOf("provider."+p, v1alpha1.ApprovalScope{Provider: p}, a...)
}

func namespacePolicy(ns string, a ...v1alpha1.Approver) v1alpha1.ApprovalPolicy {
	return policyOf("namespace."+ns, v1alpha1.ApprovalScope{Namespace: ns}, a...)
}

func deploymentPolicy(ns, md string, a ...v1alpha1.Approver) v1alpha1.ApprovalPolicy {
	return policyOf("modeldeployment."+ns+"."+md, v1alpha1.ApprovalScope{ModelDeployment: &v1alpha1.ScopedDeployment{Namespace: ns, Name: md}}, a...)
}

func policyNames(r resolution) []string {
	var out []string
	for _, p := range r.policies {
		out = append(out, p.Name)
	}
	return out
}

var person = v1alpha1.Approver{User: userPerson}

// The matrix, for a target on Provider sano in namespace ns of ModelDeployment md.
func TestPolicyResolution(t *testing.T) {
	agentA := v1alpha1.Approver{ServiceAccount: &v1alpha1.ServiceAccountRef{Namespace: "hpcburst-agents", Name: "agent-a"}}
	for _, tc := range []struct {
		name     string
		provider string
		policies []v1alpha1.ApprovalPolicy
		want     []string
		none     bool
	}{
		{"the ModelDeployment's policy overrides the namespace's", "", []v1alpha1.ApprovalPolicy{namespacePolicy("ns", person), deploymentPolicy("ns", "md", agentA)}, []string{"modeldeployment.ns.md"}, false},
		{"global applies only when no other does", "", []v1alpha1.ApprovalPolicy{globalPolicy(person)}, []string{"global"}, false},
		{"a namespace policy displaces global", "", []v1alpha1.ApprovalPolicy{globalPolicy(person), namespacePolicy("ns", agentA)}, []string{"namespace.ns"}, false},
		{"a Provider policy beside a namespace policy: one approval each", "sano", []v1alpha1.ApprovalPolicy{providerPolicy("sano", agentA), namespacePolicy("ns", person)}, []string{"namespace.ns", "provider.sano"}, false},
		{"a Provider policy beside the ModelDeployment's: one approval each", "sano", []v1alpha1.ApprovalPolicy{providerPolicy("sano", agentA), deploymentPolicy("ns", "md", person), namespacePolicy("ns", person)}, []string{"modeldeployment.ns.md", "provider.sano"}, false},
		// The owner's Sano example: a Provider policy naming agent A, global beside it: A's own policy applies alone.
		{"a Provider policy alone, with only global beside it, applies alone", "sano", []v1alpha1.ApprovalPolicy{providerPolicy("sano", agentA), globalPolicy(person)}, []string{"provider.sano"}, false},
		{"another Provider's policy does not apply: global does", "sano", []v1alpha1.ApprovalPolicy{providerPolicy("helios", agentA), globalPolicy(person)}, []string{"global"}, false},
		{"another namespace's policy does not apply: global does", "sano", []v1alpha1.ApprovalPolicy{namespacePolicy("elsewhere", agentA), globalPolicy(person)}, []string{"global"}, false},
		{"another deployment's policy does not apply", "sano", []v1alpha1.ApprovalPolicy{deploymentPolicy("ns", "other", agentA), globalPolicy(person)}, []string{"global"}, false},
		{"a target with no Provider has the namespace side only", "", []v1alpha1.ApprovalPolicy{providerPolicy("sano", agentA), namespacePolicy("ns", person)}, []string{"namespace.ns"}, false},
		{"no policy at all", "sano", nil, nil, true},
		{"no policy that applies, and no global", "sano", []v1alpha1.ApprovalPolicy{providerPolicy("helios", agentA)}, nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := resolvePolicies("ns", "md", tc.provider, tc.policies)
			if got := policyNames(r); !slices.Equal(got, tc.want) || r.none != tc.none || len(r.duplicate) != 0 {
				t.Errorf("resolved %v (none %v, duplicates %v), want %v (none %v)", got, r.none, r.duplicate, tc.want, tc.none)
			}
		})
	}
}

// Two policies of one scope, which a hand-made name allows, make the target unapprovable, and the
// resolution names both. The duplicate is found by the scope field, never by the name.
func TestTwoPoliciesOfOneScopeAreUnapprovable(t *testing.T) {
	dup := namespacePolicy("ns", person)
	dup.Name = "a-hand-made-name"
	r := resolvePolicies("ns", "md", "sano", []v1alpha1.ApprovalPolicy{namespacePolicy("ns", person), dup, globalPolicy(person)})
	if want := []string{"a-hand-made-name", "namespace.ns"}; !slices.Equal(r.duplicate, want) {
		t.Errorf("duplicates %v, want %v", r.duplicate, want)
	}
	s := standingOf(v1alpha1.PlanTarget{Kind: tInst, Name: "r1"}, r, []*v1alpha1.Approval{approvalBy(userPerson, "plan-1", "uid-op", opNS)})
	if s.approved() {
		t.Error("a target under two policies of one scope is never approved, whoever approves")
	}
	if got, want := waitMessage([]targetStanding{s}), "Instance r1: unapprovable, ApprovalPolicies a-hand-made-name and namespace.ns name one scope"; got != want {
		t.Errorf("message %q, want %q", got, want)
	}
	// A duplicated scope that does not apply (another namespace's) blocks nothing.
	r = resolvePolicies("ns", "md", "sano", []v1alpha1.ApprovalPolicy{namespacePolicy("elsewhere", person), func() v1alpha1.ApprovalPolicy {
		d := namespacePolicy("elsewhere", person)
		d.Name = "x"
		return d
	}(), globalPolicy(person)})
	if len(r.duplicate) != 0 || !slices.Equal(policyNames(r), []string{"global"}) {
		t.Errorf("a duplicate of a scope that does not apply: %v, %v", policyNames(r), r.duplicate)
	}
}

func approvalBy(user, planID string, opUID types.UID, ns string, groups ...string) *v1alpha1.Approval {
	return &v1alpha1.Approval{
		ObjectMeta: metav1.ObjectMeta{Name: "ap-" + user, Namespace: ns},
		Spec: v1alpha1.ApprovalSpec{
			Operation: v1alpha1.OperationReference{Name: "op", UID: opUID}, PlanID: planID,
			Approver: v1alpha1.ApproverIdentity{Username: user, Groups: groups},
		},
	}
}

// Who counts: the policy names the approver as a user, a group of the approver's recorded groups, or a
// ServiceAccount whose user name is system:serviceaccount:<namespace>:<name>.
func TestWhoAPolicyNames(t *testing.T) {
	policy := []v1alpha1.Approver{{User: userPerson}, {Group: "operators"}, {ServiceAccount: &v1alpha1.ServiceAccountRef{Namespace: "hpcburst-agents", Name: "agent-a"}}}
	for _, tc := range []struct {
		name string
		by   v1alpha1.ApproverIdentity
		want bool
	}{
		{"the named user", v1alpha1.ApproverIdentity{Username: userPerson}, true},
		{"a member of the named group", v1alpha1.ApproverIdentity{Username: "someone", Groups: []string{"staff", "operators"}}, true},
		{"the named ServiceAccount", v1alpha1.ApproverIdentity{Username: userAgentA}, true},
		{"a ServiceAccount of the same name in another namespace", v1alpha1.ApproverIdentity{Username: "system:serviceaccount:other:agent-a"}, false},
		{"a user whose name is the group's", v1alpha1.ApproverIdentity{Username: "operators"}, false},
		{"another agent, B", v1alpha1.ApproverIdentity{Username: userAgentB, Groups: []string{"system:serviceaccounts:hpcburst-agents"}}, false},
		{"someone with no group", v1alpha1.ApproverIdentity{Username: "someone"}, false},
	} {
		if got := policyApproved(policy, []*v1alpha1.Approval{{Spec: v1alpha1.ApprovalSpec{Approver: tc.by}}}); got != tc.want {
			t.Errorf("%s: counts = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// An Approval counts only for its own Operation (by UID), plan (by ID) and namespace.
func TestWhichApprovalsCount(t *testing.T) {
	for _, tc := range []struct {
		name string
		ap   *v1alpha1.Approval
		want bool
	}{
		{"its Operation, its plan, its namespace", approvalBy(userPerson, "plan-1", "uid-op", opNS), true},
		{"another plan ID", approvalBy(userPerson, "plan-0", "uid-op", opNS), false},
		{"an Operation recreated under the same name has another UID", approvalBy(userPerson, "plan-1", "uid-old", opNS), false},
		{"another namespace", approvalBy(userPerson, "plan-1", "uid-op", "elsewhere"), false},
	} {
		if got := countsFor(tc.ap, opNS, "uid-op", "plan-1"); got != tc.want {
			t.Errorf("%s: counts = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// The owner's Sano example, with two identities. Agent A requests; the Provider policy for Sano names A, and
// global names the person. A's Approval counts on Sano, where its policy applies alone, and not on helios,
// where global applies. B, outside every policy, counts nowhere. The person counts everywhere global applies,
// and on Sano not at all: the Provider policy applies alone there.
func TestTheSanoExampleWithTwoIdentities(t *testing.T) {
	agentA := v1alpha1.Approver{ServiceAccount: &v1alpha1.ServiceAccountRef{Namespace: "hpcburst-agents", Name: "agent-a"}}
	policies := []v1alpha1.ApprovalPolicy{providerPolicy("sano", agentA), globalPolicy(person)}
	onSano := v1alpha1.PlanTarget{Kind: tInst, Name: "r1", Provider: "sano", NeedsApproval: true}
	onHelios := v1alpha1.PlanTarget{Kind: tInst, Name: "r2", Provider: "helios", NeedsApproval: true}
	approved := func(target v1alpha1.PlanTarget, by ...string) bool {
		var counted []*v1alpha1.Approval
		for _, u := range by {
			counted = append(counted, approvalBy(u, "plan-1", "uid-op", opNS))
		}
		return standingOf(target, resolvePolicies(opNS, opMD, target.Provider, policies), counted).approved()
	}
	for _, tc := range []struct {
		target v1alpha1.PlanTarget
		by     []string
		want   bool
	}{
		{onSano, nil, false},
		{onSano, []string{userAgentA}, true},
		{onSano, []string{userAgentB}, false},
		{onSano, []string{userPerson}, false},
		{onHelios, []string{userAgentA}, false},
		{onHelios, []string{userAgentB}, false},
		{onHelios, []string{userPerson}, true},
	} {
		if got := approved(tc.target, tc.by...); got != tc.want {
			t.Errorf("target on %s approved by %v: %v, want %v", tc.target.Provider, tc.by, got, tc.want)
		}
	}
}

// A target under two policies needs an Approval counted toward each; one Approval counts toward every policy
// that names its approver.
func TestOneApprovalCountsTowardEveryPolicyThatNamesItsApprover(t *testing.T) {
	agentA := v1alpha1.Approver{ServiceAccount: &v1alpha1.ServiceAccountRef{Namespace: "hpcburst-agents", Name: "agent-a"}}
	target := v1alpha1.PlanTarget{Kind: tAlloc, Name: "x", Provider: "sano", NeedsApproval: true}
	res := func(ps ...v1alpha1.ApprovalPolicy) resolution { return resolvePolicies(opNS, opMD, "sano", ps) }
	byA := []*v1alpha1.Approval{approvalBy(userAgentA, "plan-1", "uid-op", opNS)}
	byPerson := []*v1alpha1.Approval{approvalBy(userPerson, "plan-1", "uid-op", opNS)}
	both := append(slices.Clone(byA), byPerson...)

	// Provider policy names A; the namespace policy names the person: each needs its own.
	two := res(providerPolicy("sano", agentA), namespacePolicy(opNS, person))
	if s := standingOf(target, two, byA); !slices.Equal(s.lacks, []string{"namespace.ns"}) {
		t.Errorf("A alone: lacks %v, want the namespace policy", s.lacks)
	}
	if s := standingOf(target, two, byPerson); !slices.Equal(s.lacks, []string{"provider.sano"}) {
		t.Errorf("the person alone: lacks %v, want the Provider policy", s.lacks)
	}
	if !standingOf(target, two, both).approved() {
		t.Error("both approvals: approved")
	}
	// Both policies name the person: his one Approval counts toward each.
	same := res(providerPolicy("sano", person), namespacePolicy(opNS, person))
	if !standingOf(target, same, byPerson).approved() {
		t.Error("one Approval counts toward every policy that names its approver")
	}
	// No policy: nothing is approvable, whoever approves.
	if s := standingOf(target, res(), both); s.approved() || !s.none {
		t.Errorf("no policy: standing %+v, want none and not approved", s)
	}
}
