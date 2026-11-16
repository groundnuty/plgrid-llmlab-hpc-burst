package lifecycle

// Approval policy resolution and approval counting (HBEP-10, Approval policy; Approvals; #589 plan, 7.3
// and 9). Pure: both read lists the pass handed over, and nothing else.

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// scopeKey names a policy's scope, whatever the policy's own name is: a scope is one of global, a
// Provider, a namespace or a ModelDeployment (HBEP-10, Approval policy), and the controller reads the
// scope field, never the name, so two policies of one scope are found even when one has a hand-made name.
func scopeKey(s v1alpha1.ApprovalScope) string {
	switch {
	case s.Global != nil && *s.Global:
		return "global"
	case s.Provider != "":
		return "provider/" + s.Provider
	case s.Namespace != "":
		return "namespace/" + s.Namespace
	case s.ModelDeployment != nil:
		return "modeldeployment/" + s.ModelDeployment.Namespace + "/" + s.ModelDeployment.Name
	}
	return ""
}

// resolution is the policies one target needs an approval under.
type resolution struct {
	// policies are the policies that apply, one per scope that applies, in a fixed order: the namespace
	// side, the Provider side, then global.
	policies []*v1alpha1.ApprovalPolicy
	// none: no ApprovalPolicy exists that could apply, global included: the target waits.
	none bool
	// duplicate, when set, names two policies of one applying scope: the target is unapprovable until
	// one of them is removed (fail-closed).
	duplicate []string
}

// resolvePolicies resolves, for a target on Provider provider (empty for a ModelDeployment) in namespace
// ns of ModelDeployment md (HBEP-10, Approval policy; the owner, 15 Nov 2026):
//   - the namespace side is the ModelDeployment's policy if it exists, else the namespace's;
//   - the Provider side is the Provider's policy;
//   - where both sides have one, both apply; where only one has, that one applies;
//   - where neither has, the global policy applies;
//   - where none exists at all, the target waits.
func resolvePolicies(ns, md, provider string, all []v1alpha1.ApprovalPolicy) resolution {
	byScope := map[string][]*v1alpha1.ApprovalPolicy{}
	for i := range all {
		k := scopeKey(all[i].Spec.Scope)
		byScope[k] = append(byScope[k], &all[i])
	}
	var r resolution
	take := func(key string) bool {
		ps := byScope[key]
		if len(ps) == 0 {
			return false
		}
		if len(ps) > 1 {
			var names []string
			for _, p := range ps {
				names = append(names, p.Name)
			}
			sort.Strings(names)
			r.duplicate = append(r.duplicate, names...)
		}
		r.policies = append(r.policies, ps...)
		return true
	}
	// The namespace side: the ModelDeployment's own policy overrides its namespace's.
	nsSide := take("modeldeployment/"+ns+"/"+md) || take("namespace/"+ns)
	provSide := provider != "" && take("provider/"+provider)
	if !nsSide && !provSide && !take("global") {
		r.none = true
	}
	return r
}

// appliedPolicies are the policies recorded in a plan target: name, UID, generation and approvers. A
// duplicated scope lists both policies, so a change to either moves the plan's ID.
func appliedPolicies(r resolution) []v1alpha1.AppliedPolicy {
	var out []v1alpha1.AppliedPolicy
	for _, p := range r.policies {
		out = append(out, v1alpha1.AppliedPolicy{
			Name: p.Name, UID: p.UID, Generation: p.Generation,
			Approvers: slices.Clone(p.Spec.Approvers),
		})
	}
	return out
}

// namesApprover reports whether the approver a is the approving identity: the same user, a group among
// the identity's recorded groups, or a ServiceAccount whose user name, system:serviceaccount:<namespace>:
// <name>, is the identity's.
func namesApprover(a v1alpha1.Approver, who v1alpha1.ApproverIdentity) bool {
	switch {
	case a.User != "":
		return a.User == who.Username
	case a.Group != "":
		return slices.Contains(who.Groups, a.Group)
	case a.ServiceAccount != nil:
		return who.Username == "system:serviceaccount:"+a.ServiceAccount.Namespace+":"+a.ServiceAccount.Name
	}
	return false
}

// countsFor reports whether an Approval counts toward the Operation's plan: it is in the Operation's
// namespace, it names the Operation's UID, and it names the frozen plan's ID (HBEP-10, Approvals).
func countsFor(ap *v1alpha1.Approval, opNS string, opUID types.UID, planID string) bool {
	return ap.Namespace == opNS && ap.Spec.Operation.UID == opUID && ap.Spec.PlanID == planID
}

// policyApproved reports whether one of the counted Approvals comes from an approver the policy names.
// One Approval counts toward every policy that names its approver.
func policyApproved(approvers []v1alpha1.Approver, counted []*v1alpha1.Approval) bool {
	for _, ap := range counted {
		for _, a := range approvers {
			if namesApprover(a, ap.Spec.Approver) {
				return true
			}
		}
	}
	return false
}

// A target's standing under the approvals counted so far.
type targetStanding struct {
	// target is "Kind name".
	target string
	// lacks are the names of the policies still without an approval.
	lacks []string
	// none: the target has no policy at all (NoApprovalPolicy).
	none bool
	// duplicate names the two policies of one scope that make the target unapprovable.
	duplicate []string
}

func (s targetStanding) approved() bool { return len(s.lacks) == 0 && !s.none && len(s.duplicate) == 0 }

// standingOf counts the Approvals for one target that needs approval.
func standingOf(t v1alpha1.PlanTarget, r resolution, counted []*v1alpha1.Approval) targetStanding {
	s := targetStanding{target: fmt.Sprintf("%s %s", t.Kind, t.Name), none: r.none, duplicate: r.duplicate}
	for _, p := range r.policies {
		if !policyApproved(p.Spec.Approvers, counted) {
			s.lacks = append(s.lacks, p.Name)
		}
	}
	return s
}

// waitMessage says, for an Operation that waits, which targets still lack an approval and under which
// policy, in a stable order so the same wait writes the same text.
func waitMessage(standings []targetStanding) string {
	var parts []string
	for _, s := range standings {
		switch {
		case s.none:
			parts = append(parts, fmt.Sprintf("%s: no ApprovalPolicy applies", s.target))
		case len(s.duplicate) > 0:
			parts = append(parts, fmt.Sprintf("%s: unapprovable, ApprovalPolicies %s name one scope", s.target, strings.Join(s.duplicate, " and ")))
		case len(s.lacks) > 0:
			parts = append(parts, fmt.Sprintf("%s: needs an approval under %s", s.target, strings.Join(s.lacks, ", ")))
		}
	}
	sort.Strings(parts)
	return strings.Join(parts, "; ")
}
