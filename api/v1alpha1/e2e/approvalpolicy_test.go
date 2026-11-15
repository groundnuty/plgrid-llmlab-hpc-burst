package e2e

import (
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// scopeName is the policy name HBEP-10 gives a scope: its kind, then the names
// the scope holds, joined by dots. Every test below names its policies with it,
// so what they create is a policy that is correctly named whether or not the CRD
// checks the name.
func scopeName(s v1alpha1.ApprovalScope) string {
	switch {
	case s.Global != nil:
		return "global"
	case s.Provider != "":
		return "provider." + s.Provider
	case s.Namespace != "":
		return "namespace." + s.Namespace
	case s.ModelDeployment != nil:
		return "modeldeployment." + s.ModelDeployment.Namespace + "." + s.ModelDeployment.Name
	}
	return "no-scope"
}

// Fixtures: example scopes and approvers.
func providerScope() v1alpha1.ApprovalScope {
	return v1alpha1.ApprovalScope{Provider: "example-provider"}
}
func namespaceScope() v1alpha1.ApprovalScope {
	return v1alpha1.ApprovalScope{Namespace: "example-team"}
}
func globalScope() v1alpha1.ApprovalScope { return v1alpha1.ApprovalScope{Global: ptr.To(true)} }
func deploymentScope() v1alpha1.ApprovalScope {
	return v1alpha1.ApprovalScope{ModelDeployment: &v1alpha1.ScopedDeployment{Namespace: "example-team", Name: "example-md"}}
}

func userApprover() v1alpha1.Approver  { return v1alpha1.Approver{User: "example-person"} }
func groupApprover() v1alpha1.Approver { return v1alpha1.Approver{Group: "example-group"} }
func accountApprover() v1alpha1.Approver {
	return v1alpha1.Approver{ServiceAccount: &v1alpha1.ServiceAccountRef{Namespace: "example-agents", Name: "example-agent"}}
}

// newApprovalPolicy returns a policy for scope with one user approver, named by
// scopeName.
func newApprovalPolicy(scope v1alpha1.ApprovalScope, approvers ...v1alpha1.Approver) *v1alpha1.ApprovalPolicy {
	if len(approvers) == 0 {
		approvers = []v1alpha1.Approver{userApprover()}
	}
	return &v1alpha1.ApprovalPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: scopeName(scope)},
		Spec:       v1alpha1.ApprovalPolicySpec{Scope: scope, Approvers: approvers},
	}
}

// policyAdmits asks the API server's admission to accept or refuse the policy,
// as a dry run: nothing is stored, so no test leaves a policy behind or meets one
// that another test or the host created. A dry-run create of a name that exists
// answers AlreadyExists, which comes after validation, so it means the policy
// passed every rule, and the helper reports it as admitted.
func policyAdmits(obj *v1alpha1.ApprovalPolicy) error {
	err := c.Create(bg(), obj, client.DryRunAll)
	if apierrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

type policyCase struct {
	name   string
	policy *v1alpha1.ApprovalPolicy
	want   string // refusals only: the rule's message
}

func runPolicyCases(t *testing.T, refused, accepted []policyCase) {
	t.Helper()
	for _, tc := range refused {
		t.Run("refuse "+tc.name, func(t *testing.T) {
			requireRefused(t, policyAdmits(tc.policy), tc.want)
		})
	}
	for _, tc := range accepted {
		t.Run("accept "+tc.name, func(t *testing.T) {
			must(t, policyAdmits(tc.policy), "create ApprovalPolicy")
		})
	}
}

// A policy names exactly one scope. This is the rule
// approvalpolicies:$.spec.scope#0.
func TestApprovalPolicyNamesExactlyOneScope(t *testing.T) {
	const msg = "a scope names exactly one of global, provider, namespace and modelDeployment"
	// Two scopes at once: each pair, and all four. The name of such a policy is
	// the first scope's, which is as good as any.
	twoScopes := func(a, b v1alpha1.ApprovalScope) v1alpha1.ApprovalScope {
		out := a
		if b.Global != nil {
			out.Global = b.Global
		}
		if b.Provider != "" {
			out.Provider = b.Provider
		}
		if b.Namespace != "" {
			out.Namespace = b.Namespace
		}
		if b.ModelDeployment != nil {
			out.ModelDeployment = b.ModelDeployment
		}
		return out
	}
	all := twoScopes(twoScopes(globalScope(), providerScope()), twoScopes(namespaceScope(), deploymentScope()))
	policyOf := func(s v1alpha1.ApprovalScope) *v1alpha1.ApprovalPolicy {
		p := newApprovalPolicy(s)
		p.Name = "example-policy"
		return p
	}
	runPolicyCases(t, []policyCase{
		{"no scope", policyOf(v1alpha1.ApprovalScope{}), msg},
		{"global and a provider", policyOf(twoScopes(globalScope(), providerScope())), msg},
		{"global and a namespace", policyOf(twoScopes(globalScope(), namespaceScope())), msg},
		{"global and a deployment", policyOf(twoScopes(globalScope(), deploymentScope())), msg},
		{"a provider and a namespace", policyOf(twoScopes(providerScope(), namespaceScope())), msg},
		{"a provider and a deployment", policyOf(twoScopes(providerScope(), deploymentScope())), msg},
		{"a namespace and a deployment", policyOf(twoScopes(namespaceScope(), deploymentScope())), msg},
		{"all four", policyOf(all), msg},
	}, []policyCase{
		{"global", newApprovalPolicy(globalScope()), ""},
		{"a provider", newApprovalPolicy(providerScope()), ""},
		{"a namespace", newApprovalPolicy(namespaceScope()), ""},
		{"a deployment", newApprovalPolicy(deploymentScope()), ""},
	})
}

// global, when set, is true. This is the rule
// approvalpolicies:$.spec.scope.global#0. A policy of global: false would name a
// scope that applies to nothing, and counts as one scope in the rule above, so
// only this rule refuses it.
func TestApprovalPolicyGlobalIsTrueWhenSet(t *testing.T) {
	const msg = "global is true when it is set"
	p := newApprovalPolicy(globalScope())
	p.Spec.Scope.Global = ptr.To(false)
	runPolicyCases(t, []policyCase{
		{"global false", p, msg},
	}, []policyCase{
		{"global true", newApprovalPolicy(globalScope()), ""},
		{"a provider and no global", newApprovalPolicy(providerScope()), ""},
	})
}

// Each approver names exactly one of a user, a group and a ServiceAccount. This
// is the rule approvalpolicies:$.spec.approvers[]#0. The other cases are the
// schema's bounds on the list and its strings.
func TestApprovalPolicyApproversNameExactlyOneKind(t *testing.T) {
	const msg = "an approver names exactly one of user, group and serviceAccount"
	account := accountApprover().ServiceAccount
	many := func(n int) []v1alpha1.Approver {
		out := make([]v1alpha1.Approver, n)
		for i := range out {
			out[i] = v1alpha1.Approver{User: "example-person-" + strings.Repeat("a", i+1)}
		}
		return out
	}
	runPolicyCases(t, []policyCase{
		{"an approver with a user and a group", newApprovalPolicy(globalScope(), v1alpha1.Approver{User: "example-person", Group: "example-group"}), msg},
		{"an approver with a user and a ServiceAccount", newApprovalPolicy(globalScope(), v1alpha1.Approver{User: "example-person", ServiceAccount: account}), msg},
		{"an approver with a group and a ServiceAccount", newApprovalPolicy(globalScope(), v1alpha1.Approver{Group: "example-group", ServiceAccount: account}), msg},
		{"an approver with all three", newApprovalPolicy(globalScope(), v1alpha1.Approver{User: "example-person", Group: "example-group", ServiceAccount: account}), msg},
		{"an approver with none", newApprovalPolicy(globalScope(), v1alpha1.Approver{}), msg},
		{"a good approver beside a bad one", newApprovalPolicy(globalScope(), userApprover(), v1alpha1.Approver{User: "example-person", Group: "example-group"}), msg},
		{"no approvers", withApprovers(newApprovalPolicy(globalScope()), []v1alpha1.Approver{}), "spec.approvers"},
		{"65 approvers", newApprovalPolicy(globalScope(), many(65)...), "spec.approvers"},
		{"an empty user name", newApprovalPolicy(globalScope(), v1alpha1.Approver{User: ""}), "spec.approvers[0]"},
		{"a group of 1025 characters", newApprovalPolicy(globalScope(), v1alpha1.Approver{Group: strings.Repeat("a", 1025)}), "spec.approvers[0].group"},
		{"a ServiceAccount with no namespace", newApprovalPolicy(globalScope(), v1alpha1.Approver{ServiceAccount: &v1alpha1.ServiceAccountRef{Name: "example-agent"}}),
			"spec.approvers[0].serviceAccount.namespace"},
		{"a ServiceAccount with no name", newApprovalPolicy(globalScope(), v1alpha1.Approver{ServiceAccount: &v1alpha1.ServiceAccountRef{Namespace: "example-agents"}}),
			"spec.approvers[0].serviceAccount.name"},
	}, []policyCase{
		{"a user", newApprovalPolicy(globalScope(), userApprover()), ""},
		{"a group", newApprovalPolicy(globalScope(), groupApprover()), ""},
		{"a ServiceAccount", newApprovalPolicy(globalScope(), accountApprover()), ""},
		{"a mix of the three kinds", newApprovalPolicy(globalScope(), userApprover(), groupApprover(), accountApprover()), ""},
		{"64 approvers", newApprovalPolicy(globalScope(), many(64)...), ""},
	})
}

// The schema's bounds on a scope's names.
func TestApprovalPolicyScopeNamesAreBounded(t *testing.T) {
	runPolicyCases(t, []policyCase{
		{"a provider of 64 characters", withPolicyName(newApprovalPolicy(v1alpha1.ApprovalScope{Provider: strings.Repeat("a", 64)}), "example-policy"), "spec.scope.provider"},
		{"a namespace of 64 characters", withPolicyName(newApprovalPolicy(v1alpha1.ApprovalScope{Namespace: strings.Repeat("a", 64)}), "example-policy"), "spec.scope.namespace"},
		{"a deployment with no name", withPolicyName(newApprovalPolicy(v1alpha1.ApprovalScope{ModelDeployment: &v1alpha1.ScopedDeployment{Namespace: "example-team"}}), "example-policy"),
			"spec.scope.modelDeployment.name"},
		{"a deployment with no namespace", withPolicyName(newApprovalPolicy(v1alpha1.ApprovalScope{ModelDeployment: &v1alpha1.ScopedDeployment{Name: "example-md"}}), "example-policy"),
			"spec.scope.modelDeployment.namespace"},
		{"a deployment of a namespace of 64 characters", withPolicyName(newApprovalPolicy(v1alpha1.ApprovalScope{ModelDeployment: &v1alpha1.ScopedDeployment{
			Namespace: strings.Repeat("a", 64), Name: "example-md"}}), "example-policy"), "spec.scope.modelDeployment.namespace"},
	}, []policyCase{
		{"a provider of 63 characters", newApprovalPolicy(v1alpha1.ApprovalScope{Provider: strings.Repeat("a", 63)}), ""},
		{"a namespace of 63 characters", newApprovalPolicy(v1alpha1.ApprovalScope{Namespace: strings.Repeat("a", 63)}), ""},
	})
}

// withPolicyName returns the policy under another name. The name of a policy
// whose scope is out of bounds cannot be derived, so these cases name it.
func withPolicyName(p *v1alpha1.ApprovalPolicy, name string) *v1alpha1.ApprovalPolicy {
	p.Name = name
	return p
}

// withApprovers returns the policy with exactly these approvers, which may be
// none: newApprovalPolicy gives a policy with no approvers argument one user.
func withApprovers(p *v1alpha1.ApprovalPolicy, approvers []v1alpha1.Approver) *v1alpha1.ApprovalPolicy {
	p.Spec.Approvers = approvers
	return p
}

// The name of a policy derives from its scope: `global`, `provider.<p>`,
// `namespace.<n>` or `modeldeployment.<n>.<m>`. This is the rule
// approvalpolicies:$#0. A name that is not the scope's is refused with a message
// that says which name the scope needs; the right one is accepted.
func TestApprovalPolicyNameDerivesFromItsScope(t *testing.T) {
	named := func(scope v1alpha1.ApprovalScope, name string) *v1alpha1.ApprovalPolicy {
		return withPolicyName(newApprovalPolicy(scope), name)
	}
	const need = "this scope needs the name "
	runPolicyCases(t, []policyCase{
		{"global, under another name", named(globalScope(), "example-policy"), need + "global (HBEP-10"},
		{"global, under the Provider form", named(globalScope(), "provider.global"), need + "global (HBEP-10"},
		{"a provider, under its bare name", named(providerScope(), "example-provider"), need + "provider.example-provider (HBEP-10"},
		{"a provider, under another provider's name", named(providerScope(), "provider.another-provider"), need + "provider.example-provider (HBEP-10"},
		{"a provider, under the namespace form", named(providerScope(), "namespace.example-provider"), need + "provider.example-provider (HBEP-10"},
		{"a namespace, under its bare name", named(namespaceScope(), "example-team"), need + "namespace.example-team (HBEP-10"},
		{"a namespace, under the Provider form", named(namespaceScope(), "provider.example-team"), need + "namespace.example-team (HBEP-10"},
		{"a namespace, under another namespace's name", named(namespaceScope(), "namespace.another-team"), need + "namespace.example-team (HBEP-10"},
		{"a deployment, under its namespace's name", named(deploymentScope(), "namespace.example-team"), need + "modeldeployment.example-team.example-md (HBEP-10"},
		{"a deployment, with the name left out", named(deploymentScope(), "modeldeployment.example-team"), need + "modeldeployment.example-team.example-md (HBEP-10"},
		{"a deployment, with namespace and name swapped", named(deploymentScope(), "modeldeployment.example-md.example-team"), need + "modeldeployment.example-team.example-md (HBEP-10"},
		{"a deployment, with a dash for the dot", named(deploymentScope(), "modeldeployment.example-team-example-md"), need + "modeldeployment.example-team.example-md (HBEP-10"},
	}, []policyCase{
		{"global, named global", named(globalScope(), "global"), ""},
		{"a provider, named provider.<p>", named(providerScope(), "provider.example-provider"), ""},
		{"a namespace, named namespace.<n>", named(namespaceScope(), "namespace.example-team"), ""},
		{"a deployment, named modeldeployment.<n>.<m>", named(deploymentScope(), "modeldeployment.example-team.example-md"), ""},
		{"a deployment whose name holds a dot", named(v1alpha1.ApprovalScope{ModelDeployment: &v1alpha1.ScopedDeployment{
			Namespace: "example-team", Name: "example.md"}}, "modeldeployment.example-team.example.md"), ""},
	})
}

// A ModelDeployment scope whose policy name would pass 253 characters, the most
// a Kubernetes name holds, is refused with a message that says so: no policy can
// be named for it, so the scope cannot exist. This is the rule
// approvalpolicies:$#1. A name of exactly 253 characters is accepted.
func TestApprovalPolicyOfAScopeWithATooLongNameIsRefused(t *testing.T) {
	const msg = "would need a policy name above the 253 characters"
	// "modeldeployment." is 16 characters, "." is one more: with a one-character
	// namespace the deployment's name has 253 - 18 = 235 characters at the limit.
	scopeOf := func(nameLen int) v1alpha1.ApprovalScope {
		return v1alpha1.ApprovalScope{ModelDeployment: &v1alpha1.ScopedDeployment{Namespace: "a", Name: strings.Repeat("b", nameLen)}}
	}
	atLimit := newApprovalPolicy(scopeOf(235))
	if len(atLimit.Name) != 253 {
		t.Fatalf("the fixture's policy name has %d characters, want 253", len(atLimit.Name))
	}
	over := withPolicyName(newApprovalPolicy(scopeOf(236)), "example-policy")
	runPolicyCases(t, []policyCase{
		{"a name that would have 254 characters", over, msg},
	}, []policyCase{
		{"a name of exactly 253 characters", atLimit, ""},
	})
}
