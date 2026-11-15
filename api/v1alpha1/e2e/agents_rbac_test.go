package e2e

// The agents' RBAC (issue #589, PR 2; HBEP-10, Identities and roles), judged by the API server's own
// decisions for each AGENT identity: its SelfSubjectAccessReview from its own token, and real requests
// it makes. A test run as the admin would pass every row, so each judgement first asks the API server
// who the credential is, and the admin and an outsider are controls beside the agents, never the
// subject. The matrix is written by hand (agents_helpers_test.go).

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// Two agents, A and B, get the same decisions on every row of the matrix. The admin gets every row
// allowed. A ServiceAccount of another namespace, which is outside the agents' group, gets none of the
// agent role: the excluded case, which a binding per ServiceAccount would get wrong.
func TestAgentRolesMatrix(t *testing.T) {
	agents := newAgents(t, "agent-a", "agent-b")
	rows := expectedAgentMatrix()
	if len(rows) < 250 {
		t.Fatalf("the matrix has only %d rows", len(rows))
	}
	for _, a := range agents {
		requireAgentIdentity(t, a)
	}
	// The gate: every allowed row is allowed, so both roles and their bindings are visible to the
	// authorizer. After it, a denied row is judged for what the roles hold, not for what the
	// authorizer has not read yet.
	waitForAgentMatrix(t, agents[0].cs, rows)
	for _, a := range agents {
		waitForAgentMatrix(t, a.cs, rows)
		judge(t, "agent "+a.name, a.cs, rows, func(r accessRow) bool { return r.agentMay })
	}

	// Control: the admin passes every row, which shows the rows can be answered yes.
	adminCS, _ := clientsFromConfig(t, cfg)
	judge(t, "admin", adminCS, rows, func(accessRow) bool { return true })

	// The excluded case: a ServiceAccount in another namespace is not in the agents' group, and gets
	// nothing of the agent role. A binding per ServiceAccount would not give it; the one to the group
	// must not either.
	ns := newNamespace(t)
	outCS, outC := tokenClient(t, ns, "outsider")
	u := whoAmI(t, outCS)
	if u.Username != "system:serviceaccount:"+ns+":outsider" || slices.Contains(u.Groups, agentsGroup) || !slices.Contains(u.Groups, "system:serviceaccounts:"+ns) {
		t.Fatalf("the control authenticates as %q in %v, want an outsider of %s", u.Username, u.Groups, agentsGroup)
	}
	judge(t, "outsider "+ns, outCS, rows, func(accessRow) bool { return false })
	// A real read beside the access reviews: an agent lists ModelDeployments, and the outsider may not.
	var mds v1alpha1.ModelDeploymentList
	must(t, agents[0].c.List(bg(), &mds, client.InNamespace(ns)), "an agent's list of ModelDeployments")
	if err := outC.List(bg(), &mds, client.InNamespace(ns)); !apierrors.IsForbidden(err) {
		t.Errorf("an outsider's list of ModelDeployments: %v, want Forbidden, while an agent's is allowed", err)
	}
}

// Real requests of an agent, each beside the same request of the admin, which succeeds (as a dry run),
// so a refusal is the agent's role and not a broken object.
func TestAgentRealRequests(t *testing.T) {
	agents := newAgents(t, "agent-a", "agent-b")
	a, b := agents[0], agents[1]
	requireAgentIdentity(t, a)
	requireAgentIdentity(t, b)
	waitForAgentMatrix(t, a.cs, expectedAgentMatrix())
	adminCS, _ := clientsFromConfig(t, cfg)

	ns := newNamespace(t)
	md := newModelDeployment(ns, "md-a")
	alloc := newAllocation(ns, "alloc-a")
	inst := newInstance(ns, "inst-a")
	for _, o := range []client.Object{md, alloc, inst} {
		must(t, c.Create(bg(), o), "create "+o.GetName()+" as the admin")
	}

	// forbidden requires an RBAC refusal: Forbidden, naming this agent, the verb and the resource.
	forbidden := func(what string, err error, verb, resource string) {
		t.Helper()
		want := fmt.Sprintf(`User %q cannot %s resource %q`, a.username(), verb, resource)
		if !apierrors.IsForbidden(err) || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: %v, want a Forbidden RBAC refusal containing %s", what, err, want)
		}
	}
	// unchanged requires that the object is still there, as it was: the refusal did nothing.
	unchanged := func(o client.Object) {
		t.Helper()
		before := o.GetResourceVersion()
		must(t, c.Get(bg(), objKey(o), o), "read "+o.GetName()+" as the admin")
		if o.GetDeletionTimestamp() != nil || (before != "" && o.GetResourceVersion() != before) {
			t.Errorf("%s changed after a refused request: deletionTimestamp %v, resourceVersion %s then %s", o.GetName(), o.GetDeletionTimestamp(), before, o.GetResourceVersion())
		}
	}

	// An agent reads what it may.
	for _, r := range []struct {
		name string
		obj  client.Object
	}{{"md-a", &v1alpha1.ModelDeployment{}}, {"alloc-a", &v1alpha1.Allocation{}}, {"inst-a", &v1alpha1.Instance{}}} {
		must(t, a.c.Get(bg(), client.ObjectKey{Namespace: ns, Name: r.name}, r.obj), "an agent's read of "+r.name)
	}
	var mds v1alpha1.ModelDeploymentList
	must(t, a.c.List(bg(), &mds, client.InNamespace(ns)), "an agent's list of ModelDeployments")
	if len(mds.Items) != 1 {
		t.Errorf("an agent lists %d ModelDeployments, want 1", len(mds.Items))
	}

	// Update and delete of an Allocation and an Instance: Forbidden, and each is untouched.
	for _, tc := range []struct {
		obj      client.Object
		resource string
	}{{alloc, "allocations"}, {inst, "instances"}} {
		mine := tc.obj.DeepCopyObject().(client.Object)
		must(t, a.c.Get(bg(), objKey(tc.obj), mine), "read "+tc.obj.GetName()+" as an agent")
		mine.SetLabels(map[string]string{"changed-by": "agent"})
		forbidden("update of "+tc.resource, a.c.Update(bg(), mine), "update", tc.resource)
		forbidden("delete of "+tc.resource, a.c.Delete(bg(), tc.obj.DeepCopyObject().(client.Object)), "delete", tc.resource)
		unchanged(tc.obj)
		// The same two requests, as the admin, as dry runs: they pass.
		adminMine := tc.obj.DeepCopyObject().(client.Object)
		must(t, c.Get(bg(), objKey(tc.obj), adminMine), "read as the admin")
		adminMine.SetLabels(map[string]string{"changed-by": "admin"})
		must(t, c.Update(bg(), adminMine, client.DryRunAll), "the admin's dry-run update of "+tc.resource)
		must(t, c.Delete(bg(), tc.obj.DeepCopyObject().(client.Object), client.DryRunAll), "the admin's dry-run delete of "+tc.resource)
	}

	// A ModelDeployment: delete, update, patch and create are all Forbidden in this PR. The agent role is
	// read-only until PR 3's admission policies bound each write (the science agent's check of #595: a
	// create with an owner reference to a short-lived object is a garbage-collector release outside every
	// guard).
	forbidden("delete of a ModelDeployment", a.c.Delete(bg(), md.DeepCopy()), "delete", "modeldeployments")
	mine := md.DeepCopy()
	must(t, a.c.Get(bg(), objKey(md), mine), "read the ModelDeployment as an agent")
	mine.Spec.Instances = 1
	forbidden("update of a ModelDeployment", a.c.Update(bg(), mine), "update", "modeldeployments")
	forbidden("patch of a ModelDeployment", a.c.Patch(bg(), md.DeepCopy(), client.RawPatch(types.MergePatchType, []byte(`{"spec":{"instances":1}}`))), "patch", "modeldeployments")
	unchanged(md)
	adminMD := md.DeepCopy()
	must(t, c.Get(bg(), objKey(md), adminMD), "read the ModelDeployment as the admin")
	adminMD.Spec.Instances = 1
	must(t, c.Update(bg(), adminMD, client.DryRunAll), "the admin's dry-run update of a ModelDeployment")
	must(t, c.Delete(bg(), md.DeepCopy(), client.DryRunAll), "the admin's dry-run delete of a ModelDeployment")

	// A real create, not a dry run: if the role let it through, the object would exist.
	fresh := newModelDeployment(ns, "md-created-by-a")
	forbidden("create of a ModelDeployment", a.c.Create(bg(), fresh), "create", "modeldeployments")
	if err := c.Get(bg(), objKey(fresh), &v1alpha1.ModelDeployment{}); !apierrors.IsNotFound(err) {
		t.Errorf("after the refused create the ModelDeployment exists: %v", err)
	}
	// The admin's same create passes (a dry run), so the refusal is the agent's role.
	must(t, c.Create(bg(), newModelDeployment(ns, "md-created-by-a"), client.DryRunAll), "the admin's dry-run create of a ModelDeployment")

	// An agent mints no token: not for another agent's ServiceAccount, not for its own.
	for _, target := range []string{b.name, a.name} {
		_, err := a.cs.CoreV1().ServiceAccounts(agentsNamespace).CreateToken(bg(), target, tokenRequest(3600), metav1.CreateOptions{})
		if !apierrors.IsForbidden(err) || !strings.Contains(err.Error(), fmt.Sprintf(`User %q cannot create resource "serviceaccounts/token"`, a.username())) {
			t.Errorf("an agent's token request for %s: %v, want a Forbidden RBAC refusal", target, err)
		}
	}
	if _, err := adminCS.CoreV1().ServiceAccounts(agentsNamespace).CreateToken(bg(), b.name, tokenRequest(3600), metav1.CreateOptions{}); err != nil {
		t.Errorf("the admin's token request for %s: %v, want it allowed", b.name, err)
	}
	if _, err := a.cs.CoreV1().ServiceAccounts(agentsNamespace).Create(bg(), serviceAccount("made-by-an-agent"), metav1.CreateOptions{}); !apierrors.IsForbidden(err) {
		t.Errorf("an agent's create of a ServiceAccount: %v, want Forbidden", err)
	}

	// A namespace: the agent's delete is Forbidden, and the namespace is not terminating.
	victim := newNamespace(t)
	err := a.cs.CoreV1().Namespaces().Delete(bg(), victim, metav1.DeleteOptions{})
	forbidden("delete of a namespace", err, "delete", "namespaces")
	got, gerr := adminCS.CoreV1().Namespaces().Get(bg(), victim, metav1.GetOptions{})
	if gerr != nil || got.DeletionTimestamp != nil {
		t.Errorf("after the refused delete, namespace %s: %v, deletionTimestamp %v", victim, gerr, got.DeletionTimestamp)
	}
	must(t, adminCS.CoreV1().Namespaces().Delete(bg(), victim, metav1.DeleteOptions{DryRun: []string{metav1.DryRunAll}}), "the admin's dry-run delete of a namespace")
}

// The binding is to the group and to nothing else: after two agents exist, no ClusterRoleBinding or
// RoleBinding that names an hpcburst role has a ServiceAccount, a user or any other group as a subject,
// the two agent roles are bound to the agents' group alone, and the operator role to no one. Read from
// the cluster, with what the issue says written here.
func TestAgentRolesAreBoundToTheGroupAndNoOneElse(t *testing.T) {
	newAgents(t, "agent-a", "agent-b")
	wantSubjects := []rbacv1.Subject{{APIGroup: "rbac.authorization.k8s.io", Kind: "Group", Name: agentsGroup}}

	var crbs rbacv1.ClusterRoleBindingList
	must(t, c.List(bg(), &crbs), "list ClusterRoleBindings")
	var rbs rbacv1.RoleBindingList
	must(t, c.List(bg(), &rbs), "list RoleBindings")
	type binding struct {
		kind, name string
		ref        rbacv1.RoleRef
		subjects   []rbacv1.Subject
	}
	var all []binding
	for _, b := range crbs.Items {
		all = append(all, binding{"ClusterRoleBinding", b.Name, b.RoleRef, b.Subjects})
	}
	for _, b := range rbs.Items {
		all = append(all, binding{"RoleBinding " + b.Namespace + "/", b.Name, b.RoleRef, b.Subjects})
	}
	bound := map[string][]string{}
	for _, b := range all {
		if !strings.HasPrefix(b.ref.Name, "hpcburst-") {
			continue
		}
		bound[b.ref.Name] = append(bound[b.ref.Name], b.kind+" "+b.name)
		if !slices.Equal(b.subjects, wantSubjects) {
			t.Errorf("%s %s binds %s to %v, want only the group %s", b.kind, b.name, b.ref.Name, b.subjects, agentsGroup)
		}
	}
	for role, who := range map[string][]string{
		"hpcburst-agent-read": {"ClusterRoleBinding hpcburst-agent-read"},
		"hpcburst-agent":      {"ClusterRoleBinding hpcburst-agent"},
		"hpcburst-operator":   nil,
	} {
		if !slices.Equal(bound[role], who) {
			t.Errorf("%s is bound by %v, want %v", role, bound[role], who)
		}
	}
}
