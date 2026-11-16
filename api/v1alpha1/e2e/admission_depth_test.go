package e2e

// Defence in depth, and the negative control (issue #589, PR 3; the science agent's Q5, plan section 10).
//
// RBAC already denies agents every write to Allocations, Instances and status, and every delete. A refusal
// in those rows would be RBAC's, and would prove nothing about the capacity guard: the guard is the second
// layer, for the day a role is wider than the manifest. So for these rows the test binds ONE agent, A, to
// an extra ClusterRole that grants them, asks the API server (SelfSubjectAccessReview) that RBAC now
// ALLOWS each verb, and only then expects the guard's refusal, by name. It removes the binding afterwards
// and waits until RBAC denies again, so no later case runs with it.
//
// The negative control: with the capacity guard's binding removed, agent A's lower count is accepted. That
// shows the refusals above were the guard's, and not something else's.

import (
	"io"
	"strings"
	"testing"
	"time"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/launcher"
)

// grantExtra binds agent a, and no other identity, to a ClusterRole made of rules, and removes both when
// the test ends, waiting until the API server denies the first row again.
func grantExtra(t *testing.T, a *agent, rules []rbacv1.PolicyRule, gone accessRow) {
	t.Helper()
	name := uniq("hpcburst-test-extra")
	role := &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: name}, Rules: rules}
	binding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: name},
		Subjects:   []rbacv1.Subject{{APIGroup: "rbac.authorization.k8s.io", Kind: "User", Name: a.username()}},
	}
	must(t, c.Create(bg(), role), "create the extra ClusterRole")
	must(t, c.Create(bg(), binding), "bind the extra ClusterRole to "+a.name)
	t.Cleanup(func() {
		for _, o := range []client.Object{binding, role} {
			if err := c.Delete(bg(), o); err != nil && !apierrors.IsNotFound(err) {
				t.Errorf("clean-up: delete %T %s: %v", o, name, err)
			}
		}
		waitUntilDenied(t, a.cs, gone)
	})
}

func TestCapacityGuardRefusesWhatRBACAlreadyDeniesWhenARoleIsWider(t *testing.T) {
	agents := newAgents(t, "agent-a", "agent-b")
	a := agents[0]
	requireEnforced(t)
	ns := newNamespace(t)
	person := personWriter(t)

	md := createMD(t, a, agentMD(a, ns, uniq("md")))
	other := newModelDeployment(ns, uniq("md-other"))
	must(t, c.Create(bg(), other), "the person's second ModelDeployment, for the collection delete")
	alloc, inst := newAllocation(ns, uniq("alloc")), newInstance(ns, uniq("inst"))
	for _, o := range []client.Object{alloc, inst} {
		must(t, c.Create(bg(), o), "create "+o.GetName()+" as the admin")
	}
	op := agentOperation(ns, uniq("op"), a.username())
	must(t, a.c.Create(bg(), op), "A's Operation")
	writePlan(t, op, opPlanID)

	// With the manifest's role, each of these is RBAC's refusal, which names no policy.
	rbacOnly := func(what string, err error) {
		t.Helper()
		if !apierrors.IsForbidden(err) || strings.Contains(err.Error(), "ValidatingAdmissionPolicy") {
			t.Errorf("%s with the manifest's role: %v, want RBAC's refusal, which names no policy", what, err)
		}
	}
	requireAgentIdentity(t, a)
	rbacOnly("A's delete of a ModelDeployment", a.c.Delete(bg(), md.DeepCopy()))
	rbacOnly("A's delete of the Allocation", a.c.Delete(bg(), alloc.DeepCopy()))
	rbacOnly("A's write of an Operation's status", a.c.Status().Update(bg(), op.DeepCopy()))

	const G = frameworkGroup
	rows := []accessRow{
		{G, "modeldeployments", "", ns, "delete", true}, {G, "modeldeployments", "", ns, "deletecollection", true},
		{G, "modeldeployments", "status", ns, "update", true},
		{G, "allocations", "", ns, "create", true}, {G, "allocations", "", ns, "update", true}, {G, "allocations", "", ns, "delete", true},
		{G, "allocations", "status", ns, "update", true},
		{G, "instances", "", ns, "create", true}, {G, "instances", "", ns, "update", true}, {G, "instances", "", ns, "delete", true},
		{G, "instances", "status", ns, "update", true},
		{G, "operations", "status", ns, "update", true},
	}
	var rules []rbacv1.PolicyRule
	for _, r := range rows {
		resource := r.resource
		if r.subresource != "" {
			resource += "/" + r.subresource
		}
		rules = append(rules, rbacv1.PolicyRule{APIGroups: []string{G}, Resources: []string{resource}, Verbs: []string{r.verb}})
	}
	grantExtra(t, a, rules, accessRow{G, "allocations", "", ns, "delete", false})
	// RBAC now ALLOWS each: so the refusals below can only be the guard's.
	for _, r := range rows {
		waitUntilAllowed(t, a.cs, r)
	}

	const writes = "agents never write Allocations, Instances or an Operation's status"
	setLabel := func(o client.Object) {
		o.SetLabels(map[string]string{"example": "label"})
	}
	updateAs := func(w writer, o client.Object) error {
		mine := o.DeepCopyObject().(client.Object)
		if err := w.c.Get(bg(), client.ObjectKeyFromObject(o), mine); err != nil {
			return err
		}
		setLabel(mine)
		if w.dry {
			return w.c.Update(bg(), mine, client.DryRunAll)
		}
		return w.c.Update(bg(), mine)
	}
	statusAs := func(w writer, o client.Object) error {
		mine := o.DeepCopyObject().(client.Object)
		if err := w.c.Get(bg(), client.ObjectKeyFromObject(o), mine); err != nil {
			return err
		}
		setLabel(mine)
		if w.dry {
			return w.c.Status().Update(bg(), mine, client.DryRunAll)
		}
		return w.c.Status().Update(bg(), mine)
	}
	deleteAs := func(w writer, o client.Object) error {
		if w.dry {
			return w.c.Delete(bg(), o.DeepCopyObject().(client.Object), client.DryRunAll)
		}
		return w.c.Delete(bg(), o.DeepCopyObject().(client.Object))
	}
	createAs := func(w writer, o client.Object) error {
		o = o.DeepCopyObject().(client.Object)
		o.SetResourceVersion("")
		if w.dry {
			return w.c.Create(bg(), o, client.DryRunAll)
		}
		return w.c.Create(bg(), o)
	}
	renamed := func(o client.Object) client.Object {
		o = o.DeepCopyObject().(client.Object)
		o.SetName(uniq("new"))
		return o
	}
	collection := func(w writer, _ client.Object) error {
		if w.dry {
			return w.c.DeleteAllOf(bg(), &v1alpha1.ModelDeployment{}, client.InNamespace(ns), client.DryRunAll)
		}
		return w.c.DeleteAllOf(bg(), &v1alpha1.ModelDeployment{}, client.InNamespace(ns))
	}

	for _, tc := range []struct {
		name string
		obj  client.Object
		do   func(w writer, o client.Object) error
		msg  string
	}{
		{"an Allocation created", renamed(alloc), createAs, writes},
		{"an Allocation updated", alloc, updateAs, writes},
		{"an Allocation's status written", alloc, statusAs, writes},
		{"an Allocation deleted", alloc, deleteAs, writes},
		{"an Instance created", renamed(inst), createAs, writes},
		{"an Instance updated", inst, updateAs, writes},
		{"an Instance's status written", inst, statusAs, writes},
		{"an Instance deleted", inst, deleteAs, writes},
		{"an Operation's status written", op, func(w writer, o client.Object) error {
			cur := &v1alpha1.Operation{}
			if err := w.c.Get(bg(), client.ObjectKeyFromObject(o), cur); err != nil {
				return err
			}
			cur.Status.Phase = v1alpha1.OperationPhaseExecuting
			if w.dry {
				return w.c.Status().Update(bg(), cur, client.DryRunAll)
			}
			return w.c.Status().Update(bg(), cur)
		}, writes},
		{"a ModelDeployment's status written", md, func(w writer, o client.Object) error {
			cur := &v1alpha1.ModelDeployment{}
			if err := w.c.Get(bg(), client.ObjectKeyFromObject(o), cur); err != nil {
				return err
			}
			cur.Status.Instances = 7
			if w.dry {
				return w.c.Status().Update(bg(), cur, client.DryRunAll)
			}
			return w.c.Status().Update(bg(), cur)
		}, "agents never write a ModelDeployment's status"},
		{"a ModelDeployment deleted", md, deleteAs, "cannot delete a ModelDeployment"},
		{"ModelDeployments deleted by a collection delete", md, collection, "cannot delete a ModelDeployment"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requireAgentIdentity(t, a)
			err := tc.do(agentWriter(a), tc.obj)
			requireDenied(t, "A", err, capacityGuard, tc.msg)
			// The person's same write passes (a dry run): the guard's match condition is the agents' group.
			requireAccepted(t, "the person's same write (a dry run)", tc.do(person, tc.obj))
		})
	}
	// Nothing was deleted or written: both ModelDeployments, the Allocation and the Instance are as they were.
	for _, o := range []client.Object{md, other, alloc, inst} {
		got := o.DeepCopyObject().(client.Object)
		must(t, c.Get(bg(), client.ObjectKeyFromObject(o), got), "read "+o.GetName()+" after the refused writes")
		if got.GetDeletionTimestamp() != nil || got.GetResourceVersion() != o.GetResourceVersion() {
			t.Errorf("%s changed under refused writes: deletionTimestamp %v, resourceVersion %s then %s", o.GetName(), got.GetDeletionTimestamp(), o.GetResourceVersion(), got.GetResourceVersion())
		}
	}
	countMDs := func() int {
		var l v1alpha1.ModelDeploymentList
		must(t, c.List(bg(), &l, client.InNamespace(ns)), "list ModelDeployments")
		return len(l.Items)
	}
	if n := countMDs(); n != 2 {
		t.Errorf("%d ModelDeployments remain after the refused deletes, want 2", n)
	}
}

// With the capacity guard's binding removed, A's lower count is accepted: the refusals were the guard's.
// The binding is restored through the launcher, and the guard is shown to refuse again.
func TestWithoutTheCapacityGuardsBindingALowerCountIsAccepted(t *testing.T) {
	a := newAgents(t, "agent-a", "agent-b")[0]
	requireEnforced(t)
	ns := newNamespace(t)
	md := createMD(t, a, agentMD(a, ns, uniq("md")))
	lower := func(n int32) error {
		requireAgentIdentity(t, a)
		return agentWriter(a).update(md, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = n })
	}

	requireDenied(t, "A's lower count with the guard bound", lower(2), capacityGuard, "cannot lower spec.instances")

	restore := func() {
		must(t, launcher.InstallAdmission(bg(), cfg, io.Discard), "restore the policies")
		eventually(t, 60*time.Second, "the capacity guard to refuse its probe again", func() (bool, error) {
			refused, _ := launcher.ProbeAdmission(bg(), cfg, capacityGuard)
			return refused, nil
		})
	}
	removed := false
	t.Cleanup(func() {
		if removed {
			restore()
		}
	})
	must(t, c.Delete(bg(), &admissionregistrationv1.ValidatingAdmissionPolicyBinding{ObjectMeta: metav1.ObjectMeta{Name: capacityGuard}}), "remove the capacity guard's binding")
	removed = true
	waitPolicyNotEnforced(t, capacityGuard)
	// The same write that was refused is accepted now, and stored.
	requireAccepted(t, "A's lower count with the guard's binding removed", lower(2))
	if got := storedMD(t, md); got.Spec.Instances != 2 {
		t.Errorf("spec.instances = %d after the accepted write, want 2", got.Spec.Instances)
	}
	restore()
	removed = false
	requireDenied(t, "A's lower count once the binding is back", lower(1), capacityGuard, "cannot lower spec.instances")
	if got := storedMD(t, md); got.Spec.Instances != 2 {
		t.Errorf("spec.instances = %d after the refused write, want 2", got.Spec.Instances)
	}
}
