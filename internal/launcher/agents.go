package launcher

// The agents' namespace, roles and bindings (HBEP-10, Identities and roles; issue #589, PR 2).
// config/rbac/agents.yaml holds the namespace hpcburst-agents, the ClusterRoles hpcburst-agent-read,
// hpcburst-agent and hpcburst-operator, and the two ClusterRoleBindings that give the first two to
// the GROUP system:serviceaccounts:hpcburst-agents. up installs them on every start and resets each
// to the manifest, as it does the CRD deletion guard (guard.go); status reports whether they are the
// manifest's. identity.go makes the ServiceAccounts that the group holds.
//
// A binding to the group, never one per agent, is the point: the RBAC subject and the admission
// policies' match condition (PR 3) are then one predicate, so no identity can hold the agent role
// and be outside the policies, and a ServiceAccount made by hand in another namespace gets nothing.
//
// The role hpcburst-agent has two forms, in two hand-written files (PR 3): the NARROW one, read-only
// (config/rbac/agents-narrow.yaml), and the WIDE one with the writes the admission policies bound
// (config/rbac/agents.yaml, the manifest). up applies the narrow one before the policies are known to be
// enforced, and the wide one after (InstallAgentAccess in admission.go), so no moment lets an agent's
// role grant a write before the policy that bounds it is enforced. InstallAgentRBAC applies the wide
// form: call it only after the policies are enforced, as InstallAgentAccess does.

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"slices"
	"strings"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/retry"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/config/rbac"
)

const (
	// AgentsNamespace is the namespace of the agents' ServiceAccounts.
	AgentsNamespace = "hpcburst-agents"
	// AgentsGroup is the group every ServiceAccount of AgentsNamespace belongs to (Kubernetes names
	// it system:serviceaccounts:<namespace>). The two agent bindings name it.
	AgentsGroup = "system:serviceaccounts:" + AgentsNamespace
)

var (
	agentRoleNames    = []string{"hpcburst-agent-read", "hpcburst-agent", "hpcburst-operator"}
	agentBindingNames = []string{"hpcburst-agent-read", "hpcburst-agent"}

	namespaceGVR          = schemaGVR("", "v1", "namespaces")
	clusterRoleGVR        = schemaGVR("rbac.authorization.k8s.io", "v1", "clusterroles")
	clusterRoleBindingGVR = schemaGVR("rbac.authorization.k8s.io", "v1", "clusterrolebindings")
)

// agentManifest is the decoded config/rbac/agents.yaml, in apply order.
type agentManifest struct {
	namespace *unstructured.Unstructured
	roles     []*unstructured.Unstructured
	bindings  []*unstructured.Unstructured
}

// all returns the objects in apply order: the namespace, the roles, then the bindings.
func (m *agentManifest) all() []*unstructured.Unstructured {
	out := []*unstructured.Unstructured{m.namespace}
	out = append(out, m.roles...)
	return append(out, m.bindings...)
}

// agentObjects decodes the embedded manifest and checks its shape (agentManifestOf).
func agentObjects() (*agentManifest, error) {
	objs, err := decodeManifests(bytes.NewReader(rbac.Agents), "config/rbac/agents.yaml")
	if err != nil {
		return nil, err
	}
	return agentManifestOf(objs)
}

// agentManifestOf sorts decoded objects into an agentManifest. It refuses anything but the namespace,
// the three roles and the two bindings, each once, so that a change to the file that the launcher
// does not understand stops here and not at a start.
func agentManifestOf(objs []*unstructured.Unstructured) (*agentManifest, error) {
	m := &agentManifest{}
	for _, o := range objs {
		switch {
		case o.GetKind() == "Namespace" && m.namespace == nil && o.GetName() == AgentsNamespace:
			m.namespace = o
		case o.GetKind() == "ClusterRole" && o.GetAPIVersion() == "rbac.authorization.k8s.io/v1":
			m.roles = append(m.roles, o)
		case o.GetKind() == "ClusterRoleBinding" && o.GetAPIVersion() == "rbac.authorization.k8s.io/v1":
			m.bindings = append(m.bindings, o)
		default:
			return nil, fmt.Errorf("the agents manifest holds an unexpected %s %s, want the namespace %s, the ClusterRoles %v and the ClusterRoleBindings %v",
				o.GetKind(), o.GetName(), AgentsNamespace, agentRoleNames, agentBindingNames)
		}
	}
	names := func(os []*unstructured.Unstructured) []string {
		var n []string
		for _, o := range os {
			n = append(n, o.GetName())
		}
		slices.Sort(n)
		return n
	}
	if m.namespace == nil || !slices.Equal(names(m.roles), sorted(agentRoleNames)) || !slices.Equal(names(m.bindings), sorted(agentBindingNames)) {
		return nil, fmt.Errorf("the agents manifest holds the roles %v and the bindings %v, want the namespace %s, the ClusterRoles %v and the ClusterRoleBindings %v, each once",
			names(m.roles), names(m.bindings), AgentsNamespace, sorted(agentRoleNames), sorted(agentBindingNames))
	}
	return m, nil
}

func sorted(s []string) []string {
	c := slices.Clone(s)
	slices.Sort(c)
	return c
}

// AgentRoleName is the ClusterRole that has a narrow and a wide form.
const AgentRoleName = "hpcburst-agent"

// agentNarrowRole decodes the embedded narrow form of the agent role and checks its shape: one
// ClusterRole, named AgentRoleName.
func agentNarrowRole() (*unstructured.Unstructured, error) {
	objs, err := decodeManifests(bytes.NewReader(rbac.AgentsNarrow), "config/rbac/agents-narrow.yaml")
	if err != nil {
		return nil, err
	}
	if len(objs) != 1 || objs[0].GetKind() != "ClusterRole" || objs[0].GetAPIVersion() != "rbac.authorization.k8s.io/v1" || objs[0].GetName() != AgentRoleName {
		return nil, fmt.Errorf("config/rbac/agents-narrow.yaml holds %d objects, want the one ClusterRole %s", len(objs), AgentRoleName)
	}
	return objs[0], nil
}

// narrowed returns a copy of m whose agent role is the narrow form. The namespace, the other roles and
// the bindings are the manifest's, and m is not changed.
func (m *agentManifest) narrowed() (*agentManifest, error) {
	narrow, err := agentNarrowRole()
	if err != nil {
		return nil, err
	}
	out := &agentManifest{namespace: m.namespace, bindings: m.bindings}
	for _, r := range m.roles {
		if r.GetName() == AgentRoleName {
			r = narrow
		}
		out.roles = append(out.roles, r)
	}
	return out, nil
}

// InstallAgentRBAC makes the namespace, the roles and the bindings exist and be the manifest's, the
// agent role in its WIDE form, and applies them with server-side apply, so repeating it is safe. Objects
// that exist already are set to the manifest's rules and subjects first (resetAgentObjects); only then
// does the apply create what is missing. The wide role grants the writes the admission policies bound,
// so it is applied after they are enforced: up goes through InstallAgentAccess (admission.go), which
// does that. A caller that applies it alone opens the window that function closes.
func InstallAgentRBAC(ctx context.Context, cfg *rest.Config, log io.Writer) error {
	m, err := agentObjects()
	if err != nil {
		return err
	}
	return installAgentManifest(ctx, cfg, m, log)
}

// InstallAgentRBACNarrow is InstallAgentRBAC with the agent role in its NARROW form, which is read-only.
// It also narrows a role that an earlier start widened.
func InstallAgentRBACNarrow(ctx context.Context, cfg *rest.Config, log io.Writer) error {
	wide, err := agentObjects()
	if err != nil {
		return err
	}
	m, err := wide.narrowed()
	if err != nil {
		return err
	}
	return installAgentManifest(ctx, cfg, m, log)
}

func installAgentManifest(ctx context.Context, cfg *rest.Config, m *agentManifest, log io.Writer) error {
	dyn, mapper, err := newApplier(cfg)
	if err != nil {
		return err
	}
	if err := resetAgentObjects(ctx, dyn, m, log); err != nil {
		return err
	}
	for _, o := range m.all() {
		if err := applyOne(ctx, dyn, mapper, o); err != nil {
			return err
		}
		logf(log, "applied %s %s", o.GetKind(), o.GetName())
	}
	return nil
}

// resetAgentObjects sets each role and binding that exists to the manifest's, before the apply, so that
// what was added by hand does not survive a start. A role keeps exactly the manifest's rules and has no
// aggregationRule (a controller would fill the rules of a role that has one, after this reset). A
// binding keeps exactly the manifest's subjects; one whose roleRef differs is deleted, for roleRef
// cannot change, and the apply makes it again. The objects are replaced in place when they can be,
// and a conflicting write is retried.
func resetAgentObjects(ctx context.Context, dyn dynamic.Interface, m *agentManifest, log io.Writer) error {
	for _, want := range m.roles {
		if err := resetFields(ctx, dyn, clusterRoleGVR, want, "rules", "aggregationRule"); err != nil {
			return err
		}
	}
	for _, want := range m.bindings {
		cur, err := dyn.Resource(clusterRoleBindingGVR).Get(ctx, want.GetName(), metav1.GetOptions{})
		switch {
		case apierrors.IsNotFound(err):
			continue // the apply creates it
		case err != nil:
			return fmt.Errorf("read the ClusterRoleBinding %s: %w", want.GetName(), err)
		}
		if !sameField(cur, want, "roleRef") {
			uid := cur.GetUID()
			err := dyn.Resource(clusterRoleBindingGVR).Delete(ctx, want.GetName(), metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}})
			if err != nil && !apierrors.IsNotFound(err) {
				return fmt.Errorf("delete the ClusterRoleBinding %s, whose roleRef differs from the manifest's: %w", want.GetName(), err)
			}
			logf(log, "deleted ClusterRoleBinding %s: its roleRef differs from the manifest's, and a roleRef cannot change", want.GetName())
			continue
		}
		if err := resetFields(ctx, dyn, clusterRoleBindingGVR, want, "subjects"); err != nil {
			return err
		}
	}
	return nil
}

// sameField reports whether got and want hold equal values at the top-level field key, and both
// lack it or both have it.
func sameField(got, want *unstructured.Unstructured, key string) bool {
	g, gok := got.Object[key]
	w, wok := want.Object[key]
	return gok == wok && apiequality.Semantic.DeepEqual(g, w)
}

// resourceOf is the client of want's kind: namespaced when want names a namespace (a Role, a RoleBinding),
// else cluster-wide (a ClusterRole, a ClusterRoleBinding).
func resourceOf(dyn dynamic.Interface, gvr schema.GroupVersionResource, want *unstructured.Unstructured) dynamic.ResourceInterface {
	if ns := want.GetNamespace(); ns != "" {
		return dyn.Resource(gvr).Namespace(ns)
	}
	return dyn.Resource(gvr)
}

// resetFields makes the top-level fields keys of the existing object named like want equal to want's:
// set when want has the field, removed when it has not. An object that does not exist is left to the
// apply. A conflicting write is retried.
func resetFields(ctx context.Context, dyn dynamic.Interface, gvr schema.GroupVersionResource, want *unstructured.Unstructured, keys ...string) error {
	res := resourceOf(dyn, gvr, want)
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		cur, err := res.Get(ctx, want.GetName(), metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		changed := false
		for _, k := range keys {
			if sameField(cur, want, k) {
				continue
			}
			changed = true
			if w, ok := want.Object[k]; ok {
				cur.Object[k] = runtime.DeepCopyJSONValue(w)
			} else {
				delete(cur.Object, k)
			}
		}
		if !changed {
			return nil
		}
		_, err = res.Update(ctx, cur, metav1.UpdateOptions{FieldManager: fieldManager})
		return err
	})
	if err != nil {
		return fmt.Errorf("set the %s %s to the manifest's: %w", want.GetKind(), want.GetName(), err)
	}
	return nil
}

// AgentRBACState is what the API server says about the agents' namespace, roles and bindings.
type AgentRBACState struct {
	// Present is true when the namespace, the three roles and the two bindings exist and are the
	// manifest's: the roles' rules, the bindings' roleRef and subjects. The agent role may be either of
	// its two forms (the wide one in agents.yaml, or the narrow one in agents-narrow.yaml).
	Present bool `json:"present"`
	// Writes is true when the agent role is the wide form, which grants the writes the admission policies
	// bound. Present with Writes false is a host whose agents are read-only: up found no CRD to probe a
	// policy with, or is in the middle of its wait.
	Writes bool `json:"writes"`
	// Detail names what differs when Present is false, and why the agents' writes are off when Present is
	// true and Writes is not.
	Detail string `json:"detail,omitempty"`
}

// CheckAgentRBAC asks the API server for the namespace, the roles and the bindings. An error is a
// failure to ask, never a role that differs.
func CheckAgentRBAC(ctx context.Context, cfg *rest.Config) (*AgentRBACState, error) {
	dyn, _, err := newApplier(cfg)
	if err != nil {
		return nil, err
	}
	return checkAgentRBAC(ctx, dyn)
}

func checkAgentRBAC(ctx context.Context, dyn dynamic.Interface) (*AgentRBACState, error) {
	m, err := agentObjects()
	if err != nil {
		return nil, err
	}
	narrow, err := agentNarrowRole()
	if err != nil {
		return nil, err
	}
	var diffs []string
	writes, narrowForm := false, false
	get := func(gvr schema.GroupVersionResource, want *unstructured.Unstructured) (*unstructured.Unstructured, error) {
		cur, err := dyn.Resource(gvr).Get(ctx, want.GetName(), metav1.GetOptions{})
		switch {
		case apierrors.IsNotFound(err):
			diffs = append(diffs, fmt.Sprintf("the %s %s is not there", want.GetKind(), want.GetName()))
			return nil, nil
		case err != nil:
			return nil, fmt.Errorf("read the %s %s: %w", want.GetKind(), want.GetName(), err)
		}
		return cur, nil
	}
	if _, err := get(namespaceGVR, m.namespace); err != nil {
		return nil, err
	}
	for _, want := range m.roles {
		cur, err := get(clusterRoleGVR, want)
		if err != nil {
			return nil, err
		}
		if cur == nil {
			continue
		}
		switch {
		case sameField(cur, want, "rules"):
			if want.GetName() == AgentRoleName {
				writes = true
			}
		case want.GetName() == AgentRoleName && sameField(cur, narrow, "rules"):
			narrowForm = true
		default:
			diffs = append(diffs, fmt.Sprintf("the ClusterRole %s has rules that differ from the manifest's", want.GetName()))
		}
		if _, ok := cur.Object["aggregationRule"]; ok {
			diffs = append(diffs, fmt.Sprintf("the ClusterRole %s has an aggregationRule, which the manifest does not set and which lets a controller rewrite its rules", want.GetName()))
		}
	}
	for _, want := range m.bindings {
		cur, err := get(clusterRoleBindingGVR, want)
		if err != nil {
			return nil, err
		}
		if cur == nil {
			continue
		}
		if !sameField(cur, want, "roleRef") {
			diffs = append(diffs, fmt.Sprintf("the ClusterRoleBinding %s has a roleRef that differs from the manifest's", want.GetName()))
		}
		if !sameField(cur, want, "subjects") {
			diffs = append(diffs, fmt.Sprintf("the ClusterRoleBinding %s has subjects that differ from the manifest's (it must name only the group %s)", want.GetName(), AgentsGroup))
		}
	}
	if len(diffs) > 0 {
		return &AgentRBACState{Detail: strings.Join(diffs, "; ")}, nil
	}
	if narrowForm {
		return &AgentRBACState{Present: true, Detail: "the ClusterRole " + AgentRoleName + " is the narrow, read-only form: the agents' writes are off until an up finds the ModelDeployment, Operation and Approval custom resource definitions and sees each admission policy refuse its probe"}, nil
	}
	return &AgentRBACState{Present: true, Writes: writes}, nil
}
