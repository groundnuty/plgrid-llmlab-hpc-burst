package launcher

// The agent role's two forms (agents.go; issue #589, PR 3): the NARROW read-only one that up applies
// before the admission policies are enforced, and the WIDE one with the writes they bound. Both are
// hand-written files; this file states, by hand from the issue, what separates them, so that neither is
// judged against the other's bytes.

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

func roleRules(t *testing.T, role *unstructured.Unstructured) []any {
	t.Helper()
	rules, _, err := unstructured.NestedSlice(role.Object, "rules")
	if err != nil {
		t.Fatal(err)
	}
	return rules
}

func wideAgentRole(t *testing.T) *unstructured.Unstructured {
	t.Helper()
	m, err := agentObjects()
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range m.roles {
		if r.GetName() == AgentRoleName {
			return r
		}
	}
	t.Fatalf("the manifest has no ClusterRole %s", AgentRoleName)
	return nil
}

func TestNarrowAgentRoleIsTheFileOfTheRepository(t *testing.T) {
	f, err := os.Open(filepath.Join("..", "..", "config", "rbac", "agents-narrow.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	onDisk, err := decodeManifests(f, "agents-narrow.yaml")
	if err != nil {
		t.Fatal(err)
	}
	narrow, err := agentNarrowRole()
	if err != nil {
		t.Fatal(err)
	}
	if len(onDisk) != 1 || !reflect.DeepEqual(onDisk[0].Object, narrow.Object) {
		t.Errorf("the embedded narrow role differs from config/rbac/agents-narrow.yaml on disk")
	}
}

// The narrow form is read-only, whatever it holds: no verb but get, list and watch on any rule.
func TestNarrowAgentRoleGrantsNoWrite(t *testing.T) {
	narrow, err := agentNarrowRole()
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range roleRules(t, narrow) {
		verbs, _ := r.(map[string]any)["verbs"].([]any)
		for _, v := range verbs {
			if !slices.Contains([]any{"get", "list", "watch"}, v) {
				t.Errorf("the narrow role has the verb %v in %v: it is the read-only form", v, r)
			}
		}
	}
}

// What the wide form adds to the narrow one, from the issue and HBEP-764: ModelDeployment create, update, patch;
// the scale subresource get, update, patch; Operation create, update, patch; Approval create; Workload create,
// update, patch. Nothing else differs: every rule of the narrow form is in the wide one, and the wide one has
// these five more.
func TestWideAgentRoleIsTheNarrowRolePlusExactlyTheWrites(t *testing.T) {
	narrow, err := agentNarrowRole()
	if err != nil {
		t.Fatal(err)
	}
	wide := roleRules(t, wideAgentRole(t))
	rule := func(resource string, verbs ...string) any {
		vs := make([]any, len(verbs))
		for i, v := range verbs {
			vs[i] = v
		}
		return map[string]any{"apiGroups": []any{"hpcburst.groundnuty.github.io"}, "resources": []any{resource}, "verbs": vs}
	}
	writes := []any{
		rule("modeldeployments", "create", "update", "patch"),
		rule("modeldeployments/scale", "get", "update", "patch"),
		rule("operations", "create", "update", "patch"),
		rule("approvals", "create"),
		rule("workloads", "create", "update", "patch"),
	}
	var extra []any
	for _, r := range wide {
		if !slices.ContainsFunc(roleRules(t, narrow), func(n any) bool { return reflect.DeepEqual(n, r) }) {
			extra = append(extra, r)
		}
	}
	if !reflect.DeepEqual(extra, writes) {
		t.Errorf("the wide role has these rules beyond the narrow one: %v, want exactly %v", extra, writes)
	}
	for _, n := range roleRules(t, narrow) {
		if !slices.ContainsFunc(wide, func(w any) bool { return reflect.DeepEqual(n, w) }) {
			t.Errorf("the narrow rule %v is not in the wide role", n)
		}
	}
}

// narrowed swaps the narrow form in; the namespace, the other roles and the bindings are the manifest's.
func TestNarrowedManifestChangesOnlyTheAgentRole(t *testing.T) {
	m, err := agentObjects()
	if err != nil {
		t.Fatal(err)
	}
	n, err := m.narrowed()
	if err != nil {
		t.Fatal(err)
	}
	if len(n.roles) != len(m.roles) || n.namespace != m.namespace || !reflect.DeepEqual(n.bindings, m.bindings) {
		t.Fatalf("narrowed changed more than the roles' rules: %d roles, want %d", len(n.roles), len(m.roles))
	}
	for i, r := range m.roles {
		same := reflect.DeepEqual(r.Object, n.roles[i].Object)
		if (r.GetName() == AgentRoleName) == same {
			t.Errorf("role %s: unchanged by narrowed = %v, want it changed only for %s", r.GetName(), same, AgentRoleName)
		}
	}
	if wide := wideAgentRole(t); len(roleRules(t, wide)) == len(roleRules(t, n.roles[slices.IndexFunc(n.roles, func(r *unstructured.Unstructured) bool { return r.GetName() == AgentRoleName })])) {
		t.Error("the narrowed agent role has as many rules as the wide one")
	}
}

// The check says which form the host holds: the wide one has the writes, the narrow one is present with
// the writes off and says why, and anything else differs.
func TestCheckAgentRBACNamesTheFormOfTheAgentRole(t *testing.T) {
	_, objs := installedAgentObjects(t)
	f, _ := rbacFake(objs...)
	st, err := checkAgentRBAC(context.Background(), f)
	if err != nil || !st.Present || !st.Writes || st.Detail != "" {
		t.Fatalf("the wide form = %+v, %v, want present with the writes on", st, err)
	}

	narrow, _ := agentNarrowRole()
	cur := getObj(t, f, clusterRoleGVR, AgentRoleName)
	cur.Object["rules"] = runtime.DeepCopyJSONValue(narrow.Object["rules"])
	if _, err := f.Resource(clusterRoleGVR).Update(context.Background(), cur, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	st, err = checkAgentRBAC(context.Background(), f)
	if err != nil || !st.Present || st.Writes || !strings.Contains(st.Detail, "narrow, read-only") || !strings.Contains(st.Detail, "writes are off") {
		t.Errorf("the narrow form = %+v, %v, want present, writes off, with the reason", st, err)
	}

	// Neither form: the narrow rules and one more.
	cur = getObj(t, f, clusterRoleGVR, AgentRoleName)
	cur.Object["rules"] = append(runtime.DeepCopyJSONValue(narrow.Object["rules"]).([]any), map[string]any{
		"apiGroups": []any{"hpcburst.groundnuty.github.io"}, "resources": []any{"allocations"}, "verbs": []any{"delete"},
	})
	if _, err := f.Resource(clusterRoleGVR).Update(context.Background(), cur, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	st, err = checkAgentRBAC(context.Background(), f)
	if err != nil || st.Present || st.Writes || !strings.Contains(st.Detail, "ClusterRole hpcburst-agent has rules that differ") {
		t.Errorf("a role of neither form = %+v, %v, want absent, naming the role", st, err)
	}
}
