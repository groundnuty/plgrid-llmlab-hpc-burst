package launcher

// The agents' namespace, roles and bindings, launcher side (agents.go; issue #589, PR 2), without an
// API server: what the embedded manifest holds, what the reset does to each kind of hand edit, and what
// the check reports. Whether the API server then grants or refuses what an agent asks is judged
// elsewhere, by the API server's own decisions for each agent identity: api/v1alpha1/e2e on envtest
// and cmd/hpcburst on a real k3s.

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"
)

// The launcher embeds the file of the repository: what it decodes is what decoding the file on disk
// gives, so there is one source and no copy to drift.
func TestAgentManifestIsTheFileOfTheRepository(t *testing.T) {
	f, err := os.Open(filepath.Join("..", "..", "config", "rbac", "agents.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	onDisk, err := decodeManifests(f, "agents.yaml")
	if err != nil {
		t.Fatal(err)
	}
	m, err := agentObjects()
	if err != nil {
		t.Fatal(err)
	}
	all := m.all()
	if len(onDisk) != len(all) {
		t.Fatalf("the file holds %d objects, the embedded copy %d", len(onDisk), len(all))
	}
	for i := range all {
		if !reflect.DeepEqual(onDisk[i].Object, all[i].Object) {
			t.Errorf("the embedded %s %s differs from config/rbac/agents.yaml on disk", all[i].GetKind(), all[i].GetName())
		}
	}
}

// What binds the agents, written from HBEP-10 and the issue, not read back from the file: each of the
// two bindings names the group and nothing else, and its role by the same name. A binding that named a
// ServiceAccount, or any user, would give the role to one the admission policies do not match.
func TestAgentBindingsNameTheGroupAndNothingElse(t *testing.T) {
	m, err := agentObjects()
	if err != nil {
		t.Fatal(err)
	}
	if len(m.bindings) != 2 {
		t.Fatalf("%d bindings, want 2", len(m.bindings))
	}
	for _, b := range m.bindings {
		subjects, _, _ := unstructured.NestedSlice(b.Object, "subjects")
		want := []any{map[string]any{"apiGroup": "rbac.authorization.k8s.io", "kind": "Group", "name": "system:serviceaccounts:hpcburst-agents"}}
		if !reflect.DeepEqual(subjects, want) {
			t.Errorf("binding %s subjects = %v, want only the group system:serviceaccounts:hpcburst-agents", b.GetName(), subjects)
		}
		ref, _, _ := unstructured.NestedStringMap(b.Object, "roleRef")
		if ref["kind"] != "ClusterRole" || ref["name"] != b.GetName() || ref["apiGroup"] != "rbac.authorization.k8s.io" {
			t.Errorf("binding %s roleRef = %v, want the ClusterRole of the same name", b.GetName(), ref)
		}
	}
	if AgentsGroup != "system:serviceaccounts:hpcburst-agents" {
		t.Errorf("AgentsGroup = %q", AgentsGroup)
	}
}

func rbacFake(objs ...runtime.Object) (*fake.FakeDynamicClient, *[]string) {
	kinds := map[schema.GroupVersionResource]string{
		namespaceGVR: "NamespaceList", clusterRoleGVR: "ClusterRoleList", clusterRoleBindingGVR: "ClusterRoleBindingList",
	}
	f := fake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), kinds, objs...)
	var verbs []string
	f.PrependReactor("*", "*", func(a k8stesting.Action) (bool, runtime.Object, error) {
		if v := a.GetVerb(); v == "update" || v == "delete" || v == "create" {
			verbs = append(verbs, v+" "+a.GetResource().Resource)
			if d, ok := a.(k8stesting.DeleteAction); ok {
				verbs[len(verbs)-1] += "/" + d.GetName()
			}
		}
		return false, nil, nil
	})
	return f, &verbs
}

// installedAgentObjects is a host that holds exactly the manifest's objects.
func installedAgentObjects(t *testing.T) (*agentManifest, []runtime.Object) {
	t.Helper()
	m, err := agentObjects()
	if err != nil {
		t.Fatal(err)
	}
	var objs []runtime.Object
	for _, o := range m.all() {
		objs = append(objs, o.DeepCopy())
	}
	return m, objs
}

func getObj(t *testing.T, f *fake.FakeDynamicClient, gvr schema.GroupVersionResource, name string) *unstructured.Unstructured {
	t.Helper()
	o, err := f.Resource(gvr).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get %s %s: %v", gvr.Resource, name, err)
	}
	return o
}

func TestCheckAgentRBACOfWhatTheManifestHolds(t *testing.T) {
	_, objs := installedAgentObjects(t)
	f, _ := rbacFake(objs...)
	st, err := checkAgentRBAC(context.Background(), f)
	if err != nil || !st.Present || st.Detail != "" {
		t.Fatalf("checkAgentRBAC of the manifest's own objects = %+v, %v, want present", st, err)
	}
	empty, _ := rbacFake()
	st, err = checkAgentRBAC(context.Background(), empty)
	if err != nil || st.Present {
		t.Fatalf("checkAgentRBAC of an empty host = %+v, %v, want absent", st, err)
	}
	for _, want := range []string{"Namespace hpcburst-agents", "ClusterRole hpcburst-agent-read", "ClusterRole hpcburst-agent ", "ClusterRole hpcburst-operator", "ClusterRoleBinding hpcburst-agent-read", "ClusterRoleBinding hpcburst-agent "} {
		if !strings.Contains(st.Detail+" ", want) {
			t.Errorf("the detail of an empty host %q does not name %q", st.Detail, want)
		}
	}
}

// One hand edit of each kind the brief names (an extra rule, a rule removed, an extra subject), and the
// two the reset must also undo (an aggregationRule, a roleRef): the check names the object, and the
// install's reset puts it back, by update for a field and by delete for a roleRef.
func TestHandEditsAreReportedAndReset(t *testing.T) {
	for _, tc := range []struct {
		name     string
		gvr      schema.GroupVersionResource
		object   string
		edit     func(o *unstructured.Unstructured)
		detail   string
		wantVerb string // the write the reset makes
	}{
		{"an extra rule in the agent role", clusterRoleGVR, "hpcburst-agent", func(o *unstructured.Unstructured) {
			rules, _, _ := unstructured.NestedSlice(o.Object, "rules")
			rules = append(rules, map[string]any{"apiGroups": []any{""}, "resources": []any{"secrets"}, "verbs": []any{"get"}})
			_ = unstructured.SetNestedSlice(o.Object, rules, "rules")
		}, "ClusterRole hpcburst-agent has rules that differ", "update clusterroles"},
		{"a rule removed from the read role", clusterRoleGVR, "hpcburst-agent-read", func(o *unstructured.Unstructured) {
			_ = unstructured.SetNestedSlice(o.Object, []any{}, "rules")
		}, "ClusterRole hpcburst-agent-read has rules that differ", "update clusterroles"},
		{"a verb added to the operator role's rule", clusterRoleGVR, "hpcburst-operator", func(o *unstructured.Unstructured) {
			rules, _, _ := unstructured.NestedSlice(o.Object, "rules")
			rule := rules[0].(map[string]any)
			rule["verbs"] = append(rule["verbs"].([]any), "deletecollection")
			_ = unstructured.SetNestedSlice(o.Object, rules, "rules")
		}, "ClusterRole hpcburst-operator has rules that differ", "update clusterroles"},
		{"an aggregationRule on the agent role", clusterRoleGVR, "hpcburst-agent", func(o *unstructured.Unstructured) {
			o.Object["aggregationRule"] = map[string]any{"clusterRoleSelectors": []any{map[string]any{"matchLabels": map[string]any{"x": "y"}}}}
		}, "ClusterRole hpcburst-agent has an aggregationRule", "update clusterroles"},
		{"an extra subject on the agent binding", clusterRoleBindingGVR, "hpcburst-agent", func(o *unstructured.Unstructured) {
			s, _, _ := unstructured.NestedSlice(o.Object, "subjects")
			s = append(s, map[string]any{"apiGroup": "rbac.authorization.k8s.io", "kind": "User", "name": "example-user"})
			_ = unstructured.SetNestedSlice(o.Object, s, "subjects")
		}, "ClusterRoleBinding hpcburst-agent has subjects that differ", "update clusterrolebindings"},
		{"a ServiceAccount subject instead of the group", clusterRoleBindingGVR, "hpcburst-agent-read", func(o *unstructured.Unstructured) {
			_ = unstructured.SetNestedSlice(o.Object, []any{map[string]any{"kind": "ServiceAccount", "name": "agent-a", "namespace": "hpcburst-agents"}}, "subjects")
		}, "ClusterRoleBinding hpcburst-agent-read has subjects that differ", "update clusterrolebindings"},
		{"a roleRef to another role", clusterRoleBindingGVR, "hpcburst-agent-read", func(o *unstructured.Unstructured) {
			_ = unstructured.SetNestedField(o.Object, "hpcburst-operator", "roleRef", "name")
		}, "ClusterRoleBinding hpcburst-agent-read has a roleRef that differs", "delete clusterrolebindings/hpcburst-agent-read"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, objs := installedAgentObjects(t)
			f, verbs := rbacFake(objs...)
			cur := getObj(t, f, tc.gvr, tc.object)
			tc.edit(cur)
			if _, err := f.Resource(tc.gvr).Update(context.Background(), cur, metav1.UpdateOptions{}); err != nil {
				t.Fatal(err)
			}
			*verbs = nil

			st, err := checkAgentRBAC(context.Background(), f)
			if err != nil || st.Present || !strings.Contains(st.Detail, tc.detail) {
				t.Fatalf("checkAgentRBAC after %s = %+v, %v, want absent with a detail containing %q", tc.name, st, err, tc.detail)
			}
			if err := resetAgentObjects(context.Background(), f, m, io.Discard); err != nil {
				t.Fatal(err)
			}
			if !slices.Contains(*verbs, tc.wantVerb) {
				t.Errorf("the reset wrote %v, want it to include %q", *verbs, tc.wantVerb)
			}
			// A roleRef is deleted and the apply makes the binding again; stand in for that here.
			if strings.HasPrefix(tc.wantVerb, "delete") {
				if _, err := f.Resource(tc.gvr).Get(context.Background(), tc.object, metav1.GetOptions{}); err == nil {
					t.Fatalf("the binding with a wrong roleRef is still there after the reset")
				}
				for _, w := range m.bindings {
					if w.GetName() == tc.object {
						if _, err := f.Resource(tc.gvr).Create(context.Background(), w.DeepCopy(), metav1.CreateOptions{}); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
			st, err = checkAgentRBAC(context.Background(), f)
			if err != nil || !st.Present {
				t.Errorf("checkAgentRBAC after the reset = %+v, %v, want present", st, err)
			}
		})
	}
}

// A host that is the manifest's already is not written to: the reset touches nothing that is right.
func TestResetWritesNothingWhenTheManifestIsInstalled(t *testing.T) {
	m, objs := installedAgentObjects(t)
	f, verbs := rbacFake(objs...)
	if err := resetAgentObjects(context.Background(), f, m, io.Discard); err != nil {
		t.Fatal(err)
	}
	if len(*verbs) != 0 {
		t.Errorf("the reset of an installed host wrote %v, want nothing", *verbs)
	}
}

func TestAgentObjectsRefuseAManifestThatHoldsSomethingElse(t *testing.T) {
	// A copy of the real objects with one more ClusterRole.
	m, err := agentObjects()
	if err != nil {
		t.Fatal(err)
	}
	stray := m.roles[0].DeepCopy()
	stray.SetName("hpcburst-extra")
	if _, err := agentManifestOf(append(m.all(), stray)); err == nil || !strings.Contains(err.Error(), "hpcburst-extra") {
		t.Errorf("a manifest with an extra ClusterRole: %v, want a refusal naming it", err)
	}
}
