package launcher

// Tests of the controllers' own identities (HBEP-10, Audit; issue #660, PR A): the manifest's shape and
// the rules each role holds, the tokens' files, the supervised children's command lines, and the order in
// which up and upgrade make the identities ready. Everything that needs an API server (the roles under the
// real controllers, the SelfSubjectReview of each identity, the audit log) is cmd/hpcburst's k3s test and
// cmd/hpcburst-controller's harness.

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	gotoken "go/token"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/config/rbac"
)

// writePlaceholderKubeconfigs writes the three controller kubeconfigs where the layout says (typed out
// here, as the tests of the supervised command lines type it: <state>/identities/system/<name>.kubeconfig),
// mode 600 in directories of mode 700, each holding token. The files stand in for what up issues, for the
// tests that supervise a fake controller without an API server.
func writePlaceholderKubeconfigs(t testing.TB, l layout, token string) {
	t.Helper()
	dir := filepath.Join(l.dir, "identities", "system")
	for _, d := range []string{filepath.Dir(dir), dir} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"hpcburst-lifecycle", "hpcburst-provider", "hpcburst-front"} {
		if err := os.WriteFile(filepath.Join(dir, name+".kubeconfig"), []byte("users:\n- name: "+name+"\n  user:\n    token: "+token+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

func randomToken(t testing.TB) string {
	t.Helper()
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return "tok." + hex.EncodeToString(b)
}

// wantControllerRules is the enumeration of each controller's API calls, from the code (the table of the
// PR body, with file:line), written out by hand. It is not derived from config/rbac/controllers.yaml, which
// this test checks it against: the two are made apart, from the call sites, and agreement shows that no
// later edit widened a role or dropped a verb. That the roles are also ENOUGH is shown by the real
// controllers' suites, which run under these identities and fail on a Forbidden (cmd/hpcburst-controller).
// One line is a group, its resources and the verbs they take.
var wantControllerRules = map[string][]string{
	"hpcburst-lifecycle": {
		"hpcburst.groundnuty.github.io modeldeployments: get list watch",
		"hpcburst.groundnuty.github.io providers platformprofiles operations approvals approvalpolicies: list watch",
		"hpcburst.groundnuty.github.io instances: get list watch create update delete",
		"hpcburst.groundnuty.github.io instances/status: update",
		"hpcburst.groundnuty.github.io allocations: get list watch create update delete",
		"hpcburst.groundnuty.github.io modeldeployments/status operations/status: update",
		"hpcburst.groundnuty.github.io providers/status: patch",
		"apps controllerrevisions: get list watch create",
		"events.k8s.io events: create patch",
	},
	"hpcburst-provider": {
		"hpcburst.groundnuty.github.io instances: list watch",
		"hpcburst.groundnuty.github.io providers platformprofiles: get list watch",
		"hpcburst.groundnuty.github.io allocations: get list watch update",
		"hpcburst.groundnuty.github.io allocations/status: update patch",
		"hpcburst.groundnuty.github.io providers/status: update",
		"apps controllerrevisions: get create",
		"core events: create patch",
		"events.k8s.io events: create patch",
	},
	"hpcburst-front": {
		"hpcburst.groundnuty.github.io modeldeployments instances providers platformprofiles: list watch",
		"apps controllerrevisions: list",
		"events.k8s.io events: create patch",
	},
}

// wantLeaseVerbs is each identity's Role in hpcburst-system, by hand: every controller's leader Lease (or, for the
// front, its heartbeat Lease) lives in hpcburst-system alone, and a Lock is GET, CREATE and UPDATE.
var wantLeaseVerbs = []string{"coordination.k8s.io leases create", "coordination.k8s.io leases get", "coordination.k8s.io leases update"}

// triples expands a role's rules into "group resource verb" strings.
func rulesOf(t testing.TB, role *unstructured.Unstructured) []string {
	t.Helper()
	rules, _, err := unstructured.NestedSlice(role.Object, "rules")
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, r := range rules {
		rm := r.(map[string]any)
		strs := func(key string) []string {
			var s []string
			for _, v := range rm[key].([]any) {
				s = append(s, v.(string))
			}
			return s
		}
		for _, g := range strs("apiGroups") {
			for _, res := range strs("resources") {
				for _, v := range strs("verbs") {
					out = append(out, g+" "+res+" "+v)
				}
			}
		}
	}
	slices.Sort(out)
	return slices.Compact(out)
}

func wantTriples(name string) []string {
	var out []string
	for _, line := range wantControllerRules[name] {
		head, verbs, _ := strings.Cut(line, ": ")
		fields := strings.Fields(head)
		group := fields[0]
		if group == "core" {
			group = ""
		}
		for _, res := range fields[1:] {
			for _, v := range strings.Fields(verbs) {
				out = append(out, group+" "+res+" "+v)
			}
		}
	}
	slices.Sort(out)
	return out
}

// TestControllerRolesGrantExactlyTheEnumeration: each role of config/rbac/controllers.yaml holds exactly the
// calls of its component, written out by hand above, and no other: no wildcard, nothing outside the verbs a
// controller calls, and none of the grants the controllers never use.
func TestControllerRolesGrantExactlyTheEnumeration(t *testing.T) {
	m, err := controllerObjects()
	if err != nil {
		t.Fatal(err)
	}
	for _, role := range m.roles {
		// The Lease verbs are a Role of hpcburst-system, never a ClusterRole's: a Lease write anywhere else is Forbidden.
		if got := rulesOf(t, role); !slices.Equal(got, wantLeaseVerbs) || role.GetNamespace() != "hpcburst-system" {
			t.Errorf("the Role %s of %s grants %q, want %q in hpcburst-system", role.GetName(), role.GetNamespace(), got, wantLeaseVerbs)
		}
	}
	for _, role := range m.clusterRoles {
		name := role.GetName()
		got, want := rulesOf(t, role), wantTriples(name)
		if !slices.Equal(got, want) {
			t.Errorf("the ClusterRole %s\n got %q\nwant %q", name, got, want)
		}
		for _, tr := range got {
			f := strings.Fields(tr)
			if len(f) == 2 { // the core group is the empty string
				f = append([]string{""}, f...)
			}
			if f[1] == "leases" {
				t.Errorf("the ClusterRole %s grants %q: the Lease verbs are the Role of hpcburst-system", name, tr)
			}
			if f[0] == "*" || f[1] == "*" || f[2] == "*" {
				t.Errorf("the ClusterRole %s holds a wildcard: %q", name, tr)
			}
			// What no controller calls: secrets, deletes outside the lifecycle's own two kinds, the verbs that
			// widen a role, and anything of RBAC, the CRDs, admission or tokens.
			switch {
			case strings.Contains(f[1], "secrets"), strings.Contains(f[1], "serviceaccounts"), strings.Contains(f[1], "nodes"), strings.Contains(f[1], "namespaces"),
				f[0] == "rbac.authorization.k8s.io", f[0] == "apiextensions.k8s.io", f[0] == "admissionregistration.k8s.io", f[0] == "authentication.k8s.io":
				t.Errorf("the ClusterRole %s grants %q, which no controller reaches", name, tr)
			case slices.Contains([]string{"deletecollection", "impersonate", "escalate", "bind"}, f[2]):
				t.Errorf("the ClusterRole %s grants the verb of %q", name, tr)
			case f[2] == "delete" && !(name == "hpcburst-lifecycle" && (f[1] == "instances" || f[1] == "allocations")):
				t.Errorf("the ClusterRole %s grants %q: only the lifecycle controller deletes, and only an Instance or an Allocation", name, tr)
			}
		}
	}
}

// TestControllerManifestIsRefusedWhenItsShapeIsWrong: a change to the manifest that the launcher does not
// understand stops at its check (controllerManifestOf), not at a start, and the check refuses each way of
// widening an identity. The control: the manifest as it is passes.
func TestControllerManifestIsRefusedWhenItsShapeIsWrong(t *testing.T) {
	decode := func() []*unstructured.Unstructured {
		objs, err := decodeManifests(bytes.NewReader(rbac.Controllers), "controllers.yaml")
		if err != nil {
			t.Fatal(err)
		}
		return objs
	}
	if _, err := controllerManifestOf(decode()); err != nil {
		t.Fatalf("the manifest as it is: %v", err)
	}
	find := func(objs []*unstructured.Unstructured, kind, name string) *unstructured.Unstructured {
		for _, o := range objs {
			if o.GetKind() == kind && o.GetName() == name {
				return o
			}
		}
		t.Fatalf("no %s %s", kind, name)
		return nil
	}
	setRule := func(role *unstructured.Unstructured, key string, val []any) {
		rules, _, _ := unstructured.NestedSlice(role.Object, "rules")
		rules[0].(map[string]any)[key] = val
		_ = unstructured.SetNestedSlice(role.Object, rules, "rules")
	}
	for _, c := range []struct {
		name   string
		mutate func(objs []*unstructured.Unstructured) []*unstructured.Unstructured
		want   string
	}{
		{"a wildcard verb", func(o []*unstructured.Unstructured) []*unstructured.Unstructured {
			setRule(find(o, "ClusterRole", "hpcburst-front"), "verbs", []any{"*"})
			return o
		}, "wildcard"},
		{"a wildcard resource", func(o []*unstructured.Unstructured) []*unstructured.Unstructured {
			setRule(find(o, "ClusterRole", "hpcburst-provider"), "resources", []any{"*"})
			return o
		}, "wildcard"},
		{"a wildcard group", func(o []*unstructured.Unstructured) []*unstructured.Unstructured {
			setRule(find(o, "ClusterRole", "hpcburst-lifecycle"), "apiGroups", []any{"*"})
			return o
		}, "wildcard"},
		{"an aggregationRule", func(o []*unstructured.Unstructured) []*unstructured.Unstructured {
			find(o, "ClusterRole", "hpcburst-front").Object["aggregationRule"] = map[string]any{}
			return o
		}, "aggregationRule"},
		{"a second subject", func(o []*unstructured.Unstructured) []*unstructured.Unstructured {
			b := find(o, "ClusterRoleBinding", "hpcburst-front")
			subjects, _, _ := unstructured.NestedSlice(b.Object, "subjects")
			_ = unstructured.SetNestedSlice(b.Object, append(subjects, map[string]any{"kind": "Group", "name": "system:authenticated"}), "subjects")
			return o
		}, "subjects"},
		{"a subject of another ServiceAccount", func(o []*unstructured.Unstructured) []*unstructured.Unstructured {
			b := find(o, "ClusterRoleBinding", "hpcburst-front")
			_ = unstructured.SetNestedSlice(b.Object, []any{map[string]any{"kind": "ServiceAccount", "name": "hpcburst-lifecycle", "namespace": "hpcburst-system"}}, "subjects")
			return o
		}, "subject"},
		{"a subject in another namespace", func(o []*unstructured.Unstructured) []*unstructured.Unstructured {
			b := find(o, "ClusterRoleBinding", "hpcburst-front")
			_ = unstructured.SetNestedSlice(b.Object, []any{map[string]any{"kind": "ServiceAccount", "name": "hpcburst-front", "namespace": "default"}}, "subjects")
			return o
		}, "subject"},
		{"a binding to another role", func(o []*unstructured.Unstructured) []*unstructured.Unstructured {
			_ = unstructured.SetNestedField(find(o, "ClusterRoleBinding", "hpcburst-front").Object, "hpcburst-lifecycle", "roleRef", "name")
			return o
		}, "role"},
		{"a Role that grants more than leases", func(o []*unstructured.Unstructured) []*unstructured.Unstructured {
			setRule(find(o, "Role", "hpcburst-front"), "resources", []any{"leases", "pods"})
			return o
		}, "leases alone"},
		{"a Role of another API group", func(o []*unstructured.Unstructured) []*unstructured.Unstructured {
			setRule(find(o, "Role", "hpcburst-provider"), "apiGroups", []any{"apps"})
			return o
		}, "Lease verbs"},
		{"a Role with a wildcard verb", func(o []*unstructured.Unstructured) []*unstructured.Unstructured {
			setRule(find(o, "Role", "hpcburst-lifecycle"), "verbs", []any{"*"})
			return o
		}, "wildcard"},
		{"a Role in another namespace", func(o []*unstructured.Unstructured) []*unstructured.Unstructured {
			find(o, "Role", "hpcburst-front").SetNamespace("default")
			return o
		}, "unexpected Role"},
		{"a RoleBinding to a ClusterRole", func(o []*unstructured.Unstructured) []*unstructured.Unstructured {
			_ = unstructured.SetNestedField(find(o, "RoleBinding", "hpcburst-front").Object, "ClusterRole", "roleRef", "kind")
			return o
		}, "role ClusterRole"},
		{"a RoleBinding to another subject", func(o []*unstructured.Unstructured) []*unstructured.Unstructured {
			b := find(o, "RoleBinding", "hpcburst-front")
			_ = unstructured.SetNestedSlice(b.Object, []any{map[string]any{"kind": "Group", "name": "system:authenticated"}}, "subjects")
			return o
		}, "subject"},
		{"a ClusterRole missing", func(o []*unstructured.Unstructured) []*unstructured.Unstructured {
			return slices.DeleteFunc(o, func(x *unstructured.Unstructured) bool {
				return x.GetKind() == "ClusterRole" && x.GetName() == "hpcburst-front"
			})
		}, "ClusterRoles"},
		{"a ServiceAccount missing", func(o []*unstructured.Unstructured) []*unstructured.Unstructured {
			return slices.DeleteFunc(o, func(x *unstructured.Unstructured) bool {
				return x.GetKind() == "ServiceAccount" && x.GetName() == "hpcburst-front"
			})
		}, "ServiceAccounts"},
		{"a fourth ServiceAccount", func(o []*unstructured.Unstructured) []*unstructured.Unstructured {
			extra := find(o, "ServiceAccount", "hpcburst-front").DeepCopy()
			extra.SetName("extra")
			return append(o, extra)
		}, "ServiceAccounts"},
		{"a ServiceAccount of another namespace", func(o []*unstructured.Unstructured) []*unstructured.Unstructured {
			find(o, "ServiceAccount", "hpcburst-front").SetNamespace("default")
			return o
		}, "unexpected ServiceAccount"},
		{"an object of another kind", func(o []*unstructured.Unstructured) []*unstructured.Unstructured {
			extra := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Secret", "metadata": map[string]any{"name": "x", "namespace": "hpcburst-system"}}}
			return append(o, extra)
		}, "unexpected Secret"},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err := controllerManifestOf(c.mutate(decode()))
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("the check answered %v, want a refusal naming %q", err, c.want)
			}
		})
	}
}

func TestControllerTokenLifeAndWarningAreTheRuledOnes(t *testing.T) {
	if ControllerTokenSeconds != 365*24*3600 {
		t.Errorf("a controller's token is asked for %d s, want one year", ControllerTokenSeconds)
	}
	if ControllerTokenWarnWithin != 30*24*time.Hour {
		t.Errorf("status warns within %v, want 30 days", ControllerTokenWarnWithin)
	}
	if got := ControllerIdentityNames(); !slices.Equal(got, []string{"hpcburst-lifecycle", "hpcburst-provider", "hpcburst-front"}) {
		t.Errorf("the controller identities: %v", got)
	}
}

// fakeTokenCluster is a clientset whose TokenRequest answers with token and expiry, recording what was
// asked, so the helper that writes the files runs without an API server.
func fakeTokenCluster(token string, expiry time.Time, asked *[]string, seconds *[]int64) *kubefake.Clientset {
	cs := kubefake.NewSimpleClientset()
	cs.PrependReactor("create", "serviceaccounts", func(a clienttesting.Action) (bool, runtime.Object, error) {
		if a.GetSubresource() != "token" {
			return false, nil, nil
		}
		ca := a.(clienttesting.CreateAction)
		tr := ca.GetObject().(*authenticationv1.TokenRequest)
		*asked = append(*asked, ca.GetNamespace()+"/"+ca.(clienttesting.CreateActionImpl).Name)
		*seconds = append(*seconds, *tr.Spec.ExpirationSeconds)
		return true, &authenticationv1.TokenRequest{Status: authenticationv1.TokenRequestStatus{Token: token, ExpirationTimestamp: metav1.NewTime(expiry)}}, nil
	})
	return cs
}

// TestIssueControllerTokensWritesTheKubeconfigsAndNothingElse: one token per controller identity into
// <state>/identities/system/<name>.kubeconfig, mode 600 in directories of mode 700 (an older directory that
// is looser is set right), for a year, as the identity the API server reports; the token is in the file, and
// in no result, no log line and no error.
func TestIssueControllerTokensWritesTheKubeconfigsAndNothingElse(t *testing.T) {
	l, err := newLayout(filepath.Join(shortDir(t), "st"))
	if err != nil {
		t.Fatal(err)
	}
	// An earlier release's identities directory, looser than it should be.
	if err := os.MkdirAll(filepath.Join(l.dir, "identities"), 0o755); err != nil {
		t.Fatal(err)
	}
	token := randomToken(t)
	expiry := time.Date(2027, 11, 17, 8, 0, 0, 0, time.UTC)
	var asked []string
	var seconds []int64
	cs := fakeTokenCluster(token, expiry, &asked, &seconds)
	admin := &rest.Config{Host: "https://127.0.0.1:28521", TLSClientConfig: rest.TLSClientConfig{Insecure: true}}
	var log bytes.Buffer
	ids, err := issueControllerTokens(context.Background(), cs, admin, l, &log)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"hpcburst-system/hpcburst-lifecycle", "hpcburst-system/hpcburst-provider", "hpcburst-system/hpcburst-front"}; !slices.Equal(asked, want) {
		t.Errorf("tokens were requested for %v, want %v", asked, want)
	}
	for _, s := range seconds {
		if s != 365*24*3600 {
			t.Errorf("a token was requested for %d s, want a year", s)
		}
	}
	for _, d := range []string{filepath.Join(l.dir, "identities"), filepath.Join(l.dir, "identities", "system")} {
		if fi, err := os.Stat(d); err != nil || fi.Mode().Perm() != 0o700 {
			t.Errorf("%s: %v, %v; want mode 700", d, fi, err)
		}
	}
	for i, name := range []string{"hpcburst-lifecycle", "hpcburst-provider", "hpcburst-front"} {
		path := filepath.Join(l.dir, "identities", "system", name+".kubeconfig")
		fi, err := os.Stat(path)
		if err != nil || fi.Mode().Perm() != 0o600 {
			t.Errorf("%s: %v, %v; want mode 600", path, fi, err)
		}
		kc, err := clientcmd.LoadFromFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if got := kc.AuthInfos[name]; got == nil || got.Token != token {
			t.Errorf("%s does not hold the token of %s", path, name)
		}
		if ids[i].Name != name || ids[i].Kubeconfig != path || !ids[i].Expires.Equal(expiry) || ids[i].Username != "system:serviceaccount:hpcburst-system:"+name {
			t.Errorf("the result for %s: %+v", name, ids[i])
		}
	}
	res, _ := json.Marshal(ids)
	if strings.Contains(string(res)+log.String(), token) {
		t.Errorf("the token is in the result or the log:\n%s\n%s", res, log.String())
	}
	if log.Len() == 0 {
		t.Error("nothing was logged: the check above proves nothing")
	}
}

// A token request that fails stops the issue and names the ServiceAccount, never a token.
func TestIssueControllerTokensFailsOnTheFirstRefusal(t *testing.T) {
	l, err := newLayout(filepath.Join(shortDir(t), "st"))
	if err != nil {
		t.Fatal(err)
	}
	cs := kubefake.NewSimpleClientset()
	cs.PrependReactor("create", "serviceaccounts", func(a clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("forbidden by a test")
	})
	_, err = issueControllerTokens(context.Background(), cs, &rest.Config{Host: "https://127.0.0.1:28521", TLSClientConfig: rest.TLSClientConfig{Insecure: true}}, l, nil)
	if err == nil || !strings.Contains(err.Error(), "hpcburst-lifecycle") {
		t.Fatalf("the issue answered %v, want an error naming the first ServiceAccount", err)
	}
	if _, err := os.Stat(filepath.Join(l.dir, "identities", "system", "hpcburst-lifecycle.kubeconfig")); err == nil {
		t.Error("a kubeconfig was written for a token that was refused")
	}
}

// TestSupervisedChildrenNameOnlyTheirOwnKubeconfig: the controller's command line names its two
// identities' kubeconfigs by path, exactly once each, the front's names its one, and no other supervised
// process (frps, the bulk frps, the keeper, the durable log) names a kubeconfig at all; and no token is in any
// command line or environment. A token is planted in the files first, and read back from them, so that its
// absence from the argv means something.
func TestSupervisedChildrenNameOnlyTheirOwnKubeconfig(t *testing.T) {
	l, err := newLayout(filepath.Join(shortDir(t), "st"))
	if err != nil {
		t.Fatal(err)
	}
	token := randomToken(t)
	writePlaceholderKubeconfigs(t, l, token)
	for _, name := range ControllerIdentityNames() {
		if b, err := os.ReadFile(filepath.Join(l.dir, "identities", "system", name+".kubeconfig")); err != nil || !strings.Contains(string(b), token) {
			t.Fatalf("the planted token is not in the file of %s (%v)", name, err)
		}
	}
	ports, err := PortsFromBase(28520)
	if err != nil {
		t.Fatal(err)
	}
	st := &State{
		Ports: ports, Front: Front{Mode: "host-dialed"}, RelayKeeper: true, BulkPath: true,
		Controllers: &ControllerConfig{Binary: "/opt/hpcburst-controller"},
		FrontRun:    &FrontRunConfig{Binary: "/opt/hpcburst-front", Config: "/etc/front.json"},
		Frps:        &FrpsRunConfig{Binary: "/opt/frp/frps", Version: "0.71.1"},
		EventLog:    &EventLogConfig{Floor: 1},
	}
	set, err := st.supervisedSet(l)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, sp := range set {
		names = append(names, sp.Name)
	}
	slices.Sort(names)
	if want := []string{"controller", "eventlog", "front", "frps", "frps-bulk", "relaykeeper"}; !slices.Equal(names, want) {
		t.Fatalf("the supervised set is %v, want %v: the test must cover every process", names, want)
	}
	lifecycle := l.dir + "/identities/system/hpcburst-lifecycle.kubeconfig"
	provider := l.dir + "/identities/system/hpcburst-provider.kubeconfig"
	front := l.dir + "/identities/system/hpcburst-front.kubeconfig"
	for _, sp := range set {
		all := strings.Join(append(slices.Clone(sp.Args), sp.Env...), "\x00")
		if strings.Contains(all, token) {
			t.Errorf("the token is on the command line or in the environment of %s", sp.Name)
		}
		count := func(flag string) (n int, val string) {
			for i, a := range sp.Args {
				if a == flag {
					n++
					if i+1 < len(sp.Args) {
						val = sp.Args[i+1]
					}
				}
			}
			return n, val
		}
		switch sp.Name {
		case "controller":
			if n, v := count("--kubeconfig"); n != 1 || v != lifecycle {
				t.Errorf("the controller's --kubeconfig: %d times, %q; want once, %q", n, v, lifecycle)
			}
			if n, v := count("--provider-kubeconfig"); n != 1 || v != provider {
				t.Errorf("the controller's --provider-kubeconfig: %d times, %q; want once, %q", n, v, provider)
			}
			if strings.Contains(strings.Join(sp.Args, " "), front) {
				t.Error("the controller is told the front's kubeconfig")
			}
		case "front":
			if n, v := count("--kubeconfig"); n != 1 || v != front {
				t.Errorf("the front's --kubeconfig: %d times, %q; want once, %q", n, v, front)
			}
			if n, _ := count("--provider-kubeconfig"); n != 0 {
				t.Error("the front is told a provider kubeconfig")
			}
		default:
			if strings.Contains(all, "kubeconfig") || strings.Contains(all, "identities") {
				t.Errorf("%s names a kubeconfig: %q", sp.Name, sp.Args)
			}
		}
	}
	// The admin kubeconfig is named by none of them.
	for _, sp := range set {
		for _, a := range sp.Args {
			if a == l.kubeconfig() || a == l.adminKubeconfig() {
				t.Errorf("%s is told the admin kubeconfig %s", sp.Name, a)
			}
		}
	}
}

// TestARunningControllerAndFrontAreToldTheirIdentitiesAndHoldNoToken: the same through real child
// processes, which the supervisor starts: the argv and the environment the fake recorded name the paths and
// hold no token.
func TestARunningControllerAndFrontAreToldTheirIdentitiesAndHoldNoToken(t *testing.T) {
	f := newCtlFixture(t, Front{Mode: "direct", Addr: "front.example.test:8443"})
	token := randomToken(t)
	writePlaceholderKubeconfigs(t, f.l, token)
	f.supervise(SuperviseOptions{})
	pid := f.awaitRunning().Proc.PID
	argv, env := f.argvOf(pid), f.envOf(pid)
	i := slices.Index(argv, "--kubeconfig")
	j := slices.Index(argv, "--provider-kubeconfig")
	if i < 0 || j < 0 || argv[i+1] != f.l.dir+"/identities/system/hpcburst-lifecycle.kubeconfig" || argv[j+1] != f.l.dir+"/identities/system/hpcburst-provider.kubeconfig" {
		t.Errorf("the running controller's argv: %q", argv)
	}
	if strings.Contains(strings.Join(argv, "\x00")+strings.Join(env, "\x00"), token) {
		t.Error("the planted token is on the running controller's command line or in its environment")
	}
}

// supervisorStartOrder records the steps startControllerSupervisor runs.
func supervisorStartOrder(t *testing.T, prepareErr error) (calls *[]string) {
	t.Helper()
	old := supervisorStartSteps
	t.Cleanup(func() { supervisorStartSteps = old })
	var got []string
	supervisorStartSteps.prepare = func(ctx context.Context, l layout, ports Ports, log io.Writer) ([]ControllerIdentity, error) {
		got = append(got, "prepare")
		return []ControllerIdentity{{Name: "hpcburst-lifecycle"}}, prepareErr
	}
	supervisorStartSteps.start = func(ctx context.Context, l layout, st *State, argv []string, timeout time.Duration, log io.Writer) error {
		got = append(got, "start")
		return nil
	}
	return &got
}

// TestTheSupervisorStartsOnlyAfterTheIdentitiesAreReady: the one function up and upgrade call prepares the
// identities, then starts the supervisor; a failed preparation starts none, and its error comes back.
func TestTheSupervisorStartsOnlyAfterTheIdentitiesAreReady(t *testing.T) {
	st := &State{Ports: Ports{API: 28520}}
	l := layout{"/run/st"}
	t.Run("in order", func(t *testing.T) {
		calls := supervisorStartOrder(t, nil)
		ids, err := startControllerSupervisor(context.Background(), l, st, nil, time.Second, nil)
		if err != nil || len(ids) != 1 {
			t.Fatalf("ids %v, err %v", ids, err)
		}
		if !slices.Equal(*calls, []string{"prepare", "start"}) {
			t.Errorf("the steps ran %v, want the identities first", *calls)
		}
	})
	t.Run("a failed preparation starts nothing", func(t *testing.T) {
		boom := errors.New("the token request was refused")
		calls := supervisorStartOrder(t, boom)
		if _, err := startControllerSupervisor(context.Background(), l, st, nil, time.Second, nil); !errors.Is(err, boom) {
			t.Fatalf("the error is %v, want %v", err, boom)
		}
		if !slices.Equal(*calls, []string{"prepare"}) {
			t.Errorf("the steps ran %v, want only the preparation", *calls)
		}
	})
}

// TestControllerIdentitiesAreMadeReadyInOrder: the namespace, then the roles, then the tokens; a step that
// fails stops the later ones.
func TestControllerIdentitiesAreMadeReadyInOrder(t *testing.T) {
	old := controllerIdentitySteps
	t.Cleanup(func() { controllerIdentitySteps = old })
	var got []string
	fail := ""
	controllerIdentitySteps.namespace = func(ctx context.Context, cfg *rest.Config) error {
		got = append(got, "namespace")
		return stepErr(fail, "namespace")
	}
	controllerIdentitySteps.install = func(ctx context.Context, cfg *rest.Config, log io.Writer) error {
		got = append(got, "install")
		return stepErr(fail, "install")
	}
	controllerIdentitySteps.issue = func(ctx context.Context, cfg *rest.Config, l layout, log io.Writer) ([]ControllerIdentity, error) {
		got = append(got, "issue")
		return nil, stepErr(fail, "issue")
	}
	for _, c := range []struct {
		fail string
		want []string
	}{
		{"", []string{"namespace", "install", "issue"}},
		{"namespace", []string{"namespace"}},
		{"install", []string{"namespace", "install"}},
		{"issue", []string{"namespace", "install", "issue"}},
	} {
		got, fail = nil, c.fail
		_, err := prepareControllerIdentitiesWith(context.Background(), &rest.Config{}, layout{"/run/st"}, nil)
		if (err != nil) != (c.fail != "") || !slices.Equal(got, c.want) {
			t.Errorf("failing %q: steps %v, error %v; want %v", c.fail, got, err, c.want)
		}
	}
}

func stepErr(fail, step string) error {
	if fail == step {
		return errors.New(step + " failed")
	}
	return nil
}

// TestOnlyStartControllerSupervisorStartsTheSupervisor reads the package's sources: startSupervisor is
// called by no function of its own but through supervisorStartSteps, and startControllerSupervisor is
// called by exactly up (startControllers) and upgrade (Upgrade). So no path can start the supervisor and
// leave out the identities, and a third path would have to be added here by name.
func TestOnlyStartControllerSupervisorStartsTheSupervisor(t *testing.T) {
	fset := gotoken.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(fi fs.FileInfo) bool { return !strings.HasSuffix(fi.Name(), "_test.go") }, 0)
	if err != nil {
		t.Fatal(err)
	}
	direct := map[string]bool{}
	callers := map[string]bool{}
	for _, pkg := range pkgs {
		for _, file := range pkg.Files {
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					call, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					switch f := call.Fun.(type) {
					case *ast.Ident:
						switch f.Name {
						case "startSupervisor":
							direct[fn.Name.Name] = true
						case "startControllerSupervisor":
							callers[fn.Name.Name] = true
						}
					}
					return true
				})
			}
		}
	}
	if len(direct) != 0 {
		t.Errorf("startSupervisor is called directly by %v: it must be reached through startControllerSupervisor", direct)
	}
	if want := map[string]bool{"startControllers": true, "Upgrade": true}; len(callers) != len(want) || !callers["startControllers"] || !callers["Upgrade"] {
		t.Errorf("startControllerSupervisor is called by %v, want up's startControllers and Upgrade only", callers)
	}
	// The guard reads something: the wrapper itself calls the step variable, not the function.
	if _, err := os.Stat("controller_identities.go"); err != nil {
		t.Fatal(err)
	}
}

// startSupervisor refuses a record whose children are told a kubeconfig that is not there, or not mode 600,
// naming the file, and starts nothing.
func TestStartSupervisorRefusesMissingOrLooseKubeconfigs(t *testing.T) {
	f := newCtlFixture(t, Front{})
	path := f.l.dir + "/identities/system/hpcburst-provider.kubeconfig"
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	err := startSupervisor(context.Background(), f.l, f.st, []string{"/bin/true"}, time.Second, nil)
	if err == nil || !strings.Contains(err.Error(), path) || !strings.Contains(err.Error(), "mode 600") {
		t.Fatalf("a kubeconfig of mode 644: %v", err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := startSupervisor(context.Background(), f.l, f.st, []string{"/bin/true"}, time.Second, nil); err == nil || !strings.Contains(err.Error(), path) {
		t.Fatalf("a kubeconfig that is not there: %v", err)
	}
	if len(f.st.Procs) != 1 {
		t.Errorf("a supervisor was recorded after the refusals: %+v", f.st.Procs)
	}
}
