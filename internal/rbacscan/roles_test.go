package rbacscan

import (
	"strings"
	"testing"
)

func TestParseRolesReadsTheBoundRulesOfEachAccount(t *testing.T) {
	roles, err := ParseRoles([]byte(`
apiVersion: v1
kind: ServiceAccount
metadata: {name: sa, namespace: ns}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata: {name: wide}
rules:
- apiGroups: [example.test]
  resources: [things, things/status]
  verbs: [get, list]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata: {name: narrow, namespace: ns}
rules:
- apiGroups: [coordination.k8s.io]
  resources: [leases]
  resourceNames: [one]
  verbs: [get]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata: {name: wide}
roleRef: {apiGroup: rbac.authorization.k8s.io, kind: ClusterRole, name: wide}
subjects: [{kind: ServiceAccount, name: sa, namespace: ns}]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata: {name: narrow, namespace: ns}
roleRef: {apiGroup: rbac.authorization.k8s.io, kind: Role, name: narrow}
subjects: [{kind: ServiceAccount, name: sa, namespace: ns}]
`))
	if err != nil {
		t.Fatal(err)
	}
	want := map[Triple]bool{
		{"example.test", "things", "get"}: true, {"example.test", "things", "list"}: true,
		{"example.test", "things/status", "get"}: true, {"example.test", "things/status", "list"}: true,
		{"coordination.k8s.io", "leases", "get"}: true,
	}
	got := roles["sa"]
	if len(got) != len(want) {
		t.Fatalf("rules %v, want %v", got, want)
	}
	for tr := range want {
		if !got[tr] {
			t.Errorf("missing %v", tr)
		}
	}
}

func TestParseRolesRefusesWhatItCannotCheck(t *testing.T) {
	const sa = "apiVersion: v1\nkind: ServiceAccount\nmetadata: {name: sa}\n---\n"
	for name, tc := range map[string]struct{ doc, want string }{
		"a wildcard verb": {sa + `
kind: ClusterRole
metadata: {name: r}
rules: [{apiGroups: [x], resources: [y], verbs: ["*"]}]
---
kind: ClusterRoleBinding
metadata: {name: r}
roleRef: {kind: ClusterRole, name: r}
subjects: [{kind: ServiceAccount, name: sa}]
`, "wildcard"},
		"a binding to a role the manifest lacks": {sa + `
kind: ClusterRoleBinding
metadata: {name: r}
roleRef: {kind: ClusterRole, name: gone}
subjects: [{kind: ServiceAccount, name: sa}]
`, "does not hold"},
		"a subject that is no account of the manifest": {sa + `
kind: ClusterRole
metadata: {name: r}
rules: []
---
kind: ClusterRoleBinding
metadata: {name: r}
roleRef: {kind: ClusterRole, name: r}
subjects: [{kind: ServiceAccount, name: other}]
`, "no ServiceAccount of the manifest"},
		"a kind it does not read": {sa + "kind: Secret\nmetadata: {name: s}\n", "no ServiceAccount, role or binding"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ParseRoles([]byte(tc.doc))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %v, want one that says %q", err, tc.want)
			}
		})
	}
}
