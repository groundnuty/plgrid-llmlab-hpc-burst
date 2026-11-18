package rbacscan

import (
	"bytes"
	"fmt"
	"sort"

	rbacv1 "k8s.io/api/rbac/v1"
	"sigs.k8s.io/yaml"
)

// Roles are what each identity is granted, as the triples its roles hold. resourceNames are not read: which Lease a Role
// names is the launcher's drift guard's (internal/launcher), and a call here is checked for its resource and verb.
type Roles map[string]map[Triple]bool

// ParseRoles reads a manifest of ServiceAccounts, Roles, ClusterRoles and their bindings, and returns for each
// ServiceAccount the union of the rules of every role bound to it.
func ParseRoles(manifest []byte) (Roles, error) {
	type doc struct {
		Kind string `json:"kind"`
	}
	roles := map[string][]rbacv1.PolicyRule{} // "Role/<name>" and "ClusterRole/<name>"
	type binding struct {
		roleKind, roleName string
		subjects           []rbacv1.Subject
	}
	var bindings []binding
	accounts := map[string]bool{}
	for _, part := range bytes.Split(manifest, []byte("\n---")) {
		if len(bytes.TrimSpace(part)) == 0 {
			continue
		}
		var d doc
		if err := yaml.Unmarshal(part, &d); err != nil {
			return nil, err
		}
		switch d.Kind {
		case "ServiceAccount":
			var sa struct{ Metadata struct{ Name string } }
			if err := yaml.Unmarshal(part, &sa); err != nil {
				return nil, err
			}
			accounts[sa.Metadata.Name] = true
		case "Role":
			var r rbacv1.Role
			if err := yaml.UnmarshalStrict(part, &r); err != nil {
				return nil, err
			}
			roles["Role/"+r.Name] = r.Rules
		case "ClusterRole":
			var r rbacv1.ClusterRole
			if err := yaml.UnmarshalStrict(part, &r); err != nil {
				return nil, err
			}
			roles["ClusterRole/"+r.Name] = r.Rules
		case "RoleBinding":
			var b rbacv1.RoleBinding
			if err := yaml.UnmarshalStrict(part, &b); err != nil {
				return nil, err
			}
			bindings = append(bindings, binding{b.RoleRef.Kind, b.RoleRef.Name, b.Subjects})
		case "ClusterRoleBinding":
			var b rbacv1.ClusterRoleBinding
			if err := yaml.UnmarshalStrict(part, &b); err != nil {
				return nil, err
			}
			bindings = append(bindings, binding{b.RoleRef.Kind, b.RoleRef.Name, b.Subjects})
		default:
			return nil, fmt.Errorf("the manifest holds a %q, which is no ServiceAccount, role or binding", d.Kind)
		}
	}
	out := Roles{}
	for _, b := range bindings {
		rules, ok := roles[b.roleKind+"/"+b.roleName]
		if !ok {
			return nil, fmt.Errorf("a binding names the %s %s, which the manifest does not hold", b.roleKind, b.roleName)
		}
		for _, sub := range b.subjects {
			if sub.Kind != "ServiceAccount" || !accounts[sub.Name] {
				return nil, fmt.Errorf("a binding of %s has the subject %s %s, which is no ServiceAccount of the manifest", b.roleName, sub.Kind, sub.Name)
			}
			if out[sub.Name] == nil {
				out[sub.Name] = map[Triple]bool{}
			}
			for _, rule := range rules {
				for _, g := range rule.APIGroups {
					for _, r := range rule.Resources {
						for _, v := range rule.Verbs {
							if g == "*" || r == "*" || v == "*" {
								return nil, fmt.Errorf("the role %s grants a wildcard: %v %v %v", b.roleName, rule.APIGroups, rule.Resources, rule.Verbs)
							}
							out[sub.Name][Triple{g, r, v}] = true
						}
					}
				}
			}
		}
	}
	return out, nil
}

// Clone returns a copy a mutant can change.
func (r Roles) Clone() Roles {
	out := Roles{}
	for id, set := range r {
		out[id] = map[Triple]bool{}
		for t := range set {
			out[id][t] = true
		}
	}
	return out
}

// Without returns a copy with one triple taken from one identity.
func (r Roles) Without(ident string, t Triple) Roles {
	out := r.Clone()
	delete(out[ident], t)
	return out
}

// With returns a copy with one triple added to one identity.
func (r Roles) With(ident string, t Triple) Roles {
	out := r.Clone()
	if out[ident] == nil {
		out[ident] = map[Triple]bool{}
	}
	out[ident][t] = true
	return out
}

// Idents lists the identities, sorted.
func (r Roles) Idents() []string {
	var out []string
	for id := range r {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}
