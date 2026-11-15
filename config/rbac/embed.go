// Package rbac embeds the framework's RBAC manifest, so that the launcher installs the same bytes
// the repository holds: one source, no copy (HBEP-10, Identities and roles; issue #589). The YAML
// file is written by hand; hack/generate.sh does not touch it.
package rbac

import _ "embed"

// Agents is agents.yaml: the namespace the agents' ServiceAccounts live in, the ClusterRoles that
// bound them, and the two ClusterRoleBindings that give two of those roles to the agents' group.
//
//go:embed agents.yaml
var Agents []byte
