// Package rbac embeds the framework's RBAC manifest, so that the launcher installs the same bytes
// the repository holds: one source, no copy (HBEP-10, Identities and roles; issue #589). The YAML
// file is written by hand; hack/generate.sh does not touch it.
package rbac

import _ "embed"

// Agents is agents.yaml: the namespace the agents' ServiceAccounts live in, the ClusterRoles that
// bound them (hpcburst-agent in its wide form, with the writes the admission policies bound), and the
// two ClusterRoleBindings that give two of those roles to the agents' group.
//
//go:embed agents.yaml
var Agents []byte

// AgentsNarrow is agents-narrow.yaml: the read-only form of the ClusterRole hpcburst-agent, which up
// applies before the admission policies are enforced; Agents holds the wide form (HBEP-10,
// Identities and roles; issue #589).
//
//go:embed agents-narrow.yaml
var AgentsNarrow []byte
