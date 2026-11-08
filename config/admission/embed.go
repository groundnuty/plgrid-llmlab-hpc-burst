// Package admission embeds the framework's admission manifests, so that the launcher installs the
// same bytes the repository holds: one source, no copy (HBEP-11, Risks and Admission; issue #242).
// The YAML files are written by hand; hack/generate.sh does not touch them.
package admission

import _ "embed"

// CRDDeletionGuard is crd-deletion-guard.yaml: the ValidatingAdmissionPolicy that refuses deleting
// the framework's custom resource definitions, and the binding that makes it Deny.
//
//go:embed crd-deletion-guard.yaml
var CRDDeletionGuard []byte
