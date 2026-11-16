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

// CapacityGuard is capacity-guard.yaml: the ValidatingAdmissionPolicy that bounds what an agent
// identity may write to a ModelDeployment, an Operation and the objects the controllers own, and the
// binding that makes it Deny (HBEP-10, The capacity guard; issue #589).
//
//go:embed capacity-guard.yaml
var CapacityGuard []byte

// OwnerPin is owner-pin.yaml: the policy that fixes the owner of a ModelDeployment and an Operation
// and binds a confirm to the owner and the plan, and its Deny binding (HBEP-10, Owner and purpose,
// Confirm).
//
//go:embed owner-pin.yaml
var OwnerPin []byte

// ApproverPin is approver-pin.yaml: the policy that accepts an Approval only from the approver's own
// identity, and its Deny binding (HBEP-10, Approvals).
//
//go:embed approver-pin.yaml
var ApproverPin []byte
