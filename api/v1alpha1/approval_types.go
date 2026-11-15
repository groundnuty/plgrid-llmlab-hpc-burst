package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// ApprovalSpec is one approver's approval of one plan (HBEP-10, Approvals). The
// spec is immutable once created. The approver pin, an admission policy, refuses
// a create unless approver equals the requester's user name and groups, so an
// approval is accepted only from the approver's own identity (HBEP-10,
// Approvals; HBEP-11, Admission).
//
// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="an Approval's spec is immutable: it is bound to one plan and one approver; create a new Approval instead (HBEP-10, Approvals)"
type ApprovalSpec struct {
	// Operation names the Operation the approval is for, by name and UID, so an
	// Operation deleted and created again under the same name gets no approval
	// from before.
	Operation OperationReference `json:"operation"`

	// PlanID is the id of the plan approved (status.plan.id of the Operation). The
	// lifecycle controller counts the Approval only while this is the frozen
	// plan's id (HBEP-10, Approvals).
	//
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=64
	PlanID string `json:"planID"`

	// Approver is who approves: the user name and groups of the identity that
	// created the Approval, pinned at admission.
	Approver ApproverIdentity `json:"approver"`
}

// OperationReference names an Operation in the Approval's own namespace, with
// its UID.
type OperationReference struct {
	// Name is the Operation's name.
	//
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`

	// UID is the Operation's UID.
	//
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=64
	UID types.UID `json:"uid"`
}

// ApproverIdentity is the identity an Approval records: the user name and the
// groups the API server reported for the requester (HBEP-10, Approvals). A
// ServiceAccount is a user to the API server, so an agent's name is its
// "system:serviceaccount:<namespace>:<name>".
type ApproverIdentity struct {
	// Username is the approver's user name as the API server reports it.
	//
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=1024
	Username string `json:"username"`

	// Groups are the approver's groups as the API server reports them. A policy
	// that names a group is satisfied by an approver whose recorded groups
	// include it.
	//
	// +optional
	// +kubebuilder:validation:MaxItems=64
	// +kubebuilder:validation:items:MaxLength=1024
	Groups []string `json:"groups,omitempty"`
}

// Approval is one approver's approval of one Operation's plan, in the
// Operation's namespace (HBEP-10, Approvals). It has no status: the lifecycle
// controller reads it and writes nothing to it. It is immutable once created.
//
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced,categories=hpcburst
// +kubebuilder:printcolumn:name="Operation",type=string,JSONPath=`.spec.operation.name`
// +kubebuilder:printcolumn:name="Approver",type=string,JSONPath=`.spec.approver.username`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type Approval struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec ApprovalSpec `json:"spec"`
}

// ApprovalList is a list of Approvals.
//
// +kubebuilder:object:root=true
type ApprovalList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Approval `json:"items"`
}

func init() { SchemeBuilder.Register(&Approval{}, &ApprovalList{}) }
