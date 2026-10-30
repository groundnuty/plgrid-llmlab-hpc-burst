package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// InstanceSpec names the Instance's deployment ID, its identity record and its
// Allocation. It is immutable: an Instance never changes its ID or its
// Allocation (HBEP-3, HBEP-11).
//
// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="an Instance's spec is immutable: replace the Instance instead of editing it"
// +kubebuilder:validation:XValidation:rule="size(self.deploymentID) >= 12 && self.identityRecord == 'id-' + self.deploymentID.substring(0, 12)",message="identityRecord must be 'id-' followed by the first 12 characters of deploymentID"
type InstanceSpec struct {
	// DeploymentID is the full deployment ID.
	DeploymentID FullDeploymentID `json:"deploymentID"`

	// IdentityRecord is the name of the identity record, a ControllerRevision in
	// the Instance's namespace, named id-<first 12 hex of the ID>.
	//
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	IdentityRecord string `json:"identityRecord"`

	// AllocationRef names the Allocation, in the Instance's namespace, that
	// the Instance runs in. An Instance is bound to one Allocation for life.
	AllocationRef ObjectReference `json:"allocationRef"`
}

// InstancePhase is an Instance's phase (HBEP-3, Instance phases). Health is
// reported separately, as conditions.
//
// +kubebuilder:validation:Enum=pending;starting;verifying;ready;draining;ended
type InstancePhase string

// Instance phases.
const (
	InstancePending   InstancePhase = "pending"
	InstanceStarting  InstancePhase = "starting"
	InstanceVerifying InstancePhase = "verifying"
	InstanceReady     InstancePhase = "ready"
	InstanceDraining  InstancePhase = "draining"
	InstanceEnded     InstancePhase = "ended"
)

// Verdict is the outcome of an Instance's verification (HBEP-3, HBEP-9).
//
// +kubebuilder:validation:Enum=pass;fail
type Verdict string

// Verdicts.
const (
	VerdictPass Verdict = "pass"
	VerdictFail Verdict = "fail"
)

// InstanceStatus is the observed state of an Instance. The job ID, node, URL
// and lease end are the Instance's metadata (HBEP-3): a handover replaces the
// Instance, so they never change within one.
type InstanceStatus struct {
	ReconcileStatus `json:",inline"`

	// Phase of the Instance.
	//
	// +optional
	Phase InstancePhase `json:"phase,omitempty"`

	// VerificationRecord is the name of the Instance's verification record, a
	// ControllerRevision written once, pass or fail, before the Instance can
	// become ready.
	//
	// +optional
	VerificationRecord string `json:"verificationRecord,omitempty"`

	// Verdict mirrors the verification record's verdict.
	//
	// +optional
	Verdict Verdict `json:"verdict,omitempty"`

	// JobID is the provider job's ID.
	//
	// +optional
	JobID string `json:"jobID,omitempty"`

	// Node the Instance runs on.
	//
	// +optional
	Node string `json:"node,omitempty"`

	// URL the front reaches the Instance at.
	//
	// +optional
	URL string `json:"url,omitempty"`

	// LeaseEnd is the hard end of the Instance's lease.
	//
	// +optional
	LeaseEnd *metav1.Time `json:"leaseEnd,omitempty"`

	// +optional
	// +listType=map
	// +listMapKey=type
	// +patchStrategy=merge
	// +patchMergeKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

// Instance is one copy of a deployment inside one Allocation, as a lifecycle
// resource (HBEP-3). Its ModelDeployment owns it. It carries a drain finalizer
// (FinalizerDrain).
//
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced,shortName=inst,categories=hpcburst
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Allocation",type=string,JSONPath=`.spec.allocationRef.name`
// +kubebuilder:printcolumn:name="ID",type=string,JSONPath=`.spec.deploymentID`,priority=1
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type Instance struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   InstanceSpec   `json:"spec"`
	Status InstanceStatus `json:"status,omitempty"`
}

// InstanceList is a list of Instances.
//
// +kubebuilder:object:root=true
type InstanceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Instance `json:"items"`
}

func init() { SchemeBuilder.Register(&Instance{}, &InstanceList{}) }
