package v1alpha1

import (
	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ProviderConfigKind is the kind an Allocation's providerConfigRef must name:
// our Provider takes the place of crossplane's ProviderConfig (HBEP-3).
const ProviderConfigKind = "Provider"

// AllocationParameters are the submission parameters. They are set when the
// Allocation is created and never edited: a submitted job is not changed
// (HBEP-11, Admission).
type AllocationParameters struct {
	// Partition the job is submitted to.
	//
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Partition string `json:"partition"`

	// Account the job is charged to.
	//
	// +optional
	// +kubebuilder:validation:MaxLength=253
	Account string `json:"account,omitempty"`

	// TimeLimit in the platform's format, for example "01:00:00".
	//
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	TimeLimit string `json:"timeLimit"`

	// Wrap is the command passed to the submission, as in sbatch --wrap.
	//
	// +optional
	// +kubebuilder:validation:MaxLength=8192
	Wrap string `json:"wrap,omitempty"`
}

// AllocationSpec is the desired state of an Allocation. It embeds
// crossplane-runtime's managed-resource fields (HBEP-11, Ownership, records and
// deletion): providerConfigRef names a Provider, managementPolicies and
// writeConnectionSecretToRef are crossplane's.
//
// +kubebuilder:validation:XValidation:rule="has(self.providerConfigRef) && self.providerConfigRef.kind == 'Provider'",message="providerConfigRef must name a Provider: set kind to 'Provider' (crossplane's default, ClusterProviderConfig, is refused)"
// +kubebuilder:validation:XValidation:rule="has(self.providerConfigRef) && self.providerConfigRef.name.matches('^[a-z0-9]([-a-z0-9]*[a-z0-9])?$') && size(self.providerConfigRef.name) <= 63",message="providerConfigRef.name must be a Provider name, a DNS-1123 label"
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.providerConfigRef) || (has(self.providerConfigRef) && self.providerConfigRef == oldSelf.providerConfigRef)",message="an Allocation's providerConfigRef is immutable: the job it holds is on that provider; delete the Allocation and create a new one"
type AllocationSpec struct {
	xpv2.ManagedResourceSpec `json:",inline"`

	// ForProvider holds the submission parameters. They are immutable.
	//
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="an Allocation's submission parameters are immutable: a submitted job is never edited; delete the Allocation and create a new one"
	ForProvider AllocationParameters `json:"forProvider"`

	// ModelDeploymentRef names the ModelDeployment, in the Allocation's
	// namespace, that the Allocation is assigned to. A repurpose changes it.
	// No ModelDeployment owns an Allocation.
	//
	// +optional
	ModelDeploymentRef *ObjectReference `json:"modelDeploymentRef,omitempty"`

	// RetentionPolicy says what happens to the Allocation when it hosts no
	// Instance.
	//
	// +optional
	RetentionPolicy *RetentionPolicy `json:"retentionPolicy,omitempty"`
}

// SubmissionRecord mirrors the latest submission attempt, for display. The
// record itself lives in an annotation (AnnotationSubmission).
type SubmissionRecord struct {
	// Attempt is the attempt's number.
	Attempt int32 `json:"attempt"`

	// Token identifies the attempt.
	//
	// +optional
	Token string `json:"token,omitempty"`

	// State of the attempt, for example InFlight, Unknown, Done or Abandoned.
	State string `json:"state"`

	// Writer is the controller that wrote the record.
	//
	// +optional
	Writer string `json:"writer,omitempty"`

	// Time the record was written.
	//
	// +optional
	Time *metav1.Time `json:"time,omitempty"`

	// Reason for the state.
	//
	// +optional
	Reason string `json:"reason,omitempty"`
}

// AllocationObservation is what the provider observed about the job. Annotations
// hold the job ID and the submission record; this mirrors them for display
// only (HBEP-11).
type AllocationObservation struct {
	// Tag is the job tag, the Allocation's external name (HBEP-3).
	//
	// +optional
	Tag string `json:"tag,omitempty"`

	// JobID is the established job ID, mirrored from its annotation.
	//
	// +optional
	JobID string `json:"jobID,omitempty"`

	// Submission mirrors the latest submission attempt.
	//
	// +optional
	Submission *SubmissionRecord `json:"submission,omitempty"`

	// Phase of the job as the provider reports it.
	//
	// +optional
	Phase string `json:"phase,omitempty"`

	// StartEstimate is when the platform's scheduler expects the job to start.
	// It is absent while the platform gives no estimate: an unknown estimate
	// is not "now" and not "never". It is not StartTime, which is the actual
	// start, and it moves with every scheduler pass (HBEP-3, Vocabulary,
	// Lease: a lease runs from a start estimate, which may be unknown).
	//
	// +optional
	StartEstimate *metav1.Time `json:"startEstimate,omitempty"`

	// StartTime is when the job actually started. It is absent until it has.
	//
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// HardEnd is the lease's hard end: the time limit as currently observed
	// (HBEP-3, Vocabulary, Lease).
	//
	// +optional
	HardEnd *metav1.Time `json:"hardEnd,omitempty"`

	// Duplicates lists the IDs of extra jobs that carry the tag and are not
	// held, in fallback mode. The list is sticky.
	//
	// +optional
	// +listType=set
	Duplicates []string `json:"duplicates,omitempty"`
}

// AllocationStatus is the observed state of an Allocation.
type AllocationStatus struct {
	xpv2.ManagedResourceStatus `json:",inline"`

	// LastReconcileTime is when the controller last reconciled the object.
	//
	// +optional
	LastReconcileTime *metav1.Time `json:"lastReconcileTime,omitempty"`

	// AtProvider is the observed state of the job.
	//
	// +optional
	AtProvider AllocationObservation `json:"atProvider,omitempty"`
}

// Allocation is capacity with a lifetime, like a Kubernetes node: one lease on
// a provider, such as a Slurm job (HBEP-3). It outlives its Instances and can
// be repurposed. It is a crossplane-runtime managed resource whose external
// name is its job tag. It is deliberately not in crossplane's "managed"
// category, so that "kubectl delete managed --all" on a cluster that also runs
// Crossplane cannot reach Allocations and cancel their jobs.
//
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced,shortName=alloc,categories=hpcburst
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Provider",type=string,JSONPath=`.spec.providerConfigRef.name`
// +kubebuilder:printcolumn:name="Deployment",type=string,JSONPath=`.spec.modelDeploymentRef.name`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.atProvider.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type Allocation struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AllocationSpec   `json:"spec"`
	Status AllocationStatus `json:"status,omitempty"`
}

// AllocationList is a list of Allocations.
//
// +kubebuilder:object:root=true
type AllocationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Allocation `json:"items"`
}

func init() { SchemeBuilder.Register(&Allocation{}, &AllocationList{}) }
