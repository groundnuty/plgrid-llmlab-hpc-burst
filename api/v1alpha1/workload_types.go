package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Workload declares one class of a study's requests (HBEP-3; HBEP-5, Workloads; HBEP-764).
// ModelDeployments in its namespace name the ones they serve in spec.workloads, and a
// request names one by the front's workloadHeader or a "#<name>" suffix on its model
// name. A Workload is a routing input only: warm-up and qualification keep each
// ModelDeployment's own spec.workload, so no edit of a Workload releases capacity.
// It has no status.
//
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced,categories=hpcburst
// +kubebuilder:printcolumn:name="Model",type=string,JSONPath=`.spec.model`
// +kubebuilder:printcolumn:name="Route order",type=string,JSONPath=`.spec.routeOrder`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:validation:XValidation:rule="self.metadata.name.matches('^[a-z0-9]([-a-z0-9]*[a-z0-9])?$') && size(self.metadata.name) <= 63",message="a Workload name must be a DNS-1123 label: lowercase letters, digits and '-', starting and ending with a letter or digit, at most 63 characters, since a request names it after '#' in a model name (HBEP-3, Grammar)"
type Workload struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec WorkloadSpec `json:"spec"`
}

// WorkloadSpec is what a Workload declares (HBEP-5, Workloads).
//
// +kubebuilder:validation:XValidation:rule="!has(self.routeOrder) || self.routeOrder != 'ManagedFirst' || has(self.managedSessions)",message="routeOrder ManagedFirst needs managedSessions: the front can't see a managed provider's capacity, so the Workload's own bound on its requests there is required (HBEP-5, Workloads, Route order)"
type WorkloadSpec struct {
	// Model is the model name the Workload's requests name, as a ModelDeployment's
	// spec.model is. It never contains '#', which separates a Workload in a request's
	// model name (HBEP-3, Grammar).
	//
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=256
	// +kubebuilder:validation:XValidation:rule="!self.contains('#')",message="a model name never contains '#': a request's model name splits a Workload off at its only '#' (HBEP-3, Grammar)"
	Model string `json:"model"`

	// Shape is the Workload's typical and longest context and output, in tokens. The
	// front charges each of its requests contextTokens.typical plus outputTokens.typical
	// against an Instance's token budget (HBEP-5, Workloads, The count in tokens).
	Shape WorkloadShape `json:"shape"`

	// Sessions is how many sessions the study expects in parallel. It is informational
	// until planning for pre-staging reads it (HBEP-764, R6).
	//
	// +optional
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=1024
	Sessions *int32 `json:"sessions,omitempty"`

	// RouteOrder is which route takes the Workload's requests first. OwnFirst, the
	// default, is HBEP-5's order: LLMLab takes overflow only. ManagedFirst sends a
	// request to an eligible managed target that serves the Workload first, while the
	// Workload's requests in flight on managed targets are fewer than managedSessions.
	//
	// +optional
	// +kubebuilder:default=OwnFirst
	// +kubebuilder:validation:Enum=OwnFirst;ManagedFirst
	RouteOrder RouteOrder `json:"routeOrder,omitempty"`

	// ManagedSessions bounds the Workload's requests in flight on managed targets: required
	// with ManagedFirst, and with OwnFirst it bounds the overflow (HBEP-5, Workloads, Route order).
	//
	// +optional
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=1024
	ManagedSessions *int32 `json:"managedSessions,omitempty"`
}

// WorkloadShape is a Workload's shape: the context and output fields of a
// ModelDeployment's workload block (DeploymentWorkload), with the same types and rules.
type WorkloadShape struct {
	// ContextTokens are the context lengths, in tokens.
	ContextTokens ContextTokens `json:"contextTokens"`

	// OutputTokens are the output lengths, in tokens.
	OutputTokens OutputTokens `json:"outputTokens"`
}

// RouteOrder is a Workload's route order (HBEP-5, Workloads).
type RouteOrder string

// The route orders.
const (
	RouteOwnFirst     RouteOrder = "OwnFirst"
	RouteManagedFirst RouteOrder = "ManagedFirst"
)

// WorkloadList is a list of Workloads.
//
// +kubebuilder:object:root=true
type WorkloadList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Workload `json:"items"`
}

func init() { SchemeBuilder.Register(&Workload{}, &WorkloadList{}) }
