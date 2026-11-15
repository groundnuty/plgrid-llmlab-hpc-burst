package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ApprovalPolicySpec says who may approve in one scope (HBEP-10, Approval
// policy).
type ApprovalPolicySpec struct {
	// Scope is where the policy applies: exactly one of global, a Provider, a
	// namespace or a ModelDeployment. There is one policy per scope.
	Scope ApprovalScope `json:"scope"`

	// Approvers are the users, groups and ServiceAccounts that may approve under
	// this policy, in any mix. A ServiceAccount is a user to the API server, so an
	// agent approves as a person does (HBEP-10, Approval policy).
	//
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=64
	// +kubebuilder:validation:items:XValidation:rule="((has(self.user) ? 1 : 0) + (has(self.group) ? 1 : 0) + (has(self.serviceAccount) ? 1 : 0)) == 1",message="an approver names exactly one of user, group and serviceAccount"
	Approvers []Approver `json:"approvers"`
}

// ApprovalScope is where an ApprovalPolicy applies (HBEP-10, Approval policy,
// Resolution). Exactly one of its fields is set.
//
// +kubebuilder:validation:XValidation:rule="((has(self.global) ? 1 : 0) + (has(self.provider) ? 1 : 0) + (has(self.namespace) ? 1 : 0) + (has(self.modelDeployment) ? 1 : 0)) == 1",message="a scope names exactly one of global, provider, namespace and modelDeployment"
type ApprovalScope struct {
	// Global makes the policy the fallback for every target that no Provider or
	// namespace policy covers. When set it is true: to set no global scope, name
	// another one.
	//
	// +optional
	// +kubebuilder:validation:XValidation:rule="self == true",message="global is true when it is set: to scope a policy otherwise, omit global and name a provider, a namespace or a modelDeployment"
	Global *bool `json:"global,omitempty"`

	// Provider is the name of a Provider: the policy applies to targets on it.
	//
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	Provider string `json:"provider,omitempty"`

	// Namespace is the name of a namespace: the policy applies to every
	// ModelDeployment in it that has no policy of its own.
	//
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	Namespace string `json:"namespace,omitempty"`

	// ModelDeployment names one ModelDeployment: the policy applies to it, and
	// overrides its namespace's.
	//
	// +optional
	ModelDeployment *ScopedDeployment `json:"modelDeployment,omitempty"`
}

// ScopedDeployment names a ModelDeployment by namespace and name.
type ScopedDeployment struct {
	// Namespace of the ModelDeployment.
	//
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	Namespace string `json:"namespace"`

	// Name of the ModelDeployment.
	//
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`
}

// Approver is one party an ApprovalPolicy names: exactly one of a user, a group
// and a ServiceAccount (HBEP-10, Approval policy). The rule that it names exactly
// one is on ApprovalPolicySpec.Approvers, not on this type, because a plan's
// status copies the approvers of each policy it applied (AppliedPolicy) and
// status needs no rule.
type Approver struct {
	// User is a user name as the API server reports it.
	//
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=1024
	User string `json:"user,omitempty"`

	// Group is a group name as the API server reports it.
	//
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=1024
	Group string `json:"group,omitempty"`

	// ServiceAccount is a ServiceAccount, which is the user
	// "system:serviceaccount:<namespace>:<name>" to the API server. It names
	// both its namespace and its name.
	//
	// +optional
	ServiceAccount *ServiceAccountRef `json:"serviceAccount,omitempty"`
}

// ServiceAccountRef names a ServiceAccount.
type ServiceAccountRef struct {
	// Namespace of the ServiceAccount.
	//
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	Namespace string `json:"namespace"`

	// Name of the ServiceAccount.
	//
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`
}

// ApprovalPolicy says who may approve an Operation's plan in one scope (HBEP-10,
// Approval policy). It is cluster-scoped, so only cluster-wide RBAC can write it:
// persons, never agents. It has no status.
//
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,categories=hpcburst
// +kubebuilder:printcolumn:name="Scope",type=string,JSONPath=`.spec.scope.*`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type ApprovalPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec ApprovalPolicySpec `json:"spec"`
}

// ApprovalPolicyList is a list of ApprovalPolicies.
//
// +kubebuilder:object:root=true
type ApprovalPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ApprovalPolicy `json:"items"`
}

func init() { SchemeBuilder.Register(&ApprovalPolicy{}, &ApprovalPolicyList{}) }
