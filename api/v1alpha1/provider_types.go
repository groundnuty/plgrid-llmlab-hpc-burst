package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ProviderSpec is our access to one platform: provider settings (HBEP-4) and a
// credentials reference. Organization, platform, partition and account are
// also labels on the Provider (HBEP-3), so queries select by label.
type ProviderSpec struct {
	// PlatformProfileRef names the PlatformProfile of the provider's platform.
	// Several providers can share one platform and one profile.
	PlatformProfileRef ObjectReference `json:"platformProfileRef"`

	// Account is the account (or grant account) jobs are charged to. Provider
	// names appear in client model names, so the account is a label and a
	// field here, never part of the name.
	//
	// +optional
	// +kubebuilder:validation:MaxLength=253
	Account string `json:"account,omitempty"`

	// Grant names the grant the account belongs to.
	//
	// +optional
	// +kubebuilder:validation:MaxLength=253
	Grant string `json:"grant,omitempty"`

	// AllowedPartitions lists the platform partitions this provider may use.
	//
	// +optional
	// +listType=set
	// +kubebuilder:validation:MaxItems=64
	// +kubebuilder:validation:items:MinLength=1
	// +kubebuilder:validation:items:MaxLength=253
	AllowedPartitions []string `json:"allowedPartitions,omitempty"`

	// CredentialsRef refers to the provider's credentials: a mode-600 file on
	// the control-plane host, or a Secret. The API never holds credentials.
	//
	// +optional
	CredentialsRef *CredentialsReference `json:"credentialsRef,omitempty"`
}

// ProviderStatus is the observed state of a Provider.
type ProviderStatus struct {
	ReconcileStatus `json:",inline"`

	// LastSuccessfulStatusRead is when the provider's status was last read
	// successfully; its age is the age of the control plane's view.
	//
	// +optional
	LastSuccessfulStatusRead *metav1.Time `json:"lastSuccessfulStatusRead,omitempty"`

	// Conditions of the provider, for example a credentials file that others
	// can read, or an unreachable platform.
	//
	// +optional
	// +listType=map
	// +listMapKey=type
	// +patchStrategy=merge
	// +patchMergeKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

// Provider is our access to one platform: an account or grant, its credentials
// and its settings (HBEP-3). Provider names are DNS-1123 labels because they
// appear in client model names and never contain '@', '/' or '.'.
//
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,categories=hpcburst
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Profile",type=string,JSONPath=`.spec.platformProfileRef.name`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:validation:XValidation:rule="self.metadata.name.matches('^[a-z0-9]([-a-z0-9]*[a-z0-9])?$') && size(self.metadata.name) <= 63",message="a Provider name must be a DNS-1123 label: lowercase letters, digits and '-', starting and ending with a letter or digit, at most 63 characters, so no '.', '@' or '/'"
type Provider struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ProviderSpec   `json:"spec"`
	Status ProviderStatus `json:"status,omitempty"`
}

// ProviderList is a list of Providers.
//
// +kubebuilder:object:root=true
type ProviderList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Provider `json:"items"`
}

func init() { SchemeBuilder.Register(&Provider{}, &ProviderList{}) }
