package v1alpha1

import (
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PlatformKind is the kind of platform (HBEP-3, Vocabulary, Provider).
//
// +kubebuilder:validation:Enum=managed;batch;static
type PlatformKind string

// Platform kinds.
const (
	PlatformManaged PlatformKind = "managed"
	PlatformBatch   PlatformKind = "batch"
	PlatformStatic  PlatformKind = "static"
)

// PartitionProfile is what the framework reads of one partition: its name and
// its accelerator class (HBEP-11, API, PlatformProfile schema).
type PartitionProfile struct {
	// Name of the partition.
	//
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`

	// AcceleratorClass is the partition's GPU model and CPU architecture. They
	// are a deployment-ID input (HBEP-3).
	AcceleratorClass `json:",inline"`
}

// PlatformProfileSpec is the platform's kind and its profile data. HBEP-4
// owns the schema. For alpha the fields that the Slurm provider and the
// deployment ID read are typed; the rest of the profile is an untyped block.
// The typed fields carry the names HBEP-11 gives them.
type PlatformProfileSpec struct {
	// ProviderKind is managed, batch or static: the kind of provider the
	// platform offers (HBEP-3, Vocabulary, Provider). It is not the object's
	// own kind, which is PlatformProfile.
	ProviderKind PlatformKind `json:"providerKind"`

	// Partitions lists the accelerator class of each partition.
	//
	// +optional
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MaxItems=256
	Partitions []PartitionProfile `json:"partitions,omitempty"`

	// HoldRelease says whether a plain user's sbatch --hold is honoured and
	// that user can lift it. It is verified per platform. False runs the
	// fallback mode of spike S4 (HBEP-7, open question 11).
	//
	// +optional
	// +kubebuilder:default=false
	HoldRelease bool `json:"hold_release"`

	// LostGrace is how long a recorded job may be absent from both the queue
	// listing and accounting before it is reported Lost.
	//
	// +optional
	// +kubebuilder:default="2m"
	LostGrace *Duration `json:"lost_grace,omitempty"`

	// InflightWindow is how long an unresolved submission attempt is waited
	// on. It must exceed the worst remote sbatch time, because a local timeout
	// does not stop a remote sbatch. The platform's measured value is set
	// here; there is no default.
	//
	// +optional
	InflightWindow *Duration `json:"inflight_window,omitempty"`

	// UnknownBound is how long, in fallback mode, an Unknown attempt may stay
	// unresolved before an operator is asked.
	//
	// +optional
	// +kubebuilder:default="10m"
	UnknownBound *Duration `json:"unknown_bound,omitempty"`

	// Profile is the rest of the profile, untyped until HBEP-4 fixes the
	// schema: the time zone, the requeue default, how long a finished job stays
	// in the queue listing, job-name rules, whether accounting keeps the job
	// comment and the JSON output version (spikes S4 and S5).
	//
	// +optional
	// +kubebuilder:validation:Type=object
	// +kubebuilder:pruning:PreserveUnknownFields
	Profile *apiextensionsv1.JSON `json:"profile,omitempty"`
}

// PlatformProfileStatus is the observed state of a PlatformProfile.
type PlatformProfileStatus struct {
	ReconcileStatus `json:",inline"`

	// LastChecked is when the profile's facts were last checked.
	//
	// +optional
	LastChecked *metav1.Time `json:"lastChecked,omitempty"`
}

// PlatformProfile is what is true for everyone on a platform, as data
// (HBEP-3, HBEP-4). All Providers of one platform share one.
//
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,categories=hpcburst
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="ProviderKind",type=string,JSONPath=`.spec.providerKind`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type PlatformProfile struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PlatformProfileSpec   `json:"spec"`
	Status PlatformProfileStatus `json:"status,omitempty"`
}

// PlatformProfileList is a list of PlatformProfiles.
//
// +kubebuilder:object:root=true
type PlatformProfileList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PlatformProfile `json:"items"`
}

func init() { SchemeBuilder.Register(&PlatformProfile{}, &PlatformProfileList{}) }
