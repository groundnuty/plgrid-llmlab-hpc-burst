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

// JobTagLength is the length of the job tag (HBEP-3, Allocation and job
// identity): hb-<40 characters>-<12 hex>. A platform whose job-name rules allow
// fewer cannot take the tag, so its profile is refused.
const JobTagLength = 56

// JobNameRules are the platform's rules for the name of a job (HBEP-4). The
// provider submits each job under its tag as the job name, so the rules must
// allow JobTagLength characters.
type JobNameRules struct {
	// MaxLength is the longest job name the platform accepts. Absent when the
	// platform states none.
	//
	// +optional
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=4096
	MaxLength *int32 `json:"max_length,omitempty"`

	// Pattern is a regular expression (RE2) that a job name matches, when the
	// platform restricts the characters of a name. Absent when it does not.
	//
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=256
	Pattern string `json:"pattern,omitempty"`
}

// PlatformProfileSpec is the platform's kind and its profile data. HBEP-4
// owns the schema. For alpha the fields that the Slurm provider and the
// deployment ID read are typed; the rest of the profile is an untyped block.
// The typed fields carry the names HBEP-11 gives them, and the scheduling
// facts of spikes S4 and S5 are typed beside them (HBEP-11, API, PlatformProfile
// schema) in the same snake_case. A fact the platform does not state, or one
// that does not apply to its kind, is absent: an absent bool is unknown, not
// false.
//
// +kubebuilder:validation:XValidation:rule="!has(self.job_name) || !has(self.job_name.max_length) || self.job_name.max_length >= 56",message="job_name.max_length must allow the job tag's 56 characters (HBEP-3, Allocation and job identity)"
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

	// TimeZone is the IANA name of the time zone in which the scheduler prints
	// local times, for example "Europe/Warsaw" or "UTC". A plugin that has to
	// parse a text time uses it, and converts every time to UTC; epoch fields
	// need no zone (HBEP-4, local time in scheduler output).
	//
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=64
	// +kubebuilder:validation:Pattern=`^[A-Za-z][A-Za-z0-9_+-]*(/[A-Za-z0-9_+-]+)*$`
	TimeZone string `json:"time_zone,omitempty"`

	// RequeueDefault says whether the scheduler requeues a job by default (for
	// Slurm, JobRequeue). The framework submits with --no-requeue either way;
	// the fact records what a job without it would do (HBEP-4).
	//
	// +optional
	RequeueDefault *bool `json:"requeue_default,omitempty"`

	// MinJobAge is how long a finished job stays in the queue listing before
	// the scheduler purges it (for Slurm, MinJobAge). A plugin that reads only
	// the queue loses the final state of a job older than this, so it asks
	// accounting (HBEP-4).
	//
	// +optional
	MinJobAge *Duration `json:"min_job_age,omitempty"`

	// JobName holds the platform's job-name rules.
	//
	// +optional
	JobName *JobNameRules `json:"job_name,omitempty"`

	// JobCommentAccounted says whether accounting keeps the job comment. Where
	// it does not, the job name is the only tag that survives in accounting
	// (spike S4).
	//
	// +optional
	JobCommentAccounted *bool `json:"job_comment_accounted,omitempty"`

	// JSONDataParser is the version of the scheduler's JSON output, for
	// example "data_parser/v0.0.42" for Slurm. A plugin parses only the
	// versions it knows.
	//
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=64
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9][A-Za-z0-9_./-]*$`
	JSONDataParser string `json:"json_data_parser,omitempty"`

	// Profile is the rest of the profile, untyped: the facts that are not typed
	// above, with the provenance envelope of each (value, method, source and
	// date; HBEP-4, open question 7), until HBEP-4 fixes the schema.
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
