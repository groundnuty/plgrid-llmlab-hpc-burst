package v1alpha1

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Duration is a length of time written as a Go duration string, for example
// "90s", "2m" or "1h30m". The API keeps the text the user wrote.
//
// +kubebuilder:validation:MaxLength=63
// +kubebuilder:validation:Pattern=`^([0-9]+(\.[0-9]+)?(ns|us|µs|ms|s|m|h))+$`
type Duration string

// AsDuration parses the duration.
func (d Duration) AsDuration() (time.Duration, error) { return time.ParseDuration(string(d)) }

// DeploymentID is a deployment ID (HBEP-3): the SHA-256 of the canonical
// object, in lowercase hex. Names and labels carry its display form, the first
// 12 characters, and records keep all 64. The package that computes it is
// separate. No API field takes either form here: fields hold FullDeploymentID,
// so a controller never compares two forms of one ID. This type accepts both
// forms, for code that reads a name or a label.
//
// +kubebuilder:validation:MaxLength=64
// +kubebuilder:validation:Pattern=`^([0-9a-f]{12}|[0-9a-f]{64})$`
type DeploymentID string

// Display returns the display form: the first 12 characters.
func (id DeploymentID) Display() string {
	if len(id) <= DisplayIDLength {
		return string(id)
	}
	return string(id)[:DisplayIDLength]
}

// FullDeploymentID is a deployment ID in its full form, 64 lowercase hex
// characters.
//
// +kubebuilder:validation:MaxLength=64
// +kubebuilder:validation:Pattern=`^[0-9a-f]{64}$`
type FullDeploymentID string

// Display returns the display form: the first 12 characters.
func (id FullDeploymentID) Display() string { return DeploymentID(id).Display() }

// Lengths of a deployment ID.
const (
	DisplayIDLength = 12
	FullIDLength    = 64
)

// AcceleratorClass is a partition's GPU model and CPU architecture, as the
// platform profile names them. It is an input of the deployment ID (HBEP-3).
// The JSON keys are camelCase, like every field of the API (Kubernetes API
// conventions). The hash object's keys, gpu_model and cpu_arch (HBEP-3), stay
// snake_case; pkg/deploymentid maps these fields onto them.
type AcceleratorClass struct {
	// GPUModel is the GPU model. A partition without GPUs names "none".
	//
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	GPUModel string `json:"gpuModel"`

	// CPUArch is the CPU architecture, for example "aarch64" or "x86_64".
	//
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	CPUArch string `json:"cpuArch"`
}

// ObjectReference names an object in the same namespace, or a cluster-scoped
// object.
type ObjectReference struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`
}

// CredentialsReference refers to credentials. The API never holds credentials,
// only a reference (HBEP-11, Credentials): a mode-600 file on the control-plane
// host, or, in the team option, a Secret.
//
// +kubebuilder:validation:XValidation:rule="has(self.file) != has(self.secretRef)",message="name exactly one of file and secretRef"
type CredentialsReference struct {
	// File is the path of a mode-600 file on the control-plane host.
	//
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=4096
	File string `json:"file,omitempty"`

	// SecretRef names a key of a Secret (the team option).
	//
	// +optional
	SecretRef *SecretKeyReference `json:"secretRef,omitempty"`
}

// SecretKeyReference names one key of a Secret.
type SecretKeyReference struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`

	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	Namespace string `json:"namespace"`

	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Key string `json:"key"`
}

// RetentionMode says what happens to an Allocation that hosts no Instance
// (HBEP-7, Retention).
//
// +kubebuilder:validation:Enum=Keep;ReleaseWhenIdle
type RetentionMode string

// Retention modes.
const (
	// RetentionKeep keeps the Allocation as warm capacity until a date.
	RetentionKeep RetentionMode = "Keep"

	// RetentionReleaseWhenIdle releases the Allocation after an idle limit. It
	// is opt-in (HBEP-7, open question 13).
	RetentionReleaseWhenIdle RetentionMode = "ReleaseWhenIdle"
)

// RetentionPolicy is what an Allocation with no Instance does. A kept
// Allocation still ends at its hard end.
//
// The mode has no schema default. An unset mode means Keep, HBEP-7's decided
// default, and the controller applies it (EffectiveMode) and shows the
// effective value in status. Writing the default into every stored object
// would leave stale values behind when the default changes (review finding).
//
// +kubebuilder:validation:XValidation:rule="(has(self.mode) && self.mode == 'ReleaseWhenIdle') ? has(self.idleLimit) : !has(self.idleLimit)",message="idleLimit is required with mode ReleaseWhenIdle and refused with mode Keep"
// +kubebuilder:validation:XValidation:rule="!(has(self.mode) && self.mode == 'ReleaseWhenIdle') || !has(self.until)",message="until is refused with mode ReleaseWhenIdle: an Allocation released when idle is not kept until a date"
type RetentionPolicy struct {
	// Mode is Keep or ReleaseWhenIdle. Unset means Keep.
	//
	// +optional
	Mode RetentionMode `json:"mode,omitempty"`

	// Until is the date a kept Allocation is kept until: a date, a grant's end
	// or a deadline. When it is absent, the default is the deployment's end
	// time (HBEP-7, open question 13). It goes with mode Keep.
	//
	// +optional
	Until *metav1.Time `json:"until,omitempty"`

	// IdleLimit is how long an Allocation may be idle before it is released.
	//
	// +optional
	IdleLimit *Duration `json:"idleLimit,omitempty"`
}

// EffectiveMode returns the mode that applies: Keep when none is set. A nil
// policy is an unset one.
func (r *RetentionPolicy) EffectiveMode() RetentionMode {
	if r == nil || r.Mode == "" {
		return RetentionKeep
	}
	return r.Mode
}

// RetentionStatus shows the retention the controller applies. The spec's mode
// has no schema default (an unset mode means Keep), so the effective value is
// shown here rather than written into the spec: a later change of the default
// leaves no stale stored values.
type RetentionStatus struct {
	// EffectiveMode is the mode the controller applies: the spec's mode, or
	// Keep when the spec sets none (RetentionPolicy.EffectiveMode).
	//
	// +optional
	EffectiveMode RetentionMode `json:"effectiveMode,omitempty"`
}

// ReconcileStatus is what every status carries so that a stuck controller
// shows: the generation it last reconciled and when (HBEP-11, Risks).
type ReconcileStatus struct {
	// ObservedGeneration is the metadata.generation the controller last
	// reconciled.
	//
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// LastReconcileTime is when the controller last reconciled the object.
	//
	// +optional
	LastReconcileTime *metav1.Time `json:"lastReconcileTime,omitempty"`
}
