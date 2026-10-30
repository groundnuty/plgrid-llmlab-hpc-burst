package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// CheckpointSpec names the checkpoint to serve. Its per-file SHA-256 list is
// computed by the controller and kept in the identity record.
type CheckpointSpec struct {
	// Repository is the checkpoint's repository, for example on the Hugging
	// Face Hub.
	//
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=256
	Repository string `json:"repository"`

	// Revision is the checkpoint's full commit ID: 40 lowercase hex
	// characters. A branch, a tag or an abbreviated ID is refused, because a
	// mutable reference cannot be hashed meaningfully (HBEP-3, HBEP-9).
	//
	// +kubebuilder:validation:MaxLength=64
	// +kubebuilder:validation:XValidation:rule="self.matches('^[0-9a-f]{40}$')",message="revision must be a full commit id: 40 lowercase hex characters; a branch name, a tag or an abbreviated id is refused"
	Revision string `json:"revision"`

	// HubTokenRef refers to the token for a gated repository. It is referenced
	// like provider credentials and is never a deployment-ID input.
	//
	// +optional
	HubTokenRef *CredentialsReference `json:"hubTokenRef,omitempty"`
}

// EngineSpec names the engine and its image.
type EngineSpec struct {
	// Name of the engine, for example "vllm".
	//
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	Name string `json:"name"`

	// Version of the engine.
	//
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	Version string `json:"version"`

	// ImageDigest is the engine image's digest: "sha256:" and 64 lowercase hex
	// characters (HBEP-3, the v1 canonical object). The JSON key is the
	// canonical object's own, image_digest, like gpu_model and cpu_arch.
	//
	// +kubebuilder:validation:MaxLength=71
	// +kubebuilder:validation:XValidation:rule="self.matches('^sha256:[0-9a-f]{64}$')",message="image_digest must be 'sha256:' followed by 64 lowercase hex characters"
	ImageDigest string `json:"image_digest"`
}

// EngineFlag is one engine flag. The spec lists flags as ordered pairs, so no
// command line is parsed (HBEP-3, Deployment ID). The name rules keep every key
// of the hashed object ASCII and give one spelling per flag.
//
//   - vLLM reads max_num_seqs and max-num-seqs as the same flag
//     (vllm/utils/argparse_utils.py, v0.32.0, lines 120 and 338-344), so only
//     the dashed spelling is admitted.
//   - vLLM's parsers never set allow_abbrev, so Python's argparse expands a
//     unique prefix: "--api-ke SECRET" is "--api-key SECRET". Every prefix of
//     a refused long name is therefore refused with it (api-key, hf-token,
//     model, revision, chat-template, config). A prefix of any other flag only
//     splits one setting into two IDs, which is visible; checking names
//     against the engine's real option list belongs to the launcher.
//   - vLLM's --config replaces itself with every key of a file, so a config
//     flag could carry the flags the rules above refuse, and only its path
//     would be hashed. It is refused.
//   - vLLM generates --no-<name> for each boolean flag (argparse's
//     BooleanOptionalAction, vllm/engine/arg_utils.py, v0.32.0, line 380),
//     but HBEP-3 gives a boolean under its positive name with "true" or
//     "false", so a no- prefix is refused and a boolean's value is checked.
//
// +kubebuilder:validation:XValidation:rule="!self.name.startsWith('-')",message="an engine flag name is a long name without leading dashes: write 'max-model-len', not '--max-model-len', and never a short alias such as '-tp'"
// +kubebuilder:validation:XValidation:rule="self.name.matches('^[a-z0-9][a-z0-9-]*$')",message="an engine flag name matches ^[a-z0-9][a-z0-9-]*$: lowercase ASCII letters, digits and hyphens, so no underscores, no uppercase and no '='"
// +kubebuilder:validation:XValidation:rule="!(self.name in ['ac', 'asc', 'cc', 'dc', 'dcp', 'dp', 'dpa', 'dpb', 'dpe', 'dph', 'dpl', 'dpm', 'dpn', 'dpp', 'dpr', 'ep', 'n', 'pcp', 'pp', 'q', 'r', 'sc', 'tp'])",message="a short flag alias such as tp, pp or dp is refused: write the long flag name, for example tensor-parallel-size"
// +kubebuilder:validation:XValidation:rule="!self.name.startsWith('no-')",message="a boolean flag is given under its positive name with the value 'true' or 'false': write enable-prefix-caching with 'false', not no-enable-prefix-caching"
// +kubebuilder:validation:XValidation:rule="!(self.name in ['trust-remote-code', 'enforce-eager', 'enable-expert-parallel', 'enable-auto-tool-choice', 'enable-prefix-caching', 'enable-chunked-prefill', 'enable-log-requests']) || self.value in ['true', 'false']",message="a boolean flag takes exactly 'true' or 'false': write enable-prefix-caching with the value 'true' or 'false', not '1', 'True' or an empty value"
// +kubebuilder:validation:XValidation:rule="!['api-key', 'hf-token'].exists(k, k.startsWith(self.name))",message="a key is never written in a spec: the framework issues each Instance's API key, and a Hub token goes in checkpoint.hubTokenRef (api-key and hf-token are refused, and so is any abbreviation of them, because vLLM expands a unique prefix)"
// +kubebuilder:validation:XValidation:rule="!['model', 'revision', 'chat-template'].exists(k, k.startsWith(self.name))",message="the framework passes model, revision and chat-template itself: set checkpoint and chatTemplate instead (an abbreviation of these names is refused too, because vLLM expands a unique prefix)"
// +kubebuilder:validation:XValidation:rule="!'config'.startsWith(self.name)",message="config is refused: vLLM's --config reads a file that can set any flag, so the flags hashed here would not be the flags that run; write each flag in flags (an abbreviation of config is refused too)"
type EngineFlag struct {
	// Name is the flag's long name without its leading dashes.
	//
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	Name string `json:"name"`

	// Value is the flag's value as the user wrote it. A boolean flag is given
	// under its positive name, with "true" or "false". A value that is JSON
	// stays the string given.
	//
	// +kubebuilder:validation:MaxLength=8192
	Value string `json:"value"`
}

// EnvVar is one environment variable of the engine. Names are POSIX names in
// uppercase ASCII, so every key of the hashed object is ASCII.
//
// +kubebuilder:validation:XValidation:rule="self.name.matches('^[A-Z_][A-Z0-9_]*$')",message="an environment variable name matches ^[A-Z_][A-Z0-9_]*$: uppercase ASCII letters, digits and underscores, not starting with a digit"
// +kubebuilder:validation:XValidation:rule="!(self.name in ['HF_TOKEN', 'HUGGING_FACE_HUB_TOKEN', 'VLLM_API_KEY', 'S3_ACCESS_KEY_ID', 'S3_SECRET_ACCESS_KEY'])",message="a key is never written in a spec: a Hub token goes in checkpoint.hubTokenRef and the framework issues each Instance's API key (HF_TOKEN, HUGGING_FACE_HUB_TOKEN, VLLM_API_KEY, S3_ACCESS_KEY_ID and S3_SECRET_ACCESS_KEY are refused)"
type EnvVar struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	Name string `json:"name"`

	// +kubebuilder:validation:MaxLength=8192
	Value string `json:"value"`
}

// ServingSpec says what is served and how: the checkpoint and its revision,
// the engine and its version, engine flags, the chat template and the
// environment (HBEP-3, Vocabulary). The deployment ID's inputs come from it.
// An edit to it is allowed and rolls out new IDs (HBEP-3, open question 7).
type ServingSpec struct {
	Checkpoint CheckpointSpec `json:"checkpoint"`
	Engine     EngineSpec     `json:"engine"`

	// Flags are the engine flags, in order. A flag given more than once is
	// listed more than once.
	//
	// +optional
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=128
	Flags []EngineFlag `json:"flags,omitempty"`

	// Env lists the engine's environment variables.
	//
	// +optional
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MaxItems=64
	Env []EnvVar `json:"env,omitempty"`

	// ChatTemplate is the text of one chat template, when the spec gives one.
	// It is a pointer: an omitted template (the checkpoint's own applies,
	// HBEP-3) and an explicitly empty one are different specs that can give
	// different deployment IDs. A checkpoint's own list of named templates is
	// not given here: the deployment-ID library refuses one that has no entry
	// named default and tells the operator to name the template in the spec.
	//
	// +optional
	// +kubebuilder:validation:MaxLength=262144
	ChatTemplate *string `json:"chatTemplate,omitempty"`
}

// ProviderSelection says which providers a deployment may use: providers by
// name, by label selector, or both.
//
// +kubebuilder:validation:XValidation:rule="(has(self.names) && size(self.names) > 0) || has(self.selector)",message="name at least one provider, or give a label selector"
type ProviderSelection struct {
	// Names lists Providers by name.
	//
	// +optional
	// +listType=set
	// +kubebuilder:validation:MaxItems=64
	// +kubebuilder:validation:items:MaxLength=63
	// +kubebuilder:validation:items:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	Names []string `json:"names,omitempty"`

	// Selector selects Providers by their labels, for example every provider
	// on one platform.
	//
	// +optional
	Selector *metav1.LabelSelector `json:"selector,omitempty"`
}

// ModelDeploymentSpec is what to serve: model, serving spec, number of
// instances, where, until when, and the default retention policy.
type ModelDeploymentSpec struct {
	// Model is the model name clients send. Names follow LLMLab's.
	//
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=256
	Model string `json:"model"`

	// Serving is the serving spec. It may change while the deployment serves.
	Serving ServingSpec `json:"serving"`

	// Instances is the number of Instances wanted. It is the scale
	// subresource's replica count.
	//
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=0
	Instances int32 `json:"instances"`

	// Providers says where the deployment may run.
	Providers ProviderSelection `json:"providers"`

	// EndTime is when the deployment ends: its Instances drain and end without
	// replacement.
	//
	// +optional
	EndTime *metav1.Time `json:"endTime,omitempty"`

	// RetentionPolicy is the default retention policy of the Allocations that
	// host this deployment's Instances. When it is absent, the default is to
	// keep until the deployment's end time (HBEP-7, open question 13).
	//
	// +optional
	RetentionPolicy *RetentionPolicy `json:"retentionPolicy,omitempty"`
}

// ClassDeploymentID is the current deployment ID of one accelerator class.
type ClassDeploymentID struct {
	AcceleratorClass AcceleratorClass `json:"acceleratorClass"`

	// ID is the full deployment ID, as an Instance carries it, so that a
	// controller never compares two forms of one ID.
	ID FullDeploymentID `json:"id"`
}

// ActiveDeploymentID is a deployment ID that still has Instances, as a
// Deployment lists its ReplicaSets.
type ActiveDeploymentID struct {
	AcceleratorClass AcceleratorClass `json:"acceleratorClass"`

	// ID is the full deployment ID, as an Instance carries it.
	ID FullDeploymentID `json:"id"`

	// Instances is how many Instances of this ID exist.
	Instances int32 `json:"instances"`
}

// ModelDeploymentStatus is the observed state of a ModelDeployment.
type ModelDeploymentStatus struct {
	ReconcileStatus `json:",inline"`

	// CurrentIDs lists the current deployment ID of each accelerator class
	// (HBEP-3, Current IDs, the file list and rollout).
	//
	// +optional
	// +listType=atomic
	CurrentIDs []ClassDeploymentID `json:"currentIDs,omitempty"`

	// ActiveIDs lists every ID that still has Instances.
	//
	// +optional
	// +listType=atomic
	ActiveIDs []ActiveDeploymentID `json:"activeIDs,omitempty"`

	// Instances is the number of Instances that exist and have not ended. It
	// is the scale subresource's status count.
	//
	// +optional
	Instances int32 `json:"instances,omitempty"`

	// ReadyInstances is the number of Instances that take traffic.
	//
	// +optional
	ReadyInstances int32 `json:"readyInstances,omitempty"`

	// DesiredInstances is the number of Instances the controller currently
	// aims for.
	//
	// +optional
	DesiredInstances int32 `json:"desiredInstances,omitempty"`

	// +optional
	// +listType=map
	// +listMapKey=type
	// +patchStrategy=merge
	// +patchMergeKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

// ModelDeployment is what to serve (HBEP-3). It owns its Instances, as a
// ReplicaSet owns its pods; it owns no Allocation.
//
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced,shortName=md,categories=hpcburst
// +kubebuilder:subresource:status
// +kubebuilder:subresource:scale:specpath=.spec.instances,statuspath=.status.instances
// +kubebuilder:printcolumn:name="Model",type=string,JSONPath=`.spec.model`
// +kubebuilder:printcolumn:name="Desired",type=integer,JSONPath=`.spec.instances`
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.readyInstances`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type ModelDeployment struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ModelDeploymentSpec   `json:"spec"`
	Status ModelDeploymentStatus `json:"status,omitempty"`
}

// ModelDeploymentList is a list of ModelDeployments.
//
// +kubebuilder:object:root=true
type ModelDeploymentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ModelDeployment `json:"items"`
}

func init() { SchemeBuilder.Register(&ModelDeployment{}, &ModelDeploymentList{}) }
