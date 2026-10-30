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
	// characters (HBEP-3, the v1 canonical object). The JSON key is camelCase
	// like every field of the API; the hash object's key, image_digest, stays
	// snake_case, and pkg/deploymentid maps this field onto it.
	//
	// +kubebuilder:validation:MaxLength=71
	// +kubebuilder:validation:XValidation:rule="self.matches('^sha256:[0-9a-f]{64}$')",message="imageDigest must be 'sha256:' followed by 64 lowercase hex characters"
	ImageDigest string `json:"imageDigest"`

	// Image is the engine image's repository, without a tag or digest, for
	// example "docker.io/vllm/vllm-openai". The allocation agent pulls
	// Image@ImageDigest (HBEP-7, The allocation agent: an Instance's launch
	// holds the image reference and its digest). It is not a deployment-ID
	// input: the digest names the bytes, and pkg/deploymentid hashes only it.
	//
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=512
	// +kubebuilder:validation:Pattern=`^[a-z0-9][a-z0-9._:/-]*[a-z0-9]$`
	Image string `json:"image"`
}

// EngineFlag is one engine flag. The spec lists flags as ordered pairs, so no
// command line is parsed (HBEP-3, Deployment ID). The name rules keep every key
// of the hashed object ASCII and refuse the other spellings that reach the same
// flag. A name is written with lowercase letters, digits, dashes and
// underscores; every refusal below compares it after replacing "_" with "-",
// because vLLM reads max_num_seqs and max-num-seqs as one flag
// (vllm/utils/argparse_utils.py, v0.32.0, lines 120 and 338-344), so api_key is
// refused as api-key is. HBEP-3 and the deployment-ID library normalise the
// same way.
//
//   - vLLM's parsers never set allow_abbrev, so Python's argparse expands a
//     unique prefix: "--api-ke SECRET" is "--api-key SECRET". Every proper
//     prefix of a known long name is refused. The known names are the flags of
//     HBEP-3's v1 table, with the refused, framework-passed and boolean ones;
//     a flag outside that list is an input of unknown name, and its prefixes
//     are not checked here (the deployment-ID library has the engine's full
//     list). A name that is itself a known flag stays admitted even when it is
//     the prefix of another (tokenizer, tokenizer-mode), because argparse takes
//     an exact match first.
//   - Short aliases (tp, pp, dp and the others vLLM v0.32.0 defines) and dotted
//     sub-keys (X.k, which the parser folds into a JSON value) are refused.
//   - vLLM's --config replaces itself with every key of a file, so a config
//     flag could carry the flags refused here, and only its path would be
//     hashed. It is refused. So is lora-modules in alpha: it names other
//     checkpoints without a pinned revision.
//   - vLLM generates --no-<name> for each boolean flag (argparse's
//     BooleanOptionalAction, vllm/engine/arg_utils.py, v0.32.0, line 380), but
//     HBEP-3 gives a boolean under its positive name with "true" or "false", so
//     a no- prefix is refused and a boolean's value is checked.
//   - A speculative-config that names a draft model is refused in alpha, and
//     a tokenizer other than the checkpoint's needs a pinned tokenizer-revision
//     (ServingSpec).
//
// The alias list is vLLM v0.32.0's. The lists of known names and booleans are
// the table's; a change to the table changes these markers (HBEP-3).
//
// +kubebuilder:validation:XValidation:rule="!self.name.startsWith('-')",message="an engine flag name is a long name without leading dashes: write 'max-model-len', not '--max-model-len', and never a short alias such as '-tp'"
// +kubebuilder:validation:XValidation:rule="self.name.matches('^[a-z0-9][a-z0-9_-]*$')",message="an engine flag name matches ^[a-z0-9][a-z0-9_-]*$: lowercase ASCII letters, digits, hyphens and underscores, starting with a letter or digit, so no uppercase, no '=' and no space"
// +kubebuilder:validation:XValidation:rule="!self.name.contains('.')",message="a dotted sub-key such as hf-overrides.rope is refused: give the JSON value under the long name, hf-overrides"
// +kubebuilder:validation:XValidation:rule="!(self.name.replace('_', '-') in ['ac', 'asc', 'cc', 'dc', 'dcp', 'dp', 'dpa', 'dpb', 'dpe', 'dph', 'dpl', 'dpm', 'dpn', 'dpp', 'dpr', 'ep', 'n', 'pcp', 'pp', 'q', 'r', 'sc', 'tp'])",message="a short flag alias such as tp, pp or dp is refused: write the long flag name, for example tensor-parallel-size"
// +kubebuilder:validation:XValidation:rule="!self.name.replace('_', '-').startsWith('no-')",message="a boolean flag is given under its positive name with the value 'true' or 'false': write enable-prefix-caching with 'false', not no-enable-prefix-caching"
// +kubebuilder:validation:XValidation:rule="!(self.name.replace('_', '-') in ['trust-remote-code', 'enforce-eager', 'enable-expert-parallel', 'enable-auto-tool-choice', 'enable-prefix-caching', 'enable-chunked-prefill', 'enable-log-requests']) || self.value in ['true', 'false']",message="a boolean flag takes exactly 'true' or 'false': write enable-prefix-caching with the value 'true' or 'false', not '1', 'True' or an empty value"
// +kubebuilder:validation:XValidation:rule="!['api-key', 'hf-token'].exists(k, k.startsWith(self.name.replace('_', '-')))",message="a key is never written in a spec: the framework issues each Instance's API key, and a Hub token goes in checkpoint.hubTokenRef (api-key and hf-token are refused, in either spelling, and so is any abbreviation of them, because vLLM expands a unique prefix)"
// +kubebuilder:validation:XValidation:rule="!['model', 'revision', 'chat-template'].exists(k, k.startsWith(self.name.replace('_', '-')))",message="the framework passes model, revision and chat-template itself: set checkpoint and chatTemplate instead (an abbreviation of these names is refused too, because vLLM expands a unique prefix)"
// +kubebuilder:validation:XValidation:rule="!'config'.startsWith(self.name.replace('_', '-'))",message="config is refused: vLLM's --config reads a file that can set any flag, so the flags hashed here would not be the flags that run; write each flag in flags (an abbreviation of config is refused too)"
// +kubebuilder:validation:XValidation:rule="!'lora-modules'.startsWith(self.name.replace('_', '-'))",message="lora-modules is refused in alpha: it names other checkpoints without a pinned revision (a pinned form is for beta); an abbreviation of it is refused too"
// +kubebuilder:validation:XValidation:rule="self.name.replace('_', '-') != 'speculative-config' || !self.value.matches('[\"]model[\"][[:space:]]*:')",message="a speculative-config that names a draft model is refused in alpha: the draft model is another checkpoint without a pinned revision (a pinned form is for beta)"
// +kubebuilder:validation:XValidation:rule="self.name.replace('_', '-') in ['dtype', 'quantization', 'kv-cache-dtype', 'max-model-len', 'tokenizer', 'tokenizer-mode', 'tokenizer-revision', 'trust-remote-code', 'load-format', 'hf-overrides', 'enforce-eager', 'compilation-config', 'attention-backend', 'tensor-parallel-size', 'pipeline-parallel-size', 'enable-expert-parallel', 'speculative-config', 'seed', 'generation-config', 'override-generation-config', 'tool-call-parser', 'enable-auto-tool-choice', 'reasoning-parser', 'chat-template-content-format', 'default-chat-template-kwargs', 'model', 'revision', 'chat-template', 'enable-prefix-caching', 'max-num-seqs', 'max-num-batched-tokens', 'enable-chunked-prefill', 'gpu-memory-utilization', 'kv-cache-memory-bytes', 'cpu-offload-gb', 'host', 'port', 'uds', 'served-model-name', 'download-dir', 'distributed-executor-backend', 'log-level', 'disable-log-stats', 'enable-log-requests', 'api-key', 'hf-token', 'config', 'lora-modules'] || !['dtype', 'quantization', 'kv-cache-dtype', 'max-model-len', 'tokenizer', 'tokenizer-mode', 'tokenizer-revision', 'trust-remote-code', 'load-format', 'hf-overrides', 'enforce-eager', 'compilation-config', 'attention-backend', 'tensor-parallel-size', 'pipeline-parallel-size', 'enable-expert-parallel', 'speculative-config', 'seed', 'generation-config', 'override-generation-config', 'tool-call-parser', 'enable-auto-tool-choice', 'reasoning-parser', 'chat-template-content-format', 'default-chat-template-kwargs', 'model', 'revision', 'chat-template', 'enable-prefix-caching', 'max-num-seqs', 'max-num-batched-tokens', 'enable-chunked-prefill', 'gpu-memory-utilization', 'kv-cache-memory-bytes', 'cpu-offload-gb', 'host', 'port', 'uds', 'served-model-name', 'download-dir', 'distributed-executor-backend', 'log-level', 'disable-log-stats', 'enable-log-requests', 'api-key', 'hf-token', 'config', 'lora-modules'].exists(k, k.startsWith(self.name.replace('_', '-')))",message="a proper prefix of a known flag name is refused: vLLM's parser expands an abbreviation, so two spellings of one flag would give two deployment IDs; write the full long name"
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
//
// A tokenizer other than the checkpoint's carries the same risk as an unpinned
// checkpoint (L-8.3): it needs a tokenizer-revision flag that is a full commit
// id, or the spec is refused (HBEP-3, References to other checkpoints inside
// input values). A tokenizer-revision must be a full commit id wherever it
// appears, since a branch name is mutable. Flag names compare after replacing
// "_" with "-".
//
// +kubebuilder:validation:XValidation:rule="!has(self.flags) || !self.flags.exists(f, f.name.replace('_', '-') == 'tokenizer' && f.value != self.checkpoint.repository) || self.flags.exists(f, f.name.replace('_', '-') == 'tokenizer-revision' && f.value.matches('^[0-9a-f]{40}$'))",message="a tokenizer other than the checkpoint's needs a tokenizer-revision flag that is a full commit id: 40 lowercase hex characters"
// +kubebuilder:validation:XValidation:rule="!has(self.flags) || self.flags.all(f, f.name.replace('_', '-') != 'tokenizer-revision' || f.value.matches('^[0-9a-f]{40}$'))",message="tokenizer-revision must be a full commit id: 40 lowercase hex characters; a branch name or a tag is mutable"
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

// ManagedCatalogEntry is a model a managed platform serves from its catalog
// (HBEP-3, open question 2). The platform decides what runs, so the entry has
// no checkpoint, engine, flags or template: everything else is opaque, and the
// deployment ID is over the platform, the catalog name and any revision the
// platform reports.
type ManagedCatalogEntry struct {
	// Platform is the managed platform that serves the entry.
	//
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Platform string `json:"platform"`

	// CatalogName is the name of the entry in the platform's catalog.
	//
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	CatalogName string `json:"catalogName"`

	// Revision is the revision the platform reports for the entry. It is
	// omitted when the platform reports none; an empty revision is not the
	// same.
	//
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=256
	Revision *string `json:"revision,omitempty"`
}

// ModelSource says what a ModelDeployment serves. Exactly one of Served (a
// checkpoint run on an engine of ours, on a batch platform) and Managed (a
// catalog entry of a managed platform) is set, so a managed entry carries no
// checkpoint, engine digest or flags that do not exist for it.
//
// +kubebuilder:validation:XValidation:rule="has(self.served) != has(self.managed)",message="source names exactly one of served and managed"
type ModelSource struct {
	// Served is a checkpoint served by an engine we run. It may change while
	// the deployment serves.
	//
	// +optional
	Served *ServingSpec `json:"served,omitempty"`

	// Managed is a catalog entry of a managed platform.
	//
	// +optional
	Managed *ManagedCatalogEntry `json:"managed,omitempty"`
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
// instances, where, until when, and the default retention policy. HBEP-7 adds
// the lease limit and the workload.
//
// +kubebuilder:validation:XValidation:rule="!has(self.source.served) || has(self.workload)",message="a served deployment needs a workload: its shapes drive warm-up and qualification (HBEP-7); contextTokens.typical, outputTokens.typical and targetSessions are required"
type ModelDeploymentSpec struct {
	// Model is the model name clients send. Names follow LLMLab's.
	//
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=256
	Model string `json:"model"`

	// Source says what is served: a checkpoint on our engine, or a catalog
	// entry of a managed platform. Exactly one.
	Source ModelSource `json:"source"`

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

	// LeaseLimit is the longest time limit of the deployment's Allocations,
	// when it is shorter than the partition's maxTime (HBEP-7, What the
	// controller reads; HBEP-4, open question 11). A short lease makes a real
	// handover testable in hours.
	//
	// +optional
	LeaseLimit *Duration `json:"leaseLimit,omitempty"`

	// Workload holds the shapes for warm-up and qualification (HBEP-7, What
	// the controller reads). A served deployment needs one.
	//
	// +optional
	Workload *Workload `json:"workload,omitempty"`
}

// Workload is a deployment's request shapes (HBEP-7, What the controller
// reads). Every shape must fit: each context length with the typical output,
// and the typical context with the longest output. A shape that does not fit
// is an error, never shortened; the lifecycle controller checks the declared
// shapes against a declared max-model-len before it provisions (Degraded,
// reason SpecUnsupported).
type Workload struct {
	// ContextTokens are the context lengths, in tokens.
	ContextTokens ContextTokens `json:"contextTokens"`

	// OutputTokens are the output lengths, in tokens.
	OutputTokens OutputTokens `json:"outputTokens"`

	// TargetSessions is the number of concurrent sessions to serve. KV
	// capacity is checked against it, warm-up runs it, and throughput is
	// measured at it.
	//
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100000
	TargetSessions int32 `json:"targetSessions"`

	// ToolCalls says whether tool calls are checked and warmed up. Absent, it
	// is whether the serving spec enables automatic tool choice
	// (enable-auto-tool-choice), which the controller resolves.
	//
	// +optional
	ToolCalls *bool `json:"toolCalls,omitempty"`
}

// ContextTokens are a workload's context lengths.
type ContextTokens struct {
	// Short is the short context length.
	//
	// +optional
	// +kubebuilder:default=512
	// +kubebuilder:validation:Minimum=1
	Short *int32 `json:"short,omitempty"`

	// Typical is the typical context length.
	//
	// +kubebuilder:validation:Minimum=1
	Typical int32 `json:"typical"`

	// Longest is the longest context length. Absent, it is the resolved
	// context limit (the max-model-len the engine reports) minus
	// outputTokens.typical minus the chat template's overhead, so it is fixed
	// at verification (HBEP-7).
	//
	// +optional
	// +kubebuilder:validation:Minimum=1
	Longest *int32 `json:"longest,omitempty"`
}

// OutputTokens are a workload's output lengths.
type OutputTokens struct {
	// Typical is the typical output length.
	//
	// +kubebuilder:validation:Minimum=1
	Typical int32 `json:"typical"`

	// Longest is the longest output length.
	//
	// +optional
	// +kubebuilder:default=4096
	// +kubebuilder:validation:Minimum=1
	Longest *int32 `json:"longest,omitempty"`
}

// ClassDeploymentID is the current deployment ID of one accelerator class.
type ClassDeploymentID struct {
	// AcceleratorClass is the class the ID is current for. A managed catalog
	// entry has no accelerator class, and none is invented for it.
	//
	// +optional
	AcceleratorClass *AcceleratorClass `json:"acceleratorClass,omitempty"`

	// ID is the full deployment ID, as an Instance carries it, so that a
	// controller never compares two forms of one ID.
	ID FullDeploymentID `json:"id"`
}

// ActiveDeploymentID is a deployment ID that still has Instances, as a
// Deployment lists its ReplicaSets.
type ActiveDeploymentID struct {
	// AcceleratorClass is the class of the ID's Instances; absent for a
	// managed catalog entry.
	//
	// +optional
	AcceleratorClass *AcceleratorClass `json:"acceleratorClass,omitempty"`

	// ID is the full deployment ID, as an Instance carries it.
	ID FullDeploymentID `json:"id"`

	// Instances is how many Instances of this ID exist.
	Instances int32 `json:"instances"`
}

// ClassStartToReady is one accelerator class's last measured start-to-ready
// time.
type ClassStartToReady struct {
	// AcceleratorClass is the class it was measured in.
	AcceleratorClass `json:",inline"`

	// Duration is the time from the Allocation's start to the Instance's
	// first ready.
	Duration Duration `json:"duration"`

	// Instance is the name of the Instance it was measured on.
	//
	// +optional
	// +kubebuilder:validation:MaxLength=253
	Instance string `json:"instance,omitempty"`

	// MeasuredTime is when the Instance became ready.
	MeasuredTime metav1.Time `json:"measuredTime"`
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

	// StartToReady lists each accelerator class's last measured start-to-ready
	// time: from an Allocation's start to the first ready of the Instance
	// created with it. The replacement plan reads it, and it lives here so a
	// restarted controller plans the same times (HBEP-7, Handover; HBEP-11).
	// A class with no entry uses 30 min.
	//
	// +optional
	// +listType=map
	// +listMapKey=gpuModel
	// +listMapKey=cpuArch
	// +kubebuilder:validation:MaxItems=64
	StartToReady []ClassStartToReady `json:"startToReady,omitempty"`

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

	// Retention shows the retention mode the controller applies to the
	// deployment's default policy; an unset mode in the spec, or no policy,
	// reads as Keep.
	//
	// +optional
	Retention *RetentionStatus `json:"retention,omitempty"`

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
