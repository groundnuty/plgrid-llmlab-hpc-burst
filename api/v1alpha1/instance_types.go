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

	// Launch is what every start and restart of this Instance runs, rendered
	// from the serving spec when the Instance is created (HBEP-7, What the
	// controller reads). It is immutable with the rest of the spec, so a
	// restart runs the original launch even after the ModelDeployment's
	// serving spec has changed.
	Launch Launch `json:"launch"`
}

// Launch is an Instance's launch (HBEP-7): the command line and environment
// rendered from the serving spec, the image, the checkpoint and the GPU count.
// The identity record, which HBEP-7 also lists, is the spec's identityRecord.
// The allocation agent runs Command inside the image with Apptainer, with GPU
// support, the checkpoint bound read-only and a clean environment plus Env
// (HBEP-7, The allocation agent). The in-container paths Command may name (the
// checkpoint, the engine's Unix socket, the chat template) are fixed by
// pkg/agentapi (ContainerModelDir, ContainerRunDir, ContainerChatTemplate).
// No field has a CEL rule: the controller renders it, and the spec's
// immutability rule covers it.
type Launch struct {
	// Image is the engine image's repository, without a tag or digest, for
	// example "docker.io/vllm/vllm-openai". The agent pulls Image@ImageDigest.
	//
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=512
	Image string `json:"image"`

	// ImageDigest is "sha256:" and 64 lowercase hex characters, the serving
	// spec's engine.imageDigest.
	//
	// +kubebuilder:validation:MaxLength=71
	// +kubebuilder:validation:Pattern=`^sha256:[0-9a-f]{64}$`
	ImageDigest string `json:"imageDigest"`

	// Command is the engine's argument vector inside the image, program first,
	// rendered from the serving spec's flags in order, with the framework's own
	// flags (model, Unix socket, chat template, served model name).
	//
	// +listType=atomic
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=256
	// +kubebuilder:validation:items:MaxLength=8192
	Command []string `json:"command"`

	// Env is the engine's environment, from the serving spec's env.
	//
	// +optional
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MaxItems=64
	Env []LaunchEnvVar `json:"env,omitempty"`

	// KeyEnv names the environment variable through which the engine receives
	// the Instance's key, for example VLLM_API_KEY. The agent generates the
	// key at Instance start and sets it (HBEP-7, Keys).
	//
	// +optional
	// +kubebuilder:validation:MaxLength=128
	// +kubebuilder:validation:Pattern=`^[A-Z_][A-Z0-9_]*$`
	KeyEnv string `json:"keyEnv,omitempty"`

	// Checkpoint is the checkpoint's repository and full commit id. The agent
	// stages it under <stagingDir>/checkpoints/<repository>/<revision>/.
	Checkpoint LaunchCheckpoint `json:"checkpoint"`

	// ChatTemplate is the serving spec's chat template, when it gives one. The
	// agent writes it to ContainerChatTemplate for the engine.
	//
	// +optional
	// +kubebuilder:validation:MaxLength=262144
	ChatTemplate *string `json:"chatTemplate,omitempty"`

	// GPUs is the number of GPUs the engine uses: tensor-parallel-size times
	// pipeline-parallel-size (HBEP-4, GPU sizing).
	//
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=1024
	GPUs int32 `json:"gpus"`
}

// LaunchEnvVar is one variable of a launch's environment. It carries no CEL
// rule: the serving spec's EnvVar rules have already admitted it.
type LaunchEnvVar struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	Name string `json:"name"`

	// +kubebuilder:validation:MaxLength=8192
	Value string `json:"value"`
}

// LaunchCheckpoint names a checkpoint at a full commit id.
type LaunchCheckpoint struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=256
	Repository string `json:"repository"`

	// +kubebuilder:validation:MinLength=40
	// +kubebuilder:validation:MaxLength=40
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{40}$`
	Revision string `json:"revision"`
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

	// EndReason says why the Instance ended, once it is ended or failing: one
	// of the Reason* values in conditions.go, such as ReasonDrained or
	// ReasonAllocationEndedEarly (HBEP-7: an Allocation's failure "ends its
	// Instance with that reason").
	//
	// +optional
	// +kubebuilder:validation:MaxLength=128
	EndReason string `json:"endReason,omitempty"`

	// VerificationRecord is the name of the Instance's latest verification
	// record, ver-<instance UID>-<generation> (VerificationRecordName). Each
	// verification, at start and after every restart that reverifies, writes
	// its own immutable ControllerRevision, pass or fail, before the Instance
	// can become ready (HBEP-7, Instance start; HBEP-11). The generation
	// counts the Instance's verifications from 1.
	//
	// +optional
	VerificationRecord string `json:"verificationRecord,omitempty"`

	// Verdict mirrors the latest verification record's verdict.
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

	// StartInstructionTime is when the agent was first told to run this
	// Instance: the first desired state that named it. The start timeout
	// (health.startTimeout) counts from it, so the deadline survives a
	// controller restart, and an Instance placed on an older warm Allocation
	// gets the full timeout (HBEP-7, Instance start; HBEP-11, API).
	//
	// +optional
	StartInstructionTime *metav1.Time `json:"startInstructionTime,omitempty"`

	// ReadyTime is when the Instance first became ready. Its Allocation's start
	// to this time is the class's start-to-ready time (HBEP-7, Handover).
	//
	// +optional
	ReadyTime *metav1.Time `json:"readyTime,omitempty"`

	// Agent is what the controller keeps of the agent's registration for this
	// Instance (HBEP-7, The allocation agent).
	//
	// +optional
	Agent *AgentRegistration `json:"agent,omitempty"`

	// Throughput is the last warm-up round's time per output token and time to
	// first token at the target sessions (HBEP-7, Instance start). In alpha it
	// is recorded and gates nothing (HBEP-7, open question 12).
	//
	// +optional
	Throughput *Throughput `json:"throughput,omitempty"`

	// +optional
	// +listType=map
	// +listMapKey=type
	// +patchStrategy=merge
	// +patchMergeKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

// AgentRegistration is the agent registration an Instance was started under.
// The lifecycle controller writes it when the agent of the Instance's
// Allocation registers. A registration from the same job with another boot ID
// is a second execution of the job (a requeue): nothing from the first is
// reused (HBEP-7, Test plan).
type AgentRegistration struct {
	// BootID identifies one execution of the agent, generated at its start.
	//
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=64
	BootID string `json:"bootID"`

	// JobID is the job ID the agent registered with.
	//
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=64
	JobID string `json:"jobID"`

	// RegisteredTime is when the controller accepted the registration.
	RegisteredTime metav1.Time `json:"registeredTime"`
}

// Throughput is an Instance's measured throughput at the target sessions.
type Throughput struct {
	// TimePerOutputToken is the mean time per output token.
	TimePerOutputToken Duration `json:"timePerOutputToken"`

	// TimeToFirstToken is the mean time to the first token.
	TimeToFirstToken Duration `json:"timeToFirstToken"`

	// TargetSessions is the number of concurrent sessions it was measured at.
	//
	// +kubebuilder:validation:Minimum=1
	TargetSessions int32 `json:"targetSessions"`
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
