package v1alpha1

import (
	"fmt"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ProviderConfigKind is the kind an Allocation's providerConfigRef must name:
// our Provider takes the place of crossplane's ProviderConfig (HBEP-3).
const ProviderConfigKind = "Provider"

// BatchParameters are the submission parameters of a batch job (HBEP-4,
// Submission). The lifecycle controller sets them when it creates the
// Allocation (HBEP-7, Provisioning), and they are never edited: a submitted job
// is not changed (HBEP-11, Admission). The provider adds the account (from the
// Provider's settings), the tag and the fixed options; the batch script is the
// fixed bootstrap, so neither an account nor a command is a parameter.
type BatchParameters struct {
	// Partition the job is submitted to: the Provider's one allowed partition
	// (HBEP-4, open question 12).
	//
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Partition string `json:"partition"`

	// TimeLimit is the job's time limit: the smallest of the partition's
	// maxTime, the maxWall of the QOS limits that apply and the
	// ModelDeployment's leaseLimit (AllocationTimeLimit; HBEP-4, open question
	// 11 and QOS limits). It is a whole number of minutes, because Slurm counts limits in
	// minutes and the read-back compares the limit it reports (HBEP-4, The
	// Slurm dialect); the provider refuses any other value before a call.
	//
	// +kubebuilder:validation:XValidation:rule="duration(self) > duration('0s')",message="timeLimit must be a positive duration that fits Go's time.Duration (above 0s, at most 2562047h)"
	TimeLimit Duration `json:"timeLimit"`

	// GPUs is the number of GPUs, requested untyped (--gres=gpu:<n>). In alpha
	// it is tensor-parallel-size times pipeline-parallel-size (HBEP-4, GPU
	// sizing). Zero only on a partition whose gpuModel is none; the provider
	// refuses zero on a GPU partition, and more than the smaller of
	// gpusPerNode and the QOS limits' maxGpusPerUser (MaxAllocationGPUs;
	// HBEP-4, QOS limits).
	//
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=1024
	GPUs int32 `json:"gpus"`

	// CPUsPerTask is gpus times the partition's cpusPerGpu, capped at the QOS
	// limits' maxCpusPerUser when set (HBEP-7, Provisioning; HBEP-4, QOS
	// limits).
	//
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65536
	CPUsPerTask int32 `json:"cpusPerTask"`

	// Memory is gpus times the partition's memPerGpu, capped at the QOS
	// limits' maxMemPerUser when set (HBEP-7, Provisioning; HBEP-4, QOS
	// limits). The provider passes it to Slurm in whole MiB. It is never
	// negative.
	//
	// +kubebuilder:validation:XValidation:rule="!string(self).startsWith('-')",message="memory must not be negative"
	Memory resource.Quantity `json:"memory"`

	// Nodes is the number of nodes: 1 in alpha, whose Instances are
	// single-node (HBEP-4, Submission).
	//
	// +optional
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=1
	Nodes int32 `json:"nodes,omitempty"`

	// AgentDigest is the SHA-256, 64 lowercase hex characters, of the
	// allocation agent's binary for the partition's cpuArch (HBEP-4,
	// Submission; HBEP-7, The allocation agent). It names the staged binary,
	// agent/<agentDigest>/hb-agent under the staging directory, so it is bare
	// hex, without a "sha256:" prefix.
	//
	// +kubebuilder:validation:MinLength=64
	// +kubebuilder:validation:MaxLength=64
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{64}$`
	AgentDigest string `json:"agentDigest"`
}

// ManagedAllocation names the catalog entry a managed provider serves (HBEP-3).
// A catalog entry has no partition and no time limit, so it carries neither.
type ManagedAllocation struct {
	// CatalogName is the name of the entry in the platform's catalog.
	//
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	CatalogName string `json:"catalogName"`
}

// AllocationParameters say what the Allocation is a lease on. Exactly one of
// Batch (a job on a batch platform) and Managed (a catalog entry of a managed
// platform) is set, so no field is filled with a value that does not exist for
// the kind. The whole block is immutable: delete the Allocation and create
// another, as a submitted job is never edited.
//
// +kubebuilder:validation:XValidation:rule="has(self.batch) != has(self.managed)",message="forProvider names exactly one of batch and managed"
type AllocationParameters struct {
	// Batch holds the parameters of a job on a batch platform.
	//
	// +optional
	Batch *BatchParameters `json:"batch,omitempty"`

	// Managed holds the catalog entry of a managed platform.
	//
	// +optional
	Managed *ManagedAllocation `json:"managed,omitempty"`
}

// Management policies an Allocation takes (HBEP-4, HBEP-11): the provider
// observes the job, creates it, deletes it and late-initialises it, and never
// updates it, because a submitted job is never edited. The provider registers
// exactly this set with crossplane-runtime, which refuses any other.
//
// DefaultManagementPolicies returns the set an Allocation takes when it names
// none, in the order of the schema default. Each call returns a new slice.
func DefaultManagementPolicies() []xpv2.ManagementAction {
	return []xpv2.ManagementAction{
		xpv2.ManagementActionObserve, xpv2.ManagementActionCreate, xpv2.ManagementActionDelete, xpv2.ManagementActionLateInitialize,
	}
}

// AllocationSpec is the desired state of an Allocation. It declares the three
// fields of crossplane-runtime's managed-resource spec that it uses, with
// crossplane's types and JSON names, instead of embedding the structure: the
// embedded structure defaults providerConfigRef to ClusterProviderConfig and
// managementPolicies to "*", which allows Update (HBEP-11, Ownership, records
// and deletion). The wire format is the same.
//
// +kubebuilder:validation:XValidation:rule="has(self.providerConfigRef) && self.providerConfigRef.kind == 'Provider'",message="providerConfigRef must name a Provider: set kind to 'Provider' (a crossplane ProviderConfig or ClusterProviderConfig is refused)"
// +kubebuilder:validation:XValidation:rule="has(self.providerConfigRef) && self.providerConfigRef.name.matches('^[a-z0-9]([-a-z0-9]*[a-z0-9])?$') && size(self.providerConfigRef.name) <= 63",message="providerConfigRef.name must be a Provider name, a DNS-1123 label"
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.providerConfigRef) || (has(self.providerConfigRef) && self.providerConfigRef == oldSelf.providerConfigRef)",message="an Allocation's providerConfigRef is immutable: the job it holds is on that provider; delete the Allocation and create a new one"
type AllocationSpec struct {
	// ProviderConfigReference names the Provider that holds the job. It is
	// required, has no default, and is immutable.
	//
	// +kubebuilder:validation:Required
	ProviderConfigReference *xpv2.ProviderConfigReference `json:"providerConfigRef"`

	// ManagementPolicies are the actions the managed reconciler may take. An
	// Allocation takes exactly Observe, Create, Delete and LateInitialize, in
	// any order, and that is the default; the set is immutable. Update is not
	// in it: a submitted job is never edited. Leaving Delete out would orphan
	// the job, and an empty list would pause the reconciler, so a deletion
	// would hang.
	//
	// The Go type is a slice of crossplane's ManagementAction, not its
	// ManagementPolicies, because the schema markers cannot bound a named slice
	// type; the JSON is the same, and the accessors convert.
	//
	// +optional
	// +kubebuilder:default={"Observe","Create","Delete","LateInitialize"}
	// +kubebuilder:validation:MaxItems=8
	// +kubebuilder:validation:XValidation:rule="self.size() == 4 && self.exists(p, p == 'Observe') && self.exists(p, p == 'Create') && self.exists(p, p == 'Delete') && self.exists(p, p == 'LateInitialize')",message="managementPolicies must be exactly Observe, Create, Delete and LateInitialize: a submitted job is never updated, and the provider registers exactly this set; leave the field out to take it"
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="an Allocation's managementPolicies are immutable"
	ManagementPolicies []xpv2.ManagementAction `json:"managementPolicies,omitempty"`

	// WriteConnectionSecretToReference names a Secret for connection details.
	// An Allocation has none; the field keeps crossplane's wire format.
	//
	// +optional
	WriteConnectionSecretToReference *xpv2.LocalSecretReference `json:"writeConnectionSecretToRef,omitempty"`

	// ForProvider says what the Allocation is a lease on. It is immutable.
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

// SubmissionState is the state of one submission attempt (HBEP-7, open
// question 11, from spike S4).
//
// +kubebuilder:validation:Enum=InFlight;Rejected;Unknown;Done;Abandoned
type SubmissionState string

// Submission states.
const (
	// SubmissionInFlight: the attempt is claimed and its sbatch may be running.
	SubmissionInFlight SubmissionState = "InFlight"
	// SubmissionRejected: a definite refusal, or a command that never started.
	// The next attempt waits for backoff; Reason carries the scheduler's reason.
	SubmissionRejected SubmissionState = "Rejected"
	// SubmissionUnknown: the outcome is unknown (a time-out, a signal, a
	// transport error, unparseable output or a transient reason). It is never
	// Failed, and only finding the tag resolves it.
	SubmissionUnknown SubmissionState = "Unknown"
	// SubmissionDone: the attempt established the recorded job, or was
	// resolved by it.
	SubmissionDone SubmissionState = "Done"
	// SubmissionAbandoned: an operator abandoned the attempt in fallback mode
	// (AnnotationAbandonAttempt), which allows the next one.
	SubmissionAbandoned SubmissionState = "Abandoned"
)

// SubmissionRecord is one submission attempt. The record itself is the value of
// the annotation AnnotationSubmission, this type as JSON, written with a
// resourceVersion precondition (HBEP-11, Ownership, records and deletion);
// status.atProvider.submission mirrors it for display. Readers of the
// annotation check it with Validate; the CRD checks the mirror's code.
//
// +kubebuilder:validation:XValidation:rule="!has(self.code) || self.state == 'Rejected'",message="code is set on a Rejected attempt only"
type SubmissionRecord struct {
	// Attempt is the attempt's number, from 1.
	Attempt int32 `json:"attempt"`

	// Token identifies the attempt.
	//
	// +optional
	Token string `json:"token,omitempty"`

	// State of the attempt.
	State SubmissionState `json:"state"`

	// Writer is the controller that wrote the record.
	//
	// +optional
	Writer string `json:"writer,omitempty"`

	// Time the record was written.
	//
	// +optional
	Time *metav1.Time `json:"time,omitempty"`

	// Reason for the state: for Rejected, the scheduler's reason, which the
	// lifecycle controller shows as SubmissionRejected, or as
	// SubmitLimitReached when Code says so (HBEP-7, Provisioning). It is text
	// for people; a controller matches on Code, never on it.
	//
	// +optional
	Reason string `json:"reason,omitempty"`

	// Code classifies a Rejected attempt: ReasonSubmitLimitReached when Slurm
	// refused it for a full submit limit, told by its reason name
	// (provider.PolicyRefusal.SubmitLimit; HBEP-4, QOS limits). The
	// Allocation then stays Submitting and waits for the Rejected backoff, and
	// nothing fails. Empty for any other refusal, a policy refusal for another
	// limit included, and for every other state. The Provider's controller
	// writes it with the state, in one record, so the lifecycle controller
	// never sees a Rejected state without its code: the fake controller
	// (pkg/provider/fake) does today, the Slurm provider's from #151's
	// provider change.
	//
	// +optional
	// +kubebuilder:validation:Enum=SubmitLimitReached
	Code string `json:"code,omitempty"`
}

// Validate checks what every reader of a submission record relies on: the
// attempt counts from 1, the state is one of the five, and a code is
// ReasonSubmitLimitReached, on a Rejected attempt only (review of PR #155,
// N6). A record that fails is malformed, never "no record": guessing could
// submit twice.
func (r SubmissionRecord) Validate() error {
	if r.Attempt < 1 {
		return fmt.Errorf("the submission record names attempt %d; attempts count from 1", r.Attempt)
	}
	switch r.State {
	case SubmissionInFlight, SubmissionRejected, SubmissionUnknown, SubmissionDone, SubmissionAbandoned:
	default:
		return fmt.Errorf("the submission record's state %q is not one of InFlight, Rejected, Unknown, Done and Abandoned", r.State)
	}
	switch {
	case r.Code != "" && r.Code != ReasonSubmitLimitReached:
		return fmt.Errorf("the submission record's code %q is not %s", r.Code, ReasonSubmitLimitReached)
	case r.Code != "" && r.State != SubmissionRejected:
		return fmt.Errorf("the submission record has code %s on a %s attempt; a code is set on a Rejected attempt only", r.Code, r.State)
	}
	return nil
}

// AllocationPhase is the phase of an Allocation's job as its Provider's
// controller reports it (HBEP-4, Core operations, Phases). Held, Pending,
// Running, Ended and Failed are the job's own phases; Lost, Submitting,
// SubmissionUnknown and Refused are the reconciler's.
//
// +kubebuilder:validation:Enum=Submitting;SubmissionUnknown;Held;Pending;Running;Ended;Failed;Lost;Refused
type AllocationPhase string

// Allocation phases (HBEP-4, Core operations).
const (
	// AllocationSubmitting: an attempt is in flight, or the next one waits for
	// backoff after a Rejected one.
	AllocationSubmitting AllocationPhase = "Submitting"
	// AllocationSubmissionUnknown: an attempt's outcome is unknown and no job
	// with the tag has been found yet. The Allocation exists; it is never read
	// as missing.
	AllocationSubmissionUnknown AllocationPhase = "SubmissionUnknown"
	// AllocationHeld: established with its hold not yet lifted, or an admin
	// hold, which also sets ConditionAdminHold.
	AllocationHeld AllocationPhase = "Held"
	// AllocationPending: queued, not started.
	AllocationPending AllocationPhase = "Pending"
	// AllocationRunning: started; startTime and hardEnd are set.
	AllocationRunning AllocationPhase = "Running"
	// AllocationEnded: COMPLETED, TIMEOUT (the lease end) or CANCELLED.
	AllocationEnded AllocationPhase = "Ended"
	// AllocationFailed: FAILED, NODE_FAIL, OUT_OF_MEMORY, BOOT_FAIL, PREEMPTED or
	// DEADLINE, or a requeue in any state (reason ReasonRequeued, kept once
	// seen).
	AllocationFailed AllocationPhase = "Failed"
	// AllocationLost: absent from the queue listing and accounting for longer
	// than the profile's lostGrace, counting only reads that succeeded. It
	// returns to the real phase if the job reappears.
	AllocationLost AllocationPhase = "Lost"
	// AllocationRefused: an invalid profile, a non-canonical external name, or
	// a rewritten job name (reason ReasonJobRenamed: the read-back found the
	// recorded job under a name other than the tag, and the provider cancelled
	// it). It is kept once set.
	AllocationRefused AllocationPhase = "Refused"
)

// AllocationObservation is what the provider observed about the job. Annotations
// hold the job ID and the submission record; this mirrors them for display
// only (HBEP-11). The Provider's controller is its only writer
// (docs/development.md, Writers). Every time is in UTC (HBEP-4).
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
	Phase AllocationPhase `json:"phase,omitempty"`

	// Reason says why the phase is what it is: ReasonRequeued for a requeue,
	// otherwise the scheduler's state reason or the provider's explanation.
	//
	// +optional
	// +kubebuilder:validation:MaxLength=1024
	Reason string `json:"reason,omitempty"`

	// Held is true while the recorded job is held.
	//
	// +optional
	Held bool `json:"held,omitempty"`

	// Requeued is true once a requeue of the recorded job has been seen. It is
	// kept once set.
	//
	// +optional
	Requeued bool `json:"requeued,omitempty"`

	// RestartCount is the scheduler's restart count of the recorded job.
	//
	// +optional
	RestartCount int32 `json:"restartCount,omitempty"`

	// SubmitTime is when the scheduler accepted the recorded job.
	//
	// +optional
	SubmitTime *metav1.Time `json:"submitTime,omitempty"`

	// StartEstimate is when the platform's scheduler expects the job to start.
	// It is absent while the platform gives no estimate: an unknown estimate
	// is not "now" and not "never". It is not StartTime, which is the actual
	// start, and it moves with every scheduler pass (HBEP-3, Vocabulary,
	// Lease: a lease runs from a start estimate, which may be unknown). It is
	// advisory and never planned on (HBEP-7, open question 2).
	//
	// +optional
	StartEstimate *metav1.Time `json:"startEstimate,omitempty"`

	// StartTime is when the job actually started. It is absent until it has.
	//
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// HardEnd is the lease's hard end: the time limit as currently observed
	// (HBEP-3, Vocabulary, Lease). It is re-read on every observe, so a
	// center's extension shows without being planned for.
	//
	// +optional
	HardEnd *metav1.Time `json:"hardEnd,omitempty"`

	// EndTime is when the job ended. It is absent until it has.
	//
	// +optional
	EndTime *metav1.Time `json:"endTime,omitempty"`

	// AbsentSince is the time of the first successful read, of the queue
	// listing and accounting both, that did not show the recorded job; it is
	// cleared when the job shows again. Lost is judged from it, so the grace
	// survives a restart of the Provider's controller (HBEP-4, Core
	// operations).
	//
	// +optional
	AbsentSince *metav1.Time `json:"absentSince,omitempty"`

	// Duplicates lists the IDs of extra jobs that carry the tag and are not
	// held, in fallback mode. The list is sticky.
	//
	// +optional
	// +listType=set
	Duplicates []string `json:"duplicates,omitempty"`

	// QueueWaitRecorded is true once the Provider's controller has added the
	// recorded job's queue wait to the Provider's status (HBEP-4, Provider
	// status). It is set only after that write succeeded, so a failed write
	// is retried at the next observe; and it keeps a re-observed job from
	// adding its wait again after the entry has left the 20 the Provider
	// keeps. The wait is the job record's StartTime minus its SubmitTime, from
	// whichever source Observe read.
	//
	// +optional
	QueueWaitRecorded bool `json:"queueWaitRecorded,omitempty"`

	// Accounting is the allocated time of the recorded job: allocated, not
	// billed (HBEP-8, Provider accounting). It is absent while the job has not
	// started, so a pending Allocation's hours are unknown, never 0. The
	// Provider's controller is its only writer; the record that outlives the
	// Allocation is the ControllerRevision acct-<Allocation UID>.
	//
	// +optional
	Accounting *AllocationAccounting `json:"accounting,omitempty"`
}

// AllocationStatus is the observed state of an Allocation. Conditions,
// observedGeneration and lastHandledReconcileAt are crossplane-runtime's
// managed-resource status, declared here with its types and JSON names.
type AllocationStatus struct {
	// Conditions of the Allocation. Only one condition of each type exists.
	//
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []xpv2.Condition `json:"conditions,omitempty"`

	// ObservedGeneration is the latest metadata.generation that resulted in
	// either a ready state or a stall that needs a human.
	//
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// LastHandledReconcileAt is the most recent reconcile-requested-at token
	// the controller handled.
	//
	// +optional
	LastHandledReconcileAt string `json:"lastHandledReconcileAt,omitempty"`

	// LastReconcileTime is when the controller last reconciled the object.
	//
	// +optional
	LastReconcileTime *metav1.Time `json:"lastReconcileTime,omitempty"`

	// Retention shows the retention mode that applies; an unset mode in the
	// spec reads as Keep. The Provider's controller, the only writer of
	// Allocation status, mirrors it from the spec (docs/development.md,
	// Writers); the lifecycle controller applies the policy.
	//
	// +optional
	Retention *RetentionStatus `json:"retention,omitempty"`

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
