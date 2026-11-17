package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// OperationAction is what an Operation asks for (HBEP-10, Operations). Repurpose
// joins with HBEP-7's repurposing.
//
// +kubebuilder:validation:Enum=Scale;End;Release
type OperationAction string

// OperationPhase is where an Operation is in its life (HBEP-10, Operations,
// Phases).
//
// +kubebuilder:validation:Enum=Planning;Planned;AwaitingApproval;Executing;Done;PartlyFailed;Stale;Refused
type OperationPhase string

// TargetKind is the kind of object a plan target names.
//
// +kubebuilder:validation:Enum=Allocation;Instance;ModelDeployment
type TargetKind string

// PlanEffect is what the plan does to one target (HBEP-10, Operations, The
// plan; What needs approval). EndStarting is an Instance still starting on a
// started Allocation, ended at once; its Allocation then follows retention
// (HBEP-7, Reconcile loop, step 7). HBEP-10 does not name it (`@hpc-burst-science-agent`,
// 15 Nov 2026, the ruling on question 10 of #589's plan).
//
// +kubebuilder:validation:Enum=CancelPending;DrainInstance;DrainLastReady;EndStarting;ReleaseKept;ReleaseStarted;Keep;SetEndTime;DeleteDeployment
type PlanEffect string

// TargetState is where one committed target stands (HBEP-10, The report).
//
// +kubebuilder:validation:Enum=NotStarted;Done;Failed
type TargetState string

// OperationSpec is one request to lower the capacity of one ModelDeployment
// (HBEP-10, Operations). The spec is immutable once created, except confirm,
// which is set once; a CRD rule checks each field, and the presence of each
// optional block, because a rule on a block alone never sees the block added or
// removed. A create that sets confirm is refused, since the set-once rule reads
// the old object and so runs only on an update: the last rule here runs on a
// create too (it takes the old value as optional), and without it an agent could
// create an Operation already confirmed with a plan id it worked out itself. The
// owner pin, an admission policy, checks owner and confirm against the requester
// (HBEP-10, Owner and purpose; Confirm).
//
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.confirm) || (has(self.confirm) && self.confirm == oldSelf.confirm)",message="confirm is set once: it cannot be changed or removed after the owner confirmed a plan (HBEP-10, Confirm)"
// +kubebuilder:validation:XValidation:rule="has(self.scale) == has(oldSelf.scale)",message="spec.scale cannot be added or removed after create: an Operation's spec is immutable except confirm (HBEP-10, Operations)"
// +kubebuilder:validation:XValidation:rule="has(self.end) == has(oldSelf.end)",message="spec.end cannot be added or removed after create: an Operation's spec is immutable except confirm (HBEP-10, Operations)"
// +kubebuilder:validation:XValidation:rule="has(self.release) == has(oldSelf.release)",message="spec.release cannot be added or removed after create: an Operation's spec is immutable except confirm (HBEP-10, Operations)"
// +kubebuilder:validation:XValidation:rule="self.action != 'Scale' || (has(self.scale) && !has(self.end) && !has(self.release))",message="a Scale Operation sets scale, and neither end nor release"
// +kubebuilder:validation:XValidation:rule="self.action != 'End' || (!has(self.scale) && !has(self.release))",message="an End Operation sets end, or nothing, and neither scale nor release"
// +kubebuilder:validation:XValidation:rule="self.action != 'Release' || (has(self.release) && !has(self.scale) && !has(self.end))",message="a Release Operation sets release, and neither scale nor end"
// +kubebuilder:validation:XValidation:rule="oldSelf.hasValue() || !has(self.confirm)",message="spec.confirm cannot be set when an Operation is created: the owner confirms by an update, after the controller has written a plan (HBEP-10, Confirm)",optionalOldSelf=true
type OperationSpec struct {
	// Action is what the Operation does. It is immutable (HBEP-10, Operations).
	// Repurpose joins with HBEP-7's repurposing.
	//
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="spec.action is immutable: an Operation's spec is immutable except confirm; create a new Operation for another action (HBEP-10, Operations)"
	Action OperationAction `json:"action"`

	// ModelDeploymentRef names the one ModelDeployment the Operation acts on, in
	// the Operation's own namespace by construction. An Operation covers one
	// ModelDeployment (HBEP-10, Operations: One ModelDeployment per Operation).
	// It is immutable.
	//
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="spec.modelDeploymentRef is immutable: an Operation's spec is immutable except confirm (HBEP-10, Operations)"
	ModelDeploymentRef ObjectReference `json:"modelDeploymentRef"`

	// Scale takes the ModelDeployment to fewer Instances. Only a Scale
	// Operation sets it. It is immutable, and so is its presence.
	//
	// +optional
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="spec.scale is immutable: an Operation's spec is immutable except confirm (HBEP-10, Operations)"
	Scale *ScaleParameters `json:"scale,omitempty"`

	// End parameterises an End Operation. It is optional, since a plain End
	// needs no block. It is immutable, and so is its presence.
	//
	// +optional
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="spec.end is immutable: an Operation's spec is immutable except confirm (HBEP-10, Operations)"
	End *EndParameters `json:"end,omitempty"`

	// Release names the Allocations a Release Operation releases. Only a
	// Release Operation sets it. It is immutable, and so is its presence.
	//
	// +optional
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="spec.release is immutable: an Operation's spec is immutable except confirm (HBEP-10, Operations)"
	Release *ReleaseParameters `json:"release,omitempty"`

	// Owner is the requester, as the API server reports the user name. The owner
	// pin, an admission policy, refuses a create whose owner is not the
	// requester, and only the owner may confirm (HBEP-10, Owner and purpose;
	// Confirm). It is immutable, so no identity can make itself the owner of
	// another's Operation.
	//
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=1024
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="spec.owner is immutable: an Operation's owner is pinned to its creator (HBEP-10, Owner and purpose)"
	Owner string `json:"owner"`

	// Confirm is absent until the owner confirms the plan, so a create that sets
	// it is refused. The owner sets it once, with an update that carries the
	// resourceVersion read with the plan, so a plan written in between turns the
	// update into a conflict (HBEP-10, Confirm). It is the one field of the spec
	// that may change.
	//
	// +optional
	Confirm *OperationConfirm `json:"confirm,omitempty"`
}

// ScaleParameters is a Scale Operation's request (HBEP-10, Operations).
type ScaleParameters struct {
	// Instances is the number of Instances the ModelDeployment is to have. The
	// plan refuses a count that is not below spec.instances: raising the count is
	// a direct edit (HBEP-10, Operations; the capacity guard). Zero is the
	// shutdown case.
	//
	// +kubebuilder:validation:Minimum=0
	Instances int32 `json:"instances"`
}

// EndParameters is an End Operation's optional request (HBEP-10, Operations).
type EndParameters struct {
	// Delete removes the ModelDeployment, which releases its started Allocations
	// at once, instead of only setting its end time to now (HBEP-10, Operations;
	// HBEP-7, Release).
	//
	// +optional
	Delete bool `json:"delete,omitempty"`
}

// ReleaseParameters says which Allocations a Release Operation releases: either
// the names, or a label selector, never both. Each Allocation must be one whose
// spec.modelDeploymentRef.name is this Operation's spec.modelDeploymentRef.name,
// never matched by the model-deployment label, which a long name shortens; the
// lifecycle controller refuses an Operation naming any other (HBEP-10,
// Operations). The same holds for the selector: the controller filters its
// matches by that name, so it releases no other deployment's Allocation.
//
// +kubebuilder:validation:XValidation:rule="has(self.allocations) != has(self.selector)",message="a Release names exactly one of allocations and selector"
// +kubebuilder:validation:XValidation:rule="!has(self.selector) || (has(self.selector.matchLabels) && size(self.selector.matchLabels) > 0) || (has(self.selector.matchExpressions) && size(self.selector.matchExpressions) > 0)",message="an empty release selector would match every Allocation: name the Allocations, or give a selector with matchLabels or matchExpressions"
type ReleaseParameters struct {
	// Allocations lists the Allocations to release by name. It holds at least one
	// name, and each name once.
	//
	// +optional
	// +listType=set
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=64
	// +kubebuilder:validation:items:MinLength=1
	// +kubebuilder:validation:items:MaxLength=253
	Allocations []string `json:"allocations,omitempty"`

	// Selector picks the Allocations to release by label. It is not empty: an
	// empty selector matches everything, so it names matchLabels or
	// matchExpressions. The controller refuses a selector that matches nothing
	// (HBEP-10, Operations).
	//
	// +optional
	Selector *metav1.LabelSelector `json:"selector,omitempty"`
}

// OperationConfirm is the owner's confirmation of one plan (HBEP-10, Confirm).
type OperationConfirm struct {
	// PlanID is the id of the plan the owner confirms: status.plan.id as read
	// with the resourceVersion of the same update. A wrong or empty id is
	// refused (HBEP-10, Confirm).
	//
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=64
	PlanID string `json:"planID"`
}

// OperationStatus is what the lifecycle controller writes about an Operation.
// It is the one writer, except the start estimate in a plan's cost, which the
// Provider's controller fills in (HBEP-11, Controllers, Field managers). Status
// needs no CRD rule: an agent has no write to the status subresource, and a
// create's status is dropped.
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.countGivenBack) || has(self.countGivenBack)",message="status.countGivenBack is set once: it cannot be removed (HBEP-10, The report)"
type OperationStatus struct {
	// Phase is where the Operation is (HBEP-10, Operations, Phases).
	//
	// +optional
	Phase OperationPhase `json:"phase,omitempty"`

	// ObservedGeneration is the spec generation the status describes.
	//
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions of the Operation: Executable, NoApprovalPolicy, and the reason
	// an Operation was Refused (ConditionExecutable, ConditionNoApprovalPolicy).
	//
	// +optional
	// +listType=map
	// +listMapKey=type
	// +patchStrategy=merge
	// +patchMergeKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`

	// Plan is what the Operation would do, written without side effects. The
	// owner confirms it by its id (HBEP-10, The plan).
	//
	// +optional
	Plan *OperationPlan `json:"plan,omitempty"`

	// Committed is the list of targets the controller acts on, written once, in
	// the same status write that sets the phase to Executing. From then on the
	// controller never plans again, and a restart resumes from this list (HBEP-10,
	// Approval and execution).
	//
	// +optional
	// +kubebuilder:validation:MaxItems=256
	Committed []CommittedTarget `json:"committed,omitempty"`

	// Targets holds one outcome per committed target (HBEP-10, The report).
	//
	// +optional
	// +kubebuilder:validation:MaxItems=256
	Targets []TargetOutcome `json:"targets,omitempty"`

	// FinishedAt is when the Operation became Done, Stale or Refused, or
	// PartlyFailed with every failure final (HBEP-10, The report, as amended by
	// #665 and #666: the science agent's rulings on #664, 16 and 17 Nov 2026). The controller
	// removes the Operation 7 days later (HBEP-10, Operations, Removal).
	//
	// +optional
	FinishedAt *metav1.Time `json:"finishedAt,omitempty"`

	// CountGivenBack is a Scale's record of the count it gave back for its
	// untaken units (HBEP-10, The report, as amended by #698). It is set ONLY
	// when some unit is untaken: a Scale with no untaken unit never sets it, and
	// does not wait for it. When set, the Operation finishes only after it is
	// written. Written once by the lifecycle controller.
	//
	// +optional
	CountGivenBack *CountGivenBack `json:"countGivenBack,omitempty"`
}

// CountGivenBack records a give-back (HBEP-10, The report, as amended by #698).
//
// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="status.countGivenBack is set once: it cannot be changed (HBEP-10, The report)"
type CountGivenBack struct {
	// Units is U, the untaken units: each the capacity of one primary (its
	// Instance and Allocation, its hedge's members, and its pending
	// replacement).
	//
	// +kubebuilder:validation:Minimum=1
	Units int32 `json:"units"`

	// Instances is the spec.instances written: the plan's count plus Units,
	// absolute. It is absent when a person had set another count, which stands,
	// so nothing was given back.
	//
	// +optional
	// +kubebuilder:validation:Minimum=1
	Instances *int32 `json:"instances,omitempty"`
}

// OperationPlan is the plan of an Operation (HBEP-10, The plan).
type OperationPlan struct {
	// ID is opaque: 16 hex characters of SHA-256 over the plan's canonical form.
	// Clients compare and quote it, and never parse it. It covers the action, the
	// ModelDeployment's UID and generation, and each target's kind, name, UID,
	// generation, effect and needsApproval, with the policies' name, UID and
	// generation. Cost figures are left out, since they age (HBEP-10, The plan).
	//
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=64
	ID string `json:"id"`

	// ObservedAt is when the controller read the state the plan describes.
	ObservedAt metav1.Time `json:"observedAt"`

	// ModelDeployment is the deployment the plan was made for, by identity.
	ModelDeployment PlannedDeployment `json:"modelDeployment"`

	// Targets are the objects the plan acts on, each with its effect.
	//
	// +optional
	// +kubebuilder:validation:MaxItems=256
	Targets []PlanTarget `json:"targets,omitempty"`
}

// PlannedDeployment identifies the ModelDeployment a plan was made for, so a
// deployment deleted and created again under the same name makes the plan stale.
type PlannedDeployment struct {
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`

	// UID is the ModelDeployment's UID.
	//
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=64
	UID types.UID `json:"uid"`

	// Generation is the ModelDeployment's metadata.generation at planning.
	Generation int64 `json:"generation"`
}

// PlanTarget is one object a plan acts on (HBEP-10, The plan).
type PlanTarget struct {
	// Kind is the target's kind.
	Kind TargetKind `json:"kind"`

	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`

	// UID is the target's UID. A target whose UID is gone when it is acted on
	// counts as done (HBEP-10, Approval and execution).
	//
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=64
	UID types.UID `json:"uid"`

	// Generation is the target's metadata.generation at planning.
	Generation int64 `json:"generation"`

	// Provider is the Provider of an Allocation, or of the Allocation an Instance
	// runs on. A policy of Provider scope applies by it (HBEP-10, Approval
	// policy).
	//
	// +optional
	// +kubebuilder:validation:MaxLength=63
	Provider string `json:"provider,omitempty"`

	// Effect is what the plan does to the target.
	Effect PlanEffect `json:"effect"`

	// NeedsApproval is whether the effect needs an Approval (HBEP-10, What needs
	// approval).
	NeedsApproval bool `json:"needsApproval"`

	// ApprovalRule says which row of HBEP-10's What needs approval table made the
	// effect need approval.
	//
	// +optional
	// +kubebuilder:validation:MaxLength=256
	ApprovalRule string `json:"approvalRule,omitempty"`

	// Policies are the ApprovalPolicies that apply to the target, each recorded
	// with its generation, so a change to a policy makes the plan stale at
	// execution (HBEP-10, Approval policy).
	//
	// +optional
	Policies []AppliedPolicy `json:"policies,omitempty"`

	// Cost is the release cost of a target that lowers capacity, whether or not
	// it needs approval (HBEP-10, The release cost).
	//
	// +optional
	Cost *ReleaseCost `json:"cost,omitempty"`
}

// AppliedPolicy records one ApprovalPolicy a target needs an approval under
// (HBEP-10, Approval policy: "A plan records each policy it applied, with its
// generation").
type AppliedPolicy struct {
	// Name is the ApprovalPolicy's name.
	//
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`

	// UID is the ApprovalPolicy's UID.
	//
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=64
	UID types.UID `json:"uid"`

	// Generation is the ApprovalPolicy's metadata.generation at planning.
	Generation int64 `json:"generation"`

	// Approvers are the policy's approvers at planning.
	//
	// +optional
	// +kubebuilder:validation:MaxItems=64
	Approvers []Approver `json:"approvers,omitempty"`
}

// CommittedTarget is one target the controller committed to act on (HBEP-10,
// Approval and execution).
type CommittedTarget struct {
	Kind TargetKind `json:"kind"`

	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`

	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=64
	UID types.UID `json:"uid"`

	// Generation is the target's metadata.generation at planning.
	Generation int64 `json:"generation"`

	// Effect is what the controller does to the target.
	Effect PlanEffect `json:"effect"`
}

// TargetOutcome is the report of one committed target (HBEP-10, The report).
type TargetOutcome struct {
	Kind TargetKind `json:"kind"`

	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`

	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=64
	UID types.UID `json:"uid"`

	// State is done, failed or not started. A failed target is retried with
	// backoff unless its failure is final, as when the target changed since the
	// commit so that its approved effect cannot apply (HBEP-10, The report, as
	// amended by #665 and #666).
	State TargetState `json:"state"`

	// Reason is the CamelCase code of a failure.
	//
	// +optional
	// +kubebuilder:validation:MaxLength=256
	Reason string `json:"reason,omitempty"`

	// Message says what failed. On a done Allocation that a failure released before the Operation did (a gate miss,
	// RestartLoop, an Allocation failed or lost), it names that failure ("released by QualificationFailed"): the
	// Allocation's release cause is the failure's, not the Operation's (HBEP-10, The release cause).
	//
	// +optional
	// +kubebuilder:validation:MaxLength=1024
	Message string `json:"message,omitempty"`

	// LastTransitionTime is when State last changed.
	LastTransitionTime metav1.Time `json:"lastTransitionTime"`
}

// ReleaseCost is what releasing one target gives up, and what getting it back
// costs (HBEP-10, The release cost). Every field is optional: a figure that does
// not apply to the target is absent. A figure that applies and cannot be known
// is present with its reason in unknown, and never as a zero (HBEP-8, Notes:
// unknown is a value).
type ReleaseCost struct {
	// Note says in words what releasing the target gives up where no figure
	// applies: "no HPC capacity held" for the End of a managed-source
	// deployment that holds no Allocation (`@hpc-burst-science-agent`, 16 Nov
	// 2026, #653). It is not an unknown: unknown stays for a figure that
	// applies and cannot be observed.
	//
	// +optional
	// +kubebuilder:validation:MaxLength=256
	Note string `json:"note,omitempty"`

	// LeaseLeft is a started Allocation's hard end minus observedAt.
	//
	// +optional
	LeaseLeft *DurationFigure `json:"leaseLeft,omitempty"`

	// GPUHoursLeft is LeaseLeft times the Allocation's GPUs.
	//
	// +optional
	GPUHoursLeft *GPUHoursFigure `json:"gpuHoursLeft,omitempty"`

	// QueueAge is a pending Allocation's observedAt minus its submit time.
	//
	// +optional
	QueueAge *DurationFigure `json:"queueAge,omitempty"`

	// StartEstimate is a pending Allocation's own estimate of its start. It is
	// advisory.
	//
	// +optional
	StartEstimate *TimeFigure `json:"startEstimate,omitempty"`

	// LastReady is true when the target is the deployment's last ready Instance:
	// the model then serves nothing on its own capacity until a new Allocation
	// starts and its Instance is ready.
	//
	// +optional
	LastReady bool `json:"lastReady,omitempty"`

	// QueueWaits is the Provider's observed queue waits: what getting the
	// capacity back costs (HBEP-4, Provider status).
	//
	// +optional
	QueueWaits *QueueWaitsFigure `json:"queueWaits,omitempty"`

	// EstimateNow is Slurm's estimate for the same job submitted now. It is
	// advisory, and unknown until the estimate operation exists (HBEP-10, The
	// release cost; Graduation, beta step 4).
	//
	// +optional
	EstimateNow *TimeFigure `json:"estimateNow,omitempty"`

	// StartToReady is the class's last start-to-ready time, so the time to serve
	// again is about the queue wait plus this.
	//
	// +optional
	StartToReady *DurationFigure `json:"startToReady,omitempty"`
}

// DurationFigure is a length of time in a release cost. It carries either its
// value or Unknown, the reason it is not known, never a zero in place of one
// (HBEP-8, Notes: unknown is a value).
type DurationFigure struct {
	// Duration is the value, when known.
	//
	// +optional
	Duration *metav1.Duration `json:"duration,omitempty"`

	// Unknown is why the value is not known.
	//
	// +optional
	// +kubebuilder:validation:MaxLength=1024
	Unknown string `json:"unknown,omitempty"`

	// ObservedAt is when the inputs were read: a figure is a snapshot, never
	// recomputed from the clock.
	//
	// +optional
	ObservedAt *metav1.Time `json:"observedAt,omitempty"`

	// Advisory marks a figure that is an estimate, not a fact.
	//
	// +optional
	Advisory bool `json:"advisory,omitempty"`
}

// TimeFigure is a point in time in a release cost. It carries either its value
// or Unknown, the reason it is not known, never a zero in place of one (HBEP-8,
// Notes: unknown is a value).
type TimeFigure struct {
	// Time is the value, when known.
	//
	// +optional
	Time *metav1.Time `json:"time,omitempty"`

	// Unknown is why the value is not known.
	//
	// +optional
	// +kubebuilder:validation:MaxLength=1024
	Unknown string `json:"unknown,omitempty"`

	// ObservedAt is when the inputs were read.
	//
	// +optional
	ObservedAt *metav1.Time `json:"observedAt,omitempty"`

	// Advisory marks a figure that is an estimate, not a fact: Slurm's estimates
	// swing by hours (HBEP-4, Risks).
	//
	// +optional
	Advisory bool `json:"advisory,omitempty"`
}

// GPUHoursFigure is an amount of GPU time in a release cost. It carries either
// its value or Unknown, the reason it is not known, never a zero in place of one
// (HBEP-8, Notes: unknown is a value).
type GPUHoursFigure struct {
	// GPUHours is the value as a decimal string with at most one decimal place,
	// such as "36.0": a string, so no float is compared or rounded on the way.
	//
	// +optional
	// +kubebuilder:validation:MaxLength=32
	// +kubebuilder:validation:Pattern=`^[0-9]+([.][0-9])?$`
	GPUHours string `json:"gpuHours,omitempty"`

	// Unknown is why the value is not known.
	//
	// +optional
	// +kubebuilder:validation:MaxLength=1024
	Unknown string `json:"unknown,omitempty"`

	// ObservedAt is when the inputs were read.
	//
	// +optional
	ObservedAt *metav1.Time `json:"observedAt,omitempty"`
}

// QueueWaitsFigure is the Provider's observed queue waits in a release cost: the
// median, the p90 and the largest of the last waits in its status (HBEP-10, The
// release cost; HBEP-4, Provider status). It carries either those values or
// Unknown, the reason they are not known, never a zero in place of them (HBEP-8,
// Notes: unknown is a value).
type QueueWaitsFigure struct {
	// Median of the observed waits.
	//
	// +optional
	Median *metav1.Duration `json:"median,omitempty"`

	// P90 is the 90th percentile of the observed waits.
	//
	// +optional
	P90 *metav1.Duration `json:"p90,omitempty"`

	// Max is the longest observed wait.
	//
	// +optional
	Max *metav1.Duration `json:"max,omitempty"`

	// Count is how many waits the figures come from.
	//
	// +optional
	Count int32 `json:"count,omitempty"`

	// Unknown is why the waits are not known.
	//
	// +optional
	// +kubebuilder:validation:MaxLength=1024
	Unknown string `json:"unknown,omitempty"`

	// ObservedAt is when the Provider's status was read.
	//
	// +optional
	ObservedAt *metav1.Time `json:"observedAt,omitempty"`
}

// Operation is one request to lower the capacity of one ModelDeployment, planned
// before it runs, confirmed by its owner, and approved under the ApprovalPolicies
// that apply (HBEP-10, Operations). Its name is its idempotency key. The
// lifecycle controller plans and executes it. A status sent with a create is
// dropped, because the status subresource is the only way to write it.
//
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Namespaced,categories=hpcburst
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Action",type=string,JSONPath=`.spec.action`
// +kubebuilder:printcolumn:name="Deployment",type=string,JSONPath=`.spec.modelDeploymentRef.name`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type Operation struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   OperationSpec   `json:"spec"`
	Status OperationStatus `json:"status,omitempty"`
}

// OperationList is a list of Operations.
//
// +kubebuilder:object:root=true
type OperationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Operation `json:"items"`
}

func init() { SchemeBuilder.Register(&Operation{}, &OperationList{}) }
