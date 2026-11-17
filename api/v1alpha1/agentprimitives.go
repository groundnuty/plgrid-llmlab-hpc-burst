package v1alpha1

import (
	"crypto/sha256"
	"encoding/hex"
)

// The constants of HBEP-10's agent primitives: the owner and purpose
// annotations, and the vocabularies of Operation, Approval and ApprovalPolicy.
// The kinds themselves are in operation_types.go, approval_types.go and
// approvalpolicy_types.go.

// Annotation keys of an agent's ModelDeployment and Operation (HBEP-10, Owner
// and purpose). A CRD's own rules cannot read an object's annotations, so the
// owner policy, a ValidatingAdmissionPolicy, guards them; the CRDs only name
// the keys.
const (
	// AnnotationOwner is the owner's user name as the API server reports it. It
	// is fixed at create: no identity changes it afterwards (HBEP-10, Owner and
	// purpose). On an Operation the owner is spec.owner instead.
	AnnotationOwner = Group + "/owner"

	// AnnotationPurpose is free text saying why the object exists. It is the one
	// annotation an agent may edit on a ModelDeployment, and the same key sits on
	// an Operation (HBEP-10, Owner and purpose).
	AnnotationPurpose = Group + "/purpose"
)

// LabelOwnerHash selects the objects of one owner (HBEP-10, Owner and purpose,
// Selection by owner). Its value is OwnerHash of the owner annotation: "u-" and
// the first 12 hex characters of the SHA-256 of the owner's user name. The label
// carries a hash because the user name holds ':' (a ServiceAccount's is
// "system:serviceaccount:<namespace>:<name>"), which a label value refuses. The
// lifecycle controller sets it on the Allocations and Instances it creates for a
// ModelDeployment with an owner annotation, and on the ModelDeployment itself
// (correcting a wrong value and removing it when the annotation is absent); it
// sets it on nothing when there is no owner annotation
// (`@hpc-burst-science-agent`, 17 Nov 2026, #662).
const LabelOwnerHash = Group + "/owner-hash"

// OwnerHash returns the value of LabelOwnerHash for a user name: "u-" and the
// first 12 hex characters of the SHA-256 of the name. The value is always 14
// characters (63 at most, a label value's limit) of lowercase letters, digits
// and one hyphen, so it is a valid label value for every input, the empty name
// included. The name itself cannot be a label value, since it holds ':'.
func OwnerHash(user string) string {
	sum := sha256.Sum256([]byte(user))
	return "u-" + hex.EncodeToString(sum[:])[:12]
}

// PurposeMaxLength is the most characters the purpose annotation holds (HBEP-10,
// Owner and purpose: "Lean: at most 256 characters").
const PurposeMaxLength = 256

// OperationAction is what an Operation asks for (HBEP-10, Operations). Repurpose
// joins with HBEP-7's repurposing, in beta.
const (
	// OperationActionScale takes a ModelDeployment to fewer Instances.
	OperationActionScale OperationAction = "Scale"

	// OperationActionEnd ends a ModelDeployment now, or deletes it with
	// spec.end.delete.
	OperationActionEnd OperationAction = "End"

	// OperationActionRelease releases Allocations of one ModelDeployment that
	// host no live Instance.
	OperationActionRelease OperationAction = "Release"
)

// OperationPhase is where an Operation is in its life (HBEP-10, Operations,
// Phases).
const (
	// OperationPhasePlanning: the lifecycle controller has not written a plan
	// yet.
	OperationPhasePlanning OperationPhase = "Planning"

	// OperationPhasePlanned: a plan is written and the owner has not confirmed
	// it. The controller plans again on every pass while it waits.
	OperationPhasePlanned OperationPhase = "Planned"

	// OperationPhaseAwaitingApproval: the plan is confirmed and a target that
	// needs approval has none yet.
	OperationPhaseAwaitingApproval OperationPhase = "AwaitingApproval"

	// OperationPhaseExecuting: the targets are committed and the controller acts
	// on them. It never plans again.
	OperationPhaseExecuting OperationPhase = "Executing"

	// OperationPhaseDone: every committed target is done. Terminal.
	OperationPhaseDone OperationPhase = "Done"

	// OperationPhasePartlyFailed: a committed target failed and is being
	// retried; the Operation is Done when every target is.
	OperationPhasePartlyFailed OperationPhase = "PartlyFailed"

	// OperationPhaseStale: the plan changed before it was committed, so nothing
	// ran and its approvals no longer count. Terminal.
	OperationPhaseStale OperationPhase = "Stale"

	// OperationPhaseRefused: the request cannot be planned (a ModelDeployment
	// that is missing, a Release naming an Allocation that is not the
	// deployment's). Terminal.
	OperationPhaseRefused OperationPhase = "Refused"
)

// TargetKind is the kind of object a plan target names (HBEP-10, Operations,
// The plan).
const (
	TargetKindAllocation      TargetKind = "Allocation"
	TargetKindInstance        TargetKind = "Instance"
	TargetKindModelDeployment TargetKind = "ModelDeployment"
)

// PlanEffect is what the plan does to one target (HBEP-10, Operations, The
// plan; What needs approval).
const (
	// PlanEffectCancelPending cancels a pending Allocation: one whose job has
	// not started.
	PlanEffectCancelPending PlanEffect = "CancelPending"

	// PlanEffectDrainInstance drains an Instance while another stays ready.
	PlanEffectDrainInstance PlanEffect = "DrainInstance"

	// PlanEffectDrainLastReady drains the deployment's last ready Instance.
	PlanEffectDrainLastReady PlanEffect = "DrainLastReady"

	// PlanEffectEndStarting ends an Instance still starting on a started
	// Allocation, at once; its Allocation then follows retention (HBEP-7,
	// Reconcile loop, step 7). It has the approval rule of a drain.
	PlanEffectEndStarting PlanEffect = "EndStarting"

	// PlanEffectReleaseKept releases an Allocation that its retention policy
	// keeps.
	PlanEffectReleaseKept PlanEffect = "ReleaseKept"

	// PlanEffectReleaseStarted releases a started Allocation before its hard
	// end.
	PlanEffectReleaseStarted PlanEffect = "ReleaseStarted"

	// PlanEffectKeep keeps an Allocation under its retention policy: a target of
	// the plan that costs nothing, listed so the plan shows what stays.
	PlanEffectKeep PlanEffect = "Keep"

	// PlanEffectSetEndTime sets a ModelDeployment's end time to now (End without
	// delete).
	PlanEffectSetEndTime PlanEffect = "SetEndTime"

	// PlanEffectDeleteDeployment deletes a ModelDeployment (End with delete).
	PlanEffectDeleteDeployment PlanEffect = "DeleteDeployment"
)

// TargetState is where one committed target stands (HBEP-10, The report).
const (
	// TargetStateNotStarted: the controller has not acted on the target yet.
	TargetStateNotStarted TargetState = "NotStarted"

	// TargetStateDone: the Instance has ended or is gone, or the Allocation is
	// gone. A target whose UID is gone counts as done.
	TargetStateDone TargetState = "Done"

	// TargetStateFailed: the act failed and is retried with backoff.
	TargetStateFailed TargetState = "Failed"
)

// Operation condition types and reasons (HBEP-10, Operations, Approval and
// execution). The lifecycle controller writes them (HBEP-11, Controllers).
const (
	// ConditionExecutable says whether the Operation can run now. It is false
	// with ReasonExecutionNotBuilt in the releases that plan Operations and do
	// not execute them yet (HBEP-10, Graduation, beta step 1, built in stages:
	// the kinds, the guard, planning, then execution).
	ConditionExecutable = "Executable"

	// ConditionNoApprovalPolicy is true while a target that needs approval has no
	// ApprovalPolicy that applies: with none, nothing is approvable (HBEP-10,
	// Approval policy).
	ConditionNoApprovalPolicy = "NoApprovalPolicy"

	// ReasonExecutionNotBuilt is the reason of Executable=False in the releases
	// that plan Operations and do not execute them.
	ReasonExecutionNotBuilt = "ExecutionNotBuilt"
	// ReasonNotConfirmed is the reason of Executable=False while a plan waits for its owner's confirm
	// (phase Planned).
	ReasonNotConfirmed = "NotConfirmed"

	// ReasonAwaitingApproval is the reason of Executable=False while a confirmed plan has a target that needs
	// approval and lacks it (phase AwaitingApproval); the message says which.
	ReasonAwaitingApproval = "AwaitingApproval"

	// ReasonRefused is the reason of Executable=False on a Refused Operation; the message says why: the
	// request cannot be planned, or a confirm names a plan this controller never wrote.
	ReasonRefused = "Refused"

	// ReasonNoApprovalPolicy is the reason of NoApprovalPolicy=True when no ApprovalPolicy applies to a
	// target that needs approval: with none, nothing is approvable.
	ReasonNoApprovalPolicy = "NoApprovalPolicy"

	// ReasonDuplicateApprovalPolicy is the reason of NoApprovalPolicy=True when two ApprovalPolicies name one
	// scope that applies to a target: it is unapprovable until one is removed (fail-closed; HBEP-10, Approval
	// policy). The message names both.
	ReasonDuplicateApprovalPolicy = "DuplicateApprovalPolicy"
)
