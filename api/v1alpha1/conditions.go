package v1alpha1

import (
	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
)

// The condition vocabulary of every kind, in one place. Conditions are the
// level truth; Events are a best-effort mirror (HBEP-7, Events and conditions;
// HBEP-11). Types and reasons are CamelCase. A builder who needs a type or a
// reason that is not here asks the contracts owner to add it
// (docs/development.md, Writers); the reason strings below are the ones the
// HBEPs name, plus the few marked as a contracts decision.

// ModelDeployment condition types (metav1.Condition), written by the
// lifecycle controller (HBEP-7, Events and conditions).
const (
	// ConditionReady: at least as many ready Instances as desired. For a
	// managed source: the catalog entry is listed and its Provider's key is
	// not rejected; false with ReasonCatalogEntryUnlisted,
	// ReasonCredentialsRejected or ReasonProviderNotOnPlatform (HBEP-7,
	// Reconcile loop, step 1).
	ConditionReady = "Ready"
	// ConditionDegraded, with one of the Reason* values below.
	ConditionDegraded = "Degraded"
	// ConditionProgressing: the controller is acting towards the spec. False
	// with ReasonManagedSource for a managed source, which has no Instances
	// by design.
	ConditionProgressing = "Progressing"
)

// Reasons of a managed-source ModelDeployment's conditions (HBEP-7, Reconcile
// loop, step 1). The lifecycle controller provisions nothing for one: no
// Allocation and no Instance, whatever spec.instances says; desiredInstances
// and readyInstances are 0. The front routes to the catalog entry (HBEP-5).
const (
	// ReasonManagedSource: Progressing is false, because a managed source has
	// no Instances by design.
	ReasonManagedSource = "ManagedSource"
	// ReasonCatalogEntryUnlisted: Ready is false, because no allowed managed
	// Provider's status lists the entry's catalog name now.
	ReasonCatalogEntryUnlisted = "CatalogEntryUnlisted"
	// ReasonCredentialsRejected: Ready is false, because the Provider that
	// lists the entry has CredentialsRejected true.
	ReasonCredentialsRejected = "CredentialsRejected"
	// ReasonProviderNotOnPlatform: Ready is false, because no allowed managed
	// Provider carries spec.source.managed.platform as its platform label
	// (LabelPlatform), and the front routes the entry to no Provider off that
	// platform (issue #64, left over from the review of PR #63). HBEP-5 names
	// no reason; this name is a contracts decision.
	ReasonProviderNotOnPlatform = "ProviderNotOnPlatform"
)

// Reasons of a ModelDeployment's Degraded condition (HBEP-7, Events and
// conditions).
const (
	// ReasonReplacementLate: a replacement's Allocation has not started by its
	// predecessor's drain start minus the class's start-to-ready time.
	ReasonReplacementLate = "ReplacementLate"
	// ReasonCapacityDropped: fewer ready Instances than desired.
	ReasonCapacityDropped = "CapacityDropped"
	// ReasonRestartLoop: three Allocations in a row in one class failed
	// within restart.window; provisioning in that class pauses.
	ReasonRestartLoop = "RestartLoop"
	// ReasonVerificationFailed: a settings contradiction stopped provisioning
	// in a class until the serving spec changes.
	ReasonVerificationFailed = "VerificationFailed"
	// ReasonSubmissionRejected: the provider's last attempt was Rejected; the
	// message carries the scheduler's reason.
	ReasonSubmissionRejected = "SubmissionRejected"
	// ReasonProviderUnavailable: an allowed batch Provider is missing or not
	// Ready, and was skipped. A served ModelDeployment ignores managed
	// Providers in its selection, even when their labels match, and sets no
	// condition for them (HBEP-7, Reconcile loop, step 1; HBEP-5).
	ReasonProviderUnavailable = "ProviderUnavailable"
	// ReasonSpecUnsupported: a data-parallel-size above 1, or a workload shape
	// that does not fit the declared context limit. No Allocation is created.
	// A reason on Degraded (`@hpc-burst-science-agent`, 31 Oct 2026).
	ReasonSpecUnsupported = "SpecUnsupported"
	// ReasonFrontNotConfigured: no front address and dial-home mode are
	// configured, so no Allocation is submitted (HBEP-11, Single-user host:
	// "each ModelDeployment carries a condition saying why"). HBEP-11 names
	// no reason; this name is a contracts decision, which
	// `@hpc-burst-science-agent` confirmed on 31 Oct 2026, as a reason on Degraded.
	ReasonFrontNotConfigured = "FrontNotConfigured"
	// ReasonIdentityUnresolved: the deployment ID of a class cannot be
	// computed, so no Allocation is created in it. It is its own reason,
	// separate from ReasonSpecUnsupported, because it asks the operator for
	// something else: SpecUnsupported means "edit the spec"; IdentityUnresolved
	// means "fix the Hub token or the revision, or investigate the colliding
	// identity record" (`@hpc-burst-science-agent`, 31 Oct 2026).
	//
	//   - The message names the cause: a Hub 401, a Hub 404, or a collision
	//     with the record id-<12 hex> (an existing identity record of that
	//     name holds another full ID).
	//   - Hub transport errors and 5xx answers are not this reason: the
	//     controller backs off and retries them (hubfiles.Retryable).
	//   - A collision never loops: the controller sets the reason, raises the
	//     event, and evaluates the class again only when the ModelDeployment's
	//     spec or the identity records change.
	ReasonIdentityUnresolved = "IdentityUnresolved"
)

// Reasons of a ModelDeployment's Ready, Progressing and Degraded conditions in
// the ordinary case, a contracts decision (HBEP-7, Events and conditions,
// names the conditions but not these reasons).
const (
	// ReasonEnoughReady: Ready is true, with at least as many ready Instances
	// as desired.
	ReasonEnoughReady = "EnoughReady"
	// ReasonNotEnoughReady: Ready is false, with fewer ready Instances than
	// desired and no more specific reason.
	ReasonNotEnoughReady = "NotEnoughReady"
	// ReasonProvisioning: Progressing is true while the controller creates,
	// starts or replaces Allocations and Instances.
	ReasonProvisioning = "Provisioning"
	// ReasonStable: Progressing is false, with nothing to do.
	ReasonStable = "Stable"
	// ReasonAsDesired: Degraded is false.
	ReasonAsDesired = "AsDesired"
)

// Instance condition types (metav1.Condition), written by the lifecycle
// controller (HBEP-7, Events and conditions).
const (
	ConditionVerified         = "Verified"
	ConditionQualified        = "Qualified"
	ConditionWarmedUp         = "WarmedUp"
	ConditionHealthy          = "Healthy"
	ConditionAgentUnreachable = "AgentUnreachable"
)

// Reasons of an Instance's Verified, Qualified and WarmedUp conditions, a
// contracts decision (HBEP-7, Instance start and Health and restarts).
const (
	// ReasonPending: the check has not run yet for the current verification
	// generation (status Unknown).
	ReasonPending = "Pending"
	// ReasonPassed: the check passed (status True).
	ReasonPassed = "Passed"
	// ReasonFailed: the check failed (status False); the Instance ends with
	// ReasonVerificationFailed or ReasonQualificationFailed.
	ReasonFailed = "Failed"
	// ReasonWorkerRestart and ReasonInstanceRestart: the check runs again
	// after a restart of a worker or of the whole Instance (status Unknown);
	// the restart raised the verification generation.
	ReasonWorkerRestart   = "WorkerRestart"
	ReasonInstanceRestart = "InstanceRestart"
)

// Reasons of an Instance's Healthy condition. The four malfunction signals are
// also InstanceRestart.Signal (HBEP-7, Health and restarts).
const (
	// ReasonHealthy: no malfunction signal (status True).
	ReasonHealthy = "Healthy"
	// ReasonProbeFailing: health.failureThreshold failed probes in a row.
	ReasonProbeFailing = "ProbeFailing"
	// ReasonErrorsOnly: every answer a server error for health.errorOnlyWindow.
	ReasonErrorsOnly = "ErrorsOnly"
	// ReasonNoProgress: requests running and the generated-token counter
	// unchanged for health.noProgressTimeout.
	ReasonNoProgress = "NoProgress"
	// ReasonPartExited: a part exited without being told to, whatever its
	// exit code.
	ReasonPartExited = "PartExited"
)

// Reasons of an Instance's AgentUnreachable condition.
const (
	// ReasonReportsMissing: status True, no report for health.agentTimeout.
	ReasonReportsMissing = "ReportsMissing"
	// ReasonReporting: status False, the agent reports.
	ReasonReporting = "Reporting"
)

// Reasons an Instance ends or fails, for InstanceStatus.EndReason and the
// failing event (HBEP-7).
const (
	// ReasonRegistrationTimeout: the agent did not register within
	// health.registrationTimeout of the job's start.
	ReasonRegistrationTimeout = "RegistrationTimeout"
	// ReasonStartTimeout: the Instance did not reach ready within
	// health.startTimeout of its start instruction.
	ReasonStartTimeout = "StartTimeout"
	// ReasonAllocationEndedEarly: the Allocation ended while its Instance was
	// not ended, before the Instance's hard end.
	ReasonAllocationEndedEarly = "AllocationEndedEarly"
	// ReasonAllocationFailed, ReasonAllocationLost and ReasonAllocationRefused:
	// the Allocation's provider phase was Failed, Lost or Refused.
	ReasonAllocationFailed  = "AllocationFailed"
	ReasonAllocationLost    = "AllocationLost"
	ReasonAllocationRefused = "AllocationRefused"
	// ReasonQualificationFailed: a qualification or warm-up check failed.
	ReasonQualificationFailed = "QualificationFailed"
	// ReasonDrained: the Instance drained and ended normally.
	ReasonDrained = "Drained"
	// ReasonVerificationFailed and ReasonRestartLoop, the Degraded reasons
	// above, are end reasons too: an Instance whose verification failed, or
	// whose restarts formed a loop, ends with that reason (HBEP-7, Instance
	// start; Health and restarts).
)

// ReasonRequeued is the reason of an Allocation's Failed phase when the
// scheduler requeued its job, and of the Instance that ends with it (HBEP-4,
// Phases; HBEP-3, open question 13).
const ReasonRequeued = "Requeued"

// Provider condition types (metav1.Condition). The Provider's controller
// writes them, except ConditionDialHomeUnreachable (docs/development.md,
// Writers).
const (
	// ConditionProviderReady: the age of the last successful queue read is
	// within ProviderReadyStaleness (HBEP-4, Operational readiness 1). Its
	// type string is ConditionReady's. True with ReasonPolled, False with
	// ReasonReadsFailing. A reader judges it with ProviderReady, which also
	// checks the age, so a Provider whose controller stopped stops counting.
	ConditionProviderReady = ConditionReady
	// ConditionDialectMismatch: a reply's data_parser differs from the
	// profile's jsonDataParser (HBEP-4, The Slurm dialect).
	ConditionDialectMismatch = "DialectMismatch"
	// ConditionDialHomeKeyMissing: in ssh dial-home mode, a dial-home key file
	// is missing on the platform (HBEP-4, Submission).
	ConditionDialHomeKeyMissing = "DialHomeKeyMissing"
	// ConditionCredentialsExpiring and ConditionCredentialsRejected: a managed
	// provider's key expires within a day, or was refused (HBEP-4, Managed
	// provider in alpha).
	ConditionCredentialsExpiring = "CredentialsExpiring"
	ConditionCredentialsRejected = "CredentialsRejected"
	// ConditionCredentialsInvalid: the credentials file, or the key file it
	// names, is readable by others or not owned by the controller's user, so
	// no provider call is made (HBEP-11, Credentials). HBEP-11 names no
	// condition; this name is a contracts decision, which
	// `@hpc-burst-science-agent` confirmed (31 Oct 2026).
	ConditionCredentialsInvalid = "CredentialsInvalid"
	// ConditionPathUnresolved: HOME, or the variable that stagingDir starts
	// with, is unset, empty or unsafe on the login node, so no call that needs
	// the path is made (HBEP-4, Transport). A contracts decision for the name,
	// which `@hpc-burst-science-agent` confirmed (31 Oct 2026).
	ConditionPathUnresolved = "PathUnresolved"
	// ConditionDialHomeUnreachable: an Instance on this Provider could not
	// register through its dial-home path (HBEP-11, Single-user host). The
	// lifecycle controller, which sees registrations, sets it by server-side
	// apply under FieldManagerLifecycle and owns only this condition type
	// (DialHomeUnreachableApply); the Provider's controller keeps it when it
	// writes the rest of the status. The name is a contracts decision, which
	// `@hpc-burst-science-agent` confirmed (31 Oct 2026).
	ConditionDialHomeUnreachable = "DialHomeUnreachable"
)

// Reasons of a managed Provider's conditions (HBEP-4, Managed provider in
// alpha; HBEP-11, Credentials). The managed Provider's controller (#69) and
// its stand-in, pkg/provider/fake.ManagedController, both write them, so the
// lifecycle controller and the front can match them exactly.
const (
	// ReasonCatalogRead: ConditionProviderReady is True for a managed
	// Provider, because its catalog listing succeeded within the staleness
	// bound.
	ReasonCatalogRead = "CatalogRead"
	// ReasonKeyAccepted and ReasonKeyRejected: ConditionCredentialsRejected is
	// False because the gateway accepted the key, or True because it answered
	// 401 or 403.
	ReasonKeyAccepted = "KeyAccepted"
	ReasonKeyRejected = "KeyRejected"
	// ReasonNotExpiring, ReasonExpiresWithinADay and ReasonExpired: the
	// reasons of ConditionCredentialsExpiring, from the credentials file's
	// expiry. It is True within a day of the expiry and after it.
	ReasonNotExpiring       = "NotExpiring"
	ReasonExpiresWithinADay = "ExpiresWithinADay"
	ReasonExpired           = "Expired"
	// ReasonFileRead, ReasonFileRefused and ReasonNoCredentialsFile: the
	// reasons of ConditionCredentialsInvalid for a managed Provider. It is
	// False when the file was read, True when provider.ReadManagedCredentials
	// refused the file, and True when spec.credentialsRef names no file (alpha
	// reads no secretRef).
	ReasonFileRead          = "FileRead"
	ReasonFileRefused       = "FileRefused"
	ReasonNoCredentialsFile = "NoCredentialsFile"
)

// Event reasons that are not condition reasons (HBEP-8 keeps the event
// vocabulary; these are the ones the implementable HBEPs name).
const (
	// ReasonDrainWindowShort is the reason of the front's degraded event for a
	// PlatformProfile with a batch Provider whose drainWindow is below the
	// front's maxRequestDuration plus the profile's drainMargin (HBEP-5,
	// Draining, the check).
	ReasonDrainWindowShort = "DrainWindowShort"
)

// Allocation condition types (crossplane's condition type, since Allocation
// status is crossplane-runtime's managed-resource status). The Provider's
// controller writes them.
const (
	// ConditionSubmittedAsOther: the job was established with another
	// partition, time limit or GPU count than requested; the lifecycle
	// controller replaces the Allocation (HBEP-4, The Slurm dialect, Read-back).
	ConditionSubmittedAsOther xpv2.ConditionType = "SubmittedAsOther"
	// ConditionAdminHold: the recorded job carries an admin hold, which the
	// provider never lifts (HBEP-4, Phases).
	ConditionAdminHold xpv2.ConditionType = "AdminHold"
	// ConditionSubmissionResolved is False with reason ReasonNeedsOperator
	// when, in fallback mode, an attempt is still Unknown after the profile's
	// unknownBound (HBEP-7, open question 11; spike S4's names).
	ConditionSubmissionResolved xpv2.ConditionType = "SubmissionResolved"
)

// Reasons of an Allocation's SubmissionResolved condition (spike S4).
const (
	ReasonNeedsOperator xpv2.ConditionReason = "NeedsOperator"
	ReasonResolved      xpv2.ConditionReason = "Resolved"
)

// Reasons of a Provider's Ready condition (ConditionProviderReady).
const (
	// ReasonPolled: Ready is True; the Provider's controller read the
	// platform successfully within ProviderReadyStaleness.
	ReasonPolled = "Polled"
	// ReasonReadsFailing: Ready is False; no read has succeeded for longer
	// than ProviderReadyStaleness, or none ever has. The message names the
	// time of the last success, not its age, so it does not change from one
	// pass to the next.
	ReasonReadsFailing = "ReadsFailing"
)

// Reasons of the read-back (HBEP-4, The Slurm dialect, Read-back). The
// Provider's controller compares the job the scheduler established with the
// request on the first observe after the job is established: a later change,
// such as an administrator's extension of the time limit, is a later hard end
// (status.atProvider.hardEnd), never a mismatch. The name is checked on every
// observe, so a failed cancel of a renamed job is retried.
const (
	// ReasonReadBackMismatch is the reason of ConditionSubmittedAsOther, set
	// True when the job's partition, time limit or GPU count differs from the
	// request. The message names each field with both values. Once set it is
	// never cleared: the lifecycle controller replaces the Allocation.
	ReasonReadBackMismatch xpv2.ConditionReason = "ReadBackMismatch"
	// ReasonJobRenamed is the reason (status.atProvider.reason, exactly this
	// string) of the phase Refused when the scheduler stored the job under a
	// name other than the tag, as a platform that shortens job names does. The
	// provider cancels the job, and the phase stays Refused. No condition
	// carries it: the managed reconciler owns Synced.
	ReasonJobRenamed = "JobRenamed"
)
