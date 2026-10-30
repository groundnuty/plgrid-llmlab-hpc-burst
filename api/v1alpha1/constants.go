package v1alpha1

import "strconv"

// The API group and version (HBEP-11, open question 1).
const (
	Group   = "hpcburst.groundnuty.github.io"
	Version = "v1alpha1"
)

// Finalizers (HBEP-11, Ownership, records and deletion). A CRD cannot impose a
// finalizer, so the controllers add these and the tests exercise them.
const (
	// FinalizerDrain is on every Instance. The lifecycle controller removes it
	// only once the Instance is ended, so deleting a ModelDeployment drains its
	// Instances before they go.
	FinalizerDrain = Group + "/drain"

	// FinalizerCancelAll is on every Allocation. It cancels, through the
	// provider plugin, every live job that carries the Allocation's tag, and
	// stays while an attempt is unresolved.
	FinalizerCancelAll = Group + "/cancel-all"
)

// Label keys under the group's prefix (HBEP-11, API, Labels).
const (
	// On Providers (HBEP-3).
	LabelOrganization = Group + "/organization"
	LabelPlatform     = Group + "/platform"
	LabelPartition    = Group + "/partition"
	LabelAccount      = Group + "/account"

	// On Allocations and Instances.
	LabelProvider        = Group + "/provider"
	LabelModelDeployment = Group + "/model-deployment"

	// On Instances, identity records and verification records. The value is the
	// deployment ID's display form: a label value holds at most 63 characters.
	LabelDeploymentID = Group + "/deployment-id"

	// On identity and verification records: RecordIdentity or RecordVerification.
	LabelRecord = Group + "/record"

	// On verification records: the UID of the Instance they belong to.
	LabelInstanceUID = Group + "/instance-uid"
)

// Values of LabelRecord.
const (
	RecordIdentity     = "identity"
	RecordVerification = "verification"
)

// Annotation keys on an Allocation (HBEP-11, The Allocation's record). The
// provider writes each with a resourceVersion precondition. CRD validation
// cannot guard them: it reaches no metadata beyond the name.
const (
	// AnnotationExternalName is crossplane-runtime's external-name key. Its
	// value is the job tag (HBEP-3).
	AnnotationExternalName = "crossplane.io/external-name"

	// AnnotationSubmission holds the latest submission attempt: number, token,
	// state, writer, time and reason.
	AnnotationSubmission = Group + "/submission"

	// AnnotationJobID is the established job ID, written once, never changed or
	// removed.
	AnnotationJobID = Group + "/job-id"

	// AnnotationHoldLifted is the job ID whose hold was lifted. A hold is
	// never lifted again.
	AnnotationHoldLifted = Group + "/hold-lifted"

	// AnnotationAbandonAttempt is an operator's decision on an unresolved
	// attempt, in fallback mode.
	AnnotationAbandonAttempt = Group + "/abandon-attempt"
)

// Annotation keys on an Instance, written by the lifecycle controller with a
// resourceVersion precondition (HBEP-7, Controllers).
const (
	// AnnotationPredecessor is on a replacement Instance: the name of the
	// Instance it replaces. Each primary has at most one live replacement
	// (HBEP-7, Handover).
	AnnotationPredecessor = Group + "/predecessor"

	// AnnotationRestartPrefix starts the key of a restart generation
	// (RestartGenerationAnnotation).
	AnnotationRestartPrefix = Group + "/restart-"
)

// Parts of an Instance whose restart generations the lifecycle controller
// raises (HBEP-7, Health and restarts). PartInstance is the whole Instance (the
// rung that stops, rehashes, starts, verifies, qualifies and warms up); a
// worker is one engine server process, named WorkerName(i). Alpha runs one
// worker, worker-0, and no router (HBEP-7, Summary and open question 6). A
// router, once Instances have one, is the part "router".
const PartInstance = "instance"

// WorkerName returns the name of the i-th worker of an Instance: worker-<i>.
func WorkerName(i int) string { return "worker-" + strconv.Itoa(i) }

// RestartGenerationAnnotation returns the annotation key that holds a part's
// restart generation on an Instance, for example
// hpcburst.groundnuty.github.io/restart-worker-0. The value is a decimal
// integer, absent meaning 0. Before each restart the controller emits the
// restart's event, then raises the generation by compare-and-swap; the agent
// restarts the part when the generation it is sent rises (HBEP-7, Health and
// restarts: recorded first).
func RestartGenerationAnnotation(part string) string { return AnnotationRestartPrefix + part }

// ControllerNamespace holds the controllers' leader Leases (HBEP-11,
// Controllers). The launcher creates it.
const ControllerNamespace = "hpcburst-system"

// LeaseLifecycle is the name of the lifecycle controller's leader Lease
// (HBEP-7, Controllers).
const LeaseLifecycle = "lifecycle"

// ProviderLeaseName returns the name of a Provider's leader Lease: one per
// Provider, named after it (HBEP-11, Controllers). The "provider-" prefix
// keeps a Provider named "lifecycle" from sharing the lifecycle controller's
// Lease.
func ProviderLeaseName(provider string) string { return "provider-" + provider }

// AnnotationModelDeployment is on identity and verification records. Its value
// is the full name of the ModelDeployment the record belongs to. The record's
// label carries the same name shortened to fit 63 characters (LabelValue), and
// a record has no owner reference, so once the ModelDeployment is deleted this
// annotation is the only place the full name survives (HBEP-11, API, Labels).
const AnnotationModelDeployment = Group + "/model-deployment"

// IdentityRecordPrefix starts the name of an identity record, a ControllerRevision
// named id-<first 12 hex of the deployment ID> (HBEP-11).
const IdentityRecordPrefix = "id-"

// VerificationRecordPrefix starts the name of a verification record,
// ver-<instance UID>-<generation> (HBEP-7, Instance start).
const VerificationRecordPrefix = "ver-"
