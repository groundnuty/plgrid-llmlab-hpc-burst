package v1alpha1

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

// IdentityRecordPrefix starts the name of an identity record, a ControllerRevision
// named id-<first 12 hex of the deployment ID> (HBEP-11).
const IdentityRecordPrefix = "id-"

// VerificationRecordPrefix starts the name of a verification record. HBEP-11
// leaves the name open; it is ver-<Instance UID>.
const VerificationRecordPrefix = "ver-"
