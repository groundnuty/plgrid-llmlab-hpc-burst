package provider

import (
	"context"
	"os"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// SubmitTimeout and CommandTimeout are the transport's time-outs (HBEP-4,
// Transport): a time-out on sbatch is an Unknown outcome, and on any other
// command an error, so the state is unknown. A profile's inflightWindow must
// exceed SubmitTimeout.
const (
	SubmitTimeout  = 60 * time.Second
	CommandTimeout = 30 * time.Second
)

// MaxConcurrentCommands is the most commands at once on one connection, a code
// default (HBEP-4, open question 17).
const MaxConcurrentCommands = 4

// Capabilities are the capability flags (HBEP-4, open question 2). For alpha
// the kind (batch or managed) fixes the operation set, and the one flag is
// HoldRelease. A new flag is an amendment to HBEP-4.
type Capabilities struct {
	// HoldRelease: a plain user's held submission is honoured and that user
	// can lift the hold. True runs hold mode, false S4's fallback mode
	// (HBEP-7, open question 11).
	HoldRelease bool
}

// CapabilitiesOf returns the flags a profile states.
func CapabilitiesOf(p Profile) Capabilities {
	return Capabilities{HoldRelease: p.Spec.HoldRelease}
}

// ProfileReader is the platform-profile reader, the core operation every
// provider kind has (HBEP-4, Core operations).
type ProfileReader interface {
	// ReadProfile reads the named Provider, its PlatformProfile and its
	// credentials reference, and validates them: the profile's kind matches
	// the plugin; the Provider allows exactly one partition and the profile
	// lists it; jobName.maxLength is at least 56 and the tag form matches
	// jobName.pattern when one is set; jsonDataParser is one the plugin knows;
	// inflightWindow is set and exceeds SubmitTimeout; the credentials file,
	// and for ssh the key file it names, are mode 600 and owned by the
	// controller's user (HBEP-11). It fails with an *InvalidProfileError that
	// names every invalid field, or ErrCredentials for a file it refuses.
	// Read-only.
	//
	// HBEP-4's QOS limits add one check, which the Slurm plugin makes: the
	// Provider's qos, when set, and the partition's qos, when set, are listed
	// in the profile's qos.
	ReadProfile(ctx context.Context, provider string) (Profile, Settings, error)
}

// Batch is a batch provider's core operations, bound to one Provider: its
// connection, its credentials and its settings (HBEP-4, Core operations).
// Implementations are safe for concurrent use.
type Batch interface {
	// FindByTag returns every job of the calling user whose name is exactly
	// tag: from the queue listing, and from accounting, starting at since, for
	// any job the listing does not show. Jobs are sorted by job ID, numerically
	// for Slurm, never taken in output order. Read-only and idempotent. An
	// error means unknown, never "no job".
	FindByTag(ctx context.Context, tag string, since time.Time) ([]Job, error)

	// Poll reads the calling user's queue listing once and discards it; nil
	// means the platform answered. The Provider's controller calls it on a
	// pass that observes no job, at least every v1alpha1.ProviderPollInterval,
	// so the Ready condition always rests on a real read (HBEP-4, Operational
	// readiness 1). Read-only and idempotent. An error means unknown.
	Poll(ctx context.Context) error

	// Observe returns one job: from the queue listing, else from accounting.
	// found is false only when both reads succeeded and neither shows the job.
	// Read-only and idempotent. An error means unknown.
	Observe(ctx context.Context, jobID string) (job Job, found bool, err error)

	// Submit runs one submission, held when req.Hold is set, which hold mode
	// always sets. err is nil exactly when the outcome is Accepted; otherwise
	// it says why. A request refused before any call (an unsafe value, a
	// partition the Provider does not allow or the profile does not list, no
	// GPU on a GPU partition, more GPUs than gpusPerNode, a time limit above
	// maxTime or not in whole minutes) is Rejected with an error that wraps
	// ErrInvalidRequest. Not idempotent: it runs only under a claimed
	// attempt, and never retries by itself.
	//
	// HBEP-4's QOS limits extend the refusals before any call, which the
	// Slurm plugin makes, with the limits that apply on the partition under
	// req.QOS (v1alpha1.EffectiveQOSLimits): an unsafe QOS, an effective
	// maxSubmitPerUser of 0, more GPUs than v1alpha1.MaxAllocationGPUs, more
	// CPUs or memory than maxCpusPerUser or maxMemPerUser, and a time limit
	// above the smaller of maxTime and maxWall. A policy refusal is returned
	// as a *PolicyRefusal.
	Submit(ctx context.Context, req SubmitRequest) (jobID string, outcome SubmitOutcome, err error)

	// LiftHold lifts the job's user hold. Its errors wrap ErrNotHeld,
	// ErrAdminHold (reported, never retried) or ErrNotFound; any other error
	// is unknown. The reconciler calls it at most once per Allocation, guarded
	// by the hold-lifted annotation; the operation itself does not guard.
	LiftHold(ctx context.Context, jobID string) error

	// Cancel cancels each job and returns one result per job, in the order
	// given. A job that was live when it was cancelled reports Cancelled. A
	// job that had already ended reports AlreadyEnded and nothing is
	// cancelled, so Cancel is idempotent. A job the platform does not know
	// reports CancelError with an error that wraps ErrNotFound. The outcome
	// comes from the job's state at the time of the call, never from what the
	// platform's cancel command prints, which can be silent for both of the
	// last two (Slurm 24.11's scancel is). A partly applied cancel is reported
	// job by job, never as one status.
	Cancel(ctx context.Context, jobIDs []string) []CancelResult

	// EnsureFile places one file under the profile's stagingDir. It does
	// nothing when a file there already has the SHA-256. Otherwise it writes
	// the content through the connection's standard input to a temporary name,
	// sets the mode and renames it into place. The content never appears on a
	// command line or in a log. Idempotent.
	EnsureFile(ctx context.Context, f File) error

	// RemoveAllocationFiles removes the Allocation's directory under
	// stagingDir, alloc/<tag>/, once its jobs are gone. Idempotent.
	RemoveAllocationFiles(ctx context.Context, tag string) error
}

// Managed is a managed provider's core operation besides ReadProfile (HBEP-4,
// Core operations). Alpha reads the catalog at most once a minute.
type Managed interface {
	// ListCatalog returns the models the gateway lists (GET /v1/models with
	// the Provider's key). Read-only. A 401 or 403 is an error that wraps
	// ErrKeyRejected; any other error is unknown, and leaves the stored
	// catalog as it was.
	ListCatalog(ctx context.Context) ([]CatalogEntry, error)
}

// Profile is a validated platform profile, with the Provider's one allowed
// partition resolved (HBEP-4, open question 12).
type Profile struct {
	// Name is the PlatformProfile's name.
	Name string
	// Spec is the PlatformProfile's spec as stored, schema defaults applied.
	Spec v1alpha1.PlatformProfileSpec
	// Partition is the entry of the Provider's one allowed partition.
	Partition v1alpha1.PartitionProfile
}

// Settings are a Provider's settings (HBEP-4, Provider settings): account,
// grant, credentials reference, allowed partition, labels and an optional QOS.
// The grant is recorded in alpha but not read.
type Settings struct {
	// Provider is the Provider's name.
	Provider string
	// Account jobs are charged to.
	Account string
	// Grant the account belongs to.
	Grant string
	// Partition is the one partition the Provider allows.
	Partition string
	// QOS is the Provider's qos, the QOS its jobs are to be submitted under,
	// or "" for the association's default (HBEP-4, Provider settings).
	// ReadProfile fills it, checking that the profile lists it; the Slurm
	// plugin sends it as --qos.
	QOS string
	// CredentialsFile is the path of the mode-600 credentials file on the
	// control-plane host: for a Slurm provider an ssh client configuration
	// naming the login node, the user and the key file; for a managed
	// provider a ManagedCredentials file, read with ReadManagedCredentials.
	CredentialsFile string
	// Labels are the Provider's labels (organization, platform, partition,
	// account; HBEP-3).
	Labels map[string]string
}

// SubmitRequest is one submission. The provider adds the fixed options
// (--parsable, --no-requeue, --nodes, the output files) and the bootstrap,
// which checks the staged agent binary against Params.AgentDigest and runs it
// with the Allocation's directory (HBEP-4, Submission).
type SubmitRequest struct {
	// Tag is the Allocation's job tag (v1alpha1.JobTag), the job name.
	Tag string
	// Hold submits the job held.
	Hold bool
	// Account comes from the Provider's settings, never from the Allocation.
	Account string
	// QOS comes from the Provider's settings, never from the Allocation. The
	// Slurm plugin's submission adds --qos=<QOS> when it is set, and no --qos
	// at all when it is "" (HBEP-4, The Slurm dialect).
	QOS string
	// Params are the Allocation's submission parameters.
	Params v1alpha1.BatchParameters
}

// SubmitOutcome is the outcome of a submission (HBEP-4, Core operations).
type SubmitOutcome string

// Submission outcomes.
const (
	// Accepted: the scheduler printed a job ID.
	Accepted SubmitOutcome = "Accepted"
	// Rejected: a definite refusal (a reason that is not transient), or a
	// command that never started. The next attempt waits for backoff.
	Rejected SubmitOutcome = "Rejected"
	// Unknown: a time-out, a signal, a transport error, unparseable output or
	// a transient reason. The scheduler may still have registered the job;
	// only finding the tag resolves it. It is never Failed.
	Unknown SubmitOutcome = "Unknown"
)

// HoldKind says who holds a job.
type HoldKind string

// Hold kinds: HoldNone, or a user hold (JobHeldUser), which the job's user may
// lift, or an admin hold (JobHeldAdmin), which the provider never lifts.
const (
	HoldNone  HoldKind = ""
	HoldUser  HoldKind = "User"
	HoldAdmin HoldKind = "Admin"
)

// Source says where a job record was read.
type Source string

// Sources of a job record.
const (
	SourceQueue      Source = "queue"
	SourceAccounting Source = "accounting"
)

// Job is one job record (HBEP-4, Core operations). Every time is in UTC.
type Job struct {
	// ID is the job ID.
	ID string
	// Name is the job name, the tag it was submitted under as the scheduler
	// reports it.
	Name string
	// State is the state as the scheduler lists it: the base state, then any
	// flags.
	State []string
	// Phase is the job's own phase: one of Held, Pending, Running, Ended and
	// Failed (SlurmPhase). Lost, Submitting, SubmissionUnknown and Refused are
	// the reconciler's, never a job's.
	Phase v1alpha1.AllocationPhase
	// Hold says whether, and by whom, the job is held.
	Hold HoldKind
	// Reason is the scheduler's state reason, for example JobHeldUser.
	Reason string
	// RestartCount is the scheduler's restart count.
	RestartCount int32
	// Requeued is true when the job was requeued: a restart count above 0, or
	// a REQUEUED record in accounting.
	Requeued bool
	// Partition, TimeLimit and GPUs are what the job was established with,
	// for the read-back.
	Partition string
	TimeLimit time.Duration
	GPUs      int32
	// SubmitTime is when the scheduler accepted the job.
	SubmitTime time.Time
	// StartEstimate is a pending job's expected start: advisory, never
	// planned on. Nil when the scheduler gives none.
	StartEstimate *time.Time
	// StartTime is the actual start. Nil until the job has started.
	StartTime *time.Time
	// HardEnd is a running job's projected end, its start plus its time limit,
	// re-read on every observe. Nil until the job has started.
	HardEnd *time.Time
	// EndTime is when the job ended. Nil until it has.
	EndTime *time.Time
	// Source is where the record was read.
	Source Source
	// DataParser is the reply's data_parser version without its prefix.
	DataParser string
	// Accounting is what accounting reported about the job: set when the
	// record came from accounting (Source is SourceAccounting) or from a read
	// of accounting alone (Platform.ReadAccounting); nil for a record read from
	// the queue listing (HBEP-8, Provider accounting).
	Accounting *JobAccounting
}

// Held reports whether the job is held, by its user or an administrator.
func (j Job) Held() bool { return j.Hold != HoldNone }

// CancelOutcome is the result of cancelling one job.
type CancelOutcome string

// Cancel outcomes.
const (
	Cancelled    CancelOutcome = "Cancelled"
	AlreadyEnded CancelOutcome = "AlreadyEnded"
	CancelError  CancelOutcome = "Error"
)

// CancelResult is the result of cancelling one job.
type CancelResult struct {
	JobID   string
	Outcome CancelOutcome
	// Err is set exactly when Outcome is CancelError.
	Err error
}

// File is one file EnsureFile places under the staging directory.
type File struct {
	// Path is relative to the profile's stagingDir, with "/" separators and
	// no "..": for example agentapi.StagedAgentPath(digest) or
	// agentapi.StagedCredentialPath(tag).
	Path string
	// SHA256 is the content's SHA-256 in lowercase hex.
	SHA256 string
	// Mode is the file's permission bits, for example 0o600 for a credential.
	Mode os.FileMode
	// Content is the file's bytes. It never reaches a command line or a log.
	Content []byte
}

// CatalogEntry is one model a managed gateway lists (HBEP-4, Managed provider
// in alpha).
type CatalogEntry struct {
	// Name is the model's name in the catalog.
	Name string
	// Revision is the revision the gateway reports, nil when it reports none.
	Revision *string
}
