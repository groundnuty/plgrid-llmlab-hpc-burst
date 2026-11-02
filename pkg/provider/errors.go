package provider

import (
	"errors"
	"fmt"
	"strings"
)

// Error kinds. Operations wrap them, so callers test with errors.Is. An error
// that wraps none of them is unknown: the plugin cannot tell what happened.
var (
	// ErrNotHeld: LiftHold on a job that is not held.
	ErrNotHeld = errors.New("job is not held")
	// ErrAdminHold: LiftHold on a job an administrator holds. Reported as a
	// condition (v1alpha1.ConditionAdminHold), never retried.
	ErrAdminHold = errors.New("job is held by an administrator")
	// ErrNotFound: LiftHold on a job the scheduler does not know.
	ErrNotFound = errors.New("job not found")
	// ErrInvalidRequest: a submission or file refused before any call: an
	// unsafe value, a partition the Provider does not allow or the profile
	// does not list, no GPU on a GPU partition, more GPUs than gpusPerNode, a
	// time limit above maxTime or not in whole minutes, a path outside the
	// staging directory; and the QOS bounds (Batch.Submit).
	ErrInvalidRequest = errors.New("request refused before any call")
	// ErrDialectMismatch: a reply's data_parser, without its "data_parser/"
	// prefix, differs from the profile's jsonDataParser. The reply is not
	// interpreted, the read is unknown, and the Provider gets
	// v1alpha1.ConditionDialectMismatch (HBEP-4, The Slurm dialect).
	ErrDialectMismatch = errors.New("reply's data_parser differs from the profile's")
	// ErrPathUnresolved: HOME, or the variable stagingDir starts with, is
	// unset, empty or fails the safe pattern on the login node. No call that
	// needs the path is made, and the Provider gets
	// v1alpha1.ConditionPathUnresolved (HBEP-4, Transport).
	ErrPathUnresolved = errors.New("login-node path unresolved")
	// ErrCredentials: the credentials file, or the key file it names, is
	// readable by others or not owned by the controller's user. No provider
	// call is made, and the Provider gets v1alpha1.ConditionCredentialsInvalid
	// (HBEP-11, Credentials).
	ErrCredentials = errors.New("credentials file refused")
	// ErrKeyRejected: a managed gateway answered the catalog read with 401
	// or 403. The Provider gets v1alpha1.ConditionCredentialsRejected, and a
	// managed-source ModelDeployment it serves is not Ready (HBEP-4, Managed
	// provider in alpha; HBEP-7, Reconcile loop, step 1).
	ErrKeyRejected = errors.New("gateway rejected the key")
)

// FieldError is one invalid field of a Provider or its PlatformProfile.
type FieldError struct {
	// Field is the field's path, for example "spec.partitions[1].gpusPerNode"
	// or "provider.spec.allowedPartitions".
	Field string
	// Message says what is wrong with it.
	Message string
}

// InvalidProfileError is ReadProfile's refusal. It names every invalid field,
// not only the first (HBEP-4, Core operations).
type InvalidProfileError struct {
	// Provider is the Provider's name.
	Provider string
	// Fields are the invalid fields, at least one.
	Fields []FieldError
}

func (e *InvalidProfileError) Error() string {
	parts := make([]string, len(e.Fields))
	for i, f := range e.Fields {
		parts[i] = f.Field + ": " + f.Message
	}
	return fmt.Sprintf("provider %s: invalid profile or settings: %s", e.Provider, strings.Join(parts, "; "))
}

// Slurm's reason names for a full submit limit (HBEP-4, QOS limits): a QOS's
// MaxSubmitJobsPerUser, an association's MaxSubmitJobs and an account's, and
// the group limits GrpSubmitJobs of a QOS and of an association. A policy
// refusal with one of them is SubmitLimitReached; any other reason is an
// ordinary Rejected attempt (`@hpc-burst-science-agent`, 1 Nov 2026, from PR
// #151's review; the two group names from PR #155's review). Each is a job
// reason string in Slurm 24.11.5 (src/common/job_state_reason.c lines 210,
// 246, 278, 306 and 720) and 23.11.7 (src/common/slurm_protocol_defs.c).
const (
	ReasonQOSMaxSubmitJobPerUserLimit = "QOSMaxSubmitJobPerUserLimit"
	ReasonAssocMaxSubmitJobLimit      = "AssocMaxSubmitJobLimit"
	ReasonMaxSubmitJobsPerAccount     = "MaxSubmitJobsPerAccount"
	ReasonQOSGrpSubmitJobsLimit       = "QOSGrpSubmitJobsLimit"
	ReasonAssocGrpSubmitJobsLimit     = "AssocGrpSubmitJobsLimit"
)

// GroupLimitNote is what a refusal for a group submit limit adds to its
// message: the slot it waits for may be held by any of the grant's users, not
// only by this login (`@hpc-burst-science-agent`, 1 Nov 2026).
const GroupLimitNote = "group limit, shared by the grant's users"

// IsSubmitLimitReason reports whether a Slurm reason name is a full submit
// limit: exactly one of the five names above. Any other name, a stand-alone
// breach such as QOSMaxWallDurationPerJobLimit included, is not.
func IsSubmitLimitReason(name string) bool {
	switch name {
	case ReasonQOSMaxSubmitJobPerUserLimit, ReasonAssocMaxSubmitJobLimit, ReasonMaxSubmitJobsPerAccount,
		ReasonQOSGrpSubmitJobsLimit, ReasonAssocGrpSubmitJobsLimit:
		return true
	}
	return false
}

// IsGroupSubmitLimitReason reports whether a Slurm reason name is a group
// submit limit, QOSGrpSubmitJobsLimit or AssocGrpSubmitJobsLimit: a full
// slot, whose message carries GroupLimitNote.
func IsGroupSubmitLimitReason(name string) bool {
	return name == ReasonQOSGrpSubmitJobsLimit || name == ReasonAssocGrpSubmitJobsLimit
}

// PolicyLine is the line sbatch prints last for every accounting-policy
// refusal, a full submit limit or any other limit: Slurm's error text for
// ESLURM_ACCOUNTING_POLICY after "Batch job submission failed:" (HBEP-4, QOS
// limits, from src/sbatch/sbatch.c and src/common/slurm_errno.c). It is the
// same for every reason, so it never tells one from another.
const PolicyLine = "Batch job submission failed: Job violates accounting/QOS policy (job submit limit, user's size and/or time limits)"

// PolicyRefusal is a submission that Slurm refused for its accounting policy:
// sbatch's standard error ended with PolicyLine, and the line before it named
// the reason (HBEP-4, QOS limits, A limit refusal). Submit returns it,
// possibly wrapped, with the outcome Rejected; callers find it with errors.As
// and classify it by Reason, never by its text (`@hpc-burst-science-agent`,
// 1 Nov 2026). A refusal with no reason line is not a PolicyRefusal.
type PolicyRefusal struct {
	// Reason is Slurm's reason name, for example QOSMaxSubmitJobPerUserLimit.
	Reason string
	// Line is Slurm's reason line as printed, for example "sbatch: error:
	// QOSMaxSubmitJobPerUserLimit", with paths redacted. It is for people:
	// the Allocation's message carries it, through Error.
	Line string
}

// Error returns the reason line, and for a group submit limit the line
// followed by GroupLimitNote in parentheses, so the Allocation's message
// says whose jobs fill the slot.
func (e *PolicyRefusal) Error() string {
	line := e.Line
	if line == "" {
		line = "sbatch: error: " + e.Reason
	}
	if IsGroupSubmitLimitReason(e.Reason) {
		return line + " (" + GroupLimitNote + ")"
	}
	return line
}

// SubmitLimit reports whether the refusal is for a full submit limit
// (IsSubmitLimitReason): the Allocation then waits with reason
// SubmitLimitReached, and nothing fails.
func (e *PolicyRefusal) SubmitLimit() bool { return IsSubmitLimitReason(e.Reason) }
