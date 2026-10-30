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
	// staging directory.
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
