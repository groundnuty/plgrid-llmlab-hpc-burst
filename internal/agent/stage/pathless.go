package stage

import (
	"errors"
	"io/fs"
	"syscall"
)

// pathlessError is an error whose text names no path: the step that failed and the kind of the failure. The staging directory
// is account data (HBEP-4, Provider settings; #757): an operating system error holds the path it failed on, which is under it,
// and the agent logs the error and copies it into a report's staging.message, which reaches the Allocation's status. The error
// stays reachable by errors.Is and errors.As, so a caller can still tell a permission from a missing space.
type pathlessError struct {
	msg string
	err error
}

func (e *pathlessError) Error() string { return e.msg }
func (e *pathlessError) Unwrap() error { return e.err }

// stepFailed is err as "<step> failed: <kind>", where the kind is the operating system's own words for the failure (an errno's
// text: "permission denied", "no space left on device", "not a directory") and never the path of the *fs.PathError that carried
// it. An error that holds no errno says "unknown cause". step names what the stager was making, and the staging directory only
// by that word.
func stepFailed(step string, err error) error {
	if err == nil {
		return nil
	}
	kind := "unknown cause"
	var en syscall.Errno
	var pe *fs.PathError
	switch {
	case errors.As(err, &en):
		kind = en.Error()
	case errors.As(err, &pe) && pe.Err != nil:
		kind = pe.Err.Error()
	}
	return &pathlessError{msg: step + " failed: " + kind, err: err}
}
