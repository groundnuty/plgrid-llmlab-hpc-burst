//go:build !linux

package livechecks

import "errors"

// The reaper needs Linux (PR_SET_CHILD_SUBREAPER and /proc). Elsewhere the package still
// compiles, and every run fails loudly: the reaper writes an error report, never a clean one.

func setSubreaper() error { return errors.New("this needs Linux (PR_SET_CHILD_SUBREAPER)") }

func readProcTable() (map[int]procInfo, error) { return nil, errors.New("this needs Linux (/proc)") }
