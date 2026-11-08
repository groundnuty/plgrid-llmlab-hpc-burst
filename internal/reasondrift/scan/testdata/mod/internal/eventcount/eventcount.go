// Package eventcount stands for internal/eventcount: the functions whose first
// argument must be the value of a recorder builder.
package eventcount

import "github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rb"

// Events and Legacy are the counters the scan knows by default.
func Events(r rb.Recorder, controller string) rb.Recorder { return r }

func Legacy(r rb.Recorder, controller string) rb.Recorder { return r }

// Behind takes its recorder second, so it is no counter of the first argument,
// though a test lists it as one. Twice takes two recorders.
func Behind(controller string, r rb.Recorder) rb.Recorder { return r }

func Twice(a, b rb.Recorder) rb.Recorder { return a }
