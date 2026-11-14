package alpha

// The pre-staging row's helper line (internal/providerctl/conformance/stagehelper, result.line), read in one place: by the
// row itself (internal/providerctl/conformance, nowstage_test.go), by the stand-ins that pre-stage as the row does
// (standin_test.go, preStage), and by the helper's own test of its line, so that the writer and its readers cannot drift
// (#577's review: preStage kept the line's old form after the rehash fields were added).

import (
	"errors"
	"regexp"
	"strconv"
)

var (
	stageOKLine    = regexp.MustCompile(`^outcome=ok image_seconds=(\d+) image_built=(true|false) checkpoint_seconds=(\d+) files=(\d+) mismatched=(\d+) rehash_files=(\d+) rehash_listed=(\d+) rehash_bytes=(\d+) rehash_ms=(\d+)\n$`)
	stageErrorLine = regexp.MustCompile(`^outcome=error step=(image|files|checkpoint|config)\n$`)
	// stageSignalledLine is a step cut off by the signal: Slurm's TERM at the time limit, or a cancel.
	stageSignalledLine = regexp.MustCompile(`^outcome=signalled step=(image|files|checkpoint|config)\n$`)
)

// StageResult is the helper's line as read: present for any of its lines; OK for the success line, with its numbers and the
// stager's rehash (issue #575); Signalled with Step for a step the signal cut off; Step alone for a step's own error.
type StageResult struct {
	Present, OK, Signalled          bool
	Step                            string
	ImageSeconds, CheckpointSeconds int
	ImageBuilt                      bool
	Files, Mismatched               int
	Rehash                          RehashObs
}

// ParseStageResult reads the helper's line, newline-ended; anything but one of its lines is an error.
func ParseStageResult(b []byte) (StageResult, error) {
	if m := stageOKLine.FindSubmatch(b); m != nil {
		n := func(i int) int { v, _ := strconv.Atoi(string(m[i])); return v }
		n64 := func(i int) int64 { v, _ := strconv.ParseInt(string(m[i]), 10, 64); return v }
		return StageResult{Present: true, OK: true, ImageSeconds: n(1), ImageBuilt: string(m[2]) == "true", CheckpointSeconds: n(3), Files: n(4), Mismatched: n(5),
			Rehash: RehashObs{Read: true, Files: n(6), Listed: n(7), Bytes: n64(8), MS: n64(9)}}, nil
	}
	if m := stageErrorLine.FindSubmatch(b); m != nil {
		return StageResult{Present: true, Step: string(m[1])}, nil
	}
	if m := stageSignalledLine.FindSubmatch(b); m != nil {
		return StageResult{Present: true, Signalled: true, Step: string(m[1])}, nil
	}
	return StageResult{}, errors.New("the stage helper's result is not one of its lines")
}
