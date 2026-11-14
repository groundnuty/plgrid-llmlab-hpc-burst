package alpha

import "testing"

// The pre-staging helper's line, read by its one parser (stageline.go): the success line with its numbers and the rehash,
// the error and signalled lines by their step, and anything else refused: the line's old form without the rehash fields,
// a line not ended by a newline, a step outside the list, a second line.
func TestParseStageResultReadsTheHelpersLines(t *testing.T) {
	ok, err := ParseStageResult([]byte("outcome=ok image_seconds=600 image_built=true checkpoint_seconds=40 files=10 mismatched=0 rehash_files=0 rehash_listed=10 rehash_bytes=0 rehash_ms=0\n"))
	want := StageResult{Present: true, OK: true, ImageSeconds: 600, ImageBuilt: true, CheckpointSeconds: 40, Files: 10, Rehash: RehashObs{Read: true, Listed: 10}}
	if err != nil || ok != want {
		t.Errorf("the ok line read as %+v, %v; want %+v", ok, err, want)
	}
	warm, err := ParseStageResult([]byte("outcome=ok image_seconds=2 image_built=false checkpoint_seconds=9 files=10 mismatched=1 rehash_files=9 rehash_listed=10 rehash_bytes=15400000000 rehash_ms=8800\n"))
	if err != nil || warm.Mismatched != 1 || warm.ImageBuilt || warm.Rehash != (RehashObs{Read: true, Files: 9, Listed: 10, Bytes: 15400000000, MS: 8800}) {
		t.Errorf("a warm ok line read as %+v, %v", warm, err)
	}
	if r, err := ParseStageResult([]byte("outcome=error step=checkpoint\n")); err != nil || r != (StageResult{Present: true, Step: "checkpoint"}) {
		t.Errorf("the error line read as %+v, %v", r, err)
	}
	if r, err := ParseStageResult([]byte("outcome=signalled step=image\n")); err != nil || r != (StageResult{Present: true, Signalled: true, Step: "image"}) {
		t.Errorf("the signalled line read as %+v, %v", r, err)
	}
	for _, bad := range []string{
		"", "outcome=ok\n",
		"outcome=ok image_seconds=1 image_built=true checkpoint_seconds=1 files=1 mismatched=0\n",
		"outcome=ok image_seconds=1 image_built=true checkpoint_seconds=1 files=1 mismatched=0 rehash_files=0 rehash_listed=1 rehash_bytes=0 rehash_ms=0",
		"outcome=error step=/scratch\n", "outcome=signalled\n", "x\noutcome=error step=image\n",
	} {
		if _, err := ParseStageResult([]byte(bad)); err == nil {
			t.Errorf("%q read as a line of the helper's", bad)
		}
	}
}
