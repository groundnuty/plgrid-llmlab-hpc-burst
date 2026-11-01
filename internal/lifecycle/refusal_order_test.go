package lifecycle

import (
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// A failed rehash is judged before the refusal, on purpose (judgeVerify): the
// agent rehashes the checkpoint even when it refuses the file, and a rehash
// that fails or mismatches means the staged copy may differ from the one it
// read, so it is restaged and read again before it may stop the class. The
// rehash error takes the worker-restart rung and the rehash mismatch the
// restage; the refusal stops the class only on a verification whose rehash
// held (PR #169 review, Opus N2). The expected causes are written by hand.
func TestARefusalIsJudgedAfterAFailedRehash(t *testing.T) {
	refusal := &agentapi.GenerationConfigError{Param: "temperature", Value: `"warm"`, Want: "a number"}

	r := asTheAgentReportsARefusal(judgeResult(), refusal)
	r.Error = "rehash: stat config.json: input/output error"
	if got := judgeVerify(judgeDeclared(), r, true, judgeShapes()); got.Cause != causeError {
		t.Errorf("a refused file with a rehash error: cause %q (%s), want %s", got.Cause, got.Message, causeError)
	}

	r = asTheAgentReportsARefusal(judgeResult(), refusal)
	r.Files[0].Outcome = agentapi.FileMismatch
	if got := judgeVerify(judgeDeclared(), r, true, judgeShapes()); got.Cause != causeRehash {
		t.Errorf("a refused file whose rehash mismatches: cause %q (%s), want %s", got.Cause, got.Message, causeRehash)
	}

	// With the rehash clean the same refusal stops the class.
	r = asTheAgentReportsARefusal(judgeResult(), refusal)
	if got := judgeVerify(judgeDeclared(), r, true, judgeShapes()); got.Cause != causeContradiction {
		t.Errorf("a refused file whose rehash held: cause %q (%s), want %s", got.Cause, got.Message, causeContradiction)
	}
}

// A value this contract refuses although vLLM would accept it (a JSON boolean
// whose 1 or 0 passes the range) fails verification, and the message the
// controller records and sends in its event says so, so that nobody mistakes
// it for an error of vLLM's (PR #169 review, Opus N5). The message is written
// out by hand.
func TestTheVerificationMessageSaysWhenTheContractIsStricterThanVLLM(t *testing.T) {
	r := asTheAgentReportsARefusal(judgeResult(), &agentapi.GenerationConfigError{
		Param: "temperature", Value: "true", Want: "a number (a JSON boolean is refused here although vLLM would accept it)"})
	got := judgeVerify(judgeDeclared(), r, true, judgeShapes())
	want := "generation_config.json's temperature is true, not a number (a JSON boolean is refused here although vLLM would accept it)"
	if got.Cause != causeContradiction || got.Message != want {
		t.Errorf("cause %q, message %q\nwant %s and %q", got.Cause, got.Message, causeContradiction, want)
	}
}
