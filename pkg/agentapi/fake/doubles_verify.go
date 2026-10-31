package fake

import (
	"crypto/sha256"
	"encoding/hex"
	"maps"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The doubles in this file and in doubles_warmup.go and doubles_health.go are
// engines that fail one check each, for the lifecycle controller's tests (HBEP-7,
// Test plan). Each is a value for one Engine field. They report raw values
// where the controller judges, and a failed outcome only where the agent judges
// (agentapi's documentation, Who decides). The defaults pass.

// VerifyChangedFile returns a Verify that reports the file at path as a
// mismatch: it has changed since the identity record was made. The mismatch
// holds at every verification, so a re-staged copy fails again; a test that
// wants it cured can switch to DefaultVerify from OnDesired. Only a file the
// request lists can mismatch, and the request after a worker restart lists
// none. The agent's own hash is a value no record holds. Everything else is as
// DefaultVerify.
func VerifyChangedFile(path string) func(agentapi.DesiredInstance, agentapi.VerifyRequest) agentapi.VerifyResult {
	sum := sha256.Sum256([]byte("changed: " + path))
	hash := hex.EncodeToString(sum[:])
	return func(i agentapi.DesiredInstance, req agentapi.VerifyRequest) agentapi.VerifyResult {
		res := DefaultVerify(i, req)
		for n, f := range res.Files {
			if f.Path == path {
				res.Files[n] = agentapi.FileResult{Path: path, Outcome: agentapi.FileMismatch, SHA256: hash}
			}
		}
		return res
	}
}

// VerifySettings returns a Verify whose settings are DefaultVerify's with the
// overrides applied: the engine resolved other values than the spec declares,
// or settings it does not declare. The controller judges them. The overrides
// are copied.
func VerifySettings(overrides map[string]string) func(agentapi.DesiredInstance, agentapi.VerifyRequest) agentapi.VerifyResult {
	overrides = maps.Clone(overrides)
	return func(i agentapi.DesiredInstance, req agentapi.VerifyRequest) agentapi.VerifyResult {
		res := DefaultVerify(i, req)
		maps.Copy(res.Settings, overrides)
		return res
	}
}

// VerifyMaxModelLen returns a Verify that reports another max-model-len than
// DefaultVerify's 32768.
func VerifyMaxModelLen(value string) func(agentapi.DesiredInstance, agentapi.VerifyRequest) agentapi.VerifyResult {
	return VerifySettings(map[string]string{"max-model-len": value})
}

// VerifyReportsNothing is an engine that reports nothing: no version, no image
// digest, no settings and no fingerprint. The files still rehash, and match. The
// controller fails it, because those three are required.
func VerifyReportsNothing(i agentapi.DesiredInstance, req agentapi.VerifyRequest) agentapi.VerifyResult {
	return agentapi.VerifyResult{Files: DefaultVerify(i, req).Files}
}

// QualifyKVCapacity returns a Qualify that reports the given KV capacity at
// start-up, in tokens, and passes every check the agent judges. The controller
// compares the capacity with targetSessions times contextTokens.typical.
func QualifyKVCapacity(tokens int64) func(agentapi.DesiredInstance, agentapi.QualifyRequest) agentapi.QualifyResult {
	return func(i agentapi.DesiredInstance, req agentapi.QualifyRequest) agentapi.QualifyResult {
		res := DefaultQualify(i, req)
		res.KVCapacityTokens = tokens
		return res
	}
}

// QualifyLoopAtLongContext is an engine whose answer at the longest context
// loops: the behaviour check fails, and the others pass.
func QualifyLoopAtLongContext(i agentapi.DesiredInstance, req agentapi.QualifyRequest) agentapi.QualifyResult {
	res := DefaultQualify(i, req)
	for n, c := range res.Checks {
		if c.Name == agentapi.CheckBehaviour {
			res.Checks[n].Passed = false
			res.Checks[n].Message = "the answer at the longest context loops"
		}
	}
	return res
}

// goodOutputLen is the length of the outputs the greedy doubles return, enough
// for a divergence after the 32nd token.
const goodOutputLen = 64

// DefaultGreedyOutputs returns three identical outputs of 64 distinct tokens: a
// healthy engine's. The Agent's corruption check passes it.
func DefaultGreedyOutputs(agentapi.DesiredInstance, agentapi.QualifyRequest) [][]int32 {
	return GreedyDivergingAt(goodOutputLen)(agentapi.DesiredInstance{}, agentapi.QualifyRequest{})
}

// GreedyDivergingAt returns a GreedyOutputs whose three outputs share their
// first n tokens (0-based: the token at index n is the first that differs) and
// then differ. The outputs are 64 tokens long. With n below 32 the Agent's
// corruption check fails, as they differ within their first 32 tokens; with n
// of 32 or more it passes, as they differ only after the 32nd token (the
// positive control). With n of 64 or more they are identical.
func GreedyDivergingAt(n int) func(agentapi.DesiredInstance, agentapi.QualifyRequest) [][]int32 {
	return func(agentapi.DesiredInstance, agentapi.QualifyRequest) [][]int32 {
		outs := make([][]int32, corruptionOutputs)
		for k := range outs {
			o := make([]int32, goodOutputLen)
			for t := range o {
				o[t] = int32(t + 1)
				if t >= n {
					o[t] += int32(1000 * (k + 1))
				}
			}
			outs[k] = o
		}
		return outs
	}
}

// GreedySingleRepeatedToken returns three identical outputs that are one token
// repeated 64 times, which the Agent's corruption check fails.
func GreedySingleRepeatedToken(agentapi.DesiredInstance, agentapi.QualifyRequest) [][]int32 {
	outs := make([][]int32, corruptionOutputs)
	for k := range outs {
		o := make([]int32, goodOutputLen)
		for t := range o {
			o[t] = 7
		}
		outs[k] = o
	}
	return outs
}

// GreedyEmpty returns three empty outputs, which the Agent's corruption check
// fails.
func GreedyEmpty(agentapi.DesiredInstance, agentapi.QualifyRequest) [][]int32 {
	return make([][]int32, corruptionOutputs)
}
