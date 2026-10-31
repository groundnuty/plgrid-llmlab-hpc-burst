package fake

import (
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// Engine scripts what the engine behind a fake Agent does. Every func field
// has a default when nil; the defaults are exported (DefaultVerify and the
// others) so a test can take one and change a single field.
type Engine struct {
	// StartDelay is how long a worker stays in phase starting. The worker
	// becomes running once the Agent's clock reaches its StartTime plus
	// StartDelay. The default is 0.
	StartDelay time.Duration

	// Verify answers a verification request. The Agent sets Done on the result.
	Verify func(agentapi.DesiredInstance, agentapi.VerifyRequest) agentapi.VerifyResult

	// Qualify answers a qualification request. The Agent sets Done on the
	// result.
	Qualify func(agentapi.DesiredInstance, agentapi.QualifyRequest) agentapi.QualifyResult

	// WarmUpRound answers warm-up round n, counted from 1. The Agent sets the
	// round's number to n.
	WarmUpRound func(n int32, req agentapi.WarmUpRequest) agentapi.WarmUpRound

	// Health samples a running worker. The Agent sets the sample's Time to now
	// when the result has none.
	Health func(now time.Time, worker string) agentapi.Health
}

func (e Engine) verify(i agentapi.DesiredInstance, req agentapi.VerifyRequest) agentapi.VerifyResult {
	if e.Verify != nil {
		return e.Verify(i, req)
	}
	return DefaultVerify(i, req)
}

func (e Engine) qualify(i agentapi.DesiredInstance, req agentapi.QualifyRequest) agentapi.QualifyResult {
	if e.Qualify != nil {
		return e.Qualify(i, req)
	}
	return DefaultQualify(i, req)
}

func (e Engine) warmUpRound(n int32, req agentapi.WarmUpRequest) agentapi.WarmUpRound {
	if e.WarmUpRound != nil {
		return e.WarmUpRound(n, req)
	}
	return DefaultWarmUpRound(n, req)
}

func (e Engine) health(now time.Time, worker string) agentapi.Health {
	h := DefaultHealth(now, worker)
	if e.Health != nil {
		h = e.Health(now, worker)
	}
	if h.Time.IsZero() {
		h.Time = now
	}
	return h
}

// DefaultVerify matches every requested file and reports an engine of version
// 0.32.0 that runs the Instance's image with max-model-len 32768.
func DefaultVerify(i agentapi.DesiredInstance, req agentapi.VerifyRequest) agentapi.VerifyResult {
	var files []agentapi.FileResult
	for _, f := range req.Files {
		files = append(files, agentapi.FileResult{Path: f.Path, Outcome: agentapi.FileMatch})
	}
	return agentapi.VerifyResult{
		Files:            files,
		EngineVersion:    "0.32.0",
		ImageDigest:      i.Launch.ImageDigest,
		Settings:         map[string]string{"max-model-len": "32768"},
		TemplateOverhead: 12,
		Fingerprint:      "fp-example",
	}
}

// DefaultQualify passes every check (the tool-call check only when the request
// asks for it), with a KV capacity of 1<<20 tokens and one served model name.
// The checks come in the order health, chat, toolCall, corruption, behaviour.
func DefaultQualify(_ agentapi.DesiredInstance, req agentapi.QualifyRequest) agentapi.QualifyResult {
	names := []agentapi.CheckName{agentapi.CheckHealth, agentapi.CheckChat}
	if req.ToolCalls {
		names = append(names, agentapi.CheckToolCall)
	}
	names = append(names, agentapi.CheckCorruption, agentapi.CheckBehaviour)
	checks := make([]agentapi.CheckOutcome, 0, len(names))
	for _, n := range names {
		checks = append(checks, agentapi.CheckOutcome{Name: n, Passed: true})
	}
	return agentapi.QualifyResult{
		Checks:           checks,
		KVCapacityTokens: 1 << 20,
		ServedModelNames: []string{"example-model"},
	}
}

// DefaultWarmUpRound sends one request each of the shapes short, typical,
// longest and longOutput, one toolCall request when the request asks for it,
// then TargetSessions concurrent requests. Each takes 100 ms to the first
// token and 2 s in all, and produces the shape's output: the longest output
// for longOutput, the typical output for the rest.
func DefaultWarmUpRound(n int32, req agentapi.WarmUpRequest) agentapi.WarmUpRound {
	timing := func(shape agentapi.Shape, out int32) agentapi.RequestTiming {
		return agentapi.RequestTiming{
			Shape:            shape,
			TimeToFirstToken: agentapi.Duration(100 * time.Millisecond),
			Total:            agentapi.Duration(2 * time.Second),
			OutputTokens:     out,
		}
	}
	typical, longest := req.Shapes.OutputTypical, req.Shapes.OutputLongest
	reqs := []agentapi.RequestTiming{
		timing(agentapi.ShapeShort, typical),
		timing(agentapi.ShapeTypical, typical),
		timing(agentapi.ShapeLongest, typical),
		timing(agentapi.ShapeLongOutput, longest),
	}
	if req.ToolCalls {
		reqs = append(reqs, timing(agentapi.ShapeToolCall, typical))
	}
	for range req.TargetSessions {
		reqs = append(reqs, timing(agentapi.ShapeConcurrent, typical))
	}
	return agentapi.WarmUpRound{Round: n, Requests: reqs}
}

// DefaultHealth is a healthy, idle engine: the probe passes, the gauges and
// the token counter are zero and the KV cache is 10% used.
func DefaultHealth(now time.Time, _ string) agentapi.Health {
	return agentapi.Health{Time: now, ProbeOK: true, KVCacheUsage: 0.1}
}
