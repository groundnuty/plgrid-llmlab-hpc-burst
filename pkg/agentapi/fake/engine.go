package fake

import (
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// Engine scripts what the engine behind a fake Agent does. Every func field
// has a default when nil; the defaults are exported (DefaultVerify and the
// others) so a test can take one and change a single field.
//
// The Agent asks Verify, Qualify, GreedyOutputs and WarmUpRound only while the
// Instance's workers run: a section is asked once every desired worker runs a
// process started for the desired generations, and not before (a check of a
// worker that is staging, starting, stopping or exited is never asked). What
// they return is reported as the real agent reports it: free text is cut to the
// protocol's bounds (agentapi.MaxTimingErrorBytes, agentapi.MaxMessageBytes), and
// a report above agentapi.MaxBodyBytes fails the Step.
type Engine struct {
	// StartDelay is how long a worker stays in phase starting. The worker
	// becomes running once the Agent's clock reaches its StartTime plus
	// StartDelay. The default is 0. Checks wait for the worker to run, so none
	// is answered before then.
	StartDelay time.Duration

	// StagingDuration is how long staging takes. A new Instance is reported
	// with Staging inProgress until the Agent's clock reaches the Step in which
	// the Agent first took the Instance up (the first apply, after any earlier
	// Instance's processes are gone) plus StagingDuration; it is then reported
	// done, or failed when StagingError is set. No worker starts before staging
	// is done. The default is 0: staging is done at once, in the first Step.
	StagingDuration time.Duration

	// StagingError, when not empty, makes staging fail once StagingDuration has
	// passed: Staging.Phase is failed and Staging.Message is StagingError, cut
	// to agentapi.MaxMessageBytes (agentapi.TruncateUTF8). After a failure no
	// worker starts, however many Steps pass, until the desired Instance
	// generation rises past the one the failed staging was for. That re-stages:
	// staging is inProgress again for StagingDuration, then fails again, because
	// the Engine's settings are the same. A staging that succeeded is not
	// repeated when the generation rises (the agent rehashes the staged files
	// and finds them whole).
	StagingError string

	// IgnoreTerm makes every worker ignore TERM. On a stop instruction (a
	// raised restart generation, idle, stop, or a new Instance) such a worker
	// is reported stopping until the Agent's clock reaches the time of the
	// instruction plus Settings.StopGrace of the desired state that carried
	// it, and is then killed: stopped, with exit code -1. A restart's new
	// process starts in the Step after the kill, and a new verification
	// generation's checks are answered only once that process runs, never by the
	// process that is stopping. Without IgnoreTerm a worker stops at once with
	// exit code 0.
	IgnoreTerm bool

	// Verify answers a verification request. The Agent sets Done on the result.
	Verify func(agentapi.DesiredInstance, agentapi.VerifyRequest) agentapi.VerifyResult

	// Qualify answers a qualification request. The Agent sets Done on the
	// result.
	Qualify func(agentapi.DesiredInstance, agentapi.QualifyRequest) agentapi.QualifyResult

	// GreedyOutputs returns the token IDs of the outputs of the corruption
	// check's greedy request, sent three times in a row. When it is set, the
	// Agent judges them by the rule in agentapi's documentation (three
	// outputs, sharing their first 32 tokens, none empty or a single repeated
	// token) and the verdict replaces the corruption outcome that Qualify
	// returned, or is added when it returned none. When it is nil, the
	// corruption outcome is Qualify's own. DefaultGreedyOutputs passes.
	GreedyOutputs func(agentapi.DesiredInstance, agentapi.QualifyRequest) [][]int32

	// WarmUpRound answers warm-up round n, counted from 1. The Agent sets the
	// round's number to n. The request's TargetSessions is within
	// 1..agentapi.MaxTargetSessions (the Agent refuses a desired state that asks
	// for another), and a round that makes the report exceed
	// agentapi.MaxBodyBytes fails the Step.
	WarmUpRound func(n int32, req agentapi.WarmUpRequest) agentapi.WarmUpRound

	// Health samples a running worker process. The Agent sets the sample's Time
	// to now when the result has none. It never sets MetricsReadAt or a gauge:
	// a sample with no MetricsReadAt is an engine whose /metrics was never
	// read, and its gauges are unknown, so a double that wants current metrics
	// sets them (DefaultHealth and the Health doubles do). It is given the process, not the worker's
	// name, because the counters of agentapi.Health are cumulative since the
	// process started: a double whose counters rise must start them again for a
	// new process (the doubles in this package key theirs by Process.PID).
	Health func(now time.Time, p Process) agentapi.Health
}

// Process identifies one worker process the Agent started, as Engine.Health is
// told which engine it samples. A restart is a new Process: another PID, start
// time and, when a generation rose, another Generation.
type Process struct {
	// Worker is the worker's name, agentapi.WorkerName(i).
	Worker string
	// PID is the process's PID. The Agent never reuses one, across restarts and
	// Requeue.
	PID int
	// Start is the Agent's time when the process was started.
	Start time.Time
	// Generation is the worker's restart generation the process was started for.
	Generation int64
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

func (e Engine) health(now time.Time, p Process) agentapi.Health {
	h := DefaultHealth(now, p)
	if e.Health != nil {
		h = e.Health(now, p)
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
// The behaviour check ran at the request's longest context
// (CheckOutcome.ContextTokens), as an engine with no reasoning parser does.
func DefaultQualify(_ agentapi.DesiredInstance, req agentapi.QualifyRequest) agentapi.QualifyResult {
	names := []agentapi.CheckName{agentapi.CheckHealth, agentapi.CheckChat}
	if req.ToolCalls {
		names = append(names, agentapi.CheckToolCall)
	}
	names = append(names, agentapi.CheckCorruption, agentapi.CheckBehaviour)
	checks := make([]agentapi.CheckOutcome, 0, len(names))
	for _, n := range names {
		o := agentapi.CheckOutcome{Name: n, Passed: true}
		if n == agentapi.CheckBehaviour {
			o.ContextTokens = int64(req.Shapes.ContextLongest)
		}
		checks = append(checks, o)
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

// DefaultHealth is a healthy, idle engine: the probe passes, the metrics are
// read at every sample (MetricsReadAt is the sample's time), the gauges and the
// token counter are zero, set explicitly, and the KV cache is 10% used.
func DefaultHealth(now time.Time, _ Process) agentapi.Health {
	return metricsSample(now, 0, 0, 0, 0.1)
}
