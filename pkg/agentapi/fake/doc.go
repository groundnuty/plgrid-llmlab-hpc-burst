// Package fake holds test doubles for the agent protocol (package agentapi): a
// Backend, which stands in for the lifecycle controller; an Agent, which drives
// a Backend as the real allocation agent would; a Gate, which stands in for the
// agent API server's authentication step in front of a Backend; and engine
// doubles, each failing one check, for the Agent's Engine.
//
// The lifecycle controller's tests (issue #61) drive its Backend
// implementation with an Agent; the agent API server's tests (issue #60) call
// a Backend through the server. The Agent's own tests run against this
// package's Backend.
//
// The Agent scripts its engine with an Engine. The defaults pass every check.
// The doubles (VerifyChangedFile, QualifyKVCapacity, GreedyDivergingAt,
// WarmUpNeverSettles, HealthStalled and the others) are values for one Engine
// field each. They report raw values where the controller judges, and fail a
// check only where the agent judges: the corruption check, which the Agent
// judges from the raw token outputs of Engine.GreedyOutputs, and the behaviour
// check. Engine.IgnoreTerm makes a worker ignore TERM until the stop grace
// runs out. The health doubles keep their counters per worker process
// (Process.PID), so a restarted worker's counters start again.
//
// The Agent follows the protocol where a test could otherwise pass for the
// wrong reason:
//
//   - Checks need the intended running process. Verification, qualification and
//     each warm-up round are answered only while every desired worker is running
//     a process started for the desired worker and Instance generations; a new
//     verification generation gets no result from a process that is still
//     stopping, starting or exited, and its results come in the Step the new
//     process runs.
//   - Staging takes Engine.StagingDuration from the Instance's first apply and
//     can fail (Engine.StagingError). No worker starts before it is done, and a
//     failed staging is tried again only when the Instance generation rises.
//   - An Action the Agent does not know changes nothing, and the Agent keeps
//     polling and reporting (agentapi's documentation, Versioning).
//   - Every report is encoded with agentapi.EncodeBody, so one above
//     agentapi.MaxBodyBytes fails the Step as the HTTP path would refuse it; the
//     snapshot cuts RequestTiming.Error to agentapi.MaxTimingErrorBytes and
//     VerifyResult.Error, CheckOutcome.Message and Staging.Message to
//     agentapi.MaxMessageBytes (agentapi.TruncateUTF8); and a warm-up request
//     with TargetSessions outside 1..agentapi.MaxTargetSessions fails the Step.
//   - The Agent draws its key with agentapi.NewKey and its agent instance ID
//     with agentapi.NewAgentInstanceID, and the Backend answers CodeBadRequest
//     for a caller whose agent instance ID fails agentapi.CheckAgentInstanceID
//     and for a Report whose Instance key is set and fails agentapi.CheckKey, as
//     the real server and controller will.
//
// The package imports only the standard library and package agentapi, because
// the agent is a static binary and this package must not drag the Kubernetes
// libraries into it.
//
// No double has a goroutine or a timer. The Agent works one Step at a time and
// takes its time from a function the test injects, so a test decides when a
// worker starts, when a kill is due and when a report is due.
package fake
