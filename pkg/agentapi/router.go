package agentapi

import "time"

// The router part (beta, opt-in; HBEP-5, Inside an Instance; HBEP-7, Health and restarts; #429).
// An Instance whose launch names a router runs it in front of its workers: the desired state carries
// DesiredInstance.Router, and every report InstanceReport.Router, as doc.go's Parts section planned.
// A controller that sends no Router means alpha's shape, and an older agent ignores one.

// RouterKind is a router an Instance can run, the strings of v1alpha1.InstanceRouter.
type RouterKind string

// Routers.
const (
	// RouterEnvoy is Envoy 1.40.2 with ring_hash on x-session-id (internal/agent/envoypin).
	RouterEnvoy RouterKind = "envoy"
)

// PartRouter is the router's part name, under which its restart generation is raised and its
// restarts recorded (v1alpha1.PartRouter).
const PartRouter = "router"

// DesiredRouter is the router the agent should run in front of the Instance's workers.
type DesiredRouter struct {
	// Kind is the router.
	Kind RouterKind `json:"kind"`
	// Generation is the router's restart generation. When it rises, the agent stops the router
	// politely, kills it after the stop grace, and starts it again; the workers are not touched
	// (HBEP-7, the router rung). The agent never restarts the router by itself.
	Generation int64 `json:"generation"`
}

// RouterReport is the router part's state, as WorkerReport is a worker's.
type RouterReport struct {
	// Kind is the router the agent runs.
	Kind RouterKind `json:"kind"`
	// Phase is the router's: starting until its sockets are up and checked, then running; exited
	// once it exits or a start of it fails or is refused.
	Phase PartPhase `json:"phase"`
	// PID and StartTime identify the process; absent before it starts.
	PID       int        `json:"pid,omitempty"`
	StartTime *time.Time `json:"startTime,omitempty"`
	// ExitCode is set once the process has exited; -1 for a process a signal ended, and
	// startFailedExitCode for a start that failed or was refused, whose reason is in LogTail.
	ExitCode *int `json:"exitCode,omitempty"`
	// Generation is the router restart generation this process was started for: the desired
	// router generation it applied, which the controller compares with the one it desires.
	Generation int64 `json:"generation"`
	// Socket is the router's listener, as a path on the compute node: the forwarder's target.
	Socket string `json:"socket,omitempty"`
	// LogTail is the end of the router's log, at most MaxLogTailBytes and cut at a UTF-8 boundary,
	// once it has exited without being told to or could not start; for a refused start, the
	// refusal. Absent otherwise.
	LogTail string `json:"logTail,omitempty"`
}
