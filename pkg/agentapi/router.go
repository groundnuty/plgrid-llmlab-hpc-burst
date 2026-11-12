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
	// Workers are the workers as the router holds them, from its admin's /clusters?format=json, read
	// at ClustersReadAt; absent until the first read (HBEP-5, Worker health).
	Workers        []RouterWorker `json:"workers,omitempty"`
	ClustersReadAt *time.Time     `json:"clustersReadAt,omitempty"`
	// Probe is the agent's own probe of the router: GET /health through the router's socket, with no
	// x-session-id, so it reaches a worker the router holds healthy (HBEP-7, Several workers).
	Probe *RouterProbe `json:"probe,omitempty"`
}

// RouterWorker is one worker as the router holds it.
type RouterWorker struct {
	// Name is the worker's name, the endpoint's hostname in the router's config.
	Name string `json:"name"`
	// Healthy says the router routes to it: its active health check passes and it is not ejected.
	Healthy bool `json:"healthy"`
	// FailedActiveCheck says its active health check fails (Envoy's failed_active_health_check).
	FailedActiveCheck bool `json:"failedActiveCheck,omitempty"`
	// Ejected says outlier detection has ejected it (Envoy's failed_outlier_check).
	Ejected bool `json:"ejected,omitempty"`
	// ConsecutiveFailedChecks counts the agent's reads of /clusters in a row, up to the latest, that
	// found the worker's active check failing: the controller's Envoy-unhealthy signal compares it with
	// health.failureThreshold (HBEP-7, Several workers).
	ConsecutiveFailedChecks int32 `json:"consecutiveFailedChecks,omitempty"`
}

// RouterProbe is the latest of the agent's probes of the router.
type RouterProbe struct {
	Time time.Time `json:"time"`
	// OK says the probe got a 200.
	OK bool `json:"ok"`
	// ConsecutiveFailures counts the failed probes in a row, up to this one.
	ConsecutiveFailures int32 `json:"consecutiveFailures,omitempty"`
}
