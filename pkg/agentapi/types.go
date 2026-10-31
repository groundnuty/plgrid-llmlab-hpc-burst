package agentapi

import (
	"encoding/json"
	"fmt"
	"time"
)

// Duration is a time.Duration that travels as its Go string, for example
// "1.5s" or "10m0s".
type Duration time.Duration

// MarshalJSON writes the duration as a string.
func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(time.Duration(d).String()) }

// UnmarshalJSON reads a duration string.
func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("duration: want a string such as \"1.5s\": %w", err)
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("duration %q: %w", s, err)
	}
	*d = Duration(v)
	return nil
}

// ---- registration ----

// RegisterRequest is the body of PathRegister. The credential, tag, job ID and
// agent instance ID travel in headers (doc.go).
type RegisterRequest struct {
	// RestartCount is the scheduler's restart count of the job, from
	// SLURM_RESTART_COUNT; 0 when the scheduler sets none. A requeued job
	// registers with a higher count (doc.go).
	RestartCount int32 `json:"restartCount,omitempty"`
	// AgentVersion is the agent build's version, for diagnostics.
	AgentVersion string `json:"agentVersion,omitempty"`
	// Node is the compute node's host name.
	Node string `json:"node"`
	// CPUArch is the node's machine name as the kernel gives it (uname -m):
	// aarch64 or x86_64, the strings the platform profile uses.
	CPUArch string `json:"cpuArch"`
	// GPUs are the GPUs the job can use.
	GPUs []GPU `json:"gpus,omitempty"`
}

// GPU is one GPU the job can use.
type GPU struct {
	// Index is the GPU's index as the job sees it.
	Index int `json:"index"`
	// Name is the product name as the driver reports it.
	Name string `json:"name"`
	// MemoryMiB is its memory.
	MemoryMiB int64 `json:"memoryMiB,omitempty"`
}

// RegisterResponse answers a registration.
type RegisterResponse struct {
	// ProtocolVersion is the endpoint's protocol version.
	ProtocolVersion string `json:"protocolVersion"`
}

// ---- desired state ----

// Action is what an Allocation's agent should do.
type Action string

// Actions.
const (
	// ActionIdle: run nothing. Any running part is stopped (politely, then
	// killed after the stop grace); the agent stays and keeps reporting, so a
	// kept Allocation can host a later Instance (HBEP-7, After the Instance).
	ActionIdle Action = "idle"
	// ActionRun: run DesiredState.Instance.
	ActionRun Action = "run"
	// ActionStop: stop every part, then exit, which ends the job.
	ActionStop Action = "stop"
)

// DesiredState is what an Allocation's agent should run now (HBEP-7, Desired
// state, pulled).
type DesiredState struct {
	// Version identifies this state for the long poll (WithVersion).
	Version string `json:"version"`
	// Action is what to do.
	Action Action `json:"action"`
	// Instance is set exactly when Action is ActionRun.
	Instance *DesiredInstance `json:"instance,omitempty"`
	// Settings are the profile values the agent applies.
	Settings AgentSettings `json:"settings"`
}

// AgentSettings are the platform profile's values the agent itself applies.
type AgentSettings struct {
	// ProbeInterval is how often the agent probes each engine's health
	// (health.probeInterval).
	ProbeInterval Duration `json:"probeInterval"`
	// StopGrace is how long a polite stop lasts before a kill
	// (restart.stopGrace).
	StopGrace Duration `json:"stopGrace"`
	// ImageFormat is the format the agent builds and runs the engine image
	// in, from the partition's imageFormat in the platform profile
	// (v1alpha1.ImageFormatOf): ImageFormatSIF or ImageFormatSandbox. Empty,
	// from an older controller, means ImageFormatSIF.
	ImageFormat ImageFormat `json:"imageFormat,omitempty"`
}

// ImageFormat is a container image format Apptainer runs, the strings of
// v1alpha1.ImageFormat.
type ImageFormat string

// Image formats. On a partition whose compute nodes cannot run a SIF, such as
// Athena's A100 partition (spike S1, probe 2), the agent builds a sandbox
// directory instead.
const (
	ImageFormatSIF     ImageFormat = "sif"
	ImageFormatSandbox ImageFormat = "sandbox"
)

// DesiredInstance is the one Instance an Allocation should run.
type DesiredInstance struct {
	// Name and UID identify the Instance; DeploymentID is its full ID.
	Name         string `json:"name"`
	UID          string `json:"uid"`
	DeploymentID string `json:"deploymentID"`

	// Launch is the Instance's immutable launch, which every start and restart
	// runs exactly.
	Launch Launch `json:"launch"`

	// Generation is the whole Instance's restart generation. When it rises,
	// the agent stops every part, rehashes the staged files, re-stages any
	// that fail, and starts again (HBEP-7, the alpha ladder, rung 2).
	Generation int64 `json:"generation"`

	// Workers are the Instance's engine processes. Alpha: exactly one,
	// worker-0. A router, once Instances have one, is a new optional field
	// beside this one (doc.go, Parts).
	Workers []DesiredWorker `json:"workers"`

	// Checks, when set, asks the agent to verify, qualify or warm up now.
	Checks *Checks `json:"checks,omitempty"`

	// HubToken is the token for a gated checkpoint. It reaches the download
	// only through the environment, and never a file outside the job, a
	// command line or a log (HBEP-7, Keys).
	HubToken string `json:"hubToken,omitempty"`

	// KeyStored says the controller has stored the Instance's key; until it
	// is true, every report carries the key (InstanceReport.Key).
	KeyStored bool `json:"keyStored,omitempty"`
}

// DesiredWorker is one engine process of an Instance.
type DesiredWorker struct {
	// Name is the worker's name, WorkerName(i).
	Name string `json:"name"`
	// Generation is the worker's restart generation. When it rises, the agent
	// stops the worker politely, kills it after the stop grace, and starts it
	// again from the same launch (HBEP-7, the alpha ladder, rung 1).
	Generation int64 `json:"generation"`
	// EngineSocket is the Unix socket the engine listens on, as a path inside
	// the container: ContainerEngineSocket(Name). Launch.Command names it. The
	// agent binds the job's private temporary directory at ContainerRunDir,
	// so no port can clash on a shared node (HBEP-7, What it runs; L-7.7).
	EngineSocket string `json:"engineSocket"`
}

// Launch is an Instance's launch on the wire, a copy of the Instance's
// spec.launch (v1alpha1.Launch).
type Launch struct {
	// Image is the image's repository; the agent pulls Image@ImageDigest into
	// <stagingDir>/images/.
	Image       string `json:"image"`
	ImageDigest string `json:"imageDigest"`
	// Command is the argument vector inside the image, program first.
	Command []string `json:"command"`
	// Env is the engine's environment, added to a clean one.
	Env []EnvVar `json:"env,omitempty"`
	// KeyEnv names the variable through which the engine gets the Instance's
	// key, which the agent generates at Instance start (HBEP-7, Keys).
	KeyEnv string `json:"keyEnv,omitempty"`
	// Checkpoint is staged under StagedCheckpointDir and bound read-only at
	// ContainerModelDir.
	Checkpoint Checkpoint `json:"checkpoint"`
	// ChatTemplate, when set, is written to ContainerChatTemplate.
	ChatTemplate *string `json:"chatTemplate,omitempty"`
	// GPUs is the number of GPUs the engine uses.
	GPUs int32 `json:"gpus"`
	// MaxNumSeqs is the --max-num-seqs that Command passes, recorded for
	// reading; the agent runs Command as it is.
	MaxNumSeqs int32 `json:"maxNumSeqs,omitempty"`
}

// EnvVar is one environment variable.
type EnvVar struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// Checkpoint is a checkpoint at a full commit id.
type Checkpoint struct {
	Repository string `json:"repository"`
	Revision   string `json:"revision"`
}

// Checks asks the agent to run checks now (HBEP-7, Instance start). The
// controller asks for verification first; once it has judged that, it asks
// for qualification and warm-up under the same verification generation, with
// the shapes resolved (the longest context is fixed at verification). The
// agent runs each section once per generation, and reports its results until
// the generation changes.
type Checks struct {
	// VerificationGeneration is the n of the verification record
	// ver-<instance UID>-<n> that these checks belong to.
	VerificationGeneration int64 `json:"verificationGeneration"`
	// Verify asks for the file rehash and the engine's resolved settings.
	Verify *VerifyRequest `json:"verify,omitempty"`
	// Qualify asks for the qualification checks.
	Qualify *QualifyRequest `json:"qualify,omitempty"`
	// WarmUp asks for warm-up rounds. The agent runs round after round, at
	// most MaxWarmUpRounds, reporting each, until a desired state without
	// WarmUp for this generation tells it the controller has decided.
	WarmUp *WarmUpRequest `json:"warmUp,omitempty"`
}

// VerifyRequest asks for verification.
type VerifyRequest struct {
	// Files are the identity record's file list, to rehash: every file
	// present must match. Empty after a worker restart, whose verification
	// compares settings only (HBEP-7, the alpha ladder, rung 1).
	Files []FileDigest `json:"files,omitempty"`
}

// FileDigest is one file of a checkpoint and its SHA-256, as the identity
// record lists it: the path relative to the checkpoint root, with "/".
type FileDigest struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

// Shapes are a workload's resolved request shapes, in tokens.
type Shapes struct {
	ContextShort   int32 `json:"contextShort"`
	ContextTypical int32 `json:"contextTypical"`
	ContextLongest int32 `json:"contextLongest"`
	OutputTypical  int32 `json:"outputTypical"`
	OutputLongest  int32 `json:"outputLongest"`
}

// QualifyRequest asks for the qualification checks (HBEP-7, Instance start).
type QualifyRequest struct {
	Shapes Shapes `json:"shapes"`
	// ToolCalls asks for the tool-call smoke test, and a well-formed tool call
	// in the behaviour check.
	ToolCalls bool `json:"toolCalls,omitempty"`
}

// WarmUpRequest asks for warm-up rounds. Each round sends one request at each
// of the three context lengths with the typical output, one at the typical
// context with the longest output, one tool call when ToolCalls is set, then
// TargetSessions concurrent requests of the typical shape. Synthetic prompts
// only.
type WarmUpRequest struct {
	Shapes         Shapes `json:"shapes"`
	ToolCalls      bool   `json:"toolCalls,omitempty"`
	TargetSessions int32  `json:"targetSessions"`
}

// ---- reports ----

// Report is the agent's full snapshot of what it runs, sent at least every
// ReportInterval and at every change (HBEP-7, Reports). It is never a delta.
type Report struct {
	// Time is when the agent took the snapshot, by its own clock.
	Time time.Time `json:"time"`
	// Instance is the Instance the agent runs or last ran, absent when it has
	// run none since it started.
	Instance *InstanceReport `json:"instance,omitempty"`
}

// InstanceReport is what the agent knows of one Instance.
type InstanceReport struct {
	// UID is the Instance's UID, from DesiredInstance.UID.
	UID string `json:"uid"`
	// Generation is the whole-Instance restart generation the agent last
	// applied.
	Generation int64 `json:"generation"`
	// Staging is the progress of the image and checkpoint.
	Staging *Staging `json:"staging,omitempty"`
	// Workers report each engine process. Alpha: one, worker-0.
	Workers []WorkerReport `json:"workers,omitempty"`
	// ServingEndpoint is where the agent serves the Instance's serving path,
	// forwarded straight to worker-0's EngineSocket in alpha (HBEP-7, What it
	// runs). Its form is HBEP-6's; empty until the agent serves it.
	ServingEndpoint string `json:"servingEndpoint,omitempty"`
	// Key is the Instance's key, which the agent generated at Instance start,
	// sent until DesiredInstance.KeyStored is true. The controller keeps it in
	// a mode-600 file (InstanceKeyPath) and never in an object or a log.
	Key string `json:"key,omitempty"`
	// Checks are the results of the latest Checks, for its verification
	// generation.
	Checks *CheckResults `json:"checks,omitempty"`
}

// StagingPhase is the state of staging.
type StagingPhase string

// Staging phases.
const (
	StagingInProgress StagingPhase = "inProgress"
	StagingDone       StagingPhase = "done"
	StagingFailed     StagingPhase = "failed"
)

// Staging is the progress of pulling the image and downloading the
// checkpoint.
type Staging struct {
	Phase      StagingPhase `json:"phase"`
	BytesDone  int64        `json:"bytesDone,omitempty"`
	BytesTotal int64        `json:"bytesTotal,omitempty"`
	Message    string       `json:"message,omitempty"`
}

// PartPhase is the state of one process the agent runs.
type PartPhase string

// Part phases.
const (
	PartStarting PartPhase = "starting"
	PartRunning  PartPhase = "running"
	// PartStopping: the agent is stopping the part on instruction.
	PartStopping PartPhase = "stopping"
	// PartStopped: the agent stopped the part on instruction (a raised
	// generation, idle or stop).
	PartStopped PartPhase = "stopped"
	// PartExited: the part exited without being told to. That is a failure
	// whatever its exit code; a clean exit too (HBEP-7, Health and restarts;
	// L-2.9).
	PartExited PartPhase = "exited"
)

// WorkerReport is one engine process.
type WorkerReport struct {
	// Name is the worker's name, as in DesiredWorker.
	Name  string    `json:"name"`
	Phase PartPhase `json:"phase"`
	// PID and StartTime identify the process; absent before it starts.
	PID       int        `json:"pid,omitempty"`
	StartTime *time.Time `json:"startTime,omitempty"`
	// ExitCode is set once the process has exited, by itself or when stopped;
	// -1 for a process killed by a signal.
	ExitCode *int `json:"exitCode,omitempty"`
	// Generation is the worker's restart generation this process was started
	// for.
	Generation int64 `json:"generation"`
	// EngineSocket is the engine's Unix socket as a path on the compute node.
	EngineSocket string `json:"engineSocket,omitempty"`
	// Health is the latest sample of this worker's health.
	Health *Health `json:"health,omitempty"`
}

// Health is one sample of an engine's health: the local probe every
// ProbeInterval, and the engine's metrics (HBEP-7, Reports). The counters are
// cumulative since the process started, so the controller computes rates and
// stalls from successive samples.
type Health struct {
	// Time is when the sample was taken.
	Time time.Time `json:"time"`
	// ProbeOK is the latest probe's result; ConsecutiveProbeFailures counts
	// failed probes in a row up to it.
	ProbeOK                  bool  `json:"probeOK"`
	ConsecutiveProbeFailures int32 `json:"consecutiveProbeFailures,omitempty"`
	// RunningRequests and WaitingRequests are the engine's gauges.
	RunningRequests int64 `json:"runningRequests"`
	WaitingRequests int64 `json:"waitingRequests"`
	// GeneratedTokens is the engine's generated-token counter.
	GeneratedTokens int64 `json:"generatedTokens"`
	// KVCacheUsage is the KV cache's use, from 0 to 1.
	KVCacheUsage float64 `json:"kvCacheUsage"`
	// ResponsesByStatus counts the engine's responses by HTTP status code,
	// for example {"200": 1200, "500": 3}.
	ResponsesByStatus map[string]int64 `json:"responsesByStatus,omitempty"`
}

// CheckResults are the results of one verification generation's checks. A
// section is absent until it has started.
type CheckResults struct {
	VerificationGeneration int64          `json:"verificationGeneration"`
	Verify                 *VerifyResult  `json:"verify,omitempty"`
	Qualify                *QualifyResult `json:"qualify,omitempty"`
	WarmUp                 *WarmUpResult  `json:"warmUp,omitempty"`
}

// FileOutcome is the outcome of rehashing one file.
type FileOutcome string

// File outcomes. A listed file that was never staged, because the engine does
// not load it, is FileAbsent, which is not a failure (HBEP-7, Instance start).
const (
	FileMatch    FileOutcome = "match"
	FileMismatch FileOutcome = "mismatch"
	FileAbsent   FileOutcome = "absent"
)

// FileResult is one file's rehash.
type FileResult struct {
	Path    string      `json:"path"`
	Outcome FileOutcome `json:"outcome"`
	// SHA256 is the hash the agent computed, for a mismatch.
	SHA256 string `json:"sha256,omitempty"`
}

// VerifyResult is what verification reads (HBEP-7, Instance start; HBEP-9).
// The controller judges it. An engine that reports nothing fails, because the
// engine version, the image digest and max-model-len are required.
type VerifyResult struct {
	// Done is true once the agent has everything it will report.
	Done bool `json:"done"`
	// Files are the rehash results, one per requested file.
	Files []FileResult `json:"files,omitempty"`
	// EngineVersion is the version the engine reports.
	EngineVersion string `json:"engineVersion,omitempty"`
	// ImageDigest is the digest of the image the agent ran.
	ImageDigest string `json:"imageDigest,omitempty"`
	// Settings are the engine's resolved settings by flag name, values as
	// text, for example {"max-model-len": "32768"}.
	Settings map[string]string `json:"settings,omitempty"`
	// Fingerprint is the engine's fingerprint, recorded, never compared.
	Fingerprint string `json:"fingerprint,omitempty"`
	// TemplateOverhead is the chat template's overhead in tokens: an empty
	// conversation (one user message with no content, plus the generation
	// prompt) rendered through the engine's template. The controller fixes
	// the longest context with it (HBEP-7, What the controller reads).
	TemplateOverhead int32 `json:"templateOverhead,omitempty"`
	// Error says why verification could not run, when it could not.
	Error string `json:"error,omitempty"`
}

// CheckName names a check the agent judges.
type CheckName string

// The checks the agent judges itself (doc.go, Who decides).
const (
	CheckHealth     CheckName = "health"
	CheckChat       CheckName = "chat"
	CheckToolCall   CheckName = "toolCall"
	CheckCorruption CheckName = "corruption"
	CheckBehaviour  CheckName = "behaviour"
)

// CheckOutcome is one check's outcome.
type CheckOutcome struct {
	Name    CheckName `json:"name"`
	Passed  bool      `json:"passed"`
	Message string    `json:"message,omitempty"`
}

// QualifyResult is what qualification read and judged (HBEP-7, Instance
// start). The controller judges the values.
type QualifyResult struct {
	Done bool `json:"done"`
	// Checks are the outcomes of the checks the agent judges.
	Checks []CheckOutcome `json:"checks,omitempty"`
	// KVCapacityTokens is the KV capacity the engine reported at start-up.
	KVCapacityTokens int64 `json:"kvCapacityTokens,omitempty"`
	// ServedModelNames are the names the engine serves.
	ServedModelNames []string `json:"servedModelNames,omitempty"`
	// Fingerprint is the chat request's engine fingerprint.
	Fingerprint string `json:"fingerprint,omitempty"`
}

// Shape names a warm-up request's shape.
type Shape string

// Warm-up shapes (WarmUpRequest).
const (
	ShapeShort      Shape = "short"
	ShapeTypical    Shape = "typical"
	ShapeLongest    Shape = "longest"
	ShapeLongOutput Shape = "longOutput"
	ShapeToolCall   Shape = "toolCall"
	ShapeConcurrent Shape = "concurrent"
)

// WarmUpResult holds every round run so far for one verification generation.
type WarmUpResult struct {
	Rounds []WarmUpRound `json:"rounds,omitempty"`
	// Done is true once the agent stopped: it ran MaxWarmUpRounds, or the
	// desired state no longer asks for warm-up.
	Done bool `json:"done"`
}

// WarmUpRound is one round's requests.
type WarmUpRound struct {
	// Round counts from 1.
	Round    int32           `json:"round"`
	Requests []RequestTiming `json:"requests"`
}

// RequestTiming is one warm-up request's timing. The controller derives the
// time per output token as (Total - TimeToFirstToken) / (OutputTokens - 1).
type RequestTiming struct {
	Shape            Shape    `json:"shape"`
	TimeToFirstToken Duration `json:"timeToFirstToken"`
	Total            Duration `json:"total"`
	OutputTokens     int32    `json:"outputTokens"`
	// Error says why the request failed, when it did.
	Error string `json:"error,omitempty"`
}
