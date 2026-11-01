package agentapi

import (
	"encoding/json"
	"fmt"
	"strings"
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

	// HubToken is the token for a gated checkpoint. The agent keeps it in
	// memory for its in-process download; it never reaches a file outside the
	// job, a command line or a log (HBEP-7, Keys, as the science agent ruled
	// on 31 Oct 2026).
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
	// WarmUp for this generation tells it the controller has decided; it then
	// cancels any warm-up request still in flight and reports WarmUp.Done. The
	// controller marks the Instance ready only after that report (doc.go,
	// Warm-up gates ready).
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
	Shapes    Shapes `json:"shapes"`
	ToolCalls bool   `json:"toolCalls,omitempty"`
	// TargetSessions is the number of concurrent requests in a round, at most
	// MaxTargetSessions (the CRD's maximum for Workload.TargetSessions). The
	// bound keeps the report that holds every round of every request inside
	// WarmUpBudgetBytes.
	TargetSessions int32 `json:"targetSessions"`
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
	// Key is the Instance's key, which the agent generated at Instance start
	// (NewKey), sent until DesiredInstance.KeyStored is true. It has CheckKey's
	// form: 64 lowercase hex characters. The controller checks it, keeps it in
	// a mode-600 file (InstanceKeyPath, WriteKeyFile) and never in an object or
	// a log; Report.Redacted blanks it for a log line.
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
	// Message is free text, cut to MaxMessageBytes (TruncateUTF8). While an
	// image is built it reads "building image <image>@<digest> in the job's
	// temporary directory ($TMPDIR)", naming the directory by its variable,
	// not its path, which can hold the job's ID. The agent sends it before
	// the build starts, and the build waits for its delivery, so the last
	// report before a build that runs out of the job's memory (the job then
	// ends OUT_OF_MEMORY, AllocationFailed) names the build (HBEP-4; PR #138
	// review). It is cleared when the build ends.
	Message string `json:"message,omitempty"`
	// BuildPeakTmpBytes is the peak use, in bytes, of the job's temporary
	// directory while the agent built the Instance's image (a pull to a SIF
	// or a sandbox build, on a cache miss). It is reported as soon as the
	// build ends, written or failed, and every later staging report of the
	// Instance carries it, a failed one or a restage that finds the image
	// cached too (PR #138 review). Absent when no build has run for the
	// Instance, or when none of the build's samples succeeded. The
	// agent samples it with statfs, (blocks - free blocks) * block size,
	// every 250 ms and once more when the build ends, so a shorter spike can
	// be missed. On a per-job tmpfs such as Helios's $MEMFS it counts the
	// job's bytes alone; on a shared TMPDIR such as Lustre it counts other
	// users' too and is only an upper bound. It measures the RAM-disk budget
	// a partition's memPerGpu includes (HBEP-4, Platform profile schema;
	// HBEP-7, The allocation agent and Test plan).
	BuildPeakTmpBytes int64 `json:"buildPeakTmpBytes,omitempty"`
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
	// LogTail is the end of the engine's output, at most MaxLogTailBytes and
	// cut at a UTF-8 boundary, once the worker has exited without being told
	// to or could not start; absent otherwise. It says why a start failed,
	// for example a cache that ignores its variable and cannot write in a
	// container without the home directory (science-agent ruling, 1 Nov 2026,
	// #127 review of issue #125). The agent replaces in it every Instance key
	// it has drawn in the job, the current one and the earlier ones, whose
	// output the log may still hold, before it cuts the tail, so no part of a
	// key is left at the cut (PR #138 review). Report.Redacted leaves it as
	// it is.
	LogTail string `json:"logTail,omitempty"`
}

// Health is one sample of an engine's health: the local probe every
// ProbeInterval, and the engine's metrics (HBEP-7, Reports). The counters are
// cumulative since the process started, so the controller computes rates and
// stalls from successive samples.
//
// The probe (ProbeOK, ConsecutiveProbeFailures) and the metrics are separate
// reads. The metrics come from the engine's /metrics endpoint: RunningRequests,
// WaitingRequests, GeneratedTokens, KVCacheUsage, Preemptions,
// PrefixCacheQueriedTokens, PrefixCacheHitTokens, TimePerOutputTokenSecondsSum,
// TimePerOutputTokenCount and ResponsesByStatus. A read
// of it can fail while the probe passes, so a sample says which values it
// holds and when they were read, and never fills a value it does not have
// (`@hpc-burst-science-agent`, 31 Oct 2026):
//
//   - An absent value is unknown. The gauges and counters are pointers, so a
//     sample that left one out decodes to nil and never to zero, which would
//     read as an idle engine, an empty KV cache, a stalled counter, no
//     preemption or a prefix cache that never hits. A nil ResponsesByStatus is
//     unknown too; an empty map is a read that found no responses. An agent that
//     could not read /metrics leaves the values out, or repeats the last ones it
//     read, and keeps MetricsReadAt at its last success (nil when it has none);
//     it never fills a value it did not read with zero.
//   - MetricsReadAt is when the last successful /metrics read finished, by the
//     agent's clock; the values in the sample are from that read.
//   - The controller takes no gauge-based action while the metrics are stale,
//     that is while MetricsReadAt is nil or older than MetricsStaleProbeIntervals
//     probe intervals, judged by Health.MetricsFresh at the sample's creation,
//     at its Report's creation and at the controller's own clock since the
//     Report arrived: it treats every value above as unknown. The actions are
//     the ones that read them, such as idle, KV pressure, the only-errors
//     signal and the no-progress signal.
//   - It compares the counters of two samples only when MetricsReadAt advanced
//     between them. Otherwise a run of failed reads looks like a stall.
type Health struct {
	// Time is when the sample was taken.
	Time time.Time `json:"time"`
	// ProbeOK is the latest probe's result; ConsecutiveProbeFailures counts
	// failed probes in a row up to it. A failed /metrics read does not fail the
	// probe.
	ProbeOK                  bool  `json:"probeOK"`
	ConsecutiveProbeFailures int32 `json:"consecutiveProbeFailures,omitempty"`
	// MetricsReadAt is the time of the last successful /metrics read, the one
	// the values below come from; nil when there has been none.
	MetricsReadAt *time.Time `json:"metricsReadAt,omitempty"`
	// RunningRequests and WaitingRequests are the engine's gauges; nil is
	// unknown, not zero.
	RunningRequests *int64 `json:"runningRequests,omitempty"`
	WaitingRequests *int64 `json:"waitingRequests,omitempty"`
	// GeneratedTokens is the engine's generated-token counter; nil is unknown,
	// not zero.
	GeneratedTokens *int64 `json:"generatedTokens,omitempty"`
	// KVCacheUsage is the KV cache's use, from 0 to 1; nil is unknown, not
	// zero.
	KVCacheUsage *float64 `json:"kvCacheUsage,omitempty"`
	// Preemptions is the engine's cumulative count of preempted requests
	// (vllm:num_preemptions_total). It is a counter since the process started,
	// like GeneratedTokens, so a restart starts it again at 0. nil is unknown,
	// not zero: a pointer to 0 is an engine that has preempted nothing.
	Preemptions *int64 `json:"preemptions,omitempty"`
	// PrefixCacheQueriedTokens and PrefixCacheHitTokens are the engine's
	// cumulative prefix-cache counters, in tokens (vllm:prefix_cache_queries_total
	// and vllm:prefix_cache_hits_total); the hit rate is hits over queries. vLLM
	// registers both whether or not prefix caching is on, and exports 0 when it
	// is off, so a pointer to 0 is a real zero. nil is an engine that did not
	// export the line, which is unknown, never 0.
	PrefixCacheQueriedTokens *int64 `json:"prefixCacheQueriedTokens,omitempty"`
	PrefixCacheHitTokens     *int64 `json:"prefixCacheHitTokens,omitempty"`
	// TimePerOutputTokenSecondsSum and TimePerOutputTokenCount are the _sum and
	// _count of the engine's per-request time-per-output-token histogram
	// (vllm:request_time_per_output_token_seconds): the sum, in seconds, of the
	// mean time per output token of every finished request, and the number of
	// requests observed. Both are cumulative since the process started, and the
	// controller reads the mean over a period as the difference of the sums over
	// the difference of the counts. nil is unknown, not zero; the agent carries
	// no buckets or quantiles.
	TimePerOutputTokenSecondsSum *float64 `json:"timePerOutputTokenSecondsSum,omitempty"`
	TimePerOutputTokenCount      *int64   `json:"timePerOutputTokenCount,omitempty"`
	// ResponsesByStatus counts the engine's responses by the status label the
	// engine reports, verbatim. vLLM groups statuses into classes (its
	// prometheus-fastapi-instrumentator has should_group_status_codes on), so
	// the keys are classes such as "2xx", "3xx", "4xx" and "5xx", for example
	// {"2xx": 1200, "5xx": 3};
	// an engine that reports exact codes gives {"200": 1200, "500": 3}. The
	// agent cannot turn classes into codes. Read server errors with
	// ServerErrors, which counts both forms. nil is unknown, and encodes as
	// null; an empty map is a read that found no responses, and encodes as {}.
	// The tag has no omitempty, which would drop the empty map and turn a known
	// empty count into an unknown one on the wire.
	ResponsesByStatus map[string]int64 `json:"responsesByStatus"`
}

// ServerErrors is the number of server-error responses in a
// ResponsesByStatus map: the sum of every key that starts with "5", whether it
// is a class ("5xx") or an exact code ("500", "503").
func ServerErrors(byStatus map[string]int64) int64 {
	var n int64
	for k, v := range byStatus {
		if strings.HasPrefix(k, "5") {
			n += v
		}
	}
	return n
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

// The values of VerifyResult.GenerationConfigFile.
const (
	GenerationConfigAbsent     = "absent"
	GenerationConfigPresent    = "present"
	GenerationConfigUnreadable = "unreadable"
)

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
	// GenerationConfig is the model's generation defaults: the sampling
	// parameters that the staged checkpoint's generation_config.json sets of
	// those vLLM applies as the server's defaults (repetition_penalty,
	// presence_penalty, frequency_penalty, temperature, top_k, top_p, min_p
	// and max_new_tokens: vLLM v0.32.0, vllm/config/model.py lines
	// 1869-1878), each as its JSON number text, for example
	// {"repetition_penalty": "1.1", "temperature": "0.7"}. No other key of the
	// file is carried (PR #138 review; HBEP-7, The verification record). vLLM
	// takes a request's sampling defaults from them; the agent's own requests
	// set every sampling parameter they rely on, so they change no check. The
	// controller records them in the verification record (science-agent
	// rulings, 1 Nov 2026, issue #125). A value is carried only when it is a
	// JSON number, an integer for top_k and max_new_tokens, of at most
	// MaxGenerationConfigValueBytes; there are at most MaxGenerationConfigKeys
	// keys of at most MaxGenerationConfigKeyBytes each. Absent when the
	// checkpoint has no such file, it is not a JSON object, or it sets none
	// of them.
	GenerationConfig map[string]string `json:"generationConfig,omitempty"`
	// GenerationConfigFile says what the agent found at the staged
	// checkpoint's generation_config.json, so that a record can tell "the
	// checkpoint has no such file" from "the file holds none of the keys":
	// GenerationConfigAbsent (there is no such file; vLLM then builds its
	// defaults from the model's config.json, which the checks do not read),
	// GenerationConfigPresent (a file is there, and GenerationConfig holds
	// the keys it sets of those vLLM applies, none when it sets none), or
	// GenerationConfigUnreadable (something is at that path that the agent
	// could not read as a file: a directory, a permission error). Empty when
	// the agent did not look because it had no staged checkpoint directory.
	// The controller copies it into the verification record (HBEP-7, The
	// verification record: "the record says the file is absent";
	// `@hpc-burst-science-agent`, 1 Nov 2026, issue #175).
	GenerationConfigFile string `json:"generationConfigFile,omitempty"`
	// DoSample is the staged generation_config.json's do_sample, when the
	// file sets it as a JSON boolean; nil when the file is absent, is not a
	// JSON object, or does not set do_sample as a boolean. On a reasoning
	// model, false makes the checks greedy (CheckSampling; HBEP-7, The
	// allocation agent, Checks; `@hpc-burst-science-agent`, 1 Nov 2026).
	DoSample *bool `json:"doSample,omitempty"`
	// GenerationConfigRefused is the first sampling value of the staged
	// generation_config.json that CheckGenerationConfig refuses, checked on
	// the file's own JSON text, for every model; nil when it refuses none or
	// there is no file. When it is set the agent sends no check request for
	// this verification generation (not the fingerprint, not qualification
	// or warm-up), and the controller fails verification with
	// VerificationFailed, the settings contradiction's stop, keyed on the
	// deployment ID. It is how a value that is not a number reaches the
	// controller, since GenerationConfig carries numbers only
	// (`@hpc-burst-science-agent`, 1 Nov 2026, from the reviews of PRs #155
	// and #156).
	GenerationConfigRefused *GenerationConfigError `json:"generationConfigRefused,omitempty"`
	// Error says why verification could not run, when it could not. It is
	// free text, cut to MaxMessageBytes (TruncateUTF8).
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
	Name   CheckName `json:"name"`
	Passed bool      `json:"passed"`
	// Reason is a CamelCase code for why the check failed, one of the Reason*
	// constants of this package (checks.go), and empty on a pass. The
	// controller matches on it and never on Message, which is for people
	// (`@hpc-burst-science-agent`, 31 Oct 2026). A failure that no constant
	// names has an empty Reason and a Message only.
	Reason string `json:"reason,omitempty"`
	// Message is free text, cut to MaxMessageBytes (TruncateUTF8).
	Message string `json:"message,omitempty"`
	// ContextTokens is the length of the prompt, in tokens, that the check
	// actually ran at. Only the behaviour check sets it, and the controller
	// records it as the context the check was judged at. It is the
	// workload's contextTokens.longest, except when the engine declares a
	// reasoning parser, where the agent shortens the prompt to leave room
	// for the reasoning (BehaviourContextTokens). 0 for the other checks.
	ContextTokens int64 `json:"contextTokens,omitempty"`
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

// FixedLength reports whether the agent sends the shape at a fixed output
// length, min_tokens equal to max_tokens with the end-of-sequence token
// ignored, so its output length is the same in every round. Settling judges
// only these shapes (science-agent ruling, 1 Nov 2026, issue #125): the
// sequential shapes and the concurrent one. ShapeToolCall is not one: it ends
// with its call, and on a reasoning model after reasoning of any length. It
// still runs in every round to warm its path, but is not judged. A shape this
// package does not name is not fixed-length either.
func (s Shape) FixedLength() bool {
	switch s {
	case ShapeShort, ShapeTypical, ShapeLongest, ShapeLongOutput, ShapeConcurrent:
		return true
	}
	return false
}

// WarmUpResult holds every round run so far for one verification generation.
type WarmUpResult struct {
	Rounds []WarmUpRound `json:"rounds,omitempty"`
	// Done is true once the agent stopped: it ran MaxWarmUpRounds, or the
	// desired state no longer asks for warm-up.
	Done bool `json:"done"`
}

// WarmUpRound is one round's requests. The controller reads them as
// RequestTiming says: a request with an Error counts for nothing. Settling
// judges only the fixed-length shapes (Shape.FixedLength). It compares, per
// such Shape, the mean Total of the round's requests with the round before; a
// Shape whose mean Total rose by more than max(10% of the round before, 100
// ms), or that has no successful request in this round, means not settled
// (science-agent ruling, 1 Nov 2026, issue #125). A Shape whose length varies
// runs in every round, but its timings are not judged. A request of any Shape
// with an Error fails warm-up; the agent sets one only for a request that
// failed (an HTTP error, a cut stream), never for the tool call's answer,
// which qualification's tool-call smoke test judges.
// The Instance's throughput is the mean time per output token
// (RequestTiming.TimePerOutputToken) and the mean time to first token over the
// requests of the last round's ShapeConcurrent that have them; it is not
// recorded when none has.
type WarmUpRound struct {
	// Round counts from 1.
	Round    int32           `json:"round"`
	Requests []RequestTiming `json:"requests"`
}

// RequestTiming is one warm-up request's timing. What it counts for depends on
// the request:
//
//   - a request with an Error counts for nothing, whatever else it holds;
//   - TimeToFirstToken counts for a request with OutputTokens of 1 or more;
//   - the time per output token, (Total - TimeToFirstToken) / (OutputTokens - 1),
//     counts only for a request with OutputTokens of 2 or more
//     (TimePerOutputToken); a one-token request has no token interval, and the
//     formula would divide by zero;
//   - Total counts for settling for every request of a fixed-length shape
//     without an Error (see WarmUpRound).
//
// The workload schema admits an output of one token, so a request with fewer
// than two output tokens is normal, not a fault.
type RequestTiming struct {
	Shape            Shape    `json:"shape"`
	TimeToFirstToken Duration `json:"timeToFirstToken"`
	Total            Duration `json:"total"`
	OutputTokens     int32    `json:"outputTokens"`
	// Error says why the request failed, when it did. It is free text, cut to
	// MaxTimingErrorBytes (TruncateUTF8), because a round can hold more than a
	// thousand timings.
	Error string `json:"error,omitempty"`
}
