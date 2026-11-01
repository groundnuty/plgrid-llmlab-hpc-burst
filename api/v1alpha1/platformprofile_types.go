package v1alpha1

import (
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PlatformKind is the kind of platform (HBEP-3, Vocabulary, Provider).
//
// +kubebuilder:validation:Enum=managed;batch;static
type PlatformKind string

// Platform kinds.
const (
	PlatformManaged PlatformKind = "managed"
	PlatformBatch   PlatformKind = "batch"
	PlatformStatic  PlatformKind = "static"
)

// PartitionProfile is what the framework reads of one partition: its name and
// its accelerator class (HBEP-11, API, PlatformProfile schema).
//
// +kubebuilder:validation:XValidation:rule="!has(self.tmpDirFeature) || has(self.tmpDir)",message="tmpDirFeature is the node feature that tmpDir needs, so it requires tmpDir"
type PartitionProfile struct {
	// Name of the partition.
	//
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`

	// AcceleratorClass is the partition's GPU model and CPU architecture. They
	// are a deployment-ID input (HBEP-3). HBEP-4 fixes the strings: the GPU
	// product name without vendor or memory size, upper case (GH200, A100),
	// and the kernel's machine name (aarch64, x86_64).
	AcceleratorClass `json:",inline"`

	// MaxTime is the partition's longest time limit (HBEP-4, Platform profile
	// schema, the fields the lifecycle controller reads). An Allocation's time
	// limit is the smallest of it, the maxWall of the QOS limits that apply
	// and the ModelDeployment's leaseLimit (AllocationTimeLimit; HBEP-4, QOS
	// limits).
	//
	// +optional
	// +kubebuilder:validation:XValidation:rule="duration(self) > duration('0s')",message="maxTime must be a positive duration that fits Go's time.Duration (above 0s, at most 2562047h)"
	MaxTime *Duration `json:"maxTime,omitempty"`

	// GPUsPerNode is the GPUs on one node, the hardware fact. The most GPUs
	// one Allocation may request here is the smaller of it and the
	// maxGpusPerUser of the QOS limits that apply, when set; a larger count is
	// refused, never capped (HBEP-4, Submission, GPU sizing and QOS limits).
	//
	// +optional
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=1024
	GPUsPerNode *int32 `json:"gpusPerNode,omitempty"`

	// CPUsPerGPU is the CPUs an Allocation requests per GPU.
	//
	// +optional
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=4096
	CPUsPerGPU *int32 `json:"cpusPerGpu,omitempty"`

	// MemPerGPU is the memory an Allocation requests per GPU.
	//
	// +optional
	MemPerGPU *resource.Quantity `json:"memPerGpu,omitempty"`

	// ImageFormat is the container image format Apptainer runs on the
	// partition's compute nodes: sif, a single image file, or sandbox, an
	// unpacked directory. The allocation agent builds and runs the engine
	// image in this format, which the lifecycle controller sends it in the
	// desired state (agentapi.AgentSettings.ImageFormat). It is not a
	// deployment-ID input: both formats run the same image digest. Spike S1's
	// probe 2 measured it: on Helios plgrid-gpu-gh200 a SIF runs (sif); on
	// Athena plgrid-gpu-a100 Apptainer cannot run a SIF on the compute nodes
	// and a sandbox directory runs (sandbox). Absent, the platform has not
	// stated it, and the agent is sent sif, Apptainer's own format
	// (ImageFormatOf).
	//
	// +optional
	ImageFormat ImageFormat `json:"imageFormat,omitempty"`

	// TmpDir is the job's temporary directory on this partition: the batch
	// script sets TMPDIR to it before it starts the allocation agent, which
	// keeps its run directory (the engine's socket) and Apptainer's build tree
	// there. It has stagingDir's form, an absolute path or one that starts
	// with an environment variable, but the variable is one the job sets, such
	// as $MEMFS, the RAM disk of Helios's memfs feature, so it is expanded
	// inside the job, never on the login node. A variable that is unset or
	// empty there stops the job before the agent starts, and the job ends
	// AllocationFailed. Absent, the job keeps the TMPDIR the scheduler gives
	// it (HBEP-4, Submission and Platform profile schema; HBEP-7, The
	// allocation agent: on Helios plgrid-gpu-gh200 that TMPDIR is on Lustre,
	// near its inode quota).
	//
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=1024
	// +kubebuilder:validation:Pattern=`^(/|\$[A-Za-z_][A-Za-z0-9_]*)[A-Za-z0-9._:=,/-]*$`
	// +kubebuilder:validation:XValidation:rule="!self.split('/').exists(s, s == '..')",message="tmpDir must not contain a '..' segment"
	TmpDir string `json:"tmpDir,omitempty"`

	// TmpDirFeature is the Slurm node feature that TmpDir needs, such as
	// memfs, the feature that gives a Helios job its RAM disk in $MEMFS. The
	// Slurm provider then submits with --constraint=<feature>, so the job
	// gets the variable TmpDir names. One feature name, never a constraint
	// expression. Absent, no constraint is added (HBEP-4, The Slurm dialect
	// and Platform profile schema). It requires TmpDir.
	//
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=64
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9][A-Za-z0-9_.-]*$`
	TmpDirFeature string `json:"tmpDirFeature,omitempty"`

	// QOS is the partition's own QOS, which Slurm applies to every job on the
	// partition: for each limit, its value comes before the job QOS's unless
	// the job QOS has overPartQos (EffectiveQOSLimits). It must be listed in
	// the profile's qos. Helios plgrid-gpu-gh200's is plgrid, whose 2,000
	// submitted jobs per user apply before QOS now's 1 (HBEP-4, QOS limits
	// and Platform profile schema; `@hpc-burst-science-agent`, 1 Nov 2026).
	// Absent, the partition has none.
	//
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=64
	QOS string `json:"qos,omitempty"`
}

// QOSProfile is one Slurm QOS that a Provider's qos or a partition's qos names,
// with the limits the plan reads (HBEP-4, QOS limits and Platform profile
// schema; `@hpc-burst-science-agent`, 1 Nov 2026). Every limit is optional,
// and an absent one is no limit as far as the plan knows. A QOS with no limits
// is listed with none, so every QOS named is known.
type QOSProfile struct {
	// Name of the QOS, as sacctmgr shows it.
	//
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=64
	Name string `json:"name"`

	// MaxWall is the QOS's wall limit per job (MaxWall). It never lifts the
	// partition's maxTime: an Allocation's time limit is the smaller.
	//
	// +optional
	// +kubebuilder:validation:XValidation:rule="duration(self) > duration('0s')",message="maxWall must be a positive duration that fits Go's time.Duration (above 0s, at most 2562047h)"
	MaxWall *Duration `json:"maxWall,omitempty"`

	// MaxSubmitPerUser is the most jobs one user may have pending or running
	// at once under the QOS (MaxSubmitJobsPerUser). A held job is a pending
	// job, and every job of the login counts, the owner's other projects'
	// included. The lifecycle controller plans within it (HBEP-7,
	// Provisioning, Per-user limits).
	//
	// +optional
	// +kubebuilder:validation:Minimum=0
	MaxSubmitPerUser *int32 `json:"maxSubmitPerUser,omitempty"`

	// MaxGPUsPerUser is the GPU entry of the QOS's MaxTRESPerUser: the most
	// GPUs over one user's running jobs. A job that fits alone but not beside
	// the user's others pends.
	//
	// +optional
	// +kubebuilder:validation:Minimum=0
	MaxGPUsPerUser *int32 `json:"maxGpusPerUser,omitempty"`

	// MaxCPUsPerUser is the CPU entry of the QOS's MaxTRESPerUser. An
	// Allocation's CPUs, gpus times cpusPerGpu, are capped at it.
	//
	// +optional
	// +kubebuilder:validation:Minimum=0
	MaxCPUsPerUser *int32 `json:"maxCpusPerUser,omitempty"`

	// MaxMemPerUser is the memory entry of the QOS's MaxTRESPerUser. An
	// Allocation's memory, gpus times memPerGpu, is capped at it. It is never
	// negative.
	//
	// +optional
	// +kubebuilder:validation:XValidation:rule="!string(self).startsWith('-')",message="maxMemPerUser must not be negative"
	MaxMemPerUser *resource.Quantity `json:"maxMemPerUser,omitempty"`

	// OverPartQOS is whether the QOS carries Slurm's OverPartQOS flag, which
	// puts its limits before the partition QOS's (EffectiveQOSLimits). No QOS
	// the alpha profiles type has it.
	//
	// +optional
	// +kubebuilder:default=false
	OverPartQOS bool `json:"overPartQos"`
}

// ImageFormat is a container image format Apptainer runs (PartitionProfile).
//
// +kubebuilder:validation:Enum=sif;sandbox
type ImageFormat string

// Image formats.
const (
	// ImageFormatSIF is a single SIF file, Apptainer's own format.
	ImageFormatSIF ImageFormat = "sif"
	// ImageFormatSandbox is an unpacked directory image (apptainer build
	// --sandbox).
	ImageFormatSandbox ImageFormat = "sandbox"
)

// ImageFormatOf returns the format the agent is sent for a partition: its
// imageFormat, or ImageFormatSIF when the profile states none.
func ImageFormatOf(p PartitionProfile) ImageFormat {
	if p.ImageFormat == "" {
		return ImageFormatSIF
	}
	return p.ImageFormat
}

// HealthThresholds are HBEP-7's malfunction signals and start deadlines
// (HBEP-4, Platform profile schema). Defaults are HBEP-4's, tuned in beta.
type HealthThresholds struct {
	// ProbeInterval is how often the agent probes the engine's health.
	//
	// +optional
	// +kubebuilder:default="10s"
	// +kubebuilder:validation:XValidation:rule="duration(self) > duration('0s')",message="probeInterval must be a positive duration that fits Go's time.Duration (above 0s, at most 2562047h)"
	ProbeInterval *Duration `json:"probeInterval,omitempty"`

	// FailureThreshold is how many failed probes in a row are a malfunction.
	//
	// +optional
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=1
	FailureThreshold *int32 `json:"failureThreshold,omitempty"`

	// NoProgressTimeout is how long requests may run with the generated-token
	// counter unchanged. A request in prefill generates no token, so in alpha
	// it is 30 min, equal to the front's idleTimeout, and a slow prefill that
	// the front allows is never restarted (HBEP-4, Platform profile schema;
	// HBEP-7, open question 6; HBEP-5).
	//
	// +optional
	// +kubebuilder:default="30m"
	// +kubebuilder:validation:XValidation:rule="duration(self) > duration('0s')",message="noProgressTimeout must be a positive duration that fits Go's time.Duration (above 0s, at most 2562047h)"
	NoProgressTimeout *Duration `json:"noProgressTimeout,omitempty"`

	// ErrorOnlyWindow is how long every answer may be a server error.
	//
	// +optional
	// +kubebuilder:default="1m"
	// +kubebuilder:validation:XValidation:rule="duration(self) > duration('0s')",message="errorOnlyWindow must be a positive duration that fits Go's time.Duration (above 0s, at most 2562047h)"
	ErrorOnlyWindow *Duration `json:"errorOnlyWindow,omitempty"`

	// KVSkewTimeout is how long one worker may sit at a full KV cache with
	// requests waiting while another idles. It needs several workers, so alpha
	// does not use it.
	//
	// +optional
	// +kubebuilder:default="2m"
	// +kubebuilder:validation:XValidation:rule="duration(self) > duration('0s')",message="kvSkewTimeout must be a positive duration that fits Go's time.Duration (above 0s, at most 2562047h)"
	KVSkewTimeout *Duration `json:"kvSkewTimeout,omitempty"`

	// AgentTimeout is how long an agent may send no report before its
	// Instance is AgentUnreachable.
	//
	// +optional
	// +kubebuilder:default="2m"
	// +kubebuilder:validation:XValidation:rule="duration(self) > duration('0s')",message="agentTimeout must be a positive duration that fits Go's time.Duration (above 0s, at most 2562047h)"
	AgentTimeout *Duration `json:"agentTimeout,omitempty"`

	// RegistrationTimeout is the longest time from the job's start to its
	// agent's registration.
	//
	// +optional
	// +kubebuilder:default="15m"
	// +kubebuilder:validation:XValidation:rule="duration(self) > duration('0s')",message="registrationTimeout must be a positive duration that fits Go's time.Duration (above 0s, at most 2562047h)"
	RegistrationTimeout *Duration `json:"registrationTimeout,omitempty"`

	// StartTimeout is the longest time from an Instance's own start
	// instruction to ready.
	//
	// +optional
	// +kubebuilder:default="1h"
	// +kubebuilder:validation:XValidation:rule="duration(self) > duration('0s')",message="startTimeout must be a positive duration that fits Go's time.Duration (above 0s, at most 2562047h)"
	StartTimeout *Duration `json:"startTimeout,omitempty"`
}

// RestartPolicy is HBEP-7's restart ladder (HBEP-4, Platform profile schema).
type RestartPolicy struct {
	// StopGrace is how long the agent waits after a polite stop before it
	// kills a part.
	//
	// +optional
	// +kubebuilder:default="10s"
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('0s')",message="stopGrace must be a duration that fits Go's time.Duration (at most 2562047h)"
	StopGrace *Duration `json:"stopGrace,omitempty"`

	// AttemptsPerRung is how many times each rung is tried within Window.
	//
	// +optional
	// +kubebuilder:default=2
	// +kubebuilder:validation:Minimum=1
	AttemptsPerRung *int32 `json:"attemptsPerRung,omitempty"`

	// Window is the time over which attempts are counted.
	//
	// +optional
	// +kubebuilder:default="30m"
	// +kubebuilder:validation:XValidation:rule="duration(self) > duration('0s')",message="window must be a positive duration that fits Go's time.Duration (above 0s, at most 2562047h)"
	Window *Duration `json:"window,omitempty"`

	// Backoff is the time between two attempts.
	//
	// +optional
	// +kubebuilder:default="30s"
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('0s')",message="backoff must be a duration that fits Go's time.Duration (at most 2562047h)"
	Backoff *Duration `json:"backoff,omitempty"`
}

// JobTagLength is the length of the job tag (HBEP-3, Allocation and job
// identity): hb-<40 characters>-<12 hex>. A platform whose job-name rules allow
// fewer cannot take the tag, so its profile is refused.
const JobTagLength = 56

// JobNameRules are the platform's rules for the name of a job (HBEP-4). The
// provider submits each job under its tag as the job name, so the rules must
// allow JobTagLength characters.
type JobNameRules struct {
	// MaxLength is the longest job name the platform accepts. Absent when the
	// platform states none.
	//
	// +optional
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=4096
	MaxLength *int32 `json:"maxLength,omitempty"`

	// Pattern is a regular expression (RE2) that a job name matches, when the
	// platform restricts the characters of a name. Absent when it does not.
	//
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=256
	Pattern string `json:"pattern,omitempty"`
}

// PlatformProfileSpec is the platform's kind and its profile data. HBEP-4
// owns the schema. For alpha the fields that the Slurm provider and the
// deployment ID read are typed; the rest of the profile is an untyped block.
// The typed fields carry the names HBEP-11 gives them, in camelCase like every
// field of the API (HBEP-11's hold_release is holdRelease), and the scheduling
// facts of spikes S4 and S5 are typed beside them (HBEP-11, API, PlatformProfile
// schema). A fact the platform does not state, or one
// that does not apply to its kind, is absent: an absent bool is unknown, not
// false. The schema defaults (drainWindow, drainMargin, handoverMargin, health,
// restart) are written on every profile, a managed one included; HBEP-4 gives a
// managed profile no batch fields, so nothing reads them there.
//
// +kubebuilder:validation:XValidation:rule="!has(self.jobName) || !has(self.jobName.maxLength) || self.jobName.maxLength >= 56",message="jobName.maxLength must allow the job tag's 56 characters (HBEP-3, Allocation and job identity)"
// +kubebuilder:validation:XValidation:rule="!has(self.partitions) || self.partitions.all(p, !has(p.qos) || (has(self.qos) && self.qos.exists(q, q.name == p.qos)))",message="every partition's qos must be listed in qos, so the plan knows its limits (HBEP-4, QOS limits)"
type PlatformProfileSpec struct {
	// ProviderKind is managed, batch or static: the kind of provider the
	// platform offers (HBEP-3, Vocabulary, Provider). It is not the object's
	// own kind, which is PlatformProfile.
	ProviderKind PlatformKind `json:"providerKind"`

	// Partitions lists the accelerator class of each partition.
	//
	// +optional
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MaxItems=256
	Partitions []PartitionProfile `json:"partitions,omitempty"`

	// QOS lists the QOSes that a Provider's qos or a partition's qos names,
	// with their limits (HBEP-4, QOS limits and Platform profile schema;
	// `@hpc-burst-science-agent`, 1 Nov 2026). Helios lists now, plgrid and
	// normal; Athena plgrid-now. At most 16: the CEL cost of the rule on
	// maxMemPerUser, a quantity of unbounded length, grows with this bound
	// and exceeds the API server's budget at 32.
	//
	// +optional
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MaxItems=16
	QOS []QOSProfile `json:"qos,omitempty"`

	// HoldRelease says whether a plain user's sbatch --hold is honoured and
	// that user can lift it. It is verified per platform. False runs the
	// fallback mode of spike S4 (HBEP-7, open question 11).
	//
	// +optional
	// +kubebuilder:default=false
	HoldRelease bool `json:"holdRelease"`

	// LostGrace is how long a recorded job may be absent from both the queue
	// listing and accounting before it is reported Lost.
	//
	// +optional
	// +kubebuilder:default="2m"
	// +kubebuilder:validation:XValidation:rule="duration(self) > duration('0s')",message="lostGrace must be a positive duration that fits Go's time.Duration (above 0s, at most 2562047h)"
	LostGrace *Duration `json:"lostGrace,omitempty"`

	// InflightWindow is how long an unresolved submission attempt is waited
	// on. It must exceed the worst remote sbatch time, because a local timeout
	// does not stop a remote sbatch. The platform's measured value is set
	// here; there is no default.
	//
	// +optional
	// +kubebuilder:validation:XValidation:rule="duration(self) > duration('0s')",message="inflightWindow must be a positive duration that fits Go's time.Duration (above 0s, at most 2562047h)"
	InflightWindow *Duration `json:"inflightWindow,omitempty"`

	// UnknownBound is how long, in fallback mode, an Unknown attempt may stay
	// unresolved before an operator is asked.
	//
	// +optional
	// +kubebuilder:default="10m"
	// +kubebuilder:validation:XValidation:rule="duration(self) > duration('0s')",message="unknownBound must be a positive duration that fits Go's time.Duration (above 0s, at most 2562047h)"
	UnknownBound *Duration `json:"unknownBound,omitempty"`

	// TimeZone is the IANA name of the time zone in which the scheduler prints
	// local times, for example "Europe/Warsaw" or "UTC". A plugin that has to
	// parse a text time uses it, and converts every time to UTC; epoch fields
	// need no zone (HBEP-4, local time in scheduler output).
	//
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=64
	// +kubebuilder:validation:Pattern=`^[A-Za-z][A-Za-z0-9_+-]*(/[A-Za-z0-9_+-]+)*$`
	TimeZone string `json:"timeZone,omitempty"`

	// RequeueDefault says whether the scheduler requeues a job by default (for
	// Slurm, JobRequeue). The framework submits with --no-requeue either way;
	// the fact records what a job without it would do (HBEP-4).
	//
	// +optional
	RequeueDefault *bool `json:"requeueDefault,omitempty"`

	// MinJobAge is how long a finished job stays in the queue listing before
	// the scheduler purges it (for Slurm, MinJobAge). A plugin that reads only
	// the queue loses the final state of a job older than this, so it asks
	// accounting (HBEP-4).
	//
	// +optional
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('0s')",message="minJobAge must be a duration that fits Go's time.Duration (at most 2562047h)"
	MinJobAge *Duration `json:"minJobAge,omitempty"`

	// JobName holds the platform's job-name rules.
	//
	// +optional
	JobName *JobNameRules `json:"jobName,omitempty"`

	// JobCommentAccounted says whether accounting keeps the job comment. Where
	// it does not, the job name is the only tag that survives in accounting
	// (spike S4).
	//
	// +optional
	JobCommentAccounted *bool `json:"jobCommentAccounted,omitempty"`

	// JSONDataParser is the version of the scheduler's JSON output without its
	// "data_parser/" prefix: "v0.0.40" or "v0.0.42" for Slurm. Every reply's
	// meta.plugin.data_parser, with the prefix stripped, must equal it, or the
	// reply is not interpreted and the Provider gets DialectMismatch (HBEP-4,
	// The Slurm dialect). A plugin parses only the versions it knows.
	//
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=64
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9][A-Za-z0-9_./-]*$`
	JSONDataParser string `json:"jsonDataParser,omitempty"`

	// StagingDir is where the agent binary, Allocation directories, images
	// and checkpoints live: an absolute path, or one that starts with an
	// environment variable set on login and compute nodes, such as $SCRATCH,
	// which the provider resolves on the login node once per connection
	// (HBEP-4, Transport).
	//
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=1024
	// +kubebuilder:validation:Pattern=`^(/|\$[A-Za-z_][A-Za-z0-9_]*)[A-Za-z0-9._:=,/-]*$`
	// +kubebuilder:validation:XValidation:rule="!self.split('/').exists(s, s == '..')",message="stagingDir must not contain a '..' segment: agent.json refuses one (agentapi.AgentConfig)"
	StagingDir string `json:"stagingDir,omitempty"`

	// DrainWindow is HBEP-5's drain window: an Instance drains from its hard
	// end minus this. It is at least the front's maxRequestDuration (2 h)
	// plus DrainMargin, so its default is 2 h 10 min (owner decision, 30 Oct
	// 2026). A ModelDeployment may only shorten it, through
	// routing.maxRequestDuration (InstanceDrainWindow; HBEP-4, open question
	// 11). The front checks the bound and raises DrainWindowShort when a
	// profile breaks it (HBEP-5, Draining).
	//
	// +optional
	// +kubebuilder:default="2h10m"
	// +kubebuilder:validation:XValidation:rule="duration(self) > duration('0s')",message="drainWindow must be a positive duration that fits Go's time.Duration (above 0s, at most 2562047h)"
	DrainWindow *Duration `json:"drainWindow,omitempty"`

	// DrainMargin is the margin above a request's longest duration in the
	// drain window. An Instance's window is DrainWindow, or its
	// ModelDeployment's routing.maxRequestDuration plus this margin when that
	// is shorter. The front and the lifecycle controller both read it, so
	// both compute the same drain start (InstanceDrainWindow; HBEP-4; HBEP-5,
	// Draining).
	//
	// +optional
	// +kubebuilder:default="10m"
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('0s')",message="drainMargin must be a duration that fits Go's time.Duration (at most 2562047h)"
	DrainMargin *Duration `json:"drainMargin,omitempty"`

	// HandoverMargin is the margin in HBEP-7's submission plan: a replacement
	// is submitted at the drain start minus (the p90 observed queue wait + the
	// start-to-ready time + this margin), never before its predecessor's start.
	//
	// +optional
	// +kubebuilder:default="30m"
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('0s')",message="handoverMargin must be a duration that fits Go's time.Duration (at most 2562047h)"
	HandoverMargin *Duration `json:"handoverMargin,omitempty"`

	// MaxIdle releases an Allocation with no Instance after this long, even
	// when its retention policy says keep (HBEP-7, Retention). It counts from
	// the Allocation's idle-since annotation (AnnotationIdleSince). Unset,
	// there is no cap.
	//
	// +optional
	// +kubebuilder:validation:XValidation:rule="duration(self) > duration('0s')",message="maxIdle must be a positive duration that fits Go's time.Duration (above 0s, at most 2562047h)"
	MaxIdle *Duration `json:"maxIdle,omitempty"`

	// Health holds HBEP-7's malfunction thresholds and start deadlines.
	//
	// +optional
	// +kubebuilder:default={}
	Health *HealthThresholds `json:"health,omitempty"`

	// Restart holds HBEP-7's restart ladder.
	//
	// +optional
	// +kubebuilder:default={}
	Restart *RestartPolicy `json:"restart,omitempty"`

	// Profile is the rest of the profile, untyped: the facts that are not typed
	// above, with the provenance envelope of each (value, method, source and
	// date; HBEP-4, open question 7), until HBEP-4 fixes the schema.
	//
	// +optional
	// +kubebuilder:validation:Type=object
	// +kubebuilder:pruning:PreserveUnknownFields
	Profile *apiextensionsv1.JSON `json:"profile,omitempty"`
}

// PlatformProfileStatus is the observed state of a PlatformProfile.
type PlatformProfileStatus struct {
	ReconcileStatus `json:",inline"`

	// LastChecked is when the profile's facts were last checked. No
	// component writes it in alpha: nothing checks a profile's facts yet
	// (docs/development.md, Writers).
	//
	// +optional
	LastChecked *metav1.Time `json:"lastChecked,omitempty"`
}

// PlatformProfile is what is true for everyone on a platform, as data
// (HBEP-3, HBEP-4). All Providers of one platform share one.
//
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,categories=hpcburst
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="ProviderKind",type=string,JSONPath=`.spec.providerKind`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type PlatformProfile struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PlatformProfileSpec   `json:"spec"`
	Status PlatformProfileStatus `json:"status,omitempty"`
}

// PlatformProfileList is a list of PlatformProfiles.
//
// +kubebuilder:object:root=true
type PlatformProfileList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PlatformProfile `json:"items"`
}

func init() { SchemeBuilder.Register(&PlatformProfile{}, &PlatformProfileList{}) }
