package alpha

// What the baseline row saw (baseline.go): the observation Run takes from the flow, in plain values, so that the judge (baselinejudge.go) reads it with no
// cluster and no clock. Every count the judge makes is from the FRONT's usage records (by Hpcburst-Request-Id) and the ENGINE's gauges, never from the
// generator's own records (a count of the generator's own is checked against itself): the generator's records give the join alone, a request ID to the session and turn it was.

import (
	"fmt"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/usage"
)

// FrontRow is the front's usage record of one request (HBEP-5, Usage records), the fields the judge reads.
type FrontRow struct {
	RequestID       string    `json:"requestID"`
	Provider        string    `json:"provider"`
	ModelDeployment string    `json:"modelDeployment"`
	Emitter         string    `json:"emitter"`
	Instance        string    `json:"instance"`
	InstanceUID     string    `json:"instanceUID"`
	Status          int       `json:"status"`
	CutBy           string    `json:"cutBy,omitempty"`
	Start           time.Time `json:"start"`
	End             time.Time `json:"end"`
	// FirstTokenMs is the time from arrival to the first content event: it includes any wait in the front's FIFO (internal/front/metrics.go:73).
	FirstTokenMs     *int64 `json:"firstTokenMs,omitempty"`
	PromptTokens     *int64 `json:"promptTokens,omitempty"`
	CompletionTokens *int64 `json:"completionTokens,omitempty"`
	DurationMs       int64  `json:"durationMs"`
	// Finish is the stream's finish reason, nil when it had none, and ContentEvents the front's own count of content events, nil when it kept none (the
	// usage record's finish and contentEvents): a completed stream has a finish reason and, with no counts, at least one content event.
	Finish        *string `json:"finish,omitempty"`
	ContentEvents *int64  `json:"contentEvents,omitempty"`
}

// FrontRowOf reads a usage record. A record whose times cannot be read is an error: it cannot be placed in a phase.
func FrontRowOf(r usage.Record) (FrontRow, error) {
	start, err := time.Parse(time.RFC3339, r.StartTime)
	if err != nil {
		return FrontRow{}, fmt.Errorf("a usage record's startTime: %w", err)
	}
	end, err := time.Parse(time.RFC3339, r.EndTime)
	if err != nil {
		return FrontRow{}, fmt.Errorf("a usage record's endTime: %w", err)
	}
	row := FrontRow{RequestID: r.RequestID, Provider: r.Provider, ModelDeployment: r.ModelDeployment, Emitter: r.Emitter, Instance: r.Instance,
		InstanceUID: r.InstanceUID, Status: r.Status, Start: start, End: end, FirstTokenMs: r.FirstTokenMs, PromptTokens: r.PromptTokens,
		CompletionTokens: r.CompletionTokens, DurationMs: r.DurationMs, Finish: r.Finish, ContentEvents: r.ContentEvents}
	if r.CutBy != nil {
		row.CutBy = *r.CutBy
	}
	return row, nil
}

// GenRow is what the generator's record of a request gives the judge: the join (the request ID, and the session and turn it was), and the route its
// response headers named, which the judge only compares with the front's record and never counts from. A record with no RequestID got no answer from the
// front.
type GenRow struct {
	Session   string `json:"session"`
	Turn      int    `json:"turn"`
	RequestID string `json:"requestID"`
	Emitter   string `json:"emitter,omitempty"`
	Provider  string `json:"provider,omitempty"`
	Instance  string `json:"instance,omitempty"`
}

// EngineSample is one read of the served Instance's engine gauges (HBEP-8, Engine gauges per Instance), from the controller's /metrics.
type EngineSample struct {
	// At is this host's time of the scrape that FIRST saw this read, and ReadAt the agent's own time of the read (the node's clock, from
	// hpcburst_instance_metrics_read_timestamp_seconds, stamped when the agent's read returned). ReadAt tells reads apart, and is compared with this
	// host's clock for ONE purpose: placing a read inside a stretch with the cap's requests in flight (readsAtTheCap, over every phase's samples), whose bounds
	// are the front's records on this host's clock. The skew of the node's clock against this host's is a stated limit of that placement, and At is never used for it. A sample
	// with no ReadAt is never fresh and never at the cap, and the controller's gauges repeat a read between reads, so only a read newer than the last
	// one taken is a sample.
	At          time.Time `json:"at"`
	ReadAt      time.Time `json:"readAt"`
	Running     float64   `json:"running"`
	Waiting     float64   `json:"waiting"`
	Preemptions float64   `json:"preemptions"`
	// KVUsage is hpcburst_instance_kv_cache_usage_ratio: vLLM's vllm:kv_cache_usage_perc, 0 to 1.
	KVUsage float64 `json:"kvUsage"`
}

// Why a phase's generator was stopped before it ended.
const (
	StopGuard       = "llmlab-guard"
	StopGuardUnread = "llmlab-guard-unread"
	StopIneligible  = "instance-ineligible"
	StopBound       = "bound"
)

// PhaseObs is what one phase showed.
type PhaseObs struct {
	Phase Phase `json:"phase"`
	// Start and End are this host's times around the generator's run: the front runs here, and its usage records carry the same clock.
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
	// Gen are the generator's records, for the join only. Rows are every usage record the front wrote from Start on, the phase's own and any other.
	Gen  []GenRow   `json:"gen"`
	Rows []FrontRow `json:"rows"`
	// LLMLabMetric is how much the front's per-provider request metric (hpcburst_front_requests_total{provider}) for LLMLab rose over the phase, read
	// live; MetricRead is false when it could not be read. The guard reads the same series while the phase runs.
	LLMLabMetric float64 `json:"llmlabMetric"`
	MetricRead   bool    `json:"metricRead"`
	// Engine are the samples read from the phase's start to its end.
	Engine []EngineSample `json:"engine"`
	// Stopped says why the generator was stopped before it ended: StopGuard, StopIneligible or StopBound; empty when it ended by itself.
	Stopped string `json:"stopped,omitempty"`
	// Waiting are the reads of the front's OWN count of requests waiting in its FIFO (its /healthz "waiting"), taken every BaselineWaitingPoll in the detach
	// phase and once more when the generator had ended. It sums every group, so it is read as the count of one deployment's FIFO only because the front holds
	// ONE served deployment of the model: ServedOfModel counts them, read when the phase began.
	Waiting []WaitSample `json:"waiting,omitempty"`
	// DialFailures counts the requests of the phase that the front's own log reports resent after a failed dial of the served Instance (a count only: the log
	// is private state), DialLogRead says the log could be read, DialLines how many of its lines were parsed (a count only), and DialFailed holds the matched
	// requests for the count, never written to a record or a post.
	DialFailures      int           `json:"dialFailures,omitempty"`
	DialLogRead       bool          `json:"dialLogRead"`
	DialLines         int           `json:"dialLines"` // the lines of the log parsed as slog JSON objects: 0 means the wrong file or an empty one
	DialFailed        []DialFailure `json:"-"`
	ServedOfModel     int           `json:"servedOfModel,omitempty"`
	ServedOfModelRead bool          `json:"servedOfModelRead,omitempty"`
}

// WaitSample is one read of the front's count of requests waiting in its FIFO.
type WaitSample struct {
	At time.Time `json:"at"`
	N  int       `json:"n"`
}

// GateObs is the gate: one request at model@<provider> for the served Instance, answered by it.
type GateObs struct {
	Sent bool     `json:"sent"`
	Row  FrontRow `json:"row"`
}

// ProbeObs is P0: one request of P0Tokens tokens at the plain model before the GPU job, with the front's usage record of it.
type ProbeObs struct {
	Sent bool     `json:"sent"`
	Row  FrontRow `json:"row"`
}

// ServedMD and ManagedMD are what the API held for the row's two ModelDeployments after they were created.
type ServedMD struct {
	Model          string   `json:"model"`
	Providers      []string `json:"providers"`
	Instances      int32    `json:"instances"`
	OverAllocation *int32   `json:"overAllocation"`
	HedgeWidth     *int32   `json:"hedgeWidth"`
	InstanceCap    *int32   `json:"instanceCap"`
	// DeploymentID is the display form of the served Instance's deployment ID (HBEP-3), empty when it was not read.
	DeploymentID string `json:"deploymentID"`
}

// ManagedMD is the managed ModelDeployment: its model, the platform and the Providers it is attached to, by name.
type ManagedMD struct {
	Model     string   `json:"model"`
	Platform  string   `json:"platform"`
	Providers []string `json:"providers"`
}

// BaselineObs is the whole run of Row 1.
type BaselineObs struct {
	Tags []string `json:"tags"`
	// Deployment is the served ModelDeployment's plan as the API held it (providers, lease, request deadline), judged by deploymentChecks.
	Deployment DeploymentSpec `json:"deployment"`
	Served     ServedMD       `json:"served"`
	Managed    ManagedMD      `json:"managed"`
	// Row is the baseline row's name; the judge takes the row's facts by it (baselineFactsOf: Row 1's when it is empty).
	Row string `json:"row,omitempty"`
	// Cap and K are the row's constants (CapSano or CapAthena, LLMLabK) as the run used them; the judge compares them with what the API held.
	Cap int `json:"cap"`
	K   int `json:"k"`
	// LLMLabProvider is the managed Provider's name in the run.
	LLMLabProvider string `json:"llmlabProvider"`
	// OmitMinTokens says min_tokens was left out of every request (the min_tokens probe's verdict).
	OmitMinTokens bool      `json:"omitMinTokens"`
	ReadyAt       time.Time `json:"readyAt"`
	DrainStart    time.Time `json:"drainStart"`
	// LoggedKV is the engine's logged kv_cache_size_tokens, read after ready as a CHECK; KVRead is false when it could not be read.
	LoggedKV int64    `json:"loggedKV"`
	KVRead   bool     `json:"kvRead"`
	P0       ProbeObs `json:"p0"`
	// ServedCreated says the served ModelDeployment was created, which is the first thing that can submit a job: it is, only after P0 completed. A run whose P0
	// failed ends with it false and no job.
	ServedCreated bool    `json:"servedCreated"`
	Gate          GateObs `json:"gate"`
	// Plan is the phases the run was asked to make, in order; Phases the ones it made.
	Plan   []Phase    `json:"plan"`
	Phases []PhaseObs `json:"phases"`
	// Ratio is the ratio probe: what P1-long's contexts were set from.
	Ratio RatioObs `json:"ratio"`
	// StoppedBefore names the step that ended the run before any phase because the served Instance did not answer it: "the gate" or "the ratio probe".
	// No generator was started for a phase, so LLMLab saw none of the load; the served job is ended by the normal cleanup.
	StoppedBefore string `json:"stoppedBefore,omitempty"`
	// DidNotFit names the phase the time left before the drain start could not hold, which ended the run INVALID.
	DidNotFit string `json:"didNotFit,omitempty"`
	// Detached says the managed ModelDeployment was deleted and the front dropped its target before the detach phase ran.
	Detached bool `json:"detached"`
}

// RatioObs is the ratio probe's reading (baseline.go, LongContextWords): the words it sent, the prompt tokens the front's record gave, the tokens a word came to,
// and the words P1-long's contexts were set to, or why they could not be. Needed is false when no phase derives its context.
type RatioObs struct {
	Needed        bool     `json:"needed"`
	Words         int      `json:"words"`
	Row           FrontRow `json:"row"`
	Sent          bool     `json:"sent"`
	PromptTokens  int64    `json:"promptTokens"`
	TokensPerWord float64  `json:"tokensPerWord"`
	LongWords     int      `json:"longWords"`
	// Invalid says the long shape could not be set from the reading, so P1-long was not run.
	Invalid bool `json:"invalid"`
	// Stopped says why the probe's generator was stopped or never started (StopGuardUnread, StopIneligible, StopBound), empty when it ended by itself.
	Stopped string `json:"stopped,omitempty"`
}
