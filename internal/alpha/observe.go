package alpha

import "time"

// What a live run saw. The scenario fills these in from the API, the front's
// responses and the cluster; the verdicts (verdict.go) judge them. They are
// plain data, with JSON tags, because the run writes them to its private
// record. No field holds a host, an account, a job ID or a key: a job is
// named by its tag.

// Stream is one streamed request the run held open across the event under test.
type Stream struct {
	ID     string    `json:"id"`
	Opened time.Time `json:"opened"`
	Ended  time.Time `json:"ended"`
	// Complete is true when the stream finished by its endpoint's completion
	// rule (HBEP-5, Contract), never by a close that only looks complete.
	Complete bool `json:"complete"`
	// CutBy names what cut the stream (HBEP-5, Errors: cutBy); empty when none did.
	CutBy string `json:"cutBy,omitempty"`
	// Error is the error code the front answered with, if any (for example stream_cut).
	Error string `json:"error,omitempty"`
	// Tokens is the number of tokens the stream delivered, for the streams of a
	// load (the rate a stream ran at is Tokens over its length).
	Tokens int `json:"tokens,omitempty"`
	// BodyBytes is the size of the request body the front sent to the node, the
	// upload direction (issue #224); zero for a request whose size was not recorded.
	BodyBytes int `json:"bodyBytes,omitempty"`
	// BodyStart and BodyEnd are the first byte of that body and its last, as the front sent them: the upload interval
	// (PR 244, B2 of the review). A request's response can stay open long after its body was sent, so Opened and Ended
	// say nothing about when the upload ran; a large request whose interval is not recorded (both zero, or an end that
	// is not after the start) is not counted as sent while the streams were open.
	BodyStart time.Time `json:"bodyStart,omitzero"`
	BodyEnd   time.Time `json:"bodyEnd,omitzero"`
	// The live hour's large requests (the science agent's ruling (4) on PR 255: no
	// sender-side timing on the live path in alpha). ExpectedTokens is the
	// prompt's own input tokens (TokenLine), and ArrivedTokens the input tokens
	// the engine counted in its completed response (usage.prompt_tokens,
	// PromptTokensOf; ArrivalVerified). SendStart and SendEnd are the client's own window of the
	// request, from its first byte sent to its reply read: the body is in flight
	// inside it.
	// SendBodyEnd is the client's last byte of the body written to its
	// connection: its send window ends there and its prefill window begins.
	ExpectedTokens int       `json:"expectedTokens,omitempty"`
	ArrivedTokens  int       `json:"arrivedTokens,omitempty"`
	SendStart      time.Time `json:"sendStart,omitzero"`
	SendBodyEnd    time.Time `json:"sendBodyEnd,omitzero"`
	SendEnd        time.Time `json:"sendEnd,omitzero"`
	// Ticks are the times, on the client's clock, at which the reply's data
	// lines (its tokens) arrived; not in the record.
	Ticks []time.Time `json:"-"`
}

// PromptCounted reports whether the engine answered the request with a
// completed response whose usage counts its prompt (PromptTokensOf,
// WithArrival): ArrivedTokens is set from nothing else. A body cut short in
// the tunnel is no longer JSON, and is answered otherwise.
func (s Stream) PromptCounted() bool { return s.ArrivedTokens > 0 }

// ArrivalVerified reports whether the engine read the request's whole prompt:
// its completed response counted exactly the prompt's own input tokens. That
// is the second check, after the completed response itself (PromptCounted):
// the engine tokenized the whole well-formed body. A path that rewrites a body
// into other well-formed JSON is out of scope (ArrivalPrompt).
func (s Stream) ArrivalVerified() bool {
	return s.ExpectedTokens > 0 && s.ArrivedTokens == s.ExpectedTokens
}

// UploadRecorded reports whether the upload interval is recorded: a start, and an end after it.
func (s Stream) UploadRecorded() bool {
	return !s.BodyStart.IsZero() && s.BodyEnd.After(s.BodyStart)
}

// TokensPerSecond is the rate the stream delivered tokens at; zero for a stream with no length.
func (s Stream) TokensPerSecond() float64 {
	d := s.Ended.Sub(s.Opened).Seconds()
	if d <= 0 {
		return 0
	}
	return float64(s.Tokens) / d
}

// InFlightAt reports whether the stream was open at t: opened at or before it
// and not yet ended.
func (s Stream) InFlightAt(t time.Time) bool {
	return !t.IsZero() && !s.Opened.IsZero() && !s.Opened.After(t) && s.Ended.After(t)
}

// InstanceSpan is one Instance's life as the API recorded it.
type InstanceSpan struct {
	// Tag is the job tag of the Instance's Allocation (hb-<n>-<h>).
	Tag     string    `json:"tag"`
	ReadyAt time.Time `json:"readyAt"`
	EndedAt time.Time `json:"endedAt"`
}

// HandoverObs is a handover on Athena (alpha-athena-handover).
type HandoverObs struct {
	// Tags are the job tags of every Allocation the run made, the predecessor's first.
	Tags    []string `json:"tags"`
	Streams []Stream `json:"streams"`
	// Instances are the predecessor's and the successor's, in that order.
	Instances []InstanceSpan `json:"instances"`
	// Deployment is what the run's ModelDeployment held (see DeploymentSpec).
	Deployment DeploymentSpec `json:"deployment"`
	// DrainStart is the predecessor's drain start, from its recorded lease end.
	DrainStart time.Time `json:"drainStart"`
	// Handovers counts the handovers that completed end to end.
	Handovers int `json:"handovers"`
	// WorkerRestarts counts the restarts at the worker rung the run made.
	WorkerRestarts int `json:"workerRestarts"`
	// WorkerRestart is the times of the run's worker-rung fault, posted (the
	// science agent's ruling of 2 Nov 2026, condition 5); zero in a dry run.
	WorkerRestart RestartTimes `json:"workerRestart"`
	// Responses counts the responses the run received; Attributed those that
	// carried HBEP-5's attribution headers.
	Responses  int `json:"responses"`
	Attributed int `json:"attributed"`
}

// NoOverlapObs is a deployment through no-overlap mode on Helios (alpha-helios-nooverlap).
type NoOverlapObs struct {
	Tags    []string `json:"tags"`
	Streams []Stream `json:"streams"`
	// Deployment is what the run's ModelDeployment held: for this row, the
	// Helios Provider alone, ReleaseWhenIdle and the shortest lease.
	Deployment DeploymentSpec `json:"deployment"`
	// DrainStart is the predecessor's drain start; SuccessorReady is when
	// the successor Instance became ready. Their difference is the gap.
	DrainStart     time.Time `json:"drainStart"`
	SuccessorReady time.Time `json:"successorReady"`
	// WorkerRestarts counts the restarts at the worker rung the run made on this
	// platform (HBEP-7: one on each alpha platform).
	WorkerRestarts int `json:"workerRestarts"`
	// WorkerRestart is the times of the run's worker-rung fault (as HandoverObs's).
	WorkerRestart RestartTimes `json:"workerRestart"`
	// NoOverlapSlotSeen is true when the deployment carried the NoOverlapSlot
	// reason between the two (HBEP-7, No-overlap mode).
	NoOverlapSlotSeen bool `json:"noOverlapSlotSeen"`
	// NoReadyTarget counts the new requests the front answered with 503
	// no_ready_target during the gap. It is recorded, not judged: the run may
	// send none.
	NoReadyTarget int `json:"noReadyTarget"`
}

// Gap is the time from the primary's drain start until its successor is ready
// (HBEP-7, No-overlap mode); zero when either end was not observed.
func (o NoOverlapObs) Gap() time.Duration {
	if o.DrainStart.IsZero() || o.SuccessorReady.IsZero() {
		return 0
	}
	return o.SuccessorReady.Sub(o.DrainStart)
}

// The two directions of the path, named by their endpoints, from the front's side
// (issue #224). Upload carries the front's request bodies, front to node: the
// direction most constrained from outside Cyfronet's network. Download carries the
// tokens, node to front: S1's token direction (spikes/s1-frp, "Path rate", verdict
// C6), which the code named "up" before the issue named the other one; "up" is gone
// from the record, and a rate that still says it is an unknown direction.
const (
	DirUpload   = "upload"
	DirDownload = "download"
)

// PathRate is S1's measurement of the tunnel's sustained rate in one direction
// (spikes/s1-frp, "Path rate"). A tunnel-hour run measures both.
type PathRate struct {
	// Direction is DirUpload (front to node) or DirDownload (node to front).
	Direction string `json:"direction"`
	// OK is the measurement's own success flag.
	OK bool `json:"ok"`
	// AskedSeconds is how long the measurement was asked to run; TimedSeconds
	// how long the receiver timed it.
	AskedSeconds float64 `json:"askedSeconds"`
	TimedSeconds float64 `json:"timedSeconds"`
	Bytes        int64   `json:"bytes"`
	// Tokens is the number of tokens that crossed in the same time (the download
	// direction only: the upload carries request bodies), and UnderLoad says the
	// measurement ran while the concurrent streams were open: the rate the owner
	// wants is the one under load.
	Tokens    int64 `json:"tokens"`
	UnderLoad bool  `json:"underLoad"`
}

// TokensPerSecond is the token rate the receiver timed; zero when nothing was timed.
func (p PathRate) TokensPerSecond() float64 {
	if p.TimedSeconds <= 0 {
		return 0
	}
	return float64(p.Tokens) / p.TimedSeconds
}

// BytesPerSecond is the rate the receiver timed; zero when nothing was timed.
func (p PathRate) BytesPerSecond() float64 {
	if p.TimedSeconds <= 0 {
		return 0
	}
	return float64(p.Bytes) / p.TimedSeconds
}

// TunnelSample is one look at the tunnel session of the Instance: frps's
// lastStartTime for its proxy (it changes when frpc logs in again, whatever the
// requests did meanwhile) and whether the Instance's TunnelConnected condition
// was True.
type TunnelSample struct {
	At        time.Time `json:"at"`
	LastStart time.Time `json:"lastStart"`
	Connected bool      `json:"connected"`
}

// TunnelHourObs is one hour over this wiring on one cluster (tunnel-hour-*).
type TunnelHourObs struct {
	Tags []string `json:"tags"`
	// Requests are every request of the hour, in any order.
	Requests []Stream `json:"requests"`
	// Samples are the looks at the tunnel session across the hour.
	Samples []TunnelSample `json:"samples"`
	// DrainStart is the Instance's drain start: the hour must end before it.
	DrainStart time.Time `json:"drainStart"`
	// Deployment is what the run's ModelDeployment held (see DeploymentSpec).
	Deployment DeploymentSpec `json:"deployment"`
	// Rates are the path rates, one per direction (DirUpload and DirDownload).
	Rates []PathRate `json:"rates"`
	// UploadNotMeasured, when set, says why the upload's interval and rate were
	// not measured: a live row in alpha, with no sender-side timing on the live
	// path (the science agent's ruling (4) on PR 255; front-side timing is #273).
	// The verdict then takes the large requests' arrival and their client
	// windows in place of their upload intervals; a dry run leaves it empty and
	// keeps #244's full check.
	UploadNotMeasured string `json:"uploadNotMeasured,omitempty"`
	// HeadOfLine is the streams' longest gap between two tokens while a large
	// body was in flight, against the rest of the hour (live rows).
	HeadOfLine *HeadOfLine `json:"headOfLine,omitempty"`
	// LongPlan is the hour's long stream as the live row sized it, once, before the hour (FitLong), and posted with the result
	// (issue #451). A dry run, which sizes nothing, leaves it empty, and the verdict then asks for NearDeadline.
	LongPlan LongPlan `json:"longPlan,omitzero"`
}

// HeadOfLine is the head-of-line measure of the tunnel hour (HeadOfLineOf):
// the longest gap between two tokens of any stream in a large request's send
// window (Send), in a prefill window and no send window (Prefill), either
// (InFlight), and in neither (Elsewhere), and how many gaps fell in a send or
// prefill window.
type HeadOfLine struct {
	InFlight  time.Duration `json:"inFlight"`
	Send      time.Duration `json:"send"`
	Prefill   time.Duration `json:"prefill"`
	Elsewhere time.Duration `json:"elsewhere"`
	Gaps      int           `json:"gapsInFlight"`
}

// AccountingObs is the kept accounting record of one real GPU job beside the
// GPUs the job's node showed (gpu-accounting-*).
type AccountingObs struct {
	// Partition is the partition the job ran on, as the record names it.
	Partition string `json:"partition"`
	// RecordGPUs is the record's allocatedGPUs; nil when the record marks it unknown.
	RecordGPUs *int32 `json:"recordGPUs"`
	// Basis is the record's basis; HBEP-8 fixes it to "allocated".
	Basis string `json:"basis"`
	// RequestedGPUs is what the job asked for.
	RequestedGPUs int `json:"requestedGPUs"`
	// NodeGPUs is the number of GPUs the job was given on its node, read by the
	// job itself, not from the scheduler's accounting the record is built from:
	// the count of its CUDA_VISIBLE_DEVICES, which Slurm sets to the GPUs it gave
	// the job, or, where that is unset, the count nvidia-smi lists. NodeSource
	// says which.
	NodeGPUs   int    `json:"nodeGPUs"`
	NodeSource string `json:"nodeSource,omitempty"`
	// SmiGPUs is what nvidia-smi -L listed. It is recorded and posted, not
	// judged: it counts the whole node where the cluster does not confine
	// devices to the job.
	SmiGPUs int `json:"smiGPUs"`
}
