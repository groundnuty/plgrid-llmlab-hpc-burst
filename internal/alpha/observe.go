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
	// DrainStart is the predecessor's drain start, from its recorded lease end.
	DrainStart time.Time `json:"drainStart"`
	// Handovers counts the handovers that completed end to end.
	Handovers int `json:"handovers"`
	// WorkerRestarts counts the restarts at the worker rung the run made.
	WorkerRestarts int `json:"workerRestarts"`
	// Responses counts the responses the run received; Attributed those that
	// carried HBEP-5's attribution headers.
	Responses  int `json:"responses"`
	Attributed int `json:"attributed"`
}

// NoOverlapObs is a deployment through no-overlap mode on Helios (alpha-helios-nooverlap).
type NoOverlapObs struct {
	Tags    []string `json:"tags"`
	Streams []Stream `json:"streams"`
	// DrainStart is the predecessor's drain start; SuccessorReady is when
	// the successor Instance became ready. Their difference is the gap.
	DrainStart     time.Time `json:"drainStart"`
	SuccessorReady time.Time `json:"successorReady"`
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

// PathRate is S1's measurement of the tunnel's sustained rate in the token
// direction, node to front (spikes/s1-frp, "Path rate", verdict C6).
type PathRate struct {
	Direction string `json:"direction"`
	// OK is the measurement's own success flag.
	OK bool `json:"ok"`
	// AskedSeconds is how long the measurement was asked to run; TimedSeconds
	// how long the receiver timed it.
	AskedSeconds float64 `json:"askedSeconds"`
	TimedSeconds float64 `json:"timedSeconds"`
	Bytes        int64   `json:"bytes"`
}

// BytesPerSecond is the rate the receiver timed; zero when nothing was timed.
func (p PathRate) BytesPerSecond() float64 {
	if p.TimedSeconds <= 0 {
		return 0
	}
	return float64(p.Bytes) / p.TimedSeconds
}

// TunnelHourObs is one hour over this wiring on one cluster (tunnel-hour-*).
type TunnelHourObs struct {
	Tags []string `json:"tags"`
	// Lanes are the streams held behind frpc for the hour, one per lane.
	Lanes []Stream `json:"lanes"`
	// TunnelDrops counts the times the Instance's TunnelConnected condition
	// went False during the hour.
	TunnelDrops int      `json:"tunnelDrops"`
	Rate        PathRate `json:"rate"`
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
	// NodeGPUs is the number of GPUs the job saw on its node, read by the job
	// itself, not from the scheduler's accounting the record is built from.
	NodeGPUs int `json:"nodeGPUs"`
}
