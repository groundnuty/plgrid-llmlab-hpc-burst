// Package evlayer is the thin layer on top of embedded JetStream: CloudEvents
// envelopes, a publisher with a de-duplication id, a subscriber with an
// explicit gap marker, per-resource re-sequencing, and a stored cursor.
package evlayer

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/event"
)

const (
	// SubjectPrefix roots every event subject: hb.ev.<kind>.<name>.
	SubjectPrefix = "hb.ev."
	// SubjectsAll is the stream's subject space.
	SubjectsAll = SubjectPrefix + ">"

	// TypeGap is the CloudEvents type of a gap marker.
	TypeGap = "io.hpcburst.gap.v1"
	// SourceEmitter is the source used for test events.
	SourceEmitter = "urn:hpcburst:s8:emitter"
)

// ValidResource reports whether kind and name are usable as NATS subject tokens.
// Kubernetes names (DNS-1123) always are; anything else is refused up front.
func ValidResource(kind, name string) error {
	for _, tok := range []string{kind, name} {
		if tok == "" || strings.ContainsAny(tok, ".*> \t\r\n/") {
			return fmt.Errorf("evlayer: %q is not a valid subject token", tok)
		}
	}
	return nil
}

// ResourceKey is "kind/name"; it is the CloudEvents subject attribute.
func ResourceKey(kind, name string) string { return kind + "/" + name }

// NATSSubject maps a resource key to its NATS subject.
func NATSSubject(resourceKey string) string {
	return SubjectPrefix + strings.Replace(resourceKey, "/", ".", 1)
}

// NewEvent builds a CloudEvents 1.0 event (spec 1.0.2) for a resource.
// The envelope is created by the sdk-go v2 event type, never hand-rolled.
func NewEvent(id, kind, name, typ string, at time.Time, data any) (event.Event, error) {
	if err := ValidResource(kind, name); err != nil {
		return event.Event{}, err
	}
	e := cloudevents.NewEvent()
	e.SetID(id)
	e.SetSource(SourceEmitter)
	e.SetType(typ)
	e.SetSubject(ResourceKey(kind, name))
	e.SetTime(at.UTC())
	if data != nil {
		if err := e.SetData(cloudevents.ApplicationJSON, data); err != nil {
			return event.Event{}, err
		}
	}
	if err := e.Validate(); err != nil {
		return event.Event{}, err
	}
	return e, nil
}

// Encode renders the structured-mode JSON of an event.
func Encode(e event.Event) ([]byte, error) { return json.Marshal(e) }

// Decode parses and validates a structured-mode JSON event.
func Decode(b []byte) (event.Event, error) {
	var e event.Event
	if err := json.Unmarshal(b, &e); err != nil {
		return event.Event{}, err
	}
	if err := e.Validate(); err != nil {
		return event.Event{}, err
	}
	return e, nil
}

// GapInfo is the payload of a gap marker: stream sequences From..To (inclusive)
// were evicted by retention before this subscriber saw them.
type GapInfo struct {
	Stream   string `json:"stream"`
	From     uint64 `json:"from"`
	To       uint64 `json:"to"`
	FirstSeq uint64 `json:"first_seq"` // oldest sequence the stream still holds
	Reason   string `json:"reason"`
}

// Count is the number of sequences the gap covers.
func (g GapInfo) Count() uint64 { return g.To - g.From + 1 }

// GapEvent wraps a gap as a CloudEvent, for forwarding to other consumers.
func GapEvent(id string, at time.Time, g GapInfo) (event.Event, error) {
	e := cloudevents.NewEvent()
	e.SetID(id)
	e.SetSource("urn:hpcburst:s8:subscriber")
	e.SetType(TypeGap)
	e.SetSubject("stream/" + g.Stream)
	e.SetTime(at.UTC())
	if err := e.SetData(cloudevents.ApplicationJSON, g); err != nil {
		return event.Event{}, err
	}
	return e, e.Validate()
}
