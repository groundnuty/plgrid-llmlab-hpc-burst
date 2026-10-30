package evlayer

import (
	"context"
	"fmt"

	"github.com/cloudevents/sdk-go/v2/event"
)

// Record is what a subscriber handler receives: either an event or a gap marker.
type Record struct {
	Seq         uint64       // stream sequence (for a gap: GapInfo.To)
	Resource    string       // CloudEvents subject, "kind/name" (empty for a gap)
	Event       *event.Event // nil for a gap
	Gap         *GapInfo     // non-nil for a gap marker
	Redelivered bool         // the broker delivered this message more than once
	Resequenced bool         // fetched from the stream by the re-sequencer, ahead of a later delivery
}

// Handler processes one record. A non-nil error means "not handled": the record
// is retried (durable subscriber) or the subscriber stops (cursor tail).
type Handler func(ctx context.Context, r Record) error

// gapTracker finds sequence jumps. next is the lowest stream sequence that has
// not been delivered or reported yet; 0 means "fresh subscription, no history owed".
type gapTracker struct {
	stream string
	next   uint64
}

const reasonEvicted = "evicted by retention before delivery"

// observe is called with the stream sequence of a first delivery. It returns the
// gap that precedes it, if any, and advances next.
func (g *gapTracker) observe(seq uint64) *GapInfo {
	var gap *GapInfo
	if g.next != 0 && seq > g.next {
		gap = &GapInfo{Stream: g.stream, From: g.next, To: seq - 1, FirstSeq: seq, Reason: reasonEvicted}
	}
	if seq >= g.next {
		g.next = seq + 1
	}
	return gap
}

// idle is called when nothing is left to deliver. firstSeq is the oldest
// sequence the stream still holds (lastSeq+1 when the stream is empty).
func (g *gapTracker) idle(firstSeq uint64) *GapInfo {
	if g.next == 0 || firstSeq <= g.next {
		return nil
	}
	gap := &GapInfo{Stream: g.stream, From: g.next, To: firstSeq - 1, FirstSeq: firstSeq, Reason: reasonEvicted}
	g.next = firstSeq
	return gap
}

func decodeRecord(seq uint64, data []byte) (Record, error) {
	e, err := Decode(data)
	if err != nil {
		return Record{}, fmt.Errorf("evlayer: stream seq %d: %w", seq, err)
	}
	return Record{Seq: seq, Resource: e.Subject(), Event: &e}, nil
}
