package tests

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s8/evlayer"
)

// rec is one record a handler saw.
type rec struct {
	Seq         uint64 // for a gap: To
	From, To    uint64 // gap range (zero for events)
	Gap         bool
	ID          string
	Resource    string
	Resequenced bool
	Redelivered bool
}

// collector is a thread-safe handler that records everything it is given.
type collector struct {
	mu    sync.Mutex
	recs  []rec
	delay time.Duration
	// failOnce makes the handler return an error the first time it sees an event id in the set.
	failOnce map[string]bool
}

func (c *collector) handle(ctx context.Context, r evlayer.Record) error {
	if c.delay > 0 {
		select {
		case <-time.After(c.delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if r.Gap != nil {
		c.recs = append(c.recs, rec{Seq: r.Gap.To, From: r.Gap.From, To: r.Gap.To, Gap: true})
		return nil
	}
	id := r.Event.ID()
	if c.failOnce[id] {
		delete(c.failOnce, id)
		return context.DeadlineExceeded // any error: "not handled"
	}
	c.recs = append(c.recs, rec{Seq: r.Seq, ID: id, Resource: r.Resource, Resequenced: r.Resequenced, Redelivered: r.Redelivered})
	return nil
}

func (c *collector) snapshot() []rec {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]rec(nil), c.recs...)
}

// maxCovered is the highest stream sequence handled or covered by a gap.
func (c *collector) maxCovered() uint64 {
	var m uint64
	for _, r := range c.snapshot() {
		if r.Seq > m {
			m = r.Seq
		}
	}
	return m
}

func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", d, what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func maxSeq(acks []acked) uint64 {
	var m uint64
	for _, a := range acks {
		if a.Seq > m {
			m = a.Seq
		}
	}
	return m
}

// assertCoverage checks the gap contract against what the publisher was told:
// every acknowledged sequence from start on is either handled exactly once or inside
// exactly one reported gap, in sequence order, with no hole between consecutive records.
// The expected set comes from the publisher's acknowledgements, not from the subscriber.
func assertCoverage(t *testing.T, recs []rec, acks []acked, start uint64) (handled, gapped int) {
	t.Helper()
	byID := map[string]uint64{}
	for _, a := range acks {
		byID[a.ID] = a.Seq
	}
	next := start
	for i, r := range recs {
		lo, hi := r.Seq, r.Seq
		if r.Gap {
			lo, hi = r.From, r.To
			if lo > hi {
				t.Errorf("record %d: gap with From %d > To %d", i, lo, hi)
			}
		} else if want, ok := byID[r.ID]; !ok {
			t.Errorf("record %d: event %s was never acknowledged to the publisher", i, r.ID)
		} else if want != r.Seq {
			t.Errorf("record %d: event %s has stream sequence %d, the publisher was acknowledged %d", i, r.ID, r.Seq, want)
		}
		if lo != next {
			t.Errorf("record %d covers %d..%d but the next uncovered sequence is %d (hole or overlap)", i, lo, hi, next)
			return
		}
		next = hi + 1
		if r.Gap {
			gapped += int(hi - lo + 1)
		} else {
			handled++
		}
	}
	if want := maxSeq(acks) + 1; next != want {
		t.Errorf("records cover up to %d but the publisher was acknowledged up to %d", next-1, want-1)
	}
	return handled, gapped
}

// perResource groups handled events by resource, in the order the handler saw them.
func perResource(recs []rec) map[string][]rec {
	m := map[string][]rec{}
	for _, r := range recs {
		if !r.Gap {
			m[r.Resource] = append(m[r.Resource], r)
		}
	}
	return m
}

func inSeqOrder(rs []rec) bool {
	return sort.SliceIsSorted(rs, func(i, j int) bool { return rs[i].Seq < rs[j].Seq })
}
