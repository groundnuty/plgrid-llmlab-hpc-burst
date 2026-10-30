package tests

import (
	"context"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s8/evlayer"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s8/jsnode"
	"github.com/nats-io/nats.go/jetstream"
)

// expectedOrder is the per-resource order the publisher established: the stream
// sequence order of its acknowledgements (publishSeq publishes one event at a time).
func expectedOrder(acks []acked) map[string][]string {
	m := map[string][]string{}
	sorted := append([]acked(nil), acks...)
	for i := 1; i < len(sorted); i++ {
		for j := i; j > 0 && sorted[j].Seq < sorted[j-1].Seq; j-- {
			sorted[j], sorted[j-1] = sorted[j-1], sorted[j]
		}
	}
	for _, a := range sorted {
		m[a.Resource] = append(m[a.Resource], a.ID)
	}
	return m
}

func ids(rs []rec) []string {
	var out []string
	for _, r := range rs {
		out = append(out, r.ID)
	}
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// assertExactlyOncePerResourceInOrder: for every resource the handler saw exactly
// the acknowledged events, once each, in publish order.
func assertExactlyOncePerResourceInOrder(t *testing.T, recs []rec, acks []acked) {
	t.Helper()
	got := perResource(recs)
	for res, want := range expectedOrder(acks) {
		if !equalStrings(ids(got[res]), want) {
			t.Errorf("resource %s: handler saw %v, want %v", res, ids(got[res]), want)
		}
	}
}

// rawConsume is the control: a plain JetStream pull loop with no layer. A handler
// error is a NAK with a delay, as a plain client would do.
func rawConsume(ctx context.Context, cons jetstream.Consumer, c *collector, nakDelay time.Duration) {
	for ctx.Err() == nil {
		b, err := cons.Fetch(8, jetstream.FetchMaxWait(200*time.Millisecond))
		if err != nil {
			continue
		}
		for m := range b.Messages() {
			md, _ := m.Metadata()
			e, err := evlayer.Decode(m.Data())
			if err != nil {
				_ = m.Term()
				continue
			}
			err = c.handle(ctx, evlayer.Record{Seq: md.Sequence.Stream, Resource: e.Subject(), Event: &e, Redelivered: md.NumDelivered > 1})
			if err != nil {
				_ = m.NakWithDelay(nakDelay)
				continue
			}
			_ = m.Ack()
		}
	}
}

// Acceptance test 3a: a NAK makes the broker redeliver an event after later ones.
// The control proves the disorder is real; the layer must remove it.
func TestOrder_AfterNak(t *testing.T) {
	for _, withLayer := range []bool{false, true} {
		name := "control_plain_consumer"
		if withLayer {
			name = "with_layer"
		}
		t.Run(name, func(t *testing.T) {
			n, _ := inProcNode(t, jsnode.SyncAlways)
			js := connect(t, n.URL())
			ctx := ctxT(t, 60*time.Second)
			if _, err := evlayer.EnsureStream(ctx, js, evlayer.StreamOptions{Name: streamName}); err != nil {
				t.Fatal(err)
			}
			acks := publishSeq(ctx, t, js, "o", 60, 3)
			// The third event of resource r0 fails once.
			var victim string
			for _, a := range acks {
				if a.Resource == "allocation/r0" && a.N == 3 {
					victim = a.ID
				}
			}
			c := &collector{failOnce: map[string]bool{victim: true}}
			rctx, cancel := context.WithCancel(ctx)
			done := make(chan error, 1)
			var d *evlayer.Durable
			cfg := evlayer.DurableConfig{Stream: streamName, Durable: "nak", AckWait: 5 * time.Second, MaxAckPending: 64, Batch: 8, NakDelay: 300 * time.Millisecond}
			if withLayer {
				var err error
				if d, err = evlayer.NewDurable(ctx, js, cfg); err != nil {
					t.Fatal(err)
				}
				go func() { done <- d.Run(rctx, c.handle) }()
			} else {
				cons, err := js.CreateOrUpdateConsumer(ctx, streamName, jetstream.ConsumerConfig{Durable: "nak", AckPolicy: jetstream.AckExplicitPolicy, AckWait: 5 * time.Second, MaxAckPending: 64})
				if err != nil {
					t.Fatal(err)
				}
				go func() { rawConsume(rctx, cons, c, 300*time.Millisecond); done <- nil }()
			}
			waitFor(t, 30*time.Second, "all 60 events handled", func() bool { return len(c.snapshot()) >= len(acks) })
			time.Sleep(500 * time.Millisecond)
			cancel()
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			recs := c.snapshot()
			if !withLayer {
				r0 := perResource(recs)["allocation/r0"]
				if inSeqOrder(r0) {
					t.Fatal("control invalid: the plain consumer kept per-resource order, so the NAK did not reorder anything")
				}
				t.Logf("RESULT control: plain consumer handled allocation/r0 out of order after a NAK: %v", ids(r0)[:6])
				return
			}
			assertExactlyOncePerResourceInOrder(t, recs, acks)
			if d.Resequenced == 0 {
				t.Error("the layer never had to re-sequence, so the scenario did not exercise it")
			}
			t.Logf("RESULT layer: per-resource order holds after a NAK; re-sequenced %d, duplicates dropped %d, all %d events exactly once", d.Resequenced, d.Duplicates, len(acks))
		})
	}
}

// Acceptance test 3b: a worker takes events and never acknowledges them (it crashed);
// they come back after the ack wait, behind events that were delivered meanwhile.
func TestOrder_AfterAckWaitExpiry(t *testing.T) {
	for _, withLayer := range []bool{false, true} {
		name := "control_plain_consumer"
		if withLayer {
			name = "with_layer"
		}
		t.Run(name, func(t *testing.T) {
			n, _ := inProcNode(t, jsnode.SyncAlways)
			js := connect(t, n.URL())
			ctx := ctxT(t, 60*time.Second)
			if _, err := evlayer.EnsureStream(ctx, js, evlayer.StreamOptions{Name: streamName}); err != nil {
				t.Fatal(err)
			}
			acks := publishSeq(ctx, t, js, "w", 36, 3)
			cons, err := js.CreateOrUpdateConsumer(ctx, streamName, jetstream.ConsumerConfig{Durable: "crash", AckPolicy: jetstream.AckExplicitPolicy, AckWait: time.Second, MaxAckPending: 64})
			if err != nil {
				t.Fatal(err)
			}
			// The crashed worker: takes the first 6 events and never acknowledges them.
			b, err := cons.Fetch(6, jetstream.FetchMaxWait(2*time.Second))
			if err != nil {
				t.Fatal(err)
			}
			taken := 0
			for range b.Messages() {
				taken++
			}
			if taken != 6 {
				t.Fatalf("setup: the crashed worker took %d events, want 6", taken)
			}
			c := &collector{}
			rctx, cancel := context.WithCancel(ctx)
			done := make(chan error, 1)
			var d *evlayer.Durable
			if withLayer {
				if d, err = evlayer.NewDurable(ctx, js, evlayer.DurableConfig{Stream: streamName, Durable: "crash", AckWait: time.Second, MaxAckPending: 64, Batch: 8}); err != nil {
					t.Fatal(err)
				}
				go func() { done <- d.Run(rctx, c.handle) }()
			} else {
				go func() { rawConsume(rctx, cons, c, 300*time.Millisecond); done <- nil }()
			}
			waitFor(t, 30*time.Second, "all 36 events handled", func() bool { return len(c.snapshot()) >= len(acks) })
			time.Sleep(2500 * time.Millisecond) // let any further redelivery arrive
			cancel()
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			recs := c.snapshot()
			if !withLayer {
				disorder := 0
				for _, rs := range perResource(recs) {
					if !inSeqOrder(rs) {
						disorder++
					}
				}
				if disorder == 0 {
					t.Fatal("control invalid: the plain consumer kept per-resource order, so ack-wait expiry did not reorder anything")
				}
				t.Logf("RESULT control: after ack-wait expiry the plain consumer handled %d of 3 resources out of order", disorder)
				return
			}
			assertExactlyOncePerResourceInOrder(t, recs, acks)
			if d.Duplicates != 6 {
				t.Errorf("expected the 6 redelivered copies to be dropped as duplicates, got %d", d.Duplicates)
			}
			t.Logf("RESULT layer: order holds after ack-wait expiry; re-sequenced %d, redelivered copies dropped %d, all %d events exactly once", d.Resequenced, d.Duplicates, len(acks))
		})
	}
}

// Acceptance test 3c: a stored-cursor subscriber is in stream order by construction.
func TestOrder_Tail_StreamOrder(t *testing.T) {
	n, _ := inProcNode(t, jsnode.SyncAlways)
	js := connect(t, n.URL())
	ctx := ctxT(t, 60*time.Second)
	if _, err := evlayer.EnsureStream(ctx, js, evlayer.StreamOptions{Name: streamName}); err != nil {
		t.Fatal(err)
	}
	acks := publishSeq(ctx, t, js, "t", 60, 3)
	c := &collector{}
	cur := evlayer.FileCursor{Path: scratchDir(t, "cursor") + "/cursor.json"}
	rctx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- evlayer.Tail(rctx, js, evlayer.TailConfig{Stream: streamName, Store: cur}, c.handle) }()
	waitFor(t, 30*time.Second, "all 60 events handled", func() bool { return len(c.snapshot()) >= len(acks) })
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	assertExactlyOncePerResourceInOrder(t, c.snapshot(), acks)
}
