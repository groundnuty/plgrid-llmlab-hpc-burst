package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s8/evlayer"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s8/jsnode"
	"github.com/nats-io/nats.go/jetstream"
)

// A message a durable consumer was given but has not acknowledged, then evicted by
// retention, drops out of the consumer's ack state: the ack floor jumps past it, the
// pending count falls to zero and nothing redelivers it. This test pins what
// nats-server 2.15.2 does, so a change in a later release shows up here.
// The durable subscriber therefore cannot mark that range; the stored-cursor one can.
func TestFinding_EvictedWhilePending_DisappearsFromAckState(t *testing.T) {
	n, _ := inProcNode(t, jsnode.SyncAlways)
	js := connect(t, n.URL())
	ctx := ctxT(t, 60*time.Second)
	if _, err := evlayer.EnsureStream(ctx, js, evlayer.StreamOptions{Name: streamName, MaxMsgs: gapLimit}); err != nil {
		t.Fatal(err)
	}
	publishSeq(ctx, t, js, "p", 30, 2)
	cons, err := js.CreateOrUpdateConsumer(ctx, streamName, jetstream.ConsumerConfig{Durable: "pend", AckPolicy: jetstream.AckExplicitPolicy, AckWait: time.Second, MaxAckPending: 64})
	if err != nil {
		t.Fatal(err)
	}
	b, err := cons.Fetch(10, jetstream.FetchMaxWait(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	i := 0
	for m := range b.Messages() {
		i++
		if i <= 5 {
			_ = m.Ack() // 1..5 acknowledged; 6..10 delivered, never acknowledged
		}
	}
	time.Sleep(200 * time.Millisecond)
	ci, _ := cons.Info(ctx)
	if ci.NumAckPending != 5 || ci.AckFloor.Stream != 5 {
		t.Fatalf("setup: want 5 pending and ack floor 5, got pending=%d floor=%d", ci.NumAckPending, ci.AckFloor.Stream)
	}
	publishConcurrent(ctx, t, js, "q", 200, 8) // evicts everything up to well past sequence 10
	time.Sleep(1500 * time.Millisecond)        // longer than AckWait: a redelivery would be due
	ci, _ = cons.Info(ctx)
	if ci.NumAckPending != 0 || ci.AckFloor.Stream < 10 || ci.NumRedelivered != 0 {
		t.Fatalf("server behaviour changed: pending=%d ack floor=%d redelivered=%d (was 0, >=10, 0 in nats-server 2.15.2)", ci.NumAckPending, ci.AckFloor.Stream, ci.NumRedelivered)
	}
	t.Logf("RESULT finding: sequences 6..10 were delivered and never acknowledged, then evicted; the consumer now reports ack floor %d, 0 pending, 0 redelivered", ci.AckFloor.Stream)

	// What each subscriber says about 6..10 when it is started afterwards.
	d, err := evlayer.NewDurable(ctx, js, evlayer.DurableConfig{Stream: streamName, Durable: "pend", AckWait: time.Second, MaxAckPending: 64})
	if err != nil {
		t.Fatal(err)
	}
	c := &collector{}
	rctx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- d.Run(rctx, c.handle) }()
	waitFor(t, 30*time.Second, "a gap marker", func() bool { return countGaps(c.snapshot()) >= 1 })
	cancel()
	<-done
	var from uint64
	for _, r := range c.snapshot() {
		if r.Gap {
			from = r.From
			break
		}
	}
	if from != 11 {
		t.Fatalf("durable subscriber gap starts at %d; the documented limitation is that it starts at 11 and never mentions 6..10", from)
	}
	t.Logf("RESULT limitation: the durable subscriber marks the gap from sequence %d on; sequences 6..10 get no marker", from)

	// The stored-cursor subscriber, with its own cursor at 5, covers them.
	cur := evlayer.FileCursor{Path: scratchDir(t, "cursor") + "/cursor.json"}
	if err := cur.Save(evlayer.Cursor{Seq: 5, EventID: "p-4"}); err != nil {
		t.Fatal(err)
	}
	// p-4 is the 5th event published, so it is the event at sequence 5.
	_ = fmt.Sprint
	c2 := &collector{}
	rctx2, cancel2 := context.WithTimeout(ctx, 3*time.Second)
	defer cancel2()
	if err := evlayer.Tail(rctx2, js, evlayer.TailConfig{Stream: streamName, Store: cur, IdleCheck: 200 * time.Millisecond}, c2.handle); err != nil {
		t.Fatal(err)
	}
	var tailFrom uint64
	for _, r := range c2.snapshot() {
		if r.Gap {
			tailFrom = r.From
			break
		}
	}
	if tailFrom != 6 {
		t.Fatalf("stored-cursor subscriber gap starts at %d, want 6", tailFrom)
	}
	t.Logf("RESULT stored cursor: gap starts at sequence %d, covering 6..10", tailFrom)
}
