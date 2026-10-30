package tests

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s8/evlayer"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s8/jsnode"
	"github.com/nats-io/nats.go/jetstream"
)

const gapLimit = 50 // MaxMsgs of the small stream: the limit that must actually be reached

// publishConcurrent publishes n events from several goroutines and returns the acknowledgements.
func publishConcurrent(ctx context.Context, t *testing.T, js jetstream.JetStream, tag string, n, workers int) []acked {
	t.Helper()
	var mu sync.Mutex
	var out []acked
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := w; i < n; i += workers {
				name := fmt.Sprintf("r%d", i%4)
				id := fmt.Sprintf("%s-%d", tag, i)
				e, err := evlayer.NewEvent(id, "allocation", name, "io.hpcburst.test.v1", time.Now(), map[string]int{"i": i})
				if err != nil {
					t.Error(err)
					return
				}
				ack, err := evlayer.Publish(ctx, js, e)
				if err != nil {
					t.Errorf("publish %s: %v", id, err)
					return
				}
				mu.Lock()
				out = append(out, acked{Seq: ack.Sequence, ID: id, Resource: evlayer.ResourceKey("allocation", name)})
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()
	return out
}

func assertLimitReached(t *testing.T, js jetstream.JetStream, acks []acked) {
	t.Helper()
	st, err := js.Stream(ctxT(t, 10*time.Second), streamName)
	if err != nil {
		t.Fatal(err)
	}
	si, err := st.Info(ctxT(t, 10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	// The limit was reached and enforced: the stream holds only its limit and its oldest sequence moved.
	if len(acks) <= 3*gapLimit || si.State.Msgs > gapLimit || si.State.FirstSeq <= 1 {
		t.Fatalf("eviction was not exercised: published %d, stream holds %d (limit %d), first seq %d", len(acks), si.State.Msgs, gapLimit, si.State.FirstSeq)
	}
	t.Logf("limit reached: published %d, stream holds %d of limit %d, first seq now %d", len(acks), si.State.Msgs, gapLimit, si.State.FirstSeq)
}

// Acceptance test 2 (durable consumer): a slow subscriber is overtaken by retention
// and sees an explicit gap marker; nothing is missing without a marker.
func TestGap_Durable_SlowSubscriber_MaxMsgs(t *testing.T) {
	n, _ := inProcNode(t, jsnode.SyncAlways)
	js := connect(t, n.URL())
	ctx := ctxT(t, 120*time.Second)
	if _, err := evlayer.EnsureStream(ctx, js, evlayer.StreamOptions{Name: streamName, MaxMsgs: gapLimit}); err != nil {
		t.Fatal(err)
	}
	d, err := evlayer.NewDurable(ctx, js, evlayer.DurableConfig{Stream: streamName, Durable: "slow", AckWait: 5 * time.Second, MaxAckPending: 64, Batch: 5})
	if err != nil {
		t.Fatal(err)
	}
	c := &collector{delay: 15 * time.Millisecond}
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- d.Run(runCtx, c.handle) }()

	acks := publishConcurrent(ctx, t, js, "g", 600, 8)
	assertLimitReached(t, js, acks)
	waitFor(t, 90*time.Second, "the subscriber to drain", func() bool { return c.maxCovered() >= maxSeq(acks) })
	stop()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	recs := c.snapshot()
	handled, gapped := assertCoverage(t, recs, acks, 1)
	if gapped == 0 {
		t.Fatal("retention evicted unread messages but the subscriber saw no gap marker")
	}
	t.Logf("RESULT slow durable subscriber: handled %d, gap-marked %d sequences in %d markers, of %d acknowledged", handled, gapped, countGaps(recs), len(acks))
}

func countGaps(recs []rec) int {
	n := 0
	for _, r := range recs {
		if r.Gap {
			n++
		}
	}
	return n
}

// Negative control: a plain JetStream consumer in the same situation skips ahead with no marker.
func TestGap_Control_PlainConsumerSkipsSilently(t *testing.T) {
	n, _ := inProcNode(t, jsnode.SyncAlways)
	js := connect(t, n.URL())
	ctx := ctxT(t, 120*time.Second)
	if _, err := evlayer.EnsureStream(ctx, js, evlayer.StreamOptions{Name: streamName, MaxMsgs: gapLimit}); err != nil {
		t.Fatal(err)
	}
	cons, err := js.CreateOrUpdateConsumer(ctx, streamName, jetstream.ConsumerConfig{Durable: "plain", AckPolicy: jetstream.AckExplicitPolicy, MaxAckPending: 64})
	if err != nil {
		t.Fatal(err)
	}
	var seqs []uint64
	var mu sync.Mutex
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for runCtx.Err() == nil {
			b, err := cons.Fetch(5, jetstream.FetchMaxWait(300*time.Millisecond))
			if err != nil {
				continue
			}
			for m := range b.Messages() {
				time.Sleep(15 * time.Millisecond)
				md, _ := m.Metadata()
				mu.Lock()
				seqs = append(seqs, md.Sequence.Stream)
				mu.Unlock()
				_ = m.Ack()
			}
		}
	}()
	acks := publishConcurrent(ctx, t, js, "c", 600, 8)
	assertLimitReached(t, js, acks)
	waitFor(t, 90*time.Second, "the plain consumer to drain", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seqs) > 0 && seqs[len(seqs)-1] >= maxSeq(acks)
	})
	stop()
	<-done
	jumps := 0
	for i := 1; i < len(seqs); i++ {
		if seqs[i] != seqs[i-1]+1 {
			jumps++
		}
	}
	if jumps == 0 {
		t.Fatal("control invalid: the plain consumer saw no sequence jump, so eviction did not outrun it")
	}
	t.Logf("RESULT control: the plain consumer saw %d jump(s) in sequence and no marker of any kind (%d of %d messages delivered)", jumps, len(seqs), len(acks))
}

// stoppedThenEvicted: a subscriber handles the first events, stops, retention evicts, it resumes.
func stoppedThenEvicted(t *testing.T, run func(ctx context.Context, js jetstream.JetStream, c *collector) (stop func() error)) {
	n, _ := inProcNode(t, jsnode.SyncAlways)
	js := connect(t, n.URL())
	ctx := ctxT(t, 120*time.Second)
	if _, err := evlayer.EnsureStream(ctx, js, evlayer.StreamOptions{Name: streamName, MaxMsgs: gapLimit}); err != nil {
		t.Fatal(err)
	}
	acks := publishSeq(ctx, t, js, "a", 10, 2)
	c := &collector{}
	stop := run(ctx, js, c)
	waitFor(t, 30*time.Second, "the first 10 events", func() bool { return c.maxCovered() >= maxSeq(acks) })
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	more := publishConcurrent(ctx, t, js, "b", 300, 8)
	acks = append(acks, more...)
	assertLimitReached(t, js, acks)
	stop = run(ctx, js, c)
	waitFor(t, 60*time.Second, "the resumed subscriber to catch up", func() bool { return c.maxCovered() >= maxSeq(acks) })
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	recs := c.snapshot()
	handled, gapped := assertCoverage(t, recs, acks, 1)
	if gapped == 0 || countGaps(recs) != 1 {
		t.Fatalf("expected exactly one gap marker after the stop, got %d marking %d sequences", countGaps(recs), gapped)
	}
	t.Logf("RESULT stopped then evicted: handled %d, one gap marker covering %d sequences, of %d acknowledged", handled, gapped, len(acks))
}

func TestGap_Durable_StoppedThenEvicted_MaxMsgs(t *testing.T) {
	stoppedThenEvicted(t, func(ctx context.Context, js jetstream.JetStream, c *collector) func() error {
		d, err := evlayer.NewDurable(ctx, js, evlayer.DurableConfig{Stream: streamName, Durable: "resume", AckWait: 5 * time.Second, MaxAckPending: 64, Batch: 8})
		if err != nil {
			t.Fatal(err)
		}
		rctx, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- d.Run(rctx, c.handle) }()
		return func() error { cancel(); return <-done }
	})
}

func TestGap_Tail_StoppedThenEvicted_MaxMsgs(t *testing.T) {
	cur := evlayer.FileCursor{Path: scratchDir(t, "cursor") + "/cursor.json"}
	stoppedThenEvicted(t, func(ctx context.Context, js jetstream.JetStream, c *collector) func() error {
		rctx, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() {
			done <- evlayer.Tail(rctx, js, evlayer.TailConfig{Stream: streamName, Store: cur}, c.handle)
		}()
		return func() error { cancel(); return <-done }
	})
}

// A subscriber stops; every message expires by age; on restart it must be told, with no new event needed.
func allExpired(t *testing.T, run func(ctx context.Context, js jetstream.JetStream, c *collector) (stop func() error)) {
	n, _ := inProcNode(t, jsnode.SyncAlways)
	js := connect(t, n.URL())
	ctx := ctxT(t, 120*time.Second)
	if _, err := evlayer.EnsureStream(ctx, js, evlayer.StreamOptions{Name: streamName, MaxAge: time.Second}); err != nil {
		t.Fatal(err)
	}
	acks := publishSeq(ctx, t, js, "a", 5, 2)
	c := &collector{}
	stop := run(ctx, js, c)
	waitFor(t, 30*time.Second, "the first 5 events", func() bool { return c.maxCovered() >= maxSeq(acks) })
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	more := publishSeq(ctx, t, js, "b", 40, 2)
	acks = append(acks, more...)
	time.Sleep(4 * time.Second) // everything is now older than MaxAge
	st, _ := js.Stream(ctx, streamName)
	si, err := st.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if si.State.Msgs != 0 || si.State.FirstSeq != maxSeq(acks)+1 {
		t.Fatalf("age limit not reached: stream still holds %d messages, first seq %d (last acked %d)", si.State.Msgs, si.State.FirstSeq, maxSeq(acks))
	}
	stop = run(ctx, js, c)
	waitFor(t, 30*time.Second, "a gap marker with no new event published", func() bool { return countGaps(c.snapshot()) == 1 })
	e, _ := evlayer.NewEvent("after-gap", "allocation", "r0", "io.hpcburst.test.v1", time.Now(), nil)
	ack, err := evlayer.Publish(ctx, js, e)
	if err != nil {
		t.Fatal(err)
	}
	acks = append(acks, acked{Seq: ack.Sequence, ID: "after-gap", Resource: "allocation/r0"})
	waitFor(t, 30*time.Second, "the event published after the gap", func() bool { return c.maxCovered() >= ack.Sequence })
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	recs := c.snapshot()
	handled, gapped := assertCoverage(t, recs, acks, 1)
	t.Logf("RESULT all expired by MaxAge: handled %d, gap-marked %d (idle subscriber, marker before any new event)", handled, gapped)
}

func TestGap_Durable_AllExpired_MaxAge(t *testing.T) {
	allExpired(t, func(ctx context.Context, js jetstream.JetStream, c *collector) func() error {
		d, err := evlayer.NewDurable(ctx, js, evlayer.DurableConfig{Stream: streamName, Durable: "age", AckWait: 5 * time.Second, MaxAckPending: 64})
		if err != nil {
			t.Fatal(err)
		}
		rctx, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- d.Run(rctx, c.handle) }()
		return func() error { cancel(); return <-done }
	})
}

func TestGap_Tail_AllExpired_MaxAge(t *testing.T) {
	cur := evlayer.FileCursor{Path: scratchDir(t, "cursor") + "/cursor.json"}
	allExpired(t, func(ctx context.Context, js jetstream.JetStream, c *collector) func() error {
		rctx, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() {
			done <- evlayer.Tail(rctx, js, evlayer.TailConfig{Stream: streamName, Store: cur, IdleCheck: 200 * time.Millisecond}, c.handle)
		}()
		return func() error { cancel(); return <-done }
	})
}

// No false positive: without eviction there is no marker, however slow the subscriber.
func TestGap_NoFalsePositive_WithoutEviction(t *testing.T) {
	for _, kind := range []string{"durable", "tail"} {
		t.Run(kind, func(t *testing.T) {
			n, _ := inProcNode(t, jsnode.SyncAlways)
			js := connect(t, n.URL())
			ctx := ctxT(t, 60*time.Second)
			if _, err := evlayer.EnsureStream(ctx, js, evlayer.StreamOptions{Name: streamName}); err != nil {
				t.Fatal(err)
			}
			c := &collector{delay: 5 * time.Millisecond}
			rctx, cancel := context.WithCancel(ctx)
			done := make(chan error, 1)
			if kind == "durable" {
				d, err := evlayer.NewDurable(ctx, js, evlayer.DurableConfig{Stream: streamName, Durable: "nf", AckWait: 5 * time.Second, MaxAckPending: 64, Batch: 5})
				if err != nil {
					t.Fatal(err)
				}
				go func() { done <- d.Run(rctx, c.handle) }()
			} else {
				cur := evlayer.FileCursor{Path: scratchDir(t, "cursor") + "/cursor.json"}
				go func() {
					done <- evlayer.Tail(rctx, js, evlayer.TailConfig{Stream: streamName, Store: cur, IdleCheck: 100 * time.Millisecond}, c.handle)
				}()
			}
			acks := publishConcurrent(ctx, t, js, "n", 150, 4)
			waitFor(t, 30*time.Second, "the subscriber to drain", func() bool { return c.maxCovered() >= maxSeq(acks) })
			time.Sleep(600 * time.Millisecond) // give an idle check the chance to misfire
			cancel()
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			recs := c.snapshot()
			handled, gapped := assertCoverage(t, recs, acks, 1)
			if gapped != 0 || handled != len(acks) {
				t.Fatalf("expected %d events and no gap, got handled=%d gapped=%d", len(acks), handled, gapped)
			}
		})
	}
}
