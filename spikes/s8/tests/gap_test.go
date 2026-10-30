package tests

import (
	"context"
	"errors"
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

// assertRetainedSuffixDelivered is the oracle that coverage alone cannot give. Once publishing
// has stopped, the stream holds exactly the top `limit` acknowledged sequences, and a
// subscriber must have handed every one of them over as an event, once. An implementation
// that marked retained messages as a gap would still cover every sequence.
func assertRetainedSuffixDelivered(t *testing.T, js jetstream.JetStream, recs []rec, acks []acked, limit int) {
	t.Helper()
	last := maxSeq(acks)
	first := last - uint64(limit) + 1
	// The expected suffix comes from the publisher's acknowledgements; check it against the stream.
	st, err := js.Stream(ctxT(t, 10*time.Second), streamName)
	if err != nil {
		t.Fatal(err)
	}
	si, err := st.Info(ctxT(t, 10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if si.State.FirstSeq != first || si.State.LastSeq != last || si.State.Msgs != uint64(limit) {
		t.Fatalf("oracle wrong: stream holds %d messages %d..%d, the acknowledgements say %d..%d", si.State.Msgs, si.State.FirstSeq, si.State.LastSeq, first, last)
	}
	handled := map[string]int{}
	for _, r := range recs {
		if !r.Gap {
			handled[r.ID]++
		}
	}
	for _, a := range acks {
		if a.Seq >= first && handled[a.ID] != 1 {
			t.Errorf("retained event %s (sequence %d of the final %d..%d) was handled %d times, want once", a.ID, a.Seq, first, last, handled[a.ID])
		}
	}
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
	assertRetainedSuffixDelivered(t, js, recs, acks, gapLimit)
	t.Logf("RESULT slow durable subscriber: handled %d, gap-marked %d sequences in %d markers, of %d acknowledged", handled, gapped, countGaps(recs), len(acks))
}

// slowTail runs a stored-cursor subscriber with a slow handler while the publisher
// outruns retention, and checks the coverage contract.
func slowTail(t *testing.T, pull int) (handled, gapped, markers int, acks []acked) {
	n, _ := inProcNode(t, jsnode.SyncAlways)
	js := connect(t, n.URL())
	ctx := ctxT(t, 120*time.Second)
	if _, err := evlayer.EnsureStream(ctx, js, evlayer.StreamOptions{Name: streamName, MaxMsgs: gapLimit}); err != nil {
		t.Fatal(err)
	}
	cur := evlayer.FileCursor{Path: scratchDir(t, "cursor") + "/cursor.json"}
	c := &collector{delay: 15 * time.Millisecond}
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- evlayer.Tail(runCtx, js, evlayer.TailConfig{Stream: streamName, Store: cur, PullMaxMessages: pull}, c.handle)
	}()
	acks = publishConcurrent(ctx, t, js, "s", 600, 8)
	assertLimitReached(t, js, acks)
	waitFor(t, 90*time.Second, "the subscriber to drain", func() bool { return c.maxCovered() >= maxSeq(acks) })
	stop()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	recs := c.snapshot()
	handled, gapped = assertCoverage(t, recs, acks, 1)
	assertRetainedSuffixDelivered(t, js, recs, acks, gapLimit)
	return handled, gapped, countGaps(recs), acks
}

// Acceptance test 2 (stored cursor): a slow live subscriber is overtaken by retention.
// The client holds only 5 messages ahead, as the durable test does with Batch 5.
func TestGap_Tail_SlowSubscriber_MaxMsgs(t *testing.T) {
	handled, gapped, markers, acks := slowTail(t, 5)
	if gapped == 0 {
		t.Fatal("retention evicted unread messages but the subscriber saw no gap marker")
	}
	t.Logf("RESULT slow stored-cursor subscriber (5 ahead): handled %d, gap-marked %d sequences in %d markers, of %d acknowledged", handled, gapped, markers, len(acks))
}

// Finding: with the default client buffer (500 messages) a slow subscriber has already
// pulled most of the stream into memory when retention evicts it, so it sees few or no
// gaps while coverage still holds. Nothing is lost; the eviction simply has not reached it.
func TestGap_Tail_SlowSubscriber_DefaultBuffer_Finding(t *testing.T) {
	handled, gapped, markers, acks := slowTail(t, 0)
	t.Logf("RESULT slow stored-cursor subscriber (default buffer of 500): handled %d, gap-marked %d sequences in %d markers, of %d acknowledged; coverage exact", handled, gapped, markers, len(acks))
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
	// Coverage accepts any contiguous gap, so also pin the exact one. Ten events were handled
	// before the stop (1..10), 310 were published, the stream keeps the last 50 (261..310):
	// the gap is 11..260 and the 50 retained events are all delivered.
	const handledBeforeStop, published = 10, 310
	if last := maxSeq(acks); last != published {
		t.Fatalf("setup: the last acknowledged sequence is %d, want %d", last, published)
	}
	wantFrom, wantTo := uint64(handledBeforeStop+1), uint64(published-gapLimit)
	for _, r := range recs {
		if r.Gap && (r.From != wantFrom || r.To != wantTo) {
			t.Errorf("gap marker %d..%d, want %d..%d", r.From, r.To, wantFrom, wantTo)
		}
	}
	assertRetainedSuffixDelivered(t, js, recs, acks, gapLimit)
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

// publishOn publishes n events one after another on one resource and returns the acknowledgements.
func publishOn(ctx context.Context, t *testing.T, js jetstream.JetStream, tag, name string, n int) []acked {
	t.Helper()
	var out []acked
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("%s-%d", tag, i)
		e, err := evlayer.NewEvent(id, "allocation", name, "io.hpcburst.test.v1", time.Now(), nil)
		if err != nil {
			t.Fatal(err)
		}
		ack, err := evlayer.Publish(ctx, js, e)
		if err != nil {
			t.Fatalf("publish %s: %v", id, err)
		}
		out = append(out, acked{Seq: ack.Sequence, ID: id, Resource: evlayer.ResourceKey("allocation", name)})
	}
	return out
}

// assertOneGapThenSuffix checks the exact outcome of a subscriber that had handled nothing when
// retention overtook it: one marker for 1..last-limit, then every retained event once.
func assertOneGapThenSuffix(t *testing.T, js jetstream.JetStream, recs []rec, acks []acked, limit int) {
	t.Helper()
	assertCoverage(t, recs, acks, 1)
	wantTo := maxSeq(acks) - uint64(limit)
	if countGaps(recs) != 1 || !recs[0].Gap || recs[0].From != 1 || recs[0].To != wantTo {
		t.Fatalf("records %+v: want one gap marker 1..%d first, then the %d retained events", recs, wantTo, limit)
	}
	assertRetainedSuffixDelivered(t, js, recs, acks, limit)
}

// A subscription's starting boundary is fixed and stored when it starts, not when its first
// handler call succeeds. If it is only kept after a success, a subscription whose first events
// are evicted before any succeeded restarts as a new one and skips them without a marker.
func TestGap_Tail_BoundarySurvivesEvictionBeforeTheFirstSuccess(t *testing.T) {
	for _, tc := range []struct {
		name       string
		firstBatch int // events published before the subscriber starts
	}{
		{"handler_fails_on_the_first_event", 5},
		{"stopped_before_any_event_arrived", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n, _ := inProcNode(t, jsnode.SyncAlways)
			js := connect(t, n.URL())
			ctx := ctxT(t, 120*time.Second)
			if _, err := evlayer.EnsureStream(ctx, js, evlayer.StreamOptions{Name: streamName, MaxMsgs: gapLimit}); err != nil {
				t.Fatal(err)
			}
			var acks []acked
			if tc.firstBatch > 0 {
				acks = publishSeq(ctx, t, js, "e", tc.firstBatch, 2)
			}
			cur := evlayer.FileCursor{Path: scratchDir(t, "cursor") + "/cursor.json"}
			tail := func(c *collector) (done chan error, cancel context.CancelFunc) {
				rctx, cancel := context.WithCancel(ctx)
				done = make(chan error, 1)
				go func() {
					done <- evlayer.Tail(rctx, js, evlayer.TailConfig{Stream: streamName, Store: cur, IdleCheck: 100 * time.Millisecond}, c.handle)
				}()
				return done, cancel
			}

			// First run: no handler call ever succeeds.
			first := &collector{}
			if tc.firstBatch > 0 {
				first.failOnce = map[string]bool{acks[0].ID: true}
			}
			done, cancel := tail(first)
			if tc.firstBatch > 0 {
				select {
				case err := <-done: // the failing handler stops the subscriber with its error
					if err == nil {
						t.Fatal("the subscriber ended without an error although its handler failed")
					}
				case <-time.After(10 * time.Second):
					t.Fatal("the subscriber did not stop at its failing handler")
				}
			} else {
				waitFor(t, 10*time.Second, "the starting boundary to be stored", func() bool { c, err := cur.Load(); return err == nil && c.Started })
				cancel()
				if err := <-done; err != nil {
					t.Fatal(err)
				}
			}
			cancel()
			if got := first.snapshot(); len(got) != 0 {
				t.Fatalf("setup: %d records were handled in the first run, want none", len(got))
			}
			if c, err := cur.Load(); err != nil || !c.Started || c.Seq != 0 {
				t.Fatalf("stored boundary after the first run: %+v, %v; want a started subscription at sequence 0", c, err)
			}

			// Retention evicts everything the subscriber was owed.
			acks = append(acks, publishConcurrent(ctx, t, js, "m", 200, 8)...)
			assertLimitReached(t, js, acks)

			second := &collector{}
			done2, cancel2 := tail(second)
			waitFor(t, 60*time.Second, "the restarted subscriber to catch up", func() bool { return second.maxCovered() >= maxSeq(acks) })
			cancel2()
			if err := <-done2; err != nil {
				t.Fatal(err)
			}
			assertOneGapThenSuffix(t, js, second.snapshot(), acks, gapLimit)
		})
	}
}

// The same for a durable subscriber: the consumer exists on the server before any delivery,
// and retention overtakes it before its first fetch.
func TestGap_Durable_BoundarySurvivesEvictionBeforeTheFirstDelivery(t *testing.T) {
	n, _ := inProcNode(t, jsnode.SyncAlways)
	js := connect(t, n.URL())
	ctx := ctxT(t, 120*time.Second)
	if _, err := evlayer.EnsureStream(ctx, js, evlayer.StreamOptions{Name: streamName, MaxMsgs: gapLimit}); err != nil {
		t.Fatal(err)
	}
	cfg := evlayer.DurableConfig{Stream: streamName, Durable: "early", AckWait: 5 * time.Second, MaxAckPending: 64, Batch: 8}
	if _, err := evlayer.NewDurable(ctx, js, cfg); err != nil { // creates the consumer; it never runs
		t.Fatal(err)
	}
	acks := publishConcurrent(ctx, t, js, "m", 200, 8)
	assertLimitReached(t, js, acks)

	d, err := evlayer.NewDurable(ctx, js, cfg) // attaches to the existing consumer
	if err != nil {
		t.Fatal(err)
	}
	c := &collector{}
	rctx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- d.Run(rctx, c.handle) }()
	waitFor(t, 60*time.Second, "the subscriber to catch up", func() bool { return c.maxCovered() >= maxSeq(acks) })
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	assertOneGapThenSuffix(t, js, c.snapshot(), acks, gapLimit)
}

// The boundary must not be too wide either: what was evicted before a subscription ever
// started is not owed to it, so a fresh subscriber on a stream that has already evicted
// reports no marker.
func TestGap_FreshSubscriptionIsNotOwedWhatWasEvictedBeforeItStarted(t *testing.T) {
	for _, kind := range []string{"durable", "tail"} {
		t.Run(kind, func(t *testing.T) {
			n, _ := inProcNode(t, jsnode.SyncAlways)
			js := connect(t, n.URL())
			ctx := ctxT(t, 120*time.Second)
			if _, err := evlayer.EnsureStream(ctx, js, evlayer.StreamOptions{Name: streamName, MaxMsgs: gapLimit}); err != nil {
				t.Fatal(err)
			}
			acks := publishConcurrent(ctx, t, js, "old", 200, 8)
			assertLimitReached(t, js, acks)
			c := &collector{}
			rctx, cancel := context.WithCancel(ctx)
			done := make(chan error, 1)
			if kind == "durable" {
				d, err := evlayer.NewDurable(ctx, js, evlayer.DurableConfig{Stream: streamName, Durable: "fresh", AckWait: 5 * time.Second, MaxAckPending: 64})
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
			waitFor(t, 30*time.Second, "the subscriber to catch up", func() bool { return c.maxCovered() >= maxSeq(acks) })
			time.Sleep(400 * time.Millisecond) // give an idle check the chance to misfire
			cancel()
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			recs := c.snapshot()
			if countGaps(recs) != 0 {
				t.Fatalf("a fresh subscriber was given a gap marker for history evicted before it started: %+v", recs[0])
			}
			assertRetainedSuffixDelivered(t, js, recs, acks, gapLimit)
		})
	}
}

// Documented limitation of the durable subscriber, pinned so that a change shows up: the gap
// tracker advances when a message is delivered, not when its handler succeeds, and the
// re-sequencer reads only retained predecessors. An event whose handler keeps failing until
// retention evicts it therefore disappears without a handled record and without a marker, while
// the subscriber stays up. (The stored-cursor subscriber stops at the failing event instead.)
func TestFinding_Durable_FailedThenEvictedEventLeavesNoMarkerWhileRunning(t *testing.T) {
	n, _ := inProcNode(t, jsnode.SyncAlways)
	js := connect(t, n.URL())
	ctx := ctxT(t, 120*time.Second)
	if _, err := evlayer.EnsureStream(ctx, js, evlayer.StreamOptions{Name: streamName, MaxMsgs: gapLimit}); err != nil {
		t.Fatal(err)
	}
	early := publishSeq(ctx, t, js, "f", 5, 5) // sequences 1..5, one resource each
	victim := early[0]
	c := &collector{}
	var mu sync.Mutex
	attempts := 0
	handle := func(ctx context.Context, r evlayer.Record) error {
		if r.Gap == nil && r.Event.ID() == victim.ID {
			mu.Lock()
			attempts++
			mu.Unlock()
			return errors.New("handler failure that outlasts retention")
		}
		return c.handle(ctx, r)
	}
	// The NAK delay is longer than the test: the failed event waits until retention evicts it.
	d, err := evlayer.NewDurable(ctx, js, evlayer.DurableConfig{Stream: streamName, Durable: "fail", AckWait: 5 * time.Second, MaxAckPending: 64, Batch: 8, NakDelay: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	rctx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- d.Run(rctx, handle) }()
	waitFor(t, 10*time.Second, "the other four early events", func() bool { return len(c.snapshot()) == 4 })

	acks := append(early, publishOn(ctx, t, js, "late", "other", 200)...)
	assertLimitReached(t, js, acks)
	waitFor(t, 60*time.Second, "the subscriber to reach the last event", func() bool { return c.maxCovered() >= maxSeq(acks) })
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if attempts != 1 {
		t.Fatalf("setup: the failing event was handled %d times, want exactly one failed attempt", attempts)
	}
	covered := map[uint64]int{} // sequence -> how many records (events or markers) cover it
	for _, r := range c.snapshot() {
		if r.Gap {
			for s := r.From; s <= r.To; s++ {
				covered[s]++
			}
		} else {
			covered[r.Seq]++
		}
	}
	if covered[victim.Seq] != 0 {
		t.Fatalf("the failed, evicted event (sequence %d) was reported after all (%d record(s)): the documented limitation no longer holds, update the README", victim.Seq, covered[victim.Seq])
	}
	for s := victim.Seq + 1; s <= maxSeq(acks); s++ {
		if covered[s] != 1 {
			t.Errorf("sequence %d is covered %d times, want once", s, covered[s])
		}
	}
	t.Logf("RESULT limitation: sequence %d failed once and was evicted while the subscriber ran; no record and no marker mentions it (all of %d..%d are covered once)", victim.Seq, victim.Seq+1, maxSeq(acks))
}
