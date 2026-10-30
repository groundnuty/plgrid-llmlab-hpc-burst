package tests

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s8/evlayer"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s8/jsnode"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func uniqueIDs(recs []rec) map[string]int {
	m := map[string]int{}
	for _, r := range recs {
		if !r.Gap {
			m[r.ID]++
		}
	}
	return m
}

// assertResumed checks what a resumed subscriber must deliver: every acknowledged
// event at least once (no loss), repeats only inside the small in-flight window
// (at-least-once, so the handler de-duplicates by event id), and the first
// delivery of each event in per-resource publish order.
func assertResumed(t *testing.T, recs []rec, acks []acked, maxRepeats int) {
	t.Helper()
	seen := uniqueIDs(recs)
	repeats := 0
	for _, a := range acks {
		if seen[a.ID] == 0 {
			t.Errorf("event %s (stream seq %d) was acknowledged to the publisher and never delivered", a.ID, a.Seq)
		}
		repeats += seen[a.ID] - 1
	}
	if repeats > maxRepeats {
		t.Errorf("%d repeated deliveries across the restarts, more than the in-flight window of %d", repeats, maxRepeats)
	}
	if countGaps(recs) != 0 {
		t.Errorf("unexpected gap marker without eviction")
	}
	first := map[string]bool{}
	var firsts []rec
	for _, r := range recs {
		if !first[r.ID] {
			first[r.ID] = true
			firsts = append(firsts, r)
		}
	}
	assertExactlyOncePerResourceInOrder(t, firsts, acks)
	t.Logf("delivered %d events for %d acknowledged, %d repeats (in-flight replays), no gap", len(seen), len(acks), repeats)
}

// runUntil runs start() until enough records exist, then stops it.
func runUntil(t *testing.T, c *collector, n int, start func(ctx context.Context) <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := start(ctx)
	waitFor(t, 60*time.Second, "enough records", func() bool { return len(uniqueIDs(c.snapshot())) >= n })
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// Acceptance test 4a: a durable consumer resumes after a subscriber restart and after
// the server was SIGKILLed and restarted on the same store directory. The kill comes a second
// after the last handled event, longer than the server's throttle on consumer state writes, so
// the ack state is on disk and the repeats are only the events in flight at each restart. What
// the throttle does to a kill that comes sooner is a separate test with confirmed
// acknowledgements (TestCursor_AckStateLagsBehindConfirmedAcks).
func TestCursor_Durable_ResumesAfterSubscriberRestartAndServerKill(t *testing.T) {
	capacity(t)
	dir := scratchDir(t, "cursor-durable")
	host, url := startHost(t, dir+"/js", jsnode.SyncAlways)
	js := connect(t, url)
	ctx := ctxT(t, 120*time.Second)
	if _, err := evlayer.EnsureStream(ctx, js, evlayer.StreamOptions{Name: streamName}); err != nil {
		t.Fatal(err)
	}
	acks := publishSeq(ctx, t, js, "d1", 100, 4)
	c := &collector{}
	cfg := evlayer.DurableConfig{Stream: streamName, Durable: "cursor", AckWait: time.Second, MaxAckPending: 64, Batch: 8}
	sub := func(js jetstream.JetStream) func(ctx context.Context) <-chan error {
		return func(ctx context.Context) <-chan error {
			d, err := evlayer.NewDurable(ctx, js, cfg)
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- d.Run(ctx, c.handle) }()
			return done
		}
	}
	runUntil(t, c, 40, sub(js)) // subscriber 1 stops after 40
	runUntil(t, c, 100, sub(js))
	acks = append(acks, publishSeq(ctx, t, js, "d2", 100, 4)...)
	runUntil(t, c, 130, sub(js))
	time.Sleep(time.Second)

	if err := host.KillStrict(); err != nil { // SIGKILL the server while the consumer has unacknowledged state
		t.Fatal(err)
	}
	_, url2 := startHost(t, dir+"/js", jsnode.SyncAlways)
	js2 := connect(t, url2)
	runUntil(t, c, 200, sub(js2))
	assertResumed(t, c.snapshot(), acks, 3*cfg.Batch)
}

// lagTrial is one run of the consumer-state experiment.
type lagTrial struct {
	PreFloor, PreDelivered uint64   // server-reported ack state before the kill, after every ack was confirmed
	RecFloor, RecDelivered uint64   // the recovered consumer's state after the restart
	Deliveries             []uint64 // stream sequences delivered after the restart
}

// ackStateTrial acknowledges every message with a confirmed acknowledgement (DoubleAck waits for
// the server's reply, so the server has processed it), reads the consumer's state from the
// server, SIGKILLs the server after `settle`, restarts it on the same store and reads the state
// and the deliveries of the recovered consumer.
func ackStateTrial(t *testing.T, msgs int, settle time.Duration) lagTrial {
	t.Helper()
	capacity(t)
	dir := scratchDir(t, "lag")
	host, url := startHost(t, dir+"/js", jsnode.SyncAlways)
	js := connect(t, url)
	ctx := ctxT(t, 120*time.Second)
	if _, err := evlayer.EnsureStream(ctx, js, evlayer.StreamOptions{Name: streamName}); err != nil {
		t.Fatal(err)
	}
	publishSeq(ctx, t, js, "l", msgs, 4)
	ccfg := jetstream.ConsumerConfig{Durable: "lag", AckPolicy: jetstream.AckExplicitPolicy, AckWait: time.Second, MaxAckPending: 1000}
	cons, err := js.CreateOrUpdateConsumer(ctx, streamName, ccfg)
	if err != nil {
		t.Fatal(err)
	}
	b, err := cons.Fetch(msgs, jetstream.FetchMaxWait(10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for m := range b.Messages() {
		if err := m.DoubleAck(ctx); err != nil {
			t.Fatalf("confirmed acknowledgement %d: %v", n+1, err)
		}
		n++
	}
	if err := b.Error(); err != nil && !errors.Is(err, nats.ErrTimeout) {
		t.Fatal(err)
	}
	if n != msgs {
		t.Fatalf("setup: fetched and acknowledged %d of %d messages", n, msgs)
	}
	ci, err := cons.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var tr lagTrial
	tr.PreFloor, tr.PreDelivered = ci.AckFloor.Stream, ci.Delivered.Stream
	if tr.PreFloor != uint64(msgs) || ci.NumAckPending != 0 {
		t.Fatalf("setup: after %d confirmed acknowledgements the server reports ack floor %d and %d pending", msgs, tr.PreFloor, ci.NumAckPending)
	}

	time.Sleep(settle)
	if err := host.KillStrict(); err != nil {
		t.Fatal(err)
	}
	_, url2 := startHost(t, dir+"/js", jsnode.SyncAlways)
	js2 := connect(t, url2)
	cons2, err := js2.Consumer(ctx, streamName, "lag")
	if err != nil {
		t.Fatal(err)
	}
	ci2, err := cons2.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	tr.RecFloor, tr.RecDelivered = ci2.AckFloor.Stream, ci2.Delivered.Stream
	// Messages the recovered state still holds as pending come back after the ack wait (1 s).
	for quiet := 0; quiet < 2; { // pull until two fetches in a row return nothing
		b, err := cons2.Fetch(msgs, jetstream.FetchMaxWait(1500*time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		got := 0
		for m := range b.Messages() {
			md, err := m.Metadata()
			if err != nil {
				t.Fatal(err)
			}
			tr.Deliveries = append(tr.Deliveries, md.Sequence.Stream)
			if err := m.DoubleAck(ctx); err != nil { // so that it is not delivered a second time during the pull
				t.Fatalf("acknowledging redelivered sequence %d: %v", md.Sequence.Stream, err)
			}
			got++
		}
		if err := b.Error(); err != nil && !errors.Is(err, nats.ErrTimeout) {
			t.Fatal(err)
		}
		if got == 0 {
			quiet++
		} else {
			quiet = 0
		}
	}
	return tr
}

// assertRecovered checks what must hold after any kill: the recovered consumer is never ahead of
// what was confirmed, and it delivers exactly the sequences above its recovered ack floor, each
// once (nothing is lost, and everything it re-delivers is in the replay window).
func assertRecovered(t *testing.T, tr lagTrial, msgs int) (replayed int) {
	t.Helper()
	if tr.RecFloor > tr.PreFloor {
		t.Errorf("recovered ack floor %d is ahead of the %d the server confirmed", tr.RecFloor, tr.PreFloor)
	}
	seen := map[uint64]int{}
	for _, s := range tr.Deliveries {
		seen[s]++
	}
	for s := tr.RecFloor + 1; s <= uint64(msgs); s++ {
		if seen[s] != 1 {
			t.Errorf("sequence %d above the recovered floor %d was delivered %d times after the restart, want once", s, tr.RecFloor, seen[s])
		}
	}
	for s := range seen {
		if s <= tr.RecFloor || s > uint64(msgs) {
			t.Errorf("sequence %d was delivered after the restart but is outside %d..%d", s, tr.RecFloor+1, msgs)
		}
		if s <= tr.PreFloor {
			replayed++ // acknowledged and confirmed before the kill, delivered again
		}
	}
	return replayed
}

// Finding 4, isolated. Every message is acknowledged with a confirmed acknowledgement and the
// consumer's state is read from the server before the kill; the recovered state and deliveries
// are then compared with it. The server writes consumer state at most every 100 ms
// (flushLoop in nats-server filestore.go), so a kill inside that window recovers an older state
// and the already-acknowledged tail is delivered again. A kill after the state caught up is the
// control: it must roll nothing back, or the immediate result would not come from the throttle.
func TestCursor_AckStateLagsBehindConfirmedAcks(t *testing.T) {
	const msgs = 60
	immediate, settled := envInt("S8_LAG_IMMEDIATE", 8), envInt("S8_LAG_SETTLED", 3)
	t.Run("settled_control", func(t *testing.T) {
		for i := 0; i < settled; i++ {
			tr := ackStateTrial(t, msgs, time.Second)
			replayed := assertRecovered(t, tr, msgs)
			if tr.RecFloor != tr.PreFloor || replayed != 0 {
				t.Errorf("trial %d: after a second's pause the recovered floor is %d (confirmed %d) and %d messages were delivered again, want no rollback", i, tr.RecFloor, tr.PreFloor, replayed)
			}
		}
		t.Logf("RESULT control: %d trials killed one second after the last confirmed acknowledgement: no rollback, no replay", settled)
	})
	t.Run("immediate", func(t *testing.T) {
		rolled, replayedTotal := 0, 0
		var detail []string
		for i := 0; i < immediate; i++ {
			tr := ackStateTrial(t, msgs, 0)
			replayed := assertRecovered(t, tr, msgs)
			if tr.RecFloor < tr.PreFloor {
				rolled++
			}
			if want := int(tr.PreFloor - tr.RecFloor); replayed != want {
				t.Errorf("trial %d: %d confirmed messages were delivered again but the recovered floor %d implies %d", i, replayed, tr.RecFloor, want)
			}
			replayedTotal += replayed
			detail = append(detail, fmt.Sprintf("%d->%d", tr.PreFloor, tr.RecFloor))
		}
		if rolled == 0 {
			t.Fatalf("no rollback in %d immediate trials (confirmed floor -> recovered floor: %v): the consumer-state lag was not observed with confirmed acknowledgements", immediate, detail)
		}
		t.Logf("RESULT immediate kill after confirmed acknowledgements: rollback in %d of %d trials, %d confirmed messages delivered again in total (confirmed floor -> recovered floor: %v)", rolled, immediate, replayedTotal, detail)
	})
}

// Acceptance test 4b: a subscriber with a stored cursor resumes where it stopped,
// across a subscriber restart and a SIGKILL of the server.
func TestCursor_Tail_ResumesAfterSubscriberRestartAndServerKill(t *testing.T) {
	capacity(t)
	dir := scratchDir(t, "cursor-tail")
	host, url := startHost(t, dir+"/js", jsnode.SyncAlways)
	js := connect(t, url)
	ctx := ctxT(t, 120*time.Second)
	if _, err := evlayer.EnsureStream(ctx, js, evlayer.StreamOptions{Name: streamName}); err != nil {
		t.Fatal(err)
	}
	acks := publishSeq(ctx, t, js, "t1", 100, 4)
	c := &collector{delay: time.Millisecond}
	store := evlayer.FileCursor{Path: dir + "/cursor.json"}
	sub := func(js jetstream.JetStream) func(ctx context.Context) <-chan error {
		return func(ctx context.Context) <-chan error {
			done := make(chan error, 1)
			go func() { done <- evlayer.Tail(ctx, js, evlayer.TailConfig{Stream: streamName, Store: store}, c.handle) }()
			return done
		}
	}
	runUntil(t, c, 40, sub(js))
	cur, err := store.Load()
	if err != nil || cur.Seq < 40 {
		t.Fatalf("stored cursor after the first run: %+v, %v", cur, err)
	}
	runUntil(t, c, 100, sub(js))
	acks = append(acks, publishSeq(ctx, t, js, "t2", 100, 4)...)
	runUntil(t, c, 130, sub(js))

	if err := host.KillStrict(); err != nil {
		t.Fatal(err)
	}
	_, url2 := startHost(t, dir+"/js", jsnode.SyncAlways)
	js2 := connect(t, url2)
	runUntil(t, c, 200, sub(js2))
	assertResumed(t, c.snapshot(), acks, 3)
}

func copyDir(t *testing.T, from, to string) {
	t.Helper()
	if out, err := exec.Command("rm", "-rf", to).CombinedOutput(); err != nil {
		t.Fatalf("rm: %v: %s", err, out)
	}
	if out, err := exec.Command("cp", "-a", from, to).CombinedOutput(); err != nil {
		t.Fatalf("cp: %v: %s", err, out)
	}
}

// The failure the default fsync policy risks is acknowledged writes lost after a power
// failure. SIGKILL cannot cause it, so the lost tail is emulated by restoring an older
// copy of the store directory. A stored cursor must turn that into an error, not silence.
func TestCursor_Tail_DetectsLostAcknowledgedTail(t *testing.T) {
	for _, tc := range []struct {
		name       string
		refill     int // events published after the restore
		wantDetail string
	}{
		{"stream_ends_before_cursor", 0, "ends at"},
		{"same_sequences_new_events", 40, "holds event"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			capacity(t)
			dir := scratchDir(t, "cursor-lost")
			store := evlayer.FileCursor{Path: dir + "/cursor.json"}
			ctx := ctxT(t, 120*time.Second)

			host, url := startHost(t, dir+"/js", jsnode.SyncAlways)
			js := connect(t, url)
			if _, err := evlayer.EnsureStream(ctx, js, evlayer.StreamOptions{Name: streamName}); err != nil {
				t.Fatal(err)
			}
			publishSeq(ctx, t, js, "x", 30, 3)
			host.Stop(30 * time.Second)
			copyDir(t, dir+"/js", dir+"/snapshot30") // the disk state "before the power failure"

			host, url = startHost(t, dir+"/js", jsnode.SyncAlways)
			js = connect(t, url)
			acks := publishSeq(ctx, t, js, "y", 20, 3) // sequences 31..50, acknowledged
			if maxSeq(acks) != 50 {
				t.Fatalf("setup: expected sequence 50, got %d", maxSeq(acks))
			}
			c := &collector{}
			runUntil(t, c, 50, func(rctx context.Context) <-chan error {
				done := make(chan error, 1)
				go func() { done <- evlayer.Tail(rctx, js, evlayer.TailConfig{Stream: streamName, Store: store}, c.handle) }()
				return done
			})
			host.Stop(30 * time.Second)
			copyDir(t, dir+"/snapshot30", dir+"/js") // power failure: sequences 31..50 are gone

			_, url = startHost(t, dir+"/js", jsnode.SyncAlways)
			js = connect(t, url)
			if tc.refill > 0 {
				publishSeq(ctx, t, js, "z", tc.refill, 3) // sequences 31..70, different ids
			}
			err := evlayer.Tail(ctxT(t, 30*time.Second), js, evlayer.TailConfig{Stream: streamName, Store: store}, c.handle)
			if !errors.Is(err, evlayer.ErrHistoryRewritten) {
				t.Fatalf("expected ErrHistoryRewritten, got %v", err)
			}
			t.Logf("RESULT detected: %v", err)
		})
	}
}
