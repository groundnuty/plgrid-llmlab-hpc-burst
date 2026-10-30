package tests

import (
	"context"
	"errors"
	"os/exec"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s8/evlayer"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s8/jsnode"
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
// the server was SIGKILLed and restarted on the same store directory. The server
// writes a consumer's ack state at most every 100 ms, so a SIGKILL right after handling
// can roll the cursor back by that window: events are then delivered again (never lost).
// "settled" waits past that window before the kill; "immediate" does not.
func TestCursor_Durable_ResumesAfterSubscriberRestartAndServerKill(t *testing.T) {
	for _, tc := range []struct {
		name   string
		settle time.Duration
		bound  bool // repeats must stay inside the in-flight window
	}{{"settled", 600 * time.Millisecond, true}, {"immediate", 0, false}} {
		t.Run(tc.name, func(t *testing.T) {
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
			time.Sleep(tc.settle)

			host.Kill() // SIGKILL the server while the consumer has unacknowledged state
			_, url2 := startHost(t, dir+"/js", jsnode.SyncAlways)
			js2 := connect(t, url2)
			runUntil(t, c, 200, sub(js2))
			bound := 3 * cfg.Batch
			if !tc.bound {
				bound = 1 << 30
			}
			assertResumed(t, c.snapshot(), acks, bound)
		})
	}
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

	host.Kill()
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
