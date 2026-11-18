package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
)

// Run runs every workload's sessions concurrently and returns every request's record, in the order they finished. Each session sends
// Turns streamed chat requests; the assistant's reply text is appended as an assistant message before the next user turn, and ThinkTime
// passes between a reply's end and the next request. A session whose request fails stops (its conversation has no reply to continue
// from); the others go on. sink, when set, is called with each record as it arrives, from one goroutine; if it fails the run is cancelled
// and its error returned with the records so far. apiKey may be empty.
func Run(ctx context.Context, cfg Config, apiKey string, sink func(Record) error) ([]Record, error) {
	return runWith(ctx, cfg, apiKey, sink, hooks{})
}

// hooks are the seams a test replaces; a nil field is the real thing.
type hooks struct {
	// startWait is how a session waits for its scheduled start: offset is j * StartStagger for the session's index j in its workload.
	// A test records the offsets, and returns at once, so the schedule is checked without a clock.
	startWait func(ctx context.Context, session string, offset time.Duration) bool
}

func runWith(ctx context.Context, cfg Config, apiKey string, sink func(Record) error, h hooks) ([]Record, error) {
	startWait := h.startWait
	if startWait == nil {
		startWait = func(ctx context.Context, _ string, offset time.Duration) bool { return sleep(ctx, offset) }
	}
	runID := cfg.RunID
	if runID == "" {
		var b [3]byte
		if _, err := rand.Read(b[:]); err != nil {
			return nil, fmt.Errorf("run id: %w", err)
		}
		runID = hex.EncodeToString(b[:])
	}
	parent := ctx
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	cl := newChatClient(cfg, apiKey)

	out := make(chan Record)
	var wg sync.WaitGroup
	for _, w := range cfg.Workloads {
		for j := 0; j < w.Sessions; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				id := fmt.Sprintf("%s-%s-%03d", runID, w.Name, j)
				if !startWait(ctx, id, time.Duration(j)*time.Duration(w.StartStagger)) {
					return
				}
				runSession(ctx, cl, w, id, out)
			}()
		}
	}
	go func() { wg.Wait(); close(out) }()

	var recs []Record
	var sinkErr error
	for rec := range out {
		recs = append(recs, rec)
		if sink != nil && sinkErr == nil {
			if sinkErr = sink(rec); sinkErr != nil {
				cancel()
			}
		}
	}
	if sinkErr != nil {
		return recs, fmt.Errorf("writing a record: %w", sinkErr)
	}
	return recs, parent.Err() // nil unless the caller stopped the run
}

// runSession is one conversation: user message, reply, think time, and so on, for w.Turns turns.
func runSession(ctx context.Context, cl *chatClient, w Workload, session string, out chan<- Record) {
	var msgs []chatMessage
	for turn := 1; turn <= w.Turns; turn++ {
		n := w.TurnTokens
		if turn == 1 {
			n = w.ContextTokens
		}
		msgs = append(msgs, chatMessage{Role: "user", Content: words(n, seedOf(session, turn))})
		reply, rec := cl.chat(ctx, w, session, turn, msgs)
		select {
		case out <- rec:
		case <-ctx.Done():
			return
		}
		if rec.Error != "" {
			return
		}
		msgs = append(msgs, chatMessage{Role: "assistant", Content: reply})
		if turn < w.Turns && !sleep(ctx, time.Duration(w.ThinkTime)) {
			return
		}
	}
}

// sleep waits d, or until ctx ends; it reports whether the full wait passed.
func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
