package main

// Behaviours of Run beyond the two-workload row: session starts staggered, a failed turn stopping only its own session, and the first
// token not coming before the engine's own delay. Each is a lower bound or a count, none a window.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/enginedouble"
)

func mustConfig(t *testing.T, url, workload string) Config {
	t.Helper()
	c, err := ParseConfig([]byte(fmt.Sprintf(`{"baseURL": %q, "model": "model", "runId": "r", "workloads": [%s]}`, url+"/v1", workload)))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// Session j starts j*100ms after the run, so the k-th earliest arrival at the engine is at least k*100ms after the time taken before Run:
// a lower bound that a slow machine only makes safer. A generator that ignored the stagger would send all four at once.
func TestStartStaggerSpacesTheSessionStarts(t *testing.T) {
	var mu sync.Mutex
	arrivals := map[string]time.Time{}
	inner := enginedouble.Handler(enginedouble.DefaultConfig())
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		now := time.Now()
		mu.Lock()
		if _, ok := arrivals[r.Header.Get("x-session-id")]; !ok {
			arrivals[r.Header.Get("x-session-id")] = now
		}
		mu.Unlock()
		inner.ServeHTTP(w, r)
	}))
	defer srv.Close()
	cfg := mustConfig(t, srv.URL, `{"name": "s", "sessions": 4, "turns": 1, "contextTokens": 5, "outputTokens": 3, "startStagger": "100ms"}`)
	before := time.Now()
	recs, err := Run(context.Background(), cfg, "", nil)
	srv.Close()
	if err != nil || len(recs) != 4 {
		t.Fatalf("%d records, %v, want 4", len(recs), err)
	}
	var at []time.Time
	for _, a := range arrivals {
		at = append(at, a)
	}
	slices.SortFunc(at, func(a, b time.Time) int { return a.Compare(b) })
	if len(at) != 4 {
		t.Fatalf("%d sessions reached the engine, want 4", len(at))
	}
	for k, a := range at {
		if gap := a.Sub(before); gap < time.Duration(k)*100*time.Millisecond {
			t.Errorf("the %d-th earliest session arrived %v after the run began, want at least %v", k, gap, time.Duration(k)*100*time.Millisecond)
		}
	}
}

// One session's turn 2 fails: that session stops after the failed request (2 records, the second an error), and the other session, with
// no failure, sends all 3 turns.
func TestAFailedTurnStopsOnlyItsOwnSession(t *testing.T) {
	var mu sync.Mutex
	seen := map[string]int{}
	inner := enginedouble.Handler(enginedouble.DefaultConfig())
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(raw))
		var body struct {
			Messages []json.RawMessage `json:"messages"`
		}
		_ = json.Unmarshal(raw, &body)
		session := r.Header.Get("x-session-id")
		mu.Lock()
		seen[session]++
		mu.Unlock()
		if strings.HasSuffix(session, "-000") && len(body.Messages) == 3 { // turn 2 of the first session
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		inner.ServeHTTP(w, r)
	}))
	defer srv.Close()
	cfg := mustConfig(t, srv.URL, `{"name": "w", "sessions": 2, "turns": 3, "contextTokens": 5, "turnTokens": 2, "outputTokens": 3}`)
	recs, err := Run(context.Background(), cfg, "", nil)
	srv.Close()
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 5 { // 2 + 3
		t.Errorf("%d records, want 5", len(recs))
	}
	if seen["r-w-000"] != 2 || seen["r-w-001"] != 3 {
		t.Errorf("requests reaching the engine per session: %v, want r-w-000: 2 and r-w-001: 3", seen)
	}
	errs := 0
	for _, r := range recs {
		if r.Error != "" {
			errs++
			if r.Session != "r-w-000" || r.Turn != 2 || r.Error != "status 500" {
				t.Errorf("failed record %s turn %d %q, want r-w-000 turn 2 status 500", r.Session, r.Turn, r.Error)
			}
		}
	}
	if errs != 1 {
		t.Errorf("%d failed records, want 1", errs)
	}
}

// The double waits 20 ms before each token, so a first token cannot be seen sooner than 20 ms after the request began: a lower bound.
func TestTimeToFirstTokenIsNotBelowTheEnginesDelay(t *testing.T) {
	ec := enginedouble.DefaultConfig()
	ec.TokenDelay = 20 * time.Millisecond
	srv := httptest.NewServer(enginedouble.Handler(ec))
	defer srv.Close()
	cfg := mustConfig(t, srv.URL, `{"name": "w", "sessions": 1, "turns": 1, "contextTokens": 5, "outputTokens": 6}`)
	recs, err := Run(context.Background(), cfg, "", nil)
	if err != nil || len(recs) != 1 || recs[0].Error != "" {
		t.Fatalf("%v %+v", err, recs)
	}
	r := recs[0]
	if r.TTFTMs == nil || *r.TTFTMs < 20 || r.End.Sub(r.Start) < 6*20*time.Millisecond || r.CompletionTokens == nil || *r.CompletionTokens != 6 {
		t.Errorf("ttft %v, duration %v, completion %v: want ttft at least 20ms, duration at least 120ms, 6 tokens", r.TTFTMs, r.End.Sub(r.Start), r.CompletionTokens)
	}
}

// Cancelling the run ends it with the records so far and the cancellation as the error; nothing is left sending.
func TestACancelledRunReturnsWhatItHas(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{}, 4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started <- struct{}{}
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(release)
	cfg := mustConfig(t, srv.URL, `{"name": "w", "sessions": 4, "turns": 2, "contextTokens": 5, "turnTokens": 2, "outputTokens": 3}`)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := Run(ctx, cfg, "", nil); done <- err }()
	for range 4 {
		<-started // every session has a request in flight
	}
	cancel()
	if err := <-done; err != context.Canceled {
		t.Errorf("Run returned %v, want context.Canceled", err)
	}
}
