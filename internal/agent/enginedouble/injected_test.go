package enginedouble

// The engine's log says what the modes made a request wait (InjectedNS) and when a request is in
// flight ("admitted"), so a test that needs "each request slower than the one before" or "the request
// is in flight now" reads the double's own record and does not guess from a clock. The expected waits
// are computed here from the documented formulas: slow-first waits its duration before the first
// request of a shape, and never-settle waits n times its percent of TokenDelay * tokens before the
// n-th (from 0) request of a shape.

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// recorded serves cfg and collects the log events the double writes.
type recorded struct {
	t   *testing.T
	url string
	mu  sync.Mutex
	evs []LogEvent
}

func serveRecorded(t *testing.T, cfg Config) *recorded {
	t.Helper()
	r := &recorded{t: t}
	srv := httptest.NewServer(newHandler(cfg, func(ev LogEvent) {
		r.mu.Lock()
		r.evs = append(r.evs, ev)
		r.mu.Unlock()
	}))
	t.Cleanup(srv.Close)
	r.url = srv.URL
	return r
}

func (r *recorded) events(event string) []LogEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []LogEvent
	for _, ev := range r.evs {
		if ev.Event == event {
			out = append(out, ev)
		}
	}
	return out
}

// chat sends a request and waits for the whole answer.
func (r *recorded) chat(ctx context.Context, body map[string]any) error {
	b, err := json.Marshal(body)
	if err != nil {
		r.t.Fatal(err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.url+"/v1/chat/completions", bytes.NewReader(b))
	if err != nil {
		r.t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var sink bytes.Buffer
	_, err = sink.ReadFrom(resp.Body)
	return err
}

func shapeBody(words, max int) map[string]any {
	return chatBody(words, map[string]any{"max_tokens": max, "ignore_eos": true})
}

// never-settle=100 with TokenDelay 1 ms and 8 tokens: the n-th request of a shape waits n * 8 ms. A
// request of another shape starts again at 0, and each request's record holds exactly that wait.
func TestRequestRecordsTheWaitNeverSettleInjected(t *testing.T) {
	cfg := cfgFor()
	cfg.TokenDelay, cfg.NeverSettle = time.Millisecond, 100
	r := serveRecorded(t, cfg)
	for i := 0; i < 4; i++ {
		if err := r.chat(context.Background(), shapeBody(5, 8)); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.chat(context.Background(), shapeBody(6, 8)); err != nil { // another prompt length: another shape
		t.Fatal(err)
	}
	var got []time.Duration
	for _, ev := range r.events("request") {
		got = append(got, time.Duration(ev.InjectedNS))
	}
	want := []time.Duration{0, 8 * time.Millisecond, 16 * time.Millisecond, 24 * time.Millisecond, 0}
	if len(got) != len(want) {
		t.Fatalf("%d request records, want %d", len(got), len(want))
	}
	for i := range want {
		if d := got[i] - want[i]; d < -time.Microsecond || d > time.Microsecond {
			t.Errorf("request %d waited %v by injection, want %v (all: %v)", i, got[i], want[i], got)
		}
	}
}

// slow-first=40ms delays the first request of a shape by 40 ms and no other.
func TestRequestRecordsTheWaitSlowFirstInjected(t *testing.T) {
	cfg := cfgFor()
	cfg.SlowFirst = 40 * time.Millisecond
	r := serveRecorded(t, cfg)
	for i := 0; i < 2; i++ {
		if err := r.chat(context.Background(), shapeBody(5, 4)); err != nil {
			t.Fatal(err)
		}
	}
	evs := r.events("request")
	if len(evs) != 2 || evs[0].InjectedNS != (40*time.Millisecond).Nanoseconds() || evs[1].InjectedNS != 0 {
		t.Errorf("injected waits %+v, want 40ms and then none", evs)
	}
}

// A request that holds a slot is logged as admitted at once; the request line comes when it ends. In the
// stall mode the request does not end by itself, so the "admitted" line is how a test sees it in flight,
// and the request line (cancelled) comes when the client goes away.
func TestAdmittedIsLoggedWhileTheRequestIsInFlight(t *testing.T) {
	cfg := cfgFor()
	cfg.Stall = true
	r := serveRecorded(t, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- r.chat(ctx, map[string]any{"model": "m", "stream": true, "max_tokens": 4, "ignore_eos": true,
			"messages": []map[string]string{{"role": "user", "content": "a b c"}}})
	}()
	deadline := time.Now().Add(30 * time.Second)
	for len(r.events("admitted")) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no admitted line for a request in flight")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if n := len(r.events("request")); n != 0 {
		t.Fatalf("%d request lines while the request is stalled in flight", n)
	}
	if a := r.events("admitted")[0]; a.MaxTokens != 4 || a.PromptTokens == 0 || !a.Stream || a.Path != "" {
		t.Errorf("admitted line %+v: want the prompt, max tokens and stream flag, and no path (a reader that counts chats by path must not count it)", a)
	}
	cancel()
	<-done
	for len(r.events("request")) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no request line after the client went away")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if ev := r.events("request")[0]; ev.Outcome != "cancelled" {
		t.Errorf("request line %+v, want outcome cancelled", ev)
	}
}
