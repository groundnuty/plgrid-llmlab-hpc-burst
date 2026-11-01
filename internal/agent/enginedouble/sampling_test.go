package enginedouble

import (
	"context"
	"net"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

// serveRecording serves HandlerRecording(cfg) and returns the server and a
// func that shuts it down (waiting for every handler to return, so every
// request's record has been made) and returns the records in the order they
// were made.
func serveRecording(t *testing.T, cfg Config) (*testServer, func() []LogEvent) {
	t.Helper()
	var mu sync.Mutex
	var events []LogEvent
	socket := sockPath(t, shortDir(t), "e.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: HandlerRecording(cfg, func(ev LogEvent) {
		mu.Lock()
		events = append(events, ev)
		mu.Unlock()
	})}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return &testServer{t: t, hc: unixClient(socket)}, func() []LogEvent {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			t.Fatalf("shutting the double down: %v", err)
		}
		mu.Lock()
		defer mu.Unlock()
		return append([]LogEvent(nil), events...)
	}
}

func f64(v float64) *float64 { return &v }
func i64(v int64) *int64     { return &v }

// The record holds each sampling parameter as the request sent it: a value of
// 0 is present, a parameter left out is absent, and the same holds for the
// seed. The expected values are written out by hand.
func TestTheDoubleRecordsTheSamplingParametersAsSent(t *testing.T) {
	s, finish := serveRecording(t, cfgFor())
	s.chat(chatBody(5, map[string]any{
		"temperature": 0.6, "top_p": 0.95, "top_k": 20, "min_p": 0,
		"repetition_penalty": 1, "presence_penalty": 0, "frequency_penalty": 0, "seed": 0,
	}))
	s.chat(chatBody(5, map[string]any{"temperature": 0, "top_p": 1, "repetition_penalty": 1, "presence_penalty": 0, "frequency_penalty": 0}))
	s.chat(chatBody(5, nil))
	got := finish()
	if len(got) != 3 {
		t.Fatalf("%d records, want 3", len(got))
	}
	want := []RequestSampling{
		{Temperature: f64(0.6), TopP: f64(0.95), TopK: i64(20), MinP: f64(0), RepetitionPenalty: f64(1), PresencePenalty: f64(0), FrequencyPenalty: f64(0), Seed: i64(0)},
		{Temperature: f64(0), TopP: f64(1), RepetitionPenalty: f64(1), PresencePenalty: f64(0), FrequencyPenalty: f64(0)},
		{},
	}
	for i, w := range want {
		if got[i].Event != "request" || got[i].Sampling == nil || !reflect.DeepEqual(*got[i].Sampling, w) {
			t.Errorf("record %d: sampling %+v, want %+v", i, got[i].Sampling, w)
		}
	}
}

// The record keeps the closing words of the prompt, cut at 120 bytes, so a
// test tells the agent's checks apart without the double knowing them.
func TestTheDoubleRecordsTheTailOfThePrompt(t *testing.T) {
	s, finish := serveRecording(t, cfgFor())
	s.chat(map[string]any{"model": "m", "messages": []map[string]string{
		{"role": "user", "content": "first message"},
		{"role": "assistant", "content": "ok"},
		{"role": "user", "content": nWords(60) + " Say hello in one short sentence."},
	}})
	s.chat(map[string]any{"model": "m", "messages": []map[string]string{{"role": "user", "content": "Reply with one word: ready."}}})
	got := finish()
	if len(got) != 2 {
		t.Fatalf("%d records, want 2", len(got))
	}
	if len(got[0].Tail) != 120 || !strings.HasSuffix(got[0].Tail, " w60 Say hello in one short sentence.") {
		t.Errorf("long prompt: tail %q (%d bytes), want 120 bytes ending in the closing words", got[0].Tail, len(got[0].Tail))
	}
	if got[1].Tail != "Reply with one word: ready." {
		t.Errorf("short prompt: tail %q, want the whole prompt", got[1].Tail)
	}
}

func seededRequest(seed any, extra map[string]any) map[string]any {
	m := map[string]any{"temperature": 0.6, "top_p": 0.95, "top_k": 20}
	if seed != nil {
		m["seed"] = seed
	}
	for k, v := range extra {
		m[k] = v
	}
	return longRequest(48, m)
}

// A sampled request with a seed gives the same tokens every time it is sent,
// and other tokens for another seed and for another prompt; one without a seed
// is random, as before. The corruption check relies on this for a reasoning
// model, whose three identical requests carry one seed.
func TestASeededSampledRequestRepeats(t *testing.T) {
	s := serve(t, cfgFor())
	a, b := s.tokens(seededRequest(1104, nil)), s.tokens(seededRequest(1104, nil))
	if len(a) != 48 || !reflect.DeepEqual(a, b) {
		t.Errorf("two sampled requests with seed 1104 gave different tokens:\n%v\n%v", a, b)
	}
	if other := s.tokens(seededRequest(1105, nil)); reflect.DeepEqual(a, other) {
		t.Error("seeds 1104 and 1105 gave the same tokens")
	}
	if other := s.tokens(seededRequest(0, nil)); reflect.DeepEqual(a, other) || len(other) != 48 {
		t.Error("seed 0 gave the tokens of seed 1104, or none")
	}
	greedy := s.tokens(longRequest(48, nil))
	if reflect.DeepEqual(a, greedy) {
		t.Error("a seeded sampled request gave the greedy tokens")
	}
	// A seed does not change a greedy request (temperature 0): vLLM samples
	// nothing there.
	if g := s.tokens(longRequest(48, map[string]any{"temperature": 0, "seed": 7})); !reflect.DeepEqual(g, greedy) {
		t.Error("temperature 0 with a seed differs from the greedy output")
	}
	first := s.tokens(seededRequest(nil, nil))
	for i := 0; i < 4; i++ {
		if !reflect.DeepEqual(first, s.tokens(seededRequest(nil, nil))) {
			return
		}
	}
	t.Error("five sampled requests without a seed gave the same tokens")
}

// greedy-diverge acts on a seeded sampled request as on a greedy one: the
// repeats of an identical request share the first 40 tokens and differ after.
// A different seed starts its own count.
func TestGreedyDivergeActsOnSeededRequests(t *testing.T) {
	cfg := cfgFor()
	modes, err := ParseModes("greedy-diverge=40")
	if err != nil {
		t.Fatal(err)
	}
	cfg.GreedyDiverge, cfg.GreedyDivergeAt = modes.GreedyDiverge, modes.GreedyDivergeAt
	s := serve(t, cfg)
	first := s.tokens(seededRequest(1104, map[string]any{"max_tokens": 60, "min_tokens": 60}))
	rep := s.tokens(seededRequest(1104, map[string]any{"max_tokens": 60, "min_tokens": 60}))
	if len(first) != 60 || len(rep) != 60 || !reflect.DeepEqual(first[:40], rep[:40]) || reflect.DeepEqual(first[40:], rep[40:]) {
		t.Errorf("the repeat of a seeded request should share 40 tokens and differ after:\n%v\n%v", first, rep)
	}
	fresh := s.tokens(seededRequest(1105, map[string]any{"max_tokens": 60, "min_tokens": 60}))
	again := s.tokens(seededRequest(1105, map[string]any{"max_tokens": 60, "min_tokens": 60}))
	if reflect.DeepEqual(fresh[:40], first[:40]) || !reflect.DeepEqual(fresh[:40], again[:40]) {
		t.Error("seed 1105 should walk on its own and diverge only on its own repeat")
	}
}
