// Package enginetest is an engine double for the front's tests: an
// OpenAI-compatible server on loopback standing in for one Instance's serving
// path (HBEP-5, Test plan: "an engine double"). It checks the Instance key
// the front must send, records every request as it arrived, and plays the
// behaviours a test sets: a number of tokens at a pace, a stream with no
// finish reason, a connection killed mid-stream, a held response. Its pace
// runs on the front's clock, so with a fake clock a 7,000-token stream at one
// token per second takes milliseconds while the front's real 2 h and 30 min
// timers see two hours pass.
//
// Standard library only, plus the front's clock package.
package enginetest

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/clock"
)

// Behaviour is what the engine does with the next requests.
type Behaviour struct {
	Tokens     int           // content tokens per reply; 0 means 3
	PerToken   time.Duration // pause before each token after the first
	FirstToken time.Duration // pause before the first token
	NoFinish   bool          // a stream ends with no finish reason (and no response.completed)
	NoDone     bool          // a chat or completions stream ends without [DONE]
	KillAfter  int           // close the connection abruptly after this many tokens
	Status     int           // answer this status with ErrorBody instead
	ErrorBody  string
	// Hold, when set, holds each request after its headers are read until
	// the channel is closed or the request ends.
	Hold chan struct{}
}

// Request is one request as it arrived.
type Request struct {
	Method string
	Path   string
	Header http.Header
	Body   []byte
	// ContentLength is the header's value; BodyLen what was read.
	ContentLength int64
}

// Options configure an engine.
type Options struct {
	PortLo, PortHi int    // loopback port block to bind in
	Key            string // the Instance key; requests without "Bearer <Key>" get 401
	Clock          clock.Clock
}

// Engine is a running double.
type Engine struct {
	srv  *http.Server
	ln   net.Listener
	key  string
	clk  clock.Clock
	mu   sync.Mutex
	b    Behaviour
	reqs []Request
	open int // requests being answered
}

// Start binds the first free port of the block and serves until the test
// ends.
func Start(t testing.TB, o Options) *Engine {
	t.Helper()
	ln, err := Listen(o.PortLo, o.PortHi)
	if err != nil {
		t.Fatal(err)
	}
	if o.Clock == nil {
		o.Clock = clock.Real{}
	}
	e := &Engine{ln: ln, key: o.Key, clk: o.Clock}
	e.srv = &http.Server{Handler: http.HandlerFunc(e.serve), ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = e.srv.Serve(ln) }()
	t.Cleanup(func() { _ = e.srv.Close() })
	return e
}

// Listen binds 127.0.0.1 on the first free port in [lo, hi].
func Listen(lo, hi int) (net.Listener, error) {
	for p := lo; p <= hi; p++ {
		if l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", p)); err == nil {
			return l, nil
		}
	}
	return nil, fmt.Errorf("enginetest: no free port in 127.0.0.1:%d-%d", lo, hi)
}

// Addr is the engine's host:port.
func (e *Engine) Addr() string { return e.ln.Addr().String() }

// Close stops the engine and cuts its connections, as a killed Instance.
func (e *Engine) Close() { _ = e.srv.Close() }

// Set replaces the behaviour.
func (e *Engine) Set(b Behaviour) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.b = b
}

// Requests returns every request so far.
func (e *Engine) Requests() []Request {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]Request(nil), e.reqs...)
}

// Open is the number of requests being answered now.
func (e *Engine) Open() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.open
}

func (e *Engine) serve(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	e.mu.Lock()
	e.reqs = append(e.reqs, Request{Method: r.Method, Path: r.URL.Path, Header: r.Header.Clone(), Body: body, ContentLength: r.ContentLength})
	b := e.b
	e.open++
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		e.open--
		e.mu.Unlock()
	}()
	if e.key != "" && r.Header.Get("Authorization") != "Bearer "+e.key {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": map[string]any{"message": "invalid key", "type": "auth", "code": 401}})
		return
	}
	if b.Hold != nil {
		select {
		case <-b.Hold:
		case <-r.Context().Done():
			return
		}
	}
	if b.Status != 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(b.Status)
		_, _ = io.WriteString(w, b.ErrorBody)
		return
	}
	var req struct {
		Model         string `json:"model"`
		Stream        bool   `json:"stream"`
		StreamOptions struct {
			IncludeUsage bool `json:"include_usage"`
		} `json:"stream_options"`
		Messages []struct {
			Content any `json:"content"`
		} `json:"messages"`
		Prompt any `json:"prompt"`
		Input  any `json:"input"`
	}
	_ = json.Unmarshal(body, &req)
	p := play{e: e, w: w, b: b, model: req.Model, usage: req.StreamOptions.IncludeUsage, prompt: promptTokens(req.Messages, req.Prompt, req.Input)}
	switch {
	case r.URL.Path == "/v1/embeddings":
		p.embeddings()
	case !req.Stream:
		p.whole(r.URL.Path)
	case r.URL.Path == "/v1/responses":
		p.responsesStream()
	case r.URL.Path == "/v1/chat/completions" || r.URL.Path == "/v1/completions":
		p.chatStream(r.URL.Path == "/v1/completions")
	default:
		p.rawStream()
	}
}

// promptTokens counts whitespace-separated words of the request's text, as
// the LLMLab double does, so a test can compare prompt counts.
func promptTokens(msgs []struct {
	Content any `json:"content"`
}, prompt, input any) int64 {
	n := 0
	count := func(v any) {
		if s, ok := v.(string); ok {
			n += len(strings.Fields(s))
		}
	}
	for _, m := range msgs {
		count(m.Content)
	}
	count(prompt)
	count(input)
	return int64(n)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
