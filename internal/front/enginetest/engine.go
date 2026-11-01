// Package enginetest is an engine double for the front's tests: an
// OpenAI-compatible server on loopback standing in for one Instance's serving
// path (HBEP-5, Test plan: "an engine double"). It checks the Instance key
// the front must send, records every request as it arrived, and plays the
// behaviours a test sets: a number of tokens at a pace, a stream with no
// finish reason, a last token in the finish chunk, a connection killed
// mid-stream, a held response. Its pace
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
	// ResponsesEnd is how a Responses stream ends: "" with
	// response.completed, "incomplete" with response.incomplete (a capped
	// reply), "failed" with response.failed and error.code server_error.
	ResponsesEnd string
	// LastWithFinish puts the reply's last token in the finish chunk's delta
	// instead of a chunk of its own, as vLLM v0.32.0 does with the token
	// that ends a reply (spike S94: "delta":{"reasoning":","} next to
	// "finish_reason":"length"). With Tokens 1 the whole reply is the finish
	// chunk. It has no effect while NoFinish is set.
	LastWithFinish bool
	// DeltaField is the member of a chat stream's delta that holds the
	// token text: "" or "content", "reasoning" (vLLM v0.32.0) or
	// "reasoning_content" (older engines). A completions stream always uses
	// "text".
	DeltaField string
	// FinishReason is the finish chunk's reason in a chat or completions
	// stream; "" means "stop".
	FinishReason string
	// CachedTokens is the cached prompt token count the usage reports. Nil,
	// the default, leaves prompt_tokens_details (input_tokens_details on
	// /v1/responses) out of the usage, as vLLM does without
	// --enable-prompt-tokens-details.
	CachedTokens *int64
	// CloseBeforeHeaders reads the request, then closes the connection with
	// no response byte, as frps does with a request it accepted for a dead
	// tunnel (HBEP-6).
	CloseBeforeHeaders bool
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
	// IdleTimeout is the server's idle time-out: a connection that holds no
	// request for this long is closed by the engine, as vLLM closes at its
	// keep-alive of 5 s (HBEP-5, Time-outs and keep-alive). Zero: never.
	IdleTimeout time.Duration
}

// Conn is one connection the engine accepted, from its server's connection
// states. The times are wall-clock times, whatever the engine's clock.
type Conn struct {
	Opened time.Time // the connection was accepted
	Idle   time.Time // the latest time it went idle after a request; zero before
	Closed time.Time // it was closed, by either side; zero while open
	// Requests is how many requests began on it.
	Requests int
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

	conns  []Conn
	connAt map[net.Conn]int
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
	e := &Engine{ln: ln, key: o.Key, clk: o.Clock, connAt: map[net.Conn]int{}}
	e.srv = &http.Server{Handler: http.HandlerFunc(e.serve), ReadHeaderTimeout: 10 * time.Second, IdleTimeout: o.IdleTimeout, ConnState: e.connState}
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

// Conns returns every connection accepted so far, oldest first.
func (e *Engine) Conns() []Conn {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]Conn(nil), e.conns...)
}

func (e *Engine) connState(c net.Conn, st http.ConnState) {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := time.Now()
	if st == http.StateNew {
		e.connAt[c] = len(e.conns)
		e.conns = append(e.conns, Conn{Opened: now})
		return
	}
	i, ok := e.connAt[c]
	if !ok {
		return
	}
	switch st {
	case http.StateActive:
		e.conns[i].Requests++
	case http.StateIdle:
		e.conns[i].Idle = now
	case http.StateClosed, http.StateHijacked:
		e.conns[i].Closed = now
		delete(e.connAt, c)
	}
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
	if b.CloseBeforeHeaders {
		if hj, ok := w.(http.Hijacker); ok {
			if conn, _, err := hj.Hijack(); err == nil {
				conn.Close()
				return
			}
		}
		panic(http.ErrAbortHandler)
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
