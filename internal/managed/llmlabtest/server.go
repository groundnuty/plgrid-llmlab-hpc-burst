package llmlabtest

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"
)

// Defaults from the lessons, for a test that wants LLMLab's own values.
const (
	// NoByteCut is LLMLab's cut: a reply that sends nothing for 180 s ends
	// with 504 (L-6.1; HBEP-5, Notes and The LLMLab route).
	NoByteCut = 180 * time.Second
	// StallPeriod is the period of LLMLab's database sync, which stalls every
	// model (L-6.2).
	StallPeriod = 30 * time.Minute
	// InjectedTokens is the length of the prompt segment the gateway adds,
	// within L-6.6's 10 to 15 tokens.
	InjectedTokens = 12
)

// DefaultReply is the text a fresh Server answers with: five words, so five
// completion tokens and five stream chunks.
const DefaultReply = "This is the LLMLab double."

// DefaultAllowlist is the body members the gateway lets through when
// rewriting is on (L-6.6): the OpenAI request members of the four routes. A
// member outside it, such as cache_salt, gets 422.
var DefaultAllowlist = []string{
	"model", "messages", "prompt", "input", "instructions", "stream", "stream_options",
	"max_tokens", "max_completion_tokens", "max_output_tokens", "temperature", "top_p", "n",
	"stop", "presence_penalty", "frequency_penalty", "logit_bias", "logprobs", "top_logprobs",
	"user", "seed", "tools", "tool_choice", "parallel_tool_calls", "response_format",
	"suffix", "echo", "best_of", "encoding_format", "dimensions", "metadata", "store",
	"previous_response_id", "reasoning", "text",
}

// Model is one catalog entry of the double.
type Model struct {
	// ID is the model's name in the catalog, the listing's "id".
	ID string
	// Revision, when not empty, is listed as the entry's "revision" member.
	// The member name is an assumption (package doc).
	Revision string
	// OwnedBy is the listing's "owned_by"; empty lists "llmlab".
	OwnedBy string
	// ContextWindow is the model's context length. With rewriting on, a
	// request whose max_tokens equals it gets 400 (L-6.6). Zero: no check.
	ContextWindow int
	// MaxTokensCap is a hidden server-side output cap (L-6.5): output stops
	// after this many tokens with finish reason "length", whatever the
	// request asked. Zero: no cap.
	MaxTokensCap int
	// TruncateAfter ends every stream of this model after this many content
	// chunks, with no finish reason and no [DONE] (L-6.7). Zero: off.
	TruncateAfter int
}

// Stalls is a periodic stall schedule (L-6.2): windows of length Duration
// that start at Start, Start+Period, Start+2*Period and so on.
type Stalls struct {
	Period   time.Duration
	Duration time.Duration
	// Start is when the first window opens; zero means when SetStalls is
	// called.
	Start time.Time
}

// Latency is the gateway's own pace on the inference routes.
type Latency struct {
	// FirstByte is the time from a request's arrival to its first byte.
	FirstByte time.Duration
	// PerChunk is the gap between two stream chunks.
	PerChunk time.Duration
}

// Rewriting is the gateway's request rewriting (L-6.6).
type Rewriting struct {
	// InjectedTokens is added to every chat, completions and responses
	// request's prompt tokens. Zero takes InjectedTokens.
	InjectedTokens int
	// Allowlist is the body members let through; nil takes DefaultAllowlist.
	Allowlist []string
}

// Options configure a Server.
type Options struct {
	// PortLo and PortHi are the loopback port range, inclusive, that the
	// Server binds the first free port of. Required.
	PortLo, PortHi int
	// Now is the Server's clock. Nil is time.Now.
	Now func() time.Time
	// Keys are the keys accepted from the start.
	Keys []string
	// Models is the catalog from the start, all listed and active.
	Models []Model
}

// Request is one request as it arrived, and the status it got.
type Request struct {
	// Time is when it arrived, by the Server's clock.
	Time time.Time
	// Method and Path are the request line's.
	Method, Path string
	// Header is the request's headers as they arrived, Authorization
	// included.
	Header http.Header
	// Body is the request's body as it arrived, byte for byte.
	Body []byte
	// Model is the body's "model" member, when it has a string one.
	Model string
	// Status is the status the Server answered with; 0 when the request ended
	// before an answer (the client went away, or the Server closed).
	Status int
	// FirstByte is when the Server sent the first byte of its answer, by its
	// clock; zero when it sent none.
	FirstByte time.Time
	// Truncated is true when a stream ended without its finish (L-6.7, or the
	// no-byte cut after the first byte).
	Truncated bool
}

type keyState struct {
	expiry time.Time // zero: none
	reject int       // 0, 401 or 403
}

type modelState struct {
	Model
	listed   bool
	inactive bool
	blocked  bool
	order    int
}

// Server is the double. All its methods are safe for concurrent use.
type Server struct {
	srv *httptest.Server
	now func() time.Time

	closeOnce sync.Once
	closed    chan struct{}

	mu        sync.Mutex
	changed   chan struct{} // closed and replaced on every change a test makes
	keys      map[string]keyState
	models    map[string]*modelState
	nextOrder int
	echoKey   bool
	ambiguous bool
	cut       time.Duration
	manual    bool
	stalls    Stalls
	latency   Latency
	rewriting *Rewriting
	reply     string
	requests  []Request
	seq       int
}

// Start starts a Server for a test and closes it when the test ends. It fails
// the test when no port of the range is free.
func Start(t testing.TB, opts Options) *Server {
	t.Helper()
	s, err := New(opts)
	if err != nil {
		t.Fatalf("llmlabtest: %v", err)
	}
	t.Cleanup(s.Close)
	return s
}

// New starts a Server. The caller closes it.
func New(opts Options) (*Server, error) {
	ln, err := Listen(opts.PortLo, opts.PortHi)
	if err != nil {
		return nil, err
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	s := &Server{
		now:     now,
		closed:  make(chan struct{}),
		changed: make(chan struct{}),
		keys:    map[string]keyState{},
		models:  map[string]*modelState{},
		reply:   DefaultReply,
	}
	for _, k := range opts.Keys {
		s.keys[k] = keyState{}
	}
	for _, m := range opts.Models {
		s.addModelLocked(m)
	}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(s.serve))
	if err := srv.Listener.Close(); err != nil {
		_ = ln.Close()
		return nil, fmt.Errorf("close the default listener: %w", err)
	}
	srv.Listener = ln
	srv.Start()
	s.srv = srv
	return s, nil
}

// Listen binds the first free TCP port of lo to hi, inclusive, on 127.0.0.1.
func Listen(lo, hi int) (net.Listener, error) {
	if lo <= 1024 || hi > 65535 || lo > hi {
		return nil, fmt.Errorf("port range %d-%d: want 1024 < lo <= hi <= 65535", lo, hi)
	}
	var errs []error
	for p := lo; p <= hi; p++ {
		ln, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(p)))
		if err == nil {
			return ln, nil
		}
		errs = append(errs, err)
	}
	return nil, fmt.Errorf("no free port on 127.0.0.1 in %d-%d: %w", lo, hi, errors.Join(errs...))
}

// URL is the Server's root, http://127.0.0.1:<port>.
func (s *Server) URL() string { return s.srv.URL }

// BaseURL is the OpenAI-style base URL, with its version segment, as a
// managed credentials file names it: URL()+"/v1".
func (s *Server) BaseURL() string { return s.srv.URL + "/v1" }

// Close stops the Server. Waiting requests end at once, with no answer. The
// connections go first: a waiting handler that woke before them would return
// without writing, and net/http would then answer 200 with an empty body.
func (s *Server) Close() {
	s.closeOnce.Do(func() {
		s.srv.CloseClientConnections()
		close(s.closed)
		s.srv.Close()
	})
}

// change runs f under the lock and wakes every waiting request.
func (s *Server) change(f func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f()
	close(s.changed)
	s.changed = make(chan struct{})
}

// AddKey makes the Server accept key, with no expiry and no rejection.
func (s *Server) AddKey(key string) { s.change(func() { s.keys[key] = keyState{} }) }

// RemoveKey makes the Server refuse key with 401, as an unknown key.
func (s *Server) RemoveKey(key string) { s.change(func() { delete(s.keys, key) }) }

// ExpireKeyAt makes key fail with 401 from t on, by the Server's clock
// (L-6.4). The zero time clears the expiry. It panics for a key the Server
// does not have, one never added or one removed: a call on such a key would
// make it valid again.
func (s *Server) ExpireKeyAt(key string, t time.Time) {
	s.change(func() {
		st := s.mustKey("ExpireKeyAt", key)
		st.expiry = t
		s.keys[key] = st
	})
}

// RejectKey answers every request with key with status, 401 or 403; 0
// accepts the key again. It panics on any other status, and for a key the
// Server does not have (see ExpireKeyAt).
func (s *Server) RejectKey(key string, status int) {
	if status != 0 && status != http.StatusUnauthorized && status != http.StatusForbidden {
		panic(fmt.Sprintf("llmlabtest: RejectKey status %d: want 0, 401 or 403", status))
	}
	s.change(func() {
		st := s.mustKey("RejectKey", key)
		st.reject = status
		s.keys[key] = st
	})
}

// mustKey returns the state of a key the Server has. The panic does not name
// the key, so a real key never reaches a test log. It runs under the lock.
func (s *Server) mustKey(method, key string) keyState {
	st, ok := s.keys[key]
	if !ok {
		panic("llmlabtest: " + method + ": the Server has no such key (never added, or removed)")
	}
	return st
}

// EchoKey makes every error body carry the request's Authorization header.
func (s *Server) EchoKey(on bool) { s.change(func() { s.echoKey = on }) }

// SetModels replaces the listing: these models are listed and active, in
// this order, and every other model is unlisted. A model listed before keeps
// its active and blocked state.
func (s *Server) SetModels(models ...Model) {
	s.change(func() {
		for _, m := range s.models {
			m.listed = false
		}
		for _, m := range models {
			s.addModelLocked(m)
		}
	})
}

// AddModel lists m, at the end of the listing. A model that was listed before
// returns with its active and blocked state.
func (s *Server) AddModel(m Model) { s.change(func() { s.addModelLocked(m) }) }

func (s *Server) addModelLocked(m Model) {
	st, ok := s.models[m.ID]
	if !ok {
		st = &modelState{}
		s.models[m.ID] = st
	}
	st.Model, st.listed = m, true
	st.order = s.nextOrder
	s.nextOrder++
}

// RemoveModel takes the model out of the listing. Its requests then get the
// ambiguous 400, as for an unknown model.
func (s *Server) RemoveModel(id string) {
	s.change(func() {
		if m, ok := s.models[id]; ok {
			m.listed = false
		}
	})
}

// SetActive makes a listed model active or inactive (L-6.5). An inactive
// model stays listed; its requests get 503, or the ambiguous 400 with
// SetAmbiguous. It panics for a model the Server never had.
func (s *Server) SetActive(id string, active bool) {
	s.change(func() { s.mustModel(id).inactive = !active })
}

// SetBlocked makes a model blocked for the key's grant: its requests get the
// ambiguous 400 (L-6.5). It panics for a model the Server never had.
func (s *Server) SetBlocked(id string, blocked bool) {
	s.change(func() { s.mustModel(id).blocked = blocked })
}

func (s *Server) mustModel(id string) *modelState {
	m, ok := s.models[id]
	if !ok {
		panic(fmt.Sprintf("llmlabtest: no model %q", id))
	}
	return m
}

// SetAmbiguous makes an inactive model answer with the shared 400 of an
// unknown model instead of 503 (L-6.5).
func (s *Server) SetAmbiguous(on bool) { s.change(func() { s.ambiguous = on }) }

// SetNoByteCut sets the no-byte cut (L-6.1); zero turns it off.
func (s *Server) SetNoByteCut(d time.Duration) { s.change(func() { s.cut = d }) }

// Stall starts or ends a manual stall (L-6.2): while it lasts, no request
// sends its first byte.
func (s *Server) Stall(on bool) { s.change(func() { s.manual = on }) }

// SetStalls sets the periodic stall schedule (L-6.2); a zero Period turns it
// off.
func (s *Server) SetStalls(st Stalls) {
	s.change(func() {
		if st.Period > 0 && st.Start.IsZero() {
			st.Start = s.now()
		}
		s.stalls = st
	})
}

// Stalled reports whether a request arriving at t would wait for a stall.
func (s *Server) Stalled(t time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stalledLocked(t)
}

func (s *Server) stalledLocked(t time.Time) bool {
	if s.manual {
		return true
	}
	st := s.stalls
	if st.Period <= 0 || st.Duration <= 0 || t.Before(st.Start) {
		return false
	}
	return t.Sub(st.Start)%st.Period < st.Duration
}

// SetLatency sets the pace of the inference routes.
func (s *Server) SetLatency(l Latency) { s.change(func() { s.latency = l }) }

// SetRewriting turns request rewriting on (L-6.6) with r's settings, or off
// with nil.
func (s *Server) SetRewriting(r *Rewriting) {
	s.change(func() {
		if r == nil {
			s.rewriting = nil
			return
		}
		c := *r
		if c.InjectedTokens == 0 {
			c.InjectedTokens = InjectedTokens
		}
		if c.Allowlist == nil {
			c.Allowlist = DefaultAllowlist
		}
		c.Allowlist = append([]string(nil), c.Allowlist...)
		s.rewriting = &c
	})
}

// SetReply sets the text every completion answers with.
func (s *Server) SetReply(text string) { s.change(func() { s.reply = text }) }

// Requests returns a copy of the request log, oldest first. A request is
// logged when its answer ends.
func (s *Server) Requests() []Request {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Request, len(s.requests))
	for i, r := range s.requests {
		out[i] = r
		out[i].Header = r.Header.Clone()
		out[i].Body = append([]byte(nil), r.Body...)
	}
	return out
}

// Count returns how many logged requests had this method and path.
func (s *Server) Count(method, path string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, r := range s.requests {
		if r.Method == method && r.Path == path {
			n++
		}
	}
	return n
}

// Listed returns the IDs the listing shows now, in its order.
func (s *Server) Listed() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, m := range s.listedLocked() {
		out = append(out, m.ID)
	}
	return out
}

func (s *Server) listedLocked() []Model {
	var ms []*modelState
	for _, m := range s.models {
		if m.listed {
			ms = append(ms, m)
		}
	}
	sort.Slice(ms, func(i, k int) bool { return ms[i].order < ms[k].order })
	out := make([]Model, len(ms))
	for i, m := range ms {
		out[i] = m.Model
	}
	return out
}

func (s *Server) log(r Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests = append(s.requests, r)
}
