package main

// The engine side of the row: a test router and, behind it, two engine doubles (internal/agent/enginedouble), each in a wrapper that
// stands for the front's attribution (it sets the Hpcburst-* response headers; the doubles set none) and records what reached the engine.

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/enginedouble"
)

const (
	testSessionHeader  = "x-session-id"
	testWorkloadHeader = "x-test-workload" // the static header the test config sets; the router reads it
	barrierWait        = 60 * time.Second  // generous: only a generator that cannot run the sessions together ever waits this long
)

// seenReq is one request as the engine side saw it.
type seenReq struct {
	session, workload, route string
	roles                    []string
	words                    []int // the word count of each message
	contents                 []string
	maxTokens                int
	minTokens                *int
	stream, includeUsage     bool
	arrived                  time.Time // the wrapper's entry
	lastWrite                time.Time // stamped BEFORE the last write or flush of the reply, so the client cannot have the reply's end earlier
	requestID                string    // the Hpcburst-Request-Id the wrapper sent
	hasUsage                 bool
	prompt, completion       int    // the double's usage
	reply                    string // the content the double streamed
}

// rig holds what every wrapper records, and the barrier.
type rig struct {
	t       *testing.T
	want    int // the number of sessions whose first requests the barrier waits for
	mu      sync.Mutex
	bySess  map[string][]*seenReq
	firsts  int
	barrier chan struct{}
	once    sync.Once
	missed  atomic.Bool // the barrier's deadline passed
	reqs    atomic.Int64
	events  []enginedouble.LogEvent // the doubles' own request log lines
}

func newRig(t *testing.T, want int) *rig {
	return &rig{t: t, want: want, bySess: map[string][]*seenReq{}, barrier: make(chan struct{})}
}

func (g *rig) record(ev enginedouble.LogEvent) {
	if ev.Event != "request" {
		return
	}
	g.mu.Lock()
	g.events = append(g.events, ev)
	g.mu.Unlock()
}

// enter registers an arriving request, and reports whether its session is new (a first-turn request).
func (g *rig) enter(s *seenReq) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	first := len(g.bySess[s.session]) == 0
	g.bySess[s.session] = append(g.bySess[s.session], s)
	if first {
		g.firsts++
		if g.firsts == g.want {
			g.release()
		}
	}
	return first
}

func (g *rig) release() { g.once.Do(func() { close(g.barrier) }) }

// hold waits at the barrier until every session's first request has arrived. A deadline is a failed run (and releases everyone, so the
// run still ends).
func (g *rig) hold() {
	select {
	case <-g.barrier:
	case <-time.After(barrierWait):
		g.missed.Store(true)
		g.release()
	}
}

type route struct {
	g                              *rig
	name                           string
	provider, deployment, instance string
	inner                          http.Handler
}

// newRoute is one engine double behind its wrapper.
func (g *rig) newRoute(name, provider, deployment, instance, apiKey string) *route {
	cfg := enginedouble.DefaultConfig()
	cfg.APIKey = apiKey
	return &route{g: g, name: name, provider: provider, deployment: deployment, instance: instance,
		inner: enginedouble.HandlerRecording(cfg, g.record)}
}

func (rt *route) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	arrived := time.Now()
	raw, _ := io.ReadAll(r.Body)
	r.Body = io.NopCloser(bytes.NewReader(raw))
	var body struct {
		Messages []struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"messages"`
		MaxTokens     int  `json:"max_tokens"`
		MinTokens     *int `json:"min_tokens"`
		Stream        bool `json:"stream"`
		StreamOptions struct {
			IncludeUsage bool `json:"include_usage"`
		} `json:"stream_options"`
	}
	if r.URL.Path != "/v1/chat/completions" || json.Unmarshal(raw, &body) != nil {
		http.Error(w, "not a chat request", http.StatusBadRequest)
		return
	}
	s := &seenReq{
		session: r.Header.Get(testSessionHeader), workload: r.Header.Get(testWorkloadHeader), route: rt.name,
		maxTokens: body.MaxTokens, minTokens: body.MinTokens, stream: body.Stream, includeUsage: body.StreamOptions.IncludeUsage,
		arrived: arrived,
	}
	for _, m := range body.Messages {
		s.roles = append(s.roles, m.Role)
		s.contents = append(s.contents, m.Content)
		s.words = append(s.words, len(strings.Fields(m.Content)))
	}
	s.requestID = "req-" + rt.name + "-" + itoa(rt.g.reqs.Add(1))
	if rt.g.enter(s) {
		rt.g.hold()
	}

	h := w.Header()
	h.Set("Hpcburst-Request-Id", s.requestID)
	h.Set("Hpcburst-Emitter", "front")
	h.Set("Hpcburst-Provider", rt.provider)
	h.Set("Hpcburst-Deployment-Id", rt.deployment)
	h.Set("Hpcburst-Instance", rt.instance)
	cw := &capWriter{ResponseWriter: w}
	rt.inner.ServeHTTP(cw, r)

	s.lastWrite = cw.last
	s.reply, s.prompt, s.completion, s.hasUsage = parseStream(cw.buf.Bytes())
}

// capWriter keeps what the double streamed, and the time of its last write or flush, taken before the call.
type capWriter struct {
	http.ResponseWriter
	buf  bytes.Buffer
	last time.Time
}

func (c *capWriter) Write(b []byte) (int, error) {
	c.last = time.Now()
	c.buf.Write(b)
	return c.ResponseWriter.Write(b)
}

func (c *capWriter) Flush() {
	c.last = time.Now()
	if f, ok := c.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (c *capWriter) Unwrap() http.ResponseWriter { return c.ResponseWriter }

// parseStream reads the double's server-sent events: the content deltas joined, and the usage chunk's figures.
func parseStream(b []byte) (reply string, prompt, completion int, hasUsage bool) {
	var sb strings.Builder
	sc := bufio.NewScanner(bytes.NewReader(b))
	for sc.Scan() {
		data, ok := strings.CutPrefix(sc.Text(), "data: ")
		if !ok || data == "[DONE]" {
			continue
		}
		var ev struct {
			Choices []struct {
				Delta struct {
					Content string `json:"content"`
				} `json:"delta"`
			} `json:"choices"`
			Usage *struct {
				PromptTokens     int `json:"prompt_tokens"`
				CompletionTokens int `json:"completion_tokens"`
			} `json:"usage"`
		}
		if json.Unmarshal([]byte(data), &ev) != nil {
			continue
		}
		for _, c := range ev.Choices {
			sb.WriteString(c.Delta.Content)
		}
		if ev.Usage != nil {
			prompt, completion, hasUsage = ev.Usage.PromptTokens, ev.Usage.CompletionTokens, true
		}
	}
	return sb.String(), prompt, completion, hasUsage
}

// router sends a request to a route by the static workload header; a request with another value gets 502, with no attribution.
func router(routes map[string]*route) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rt := routes[r.Header.Get(testWorkloadHeader)]
		if rt == nil {
			http.Error(w, "no route", http.StatusBadGateway)
			return
		}
		rt.ServeHTTP(w, r)
	})
}

func itoa(n int64) string { b, _ := json.Marshal(n); return string(b) }
