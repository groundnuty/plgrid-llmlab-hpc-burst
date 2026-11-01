package engine

import (
	"bytes"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/enginedouble"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// serveOn serves h on a fresh Unix socket and returns its path. The socket
// directory is short (a Unix socket path is limited to about 100 bytes).
func serveOn(t *testing.T, h http.Handler) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "hbeng")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	socket := filepath.Join(dir, "e.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: h}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return socket
}

// double returns a Client of an engine double with the modes (the value of
// HB_ENGINE_DOUBLE) and the config adjusted by tune, which may be nil. Its
// token delay is 0 unless the modes set one.
func double(t *testing.T, modes string, tune func(*enginedouble.Config), opts ...Option) *Client {
	t.Helper()
	c, _ := doubleWith(t, modes, tune, nil, opts...)
	return c
}

// doubleWith is double with a middleware in front of the double's handler,
// which may be nil, and returns the recorder of what the client sent.
func doubleWith(t *testing.T, modes string, tune func(*enginedouble.Config), wrap func(http.Handler) http.Handler, opts ...Option) (*Client, *recorder) {
	t.Helper()
	cfg, err := enginedouble.ParseModes(modes)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ServedName = "m"
	if !strings.Contains(modes, "token-delay") {
		cfg.TokenDelay = 0
	}
	if tune != nil {
		tune(&cfg)
	}
	var h http.Handler = enginedouble.Handler(cfg)
	if wrap != nil {
		h = wrap(h)
	}
	rec := &recorder{}
	return New(serveOn(t, rec.wrap(h)), "key", opts...), rec
}

// recorder keeps the chat requests the client sent and what the engine
// answered with their usage.
type recorder struct {
	mu       sync.Mutex
	requests []map[string]any
	prompts  []int    // usage.prompt_tokens of each non-streamed answer
	streams  []string // the body of each streamed answer
}

// teeWriter copies a streamed answer while it passes, flushes included.
type teeWriter struct {
	http.ResponseWriter
	buf *bytes.Buffer
}

func (t teeWriter) Write(p []byte) (int, error) {
	t.buf.Write(p)
	return t.ResponseWriter.Write(p)
}

func (t teeWriter) Flush() {
	if f, ok := t.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// finishDeltas counts the recorded streams' finish chunks whose delta is not
// empty: content, reasoning or a tool call.
func (r *recorder) finishDeltas() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, body := range r.streams {
		for _, ev := range strings.Split(body, "\n\n") {
			data, ok := strings.CutPrefix(strings.TrimSpace(ev), "data: ")
			if !ok || data == "[DONE]" {
				continue
			}
			var c struct {
				Choices []struct {
					Delta struct {
						Content   string            `json:"content"`
						Reasoning string            `json:"reasoning"`
						ToolCalls []json.RawMessage `json:"tool_calls"`
					} `json:"delta"`
					FinishReason *string `json:"finish_reason"`
				} `json:"choices"`
			}
			if json.Unmarshal([]byte(data), &c) != nil {
				continue
			}
			for _, ch := range c.Choices {
				if ch.FinishReason != nil && (ch.Delta.Content != "" || ch.Delta.Reasoning != "" || len(ch.Delta.ToolCalls) > 0) {
					n++
				}
			}
		}
	}
	return n
}

func (r *recorder) wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/v1/chat/completions" {
			next.ServeHTTP(w, req)
			return
		}
		b, _ := io.ReadAll(req.Body)
		var body map[string]any
		_ = json.Unmarshal(b, &body)
		req.Body = io.NopCloser(bytes.NewReader(b))
		r.mu.Lock()
		r.requests = append(r.requests, body)
		r.mu.Unlock()
		if body["stream"] != true {
			next.ServeHTTP(w, req)
			return
		}
		buf := &bytes.Buffer{}
		next.ServeHTTP(teeWriter{w, buf}, req)
		r.mu.Lock()
		r.streams = append(r.streams, buf.String())
		r.mu.Unlock()
	})
}

// sent returns the recorded chat requests, in order.
func (r *recorder) sent() []map[string]any {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]map[string]any(nil), r.requests...)
}

// maxTokensOf returns the max_tokens of the recorded requests that offered
// tools (toolsOffered) or did not.
func (r *recorder) maxTokensOf(toolsOffered bool) []int {
	var out []int
	for _, req := range r.sent() {
		if _, has := req["tools"]; has == toolsOffered {
			out = append(out, int(req["max_tokens"].(float64)))
		}
	}
	return out
}

// shapes are small request shapes for the doubles: the typical output is
// 64 tokens, the longest 100.
var shapes = agentapi.Shapes{ContextShort: 20, ContextTypical: 60, ContextLongest: 200, OutputTypical: 64, OutputLongest: 100}

func outcomeOf(t *testing.T, res agentapi.QualifyResult, name agentapi.CheckName) agentapi.CheckOutcome {
	t.Helper()
	for _, o := range res.Checks {
		if o.Name == name {
			return o
		}
	}
	t.Fatalf("no %s outcome in %+v", name, res.Checks)
	return agentapi.CheckOutcome{}
}
