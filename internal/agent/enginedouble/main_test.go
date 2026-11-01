package enginedouble

import (
	"bytes"
	"context"
	"encoding/json"
	"hash/fnv"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestMain makes this test binary the fake programs when it is started as
// one: the end-to-end tests run it as apptainer, as ssh and as the engine.
func TestMain(m *testing.M) {
	Dispatch()
	os.Exit(m.Run())
}

// shortDir returns a fresh directory with a short path: a Unix socket path
// must stay under 108 bytes, and t.TempDir's is too long.
func shortDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "hbed")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

// sockPath returns a socket path in dir and checks it fits sockaddr_un.
func sockPath(t *testing.T, dir, name string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if len(p) >= 108 {
		t.Fatalf("socket path %q is %d bytes, the limit is 107", p, len(p))
	}
	return p
}

// unixClient returns an HTTP client that dials the Unix socket.
func unixClient(socket string) *http.Client {
	return &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", socket)
		},
		DisableKeepAlives: true,
	}}
}

// testServer serves Handler(cfg) on a Unix socket, as the engine does.
type testServer struct {
	t  *testing.T
	hc *http.Client
}

func serve(t *testing.T, cfg Config) *testServer {
	t.Helper()
	return serveHandler(t, Handler(cfg))
}

// serveHandler serves h on a Unix socket, for a test that builds the handler itself (newHandler, with a
// record function).
func serveHandler(t *testing.T, h http.Handler) *testServer {
	t.Helper()
	socket := sockPath(t, shortDir(t), "e.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: h}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return &testServer{t: t, hc: unixClient(socket)}
}

// cfgFor is a Config for tests: the defaults, no token delay, served as "m".
func cfgFor() Config {
	c := DefaultConfig()
	c.ServedName, c.TokenDelay = "m", 0
	return c
}

type response struct {
	status int
	header http.Header
	body   []byte
}

func (s *testServer) do(method, path, key string, body any) response {
	s.t.Helper()
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			s.t.Fatal(err)
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, "http://engine"+path, rd)
	if err != nil {
		s.t.Fatal(err)
	}
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	resp, err := s.hc.Do(req)
	if err != nil {
		s.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		s.t.Fatalf("%s %s: reading the body: %v", method, path, err)
	}
	return response{resp.StatusCode, resp.Header, b}
}

func (s *testServer) get(path string) response { return s.do(http.MethodGet, path, "", nil) }

func (s *testServer) post(path string, body any) response {
	return s.do(http.MethodPost, path, "", body)
}

func (r response) json(t *testing.T, v any) {
	t.Helper()
	if err := json.Unmarshal(r.body, v); err != nil {
		t.Fatalf("not JSON (%v): %s", err, r.body)
	}
}

// chatBody is a chat request with one user message of n words.
func chatBody(words int, extra map[string]any) map[string]any {
	m := map[string]any{
		"model":    "m",
		"messages": []map[string]string{{"role": "user", "content": nWords(words)}},
	}
	for k, v := range extra {
		m[k] = v
	}
	return m
}

// nWords is n distinct words, "w1 w2 ... wn".
func nWords(n int) string {
	ws := make([]string, n)
	for i := range ws {
		ws[i] = "w" + strconv.Itoa(i+1)
	}
	return strings.Join(ws, " ")
}

// refID is the token id of a word, computed here with hash/fnv directly and
// not with the double's own function.
func refID(w string) int32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(w))
	return int32(h.Sum32()%100000) + 1
}

// eventually polls cond every 10 ms until it holds or d has passed.
func eventually(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("not within %v: %s", d, what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// chatResult is the part of a non-streamed answer the tests read.
type chatResult struct {
	Choices []struct {
		Message struct {
			Role      string  `json:"role"`
			Content   *string `json:"content"`
			Reasoning *string `json:"reasoning"`
			ToolCalls []struct {
				ID       string `json:"id"`
				Type     string `json:"type"`
				Function struct {
					Name      string `json:"name"`
					Arguments string `json:"arguments"`
				} `json:"function"`
			} `json:"tool_calls"`
		} `json:"message"`
		FinishReason string  `json:"finish_reason"`
		TokenIDs     []int32 `json:"token_ids"`
	} `json:"choices"`
	Usage struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
		TotalTokens      int `json:"total_tokens"`
	} `json:"usage"`
	SystemFingerprint *string `json:"system_fingerprint"`
}

func (s *testServer) chat(body map[string]any) chatResult {
	s.t.Helper()
	r := s.post("/v1/chat/completions", body)
	if r.status != 200 {
		s.t.Fatalf("chat: HTTP %d: %s", r.status, r.body)
	}
	var out chatResult
	r.json(s.t, &out)
	if len(out.Choices) != 1 {
		s.t.Fatalf("chat: %d choices: %s", len(out.Choices), r.body)
	}
	return out
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func decodeLines(t *testing.T, lines []string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, l := range lines {
		var m map[string]any
		if err := json.Unmarshal([]byte(l), &m); err != nil {
			t.Fatalf("log line %q: %v", l, err)
		}
		out = append(out, m)
	}
	return out
}

func toStrings(v any) []string {
	var out []string
	list, _ := v.([]any)
	for _, x := range list {
		s, _ := x.(string)
		out = append(out, s)
	}
	return out
}
