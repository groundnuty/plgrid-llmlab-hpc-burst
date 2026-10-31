package llmlabtest

import (
	"encoding/json"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// Names the behaviour tests share. The route strings are literals on purpose:
// a test that took them from the double's own constants could not catch a
// wrong path.
const (
	modelA = "example-9b"
	modelB = "example-27b"
	modelC = "example-70b"

	keyOne = testKey // "example-key-0001"
	keyTwo = "example-key-0002"

	threeWords = "one two three"

	routeModels      = "/v1/models"
	routeChat        = "/v1/chat/completions"
	routeCompletions = "/v1/completions"
	routeResponses   = "/v1/responses"
	routeEmbeddings  = "/v1/embeddings"
)

// testClient has a time-out, so a bug in the double fails a test instead of
// hanging the test binary. It opens a connection per request, so no test
// inherits a connection from an earlier Server.
var testClient = &http.Client{
	Timeout:   10 * time.Second,
	Transport: &http.Transport{DisableKeepAlives: true},
}

// answer is what a client got: a reply, or the error that ended the request.
type answer struct {
	Status int
	Header http.Header
	Body   []byte
	Err    error
	Took   time.Duration
	// HeadersAfter is how long the response headers took to arrive, and
	// HeadersAt when (time.Now, the Server's default clock) they did.
	HeadersAfter time.Duration
	HeadersAt    time.Time
}

func bearer(key string) string {
	if key == "" {
		return ""
	}
	return "Bearer " + key
}

// send makes one request and never fails a test, so a goroutine may call it.
// auth is the whole Authorization header, empty for none.
func send(s *Server, method, path, auth, body string) answer {
	start := time.Now()
	req, err := http.NewRequest(method, s.URL()+path, strings.NewReader(body))
	if err != nil {
		return answer{Err: err}
	}
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	resp, err := testClient.Do(req)
	headersAt := time.Now() // Do returns when the response headers have arrived
	if err != nil {
		return answer{Err: err, Took: time.Since(start)}
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return answer{Status: resp.StatusCode, Header: resp.Header, Body: b, Err: err, Took: time.Since(start),
		HeadersAfter: headersAt.Sub(start), HeadersAt: headersAt}
}

// askAuth is send for the test goroutine: an error fails the test.
func askAuth(t testing.TB, s *Server, method, path, auth, body string) answer {
	t.Helper()
	a := send(s, method, path, auth, body)
	if a.Err != nil {
		t.Fatalf("%s %s: %v", method, path, a.Err)
	}
	return a
}

// ask sends the request with "Bearer key"; an empty key sends no header.
func ask(t testing.TB, s *Server, method, path, key, body string) answer {
	t.Helper()
	return askAuth(t, s, method, path, bearer(key), body)
}

// askAsync sends the request in a goroutine and returns the channel its
// answer arrives on.
func askAsync(s *Server, method, path, key, body string) <-chan answer {
	ch := make(chan answer, 1)
	go func() { ch <- send(s, method, path, bearer(key), body) }()
	return ch
}

func postChat(t testing.TB, s *Server, model string) answer {
	t.Helper()
	return ask(t, s, http.MethodPost, routeChat, keyOne, chatReq(model))
}

func chatReq(model string) string {
	return `{"model":"` + model + `","messages":[{"role":"user","content":"` + threeWords + `"}]}`
}

func streamReq(model string) string {
	return `{"model":"` + model + `","stream":true,"messages":[{"role":"user","content":"` + threeWords + `"}]}`
}

// startS starts a Server on issue #69's ports with one key (keyOne) and the
// models, modelA alone when none is given.
func startS(t testing.TB, models ...Model) *Server {
	t.Helper()
	return startWith(t, nil, models...)
}

// startWith is startS with the Server's clock.
func startWith(t testing.TB, now func() time.Time, models ...Model) *Server {
	t.Helper()
	if len(models) == 0 {
		models = []Model{{ID: modelA}}
	}
	return Start(t, Options{PortLo: testPortLo, PortHi: testPortHi, Now: now, Keys: []string{keyOne}, Models: models})
}

// requirePending fails when the request answers within d.
func requirePending(t testing.TB, ch <-chan answer, d time.Duration, what string) {
	t.Helper()
	select {
	case a := <-ch:
		t.Fatalf("%s: answered (status %d, err %v) within %v, want it still waiting", what, a.Status, a.Err, d)
	case <-time.After(d):
	}
}

// pollUntil polls cond every few milliseconds for up to 5 s, and fails with
// what when it never holds.
func pollUntil(t testing.TB, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("%s: still not so after 5s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// requireWaiting polls until n requests are blocked in a wait of the Server:
// the barrier a test needs before it releases a held request. A request that
// has not arrived yet, or that has not met the stall, is not counted.
func requireWaiting(t testing.TB, s *Server, n int, what string) {
	t.Helper()
	pollUntil(t, what+": Waiting() == "+strconv.Itoa(n), func() bool { return s.Waiting() == n })
}

// requireHeld waits until the one request behind ch is blocked in the Server,
// then checks that it has not been answered. Nothing is slept: a request that
// is blocked in a wait cannot have an answer.
func requireHeld(t testing.TB, s *Server, ch <-chan answer, what string) {
	t.Helper()
	requireWaiting(t, s, 1, what)
	select {
	case a := <-ch:
		t.Fatalf("%s: answered (status %d, err %v) while blocked in a wait", what, a.Status, a.Err)
	default:
	}
}

// requireAnswer waits up to within for the request's answer and fails on an
// error.
func requireAnswer(t testing.TB, ch <-chan answer, within time.Duration, what string) answer {
	t.Helper()
	select {
	case a := <-ch:
		if a.Err != nil {
			t.Fatalf("%s: %v", what, a.Err)
		}
		return a
	case <-time.After(within):
		t.Fatalf("%s: no answer within %v", what, within)
	}
	return answer{}
}

// waitLogged returns the request log once it holds at least n entries. A
// stream's last bytes can reach the client before the Server logs it.
func waitLogged(t testing.TB, s *Server, n int) []Request {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if got := s.Requests(); len(got) >= n {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("the request log has %d entries after 5s, want %d", len(s.Requests()), n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// manualClock is a clock a test moves by hand. It never calls the Server.
type manualClock struct {
	mu  sync.Mutex
	now time.Time
}

func newManualClock() *manualClock {
	return &manualClock{now: time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)}
}

func (c *manualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *manualClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t
}

func (c *manualClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func decode(t testing.TB, b []byte, v any) {
	t.Helper()
	if err := json.Unmarshal(b, v); err != nil {
		t.Fatalf("not JSON (%v): %s", err, b)
	}
}

// errorBody is the assumed shape of every error body (package doc).
type errorBody struct {
	Error struct {
		Message string  `json:"message"`
		Type    string  `json:"type"`
		Param   *string `json:"param"`
		Code    string  `json:"code"`
	} `json:"error"`
}

func parseError(t testing.TB, b []byte) errorBody {
	t.Helper()
	var e errorBody
	decode(t, b, &e)
	if e.Error.Message == "" {
		t.Fatalf("no error message in %s", b)
	}
	return e
}

// listed returns the IDs GET /v1/models shows, in its order.
func listed(t testing.TB, s *Server) []string {
	t.Helper()
	a := ask(t, s, http.MethodGet, routeModels, keyOne, "")
	if a.Status != http.StatusOK {
		t.Fatalf("listing = %d %s", a.Status, a.Body)
	}
	var l struct {
		Object string `json:"object"`
		Data   []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	decode(t, a.Body, &l)
	if l.Object != "list" {
		t.Fatalf("listing object = %q, want list", l.Object)
	}
	ids := []string{}
	for _, m := range l.Data {
		ids = append(ids, m.ID)
	}
	return ids
}

func requireListed(t testing.TB, s *Server, want ...string) {
	t.Helper()
	if want == nil {
		want = []string{}
	}
	if got := listed(t, s); !slices.Equal(got, want) {
		t.Fatalf("listing = %v, want %v", got, want)
	}
}

// requireStatus fails unless the request got want.
func requireStatus(t testing.TB, got answer, want int, what string) {
	t.Helper()
	if got.Status != want {
		t.Fatalf("%s: status %d, want %d: %s", what, got.Status, want, got.Body)
	}
}

// mustPanic fails unless f panics.
func mustPanic(t testing.TB, what string, f func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Errorf("%s did not panic", what)
		}
	}()
	f()
}

// sseChunk is the part of a stream event the tests read: a chat chunk's delta
// and a completions chunk's text.
type sseChunk struct {
	Choices []struct {
		Delta        struct{ Content string } `json:"delta"`
		Text         string                   `json:"text"`
		FinishReason *string                  `json:"finish_reason"`
	} `json:"choices"`
	Usage *struct {
		PromptTokens int `json:"prompt_tokens"`
	} `json:"usage"`
}

// sseStream is a parsed server-sent-event body.
type sseStream struct {
	Events []string // the "event:" names, in order
	Chunks []sseChunk
	Done   bool // "data: [DONE]" came
}

func parseSSE(t testing.TB, body []byte) sseStream {
	t.Helper()
	var st sseStream
	for _, line := range strings.Split(string(body), "\n") {
		if name, ok := strings.CutPrefix(line, "event: "); ok {
			st.Events = append(st.Events, name)
			continue
		}
		data, ok := strings.CutPrefix(line, "data: ")
		if !ok {
			continue
		}
		if data == "[DONE]" {
			st.Done = true
			continue
		}
		var c sseChunk
		if err := json.Unmarshal([]byte(data), &c); err != nil {
			t.Fatalf("stream event is not JSON (%v): %s", err, data)
		}
		st.Chunks = append(st.Chunks, c)
	}
	return st
}

// contents are the non-empty pieces of generated text, in order.
func (st sseStream) contents() []string {
	var out []string
	for _, c := range st.Chunks {
		for _, ch := range c.Choices {
			switch {
			case ch.Delta.Content != "":
				out = append(out, ch.Delta.Content)
			case ch.Text != "":
				out = append(out, ch.Text)
			}
		}
	}
	return out
}

// finishes are the non-null finish reasons, in order.
func (st sseStream) finishes() []string {
	var out []string
	for _, c := range st.Chunks {
		for _, ch := range c.Choices {
			if ch.FinishReason != nil {
				out = append(out, *ch.FinishReason)
			}
		}
	}
	return out
}
