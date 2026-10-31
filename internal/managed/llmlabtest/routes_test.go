package llmlabtest

import (
	"net"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestRouteCompletions(t *testing.T) {
	s := startS(t)
	type completion struct {
		Object  string
		Model   string
		Choices []struct {
			Text         string
			FinishReason string `json:"finish_reason"`
		}
		Usage struct {
			PromptTokens     int `json:"prompt_tokens"`
			CompletionTokens int `json:"completion_tokens"`
		}
	}

	a := ask(t, s, http.MethodPost, routeCompletions, keyOne, `{"model":"example-9b","prompt":"one two three"}`)
	requireStatus(t, a, http.StatusOK, "completions")
	var c completion
	decode(t, a.Body, &c)
	if c.Object != "text_completion" || c.Model != modelA || len(c.Choices) != 1 || c.Choices[0].Text != DefaultReply ||
		c.Choices[0].FinishReason != "stop" || c.Usage.PromptTokens != 3 || c.Usage.CompletionTokens != 5 {
		t.Errorf("completions answer: %s", a.Body)
	}

	a = ask(t, s, http.MethodPost, routeCompletions, keyOne, `{"model":"example-9b","prompt":"one two three","stream":true}`)
	requireStatus(t, a, http.StatusOK, "streamed completions")
	if ct := a.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("Content-Type = %q, want text/event-stream", ct)
	}
	st := parseSSE(t, a.Body)
	if got := strings.Join(st.contents(), ""); got != DefaultReply || !st.Done || !slices.Equal(st.finishes(), []string{"stop"}) {
		t.Errorf("streamed completions: text %q done %v finishes %v", got, st.Done, st.finishes())
	}
}

func TestRouteResponses(t *testing.T) {
	s := startS(t)

	a := ask(t, s, http.MethodPost, routeResponses, keyOne, `{"model":"example-9b","instructions":"be brief","input":"one two three"}`)
	requireStatus(t, a, http.StatusOK, "responses")
	var r struct {
		Object string
		Status string
		Output []struct {
			Type    string
			Content []struct{ Type, Text string }
		}
		Usage struct {
			InputTokens  int `json:"input_tokens"`
			OutputTokens int `json:"output_tokens"`
		}
	}
	decode(t, a.Body, &r)
	if r.Object != "response" || r.Status != "completed" || len(r.Output) != 1 || len(r.Output[0].Content) != 1 ||
		r.Output[0].Content[0].Text != DefaultReply || r.Usage.InputTokens != 5 || r.Usage.OutputTokens != 5 {
		t.Errorf("responses answer: %s", a.Body)
	}

	a = ask(t, s, http.MethodPost, routeResponses, keyOne, `{"model":"example-9b","input":"one two three","stream":true}`)
	requireStatus(t, a, http.StatusOK, "streamed responses")
	st := parseSSE(t, a.Body)
	if n := len(st.Events); n < 3 || st.Events[0] != "response.created" || st.Events[n-1] != "response.completed" {
		t.Errorf("event names = %v, want response.created first and response.completed last", st.Events)
	}
	deltas := 0
	for _, e := range st.Events {
		if e == "response.output_text.delta" {
			deltas++
		}
	}
	if deltas != 5 {
		t.Errorf("%d text deltas, want 5, one per word", deltas)
	}
	if st.Done || !strings.Contains(string(a.Body), `"text":"`+DefaultReply+`"`) {
		t.Errorf("a responses stream ends with response.completed carrying the text, with no [DONE]: done %v\n%s", st.Done, a.Body)
	}
}

func TestRouteEmbeddings(t *testing.T) {
	s := startS(t)
	type embeddings struct {
		Object string
		Model  string
		Data   []struct {
			Object    string
			Index     int
			Embedding []float64
		}
		Usage struct {
			PromptTokens int `json:"prompt_tokens"`
		}
	}
	for _, c := range []struct {
		name, input string
		vectors     int
		tokens      int
	}{
		{"one string", `"one two three"`, 1, 3},
		{"an array of one", `["one two"]`, 1, 2},
		{"an array of three", `["one two","three","four five six"]`, 3, 6},
	} {
		t.Run(c.name, func(t *testing.T) {
			a := ask(t, s, http.MethodPost, routeEmbeddings, keyOne, `{"model":"example-9b","input":`+c.input+`}`)
			requireStatus(t, a, http.StatusOK, "embeddings")
			var e embeddings
			decode(t, a.Body, &e)
			if e.Object != "list" || e.Model != modelA || len(e.Data) != c.vectors || e.Usage.PromptTokens != c.tokens {
				t.Fatalf("embeddings answer: %s", a.Body)
			}
			for i, d := range e.Data {
				if d.Object != "embedding" || d.Index != i || len(d.Embedding) == 0 {
					t.Errorf("data[%d] = %+v, want an embedding with index %d and a vector", i, d, i)
				}
			}
		})
	}
}

func TestUnknownRoutesAndMethods(t *testing.T) {
	s := startS(t)

	for _, c := range []struct{ method, path string }{
		{http.MethodGet, "/v1/unknown"},
		{http.MethodPost, "/v1/unknown"},
		{http.MethodGet, "/"},
		{http.MethodGet, "/v2/models"},
		{http.MethodPost, routeChat + "/extra"},
		{http.MethodGet, "/v1/models/"},
	} {
		requireStatus(t, ask(t, s, c.method, c.path, keyOne, ""), http.StatusNotFound, c.method+" "+c.path)
	}

	for _, c := range []struct{ method, path, allow string }{
		{http.MethodGet, routeChat, http.MethodPost},
		{http.MethodGet, routeCompletions, http.MethodPost},
		{http.MethodGet, routeResponses, http.MethodPost},
		{http.MethodGet, routeEmbeddings, http.MethodPost},
		{http.MethodDelete, routeChat, http.MethodPost},
		{http.MethodPost, routeModels, http.MethodGet},
		{http.MethodPost, routeModels + "/" + modelA, http.MethodGet},
	} {
		a := ask(t, s, c.method, c.path, keyOne, "")
		requireStatus(t, a, http.StatusMethodNotAllowed, c.method+" "+c.path)
		if got := a.Header.Get("Allow"); got != c.allow {
			t.Errorf("%s %s: Allow = %q, want %q", c.method, c.path, got, c.allow)
		}
	}
}

// A body without a string model gets 400, on every inference route.
func TestBodyWithoutAModel(t *testing.T) {
	s := startS(t)
	for _, route := range []string{routeChat, routeCompletions, routeResponses, routeEmbeddings} {
		for _, body := range []string{
			`{"messages":[]}`,
			`{"model":5,"messages":[]}`,
			`{"model":null}`,
			`{"model":""}`,
			`{"model":["example-9b"]}`,
			`[]`,
			`not json`,
			``,
		} {
			a := ask(t, s, http.MethodPost, route, keyOne, body)
			requireStatus(t, a, http.StatusBadRequest, route+" "+body)
			parseError(t, a.Body)
		}
	}
	// With a string model the same routes answer.
	requireStatus(t, postChat(t, s, modelA), http.StatusOK, "a body with a model")
}

// Close returns at once even with a request held, and the held request does
// not get an answer (it ends in an error).
func TestCloseDoesNotHang(t *testing.T) {
	closeWithin2s := func(t *testing.T, s *Server) {
		t.Helper()
		closed := make(chan struct{})
		go func() { s.Close(); close(closed) }()
		select {
		case <-closed:
		case <-time.After(2 * time.Second):
			t.Fatal("Close did not return within 2s with a request held")
		}
	}

	t.Run("a request held by a stall", func(t *testing.T) {
		s := startS(t)
		s.Stall(true)
		ch := askAsync(s, http.MethodPost, routeChat, keyOne, chatReq(modelA))
		requirePending(t, ch, held, "chat in a stall")
		if n := len(s.Requests()); n != 0 {
			t.Fatalf("%d requests logged while the only one is held", n)
		}

		closeWithin2s(t, s)
		select {
		case a := <-ch:
			if a.Err == nil && a.Status >= 200 && a.Status < 300 {
				t.Errorf("the held request got a %d answer from a Server that was closing", a.Status)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("the held client request never ended")
		}
		logs := s.Requests()
		if len(logs) != 1 || logs[0].Status != 0 || !logs[0].FirstByte.IsZero() {
			t.Errorf("request log after Close = %+v, want one request with status 0 and no first byte", logs)
		}
		s.Close() // a second Close is harmless
	})

	t.Run("a stream held between chunks", func(t *testing.T) {
		s := startS(t)
		s.SetLatency(Latency{PerChunk: time.Hour})
		ch := askAsync(s, http.MethodPost, routeChat, keyOne, streamReq(modelA))
		requirePending(t, ch, held, "stream between chunks")

		closeWithin2s(t, s)
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
			t.Fatal("the held stream never ended")
		}
		if logs := s.Requests(); len(logs) != 1 || logs[0].Status != http.StatusOK {
			t.Errorf("request log after Close = %+v, want the one stream, status 200", logs)
		}
	})
}

// The Server and Listen bind 127.0.0.1, on the first free port of the range.
func TestListen(t *testing.T) {
	ln, err := Listen(testPortLo, testPortHi)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	taken := ln.Addr().(*net.TCPAddr)
	if taken.IP.String() != "127.0.0.1" || taken.Port < testPortLo || taken.Port > testPortHi {
		t.Fatalf("Listen bound %v, want 127.0.0.1 in %d-%d", taken, testPortLo, testPortHi)
	}

	s := startS(t)
	u, err := url.Parse(s.URL())
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(u.Port())
	if err != nil {
		t.Fatal(err)
	}
	if u.Hostname() != "127.0.0.1" || port < testPortLo || port > testPortHi || port == taken.Port {
		t.Errorf("Server at %s: want 127.0.0.1, in %d-%d, and not the taken port %d", s.URL(), testPortLo, testPortHi, taken.Port)
	}
	if s.BaseURL() != s.URL()+"/v1" {
		t.Errorf("BaseURL = %q, want %q", s.BaseURL(), s.URL()+"/v1")
	}

	if _, err := Listen(taken.Port, taken.Port); err == nil {
		t.Error("Listen on a port that is taken succeeded")
	}
	for _, r := range [][2]int{{1024, 2000}, {testPortHi, testPortLo}, {testPortLo, 70000}, {0, 0}} {
		if l, err := Listen(r[0], r[1]); err == nil {
			l.Close()
			t.Errorf("Listen(%d, %d) succeeded, want an error for the range", r[0], r[1])
		}
	}
}
