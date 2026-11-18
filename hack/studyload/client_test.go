package main

// One request against canned servers: what is sent, and how each kind of answer is recorded. The timing arithmetic runs on a scripted
// clock, so its expected values are exact and no test here waits on a window.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// scriptedClock returns the given offsets from a fixed origin, one per call; after the last it repeats the last.
func scriptedClock(offsets ...time.Duration) func() time.Time {
	origin := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	var mu sync.Mutex
	i := 0
	return func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		d := offsets[min(i, len(offsets)-1)]
		i++
		return origin.Add(d)
	}
}

func testClient(t *testing.T, srv *httptest.Server, key string) *chatClient {
	t.Helper()
	cfg, err := ParseConfig([]byte(fmt.Sprintf(`{"baseURL": %q, "model": "m", "workloads": [
	  {"name": "w", "sessions": 1, "turns": 1, "contextTokens": 3, "outputTokens": 4, "headers": {"x-static": "yes"}}]}`, srv.URL+"/v1")))
	if err != nil {
		t.Fatal(err)
	}
	return newChatClient(cfg, key)
}

var oneWorkload = Workload{Name: "w", Sessions: 1, Turns: 1, ContextTokens: 3, OutputTokens: 4, Headers: map[string]string{"x-static": "yes"}}

func sse(events ...string) string {
	var b strings.Builder
	for _, e := range events {
		b.WriteString("data: " + e + "\n\n")
	}
	return b.String()
}

func content(s string) string {
	return `{"choices":[{"index":0,"delta":{"content":"` + s + `"}}]}`
}

func TestRequestCarriesTheHeadersAndBodyItIsGiven(t *testing.T) {
	var got http.Header
	var body map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Clone()
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &body)
		if r.URL.Path != "/v1/chat/completions" || r.Method != http.MethodPost {
			t.Errorf("%s %s, want POST /v1/chat/completions", r.Method, r.URL.Path)
		}
		fmt.Fprint(w, sse(content("a"), "[DONE]"))
	}))
	defer srv.Close()
	c := testClient(t, srv, "k-123")
	_, rec := c.chat(context.Background(), oneWorkload, "run-w-007", 1, []chatMessage{{"user", "hello there"}})
	if rec.Error != "" {
		t.Fatal(rec.Error)
	}
	for name, want := range map[string]string{"X-Session-Id": "run-w-007", "X-Static": "yes", "Authorization": "Bearer k-123", "Content-Type": "application/json"} {
		if v := got.Get(name); v != want {
			t.Errorf("header %s = %q, want %q", name, v, want)
		}
	}
	if body["model"] != "m" || body["stream"] != true || body["max_tokens"] != float64(4) || body["min_tokens"] != float64(4) {
		t.Errorf("body %v, want model m, stream true, max_tokens 4, min_tokens 4", body)
	}
	if so, _ := body["stream_options"].(map[string]any); so["include_usage"] != true {
		t.Errorf("stream_options %v, want include_usage true", body["stream_options"])
	}
}

func TestOmitMinTokensLeavesTheFieldOut(t *testing.T) {
	var body map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &body)
		fmt.Fprint(w, sse("[DONE]"))
	}))
	defer srv.Close()
	c := testClient(t, srv, "")
	c.omitMinTokens = true
	c.chat(context.Background(), oneWorkload, "s", 1, []chatMessage{{"user", "x"}})
	if _, has := body["min_tokens"]; has || body["max_tokens"] != float64(4) {
		t.Errorf("body %v: want max_tokens 4 and no min_tokens", body)
	}
}

func TestTimingAndUsageOnAScriptedClock(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Hpcburst-Request-Id", "req-9")
		w.Header().Set("Hpcburst-Provider", "prov")
		fmt.Fprint(w, sse(
			`{"choices":[{"delta":{"role":"assistant","content":""}}]}`, // the role chunk: no token yet
			content("one"), content(" two"), content(" three"),
			`{"choices":[],"usage":{"prompt_tokens":17,"completion_tokens":3,"total_tokens":20}}`, "[DONE]"))
	}))
	defer srv.Close()
	c := testClient(t, srv, "")
	// The clock is read at the start, at each content delta (the role chunk is not one), and at the end.
	c.now = scriptedClock(0, 100*time.Millisecond, 150*time.Millisecond, 200*time.Millisecond, 500*time.Millisecond)
	reply, rec := c.chat(context.Background(), oneWorkload, "s", 1, []chatMessage{{"user", "x"}})
	if rec.Error != "" {
		t.Fatal(rec.Error)
	}
	if reply != "one two three" {
		t.Errorf("reply %q, want %q", reply, "one two three")
	}
	// ttft 100 ms; 2 tokens after the first, over 100 ms between the first and the last delta: 20 tokens/s.
	if rec.TTFTMs == nil || *rec.TTFTMs != 100 || rec.DecodeTokensPerSec == nil || *rec.DecodeTokensPerSec != 20 {
		t.Errorf("ttft %v speed %v, want 100 ms and 20 tok/s", rec.TTFTMs, rec.DecodeTokensPerSec)
	}
	if rec.End.Sub(rec.Start) != 500*time.Millisecond || rec.Status != 200 {
		t.Errorf("start to end %v status %d, want 500ms and 200", rec.End.Sub(rec.Start), rec.Status)
	}
	if rec.PromptTokens == nil || *rec.PromptTokens != 17 || rec.CompletionTokens == nil || *rec.CompletionTokens != 3 {
		t.Errorf("usage %v/%v, want 17/3", rec.PromptTokens, rec.CompletionTokens)
	}
	if rec.RequestID != "req-9" || rec.Provider != "prov" || rec.Emitter != "" {
		t.Errorf("attribution %+v, want request id req-9, provider prov, no emitter", rec.Attribution)
	}
}

func TestEachFailureIsAClassNotBodyText(t *testing.T) {
	cases := []struct {
		name    string
		serve   func(w http.ResponseWriter)
		want    string
		status  int
		keepTTF bool
	}{
		{"status", func(w http.ResponseWriter) {
			w.Header().Set("Hpcburst-Provider", "prov")
			w.WriteHeader(503)
			fmt.Fprint(w, "BODY-TEXT-THAT-MUST-NOT-APPEAR")
		}, "status 503", 503, false},
		{"no DONE", func(w http.ResponseWriter) { fmt.Fprint(w, sse(content("a"))) }, "stream ended without [DONE]", 200, true},
		{"malformed", func(w http.ResponseWriter) { fmt.Fprint(w, sse("{not json BODY-TEXT-THAT-MUST-NOT-APPEAR")) }, "malformed event", 200, false},
		{"error event", func(w http.ResponseWriter) {
			fmt.Fprint(w, sse(`{"error":{"message":"BODY-TEXT-THAT-MUST-NOT-APPEAR"}}`))
		}, "error event", 200, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { tc.serve(w) }))
			defer srv.Close()
			_, rec := testClient(t, srv, "").chat(context.Background(), oneWorkload, "s", 1, nil)
			if rec.Error != tc.want || rec.Status != tc.status || (rec.TTFTMs != nil) != tc.keepTTF {
				t.Errorf("error %q status %d ttft-set %v, want %q %d %v", rec.Error, rec.Status, rec.TTFTMs != nil, tc.want, tc.status, tc.keepTTF)
			}
			if b, _ := json.Marshal(rec); strings.Contains(string(b), "BODY-TEXT") {
				t.Errorf("the record carries body text: %s", b)
			}
			if tc.name == "status" && rec.Provider != "prov" {
				t.Errorf("a 503 still has its attribution headers recorded, got provider %q", rec.Provider)
			}
		})
	}
}

func TestAHungRequestEndsAtItsTimeout(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-release }))
	defer srv.Close()
	defer close(release)
	c := testClient(t, srv, "")
	c.timeout = 50 * time.Millisecond
	_, rec := c.chat(context.Background(), oneWorkload, "s", 1, nil)
	if rec.Error != "transport error" || rec.Status != 0 {
		t.Errorf("error %q status %d, want transport error and 0", rec.Error, rec.Status)
	}
}

func TestNoUsageMeansNoCountsAndNoSpeed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, sse(content("a"), content(" b"), "[DONE]"))
	}))
	defer srv.Close()
	_, rec := testClient(t, srv, "").chat(context.Background(), oneWorkload, "s", 1, nil)
	if rec.Error != "" || rec.PromptTokens != nil || rec.CompletionTokens != nil || rec.DecodeTokensPerSec != nil || rec.TTFTMs == nil {
		t.Errorf("record %+v: want a ttft and no counts or speed", rec)
	}
}
