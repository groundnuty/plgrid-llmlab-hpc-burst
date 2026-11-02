package front

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/enginedouble"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
)

// The engine double (package enginedouble, which the one-host rehearsal uses
// as its engine) serves POST /v1/completions in vLLM v0.32.0's shape. These
// tests check that the front reads that shape as it reads vLLM's replies, so
// a rehearsal that sends completions through the front is judged right.
//
// The expected values are counted from the request, not read from the double:
// a prompt of 3 words is 3 prompt tokens (the double's documented tokenizer),
// and min_tokens equal to max_tokens fixes the reply at max_tokens tokens
// and the finish reason "length" (the double's documented generation rules).
// The double lives in a package the front otherwise does not use, and only
// its tests import it.

const (
	cdPrompt     = "alpha beta gamma"
	cdPromptToks = 3
	cdTokens     = 5
	cdWhole      = `{"model":"` + scModel + `","prompt":"` + cdPrompt + `","max_tokens":5,"min_tokens":5}`
	cdStreamed   = `{"model":"` + scModel + `","prompt":"` + cdPrompt + `","max_tokens":5,"min_tokens":5,"stream":true,"stream_options":{"include_usage":true}}`
	cdNoUsage    = `{"model":"` + scModel + `","prompt":"` + cdPrompt + `","max_tokens":5,"min_tokens":5,"stream":true}`
)

// cdDouble is the engine double with no token delay, serving scModel.
func cdDouble() http.Handler {
	cfg := enginedouble.DefaultConfig()
	cfg.ServedName, cfg.TokenDelay = scModel, 0
	return enginedouble.Handler(cfg)
}

// cdEngine serves the double on a loopback port of the test block, as an
// Instance's engine (rawEngine reads the request body before its handler runs,
// and the double needs it).
func cdEngine(t *testing.T) string {
	t.Helper()
	ln, err := enginetest.Listen(testPortLo, testPortHi)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: cdDouble(), ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return ln.Addr().String()
}

// cdAsk plays one request against the double in process and returns the body.
func cdAsk(t *testing.T, body string) string {
	t.Helper()
	rec := httptest.NewRecorder()
	cdDouble().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/completions", strings.NewReader(body)))
	if rec.Code != http.StatusOK {
		t.Fatalf("the double answered HTTP %d: %s", rec.Code, rec.Body)
	}
	return rec.Body.String()
}

// TestJudgeReadsTheDoublesCompletionsStream: the judge, run over the events
// of a completions stream from the double, sees what the front's rules need:
// each token chunk is a content event (the "text" field), the finish reason
// is on the last of them, the usage chunk carries the counts, and [DONE]
// completes the stream.
func TestJudgeReadsTheDoublesCompletionsStream(t *testing.T) {
	body := cdAsk(t, cdStreamed)
	j := &judge{kind: endpointCompletions}
	events := strings.SplitAfter(body, "\n\n")
	var done bool
	n := 0
	for _, ev := range events {
		if ev == "" {
			continue
		}
		n++
		forward, fin := j.event([]byte(ev))
		if !forward {
			t.Errorf("event %d (%q) is not forwarded", n, ev)
		}
		if fin {
			done = true
			if ev != "data: [DONE]\n\n" {
				t.Errorf("the judge ended the stream at %q, want [DONE]", ev)
			}
		}
	}
	if n != cdTokens+2 { // a chunk per token, the usage chunk, [DONE]
		t.Fatalf("%d events, want %d: %q", n, cdTokens+2, body)
	}
	if !done || !j.complete || !j.sawFinish || j.finish != "length" {
		t.Errorf("done %v complete %v sawFinish %v finish %q, want a complete stream that finished with length", done, j.complete, j.sawFinish, j.finish)
	}
	if j.contentEvents != cdTokens {
		t.Errorf("contentEvents = %d, want %d: a text chunk per token and none for the usage chunk", j.contentEvents, cdTokens)
	}
	if j.usage == nil || j.usage.prompt == nil || j.usage.completion == nil || *j.usage.prompt != cdPromptToks || *j.usage.completion != cdTokens {
		t.Errorf("usage = %+v, want prompt %d and completion %d", j.usage, cdPromptToks, cdTokens)
	}
	if j.usage != nil && j.usage.cached != nil {
		t.Errorf("cached = %d: the double reports no cached count, vLLM does so only with prompt token details enabled", *j.usage.cached)
	}
}

// TestWholeResultReadsTheDoublesCompletion: the front's reading of a
// non-streamed completions body.
func TestWholeResultReadsTheDoublesCompletion(t *testing.T) {
	finish, u := wholeResult(endpointCompletions, []byte(cdAsk(t, cdWhole)))
	if finish == nil || *finish != "length" {
		t.Errorf("finish = %v, want length", finish)
	}
	if u == nil || u.prompt == nil || u.completion == nil || *u.prompt != cdPromptToks || *u.completion != cdTokens {
		t.Errorf("usage = %+v, want prompt %d and completion %d", u, cdPromptToks, cdTokens)
	}
}

// TestFrontRelaysTheDoublesCompletions: the real front, with the engine
// double behind it as the Instance's engine. The client gets the engine's
// answer unchanged and the usage record has what the front read from it.
func TestFrontRelaysTheDoublesCompletions(t *testing.T) {
	for _, c := range []struct {
		name, body  string
		wantSource  string
		wantContent int64 // contentEvents; -1 when the engine's usage is used
	}{
		{"whole", cdWhole, "engine", -1},
		{"streamed with usage", cdStreamed, "engine", -1},
		{"streamed without usage", cdNoUsage, "front_events", cdTokens},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := newScFix(t, scOpts{})
			f.retarget(f.uids[0], cdEngine(t))
			resp, body := f.prSend(t, "/v1/completions", c.body)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("HTTP %d: %s", resp.StatusCode, body)
			}
			if !strings.Contains(body, `"object":"text_completion"`) || (strings.Contains(c.body, `"stream":true`) && !strings.HasSuffix(body, "data: [DONE]\n\n")) {
				t.Errorf("the client's answer is not the double's completion: %q", body)
			}
			rec := f.record(resp.Header.Get("hpcburst-request-id"))
			if rec.Finish == nil || *rec.Finish != "length" {
				t.Errorf("finish = %s, want length", strStr(rec.Finish))
			}
			if rec.CountsSource == nil || *rec.CountsSource != c.wantSource {
				t.Errorf("countsSource = %s, want %s", strStr(rec.CountsSource), c.wantSource)
			}
			if c.wantContent < 0 {
				if !ptrIs(rec.PromptTokens, cdPromptToks) || !ptrIs(rec.CompletionTokens, cdTokens) {
					t.Errorf("tokens = %s prompt and %s completion, want %d and %d", ptrStr(rec.PromptTokens), ptrStr(rec.CompletionTokens), cdPromptToks, cdTokens)
				}
			} else if !ptrIs(rec.ContentEvents, c.wantContent) {
				t.Errorf("contentEvents = %s, want %d", ptrStr(rec.ContentEvents), c.wantContent)
			}
		})
	}
}
