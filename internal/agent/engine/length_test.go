package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// stripMinTokens simulates an engine that ignores min_tokens: it removes the
// field from the request before the double sees it.
func stripMinTokens(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/chat/completions" {
			b, _ := io.ReadAll(r.Body)
			var body map[string]any
			_ = json.Unmarshal(b, &body)
			delete(body, "min_tokens")
			b, _ = json.Marshal(body)
			r.Body = io.NopCloser(bytes.NewReader(b))
			r.ContentLength = int64(len(b))
		}
		next.ServeHTTP(w, r)
	})
}

func fixedLength(round agentapi.WarmUpRound) []agentapi.RequestTiming {
	var out []agentapi.RequestTiming
	for _, q := range round.Requests {
		if q.Shape != agentapi.ShapeToolCall {
			out = append(out, q)
		}
	}
	return out
}

func wantOutput(s agentapi.Shape) int32 {
	if s == agentapi.ShapeLongOutput {
		return shapes.OutputLongest
	}
	return shapes.OutputTypical
}

// ignore_eos alone does not fix the length: vLLM merges the server's default
// stop-token ids, so a model that would stop after 10 tokens does. A warm-up
// request of a fixed-length shape asks for min_tokens = max_tokens, and gets
// exactly the output it asked for from the engine that stops early.
func TestWarmUpAsksForTheExactLength(t *testing.T) {
	c, rec := doubleWith(t, "early-stop=10", nil, nil)
	round := c.WarmUpRound(context.Background(), 1, agentapi.WarmUpRequest{Shapes: shapes, TargetSessions: 2}, testGreedy)
	timings := fixedLength(round)
	if len(timings) != 6 { // short, typical, longest, longOutput, 2 concurrent
		t.Fatalf("%d requests, want 6", len(timings))
	}
	for _, q := range timings {
		if q.Error != "" || q.OutputTokens != wantOutput(q.Shape) {
			t.Errorf("%s: %+v, want no error and %d output tokens", q.Shape, q, wantOutput(q.Shape))
		}
	}
	for _, req := range rec.sent() {
		if req["min_tokens"] != req["max_tokens"] || req["ignore_eos"] != true {
			t.Errorf("request min_tokens %v, max_tokens %v, ignore_eos %v; want min = max and ignore_eos", req["min_tokens"], req["max_tokens"], req["ignore_eos"])
		}
	}
}

// An engine that stops early whatever it was asked leaves the timing with an
// error that says so, and the count it did produce: the request counts for
// nothing to the controller (agentapi.RequestTiming).
func TestWarmUpDetectsAnEarlyStop(t *testing.T) {
	c, _ := doubleWith(t, "early-stop=10", nil, stripMinTokens)
	round := c.WarmUpRound(context.Background(), 1, agentapi.WarmUpRequest{Shapes: shapes, TargetSessions: 2}, testGreedy)
	for _, q := range fixedLength(round) {
		want := "stopped early: 10 of 64 tokens"
		if q.Shape == agentapi.ShapeLongOutput {
			want = "stopped early: 10 of 100 tokens"
		}
		if q.Error != want || q.OutputTokens != 10 {
			t.Errorf("%s: error %q with %d output tokens, want %q and 10", q.Shape, q.Error, q.OutputTokens, want)
		}
	}
}

// The corruption check sends min_tokens too: its three outputs are 64 tokens
// each, not whatever the engine stops at.
func TestCorruptionCheckAsksForTheExactLength(t *testing.T) {
	c, rec := doubleWith(t, "early-stop=10", nil, nil)
	own := shapes
	own.OutputTypical = 50 // the behaviour check's budget must not look like the corruption check's 64
	res := c.Qualify(context.Background(), agentapi.QualifyRequest{Shapes: own}, testGreedy)
	if o := outcomeOf(t, res, agentapi.CheckCorruption); !o.Passed {
		t.Fatalf("corruption: %+v", o)
	}
	var corruption int
	for _, req := range rec.sent() {
		if req["return_token_ids"] == true && req["max_tokens"] == float64(64) && req["tools"] == nil {
			corruption++
			if req["min_tokens"] != float64(64) {
				t.Errorf("a corruption request has min_tokens %v, want 64 (= max_tokens, twice CorruptionPrefix)", req["min_tokens"])
			}
		}
	}
	if corruption != 3 {
		t.Errorf("%d corruption requests, want 3", corruption)
	}
}

// Against a model that overruns, the count is wrong the other way.
func TestWarmUpOverrunMessage(t *testing.T) {
	got := timedOnce(t, sseHandler([]sseEvent2{
		{0, chunk(`{"role":"assistant","content":""}`, `null`)},
		{0, chunk(`{"content":"x"}`, `"length"`)},
		{0, usageChunk(9)}, {0, "[DONE]"},
	}), 5)
	if !strings.Contains(got.Error, "overran: 9 of 5 tokens") {
		t.Errorf("timing %+v, want an overrun error", got)
	}
}
