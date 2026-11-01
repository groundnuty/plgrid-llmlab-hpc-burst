package engine

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/enginedouble"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// Every request the agent sends the engine sets every sampling parameter its
// checks rely on (HBEP-7, The allocation agent, Checks; science-agent rulings,
// 1 Nov 2026, issues #125 and #151). Verification, every qualification check
// and every warm-up shape are sent here and recorded at the engine double.
// The expected values are written out by hand, never computed by
// agentapi.CheckSampling.

// qwen3Sampled is the sampling a reasoning model with Qwen3-0.6B's
// generation_config.json (temperature 0.6, top_k 20, top_p 0.95) gets, as
// HBEP-7's Test plan states it: those three, min_p 0, the penalties neutral,
// and a seed per check.
var qwen3Sampled = agentapi.Sampling{
	Temperature: 0.6, TopP: 0.95, TopK: i64(20), MinP: f64(0),
	RepetitionPenalty: 1, PresencePenalty: 0, FrequencyPenalty: 0,
	Seeds: map[string]int64{"verify": 1101, "chat": 1102, "toolCall": 1103, "corruption": 1104, "behaviour": 1105, "warmUp": 1106},
}

// recordingDouble returns a Client of an engine double and a func that shuts
// the double down, waiting for every handler to return so that every request's
// record has been made, and returns what the double recorded in the order it
// was recorded.
func recordingDouble(t *testing.T, modes string, opts ...Option) (*Client, func() []enginedouble.LogEvent) {
	t.Helper()
	cfg, err := enginedouble.ParseModes(modes)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ServedName, cfg.TokenDelay = "m", 0
	var mu sync.Mutex
	var events []enginedouble.LogEvent
	h := enginedouble.HandlerRecording(cfg, func(ev enginedouble.LogEvent) {
		mu.Lock()
		events = append(events, ev)
		mu.Unlock()
	})
	// A short directory: a Unix socket path is limited to about 100 bytes.
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
	return New(socket, "key", opts...), func() []enginedouble.LogEvent {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			t.Fatalf("shutting the double down: %v", err)
		}
		mu.Lock()
		defer mu.Unlock()
		return append([]enginedouble.LogEvent(nil), events...)
	}
}

// runEveryCheck runs verification, qualification with the tool-call checks on
// and one warm-up round of two concurrent requests, all sampled as s says.
func runEveryCheck(t *testing.T, c *Client, s agentapi.Sampling) {
	t.Helper()
	ctx := context.Background()
	if v := c.Verify(ctx, s); v.Error != "" {
		t.Fatalf("verify: %s", v.Error)
	}
	q := c.Qualify(ctx, agentapi.QualifyRequest{Shapes: shapes, ToolCalls: true}, s)
	for _, o := range q.Checks {
		if !o.Passed {
			t.Errorf("qualification: %s failed: %s %s", o.Name, o.Reason, o.Message)
		}
	}
	for _, r := range c.WarmUpRound(ctx, 1, agentapi.WarmUpRequest{Shapes: shapes, ToolCalls: true, TargetSessions: 2}, s).Requests {
		if r.Error != "" {
			t.Errorf("warm-up %s: %s", r.Shape, r.Error)
		}
	}
}

// checkOf names the check a recorded request belongs to by the closing words
// of its prompt, written out here from the agent's fixed prompts and not read
// from the agent's constants.
func checkOf(tail string) string {
	switch {
	case strings.HasSuffix(tail, "Reply with one word: ready."):
		return "verify"
	case strings.HasSuffix(tail, "Say hello in one short sentence."):
		return "chat"
	case strings.HasSuffix(tail, "Call the get_weather tool to find out."):
		return "toolCall"
	case strings.HasSuffix(tail, "separated by commas."):
		return "corruption"
	case strings.Contains(tail, "Ignore the list of words above."):
		return "behaviour"
	case strings.HasSuffix(tail, "Continue the list of words above."), strings.HasSuffix(tail, "Call the get_weather tool for the city of Krakow."):
		return "warmUp"
	}
	return "unknown: " + tail
}

// requestsPerCheck is how many requests each check sends in runEveryCheck,
// counted by hand: verification's fingerprint 1; qualification's chat 1, tool
// call 1, corruption 3, behaviour 1; one warm-up round: four sequential shapes,
// the tool-call shape, and two concurrent requests.
var requestsPerCheck = map[string]int{"verify": 1, "chat": 1, "toolCall": 1, "corruption": 3, "behaviour": 1, "warmUp": 7}

func countByCheck(events []enginedouble.LogEvent) map[string]int {
	n := map[string]int{}
	for _, ev := range events {
		n[checkOf(ev.Tail)]++
	}
	return n
}

// A reasoning model's checks: every request that reaches the engine carries
// the model's own values, min_p 0, neutral penalties and a seed; the seed is
// the same on every request of one check and differs between checks. No check
// is sent twice. The checks all pass on the double, so the corruption check's
// three requests, one seed, drew the same samples.
func TestASampledModelsEveryRequestCarriesItsValuesAndItsChecksSeed(t *testing.T) {
	c, finish := recordingDouble(t, "reasoning-first=20", WithReasoning(true))
	runEveryCheck(t, c, qwen3Sampled)
	events := finish()

	if got := countByCheck(events); !reflect.DeepEqual(got, requestsPerCheck) {
		t.Errorf("requests per check = %v, want %v", got, requestsPerCheck)
	}
	wantSeed := map[string]int64{"verify": 1101, "chat": 1102, "toolCall": 1103, "corruption": 1104, "behaviour": 1105, "warmUp": 1106}
	for i, ev := range events {
		check := checkOf(ev.Tail)
		want := enginedouble.RequestSampling{
			Temperature: f64(0.6), TopP: f64(0.95), TopK: i64(20), MinP: f64(0),
			RepetitionPenalty: f64(1), PresencePenalty: f64(0), FrequencyPenalty: f64(0), Seed: i64(wantSeed[check]),
		}
		if ev.Sampling == nil || !reflect.DeepEqual(*ev.Sampling, want) {
			t.Errorf("request %d (%s): sampling %+v, want %+v", i, check, ev.Sampling, want)
		}
		if ev.Outcome != "completed" {
			t.Errorf("request %d (%s): outcome %q, want completed", i, check, ev.Outcome)
		}
	}
}

// A model without a reasoning parser (or a reasoning model that stays greedy)
// gets HBEP-7's greedy values on every request and no top_k, min_p or seed.
func TestAGreedyModelsEveryRequestCarriesTheGreedyValuesAndNoSeed(t *testing.T) {
	c, finish := recordingDouble(t, "")
	runEveryCheck(t, c, testGreedy)
	events := finish()

	if got := countByCheck(events); !reflect.DeepEqual(got, requestsPerCheck) {
		t.Errorf("requests per check = %v, want %v", got, requestsPerCheck)
	}
	want := enginedouble.RequestSampling{
		Temperature: f64(0), TopP: f64(1), RepetitionPenalty: f64(1), PresencePenalty: f64(0), FrequencyPenalty: f64(0),
	}
	for i, ev := range events {
		if ev.Sampling == nil || !reflect.DeepEqual(*ev.Sampling, want) {
			t.Errorf("request %d (%s): sampling %+v, want %+v", i, checkOf(ev.Tail), ev.Sampling, want)
		}
	}
}

// On the wire a greedy request holds the five values (temperature 0 among
// them: a zero is sent, not left out) and none of top_k, min_p and seed; a
// sampled one holds all eight.
func TestTheWireFormOfGreedyAndSampledRequests(t *testing.T) {
	for _, tc := range []struct {
		name string
		s    agentapi.Sampling
		want map[string]any
	}{
		{"greedy", testGreedy, map[string]any{
			"temperature": 0.0, "top_p": 1.0, "repetition_penalty": 1.0, "presence_penalty": 0.0, "frequency_penalty": 0.0}},
		{"sampled", qwen3Sampled, map[string]any{
			"temperature": 0.6, "top_p": 0.95, "top_k": 20.0, "min_p": 0.0, "repetition_penalty": 1.0,
			"presence_penalty": 0.0, "frequency_penalty": 0.0, "seed": 1102.0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, rec := doubleWith(t, "", nil, nil)
			if _, err := c.chatCheck(context.Background(), tc.s); err != nil {
				t.Fatal(err)
			}
			sent := rec.sent()
			if len(sent) != 1 {
				t.Fatalf("%d requests, want 1", len(sent))
			}
			for _, k := range []string{"temperature", "top_p", "top_k", "min_p", "repetition_penalty", "presence_penalty", "frequency_penalty", "seed"} {
				got, has := sent[0][k]
				w, want := tc.want[k]
				if has != want || (has && got != w) {
					t.Errorf("%s: sent %v (present %v), want %v (present %v)", k, got, has, w, want)
				}
			}
		})
	}
}

// Each check is one attempt, with no retry: against an engine that fails every
// request, the verification fingerprint, the chat check, the tool-call check
// and the behaviour check each send one request, the corruption check stops at
// its first of three, and each of the warm-up round's seven requests is sent
// once. The double records the refused requests too.
func TestNoCheckIsSentTwiceWhenTheEngineFails(t *testing.T) {
	c, finish := recordingDouble(t, "errors-only", WithReasoning(true))
	ctx := context.Background()
	if v := c.Verify(ctx, qwen3Sampled); v.Error == "" {
		t.Error("verification did not report the failed fingerprint")
	}
	q := c.Qualify(ctx, agentapi.QualifyRequest{Shapes: shapes, ToolCalls: true}, qwen3Sampled)
	for _, name := range []agentapi.CheckName{agentapi.CheckChat, agentapi.CheckToolCall, agentapi.CheckCorruption, agentapi.CheckBehaviour} {
		if o := outcomeOf(t, q, name); o.Passed {
			t.Errorf("%s passed against an engine that fails every request", name)
		}
	}
	c.WarmUpRound(ctx, 1, agentapi.WarmUpRequest{Shapes: shapes, ToolCalls: true, TargetSessions: 2}, qwen3Sampled)
	events := finish()

	want := map[string]int{"verify": 1, "chat": 1, "toolCall": 1, "corruption": 1, "behaviour": 1, "warmUp": 7}
	if got := countByCheck(events); !reflect.DeepEqual(got, want) {
		t.Errorf("attempts per check = %v, want %v", got, want)
	}
	for i, ev := range events {
		if ev.Outcome != "refused" {
			t.Errorf("request %d (%s): outcome %q, want refused", i, checkOf(ev.Tail), ev.Outcome)
		}
	}
}

// No request goes out without the sampling of its check. A chatRequest cannot
// be marshalled by itself; a sampled one needs top_k, min_p and its check's
// seed, so the zero Sampling (top_p 0, repetition_penalty 0: not greedy)
// never reaches an engine as a default. A greedy Sampling that carries seeds
// sends none.
func TestARequestCannotLeaveASamplingParameterOut(t *testing.T) {
	req := chatRequest{Messages: []message{{Role: "user", Content: "hi"}}, MaxTokens: 4}
	if _, err := json.Marshal(req); err == nil {
		t.Error("a chatRequest marshalled without its sampling")
	}
	noTopK := qwen3Sampled
	noTopK.TopK = nil
	noMinP := qwen3Sampled
	noMinP.MinP = nil
	noSeed := qwen3Sampled
	noSeed.Seeds = map[string]int64{"verify": 1101}
	for name, s := range map[string]agentapi.Sampling{"the zero Sampling": {}, "no top_k": noTopK, "no min_p": noMinP, "no seed for the check": noSeed} {
		if _, err := json.Marshal(req.sampled(s, "chat")); err == nil {
			t.Errorf("%s: a sampled request marshalled", name)
		}
	}
	greedyWithSeeds := testGreedy
	greedyWithSeeds.Seeds = map[string]int64{"chat": 1102}
	greedyWithSeeds.TopK = i64(20)
	b, err := json.Marshal(req.sampled(greedyWithSeeds, "chat"))
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{`"seed"`, `"top_k"`, `"min_p"`} {
		if strings.Contains(string(b), k) {
			t.Errorf("a greedy request carries %s: %s", k, b)
		}
	}
}

// A check sent through the client with a sampling that cannot go out fails
// with that error, and nothing reaches the engine.
func TestAMalformedSamplingSendsNothing(t *testing.T) {
	c, finish := recordingDouble(t, "")
	if _, err := c.chatCheck(context.Background(), agentapi.Sampling{}); err == nil {
		t.Error("the chat check passed with the zero Sampling")
	}
	if events := finish(); len(events) != 0 {
		t.Errorf("%d requests reached the engine, want none", len(events))
	}
}
