package engine

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/enginedouble"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// sseHandler answers every chat request with the raw events: each one a
// "data: <payload>" block, flushed, with the pause before it. The payloads are
// written by hand, as vLLM v0.32.0 writes them, so the tests do not share a
// reading of the stream with the double.
type sseEvent2 struct {
	pause   time.Duration
	payload string
}

func sseHandler(events []sseEvent2) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/models":
			_, _ = io.WriteString(w, `{"data":[{"id":"m","max_model_len":32768}]}`)
		case "/tokenize":
			_, _ = io.WriteString(w, `{"count":9}`)
		default:
			w.Header().Set("Content-Type", "text/event-stream")
			fl, _ := w.(http.Flusher)
			for _, ev := range events {
				time.Sleep(ev.pause)
				_, _ = fmt.Fprintf(w, "data: %s\n\n", ev.payload)
				fl.Flush()
			}
		}
	})
}

func chunk(delta, finish string) string {
	return fmt.Sprintf(`{"id":"c","object":"chat.completion.chunk","model":"m","choices":[{"index":0,"delta":%s,"finish_reason":%s}]}`, delta, finish)
}

func usageChunk(completion int) string {
	return fmt.Sprintf(`{"id":"c","object":"chat.completion.chunk","model":"m","choices":[],"usage":{"prompt_tokens":9,"completion_tokens":%d,"total_tokens":%d}}`, completion, completion+9)
}

func timedOnce(t *testing.T, h http.Handler, out int32) agentapi.RequestTiming {
	t.Helper()
	c := New(serveOn(t, h), "key")
	return c.timed(context.Background(), warmShape{shape: agentapi.ShapeTypical, context: 0, output: out})
}

// A stream that is all reasoning, the whole budget spent before any content,
// is a healthy reply: its first token is the first reasoning delta, and its
// output is the usage count, which includes the reasoning. Five chunks stand
// for forty tokens (a chunk may carry several): the count is the engine's.
func TestWarmUpReasoningOnlyStream(t *testing.T) {
	role := chunk(`{"role":"assistant","content":""}`, `null`)
	think := func(s string) string { return chunk(`{"reasoning":"`+s+`"}`, `null`) }
	got := timedOnce(t, sseHandler([]sseEvent2{
		{0, role}, {0, think("a")}, {0, think(" b")}, {0, think(" c")}, {0, think(" d")},
		{0, chunk(`{"reasoning":" e"}`, `"length"`)}, {0, usageChunk(40)}, {0, "[DONE]"},
	}), 40)
	if got.Error != "" || got.OutputTokens != 40 || got.TimeToFirstToken <= 0 {
		t.Errorf("timing %+v: want no error, 40 output tokens (the usage count) and a first-token time", got)
	}
}

// Reasoning, then content: the first token is the first reasoning delta, so
// the time to the first token is the pause before it and not the pause before
// the content.
func TestWarmUpFirstTokenIsTheFirstReasoningDelta(t *testing.T) {
	got := timedOnce(t, sseHandler([]sseEvent2{
		{0, chunk(`{"role":"assistant","content":""}`, `null`)},
		{0, chunk(`{"reasoning":"thinking"}`, `null`)},
		{400 * time.Millisecond, chunk(`{"content":"answer"}`, `"stop"`)},
		{0, usageChunk(2)}, {0, "[DONE]"},
	}), 2)
	if got.Error != "" || got.OutputTokens != 2 {
		t.Fatalf("timing %+v", got)
	}
	if ttft := time.Duration(got.TimeToFirstToken); ttft > 200*time.Millisecond {
		t.Errorf("time to first token %v: the reasoning delta came at once, the content 400 ms later", ttft)
	}
	if total := time.Duration(got.Total); total < 350*time.Millisecond {
		t.Errorf("total %v, want at least the 400 ms pause", total)
	}
}

// A stream without a usage chunk cannot be counted: the timing carries the
// error, and chunks are never counted instead.
func TestWarmUpWithoutUsageIsAnError(t *testing.T) {
	got := timedOnce(t, sseHandler([]sseEvent2{
		{0, chunk(`{"role":"assistant","content":""}`, `null`)},
		{0, chunk(`{"content":"one"}`, `null`)},
		{0, chunk(`{"content":" two"}`, `"length"`)},
		{0, "[DONE]"},
	}), 2)
	if !strings.Contains(got.Error, "no usage") {
		t.Errorf("timing %+v: want an error saying the stream carried no usage", got)
	}
}

// Against the double: a model that reasons for 50 tokens has the time to its
// first token at the first reasoning delta, long before the first content.
func TestWarmUpTTFTWithReasoningDouble(t *testing.T) {
	c := double(t, "reasoning-first=50,token-delay=10ms", nil, WithReasoning(true))
	got := c.timed(context.Background(), warmShape{shape: agentapi.ShapeTypical, context: 20, output: 80})
	if got.Error != "" {
		t.Fatalf("timing %+v", got)
	}
	if ttft := time.Duration(got.TimeToFirstToken); ttft > 250*time.Millisecond {
		t.Errorf("time to first token %v, want about 10 ms: the content starts after 50 reasoning tokens of 10 ms", ttft)
	}
	if total := time.Duration(got.Total); total < 500*time.Millisecond {
		t.Errorf("total %v, want at least the 50 reasoning tokens", total)
	}
	if got.OutputTokens != 80 {
		t.Errorf("%d output tokens, want the 80 asked for", got.OutputTokens)
	}
}

// The chat check passes on text in either field: a model may spend its 32
// tokens thinking. A reply with neither fails.
func TestChatCheckAcceptsReasoningText(t *testing.T) {
	reply := func(message string) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/v1/models" {
				_, _ = io.WriteString(w, `{"data":[{"id":"m"}]}`)
				return
			}
			_, _ = io.WriteString(w, `{"choices":[{"message":`+message+`,"finish_reason":"length"}],"usage":{"completion_tokens":32},"system_fingerprint":"fp"}`)
		})
	}
	for _, tc := range []struct {
		name, message string
		pass          bool
	}{
		{"reasoning only", `{"role":"assistant","content":null,"reasoning":"hmm"}`, true},
		{"content only", `{"role":"assistant","content":"hello"}`, true},
		{"neither", `{"role":"assistant","content":null,"reasoning":null}`, false},
		{"empty strings", `{"role":"assistant","content":"","reasoning":""}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := New(serveOn(t, reply(tc.message)), "key")
			fp, err := c.chatCheck(context.Background())
			if (err == nil) != tc.pass {
				t.Errorf("chatCheck error = %v, want pass = %v", err, tc.pass)
			}
			if fp != "fp" {
				t.Errorf("fingerprint %q, want fp", fp)
			}
		})
	}
}

// Only the final answer is judged. A reply whose reasoning says the words of
// a tool call, or an answer, but which has no tool call and no content, fails
// the checks that need one. The reply is written by hand; the rest of the
// engine is the double.
func TestReasoningTextIsNeverJudged(t *testing.T) {
	reply := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/v1/chat/completions" {
				next.ServeHTTP(w, r)
				return
			}
			_, _ = io.WriteString(w, `{"choices":[{"message":{"role":"assistant","content":null,"reasoning":"I should call get_weather with {\"city\": \"Krakow\"} and the sky is blue"},"finish_reason":"stop","token_ids":[1,2,3]}],"usage":{"completion_tokens":3}}`)
		})
	}
	c, _ := doubleWith(t, "", nil, reply, WithReasoning(true))
	if err := c.toolCallCheck(context.Background()); err == nil || reasonOf(err) == agentapi.ReasonReasoningBudgetExhausted {
		t.Errorf("toolCallCheck = %v, want a failure for the missing call that is not ReasoningBudgetExhausted (the reply stopped)", err)
	}
	if _, err := c.behaviourCheck(context.Background(), agentapi.QualifyRequest{Shapes: shapes}); err == nil || !strings.Contains(err.Error(), "empty") {
		t.Errorf("behaviourCheck = %v, want the empty-answer failure", err)
	}
}

// The extra reasoning budget: with a reasoning parser the tool-call smoke test
// and the behaviour check ask for their own budget plus 4,096 tokens; without
// one, their own. A model that reasons for 500 tokens passes with the extra
// budget and fails without it.
func TestReasoningBudgetIsAdded(t *testing.T) {
	req := agentapi.QualifyRequest{Shapes: shapes, ToolCalls: true}
	on, onRec := doubleWith(t, "reasoning-first=500", nil, nil, WithReasoning(true))
	res := on.Qualify(context.Background(), req)
	for _, name := range []agentapi.CheckName{agentapi.CheckChat, agentapi.CheckToolCall, agentapi.CheckCorruption, agentapi.CheckBehaviour} {
		if o := outcomeOf(t, res, name); !o.Passed {
			t.Errorf("reasoning on, %s: %q, want passed", name, o.Message)
		}
	}
	// the tool-call check, then the behaviour check (also with tools here)
	if got, want := fmt.Sprint(onRec.maxTokensOf(true)), fmt.Sprint([]int{128 + 4096, int(shapes.OutputTypical) + 4096}); got != want {
		t.Errorf("max_tokens of the requests with tools = %s, want %s", got, want)
	}

	off, offRec := doubleWith(t, "reasoning-first=500", nil, nil)
	res = off.Qualify(context.Background(), req)
	for _, name := range []agentapi.CheckName{agentapi.CheckToolCall, agentapi.CheckBehaviour} {
		if o := outcomeOf(t, res, name); o.Passed || o.Reason != agentapi.ReasonReasoningBudgetExhausted {
			t.Errorf("reasoning off, %s: passed %v, reason %q, %q; want a ReasoningBudgetExhausted failure", name, o.Passed, o.Reason, o.Message)
		}
	}
	if got, want := fmt.Sprint(offRec.maxTokensOf(true)), fmt.Sprint([]int{128, int(shapes.OutputTypical)}); got != want {
		t.Errorf("max_tokens of the requests with tools = %s, want %s", got, want)
	}
}

// A reply that spends the whole budget on reasoning fails the tool-call smoke
// test and the behaviour check with its own reason; the chat check, which
// accepts reasoning text, passes. The outcome's reason code tells it from a
// broken engine, and its message says the model ran out of reasoning budget.
func TestReasoningBudgetExhausted(t *testing.T) {
	for _, toolCalls := range []bool{true, false} {
		t.Run(fmt.Sprintf("toolCalls=%v", toolCalls), func(t *testing.T) {
			c := double(t, "reasoning-forever", nil, WithReasoning(true))
			res := c.Qualify(context.Background(), agentapi.QualifyRequest{Shapes: shapes, ToolCalls: toolCalls})
			if o := outcomeOf(t, res, agentapi.CheckChat); !o.Passed {
				t.Errorf("chat: %q, want passed on reasoning text", o.Message)
			}
			failing := []agentapi.CheckName{agentapi.CheckBehaviour}
			if toolCalls {
				failing = append(failing, agentapi.CheckToolCall)
			}
			for _, name := range failing {
				if o := outcomeOf(t, res, name); o.Passed || o.Reason != agentapi.ReasonReasoningBudgetExhausted || !strings.Contains(o.Message, "ran out of reasoning budget") {
					t.Errorf("%s: passed %v, reason %q, message %q; want a ReasoningBudgetExhausted failure saying it ran out of reasoning budget", name, o.Passed, o.Reason, o.Message)
				}
			}
		})
	}
}

// With a reasoning parser the behaviour check shortens its prompt to leave room
// for the output and the reasoning budget, and says what context it ran at, on a
// pass too. The numbers are worked by hand: max_model_len 8192, template
// overhead 9, typical output 64 and the 4,096 reasoning tokens leave
// 8192-9-64-4096 = 4023 tokens of prompt beyond the overhead.
func TestBehaviourCheckShortensTheContextForReasoning(t *testing.T) {
	tune := func(c *enginedouble.Config) { c.MaxModelLen = 8192 }
	longest := agentapi.Shapes{ContextShort: 20, ContextTypical: 60, ContextLongest: 8000, OutputTypical: 64, OutputLongest: 100}
	req := agentapi.QualifyRequest{Shapes: longest}

	c, rec := doubleWith(t, "reasoning-first=50", tune, nil, WithReasoning(true))
	run, err := c.behaviourCheck(context.Background(), req)
	if err != nil {
		t.Fatalf("behaviour check: %v", err)
	}
	if run.contextTokens != 4023 || !strings.Contains(run.note, "ran at a context of 4023 tokens") || !strings.Contains(run.note, "shortened from 8000") {
		t.Errorf("ran at %d tokens, note %q; want 4023 and a note that says so and that 8000 was the longest", run.contextTokens, run.note)
	}
	if got := rec.maxTokensOf(false); len(got) == 0 || got[len(got)-1] != 64+4096 {
		t.Errorf("max_tokens %v, want 4160 for the behaviour request", got)
	}
	// the outcome carries the note on a pass
	res := c.Qualify(context.Background(), req)
	if o := outcomeOf(t, res, agentapi.CheckBehaviour); !o.Passed || !strings.Contains(o.Message, "ran at a context of 4023 tokens") {
		t.Errorf("behaviour outcome %+v, want passed with the context in its message", o)
	}

	// without a reasoning parser the longest context is used as it is, with no note
	off, _ := doubleWith(t, "", tune, nil)
	if run, err := off.behaviourCheck(context.Background(), req); err != nil || run.note != "" || run.contextTokens != 8000 {
		t.Errorf("reasoning off: %+v, error %v; want 8000 tokens and no note", run, err)
	}
	// room for the longest context: no shortening, the context is still named
	roomy, _ := doubleWith(t, "reasoning-first=50", nil, nil, WithReasoning(true))
	if run, err := roomy.behaviourCheck(context.Background(), agentapi.QualifyRequest{Shapes: shapes}); err != nil || run.contextTokens != 200 || run.note != "ran at a context of 200 tokens" {
		t.Errorf("roomy: %+v, error %v; want 200 tokens named", run, err)
	}
}

// Below the typical context there is no room to fit: the check runs at the
// typical context, says so, and does not fail for lack of room (science
// agent's ruling of 31 Oct 2026). The engine here enforces 32768 tokens and
// reports less, so the request itself fits and the verdict is the model's.
// Worked by hand with typical output 64, ContextTypical 60, ContextLongest 200:
//   - reported 4300: 4300-9-4160 = 131, between the two: the check runs at 131;
//   - reported 4100: 4100-9-4160 < 0, below 60: it runs at 60, the typical context;
//   - reported nothing: it cannot be fitted and runs at 60 too.
func TestBehaviourCheckContextFloor(t *testing.T) {
	req := agentapi.QualifyRequest{Shapes: shapes}
	reports := func(n int) func(*enginedouble.Config) {
		return func(c *enginedouble.Config) { c.ReportedMaxModelLen = n }
	}
	for _, tc := range []struct {
		name   string
		modes  string
		tune   func(*enginedouble.Config)
		tokens int64
		note   string
	}{
		{"between the typical and the longest", "", reports(4300), 131, "shortened from 200"},
		{"below the typical context", "", reports(4100), 60, "the typical context: max_model_len 4100"},
		{"no max_model_len reported", "reports-nothing", nil, 60, "/v1/models reports no max_model_len"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, _ := doubleWith(t, tc.modes, tc.tune, nil, WithReasoning(true))
			run, err := c.behaviourCheck(context.Background(), req)
			if err != nil {
				t.Fatalf("the check failed: %v", err)
			}
			if run.contextTokens != tc.tokens || !strings.Contains(run.note, fmt.Sprintf("ran at a context of %d tokens", tc.tokens)) || !strings.Contains(run.note, tc.note) {
				t.Errorf("ran at %d tokens, note %q; want %d tokens and a note containing %q", run.contextTokens, run.note, tc.tokens, tc.note)
			}
			res := c.Qualify(context.Background(), req)
			if o := outcomeOf(t, res, agentapi.CheckBehaviour); !o.Passed || !strings.Contains(o.Message, "ran at a context of") || o.ContextTokens != tc.tokens {
				t.Errorf("behaviour outcome %+v, want passed, naming the context, with ContextTokens %d", o, tc.tokens)
			}
		})
	}
}

// When the engine truly has no room at the typical context, the request is
// refused by the engine and the check fails with its error, not an error of its
// own about room.
func TestBehaviourCheckAtTheFloorFailsWithTheEnginesError(t *testing.T) {
	c, _ := doubleWith(t, "", func(c *enginedouble.Config) { c.MaxModelLen = 4000 }, nil, WithReasoning(true))
	run, err := c.behaviourCheck(context.Background(), agentapi.QualifyRequest{Shapes: shapes})
	if err == nil || !strings.Contains(err.Error(), "HTTP 400") || strings.Contains(err.Error(), "no room") {
		t.Errorf("error %v, want the engine's HTTP 400", err)
	}
	if run.contextTokens != 60 {
		t.Errorf("ran at %d tokens, want the typical context, 60", run.contextTokens)
	}
}

// failedOutcome is the one place that builds a failing outcome: the reason
// code in its own field, and the message, human text cut to
// agentapi.MaxMessageBytes, with no code in front.
func TestFailedOutcome(t *testing.T) {
	o := failedOutcome(agentapi.CheckBehaviour, agentapi.ReasonReasoningBudgetExhausted, "spent it all")
	if o.Name != agentapi.CheckBehaviour || o.Passed || o.Reason != "ReasoningBudgetExhausted" || o.Message != "spent it all" {
		t.Errorf("outcome %+v", o)
	}
	long := failedOutcome(agentapi.CheckToolCall, agentapi.ReasonNoToolCall, strings.Repeat("x", 1000))
	if len(long.Message) != agentapi.MaxMessageBytes || long.Reason != "NoToolCall" {
		t.Errorf("a long message: %d bytes, reason %q; want %d bytes and the code kept", len(long.Message), long.Reason, agentapi.MaxMessageBytes)
	}
	// a reasoned error carries its code through Qualify's single path
	if reasonOf(&reasonedError{"R", "m"}) != "R" || reasonOf(fmt.Errorf("plain")) != "" || reasonOf(fmt.Errorf("wrapped: %w", &reasonedError{"R", "m"})) != "R" {
		t.Error("reasonOf does not read the code of a reasoned error, wrapped or not")
	}
}
