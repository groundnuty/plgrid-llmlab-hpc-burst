package enginedouble

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"
)

// streamResult is a streamed answer cut into its events.
type streamResult struct {
	status int
	header http.Header
	raw    []byte
	events []string // the data of each event, "[DONE]" included
}

type streamChunk struct {
	Choices []struct {
		Index int `json:"index"`
		Delta struct {
			Role      *string         `json:"role"`
			Content   *string         `json:"content"`
			Reasoning *string         `json:"reasoning"`
			ToolCalls json.RawMessage `json:"tool_calls"`
		} `json:"delta"`
		FinishReason *string `json:"finish_reason"`
		TokenIDs     []int32 `json:"token_ids"`
	} `json:"choices"`
	Usage *struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
		TotalTokens      int `json:"total_tokens"`
	} `json:"usage"`
	SystemFingerprint string `json:"system_fingerprint"`
}

func (s *testServer) stream(body map[string]any) streamResult {
	s.t.Helper()
	return s.streamAt("/v1/chat/completions", body)
}

// streamAt posts a streamed request to path and cuts the answer into events.
func (s *testServer) streamAt(path string, body map[string]any) streamResult {
	s.t.Helper()
	body["stream"] = true
	r := s.post(path, body)
	res := streamResult{status: r.status, header: r.header, raw: r.body}
	if r.status != 200 {
		return res
	}
	// every event is "data: <json>" and a blank line, with nothing else
	for _, ev := range strings.SplitAfter(string(r.body), "\n\n") {
		if ev == "" {
			continue
		}
		if !strings.HasPrefix(ev, "data: ") || !strings.HasSuffix(ev, "\n\n") || strings.Count(ev, "\n") != 2 {
			s.t.Fatalf("not one SSE event: %q", ev)
		}
		res.events = append(res.events, strings.TrimSuffix(strings.TrimPrefix(ev, "data: "), "\n\n"))
	}
	return res
}

func (r streamResult) chunks(t *testing.T) []streamChunk {
	t.Helper()
	var out []streamChunk
	for _, ev := range r.events {
		if ev == "[DONE]" {
			continue
		}
		var c streamChunk
		if err := json.Unmarshal([]byte(ev), &c); err != nil {
			t.Fatalf("event %q is not JSON: %v", ev, err)
		}
		out = append(out, c)
	}
	return out
}

func TestSSEFraming(t *testing.T) {
	s := serve(t, cfgFor())
	res := s.stream(chatBody(3, map[string]any{
		"max_tokens": 5, "ignore_eos": true, "return_token_ids": true,
		"stream_options": map[string]any{"include_usage": true},
	}))
	if res.status != 200 || !strings.HasPrefix(res.header.Get("Content-Type"), "text/event-stream") {
		t.Fatalf("HTTP %d, Content-Type %q", res.status, res.header.Get("Content-Type"))
	}
	if !bytes.HasSuffix(res.raw, []byte("data: [DONE]\n\n")) {
		t.Errorf("the stream does not end with a blank-line-terminated [DONE]: %q", res.raw[max(0, len(res.raw)-40):])
	}
	// role chunk, 5 token chunks, usage chunk, [DONE]
	if len(res.events) != 8 || res.events[7] != "[DONE]" {
		t.Fatalf("%d events, want 8 ending in [DONE]: %q", len(res.events), res.events)
	}
	chunks := res.chunks(t)

	role := chunks[0].Choices[0]
	if role.Delta.Role == nil || *role.Delta.Role != "assistant" || role.Delta.Content == nil || *role.Delta.Content != "" || role.FinishReason != nil {
		t.Errorf("first chunk = %+v, want delta {role: assistant, content: \"\"} and no finish_reason", role)
	}
	for i := 1; i <= 5; i++ {
		c := chunks[i].Choices[0]
		if c.Delta.Content == nil || *c.Delta.Content == "" || len(c.TokenIDs) != 1 {
			t.Errorf("chunk %d = %+v, want a content delta and one token id", i, c)
		}
		if (c.FinishReason != nil) != (i == 5) {
			t.Errorf("chunk %d finish_reason = %v: only the last token chunk carries one", i, c.FinishReason)
		}
	}
	if fr := chunks[5].Choices[0].FinishReason; fr == nil || *fr != "length" {
		t.Errorf("last token chunk finish_reason = %v, want length", fr)
	}
	usage := chunks[6]
	if len(usage.Choices) != 0 || usage.Usage == nil || usage.Usage.PromptTokens != 12 || usage.Usage.CompletionTokens != 5 || usage.Usage.TotalTokens != 17 {
		t.Errorf("usage chunk = %+v, want no choices and usage 12/5/17", usage)
	}
	if !strings.Contains(res.events[6], `"choices":[]`) {
		t.Errorf("usage chunk %s must carry an empty choices list", res.events[6])
	}
	if usage.SystemFingerprint != "fp-enginedouble" {
		t.Errorf("the usage chunk's system_fingerprint = %q", usage.SystemFingerprint)
	}
}

func TestSSEWithoutUsageOption(t *testing.T) {
	res := serve(t, cfgFor()).stream(chatBody(3, map[string]any{"max_tokens": 4, "ignore_eos": true}))
	if len(res.events) != 6 || res.events[5] != "[DONE]" { // role, 4 tokens, [DONE]
		t.Fatalf("%d events: %q, want role chunk, 4 token chunks and [DONE]", len(res.events), res.events)
	}
	chunks := res.chunks(t)
	for i, c := range chunks {
		if c.Usage != nil {
			t.Errorf("chunk %d has usage without include_usage", i)
		}
		if len(c.Choices[0].TokenIDs) != 0 {
			t.Errorf("chunk %d has token_ids without return_token_ids", i)
		}
	}
	if chunks[4].SystemFingerprint != "fp-enginedouble" {
		t.Errorf("the finish chunk's system_fingerprint = %q, want fp-enginedouble", chunks[4].SystemFingerprint)
	}
}

// The deltas joined are the non-streamed content, and its words hash to the
// token ids: an independent computation with hash/fnv.
func TestStreamedAnswerRoundTrips(t *testing.T) {
	s := serve(t, cfgFor())
	body := func() map[string]any { return longRequest(40, nil) }
	single := s.chat(body())
	res := s.stream(body())

	var content strings.Builder
	var ids []int32
	for _, c := range res.chunks(t)[1:] {
		ch := c.Choices[0]
		content.WriteString(*ch.Delta.Content)
		ids = append(ids, ch.TokenIDs...)
	}
	if got, want := content.String(), *single.Choices[0].Message.Content; got != want {
		t.Errorf("streamed content %q != non-streamed %q", got, want)
	}
	if !reflect.DeepEqual(ids, single.Choices[0].TokenIDs) {
		t.Errorf("streamed token ids %v != non-streamed %v", ids, single.Choices[0].TokenIDs)
	}
	words := strings.Fields(content.String())
	if len(words) != 40 {
		t.Fatalf("%d words in the content, want 40", len(words))
	}
	for i, w := range words {
		if refID(w) != ids[i] {
			t.Errorf("token %d: hash/fnv of %q is %d, the answer says %d", i, w, refID(w), ids[i])
		}
	}
}

// A streamed tool call in vLLM's delta form: the first delta carries the
// call's index, id, type and function name, the later ones only the index and
// a piece of the arguments, and the last chunk the finish reason.
func TestStreamedToolCall(t *testing.T) {
	res := serve(t, cfgFor()).stream(toolBody(map[string]any{
		"max_tokens": 50, "stream_options": map[string]any{"include_usage": true},
	}))
	type call struct {
		Index    *int   `json:"index"`
		ID       string `json:"id"`
		Type     string `json:"type"`
		Function *struct {
			Name      string `json:"name"`
			Arguments string `json:"arguments"`
		} `json:"function"`
	}
	var calls []call
	var finish *string
	for _, ch := range res.chunks(t)[1:] { // after the role chunk
		if len(ch.Choices) == 0 {
			continue
		}
		c := ch.Choices[0]
		if c.Delta.Content != nil || c.Delta.Reasoning != nil {
			t.Errorf("a tool-call stream carries text: %+v", c.Delta)
		}
		var cs []call
		if err := json.Unmarshal(c.Delta.ToolCalls, &cs); err != nil || len(cs) != 1 {
			t.Fatalf("delta tool_calls %s: %v", c.Delta.ToolCalls, err)
		}
		calls = append(calls, cs[0])
		if c.FinishReason != nil {
			finish = c.FinishReason
		}
	}
	if len(calls) != 3 { // the name, then two pieces of arguments
		t.Fatalf("%d tool-call deltas, want 3: %+v", len(calls), calls)
	}
	if first := calls[0]; first.Index == nil || *first.Index != 0 || first.ID != "call_0" || first.Type != "function" ||
		first.Function == nil || first.Function.Name != "get_weather" || first.Function.Arguments != "" {
		t.Errorf("first delta = %+v, want index 0, id call_0, type function, name get_weather and no arguments", first)
	}
	var args strings.Builder
	for i, c := range calls[1:] {
		if c.Index == nil || *c.Index != 0 || c.ID != "" || c.Type != "" || c.Function == nil || c.Function.Name != "" {
			t.Errorf("argument delta %d = %+v, want only the index and arguments", i, c)
		}
		args.WriteString(c.Function.Arguments)
	}
	if args.String() != `{"city": "Krakow"}` {
		t.Errorf("the argument pieces join to %q, want {\"city\": \"Krakow\"}", args.String())
	}
	if finish == nil || *finish != "tool_calls" {
		t.Errorf("finish_reason = %v, want tool_calls on the last chunk", finish)
	}
	usage := res.chunks(t)
	if u := usage[len(usage)-1].Usage; u == nil || u.CompletionTokens != 2 {
		t.Errorf("usage = %+v, want 2 completion tokens", u)
	}
}

// The events arrive as they are made: the first is read long before the last.
func TestSSEIsFlushedPerEvent(t *testing.T) {
	cfg := cfgFor()
	cfg.TokenDelay = 40 * time.Millisecond
	s := serve(t, cfg)
	b, _ := json.Marshal(chatBody(2, map[string]any{"max_tokens": 10, "ignore_eos": true, "stream": true}))
	req, _ := http.NewRequest(http.MethodPost, "http://engine/v1/chat/completions", bytes.NewReader(b))
	start := time.Now()
	resp, err := s.hc.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	rd := bufio.NewReader(resp.Body)
	var first, done time.Duration
	for {
		line, err := rd.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				t.Fatal(err)
			}
			break
		}
		if first == 0 && strings.HasPrefix(line, "data: ") {
			first = time.Since(start)
		}
		if line == "data: [DONE]\n" {
			done = time.Since(start)
		}
	}
	if first == 0 || done == 0 || done-first < 250*time.Millisecond {
		t.Errorf("first event after %v, [DONE] after %v: the 10 tokens of 40 ms were not streamed as made", first, done)
	}
}

// stall: the stream stops after the first token and hangs; the request keeps
// counting as running until the client goes away.
func TestStallHangsAfterTheFirstToken(t *testing.T) {
	cfg := cfgFor()
	cfg.Stall = true
	s := serve(t, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b, _ := json.Marshal(chatBody(2, map[string]any{"max_tokens": 10, "ignore_eos": true, "stream": true}))
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "http://engine/v1/chat/completions", bytes.NewReader(b))
	resp, err := s.hc.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	rd := bufio.NewReader(resp.Body)
	for i := 0; i < 2; i++ { // the role chunk and the first token
		ev, err := readEvent(rd)
		if err != nil || !strings.HasPrefix(ev, "data: ") {
			t.Fatalf("event %d: %q, %v", i, ev, err)
		}
	}
	got := make(chan string, 1)
	go func() { ev, _ := readEvent(rd); got <- ev }()
	select {
	case ev := <-got:
		t.Fatalf("a third event arrived on a stalled stream: %q", ev)
	case <-time.After(300 * time.Millisecond):
	}
	if m := string(s.get("/metrics").body); !strings.Contains(m, `vllm:num_requests_running{engine="0",model_name="m"} 1.0`) {
		t.Errorf("a stalled request must keep counting as running:\n%s", m)
	}
	cancel()
	eventually(t, 2*time.Second, "the cancelled request leaves the running count", func() bool {
		return strings.Contains(string(s.get("/metrics").body), `vllm:num_requests_running{engine="0",model_name="m"} 0.0`)
	})
	if m := string(s.get("/metrics").body); !strings.Contains(m, `vllm:request_success_total{engine="0",finished_reason="abort",model_name="m"} 1.0`) {
		t.Errorf("a cancelled request counts as abort:\n%s", m)
	}
}

func readEvent(rd *bufio.Reader) (string, error) {
	var sb strings.Builder
	for {
		line, err := rd.ReadString('\n')
		sb.WriteString(line)
		if err != nil || line == "\n" {
			return sb.String(), err
		}
	}
}
