package enginedouble

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"
)

// Streamed completions: the events vLLM's completion_stream_generator sends.

func TestCompletionStreamShape(t *testing.T) {
	s := serve(t, cfgFor())
	res := s.streamAt("/v1/completions", completionBody(3, map[string]any{
		"max_tokens": 5, "min_tokens": 5, "return_token_ids": true,
		"stream_options": map[string]any{"include_usage": true},
	}))
	if res.status != 200 || !strings.HasPrefix(res.header.Get("Content-Type"), "text/event-stream") {
		t.Fatalf("HTTP %d, Content-Type %q", res.status, res.header.Get("Content-Type"))
	}
	// 5 token chunks (vLLM sends no role chunk), the usage chunk, [DONE]
	if len(res.events) != 7 || res.events[6] != "[DONE]" {
		t.Fatalf("%d events, want 7 ending in [DONE]: %q", len(res.events), res.events)
	}
	if !bytes.HasSuffix(res.raw, []byte("data: [DONE]\n\n")) || bytes.Count(res.raw, []byte("[DONE]")) != 1 {
		t.Errorf("the stream must end with exactly one blank-line-terminated [DONE]: %q", res.raw)
	}
	chunks := completionChunks(t, res)

	var text strings.Builder
	for i, c := range chunks[:5] {
		top := rawKeys(t, []byte(res.events[i]))
		checkKeys(t, "chunk", top, []string{"id", "object", "created", "model", "choices"}, chunkFields)
		var cs []json.RawMessage
		if err := json.Unmarshal(top["choices"], &cs); err != nil || len(cs) != 1 {
			t.Fatalf("chunk %d choices = %s", i, top["choices"])
		}
		checkKeys(t, "chunk choice", rawKeys(t, cs[0]), []string{"index", "text", "logprobs", "finish_reason"}, chunkChoices)
		if c.Object != "text_completion" || c.ID != chunks[0].ID || !strings.HasPrefix(c.ID, "cmpl-") {
			t.Errorf("chunk %d: object %q id %q, want text_completion and the first chunk's cmpl- id", i, c.Object, c.ID)
		}
		ch := c.Choices[0]
		// a token's delta is its word, with a separating space unless it is the first
		word := strings.TrimSpace(ch.Text)
		if len(strings.Fields(ch.Text)) != 1 || (i == 0) != (ch.Text == word) {
			t.Errorf("chunk %d text %q, want one word, with a leading space except in the first chunk", i, ch.Text)
		}
		if len(ch.TokenIDs) != 1 || ch.TokenIDs[0] != refID(word) {
			t.Errorf("chunk %d token_ids %v, want [%d] (hash/fnv of %q)", i, ch.TokenIDs, refID(word), word)
		}
		if c.Usage != nil {
			t.Errorf("chunk %d carries usage without continuous usage", i)
		}
		if i < 4 && (c.SystemFingerprint != "" || ch.FinishReason != nil) {
			t.Errorf("chunk %d: finish_reason %v, fingerprint %q, want neither before the last token", i, ch.FinishReason, c.SystemFingerprint)
		}
		text.WriteString(ch.Text)
	}
	last := chunks[4]
	if fr := last.Choices[0].FinishReason; fr == nil || *fr != "length" || last.Choices[0].Text == "" {
		t.Errorf("last token chunk = %+v, want finish_reason length and the last token's text", last.Choices[0])
	}
	if last.SystemFingerprint != "" {
		t.Errorf("the fingerprint is on the finish chunk (%q) although a usage chunk follows", last.SystemFingerprint)
	}

	usage := chunks[5]
	top := rawKeys(t, []byte(res.events[5]))
	checkKeys(t, "usage chunk", top, []string{"id", "object", "created", "model", "choices", "usage"}, chunkFields)
	if string(top["choices"]) != "[]" || len(usage.Choices) != 0 {
		t.Errorf("usage chunk choices = %s, want []", top["choices"])
	}
	if usage.Usage == nil || *usage.Usage != (usageJSON{3, 5, 8}) {
		t.Errorf("usage chunk = %+v, want 3/5/8", usage.Usage)
	}
	if usage.SystemFingerprint != "fp-enginedouble" || usage.Object != "text_completion" {
		t.Errorf("usage chunk fingerprint %q object %q", usage.SystemFingerprint, usage.Object)
	}
	if n := len(strings.Fields(text.String())); n != 5 {
		t.Errorf("the deltas join to %d words, want 5", n)
	}
}

func TestCompletionStreamWithoutUsageOption(t *testing.T) {
	res := serve(t, cfgFor()).streamAt("/v1/completions", completionBody(3, map[string]any{"max_tokens": 4, "min_tokens": 4}))
	if len(res.events) != 5 || res.events[4] != "[DONE]" { // 4 tokens, [DONE]
		t.Fatalf("%d events: %q, want 4 token chunks and [DONE]", len(res.events), res.events)
	}
	chunks := completionChunks(t, res)
	for i, c := range chunks {
		if c.Usage != nil || len(c.Choices[0].TokenIDs) != 0 {
			t.Errorf("chunk %d has usage %+v or token_ids %v without being asked", i, c.Usage, c.Choices[0].TokenIDs)
		}
	}
	// no usage chunk follows, so the finish chunk carries the fingerprint
	if chunks[3].SystemFingerprint != "fp-enginedouble" || chunks[3].Choices[0].FinishReason == nil || *chunks[3].Choices[0].FinishReason != "length" {
		t.Errorf("finish chunk = %+v, want the length finish reason and the fingerprint", chunks[3])
	}
}

// The deltas joined are the non-streamed text, as greedy requests give the
// same output every time (doc.go).
func TestCompletionStreamedTextEqualsTheWholeAnswer(t *testing.T) {
	s := serve(t, cfgFor())
	body := func() map[string]any {
		return completionBody(6, map[string]any{"max_tokens": 40, "min_tokens": 40, "return_token_ids": true})
	}
	whole := s.completion(body())
	var text strings.Builder
	var ids []int32
	for _, c := range completionChunks(t, s.streamAt("/v1/completions", body())) {
		if len(c.Choices) == 0 {
			continue
		}
		text.WriteString(c.Choices[0].Text)
		ids = append(ids, c.Choices[0].TokenIDs...)
	}
	if text.String() != whole.Choices[0].Text {
		t.Errorf("streamed text %q != whole %q", text.String(), whole.Choices[0].Text)
	}
	if !reflect.DeepEqual(ids, whole.Choices[0].TokenIDs) {
		t.Errorf("streamed token ids %v != whole %v", ids, whole.Choices[0].TokenIDs)
	}
}

// stall: a completions stream stops after its first token and hangs; the
// request keeps counting as running until the client goes away. The wait for
// a third event can only fail an engine that sends one.
func TestCompletionStallHangsAfterTheFirstToken(t *testing.T) {
	cfg := cfgFor()
	cfg.Stall = true
	s := serve(t, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b, _ := json.Marshal(completionBody(2, map[string]any{"max_tokens": 10, "min_tokens": 10, "stream": true}))
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "http://engine/v1/completions", bytes.NewReader(b))
	resp, err := s.hc.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	rd := bufio.NewReader(resp.Body)
	ev, err := readEvent(rd) // vLLM sends no role chunk: the first event is the first token
	if err != nil || !strings.HasPrefix(ev, "data: ") || !strings.Contains(ev, `"text_completion"`) {
		t.Fatalf("first event %q, %v", ev, err)
	}
	got := make(chan string, 1)
	go func() { ev, _ := readEvent(rd); got <- ev }()
	select {
	case ev := <-got:
		t.Fatalf("a second event arrived on a stalled stream: %q", ev)
	case <-time.After(300 * time.Millisecond):
	}
	if m := s.metrics(); !has(m, `vllm:num_requests_running{engine="0",model_name="m"} 1.0`) || !has(m, `vllm:generation_tokens_total{engine="0",model_name="m"} 1.0`) {
		t.Errorf("a stalled request must keep counting as running, with one token generated:\n%s", m)
	}
	cancel()
	eventually(t, 5*time.Second, "the cancelled request leaves the running count", func() bool {
		return has(s.metrics(), `vllm:num_requests_running{engine="0",model_name="m"} 0.0`)
	})
	if m := s.metrics(); !has(m, `vllm:request_success_total{engine="0",finished_reason="abort",model_name="m"} 1.0`) {
		t.Errorf("a cancelled request counts as abort:\n%s", m)
	}
}
