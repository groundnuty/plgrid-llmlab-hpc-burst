package enginetest

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/clock"
)

// play answers one request with the engine's behaviour.
type play struct {
	e      *Engine
	ctx    context.Context // the request's
	w      http.ResponseWriter
	b      Behaviour
	model  string
	usage  bool
	prompt int64
}

func (p *play) tokens() int {
	if p.b.Tokens <= 0 {
		return 3
	}
	return p.b.Tokens
}

// wait lets d pass on the engine's clock: a fake clock is advanced, so the
// front's timers on the same clock see the time pass; a real one sleeps.
func (p *play) wait(d time.Duration) {
	if d <= 0 {
		return
	}
	if f, ok := p.e.clk.(*clock.Fake); ok {
		f.Advance(d)
		return
	}
	time.Sleep(d)
}

// gate waits for the test's go-ahead on the behaviour's Gate, if it has one.
// A request that ends first is abandoned, as one whose client has gone.
func (p *play) gate() {
	if p.b.Gate == nil {
		return
	}
	select {
	case <-p.b.Gate:
	case <-p.ctx.Done():
		panic(http.ErrAbortHandler)
	}
}

// beforeToken waits the pace before token i (0-based), after the go-ahead
// for a token after the first, and kills the connection when the behaviour
// says so.
func (p *play) beforeToken(i int) {
	if i == 0 {
		p.wait(p.b.FirstToken)
	} else {
		p.gate()
		p.wait(p.b.PerToken)
	}
	if p.b.KillAfter > 0 && i >= p.b.KillAfter {
		panic(http.ErrAbortHandler) // the connection closes with no clean end
	}
}

func (p *play) event(name string, v any) {
	b, _ := json.Marshal(v)
	if name != "" {
		fmt.Fprintf(p.w, "event: %s\n", name)
	}
	fmt.Fprintf(p.w, "data: %s\n\n", b)
	_ = http.NewResponseController(p.w).Flush()
}

func (p *play) startStream() {
	p.w.Header().Set("Content-Type", "text/event-stream")
	p.w.Header().Set("Cache-Control", "no-cache")
	p.w.WriteHeader(http.StatusOK)
	_ = http.NewResponseController(p.w).Flush()
}

// chatUsage is the usage member. Like vLLM without
// --enable-prompt-tokens-details, it names no cached count unless the
// behaviour sets CachedTokens.
func (p *play) chatUsage() map[string]any {
	n := int64(p.tokens())
	u := map[string]any{"prompt_tokens": p.prompt, "completion_tokens": n, "total_tokens": p.prompt + n}
	if p.b.CachedTokens != nil {
		u["prompt_tokens_details"] = map[string]any{"cached_tokens": *p.b.CachedTokens}
	}
	return u
}

// responsesUsage is chatUsage in the Responses names.
func (p *play) responsesUsage() map[string]any {
	u := map[string]any{"input_tokens": p.prompt, "output_tokens": int64(p.tokens())}
	if p.b.CachedTokens != nil {
		u["input_tokens_details"] = map[string]any{"cached_tokens": *p.b.CachedTokens}
	}
	return u
}

// chatStream is a chat or completions stream: one token per chunk, a finish
// reason, the usage chunk when asked, then [DONE]. With LastWithFinish the
// last token rides in the finish chunk.
func (p *play) chatStream(completions bool) {
	p.startStream()
	obj := "chat.completion.chunk"
	if completions {
		obj = "text_completion"
	}
	field := p.b.DeltaField
	if field == "" {
		field = "content"
	}
	choice := func(text string, finish any) map[string]any {
		if completions {
			return map[string]any{"index": 0, "text": text, "finish_reason": finish}
		}
		delta := map[string]any{}
		if text != "" {
			delta[field] = text
		}
		return map[string]any{"index": 0, "delta": delta, "finish_reason": finish}
	}
	own := p.tokens() // tokens that get a chunk of their own
	rides := p.b.LastWithFinish && !p.b.NoFinish
	if rides {
		own--
	}
	for i := 0; i < own; i++ {
		p.beforeToken(i)
		p.event("", map[string]any{"id": "cmpl-1", "object": obj, "model": p.model, "choices": []any{choice(fmt.Sprintf("t%d ", i), nil)}})
	}
	if !p.b.NoFinish {
		text := ""
		if rides {
			p.beforeToken(own)
			text = fmt.Sprintf("t%d ", own)
		}
		reason := p.b.FinishReason
		if reason == "" {
			reason = "stop"
		}
		p.event("", map[string]any{"id": "cmpl-1", "object": obj, "model": p.model, "choices": []any{choice(text, reason)}})
	}
	if p.usage {
		p.event("", map[string]any{"id": "cmpl-1", "object": obj, "model": p.model, "choices": []any{}, "usage": p.chatUsage()})
	}
	if !p.b.NoDone {
		fmt.Fprint(p.w, "data: [DONE]\n\n")
		_ = http.NewResponseController(p.w).Flush()
	}
}

// responsesStream is a Responses stream ending with response.completed,
// which vLLM sends last.
func (p *play) responsesStream() {
	p.startStream()
	p.event("response.created", map[string]any{"type": "response.created", "response": map[string]any{"id": "resp-1", "status": "in_progress"}})
	for i := 0; i < p.tokens(); i++ {
		p.beforeToken(i)
		p.event("response.output_text.delta", map[string]any{"type": "response.output_text.delta", "delta": fmt.Sprintf("t%d ", i)})
	}
	if p.b.NoFinish {
		return
	}
	usage := p.responsesUsage()
	switch p.b.ResponsesEnd {
	case "incomplete":
		p.event("response.incomplete", map[string]any{"type": "response.incomplete", "response": map[string]any{
			"id": "resp-1", "status": "incomplete", "incomplete_details": map[string]any{"reason": "max_output_tokens"}, "usage": usage}})
	case "failed":
		p.event("response.failed", map[string]any{"type": "response.failed", "response": map[string]any{
			"id": "resp-1", "status": "failed", "error": map[string]any{"code": "server_error", "message": "the engine failed"}}})
	default:
		p.event("response.completed", map[string]any{"type": "response.completed", "response": map[string]any{
			"id": "resp-1", "status": "completed", "usage": usage}})
	}
}

// whole answers without streaming, after the whole reply is generated.
func (p *play) whole(path string) {
	var text strings.Builder
	for i := 0; i < p.tokens(); i++ {
		p.beforeToken(i)
		fmt.Fprintf(&text, "t%d ", i)
	}
	switch path {
	case "/v1/responses":
		writeJSON(p.w, http.StatusOK, map[string]any{"id": "resp-1", "object": "response", "status": "completed", "model": p.model,
			"output": []any{map[string]any{"type": "message", "content": []any{map[string]any{"type": "output_text", "text": text.String()}}}},
			"usage":  p.responsesUsage()})
	case "/v1/completions":
		writeJSON(p.w, http.StatusOK, map[string]any{"id": "cmpl-1", "object": "text_completion", "model": p.model,
			"choices": []any{map[string]any{"index": 0, "text": text.String(), "finish_reason": "stop"}}, "usage": p.chatUsage()})
	default:
		writeJSON(p.w, http.StatusOK, map[string]any{"id": "cmpl-1", "object": "chat.completion", "model": p.model,
			"choices": []any{map[string]any{"index": 0, "message": map[string]any{"role": "assistant", "content": text.String()}, "finish_reason": "stop"}},
			"usage":   p.chatUsage()})
	}
}

func (p *play) embeddings() {
	writeJSON(p.w, http.StatusOK, map[string]any{"object": "list", "model": p.model,
		"data":  []any{map[string]any{"object": "embedding", "index": 0, "embedding": []float64{0.25, -0.5}}},
		"usage": map[string]any{"prompt_tokens": p.prompt, "total_tokens": p.prompt}})
}

// rawStream answers a path the front does not judge with plain chunks.
func (p *play) rawStream() {
	p.w.Header().Set("Content-Type", "text/event-stream")
	p.w.WriteHeader(http.StatusOK)
	for i := 0; i < p.tokens(); i++ {
		p.beforeToken(i)
		fmt.Fprintf(p.w, "data: chunk %d\n\n", i)
		_ = http.NewResponseController(p.w).Flush()
	}
}
