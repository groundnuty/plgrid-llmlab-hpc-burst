package llmlabtest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// inferRequest is the part of an inference body the double reads.
type inferRequest struct {
	Model         string `json:"model"`
	Stream        bool   `json:"stream"`
	StreamOptions *struct {
		IncludeUsage bool `json:"include_usage"`
	} `json:"stream_options"`
	MaxTokens           *int            `json:"max_tokens"`
	MaxCompletionTokens *int            `json:"max_completion_tokens"`
	MaxOutputTokens     *int            `json:"max_output_tokens"`
	Messages            json.RawMessage `json:"messages"`
	Prompt              json.RawMessage `json:"prompt"`
	Input               json.RawMessage `json:"input"`
	Instructions        json.RawMessage `json:"instructions"`
}

// limit is the output limit the request asks for, or -1 for none.
func (q inferRequest) limit() int {
	for _, p := range []*int{q.MaxCompletionTokens, q.MaxTokens, q.MaxOutputTokens} {
		if p != nil {
			return *p
		}
	}
	return -1
}

// plan is what one inference answer sends.
type plan struct {
	path         string
	req          inferRequest
	model        Model
	promptTokens int
	words        []string
	finish       string // "stop" or "length"
	id           string
}

// infer answers an inference route: body checks, rewriting checks (L-6.6),
// the model's state (L-6.5), then the first-byte wait and the answer.
func (x *exchange) infer(path string, body []byte) {
	var members map[string]json.RawMessage
	var req inferRequest
	if json.Unmarshal(body, &members) != nil || json.Unmarshal(body, &req) != nil {
		x.fail(http.StatusBadRequest, "invalid_request_error", "the body is not a JSON object of this route", "")
		return
	}
	var model string
	if raw, ok := members["model"]; !ok || json.Unmarshal(raw, &model) != nil || model == "" {
		x.fail(http.StatusBadRequest, "invalid_request_error", "the body has no string model", "model")
		return
	}

	s := x.s
	s.mu.Lock()
	rw := s.rewriting
	ms, known := s.models[model]
	var m Model
	var listed, inactive, blocked bool
	if known {
		m, listed, inactive, blocked = ms.Model, ms.listed, ms.inactive, ms.blocked
	}
	ambiguous, reply := s.ambiguous, s.reply
	s.seq++
	seq := s.seq
	s.mu.Unlock()
	x.rec.Model = model

	if rw != nil {
		for name := range members {
			if !contains(rw.Allowlist, name) {
				x.fail(http.StatusUnprocessableEntity, "invalid_request_error", "body member not allowed: "+name, name)
				return
			}
		}
	}
	switch {
	case !known || !listed || blocked || (inactive && ambiguous):
		x.fail(http.StatusBadRequest, "invalid_request_error", "invalid model name passed in model", "")
		return
	case inactive:
		x.fail(http.StatusServiceUnavailable, "service_unavailable", "the model is not active", "")
		return
	}
	limit := req.limit()
	if rw != nil && m.ContextWindow > 0 && limit == m.ContextWindow {
		x.fail(http.StatusBadRequest, "invalid_request_error", "max_tokens must be below the model's context window", "max_tokens")
		return
	}

	p := plan{path: path, req: req, model: m, id: fmt.Sprintf("llmlabtest-%d", seq)}
	p.promptTokens = promptTokens(path, req)
	if rw != nil && path != pathEmbeddings {
		p.promptTokens += rw.InjectedTokens
	}
	p.words, p.finish = strings.Fields(reply), "stop"
	if limit >= 0 && len(p.words) > limit {
		p.words, p.finish = p.words[:limit], "length"
	}
	if m.MaxTokensCap > 0 && len(p.words) > m.MaxTokensCap { // hidden (L-6.5)
		p.words, p.finish = p.words[:m.MaxTokensCap], "length"
	}

	firstByte := x.arrival.Add(x.latency.FirstByte)
	if !x.waitFirstByte(func(now time.Time) bool { return !s.stalledLocked(now) && !now.Before(firstByte) }) {
		return
	}
	switch {
	case path == pathEmbeddings:
		x.writeJSON(http.StatusOK, p.embeddings())
	case req.Stream:
		x.stream(p)
	default:
		x.writeJSON(http.StatusOK, p.whole(x.arrival))
	}
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// promptTokens counts the whitespace-separated words of the request's text.
func promptTokens(path string, q inferRequest) int {
	var raws []json.RawMessage
	switch path {
	case pathChat:
		raws = []json.RawMessage{q.Messages}
	case pathCompletions:
		raws = []json.RawMessage{q.Prompt}
	case pathEmbeddings:
		raws = []json.RawMessage{q.Input}
	case pathResponses:
		raws = []json.RawMessage{q.Instructions, q.Input}
	}
	n := 0
	for _, raw := range raws {
		for _, t := range texts(raw) {
			n += len(strings.Fields(t))
		}
	}
	return n
}

// texts collects the text of a JSON value: a string itself, each element of
// an array, and the "text" and "content" members of an object.
func texts(raw json.RawMessage) []string {
	if len(raw) == 0 {
		return nil
	}
	var str string
	if json.Unmarshal(raw, &str) == nil {
		return []string{str}
	}
	var arr []json.RawMessage
	if json.Unmarshal(raw, &arr) == nil {
		var out []string
		for _, e := range arr {
			out = append(out, texts(e)...)
		}
		return out
	}
	var obj map[string]json.RawMessage
	if json.Unmarshal(raw, &obj) == nil {
		return append(texts(obj["text"]), texts(obj["content"])...)
	}
	return nil
}

func (p plan) usage() map[string]any {
	c := len(p.words)
	if p.path == pathResponses {
		return map[string]any{"input_tokens": p.promptTokens, "output_tokens": c, "total_tokens": p.promptTokens + c}
	}
	return map[string]any{"prompt_tokens": p.promptTokens, "completion_tokens": c, "total_tokens": p.promptTokens + c}
}

// piece is the i-th output token as text: words joined by single spaces.
func (p plan) piece(i int) string {
	if i == 0 {
		return p.words[0]
	}
	return " " + p.words[i]
}

func (p plan) text() string { return strings.Join(p.words, " ") }

// whole is a non-streamed answer.
func (p plan) whole(created time.Time) map[string]any {
	switch p.path {
	case pathCompletions:
		return map[string]any{
			"id": "cmpl-" + p.id, "object": "text_completion", "created": created.Unix(), "model": p.req.Model,
			"choices": []any{map[string]any{"index": 0, "text": p.text(), "logprobs": nil, "finish_reason": p.finish}},
			"usage":   p.usage(),
		}
	case pathResponses:
		return p.response(created, true)
	default:
		return map[string]any{
			"id": "chatcmpl-" + p.id, "object": "chat.completion", "created": created.Unix(), "model": p.req.Model,
			"choices": []any{map[string]any{
				"index": 0, "message": map[string]any{"role": "assistant", "content": p.text()}, "finish_reason": p.finish,
			}},
			"usage": p.usage(),
		}
	}
}

// response is a Responses API object; done false is the in-progress form.
func (p plan) response(created time.Time, done bool) map[string]any {
	r := map[string]any{
		"id": "resp_" + p.id, "object": "response", "created_at": created.Unix(), "model": p.req.Model,
		"status": "in_progress", "output": []any{},
	}
	if !done {
		return r
	}
	r["status"] = "completed"
	if p.finish == "length" {
		r["status"] = "incomplete"
		r["incomplete_details"] = map[string]any{"reason": "max_output_tokens"}
	}
	r["output"] = []any{map[string]any{
		"type": "message", "id": "msg_" + p.id, "role": "assistant", "status": "completed",
		"content": []any{map[string]any{"type": "output_text", "text": p.text(), "annotations": []any{}}},
	}}
	r["usage"] = p.usage()
	return r
}

// embeddings is an embeddings answer: one fixed vector per input.
func (p plan) embeddings() map[string]any {
	n := 1
	var arr []json.RawMessage
	if json.Unmarshal(p.req.Input, &arr) == nil {
		n = len(arr)
	}
	data := make([]any, n)
	for i := range data {
		data[i] = map[string]any{"object": "embedding", "index": i, "embedding": []float64{0.25, -0.5, 0.75, 1}}
	}
	return map[string]any{
		"object": "list", "data": data, "model": p.req.Model,
		"usage": map[string]any{"prompt_tokens": p.promptTokens, "total_tokens": p.promptTokens},
	}
}

// stream answers with server-sent events: an opening event, one event per
// output token, the finish, usage when asked, then [DONE] (chat and
// completions) or the closing event of a Responses stream: response.completed,
// or response.incomplete when the output was capped (finish "length").
// Model.TruncateAfter and the no-byte cut end it early, with no finish and no
// [DONE] (L-6.7, L-6.1).
//
// The status line goes out at once, flushed, even on /v1/completions, which
// has no opening event: from then on the cut is a mid-stream cut, and the
// logged FirstByte is the time those bytes left.
func (x *exchange) stream(p plan) {
	w := x.w
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)
	if flusher != nil {
		flusher.Flush()
	}
	x.rec.Status = http.StatusOK
	x.rec.FirstByte = x.s.now()
	seq := 0
	send := func(event string, v any) {
		var b bytes.Buffer
		if event != "" {
			fmt.Fprintf(&b, "event: %s\n", event)
		}
		data, err := json.Marshal(v)
		if err != nil {
			panic("llmlabtest: encode an event: " + err.Error())
		}
		fmt.Fprintf(&b, "data: %s\n\n", data)
		_, _ = w.Write(b.Bytes())
		if flusher != nil {
			flusher.Flush()
		}
		seq++
	}
	created := x.arrival
	chunk := func(delta map[string]any, text *string, finish any) map[string]any {
		if p.path == pathCompletions {
			t := ""
			if text != nil {
				t = *text
			}
			return map[string]any{"id": "cmpl-" + p.id, "object": "text_completion", "created": created.Unix(), "model": p.req.Model,
				"choices": []any{map[string]any{"index": 0, "text": t, "logprobs": nil, "finish_reason": finish}}}
		}
		return map[string]any{"id": "chatcmpl-" + p.id, "object": "chat.completion.chunk", "created": created.Unix(), "model": p.req.Model,
			"choices": []any{map[string]any{"index": 0, "delta": delta, "finish_reason": finish}}}
	}

	// The opening event.
	switch p.path {
	case pathResponses:
		send("response.created", map[string]any{"type": "response.created", "sequence_number": seq, "response": p.response(created, false)})
	case pathChat:
		send("", chunk(map[string]any{"role": "assistant", "content": ""}, nil, nil))
	}
	last := x.s.now()
	for i := range p.words {
		if p.model.TruncateAfter > 0 && i >= p.model.TruncateAfter {
			x.rec.Truncated = true
			return
		}
		if !x.pace(&last) {
			return
		}
		piece := p.piece(i)
		switch p.path {
		case pathResponses:
			send("response.output_text.delta", map[string]any{"type": "response.output_text.delta", "sequence_number": seq,
				"item_id": "msg_" + p.id, "output_index": 0, "content_index": 0, "delta": piece})
		default:
			send("", chunk(map[string]any{"content": piece}, &piece, nil))
		}
	}
	if !x.pace(&last) {
		return
	}
	if p.path == pathResponses {
		closing := "response.completed"
		if p.finish == "length" {
			closing = "response.incomplete" // the response says status incomplete
		}
		send(closing, map[string]any{"type": closing, "sequence_number": seq, "response": p.response(created, true)})
		return
	}
	send("", chunk(map[string]any{}, nil, p.finish))
	if p.req.StreamOptions != nil && p.req.StreamOptions.IncludeUsage {
		u := chunk(nil, nil, nil)
		u["choices"], u["usage"] = []any{}, p.usage()
		send("", u)
	}
	_, _ = w.Write([]byte("data: [DONE]\n\n"))
	if flusher != nil {
		flusher.Flush()
	}
}

// pace waits for the next chunk: Latency.PerChunk after the last byte, and
// out of any stall. A gap that reaches the no-byte cut ends the stream, as
// does the client going away or the Server closing; pace then returns false.
func (x *exchange) pace(last *time.Time) bool {
	var deadline time.Time
	if x.cut > 0 {
		deadline = last.Add(x.cut)
	}
	next := last.Add(x.latency.PerChunk)
	switch x.s.wait(x.r.Context(), deadline, func(now time.Time) bool {
		return !x.s.stalledLocked(now) && !now.Before(next)
	}) {
	case waitReady:
		*last = x.s.now()
		return true
	case waitCut:
		x.rec.Truncated = true
	}
	return false
}
