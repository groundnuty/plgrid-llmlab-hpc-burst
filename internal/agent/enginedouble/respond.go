package enginedouble

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

var idCounter atomic.Uint64

// newID is a chat completion's id, newCompletionID a completion's: vLLM
// prefixes them chatcmpl- and cmpl-.
func newID() string { return nextID("chatcmpl-") }

func newCompletionID() string { return nextID("cmpl-") }

func nextID(prefix string) string {
	return fmt.Sprintf("%s%08x%08x", prefix, uint32(time.Now().Unix()), idCounter.Add(1))
}

type functionJSON struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

// toolCallJSON is a ToolCall of a non-streamed message.
type toolCallJSON struct {
	ID       string       `json:"id"`
	Type     string       `json:"type"`
	Function functionJSON `json:"function"`
}

// deltaToolCall is a DeltaToolCall of a streamed delta
// (vllm/entrypoints/generate/base/protocol.py): everything but the index is
// optional, and vLLM leaves an unset field out. The first delta of a call
// carries its id, type and function name; later ones carry pieces of the
// arguments.
type deltaToolCall struct {
	Index    int            `json:"index"`
	ID       string         `json:"id,omitempty"`
	Type     string         `json:"type,omitempty"`
	Function *deltaFunction `json:"function,omitempty"`
}

type deltaFunction struct {
	Name      string `json:"name,omitempty"`
	Arguments string `json:"arguments,omitempty"`
}

func (a answer) toolCalls() []toolCallJSON {
	if a.tool == nil {
		return nil
	}
	// The pieces joined are the arguments, or the part of them the output limit let through.
	return []toolCallJSON{{ID: "call_0", Type: "function", Function: functionJSON{a.tool.Name, strings.Join(a.words, "")}}}
}

// chatResponse is ChatCompletionResponse
// (vllm/entrypoints/openai/chat_completion/protocol.py).
type chatResponse struct {
	ID                string       `json:"id"`
	Object            string       `json:"object"`
	Created           int64        `json:"created"`
	Model             string       `json:"model"`
	Choices           []respChoice `json:"choices"`
	Usage             usage        `json:"usage"`
	SystemFingerprint string       `json:"system_fingerprint,omitempty"`
}

type respChoice struct {
	Index        int         `json:"index"`
	Message      respMessage `json:"message"`
	Logprobs     any         `json:"logprobs"`
	FinishReason string      `json:"finish_reason"`
	StopReason   any         `json:"stop_reason"`
	TokenIDs     []int32     `json:"token_ids,omitempty"` // only with return_token_ids
}

// respMessage is ChatMessage. Content is null for a tool call and for a reply
// that spent its tokens reasoning; Reasoning is null without a reasoning model
// (vLLM writes its None fields as null).
type respMessage struct {
	Role      string         `json:"role"`
	Content   *string        `json:"content"`
	Reasoning *string        `json:"reasoning"`
	ToolCalls []toolCallJSON `json:"tool_calls,omitempty"`
}

// single answers a request that is not streamed. It returns the tokens
// generated and whether the answer was sent.
func (h *handler) single(w http.ResponseWriter, r *http.Request, req chatRequest, a answer, s *slot, u usage) (int, bool) {
	n := 0
	if !h.generateTokens(r, s, a.tokens(), func(int) bool { n++; return true }) {
		return n, false
	}
	msg := respMessage{Role: "assistant", ToolCalls: a.toolCalls()}
	if len(a.reasoning) > 0 {
		reasoning := strings.Join(a.reasoning, " ")
		msg.Reasoning = &reasoning
	}
	if a.tool == nil && len(a.words) > 0 {
		c := a.content()
		msg.Content = &c
	}
	choice := respChoice{Message: msg, FinishReason: a.finish}
	if req.ReturnTokenIDs {
		choice.TokenIDs = a.allIDs()
	}
	writeJSON(w, http.StatusOK, chatResponse{
		ID: newID(), Object: "chat.completion", Created: time.Now().Unix(), Model: h.cfg.ServedName,
		Choices: []respChoice{choice}, Usage: u, SystemFingerprint: h.fingerprint(),
	})
	return n, true
}

// chunk is ChatCompletionStreamResponse.
type chunk struct {
	ID                string        `json:"id"`
	Object            string        `json:"object"`
	Created           int64         `json:"created"`
	Model             string        `json:"model"`
	Choices           []chunkChoice `json:"choices"`
	Usage             *usage        `json:"usage,omitempty"`
	SystemFingerprint string        `json:"system_fingerprint,omitempty"`
}

type chunkChoice struct {
	Index        int     `json:"index"`
	Delta        delta   `json:"delta"`
	FinishReason *string `json:"finish_reason"`
	TokenIDs     []int32 `json:"token_ids,omitempty"`
}

type delta struct {
	Role      string          `json:"role,omitempty"`
	Content   *string         `json:"content,omitempty"`
	Reasoning *string         `json:"reasoning,omitempty"`
	ToolCalls []deltaToolCall `json:"tool_calls,omitempty"`
}

// stream answers a request with server-sent events, as vLLM's
// chat_completion_stream_generator does (chat_completion/serving.py): a first
// chunk whose delta is the role with empty content, one chunk per token, the
// finish reason on the last of them (a finish chunk with a non-empty delta,
// which real vLLM sends too: Qwen3's capture before-stream-length.sse line 7,
// issue #125 item 10), a usage chunk with no choices when
// stream_options.include_usage is set, then "data: [DONE]". Each event is
// "data: <json>" and a blank line, flushed at once. token_ids ride on a chunk
// only with return_token_ids, as in vLLM.
//
// A reasoning token is a delta with "reasoning" and no content; the answer's
// tokens follow it. A tool call is streamed as vLLM's parsers do: a delta with
// the call's index, id, type and function name, then one delta per piece of
// the arguments, each with the index and "arguments" only.
//
// The first chunk is sent with the first token, after the wait before it, so
// the time to the first byte is the time to the first token. In the stall mode
// the stream stops after the first token and waits for the client to go.
func (h *handler) stream(w http.ResponseWriter, r *http.Request, req chatRequest, a answer, s *slot, u usage) (int, bool) {
	sw := newSSEWriter(w)
	id, created := newID(), time.Now().Unix()
	send := func(c chunk) bool { return sw.send(c) }
	newChunk := func(choices ...chunkChoice) chunk {
		if choices == nil {
			choices = []chunkChoice{}
		}
		return chunk{ID: id, Object: "chat.completion.chunk", Created: created, Model: h.cfg.ServedName, Choices: choices}
	}
	includeUsage := req.StreamOptions != nil && req.StreamOptions.IncludeUsage
	finish := a.finish
	// lastChunk puts the fingerprint on the finish chunk, or on the usage chunk
	// when there is one, as vLLM does.
	lastChunk := func(c chunk) chunk {
		if !includeUsage {
			c.SystemFingerprint = h.fingerprint()
		}
		return c
	}
	role := newChunk(chunkChoice{Delta: delta{Role: "assistant", Content: new(string)}})

	n := 0
	total, nr := a.tokens(), len(a.reasoning)
	ok := h.generateTokens(r, s, total, func(g int) bool {
		n++
		if g == 0 && !send(role) {
			return false
		}
		var c chunkChoice
		var id int32
		switch j := g - nr; {
		case g < nr:
			text := deltaText(g, a.reasoning[g])
			c.Delta.Reasoning, id = &text, a.reasoningIDs[g]
		case a.tool != nil:
			if j == 0 {
				name := deltaToolCall{Index: 0, ID: "call_0", Type: "function", Function: &deltaFunction{Name: a.tool.Name}}
				if !send(newChunk(chunkChoice{Delta: delta{ToolCalls: []deltaToolCall{name}}})) {
					return false
				}
			}
			c.Delta.ToolCalls, id = []deltaToolCall{{Index: 0, Function: &deltaFunction{Arguments: a.words[j]}}}, a.ids[j]
		default:
			text := deltaText(j, a.words[j])
			c.Delta.Content, id = &text, a.ids[j]
		}
		if req.ReturnTokenIDs {
			c.TokenIDs = []int32{id}
		}
		last := g == total-1
		if last {
			c.FinishReason = &finish
		}
		out := newChunk(c)
		if last {
			out = lastChunk(out)
		}
		if !send(out) {
			return false
		}
		return !h.stalls(r, g)
	})
	if !ok {
		return n, false
	}
	if includeUsage {
		c := newChunk()
		c.Usage = &u
		c.SystemFingerprint = h.fingerprint()
		if !send(c) {
			return n, false
		}
	}
	return n, sw.done()
}

// sseWriter writes server-sent events as vLLM does: each event is
// "data: <json>" and a blank line, flushed at once. The response headers go
// out with the first event.
type sseWriter struct {
	w       http.ResponseWriter
	fl      http.Flusher
	started bool
}

func newSSEWriter(w http.ResponseWriter) *sseWriter {
	fl, _ := w.(http.Flusher)
	return &sseWriter{w: w, fl: fl}
}

// send writes v as one event; it returns false if v cannot be encoded or the
// client went away.
func (s *sseWriter) send(v any) bool {
	b, err := json.Marshal(v)
	if err != nil {
		return false
	}
	if !s.started {
		s.w.Header().Set("Content-Type", "text/event-stream")
		s.w.Header().Set("Cache-Control", "no-cache")
		s.w.WriteHeader(http.StatusOK)
		s.started = true
	}
	if _, err := fmt.Fprintf(s.w, "data: %s\n\n", b); err != nil {
		return false
	}
	s.flush()
	return true
}

// done writes the last event, "data: [DONE]".
func (s *sseWriter) done() bool {
	if _, err := fmt.Fprint(s.w, "data: [DONE]\n\n"); err != nil {
		return false
	}
	s.flush()
	return true
}

func (s *sseWriter) flush() {
	if s.fl != nil {
		s.fl.Flush()
	}
}
