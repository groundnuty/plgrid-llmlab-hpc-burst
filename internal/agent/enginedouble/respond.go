package enginedouble

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"
)

var idCounter atomic.Uint64

func newID() string {
	return fmt.Sprintf("chatcmpl-%08x%08x", uint32(time.Now().Unix()), idCounter.Add(1))
}

type functionJSON struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type toolCallJSON struct {
	Index    *int         `json:"index,omitempty"` // streamed calls carry their index
	ID       string       `json:"id"`
	Type     string       `json:"type"`
	Function functionJSON `json:"function"`
}

func (a answer) toolCalls(streamed bool) []toolCallJSON {
	if a.tool == nil {
		return nil
	}
	c := toolCallJSON{ID: "call_0", Type: "function", Function: functionJSON{a.tool.Name, a.tool.Arguments}}
	if streamed {
		zero := 0
		c.Index = &zero
	}
	return []toolCallJSON{c}
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

type respMessage struct {
	Role      string         `json:"role"`
	Content   *string        `json:"content"`
	ToolCalls []toolCallJSON `json:"tool_calls,omitempty"`
}

// single answers a request that is not streamed. It returns the tokens
// generated and whether the answer was sent.
func (h *handler) single(w http.ResponseWriter, r *http.Request, req chatRequest, a answer, s *slot, u usage) (int, bool) {
	n := 0
	if !h.generateTokens(r, s, len(a.words), func(int) bool { n++; return true }) {
		return n, false
	}
	msg := respMessage{Role: "assistant", ToolCalls: a.toolCalls(false)}
	if a.tool == nil {
		c := a.content()
		msg.Content = &c
	}
	choice := respChoice{Message: msg, FinishReason: a.finish}
	if req.ReturnTokenIDs {
		choice.TokenIDs = a.ids
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
	Role      string         `json:"role,omitempty"`
	Content   *string        `json:"content,omitempty"`
	ToolCalls []toolCallJSON `json:"tool_calls,omitempty"`
}

// stream answers a request with server-sent events, as vLLM's
// chat_completion_stream_generator does (chat_completion/serving.py): a first
// chunk whose delta is the role with empty content, one chunk per token, the
// finish reason on the last of them, a usage chunk with no choices when
// stream_options.include_usage is set, then "data: [DONE]". Each event is
// "data: <json>" and a blank line, flushed at once. token_ids ride on a chunk
// only with return_token_ids, as in vLLM.
//
// The first chunk is sent with the first token, after the wait before it, so
// the time to the first byte is the time to the first token. In the stall mode
// the stream stops after the first token and waits for the client to go.
func (h *handler) stream(w http.ResponseWriter, r *http.Request, req chatRequest, a answer, s *slot, u usage) (int, bool) {
	fl, _ := w.(http.Flusher)
	id, created := newID(), time.Now().Unix()
	started := false
	send := func(c chunk) bool {
		b, err := json.Marshal(c)
		if err != nil {
			return false
		}
		if !started {
			w.Header().Set("Content-Type", "text/event-stream")
			w.Header().Set("Cache-Control", "no-cache")
			w.WriteHeader(http.StatusOK)
			started = true
		}
		if _, err := fmt.Fprintf(w, "data: %s\n\n", b); err != nil {
			return false
		}
		if fl != nil {
			fl.Flush()
		}
		return true
	}
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
	if a.tool != nil {
		if !h.generateTokens(r, s, len(a.words), func(int) bool { n++; return true }) || !send(role) {
			return n, false
		}
		call := chunkChoice{Delta: delta{ToolCalls: a.toolCalls(true)}, FinishReason: &finish}
		if req.ReturnTokenIDs {
			call.TokenIDs = a.ids
		}
		if !send(lastChunk(newChunk(call))) {
			return n, false
		}
	} else {
		ok := h.generateTokens(r, s, len(a.words), func(i int) bool {
			n++
			if i == 0 && !send(role) {
				return false
			}
			text := deltaText(i, a.words[i])
			c := chunkChoice{Delta: delta{Content: &text}}
			if req.ReturnTokenIDs {
				c.TokenIDs = []int32{a.ids[i]}
			}
			last := i == len(a.words)-1
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
			if i == 0 && h.cfg.Stall {
				<-r.Context().Done() // hangs until the client goes away
				return false
			}
			return true
		})
		if !ok {
			return n, false
		}
	}
	if includeUsage {
		c := newChunk()
		c.Usage = &u
		c.SystemFingerprint = h.fingerprint()
		if !send(c) {
			return n, false
		}
	}
	if _, err := fmt.Fprint(w, "data: [DONE]\n\n"); err != nil {
		return n, false
	}
	if fl != nil {
		fl.Flush()
	}
	return n, true
}
