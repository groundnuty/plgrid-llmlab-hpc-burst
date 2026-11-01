package enginedouble

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"
)

// completionRequest is the part of CompletionRequest
// (vllm/entrypoints/openai/completion/protocol.py) the double reads: a prompt
// that is a string, and the sampling fields the chat path accepts, under the
// same names. The double does not read the other fields (n, echo, stop,
// logprobs, suffix); it ignores them.
type completionRequest struct {
	sampling
	Model     string          `json:"model"`
	Prompt    json.RawMessage `json:"prompt"`
	MaxTokens *int            `json:"max_tokens"`
}

// defaultCompletionMaxTokens is what a completions request that does not name
// max_tokens gets: CompletionRequest.max_tokens defaults to 16
// (completion/protocol.py:63). "max_tokens": null is the rest of the context,
// as in chat.
const defaultCompletionMaxTokens = 16

// promptText is the prompt as the double reads it: a JSON string.
func promptText(raw json.RawMessage) (string, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return "", errors.New("a completions request needs a prompt")
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", errors.New("the prompt must be a string: the double reads no other form")
	}
	return s, nil
}

// completion is POST /v1/completions. It is the chat path with a prompt in
// place of messages: the same tokenizer, limit, generation (answerFor, with no
// tools), admission, timing, modes and metrics (run), and only the shape of
// the answer is its own.
//
// The modes apply as they do to chat, except the ones that are about chat by
// design. Tool calls need tools, which a completions request has none of, so
// the tool-* and no-tool-call modes change nothing here. The reasoning modes
// do: the model reasons for the same number of tokens and the finish reason
// and usage are the same, but there is no reasoning parser on this endpoint,
// so the reasoning tokens are part of choices[0].text, where chat reports
// them as message.reasoning.
func (h *handler) completion(w http.ResponseWriter, r *http.Request) {
	if h.engineFails(w) {
		return
	}
	dflt := defaultCompletionMaxTokens
	req := completionRequest{MaxTokens: &dflt} // null in the body clears it; an absent field keeps it
	if err := decodeBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "BadRequestError", err.Error())
		return
	}
	prompt, err := promptText(req.Prompt)
	if err != nil {
		writeError(w, http.StatusBadRequest, "BadRequestError", err.Error())
		return
	}
	if !h.modelExists(w, req.Model) {
		return
	}
	_, promptIDs := tokenize(prompt)
	if len(promptIDs) == 0 {
		// vLLM refuses a prompt of no tokens (v1/engine/input_processor.py,
		// _validate_prompt_len). Chat cannot meet this: its template adds tokens.
		writeError(w, http.StatusBadRequest, "BadRequestError", "The decoder prompt cannot be empty")
		return
	}
	max, ok := h.limitFor(w, req.sampling, len(promptIDs), req.MaxTokens, nil)
	if !ok {
		return
	}
	ans := h.answerFor(req.sampling, nil, promptIDs, max)
	h.run(r, promptIDs, max, ans, req.Stream, func(s *slot, u usage) (int, bool) {
		if req.Stream {
			return h.completionStream(w, r, req, ans, s, u)
		}
		return h.completionSingle(w, r, req, ans, s, u)
	})
}

// text is the completion's tokens joined by a space, the reasoning first.
func (a answer) text() string { return strings.Join(a.allWords(), " ") }

// allWords are the words of the whole completion, reasoning first.
func (a answer) allWords() []string { return append(append([]string(nil), a.reasoning...), a.words...) }

// completionChoice is CompletionResponseChoice and, in a stream,
// CompletionResponseStreamChoice (completion/protocol.py). The double has no
// logprobs and no stop strings, so logprobs and stop_reason are null.
type completionChoice struct {
	Index        int     `json:"index"`
	Text         string  `json:"text"`
	Logprobs     any     `json:"logprobs"`
	FinishReason *string `json:"finish_reason"`
	StopReason   any     `json:"stop_reason"`
	TokenIDs     []int32 `json:"token_ids,omitempty"` // only with return_token_ids
}

// completionResponse is CompletionResponse.
type completionResponse struct {
	ID                string             `json:"id"`
	Object            string             `json:"object"`
	Created           int64              `json:"created"`
	Model             string             `json:"model"`
	Choices           []completionChoice `json:"choices"`
	Usage             usage              `json:"usage"`
	SystemFingerprint string             `json:"system_fingerprint,omitempty"`
}

// completionChunk is CompletionStreamResponse.
type completionChunk struct {
	ID                string             `json:"id"`
	Object            string             `json:"object"`
	Created           int64              `json:"created"`
	Model             string             `json:"model"`
	Choices           []completionChoice `json:"choices"`
	Usage             *usage             `json:"usage,omitempty"`
	SystemFingerprint string             `json:"system_fingerprint,omitempty"`
}

// completionSingle answers a request that is not streamed. It returns the
// tokens generated and whether the answer was sent.
func (h *handler) completionSingle(w http.ResponseWriter, r *http.Request, req completionRequest, a answer, s *slot, u usage) (int, bool) {
	n := 0
	if !h.generateTokens(r, s, a.tokens(), func(int) bool { n++; return true }) {
		return n, false
	}
	finish := a.finish
	choice := completionChoice{Text: a.text(), FinishReason: &finish}
	if req.ReturnTokenIDs {
		choice.TokenIDs = a.allIDs()
	}
	writeJSON(w, http.StatusOK, completionResponse{
		ID: newCompletionID(), Object: "text_completion", Created: time.Now().Unix(), Model: h.cfg.ServedName,
		Choices: []completionChoice{choice}, Usage: u, SystemFingerprint: h.fingerprint(),
	})
	return n, true
}

// completionStream answers with server-sent events as vLLM's
// completion_stream_generator does (completion/serving.py): one chunk per
// token, with no role chunk, and the finish reason on the last of them (which
// carries that token's text); a usage chunk with no choices when
// stream_options.include_usage is set; then "data: [DONE]". Like the chat
// stream it sends the first chunk with the first token, and it stalls in the
// stall mode.
func (h *handler) completionStream(w http.ResponseWriter, r *http.Request, req completionRequest, a answer, s *slot, u usage) (int, bool) {
	sw := newSSEWriter(w)
	id, created := newCompletionID(), time.Now().Unix()
	words, ids := a.allWords(), a.allIDs()
	includeUsage := req.StreamOptions != nil && req.StreamOptions.IncludeUsage
	finish := a.finish
	newChunk := func(choices ...completionChoice) completionChunk {
		if choices == nil {
			choices = []completionChoice{}
		}
		return completionChunk{ID: id, Object: "text_completion", Created: created, Model: h.cfg.ServedName, Choices: choices}
	}

	n := 0
	ok := h.generateTokens(r, s, a.tokens(), func(g int) bool {
		n++
		c := completionChoice{Text: deltaText(g, words[g])}
		if req.ReturnTokenIDs {
			c.TokenIDs = []int32{ids[g]}
		}
		last := g == len(words)-1
		if last {
			c.FinishReason = &finish
		}
		out := newChunk(c)
		if last && !includeUsage { // the fingerprint rides on the usage chunk when there is one
			out.SystemFingerprint = h.fingerprint()
		}
		return sw.send(out) && !h.stalls(r, g)
	})
	if !ok {
		return n, false
	}
	if includeUsage {
		c := newChunk()
		c.Usage = &u
		c.SystemFingerprint = h.fingerprint()
		if !sw.send(c) {
			return n, false
		}
	}
	return n, sw.done()
}
