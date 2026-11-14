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
// that is a string, and the sampling fields the chat path accepts (seed and
// the recorded parameters included), under the same names. The double does
// not read the other fields (n, echo, stop, logprobs, suffix); it ignores
// them.
type completionRequest struct {
	sampling
	Model  string          `json:"model"`
	Prompt json.RawMessage `json:"prompt"`
	// MaxTokens is nil when the body leaves max_tokens out or sends null, which
	// the handler turns into defaultCompletionMaxTokens before the limit is
	// resolved.
	MaxTokens *int `json:"max_tokens"`
}

// defaultCompletionMaxTokens is what a completions request gets whose
// max_tokens is absent or null. CompletionRequest.max_tokens is
// `int | None = 16` (vllm/entrypoints/openai/completion/protocol.py:63, at
// v0.32.0), and the model validator normalize_null_max_tokens (mode="before",
// :430-436 of the same class) rewrites a body whose max_tokens is null to the
// field's default, so an explicit null is the default 16 and never "no limit".
// This is unlike chat, where both limit fields default to None and the rest of
// the context applies (chat_completion/protocol.py:229-234, serving.py:343-347,
// serve/utils/api_utils.py:188-205). Issue 326: an earlier reading of the field
// alone, without the validator, took a null here for the rest of the context.
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
	var req completionRequest
	decodeErr := decodeBody(r, &req)
	if req.MaxTokens == nil { // absent, or null: normalize_null_max_tokens (completion/protocol.py:430-436)
		dflt := defaultCompletionMaxTokens
		req.MaxTokens = &dflt
	}
	prompt, promptErr := promptText(req.Prompt) // "" when the prompt is not a string
	// The record is the chat path's: a request that decoded is recorded when
	// it ends, a refused one too, and errors-only answers after the decode.
	rec := requestRecord(r, req.sampling, prompt)
	defer h.recordEnd(decodeErr == nil, rec)
	if h.engineFails(w) {
		return
	}
	if decodeErr != nil {
		writeError(w, http.StatusBadRequest, "BadRequestError", decodeErr.Error())
		return
	}
	if promptErr != nil {
		writeError(w, http.StatusBadRequest, "BadRequestError", promptErr.Error())
		return
	}
	if !h.modelExists(w, req.Model) {
		return
	}
	_, promptIDs := tokenize(prompt)
	rec.PromptTokens = len(promptIDs)
	// A limit over the window is refused before the prompt is read, an empty
	// one included (maxTokensOf).
	if !h.windowFits(w, req.MaxTokens, nil) {
		return
	}
	if len(promptIDs) == 0 {
		// vLLM refuses a prompt of no tokens (v1/engine/input_processor.py,
		// _validate_prompt_len). Chat cannot meet this: its template adds tokens.
		writeError(w, http.StatusBadRequest, "BadRequestError", "The decoder prompt cannot be empty")
		return
	}
	max, ok := h.limitFor(w, req.sampling, len(promptIDs), req.MaxTokens, nil)
	rec.MaxTokens = max
	if !ok {
		return
	}
	ans := h.answerFor(req.sampling, nil, promptIDs, max)
	h.run(w, r, rec, promptIDs, max, ans, req.Stream, func(s *slot, u usage) (int, bool) {
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
	sw.started = s.headersSent
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
