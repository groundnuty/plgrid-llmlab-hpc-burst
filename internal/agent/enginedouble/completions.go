package enginedouble

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"strings"
	"time"
)

// completionRequest is the part of CompletionRequest
// (vllm/entrypoints/openai/completion/protocol.py) the double reads: a prompt
// in one of its four forms (promptOf), and the sampling fields the chat path accepts (seed and
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

// promptOf is the prompt as the double reads it, in the four forms vLLM's
// CompletionRequest.prompt takes (completion/protocol.py, at v0.32.0:
// list[int] | list[list[int]] | str | list[str]): a string, which the double
// tokenizes; a list of token IDs, which are the prompt's tokens; and a batch,
// a list of strings or of token-ID lists, which the double answers as its
// first prompt, in one choice, as it answers one choice whatever n asks. text
// is the prompt's text for the record ("" for token IDs). An empty list, or
// an empty first prompt, has no tokens, which the handler refuses as vLLM
// does. Any other form is a 400.
func promptOf(raw json.RawMessage) (text string, ids []int32, err error) {
	if len(raw) == 0 || string(raw) == "null" {
		return "", nil, errors.New("a completions request needs a prompt")
	}
	bad := errors.New("the prompt must be a string, a list of strings, a list of token IDs or a list of token-ID lists: the double reads no other form")
	var s string
	if json.Unmarshal(raw, &s) == nil {
		_, ids = tokenize(s)
		return s, ids, nil
	}
	var list []json.RawMessage
	if json.Unmarshal(raw, &list) != nil {
		return "", nil, bad
	}
	if len(list) == 0 {
		return "", nil, nil
	}
	// Every element of the one form the first is (a null decodes into a Go string or number without an error, so the form is read off each
	// element's first byte).
	kind := jsonKind(list[0])
	for _, e := range list {
		if jsonKind(e) != kind {
			return "", nil, bad
		}
	}
	switch kind {
	case '"':
		var all []string
		if json.Unmarshal(raw, &all) != nil {
			return "", nil, bad
		}
		_, ids = tokenize(all[0])
		return all[0], ids, nil
	case '0':
		var flat []int64
		if json.Unmarshal(raw, &flat) != nil {
			return "", nil, bad
		}
		if ids, ok := tokenIDs(flat); ok {
			return "", ids, nil
		}
	case '[':
		var nested [][]json.RawMessage
		if json.Unmarshal(raw, &nested) != nil {
			return "", nil, bad
		}
		var first []int32
		for i, p := range nested {
			flat := make([]int64, len(p))
			for j, e := range p {
				if jsonKind(e) != '0' || json.Unmarshal(e, &flat[j]) != nil {
					return "", nil, bad
				}
			}
			got, ok := tokenIDs(flat)
			if !ok {
				return "", nil, bad
			}
			if i == 0 {
				first = got
			}
		}
		return "", first, nil
	}
	return "", nil, bad
}

// jsonKind is the form of a JSON value by its first byte: '"' for a string, '0' for a number, '[' for an array, and the byte itself for any
// other (null, true, false, an object).
func jsonKind(v json.RawMessage) byte {
	b := bytes.TrimSpace(v)
	if len(b) == 0 {
		return 0
	}
	if b[0] == '-' || (b[0] >= '0' && b[0] <= '9') {
		return '0'
	}
	return b[0]
}

// tokenIDs are a prompt's token IDs as the double counts them, false for one that is negative or past int32.
func tokenIDs(in []int64) ([]int32, bool) {
	out := make([]int32, len(in))
	for i, v := range in {
		if v < 0 || v > math.MaxInt32 {
			return nil, false
		}
		out[i] = int32(v)
	}
	return out, true
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
	prompt, promptIDs, promptErr := promptOf(req.Prompt) // prompt is "" for token IDs
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
