package enginedouble

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

// chatRequest is the part of ChatCompletionRequest the agent sends
// (vllm/entrypoints/openai/chat_completion/protocol.py).
type chatRequest struct {
	Model               string          `json:"model"`
	Messages            []message       `json:"messages"`
	MaxTokens           *int            `json:"max_tokens"`
	MaxCompletionTokens *int            `json:"max_completion_tokens"`
	Temperature         *float64        `json:"temperature"`
	Stream              bool            `json:"stream"`
	StreamOptions       *streamOptions  `json:"stream_options"`
	IgnoreEOS           bool            `json:"ignore_eos"`
	MinTokens           int             `json:"min_tokens"`
	ReturnTokenIDs      bool            `json:"return_token_ids"`
	Tools               []tool          `json:"tools"`
	ToolChoice          json.RawMessage `json:"tool_choice"`
}

type streamOptions struct {
	IncludeUsage bool `json:"include_usage"`
}

type tool struct {
	Function struct {
		Name string `json:"name"`
	} `json:"function"`
}

type usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// toolCall is the answer to a request with tools: one call of one function.
type toolCall struct {
	Name, Arguments string
}

// answer is what the double says to one request: the tokens of the reasoning,
// if the model reasons, then the tokens of the answer. For a tool call the
// answer's tokens are the pieces its arguments are streamed in.
type answer struct {
	reasoning    []string
	reasoningIDs []int32
	words        []string
	ids          []int32
	finish       string
	tool         *toolCall
}

// tokens is the count usage.completion_tokens reports: vLLM counts the
// reasoning tokens too.
func (a answer) tokens() int { return len(a.reasoning) + len(a.words) }

// allIDs are the token ids of the whole completion, reasoning first, as
// return_token_ids gives them.
func (a answer) allIDs() []int32 { return append(append([]int32(nil), a.reasoningIDs...), a.ids...) }

// toolCallFor decides whether the answer is a tool call: tools present, no
// tool_choice "none", and the no-tool-call mode off. The function is the one
// tool_choice names, else the first tool's.
func (h *handler) toolCallFor(req chatRequest) *toolCall {
	if len(req.Tools) == 0 || h.cfg.NoToolCall {
		return nil
	}
	name := req.Tools[0].Function.Name
	if len(req.ToolChoice) > 0 {
		var s string
		if json.Unmarshal(req.ToolChoice, &s) == nil {
			if s == "none" {
				return nil
			}
		} else {
			var obj struct {
				Function struct {
					Name string `json:"name"`
				} `json:"function"`
			}
			if json.Unmarshal(req.ToolChoice, &obj) == nil && obj.Function.Name != "" {
				name = obj.Function.Name
			}
		}
	}
	args := `{"city": "Krakow"}`
	if h.cfg.ToolMalformed {
		args = `{"city": `
	}
	return &toolCall{Name: name, Arguments: args}
}

// maxTokensOf resolves max_tokens as vLLM does: max_completion_tokens when
// set, else max_tokens (chat_completion/serving.py), else what remains of the
// context after the prompt. The error message is the double's own wording of
// a context-length overflow: the v0.32.0 source of vLLM's text was not read.
func (h *handler) maxTokensOf(req chatRequest, promptLen int) (int, error) {
	max := h.cfg.MaxModelLen - promptLen
	switch {
	case req.MaxCompletionTokens != nil:
		max = *req.MaxCompletionTokens
	case req.MaxTokens != nil:
		max = *req.MaxTokens
	}
	if max < 1 || promptLen+max > h.cfg.MaxModelLen {
		return 0, fmt.Errorf("This model's maximum context length is %d tokens. However, you requested %d output tokens and your prompt contains at least %d input tokens, for a total of %d tokens. Please reduce the length of the input prompt or the number of requested output tokens.",
			h.cfg.MaxModelLen, max, promptLen, promptLen+max)
	}
	return max, nil
}

// chat is POST /v1/chat/completions.
func (h *handler) chat(w http.ResponseWriter, r *http.Request) {
	if h.cfg.ErrorsOnly {
		writeError(w, http.StatusInternalServerError, "InternalServerError", "the engine failed")
		return
	}
	var req chatRequest
	if err := decodeBody(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "BadRequestError", err.Error())
		return
	}
	if len(req.Messages) == 0 {
		writeError(w, http.StatusBadRequest, "BadRequestError", "messages must not be empty")
		return
	}
	if req.Model != "" && req.Model != h.cfg.ServedName {
		writeError(w, http.StatusNotFound, "NotFoundError", fmt.Sprintf("The model `%s` does not exist.", req.Model))
		return
	}
	_, promptIDs := tokenize(renderChat(req.Messages, true, h.cfg.TemplateOverhead))
	max, err := h.maxTokensOf(req, len(promptIDs))
	if err != nil {
		writeError(w, http.StatusBadRequest, "BadRequestError", err.Error())
		return
	}
	if req.MinTokens < 0 || req.MinTokens > max {
		// SamplingParams._verify_args, vllm/sampling_params.py:694-701.
		writeError(w, http.StatusBadRequest, "BadRequestError", fmt.Sprintf("min_tokens must be less than or equal to max_tokens=%d, got %d.", max, req.MinTokens))
		return
	}
	ans := h.answerFor(req, promptIDs, max)

	rec := LogEvent{Event: "request", Path: r.URL.Path, PromptTokens: len(promptIDs), MaxTokens: max, Stream: req.Stream, Outcome: "cancelled"}
	finish := "abort"
	defer func() {
		if h.e.record != nil {
			h.e.record(rec)
		}
	}()

	ctx := r.Context()
	s, ok := h.e.admit(ctx, promptIDs)
	if !ok {
		return
	}
	defer func() { s.release(finish) }()
	wait := h.e.beforeFirstToken(shapeKey{len(promptIDs), max}, ans.tokens())
	if !sleep(ctx, wait) {
		return
	}

	u := usage{len(promptIDs), ans.tokens(), len(promptIDs) + ans.tokens()}
	var sent int
	var completed bool
	if req.Stream {
		sent, completed = h.stream(w, r, req, ans, s, u)
	} else {
		sent, completed = h.single(w, r, req, ans, s, u)
	}
	rec.Tokens = sent
	if completed {
		rec.Outcome, finish = "completed", ans.finish
	}
}

// reasoningLen is how many of the max tokens the model spends reasoning: the
// reasoning-first count, all of them for reasoning-forever.
func (c Config) reasoningLen(max int) int {
	switch {
	case c.ReasoningForever:
		return max
	case c.ReasoningFirst > 0:
		return min(c.ReasoningFirst, max)
	}
	return 0
}

// answerFor decides what the model says. The completion is one walk over the
// vocabulary: its first tokens are the reasoning, when the model reasons, so
// a greedy request gives the same token ids with or without the reasoning
// modes up to where the answers part. A reply that spends all max tokens
// reasoning has no answer and finishes with "length".
func (h *handler) answerFor(req chatRequest, promptIDs []int32, limit int) answer {
	r := h.cfg.reasoningLen(limit)
	tc := h.toolCallFor(req)
	if r >= limit {
		tc = nil
	}
	greedy := req.Temperature == nil || *req.Temperature == 0
	p := genParams{promptIDs: promptIDs, maxTokens: limit - r, minTokens: max(0, req.MinTokens-r), ignoreEOS: req.IgnoreEOS, greedy: greedy}

	if tc != nil {
		pieces := argumentPieces(tc.Arguments)
		_, ids := tokenize(tc.Arguments)
		finish := "tool_calls"
		if r+len(pieces) > limit { // the call is cut off by the output limit
			pieces, ids, finish = pieces[:limit-r], ids[:limit-r], "length"
		}
		a := answer{words: pieces, ids: ids, finish: finish, tool: tc}
		if r > 0 {
			a.reasoning, a.reasoningIDs = outputWords(output{tokens: walk(p.seed(), r)})
		}
		return a
	}

	if greedy {
		p.repeat = h.e.repeatOf(repeatKey{promptSeed(promptIDs), limit, req.MinTokens, req.IgnoreEOS})
	}
	n, finish := r, "length"
	if limit > r {
		an, f := h.cfg.length(p)
		n, finish = r+an, f
	}
	words, ids := outputWords(output{tokens: h.cfg.tokens(p, n)})
	return answer{reasoning: words[:r], reasoningIDs: ids[:r], words: words[r:], ids: ids[r:], finish: finish}
}

// argumentPieces splits tool-call arguments into one piece per token, each
// holding its word with the white space before it (the rest at the end goes
// to the last piece), so the pieces joined are the arguments exactly.
func argumentPieces(args string) []string {
	var pieces []string
	start, inWord := 0, false
	for i, r := range args {
		space := r == ' ' || r == '\t' || r == '\n' || r == '\r'
		switch {
		case !space && !inWord:
			inWord = true
		case space && inWord:
			pieces = append(pieces, args[start:i])
			start, inWord = i, false
		}
	}
	if len(pieces) == 0 || inWord {
		return append(pieces, args[start:])
	}
	pieces[len(pieces)-1] += args[start:]
	return pieces
}

// content is the answer's text: the tokens' words joined by a space.
func (a answer) content() string { return strings.Join(a.words, " ") }

// fingerprint is what vLLM reports as system_fingerprint; with reports-nothing
// it is absent.
func (h *handler) fingerprint() string {
	if h.cfg.ReportsNothing {
		return ""
	}
	return "fp-enginedouble"
}

// generateTokens lets the engine spend each token's time: TokenDelay before
// it, and its count in the metrics. It returns false if the client went away.
// emit is called for each token before the next one's wait.
func (h *handler) generateTokens(r *http.Request, s *slot, n int, emit func(i int) bool) bool {
	for i := 0; i < n; i++ {
		if !sleep(r.Context(), h.cfg.TokenDelay) {
			return false
		}
		s.token()
		if emit != nil && !emit(i) {
			return false
		}
	}
	return true
}
