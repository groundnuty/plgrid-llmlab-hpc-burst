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

// answer is what the double says to one request.
type answer struct {
	words  []string
	ids    []int32
	finish string
	tool   *toolCall
}

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
// set, else max_tokens, else what remains of the context after the prompt.
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
	wait := h.e.beforeFirstToken(shapeKey{len(promptIDs), max}, len(ans.words))
	if !sleep(ctx, wait) {
		return
	}

	u := usage{len(promptIDs), len(ans.words), len(promptIDs) + len(ans.words)}
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

// answerFor decides what the model says.
func (h *handler) answerFor(req chatRequest, promptIDs []int32, max int) answer {
	if tc := h.toolCallFor(req); tc != nil {
		words, ids := tokenize(tc.Arguments)
		return answer{words: words, ids: ids, finish: "tool_calls", tool: tc}
	}
	greedy := req.Temperature == nil || *req.Temperature == 0
	p := genParams{promptIDs: promptIDs, maxTokens: max, minTokens: req.MinTokens, ignoreEOS: req.IgnoreEOS, greedy: greedy}
	if greedy {
		p.repeat = h.e.repeatOf(repeatKey{promptSeed(promptIDs), max, req.MinTokens, req.IgnoreEOS})
	}
	out := h.cfg.generate(p)
	words, ids := outputWords(out)
	return answer{words: words, ids: ids, finish: out.finish}
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
