package enginedouble

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"unicode/utf8"
)

// sampling is what a request of either generation endpoint says about how to
// generate and how to answer: the fields ChatCompletionRequest
// (vllm/entrypoints/openai/chat_completion/protocol.py) and CompletionRequest
// (vllm/entrypoints/openai/completion/protocol.py) share, under the same names.
type sampling struct {
	Temperature *float64 `json:"temperature"`
	// The other sampling parameters of a vLLM request, which both
	// endpoints take under these names. The double does not sample with
	// them; it records what arrived (RequestSampling), and a Seed makes a
	// sampled request repeatable (answerFor).
	TopP              *float64       `json:"top_p"`
	TopK              *int64         `json:"top_k"`
	MinP              *float64       `json:"min_p"`
	RepetitionPenalty *float64       `json:"repetition_penalty"`
	PresencePenalty   *float64       `json:"presence_penalty"`
	FrequencyPenalty  *float64       `json:"frequency_penalty"`
	Seed              *int64         `json:"seed"`
	Stream            bool           `json:"stream"`
	StreamOptions     *streamOptions `json:"stream_options"`
	IgnoreEOS         bool           `json:"ignore_eos"`
	MinTokens         int            `json:"min_tokens"`
	ReturnTokenIDs    bool           `json:"return_token_ids"`
}

// chatRequest is the part of ChatCompletionRequest the agent sends.
type chatRequest struct {
	sampling
	Model               string          `json:"model"`
	Messages            []message       `json:"messages"`
	MaxTokens           *int            `json:"max_tokens"`
	MaxCompletionTokens *int            `json:"max_completion_tokens"`
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

// toolCallFor decides whether the answer is a tool call. With no tools, the
// no-tool-call mode, or tool_choice "none", it is not. A tool_choice that names
// a function forces a call of it, and "required" a call of the first tool;
// with "auto" or no tool_choice the model calls a tool only when the last user
// message names it, as a model asked in words to use a tool does, so a prompt
// that does not ask for a call gets text, however many tools it is offered
// (PR 87 review, Astra B9: a double that calls on any tools masked a warm-up
// prompt that never asked for one).
func (h *handler) toolCallFor(req chatRequest) *toolCall {
	if len(req.Tools) == 0 || h.cfg.NoToolCall {
		return nil
	}
	name := ""
	if len(req.ToolChoice) > 0 {
		var s string
		if json.Unmarshal(req.ToolChoice, &s) == nil {
			switch s {
			case "none":
				return nil
			case "required":
				name = req.Tools[0].Function.Name
			}
		} else {
			var obj struct {
				Function struct {
					Name string `json:"name"`
				} `json:"function"`
			}
			if json.Unmarshal(req.ToolChoice, &obj) == nil {
				name = obj.Function.Name
			}
		}
	}
	if name == "" {
		last := lastUserText(req.Messages)
		for _, t := range req.Tools {
			if t.Function.Name != "" && strings.Contains(last, t.Function.Name) {
				name = t.Function.Name
				break
			}
		}
	}
	if name == "" {
		return nil
	}
	args := `{"city": "Krakow"}`
	switch {
	case h.cfg.ToolMalformed:
		args = `{"city": `
	case h.cfg.ToolArgsNull:
		args = `null`
	case h.cfg.ToolArgsBadType:
		args = `{"city": 42}`
	}
	return &toolCall{Name: name, Arguments: args}
}

// lastUserText is the text of the last user message, "" when there is none.
func lastUserText(msgs []message) string {
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i].Role == "user" {
			return msgs[i].text()
		}
	}
	return ""
}

// maxTokensOf resolves max_tokens as vLLM does: max_completion_tokens when
// set (a chat request's; nil for a completions request), else max_tokens
// (chat_completion/serving.py:343-347), else what remains of the context after
// the prompt (get_max_tokens, serve/utils/api_utils.py:188-205). A completions
// request always has a max_tokens here: its null is the default 16
// (completions.go). The error message is the double's own wording of a
// context-length overflow: the v0.32.0 source of vLLM's text was not read.
//
// Known difference, not changed by issue 326: when the prompt and the limit
// each fit the context but not together, vLLM serves the request with the limit
// clamped to what remains (get_max_tokens, api_utils.py:196-205), and refuses
// only a prompt over the context (renderers/params.py:515-535) or a limit over
// the whole context (params.py:256-268). The double refuses the sum, as
// TestOverlongPromptIs400 pins.
func (h *handler) maxTokensOf(promptLen int, maxTokens, maxCompletionTokens *int) (int, error) {
	max := h.cfg.MaxModelLen - promptLen
	switch {
	case maxCompletionTokens != nil:
		max = *maxCompletionTokens
	case maxTokens != nil:
		max = *maxTokens
	}
	if max < 1 || promptLen+max > h.cfg.MaxModelLen {
		return 0, fmt.Errorf("This model's maximum context length is %d tokens. However, you requested %d output tokens and your prompt contains at least %d input tokens, for a total of %d tokens. Please reduce the length of the input prompt or the number of requested output tokens.",
			h.cfg.MaxModelLen, max, promptLen, promptLen+max)
	}
	return max, nil
}

// requestRecord is the "request" LogEvent of a generation request of either
// endpoint, made as soon as its body is decoded: the Outcome is "refused"
// until run takes the request, with the sampling parameters the request
// carried and the tail of its prompt text.
func requestRecord(r *http.Request, sp sampling, prompt string) *LogEvent {
	return &LogEvent{Event: "request", Path: r.URL.Path, Stream: sp.Stream, Outcome: "refused",
		Sampling: samplingOf(sp), Tail: tailOf(prompt)}
}

// recordEnd writes rec to the engine's log when a request ends, if its body
// decoded. Every request that decoded is recorded, a refused one too
// (errors-only, a 4xx), so a test counts the attempts a check made, not only
// the answers it got.
func (h *handler) recordEnd(decoded bool, rec *LogEvent) {
	if decoded && h.e.record != nil {
		h.e.record(*rec)
	}
}

// engineFails answers 500 and says so when the errors-only mode is on: both
// generation endpoints fail the same way.
func (h *handler) engineFails(w http.ResponseWriter) bool {
	if h.cfg.ErrorsOnly {
		writeError(w, http.StatusInternalServerError, "InternalServerError", "the engine failed")
	}
	return h.cfg.ErrorsOnly
}

// modelExists answers 404 and returns false for a request that names a model
// other than the served one; an absent name means the served model.
func (h *handler) modelExists(w http.ResponseWriter, model string) bool {
	if model != "" && model != h.cfg.ServedName {
		writeError(w, http.StatusNotFound, "NotFoundError", fmt.Sprintf("The model `%s` does not exist.", model))
		return false
	}
	return true
}

// limitFor resolves a tokenized request's output limit and checks min_tokens
// against it, answering 400 and returning false on a bad request. Both
// generation endpoints share it. The limit it returns is the resolved one
// whenever it resolved, a min_tokens refusal included, so the record of a
// refused request carries it; it is 0 when the limit did not resolve.
func (h *handler) limitFor(w http.ResponseWriter, sp sampling, promptLen int, maxTokens, maxCompletionTokens *int) (int, bool) {
	max, err := h.maxTokensOf(promptLen, maxTokens, maxCompletionTokens)
	if err != nil {
		writeError(w, http.StatusBadRequest, "BadRequestError", err.Error())
		return 0, false
	}
	if sp.MinTokens < 0 || sp.MinTokens > max {
		// SamplingParams._verify_args, vllm/sampling_params.py:694-701.
		writeError(w, http.StatusBadRequest, "BadRequestError", fmt.Sprintf("min_tokens must be less than or equal to max_tokens=%d, got %d.", max, sp.MinTokens))
		return max, false
	}
	return max, true
}

// chat is POST /v1/chat/completions.
func (h *handler) chat(w http.ResponseWriter, r *http.Request) {
	var req chatRequest
	decodeErr := decodeBody(r, &req)
	rec := requestRecord(r, req.sampling, lastUserText(req.Messages))
	defer h.recordEnd(decodeErr == nil, rec)
	// The errors-only mode answers 500 to any body, after the decode, so
	// the record of a refused request carries what it asked for.
	if h.engineFails(w) {
		return
	}
	if decodeErr != nil {
		writeError(w, http.StatusBadRequest, "BadRequestError", decodeErr.Error())
		return
	}
	if len(req.Messages) == 0 {
		writeError(w, http.StatusBadRequest, "BadRequestError", "messages must not be empty")
		return
	}
	if !h.modelExists(w, req.Model) {
		return
	}
	_, promptIDs := tokenize(renderChat(req.Messages, true, h.cfg.TemplateOverhead))
	rec.PromptTokens = len(promptIDs)
	max, ok := h.limitFor(w, req.sampling, len(promptIDs), req.MaxTokens, req.MaxCompletionTokens)
	rec.MaxTokens = max
	if !ok {
		return
	}
	ans := h.answerFor(req.sampling, h.toolCallFor(req), promptIDs, max)
	h.run(r, rec, promptIDs, max, ans, req.Stream, func(s *slot, u usage) (int, bool) {
		if req.Stream {
			return h.stream(w, r, req, ans, s, u)
		}
		return h.single(w, r, req, ans, s, u)
	})
}

// run takes one tokenized, validated request through the engine, whatever its
// endpoint: it is admitted to a sequence slot, waits for its first token
// (slow-first, never-settle), and then respond generates the tokens and writes
// the answer, returning how many tokens it sent and whether the answer was
// completed. run fills in rec, the request's record, which the endpoint writes
// to the engine's log when the request ends (recordEnd): from here on the
// Outcome is "cancelled" unless the answer completes. run releases the slot
// with the finish reason of a completed answer, "abort" otherwise.
func (h *handler) run(r *http.Request, rec *LogEvent, promptIDs []int32, limit int, ans answer, stream bool, respond func(s *slot, u usage) (int, bool)) {
	rec.Outcome = "cancelled"
	finish := "abort"

	ctx := r.Context()
	s, ok := h.e.admit(ctx, promptIDs)
	if !ok {
		return
	}
	defer func() { s.release(finish) }()
	if h.e.record != nil {
		h.e.record(LogEvent{Event: "admitted", PromptTokens: len(promptIDs), MaxTokens: limit, Stream: stream})
	}
	wait, nth := h.e.beforeFirstToken(shapeKey{len(promptIDs), limit}, ans.tokens())
	rec.InjectedNS, rec.Nth = wait.Nanoseconds(), nth
	if !sleep(ctx, wait) {
		return
	}

	u := usage{len(promptIDs), ans.tokens(), len(promptIDs) + ans.tokens()}
	sent, completed := respond(s, u)
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
// reasoning has no answer and finishes with "length". tc is the tool call the
// model makes, nil for none (a completions request has none).
func (h *handler) answerFor(sp sampling, tc *toolCall, promptIDs []int32, limit int) answer {
	r := h.cfg.reasoningLen(limit)
	if r >= limit {
		tc = nil
	}
	greedy := sp.Temperature == nil || *sp.Temperature == 0
	p := genParams{promptIDs: promptIDs, maxTokens: limit - r, minTokens: max(0, sp.MinTokens-r), ignoreEOS: sp.IgnoreEOS, greedy: greedy}
	if !greedy {
		// A sampled request with a seed draws the same samples every time it
		// is sent, as vLLM's seeded sampling does when its batch is invariant
		// (the double stands for that case, not for vLLM's known limit that a
		// seed does not guarantee reproducibility online). One without a
		// seed draws at random.
		p.seeded, p.seedValue = sp.Seed != nil, 0
		if sp.Seed != nil {
			p.seedValue = *sp.Seed
		}
	}

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

	if p.repeatable() {
		p.repeat = h.e.repeatOf(repeatKey{promptSeed(promptIDs), limit, sp.MinTokens, sp.IgnoreEOS, p.seeded, p.seedValue})
	}
	n, finish := r, "length"
	if limit > r {
		an, f := h.cfg.length(p)
		n, finish = r+an, f
	}
	words, ids := outputWords(output{tokens: h.cfg.tokens(p, n)})
	return answer{reasoning: words[:r], reasoningIDs: ids[:r], words: words[r:], ids: ids[r:], finish: finish}
}

// samplingOf is the sampling parameters a request carried, as sent.
func samplingOf(sp sampling) *RequestSampling {
	return &RequestSampling{
		Temperature: sp.Temperature, TopP: sp.TopP, TopK: sp.TopK, MinP: sp.MinP,
		RepetitionPenalty: sp.RepetitionPenalty, PresencePenalty: sp.PresencePenalty, FrequencyPenalty: sp.FrequencyPenalty,
		Seed: sp.Seed,
	}
}

// tailBytes is how much of a request's prompt text (a chat request's last
// user message, a completions request's prompt) the log keeps.
const tailBytes = 120

// tailOf is the last tailBytes bytes of a prompt, from a character boundary:
// enough to tell the agent's checks apart by their fixed closing words. The
// prompts are synthetic, so the log holds no user text.
func tailOf(text string) string {
	if len(text) <= tailBytes {
		return text
	}
	t := text[len(text)-tailBytes:]
	for len(t) > 0 && !utf8.RuneStart(t[0]) {
		t = t[1:]
	}
	return t
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

// stalls is the stall mode: after the first token (g is 0) of a stream the
// engine hangs until the client goes away, and stalls returns true. Both
// generation endpoints' streams call it after sending a token.
func (h *handler) stalls(r *http.Request, g int) bool {
	if g == 0 && h.cfg.Stall {
		<-r.Context().Done()
		return true
	}
	return false
}
