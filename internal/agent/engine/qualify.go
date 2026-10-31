package engine

import (
	"context"
	"errors"
	"fmt"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The qualification requests' fixed texts. Synthetic, as every request the
// agent sends is.
const (
	chatPrompt        = "Say hello in one short sentence."
	toolPrompt        = "What is the weather in Krakow right now? Call the get_weather tool to find out."
	corruptionPrompt  = "Count from one to one hundred in English words, separated by commas."
	behaviourTail     = "Ignore the list of words above. In one short sentence, say what colour the sky is on a clear day."
	behaviourToolTail = "Ignore the list of words above. Call the get_weather tool for the city of Krakow."
	corruptionTokens  = 2 * agentapi.CorruptionPrefix
)

// Qualify runs the qualification checks the agent judges, and reads the values
// the controller judges (HBEP-7, Instance start; agentapi's documentation,
// Who decides): health, one chat request with its fingerprint, the tool-call
// smoke test when req.ToolCalls, the corruption check, and the behaviour
// check at the longest context; the KV capacity the engine reported at
// start-up and the served model names. The checks come in the order health,
// chat, toolCall, corruption, behaviour.
func (c *Client) Qualify(ctx context.Context, req agentapi.QualifyRequest) agentapi.QualifyResult {
	var res agentapi.QualifyResult
	// add records an outcome; a note is a fact the controller should read
	// beside the verdict, kept on a pass too.
	add := func(name agentapi.CheckName, note string, err error) {
		if err == nil {
			res.Checks = append(res.Checks, passedOutcome(name, note))
			return
		}
		message := err.Error()
		var re *reasonedError
		if errors.As(err, &re) {
			message = re.message
		}
		if note != "" {
			message += "; " + note
		}
		res.Checks = append(res.Checks, failedOutcome(name, reasonOf(err), message))
	}
	add(agentapi.CheckHealth, "", c.Probe(ctx))
	if cards, err := c.models(ctx); err == nil {
		for _, m := range cards {
			res.ServedModelNames = append(res.ServedModelNames, cut(m.ID))
		}
	}
	if m, err := c.metrics(ctx); err == nil {
		res.KVCapacityTokens = kvCapacity(m.cacheConfig())
	}
	fp, err := c.chatCheck(ctx)
	res.Fingerprint = cut(fp)
	add(agentapi.CheckChat, "", err)
	if req.ToolCalls {
		add(agentapi.CheckToolCall, "", c.toolCallCheck(ctx))
	}
	add(agentapi.CheckCorruption, "", c.corruptionCheck(ctx))
	run, err := c.behaviourCheck(ctx, req)
	add(agentapi.CheckBehaviour, run.note, err)
	return res
}

// passedOutcome is the outcome of a check that passed, with the note the
// controller should read beside it, cut to agentapi.MaxMessageBytes.
func passedOutcome(name agentapi.CheckName, note string) agentapi.CheckOutcome {
	return agentapi.CheckOutcome{Name: name, Passed: true, Message: agentapi.TruncateUTF8(note, agentapi.MaxMessageBytes)}
}

// failedOutcome is the one place that builds the outcome of a failed check,
// from a reason code (a CamelCase word, "" when the failure has none) and a
// message. agentapi.CheckOutcome will carry the code as a Reason field once the
// contracts PR lands; until then the code is the front of the message, so that
// "ReasoningBudgetExhausted: ..." survives the cut to MaxMessageBytes. Moving to
// the field changes this function only.
func failedOutcome(name agentapi.CheckName, reason, message string) agentapi.CheckOutcome {
	if reason != "" {
		message = reason + ": " + message
	}
	return agentapi.CheckOutcome{Name: name, Message: agentapi.TruncateUTF8(message, agentapi.MaxMessageBytes)}
}

// chatCheck sends one chat request; it passes when the engine answers with
// text, in either field: a reasoning model may spend all 32 tokens thinking.
func (c *Client) chatCheck(ctx context.Context) (string, error) {
	r, err := c.chat(ctx, chatRequest{Messages: []message{{Role: "user", Content: chatPrompt}}, MaxTokens: 32})
	if err != nil {
		return "", err
	}
	if r.content() == "" && r.reasoning() == "" {
		return r.SystemFingerprint, fmt.Errorf("the chat answer is empty")
	}
	return r.SystemFingerprint, nil
}

// toolCallTokens is the tool-call smoke test's answer budget.
const toolCallTokens = 128

// toolCallCheck is the tool-call smoke test: offered the weather tool and
// asked to use it, the engine answers with a well-formed call of it. Only the
// final answer is judged, never the reasoning; with a reasoning parser the
// request asks for reasoningTokens more.
func (c *Client) toolCallCheck(ctx context.Context) error {
	budget := int32(toolCallTokens)
	if c.reasoning {
		budget += reasoningTokens
	}
	r, err := c.chat(ctx, chatRequest{
		Messages:   []message{{Role: "user", Content: toolPrompt}},
		MaxTokens:  budget,
		Tools:      []tool{weatherTool},
		ToolChoice: "auto",
	})
	if err != nil {
		return err
	}
	if err := r.reasoningExhausted(budget); err != nil {
		return err
	}
	return wellFormedToolCall(r)
}

// wellFormedToolCall checks that the answer calls the weather tool, and that
// every call it makes is valid: the type, the name and the arguments, which
// must be a JSON object that conforms to the tool's schema (validateToolCall).
func wellFormedToolCall(r chatResponse) error {
	if len(r.Choices) == 0 || len(r.Choices[0].Message.ToolCalls) == 0 {
		return fmt.Errorf("the answer calls no tool")
	}
	for _, tc := range r.Choices[0].Message.ToolCalls {
		if err := validateToolCall(weatherTool, tc.Type, tc.Function.Name, tc.Function.Arguments); err != nil {
			return err
		}
	}
	return nil
}

// corruptionCheck sends one greedy request three times in a row and judges
// the token outputs by agentapi.CorruptionVerdict: they share their first 32
// tokens and none is empty or a single repeated token (HBEP-7, Instance start).
// The outputs never leave the job.
func (c *Client) corruptionCheck(ctx context.Context) error {
	outs := make([][]int32, 0, agentapi.CorruptionOutputs)
	for range agentapi.CorruptionOutputs {
		r, err := c.chat(ctx, chatRequest{
			Messages:       []message{{Role: "user", Content: corruptionPrompt}},
			MaxTokens:      corruptionTokens,
			MinTokens:      corruptionTokens,
			IgnoreEOS:      true,
			ReturnTokenIDs: true,
		})
		if err != nil {
			return err
		}
		if len(r.Choices) == 0 {
			return fmt.Errorf("the answer has no choice")
		}
		if r.Choices[0].TokenIDs == nil {
			return fmt.Errorf("the engine returned no token ids (return_token_ids)")
		}
		outs = append(outs, r.Choices[0].TokenIDs)
	}
	if ok, msg := agentapi.CorruptionVerdict(outs); !ok {
		return fmt.Errorf("%s", msg)
	}
	return nil
}

// behaviourRun is what the behaviour check reports besides its verdict.
type behaviourRun struct {
	// contextTokens is the context the prompt ran at, in the unit of
	// Shapes.ContextLongest: tokens beyond the chat template's overhead. It
	// moves into a contract field (ContextTokens) once the contracts PR lands;
	// until then it is in the note.
	contextTokens int32
	// note says what context the check ran at, when a reasoning parser is
	// declared; it is the passing outcome's message.
	note string
}

// behaviourCheck sends one request at the workload's longest context with the
// typical output: the answer must be well-formed (text, or a well-formed
// tool call when tool calls are on) and must not loop (HBEP-7, Instance
// start). Only the final answer is judged, never the reasoning.
//
// With a reasoning parser the request asks for the typical output plus
// reasoningTokens, and only this prompt is shortened to leave that room
// (science agent's ruling of 31 Oct 2026):
//
//	context = min(ContextLongest, max(ContextTypical, max_model_len - template overhead - OutputTypical - reasoningTokens))
//
// max_model_len is read from /v1/models. Below ContextTypical the check runs
// at ContextTypical and says so: it never fails for lack of room (the engine may
// still refuse the request, and then the check fails with its error). An engine
// that reports no max_model_len cannot be fitted, and runs at ContextTypical
// too. The note names the context the check ran at, on a pass too.
func (c *Client) behaviourCheck(ctx context.Context, req agentapi.QualifyRequest) (run behaviourRun, err error) {
	tail, tools, choice := behaviourTail, []tool(nil), ""
	if req.ToolCalls {
		tail, tools, choice = behaviourToolTail, []tool{weatherTool}, "auto"
	}
	budget, tokens := req.Shapes.OutputTypical, req.Shapes.ContextLongest
	if c.reasoning {
		budget += reasoningTokens
		room, err := c.contextRoom(ctx, budget)
		if err != nil {
			return run, err
		}
		tokens, run.note = reasoningContext(req.Shapes, room, budget)
	}
	run.contextTokens = tokens
	content, err := c.prompt(ctx, tokens, tail, tools, true)
	if err != nil {
		return run, err
	}
	r, err := c.chat(ctx, chatRequest{
		Messages:       []message{{Role: "user", Content: content}},
		MaxTokens:      budget,
		ReturnTokenIDs: true,
		Tools:          tools,
		ToolChoice:     choice,
	})
	if err != nil {
		return run, err
	}
	if len(r.Choices) == 0 {
		return run, fmt.Errorf("the answer has no choice")
	}
	if err := r.reasoningExhausted(budget); err != nil {
		return run, err
	}
	ch := r.Choices[0]
	if ch.FinishReason == "length" && IsLoop(ch.TokenIDs) {
		return run, fmt.Errorf("the behaviour answer loops: it ran to the output limit repeating itself")
	}
	if req.ToolCalls {
		return run, wellFormedToolCall(r)
	}
	if r.content() == "" {
		return run, fmt.Errorf("the behaviour answer is empty")
	}
	return run, nil
}

// reasoningContext decides the context the behaviour check runs at with a
// reasoning parser, and the note that names it. budget is the output budget
// asked for, the typical output plus reasoningTokens.
func reasoningContext(s agentapi.Shapes, room contextRoom, budget int32) (tokens int32, note string) {
	tokens = s.ContextLongest
	switch {
	case !room.known:
		tokens = min(s.ContextLongest, s.ContextTypical)
		return tokens, fmt.Sprintf("ran at a context of %d tokens, the typical context: /v1/models reports no max_model_len", tokens)
	case room.fit >= int64(s.ContextLongest):
		return tokens, fmt.Sprintf("ran at a context of %d tokens", tokens)
	case room.fit < int64(s.ContextTypical):
		tokens = min(s.ContextLongest, s.ContextTypical)
		return tokens, fmt.Sprintf("ran at a context of %d tokens, the typical context: max_model_len %d leaves %d beside %d output tokens (%d answer, %d reasoning); longest is %d",
			tokens, room.maxModelLen, max(room.fit, 0), budget, s.OutputTypical, reasoningTokens, s.ContextLongest)
	}
	tokens = int32(room.fit)
	return tokens, fmt.Sprintf("ran at a context of %d tokens (shortened from %d to leave room for %d output tokens: %d answer, %d reasoning)",
		tokens, s.ContextLongest, budget, s.OutputTypical, reasoningTokens)
}

// contextRoom is how much prompt the model's context leaves beside an output.
type contextRoom struct {
	maxModelLen int64
	// fit is the most prompt tokens, beyond the chat template's overhead, that
	// fit with the output; zero or negative when none do.
	fit int64
	// known is false when /v1/models reports no max_model_len.
	known bool
}

// contextRoom reads max_model_len and the template's overhead and works out the
// room beside an output of budget tokens.
func (c *Client) contextRoom(ctx context.Context, budget int32) (contextRoom, error) {
	cards, err := c.models(ctx)
	if err != nil {
		return contextRoom{}, err
	}
	if len(cards) == 0 || cards[0].MaxModelLen <= 0 {
		return contextRoom{}, nil
	}
	overhead, err := c.templateOverhead(ctx)
	if err != nil {
		return contextRoom{}, err
	}
	return contextRoom{maxModelLen: cards[0].MaxModelLen, fit: cards[0].MaxModelLen - int64(overhead) - int64(budget), known: true}, nil
}
