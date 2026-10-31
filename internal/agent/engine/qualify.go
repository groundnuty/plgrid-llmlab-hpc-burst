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
		message, reason := err.Error(), reasonOf(err)
		var re *reasonedError
		if errors.As(err, &re) {
			message = re.message
		}
		if reason == "" {
			// A failure the checks do not name is the request's: a
			// connection error, a timeout, an error status or a failed
			// tokenize.
			reason = agentapi.ReasonRequestFailed
		}
		if note != "" {
			message += "; " + note
		}
		res.Checks = append(res.Checks, failedOutcome(name, reason, message))
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
	res.Checks[len(res.Checks)-1].ContextTokens = run.contextTokens
	return res
}

// passedOutcome is the outcome of a check that passed, with the note the
// controller should read beside it, cut to agentapi.MaxMessageBytes.
func passedOutcome(name agentapi.CheckName, note string) agentapi.CheckOutcome {
	return agentapi.CheckOutcome{Name: name, Passed: true, Message: agentapi.TruncateUTF8(note, agentapi.MaxMessageBytes)}
}

// failedOutcome is the one place that builds the outcome of a failed check:
// its reason code, one of agentapi's Reason* constants, which the controller
// matches on, and its message, human text cut to MaxMessageBytes.
func failedOutcome(name agentapi.CheckName, reason, message string) agentapi.CheckOutcome {
	return agentapi.CheckOutcome{Name: name, Reason: reason, Message: agentapi.TruncateUTF8(message, agentapi.MaxMessageBytes)}
}

// chatCheck sends one chat request; it passes when the engine answers with
// text, in either field: a reasoning model may spend all 32 tokens thinking.
func (c *Client) chatCheck(ctx context.Context) (string, error) {
	r, err := c.chat(ctx, chatRequest{Messages: []message{{Role: "user", Content: chatPrompt}}, MaxTokens: 32})
	if err != nil {
		return "", err
	}
	if r.content() == "" && r.reasoning() == "" {
		return r.SystemFingerprint, reasoned(agentapi.ReasonEmptyAnswer, "the chat answer is empty")
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
		return reasoned(agentapi.ReasonNoToolCall, "the answer calls no tool")
	}
	for _, tc := range r.Choices[0].Message.ToolCalls {
		if err := validateToolCall(weatherTool, tc.Type, tc.Function.Name, tc.Function.Arguments); err != nil {
			return err
		}
	}
	return nil
}

// corruptionCheck sends one greedy request three times in a row and judges
// the token outputs by agentapi.JudgeCorruption: they share their first 32
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
			return reasoned(agentapi.ReasonBadResponse, "the answer has no choice")
		}
		if r.Choices[0].TokenIDs == nil {
			return reasoned(agentapi.ReasonBadResponse, "the engine returned no token ids (return_token_ids)")
		}
		outs = append(outs, r.Choices[0].TokenIDs)
	}
	if o := agentapi.JudgeCorruption(outs); !o.Passed {
		return reasoned(o.Reason, "%s", o.Message)
	}
	return nil
}

// behaviourRun is what the behaviour check reports besides its verdict.
type behaviourRun struct {
	// contextTokens is the context the prompt ran at, in the unit of
	// Shapes.ContextLongest: tokens beyond the chat template's overhead
	// (agentapi.BehaviourContextTokens), reported as
	// CheckOutcome.ContextTokens.
	contextTokens int64
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
	budget := req.Shapes.OutputTypical
	run.contextTokens = int64(req.Shapes.ContextLongest)
	if c.reasoning {
		budget += reasoningTokens
		maxLen, overhead, err := c.contextLimit(ctx)
		if err != nil {
			return run, err
		}
		run.contextTokens = agentapi.BehaviourContextTokens(req.Shapes, maxLen, overhead, true)
		run.note = reasoningNote(req.Shapes, run.contextTokens, maxLen)
	}
	content, err := c.prompt(ctx, int32(run.contextTokens), tail, tools, true)
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
		return run, reasoned(agentapi.ReasonBadResponse, "the answer has no choice")
	}
	if err := r.reasoningExhausted(budget); err != nil {
		return run, err
	}
	ch := r.Choices[0]
	if ch.FinishReason == "length" && IsLoop(ch.TokenIDs) {
		return run, reasoned(agentapi.ReasonLoop, "the behaviour answer loops: it ran to the output limit repeating itself")
	}
	if req.ToolCalls {
		return run, wellFormedToolCall(r)
	}
	if r.content() == "" {
		return run, reasoned(agentapi.ReasonEmptyAnswer, "the behaviour answer is empty")
	}
	return run, nil
}

// reasoningNote names the context the behaviour check ran at with a
// reasoning parser, and why it is not the longest.
func reasoningNote(s agentapi.Shapes, tokens, maxModelLen int64) string {
	switch {
	case maxModelLen <= 0:
		return fmt.Sprintf("ran at a context of %d tokens, the typical context: /v1/models reports no max_model_len", tokens)
	case tokens == int64(s.ContextLongest):
		return fmt.Sprintf("ran at a context of %d tokens", tokens)
	case tokens == int64(s.ContextTypical):
		return fmt.Sprintf("ran at a context of %d tokens, the typical context: max_model_len %d leaves no more beside %d answer and %d reasoning tokens", tokens, maxModelLen, s.OutputTypical, reasoningTokens)
	}
	return fmt.Sprintf("ran at a context of %d tokens, shortened from %d to leave room for %d answer and %d reasoning tokens", tokens, s.ContextLongest, s.OutputTypical, reasoningTokens)
}

// contextLimit reads the max_model_len the engine reports (0 when it reports
// none) and the chat template's overhead.
func (c *Client) contextLimit(ctx context.Context) (int64, int32, error) {
	cards, err := c.models(ctx)
	if err != nil {
		return 0, 0, err
	}
	overhead, err := c.templateOverhead(ctx)
	if err != nil {
		return 0, 0, err
	}
	if len(cards) == 0 {
		return 0, overhead, nil
	}
	return cards[0].MaxModelLen, overhead, nil
}
