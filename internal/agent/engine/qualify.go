package engine

import (
	"context"
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
		o := agentapi.CheckOutcome{Name: name, Passed: err == nil, Message: note}
		if err != nil {
			o.Message = err.Error()
			if note != "" {
				o.Message += "; " + note
			}
		}
		o.Message = agentapi.TruncateUTF8(o.Message, agentapi.MaxMessageBytes)
		res.Checks = append(res.Checks, o)
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
	note, err := c.behaviourCheck(ctx, req)
	add(agentapi.CheckBehaviour, note, err)
	return res
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

// behaviourCheck sends one request at the workload's longest context with the
// typical output: the answer must be well-formed (text, or a well-formed
// tool call when tool calls are on) and must not loop (HBEP-7, Instance
// start). Only the final answer is judged, never the reasoning.
//
// With a reasoning parser the request asks for the typical output plus
// reasoningTokens, and the prompt is shortened when the longest context does
// not leave that room: context = min(ContextLongest, max_model_len - template
// overhead - OutputTypical - reasoningTokens), max_model_len read from
// /v1/models. This is the allocation agent's provisional choice, pending the
// science agent. The note says so, on a pass too.
func (c *Client) behaviourCheck(ctx context.Context, req agentapi.QualifyRequest) (note string, err error) {
	tail, tools, choice := behaviourTail, []tool(nil), ""
	if req.ToolCalls {
		tail, tools, choice = behaviourToolTail, []tool{weatherTool}, "auto"
	}
	budget, tokens := req.Shapes.OutputTypical, req.Shapes.ContextLongest
	if c.reasoning {
		budget += reasoningTokens
		fit, err := c.contextFor(ctx, budget)
		if err != nil {
			return "", err
		}
		if fit < tokens {
			note = fmt.Sprintf("the prompt was shortened from %d to %d tokens to leave room for %d output tokens (%d answer, %d reasoning) within max_model_len",
				tokens, fit, budget, req.Shapes.OutputTypical, reasoningTokens)
			tokens = fit
		}
	}
	content, err := c.prompt(ctx, tokens, tail, tools, true)
	if err != nil {
		return note, err
	}
	r, err := c.chat(ctx, chatRequest{
		Messages:       []message{{Role: "user", Content: content}},
		MaxTokens:      budget,
		ReturnTokenIDs: true,
		Tools:          tools,
		ToolChoice:     choice,
	})
	if err != nil {
		return note, err
	}
	if len(r.Choices) == 0 {
		return note, fmt.Errorf("the answer has no choice")
	}
	if err := r.reasoningExhausted(budget); err != nil {
		return note, err
	}
	ch := r.Choices[0]
	if ch.FinishReason == "length" && IsLoop(ch.TokenIDs) {
		return note, fmt.Errorf("the answer at the longest context loops: it ran to the output limit repeating itself")
	}
	if req.ToolCalls {
		return note, wellFormedToolCall(r)
	}
	if r.content() == "" {
		return note, fmt.Errorf("the answer at the longest context is empty")
	}
	return note, nil
}

// contextFor is the most prompt tokens, beyond the chat template's overhead,
// that fit with an output of budget tokens in the model's context.
func (c *Client) contextFor(ctx context.Context, budget int32) (int32, error) {
	cards, err := c.models(ctx)
	if err != nil {
		return 0, err
	}
	if len(cards) == 0 || cards[0].MaxModelLen <= 0 {
		return 0, fmt.Errorf("cannot fit the reasoning budget: /v1/models reports no max_model_len")
	}
	overhead, err := c.templateOverhead(ctx)
	if err != nil {
		return 0, err
	}
	fit := cards[0].MaxModelLen - int64(overhead) - int64(budget)
	if fit <= 0 {
		return 0, fmt.Errorf("no room for a prompt: max_model_len %d, template overhead %d, output budget %d", cards[0].MaxModelLen, overhead, budget)
	}
	return int32(min(fit, 1<<30)), nil
}
