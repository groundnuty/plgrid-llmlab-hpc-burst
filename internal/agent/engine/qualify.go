package engine

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The qualification requests' fixed texts. Synthetic, as every request the
// agent sends is.
const (
	chatPrompt       = "Say hello in one short sentence."
	toolPrompt       = "What is the weather in Krakow right now? Call the get_weather tool to find out."
	corruptionPrompt = "Count from one to one hundred in English words, separated by commas."
	behaviourTail    = "Ignore the list of words above. In one short sentence, say what colour the sky is on a clear day."
	behaviourToolTail = "Ignore the list of words above. Call the get_weather tool for the city of Krakow."
	corruptionTokens = 2 * agentapi.CorruptionPrefix
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
	add := func(name agentapi.CheckName, err error) {
		o := agentapi.CheckOutcome{Name: name, Passed: err == nil}
		if err != nil {
			o.Message = agentapi.TruncateUTF8(err.Error(), agentapi.MaxMessageBytes)
		}
		res.Checks = append(res.Checks, o)
	}
	add(agentapi.CheckHealth, c.Probe(ctx))
	if cards, err := c.models(ctx); err == nil {
		for _, m := range cards {
			res.ServedModelNames = append(res.ServedModelNames, m.ID)
		}
	}
	if m, err := c.metrics(ctx); err == nil {
		res.KVCapacityTokens = kvCapacity(m.cacheConfig())
	}
	fp, err := c.chatCheck(ctx)
	res.Fingerprint = fp
	add(agentapi.CheckChat, err)
	if req.ToolCalls {
		add(agentapi.CheckToolCall, c.toolCallCheck(ctx))
	}
	add(agentapi.CheckCorruption, c.corruptionCheck(ctx))
	add(agentapi.CheckBehaviour, c.behaviourCheck(ctx, req))
	return res
}

// chatCheck sends one chat request; it passes when the engine answers with
// text.
func (c *Client) chatCheck(ctx context.Context) (string, error) {
	r, err := c.chat(ctx, chatRequest{Messages: []message{{Role: "user", Content: chatPrompt}}, MaxTokens: 32})
	if err != nil {
		return "", err
	}
	if r.content() == "" {
		return r.SystemFingerprint, fmt.Errorf("the chat answer is empty")
	}
	return r.SystemFingerprint, nil
}

// toolCallCheck is the tool-call smoke test: offered the weather tool and
// asked to use it, the engine answers with a well-formed call of it.
func (c *Client) toolCallCheck(ctx context.Context) error {
	r, err := c.chat(ctx, chatRequest{
		Messages:   []message{{Role: "user", Content: toolPrompt}},
		MaxTokens:  128,
		Tools:      []tool{weatherTool},
		ToolChoice: "auto",
	})
	if err != nil {
		return err
	}
	return wellFormedToolCall(r)
}

// wellFormedToolCall checks that the answer calls the weather tool with a JSON
// object of arguments.
func wellFormedToolCall(r chatResponse) error {
	if len(r.Choices) == 0 || len(r.Choices[0].Message.ToolCalls) == 0 {
		return fmt.Errorf("the answer calls no tool")
	}
	for _, tc := range r.Choices[0].Message.ToolCalls {
		if tc.Function.Name != weatherTool.Function.Name {
			return fmt.Errorf("the answer calls %q, not %q", tc.Function.Name, weatherTool.Function.Name)
		}
		var args map[string]any
		if err := json.Unmarshal([]byte(tc.Function.Arguments), &args); err != nil {
			return fmt.Errorf("the tool call's arguments are not a JSON object: %v", err)
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
// tool call when tool calls are on) and must not loop (HBEP-7, Instance start).
func (c *Client) behaviourCheck(ctx context.Context, req agentapi.QualifyRequest) error {
	tail, tools, choice := behaviourTail, []tool(nil), ""
	if req.ToolCalls {
		tail, tools, choice = behaviourToolTail, []tool{weatherTool}, "auto"
	}
	content, err := c.prompt(ctx, req.Shapes.ContextLongest, tail, tools, true)
	if err != nil {
		return err
	}
	r, err := c.chat(ctx, chatRequest{
		Messages:       []message{{Role: "user", Content: content}},
		MaxTokens:      req.Shapes.OutputTypical,
		ReturnTokenIDs: true,
		Tools:          tools,
		ToolChoice:     choice,
	})
	if err != nil {
		return err
	}
	if len(r.Choices) == 0 {
		return fmt.Errorf("the answer has no choice")
	}
	ch := r.Choices[0]
	if ch.FinishReason == "length" && IsLoop(ch.TokenIDs) {
		return fmt.Errorf("the answer at the longest context loops: it ran to the output limit repeating itself")
	}
	if req.ToolCalls {
		return wellFormedToolCall(r)
	}
	if r.content() == "" {
		return fmt.Errorf("the answer at the longest context is empty")
	}
	return nil
}
