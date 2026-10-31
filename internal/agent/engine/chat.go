package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
)

// message is one chat message.
type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// tool is one tool offered to the model.
type tool struct {
	Type     string       `json:"type"`
	Function toolFunction `json:"function"`
}

type toolFunction struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
}

// weatherTool is the tool of the tool-call checks.
var weatherTool = tool{
	Type: "function",
	Function: toolFunction{
		Name:        "get_weather",
		Description: "Get the current weather in a city.",
		Parameters:  json.RawMessage(`{"type":"object","properties":{"city":{"type":"string","description":"The city's name."}},"required":["city"]}`),
	},
}

// chatRequest is the part of vLLM's chat completion request the agent sends.
type chatRequest struct {
	Model         string         `json:"model"`
	Messages      []message      `json:"messages"`
	MaxTokens     int32          `json:"max_tokens"`
	Temperature   float64        `json:"temperature"`
	Stream        bool           `json:"stream,omitempty"`
	StreamOptions *streamOptions `json:"stream_options,omitempty"`
	// IgnoreEOS, MinTokens and ReturnTokenIDs are vLLM's extensions.
	IgnoreEOS      bool   `json:"ignore_eos,omitempty"`
	MinTokens      int32  `json:"min_tokens,omitempty"`
	ReturnTokenIDs bool   `json:"return_token_ids,omitempty"`
	Tools          []tool `json:"tools,omitempty"`
	ToolChoice     string `json:"tool_choice,omitempty"`
}

type streamOptions struct {
	IncludeUsage bool `json:"include_usage"`
}

type usage struct {
	PromptTokens     int32 `json:"prompt_tokens"`
	CompletionTokens int32 `json:"completion_tokens"`
}

type toolCall struct {
	Type     string `json:"type"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

// chatResponse is the part of a non-streamed answer the agent reads.
type chatResponse struct {
	Choices []struct {
		Message struct {
			Content   *string    `json:"content"`
			Reasoning *string    `json:"reasoning"`
			ToolCalls []toolCall `json:"tool_calls"`
		} `json:"message"`
		FinishReason string  `json:"finish_reason"`
		TokenIDs     []int32 `json:"token_ids"`
	} `json:"choices"`
	Usage             usage  `json:"usage"`
	SystemFingerprint string `json:"system_fingerprint"`
}

// chat sends a non-streamed chat request.
func (c *Client) chat(ctx context.Context, req chatRequest) (chatResponse, error) {
	var out chatResponse
	if req.Model == "" {
		m, err := c.servedModel(ctx)
		if err != nil {
			return out, err
		}
		req.Model = m
	}
	err := c.postJSON(ctx, "/v1/chat/completions", req, &out)
	return out, err
}

// content returns the first choice's text, "" when there is none.
func (r chatResponse) content() string {
	if len(r.Choices) == 0 || r.Choices[0].Message.Content == nil {
		return ""
	}
	return *r.Choices[0].Message.Content
}

// reasoning returns the first choice's reasoning text, "" when there is none.
func (r chatResponse) reasoning() string {
	if len(r.Choices) == 0 || r.Choices[0].Message.Reasoning == nil {
		return ""
	}
	return *r.Choices[0].Message.Reasoning
}

// reasoningTokens is the reasoning budget the tool-call smoke test and the
// behaviour check add to their own answer budget when the launch declares a
// reasoning parser (WithReasoning). Science agent's ruling of 31 Oct 2026.
//
// Known limit: the figure is fixed in alpha. A model that reasons for longer
// than this before its final answer fails both checks with
// ReasoningBudgetExhausted, however healthy it is.
const reasoningTokens = 4096

// reasonReasoningBudgetExhausted is the reason code of a check whose reply
// spent its whole output budget on reasoning and gave no final answer.
const reasonReasoningBudgetExhausted = "ReasoningBudgetExhausted"

// reasonedError is a failure that has a reason code (a CamelCase word the
// controller tells apart), besides its message. failedOutcome turns it into the
// outcome's reason and message.
type reasonedError struct{ reason, message string }

func (e *reasonedError) Error() string { return e.reason + ": " + e.message }

// reasonOf returns the reason code of err, "" when it has none.
func reasonOf(err error) string {
	var re *reasonedError
	if errors.As(err, &re) {
		return re.reason
	}
	return ""
}

// reasoningExhausted returns the ReasoningBudgetExhausted failure when the
// reply holds reasoning but no final answer (no content and no tool call) and
// stopped at the output limit; maxTokens is the limit that was asked for. The
// final answer is content or tool_calls: the reasoning text is never judged.
func (r chatResponse) reasoningExhausted(maxTokens int32) error {
	if len(r.Choices) == 0 {
		return nil
	}
	ch := r.Choices[0]
	if r.reasoning() != "" && r.content() == "" && len(ch.Message.ToolCalls) == 0 && ch.FinishReason == "length" {
		return &reasonedError{reasonReasoningBudgetExhausted, fmt.Sprintf("the reply spent its %d output tokens on reasoning and gave no final answer", maxTokens)}
	}
	return nil
}

// tokenizeRequest is vLLM's chat form of /tokenize.
type tokenizeRequest struct {
	Model               string    `json:"model"`
	Messages            []message `json:"messages"`
	AddGenerationPrompt bool      `json:"add_generation_prompt"`
	Tools               []tool    `json:"tools,omitempty"`
}

// countTokens returns the token count of a conversation rendered through the
// engine's chat template, generation prompt included.
func (c *Client) countTokens(ctx context.Context, msgs []message, tools []tool) (int32, error) {
	m, err := c.servedModel(ctx)
	if err != nil {
		return 0, err
	}
	var out struct {
		Count int32 `json:"count"`
	}
	err = c.postJSON(ctx, "/tokenize", tokenizeRequest{Model: m, Messages: msgs, AddGenerationPrompt: true, Tools: tools}, &out)
	return out.Count, err
}

// templateOverhead is the chat template's overhead in tokens: one user message
// with no content, plus the generation prompt (HBEP-7, What the controller
// reads).
func (c *Client) templateOverhead(ctx context.Context) (int32, error) {
	c.mu.Lock()
	if c.haveOverhead {
		o := c.overhead
		c.mu.Unlock()
		return o, nil
	}
	c.mu.Unlock()
	n, err := c.countTokens(ctx, []message{{Role: "user", Content: ""}}, nil)
	if err != nil {
		return 0, err
	}
	c.mu.Lock()
	c.overhead, c.haveOverhead = n, true
	c.mu.Unlock()
	return n, nil
}

// getVersion reads GET /version.
func (c *Client) getVersion(ctx context.Context) (string, error) {
	var out struct {
		Version string `json:"version"`
	}
	b, err := c.do(ctx, http.MethodGet, "/version", nil)
	if err != nil {
		return "", err
	}
	if err := json.Unmarshal(b, &out); err != nil {
		return "", err
	}
	return out.Version, nil
}
