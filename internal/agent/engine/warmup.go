package engine

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// warmUpTail ends every warm-up prompt; warmUpToolTail ends the tool-call
// shape's, which asks for the call (an engine answers with a call only when
// the prompt asks for one).
const (
	warmUpTail     = "Continue the list of words above."
	warmUpToolTail = "Call the get_weather tool for the city of Krakow."
)

// warmShape is one request of a warm-up round.
type warmShape struct {
	shape   agentapi.Shape
	context int32
	output  int32
	tools   bool
}

// WarmUpRound runs warm-up round n (HBEP-7, Instance start: warm-up): one
// request at each of the three context lengths with the typical output, one
// at the typical context with the longest output, one tool call when
// req.ToolCalls, then req.TargetSessions concurrent requests of the typical
// shape. Each request streams and is timed to its first token and to its end.
// A request that fails carries its error; one cancelled by ctx carries
// "cancelled". Synthetic prompts only, each with its own first word, so no
// request is served from another's prefix cache.
//
// A shape with a fixed output length asks for min_tokens = max_tokens (and
// ignores the end-of-sequence token), because ignore_eos alone does not fix the
// length: vLLM merges the server's default stop-token ids into every request
// (chat_completion/protocol.py:730-740, to_sampling_params). The output is then
// verified against usage.completion_tokens, which counts the reasoning tokens
// too, and a request that stopped short carries the error. The tool-call shape
// has no fixed length: it ends with its call. It must not ignore the
// end-of-sequence token either: a streamed auto tool call finishes with
// "tool_calls" only when the engine stopped of its own accord, and with the
// underlying reason ("length") otherwise (chat_completion/serving.py:766-777).
//
// Every request is sampled as s says, with the warm-up seed on a sampled
// model: the same seed on every request of a round and of every round, whose
// prompts differ by their nonce.
func (c *Client) WarmUpRound(ctx context.Context, n int32, req agentapi.WarmUpRequest, s agentapi.Sampling) agentapi.WarmUpRound {
	sh := req.Shapes
	seq := []warmShape{
		{agentapi.ShapeShort, sh.ContextShort, sh.OutputTypical, false},
		{agentapi.ShapeTypical, sh.ContextTypical, sh.OutputTypical, false},
		{agentapi.ShapeLongest, sh.ContextLongest, sh.OutputTypical, false},
		{agentapi.ShapeLongOutput, sh.ContextTypical, sh.OutputLongest, false},
	}
	if req.ToolCalls {
		seq = append(seq, warmShape{agentapi.ShapeToolCall, sh.ContextTypical, sh.OutputTypical, true})
	}
	round := agentapi.WarmUpRound{Round: n}
	for _, w := range seq {
		round.Requests = append(round.Requests, c.timed(ctx, s, w))
	}
	sessions := int(req.TargetSessions)
	conc := make([]agentapi.RequestTiming, sessions)
	var wg sync.WaitGroup
	for i := range sessions {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conc[i] = c.timed(ctx, s, warmShape{agentapi.ShapeConcurrent, sh.ContextTypical, sh.OutputTypical, false})
		}()
	}
	wg.Wait()
	round.Requests = append(round.Requests, conc...)
	return round
}

// timed sends one streamed warm-up request and times it. The tool-call shape's
// answer is not judged in warm-up (science-agent ruling, 1 Nov 2026, issue
// #125; the owner approved it on #127): qualification's tool-call smoke test
// judges it, with its reason codes. Its RequestTiming carries an Error only
// for a request that failed, an HTTP error or a cut stream, like any shape's;
// a text answer, a reply that the token limit ended before any call (on a
// reasoning model, a reasoning-budget exhaustion) or an invalid call is only
// logged.
func (c *Client) timed(ctx context.Context, s agentapi.Sampling, w warmShape) agentapi.RequestTiming {
	t, answer := c.timedCall(ctx, s, w)
	if answer != nil && c.log != nil {
		c.log.Info("warm-up tool-call answer, not judged in warm-up", "shape", w.shape, "outcome", errorText(answer))
	}
	return t
}

// timedCall is timed, with the tool-call shape's answer judged apart: the
// error is the judgement of its assembled call (judgeStreamedCalls), nil for
// a valid call and for every other shape.
func (c *Client) timedCall(ctx context.Context, s agentapi.Sampling, w warmShape) (agentapi.RequestTiming, error) {
	t := agentapi.RequestTiming{Shape: w.shape}
	fail := func(err error) (agentapi.RequestTiming, error) {
		if ctx.Err() != nil {
			t.Error = "cancelled"
		} else {
			t.Error = agentapi.TruncateUTF8(errorText(err), agentapi.MaxTimingErrorBytes)
		}
		return t, nil
	}
	var tools []tool
	choice, tail := "", warmUpTail
	var minTokens int32
	ignoreEOS := false
	maxTokens := w.output
	if w.tools {
		tools, choice, tail = []tool{weatherTool}, "auto", warmUpToolTail
		// With a reasoning parser the call comes after the reasoning, so it
		// asks for the same extra reasoning budget as the tool-call smoke test
		// (science-agent ruling, 31 Oct 2026). A fixed figure in alpha, and a
		// known limit.
		if c.reasoning {
			maxTokens += reasoningTokens
		}
	}
	// A fixed-length shape asks for min_tokens = max_tokens; the controller
	// judges settling on these shapes alone (agentapi.Shape.FixedLength). The
	// tool-call shape ends with its call, at no fixed length: no min_tokens,
	// and the end-of-sequence token is not ignored.
	if w.shape.FixedLength() {
		minTokens, ignoreEOS = w.output, true
	}
	// The longest shape sits at the context limit: count its own prompt.
	content, err := c.prompt(ctx, w.context, tail, tools, w.shape == agentapi.ShapeLongest)
	if err != nil {
		return fail(err)
	}
	model, err := c.servedModel(ctx)
	if err != nil {
		return fail(err)
	}
	start := time.Now()
	resp, err := c.send(ctx, http.MethodPost, "/v1/chat/completions", chatRequest{
		Model:         model,
		Messages:      []message{{Role: "user", Content: content}},
		MaxTokens:     maxTokens,
		Stream:        true,
		StreamOptions: &streamOptions{IncludeUsage: true},
		IgnoreEOS:     ignoreEOS,
		MinTokens:     minTokens,
		Tools:         tools,
		ToolChoice:    choice,
	}.sampled(s, agentapi.SeedKeyWarmUp))
	if err != nil {
		return fail(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fail(fmt.Errorf("HTTP %d", resp.StatusCode))
	}
	rd := newSSEReader(resp.Body)
	var first time.Duration
	var sawFirst bool
	var completion int32 = -1
	var calls callAssembler
	var finish string
	for {
		ev, err := rd.next(func() {})
		if err != nil {
			return fail(fmt.Errorf("stream: %w", err))
		}
		ch, done, err := parseChunk(ev)
		if err != nil {
			return fail(err)
		}
		if done {
			break
		}
		if ch.Usage != nil {
			completion = ch.Usage.CompletionTokens
		}
		for _, choice := range ch.Choices {
			// The first delta of any kind is the first token: with a
			// reasoning parser the engine reasons before it answers.
			d := choice.Delta
			for _, tc := range d.ToolCalls {
				calls.add(tc)
			}
			if choice.FinishReason != nil {
				finish = *choice.FinishReason
			}
			if d.Content == "" && d.Reasoning == "" && len(d.ToolCalls) == 0 {
				continue
			}
			if !sawFirst {
				first, sawFirst = time.Since(start), true
			}
		}
	}
	t.Total = agentapi.Duration(time.Since(start))
	if !sawFirst {
		return fail(errors.New("the stream carried no token"))
	}
	t.TimeToFirstToken = agentapi.Duration(first)
	// The count of tokens is the engine's own, which includes the reasoning:
	// a chunk can carry several tokens, so chunks are never counted.
	if completion < 0 {
		return fail(errors.New("the stream carried no usage (stream_options.include_usage)"))
	}
	t.OutputTokens = completion
	if w.tools {
		return t, judgeStreamedCalls(calls.assembled(), finish, completion)
	}
	if !w.shape.FixedLength() {
		return t, nil
	}
	if completion != w.output {
		if completion < w.output {
			t.Error = fmt.Sprintf("stopped early: %d of %d tokens", completion, w.output)
		} else {
			t.Error = fmt.Sprintf("overran: %d of %d tokens", completion, w.output)
		}
	}
	return t, nil
}

// judgeStreamedCalls judges the tool-call shape's assembled calls: at least
// one, the stream finished with "tool_calls" (the call is complete, not cut
// off), and every call valid (validateToolCall). A reply with no call that
// the token limit ended says so: on a reasoning model it spent the budget
// reasoning, which qualification calls ReasoningBudgetExhausted (#94, round
// 3), not an answer that calls no tool.
func judgeStreamedCalls(calls []toolCall, finish string, tokens int32) error {
	if len(calls) == 0 {
		if finish == "length" {
			return fmt.Errorf("the reply used its whole budget of %d tokens with no tool call (finish_reason \"length\"); with a reasoning parser that is a reasoning-budget exhaustion", tokens)
		}
		return errors.New("the answer calls no tool")
	}
	if finish != "tool_calls" {
		return fmt.Errorf("the tool call is incomplete: finish_reason %q", finish)
	}
	for _, tc := range calls {
		if err := validateToolCall(weatherTool, tc.Type, tc.Function.Name, tc.Function.Arguments); err != nil {
			return err
		}
	}
	return nil
}
