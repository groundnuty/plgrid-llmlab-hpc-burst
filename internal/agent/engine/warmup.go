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

// warmUpTail ends every warm-up prompt.
const warmUpTail = "Continue the list of words above."

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
// shape. Each request streams, ignores the end-of-sequence token so it
// produces exactly its output length, and is timed to its first token and to
// its end. A request that fails carries its error; one cancelled by ctx carries
// "cancelled". Synthetic prompts only, each with its own first word, so no
// request is served from another's prefix cache.
func (c *Client) WarmUpRound(ctx context.Context, n int32, req agentapi.WarmUpRequest) agentapi.WarmUpRound {
	s := req.Shapes
	seq := []warmShape{
		{agentapi.ShapeShort, s.ContextShort, s.OutputTypical, false},
		{agentapi.ShapeTypical, s.ContextTypical, s.OutputTypical, false},
		{agentapi.ShapeLongest, s.ContextLongest, s.OutputTypical, false},
		{agentapi.ShapeLongOutput, s.ContextTypical, s.OutputLongest, false},
	}
	if req.ToolCalls {
		seq = append(seq, warmShape{agentapi.ShapeToolCall, s.ContextTypical, s.OutputTypical, true})
	}
	round := agentapi.WarmUpRound{Round: n}
	for _, w := range seq {
		round.Requests = append(round.Requests, c.timed(ctx, w))
	}
	sessions := int(req.TargetSessions)
	conc := make([]agentapi.RequestTiming, sessions)
	var wg sync.WaitGroup
	for i := range sessions {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conc[i] = c.timed(ctx, warmShape{agentapi.ShapeConcurrent, s.ContextTypical, s.OutputTypical, false})
		}()
	}
	wg.Wait()
	round.Requests = append(round.Requests, conc...)
	return round
}

// timed sends one streamed warm-up request and times it.
func (c *Client) timed(ctx context.Context, w warmShape) agentapi.RequestTiming {
	t := agentapi.RequestTiming{Shape: w.shape}
	fail := func(err error) agentapi.RequestTiming {
		if ctx.Err() != nil {
			t.Error = "cancelled"
		} else {
			t.Error = agentapi.TruncateUTF8(err.Error(), agentapi.MaxTimingErrorBytes)
		}
		return t
	}
	var tools []tool
	choice := ""
	if w.tools {
		tools, choice = []tool{weatherTool}, "auto"
	}
	// The longest shape sits at the context limit: count its own prompt.
	content, err := c.prompt(ctx, w.context, warmUpTail, tools, w.shape == agentapi.ShapeLongest)
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
		MaxTokens:     w.output,
		Stream:        true,
		StreamOptions: &streamOptions{IncludeUsage: true},
		IgnoreEOS:     true,
		Tools:         tools,
		ToolChoice:    choice,
	})
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
	return t
}
