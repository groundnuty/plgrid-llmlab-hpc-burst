package front

import (
	"bytes"
	"encoding/json"
	"strings"
)

// endpoint is a path the front judges (HBEP-5, Contract, Endpoints).
type endpoint int

const (
	endpointOther endpoint = iota
	endpointChat
	endpointCompletions
	endpointResponses
	endpointEmbeddings
)

func endpointOf(path string) endpoint {
	switch strings.TrimSuffix(path, "/") {
	case "/v1/chat/completions":
		return endpointChat
	case "/v1/completions":
		return endpointCompletions
	case "/v1/responses":
		return endpointResponses
	case "/v1/embeddings":
		return endpointEmbeddings
	}
	return endpointOther
}

// streams reports whether the front parses this endpoint's streams.
func (e endpoint) streams() bool {
	return e == endpointChat || e == endpointCompletions || e == endpointResponses
}

// counts are a response's token counts, from its usage member.
type counts struct {
	prompt, cached, completion *int64
}

// judge reads a stream's events only to see the end its endpoint's rule
// names, the first content event and the usage (HBEP-5, Request path step 8).
type judge struct {
	kind          endpoint
	sawFinish     bool
	finish        string
	complete      bool
	usage         *counts
	contentEvents int64
	sawContent    bool // set by the latest event
	// failed: the stream ended with response.failed, an engine error after
	// the stream started; errorCode is the event's error.code, if any.
	failed    bool
	errorCode string
}

// event judges one SSE event (its raw lines) and says whether to forward it.
// done means the stream reached its end: complete, or a [DONE] without a
// finish reason, which is withheld so the front never sends [DONE] for a
// stream it did not see finish.
func (j *judge) event(raw []byte) (forward, done bool) {
	j.sawContent = false
	name, data := parseEvent(raw)
	if j.kind == endpointResponses {
		return true, j.responsesEvent(name, data)
	}
	if bytes.Equal(bytes.TrimSpace(data), []byte("[DONE]")) {
		if j.sawFinish {
			j.complete = true
			return true, true
		}
		return false, true
	}
	var ev struct {
		Choices []struct {
			FinishReason *string `json:"finish_reason"`
			Text         string  `json:"text"`
			Delta        struct {
				Content *string `json:"content"`
				// vLLM v0.32.0 sends delta.reasoning; older engines send
				// reasoning_content. Both are read (@hpc-burst-science-agent,
				// 31 Oct 2026).
				Reasoning        *string         `json:"reasoning"`
				ReasoningContent *string         `json:"reasoning_content"`
				ToolCalls        json.RawMessage `json:"tool_calls"`
			} `json:"delta"`
		} `json:"choices"`
		Usage json.RawMessage `json:"usage"`
	}
	if json.Unmarshal(data, &ev) != nil {
		return true, false
	}
	for _, c := range ev.Choices {
		if c.FinishReason != nil && *c.FinishReason != "" {
			j.sawFinish, j.finish = true, *c.FinishReason
		}
		d := c.Delta
		if c.Text != "" || (d.Content != nil && *d.Content != "") || (d.Reasoning != nil && *d.Reasoning != "") ||
			(d.ReasoningContent != nil && *d.ReasoningContent != "") ||
			(len(d.ToolCalls) > 0 && string(d.ToolCalls) != "null") {
			j.sawContent = true
		}
	}
	if j.sawContent {
		j.contentEvents++
	}
	if u := chatUsage(ev.Usage); u != nil {
		j.usage = u
	}
	return true, false
}

// responsesEvent judges a Responses stream. It is complete at its terminal
// event, which carries the final response with its usage and status, and
// which passes through unchanged (@hpc-burst-science-agent, 31 Oct 2026):
//   - response.completed, which vLLM sends last (HBEP-5, Contract);
//   - response.incomplete, a capped reply as OpenAI's contract, and LLMLab,
//     end it: a clean end, not a cut, not resent, no pause count;
//   - response.failed, an engine error after the stream started: not a cut
//     and not resent, since bytes went out, but the record marks it failed
//     with the event's error.code, and it counts toward the Instance's pause
//     like a 5xx.
func (j *judge) responsesEvent(name string, data []byte) bool {
	var ev struct {
		Type     string `json:"type"`
		Delta    any    `json:"delta"`
		Response struct {
			Status string          `json:"status"`
			Usage  json.RawMessage `json:"usage"`
			Error  *struct {
				Code string `json:"code"`
			} `json:"error"`
		} `json:"response"`
		Error *struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	_ = json.Unmarshal(data, &ev)
	typ := ev.Type
	if typ == "" {
		typ = name
	}
	if strings.HasSuffix(typ, ".delta") {
		j.sawContent = true
		j.contentEvents++
	}
	if typ == "response.completed" || typ == "response.incomplete" || typ == "response.failed" {
		j.complete, j.finish = true, ev.Response.Status
		if typ == "response.failed" {
			j.failed = true
			if j.finish == "" {
				j.finish = "failed"
			}
			switch {
			case ev.Response.Error != nil && ev.Response.Error.Code != "":
				j.errorCode = ev.Response.Error.Code
			case ev.Error != nil:
				j.errorCode = ev.Error.Code
			}
		}
		if u := responsesUsage(ev.Response.Usage); u != nil {
			j.usage = u
		}
		return true
	}
	return false
}

// parseEvent returns an SSE event's name and its data lines joined by "\n".
func parseEvent(raw []byte) (string, []byte) {
	var name string
	var data [][]byte
	for _, line := range bytes.Split(raw, []byte("\n")) {
		line = bytes.TrimSuffix(line, []byte("\r"))
		switch {
		case bytes.HasPrefix(line, []byte("data:")):
			data = append(data, bytes.TrimPrefix(bytes.TrimPrefix(line, []byte("data:")), []byte(" ")))
		case bytes.HasPrefix(line, []byte("event:")):
			name = strings.TrimSpace(string(bytes.TrimPrefix(line, []byte("event:"))))
		}
	}
	return name, bytes.Join(data, []byte("\n"))
}

// chatUsage reads chat and completions usage: prompt_tokens,
// completion_tokens and prompt_tokens_details.cached_tokens.
func chatUsage(raw json.RawMessage) *counts {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var u struct {
		Prompt     *int64 `json:"prompt_tokens"`
		Completion *int64 `json:"completion_tokens"`
		Details    *struct {
			Cached *int64 `json:"cached_tokens"`
		} `json:"prompt_tokens_details"`
	}
	if json.Unmarshal(raw, &u) != nil {
		return nil
	}
	c := &counts{prompt: u.Prompt, completion: u.Completion}
	if u.Details != nil {
		c.cached = u.Details.Cached
	}
	return c
}

// responsesUsage reads Responses usage: input_tokens, output_tokens and
// input_tokens_details.cached_tokens.
func responsesUsage(raw json.RawMessage) *counts {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var u struct {
		Input   *int64 `json:"input_tokens"`
		Output  *int64 `json:"output_tokens"`
		Details *struct {
			Cached *int64 `json:"cached_tokens"`
		} `json:"input_tokens_details"`
	}
	if json.Unmarshal(raw, &u) != nil {
		return nil
	}
	c := &counts{prompt: u.Input, completion: u.Output}
	if u.Details != nil {
		c.cached = u.Details.Cached
	}
	return c
}

// wholeResult reads a non-streamed body's finish and usage.
func wholeResult(kind endpoint, body []byte) (finish *string, u *counts) {
	var v struct {
		Choices []struct {
			FinishReason *string `json:"finish_reason"`
		} `json:"choices"`
		Status string          `json:"status"`
		Usage  json.RawMessage `json:"usage"`
	}
	if json.Unmarshal(body, &v) != nil {
		return nil, nil
	}
	switch kind {
	case endpointChat, endpointCompletions:
		if len(v.Choices) > 0 && v.Choices[0].FinishReason != nil {
			finish = v.Choices[0].FinishReason
		}
		return finish, chatUsage(v.Usage)
	case endpointResponses:
		if v.Status != "" {
			finish = &v.Status
		}
		return finish, responsesUsage(v.Usage)
	case endpointEmbeddings:
		return nil, chatUsage(v.Usage)
	}
	return nil, nil
}
