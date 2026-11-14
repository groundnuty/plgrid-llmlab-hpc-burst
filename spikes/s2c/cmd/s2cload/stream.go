package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type streamOptions struct {
	IncludeUsage bool `json:"include_usage"`
}

// chatRequest puts messages first; the remaining fields carry no session tag.
type chatRequest struct {
	Model         string        `json:"model"`
	Messages      []message     `json:"messages"`
	MaxTokens     int           `json:"max_tokens"`
	MinTokens     int           `json:"min_tokens,omitempty"`
	IgnoreEOS     bool          `json:"ignore_eos,omitempty"`
	Temperature   float64       `json:"temperature"`
	Stream        bool          `json:"stream"`
	StreamOptions streamOptions `json:"stream_options"`
}

// Result is what one streamed request produced, as seen by the client.
type Result struct {
	Content          string
	TTFT             time.Duration // send to first non-empty content delta; 0 if none arrived
	Latency          time.Duration // send to [DONE]
	PromptTokens     int
	CachedTokens     int // -1 if the server reported no prompt_tokens_details
	CompletionTokens int
	Status           int
}

type chunk struct {
	Choices []struct {
		Delta struct {
			Content string `json:"content"`
		} `json:"delta"`
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
	Usage *struct {
		PromptTokens        int `json:"prompt_tokens"`
		CompletionTokens    int `json:"completion_tokens"`
		PromptTokensDetails *struct {
			CachedTokens int `json:"cached_tokens"`
		} `json:"prompt_tokens_details"`
	} `json:"usage"`
	Error json.RawMessage `json:"error"`
}

// readStream reads an OpenAI-style SSE body and returns a result only for a stream that is complete and
// whose counts can be trusted. A request counts as a measured success only if all of these hold:
//   - every event, [DONE] included, ended with its blank line (a cut stream is an error);
//   - no event is an error event or carries an error payload, and every payload is valid JSON;
//   - some content arrived, and a choice carried a non-empty finish_reason;
//   - a usage record arrived with prompt_tokens and completion_tokens above zero, and, when wantTokens > 0,
//     completion_tokens equal to it (the request set min_tokens = max_tokens, so any other count is a short or
//     overlong reply). cached_tokens stays optional: without prompt_tokens_details it is -1.
//
// On a stream that ended with [DONE] but failed the count checks, the Result is returned with the error, so the
// caller can record what was seen.
func readStream(body io.Reader, start time.Time, now func() time.Time, wantTokens int) (Result, error) {
	res := Result{CachedTokens: -1}
	var content strings.Builder
	finish, usageSeen := "", false
	sr := newSSEReader(body)
	for {
		ev, err := sr.next(func() {})
		if err != nil {
			if errors.Is(err, io.EOF) {
				return res, errors.New("stream ended without [DONE]")
			}
			return res, err
		}
		if ev.Event == "error" {
			return res, fmt.Errorf("error event: %.300s", ev.Data)
		}
		if strings.TrimSpace(ev.Data) == "[DONE]" {
			res.Latency = now().Sub(start)
			res.Content = content.String()
			return res, checkComplete(res, finish, usageSeen, wantTokens)
		}
		var c chunk
		if jerr := json.Unmarshal([]byte(ev.Data), &c); jerr != nil {
			return res, fmt.Errorf("malformed event payload (%v): %.200s", jerr, ev.Data)
		}
		if e := strings.TrimSpace(string(c.Error)); e != "" && e != "null" {
			return res, fmt.Errorf("error payload: %.300s", e)
		}
		for _, ch := range c.Choices {
			if ch.Delta.Content != "" {
				if res.TTFT == 0 {
					res.TTFT = now().Sub(start)
				}
				content.WriteString(ch.Delta.Content)
			}
			if ch.FinishReason != nil && *ch.FinishReason != "" {
				finish = *ch.FinishReason
			}
		}
		if c.Usage != nil {
			usageSeen = true
			res.PromptTokens = c.Usage.PromptTokens
			res.CompletionTokens = c.Usage.CompletionTokens
			if c.Usage.PromptTokensDetails != nil {
				res.CachedTokens = c.Usage.PromptTokensDetails.CachedTokens
			}
		}
	}
}

// checkComplete applies the checks that need the whole stream; see readStream.
func checkComplete(res Result, finish string, usageSeen bool, wantTokens int) error {
	switch {
	case res.Content == "":
		return errors.New("no content before [DONE]")
	case finish == "":
		return errors.New("no finish_reason before [DONE]")
	case !usageSeen:
		return errors.New("no usage record before [DONE]")
	case res.PromptTokens <= 0 || res.CompletionTokens <= 0:
		return fmt.Errorf("invalid usage counts: prompt_tokens %d, completion_tokens %d", res.PromptTokens, res.CompletionTokens)
	case wantTokens > 0 && res.CompletionTokens != wantTokens:
		return fmt.Errorf("completion_tokens %d, want %d", res.CompletionTokens, wantTokens)
	}
	return nil
}

// chat sends one streamed chat completion and reads it to the end.
func chat(ctx context.Context, client *http.Client, url, header, session string, req chatRequest) (Result, error) {
	b, err := json.Marshal(req)
	if err != nil {
		return Result{}, err
	}
	hreq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(b))
	if err != nil {
		return Result{}, err
	}
	hreq.Header.Set("Content-Type", "application/json")
	if header != "" {
		hreq.Header.Set(header, session)
	}
	start := time.Now()
	resp, err := client.Do(hreq)
	if err != nil {
		return Result{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 500))
		return Result{Status: resp.StatusCode}, fmt.Errorf("http %d: %s", resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	res, err := readStream(resp.Body, start, time.Now, req.MinTokens)
	res.Status = resp.StatusCode
	return res, err
}
