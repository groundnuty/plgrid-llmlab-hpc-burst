package main

import (
	"bufio"
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
	} `json:"choices"`
	Usage *struct {
		PromptTokens        int `json:"prompt_tokens"`
		CompletionTokens    int `json:"completion_tokens"`
		PromptTokensDetails *struct {
			CachedTokens int `json:"cached_tokens"`
		} `json:"prompt_tokens_details"`
	} `json:"usage"`
	Error any `json:"error"`
}

// readStream reads an OpenAI-style SSE body. It requires a [DONE] event; a body that ends
// without one is an error (a cut stream must not count as a measured request).
func readStream(body io.Reader, start time.Time, now func() time.Time) (Result, error) {
	res := Result{CachedTokens: -1}
	var content strings.Builder
	br := bufio.NewReaderSize(body, 64*1024)
	for {
		line, err := br.ReadString('\n')
		if len(line) > 0 {
			line = strings.TrimRight(line, "\r\n")
			if strings.HasPrefix(line, "data:") {
				data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
				if data == "[DONE]" {
					res.Latency = now().Sub(start)
					res.Content = content.String()
					return res, nil
				}
				var c chunk
				if jerr := json.Unmarshal([]byte(data), &c); jerr != nil {
					return res, fmt.Errorf("bad chunk: %v: %.200s", jerr, data)
				}
				if c.Error != nil {
					return res, fmt.Errorf("error event: %.300s", data)
				}
				for _, ch := range c.Choices {
					if ch.Delta.Content != "" {
						if res.TTFT == 0 {
							res.TTFT = now().Sub(start)
						}
						content.WriteString(ch.Delta.Content)
					}
				}
				if c.Usage != nil {
					res.PromptTokens = c.Usage.PromptTokens
					res.CompletionTokens = c.Usage.CompletionTokens
					if c.Usage.PromptTokensDetails != nil {
						res.CachedTokens = c.Usage.PromptTokensDetails.CachedTokens
					}
				}
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return res, errors.New("stream ended without [DONE]")
			}
			return res, err
		}
	}
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
	res, err := readStream(resp.Body, start, time.Now)
	res.Status = resp.StatusCode
	return res, err
}
