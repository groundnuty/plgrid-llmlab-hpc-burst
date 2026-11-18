package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatBody struct {
	Model         string        `json:"model"`
	Messages      []chatMessage `json:"messages"`
	Stream        bool          `json:"stream"`
	StreamOptions struct {
		IncludeUsage bool `json:"include_usage"`
	} `json:"stream_options"`
	MaxTokens int  `json:"max_tokens"`
	MinTokens *int `json:"min_tokens,omitempty"`
}

// chatClient sends one streamed chat request and reads its events.
type chatClient struct {
	http          *http.Client
	url           string // BaseURL + "/chat/completions"
	model         string
	key           string // never printed or recorded
	sessionHeader string
	omitMinTokens bool
	timeout       time.Duration
	now           func() time.Time // time.Now; a test scripts it to check the timing arithmetic exactly
}

func newChatClient(cfg Config, key string) *chatClient {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConns, tr.MaxIdleConnsPerHost = 0, 1024
	tr.DisableCompression = true // a compressed stream would be delivered in blocks, which would blur the time to the first token
	return &chatClient{
		// A redirect is never followed: Go keeps Authorization on a redirect to another port of the same host, which would send the key
		// to, and replay the study request against, another service. The 3xx is the request's status and its class "status 3xx".
		http:          &http.Client{Transport: tr, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
		url:           strings.TrimRight(cfg.BaseURL, "/") + "/chat/completions",
		model:         cfg.Model,
		key:           key,
		sessionHeader: cfg.SessionHeader,
		omitMinTokens: cfg.OmitMinTokens,
		timeout:       time.Duration(cfg.RequestTimeout),
		now:           time.Now,
	}
}

// event is the part of a chat-completion chunk the generator reads.
type event struct {
	Choices []struct {
		Delta struct {
			Content          *string `json:"content"`
			Reasoning        *string `json:"reasoning"`
			ReasoningContent *string `json:"reasoning_content"`
		} `json:"delta"`
	} `json:"choices"`
	Usage *struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
	} `json:"usage"`
	Error json.RawMessage `json:"error"`
}

func (e event) text() (content string, any bool) {
	for _, c := range e.Choices {
		d := c.Delta
		if d.Content != nil && *d.Content != "" {
			content += *d.Content
			any = true
		}
		for _, r := range []*string{d.Reasoning, d.ReasoningContent} {
			if r != nil && *r != "" {
				any = true
			}
		}
	}
	return content, any
}

// redacted replaces the API key wherever a record would show it.
const redacted = "(redacted)"

// redact returns rec with every occurrence of the API key in any of its text fields replaced by redacted. An endpoint can echo the key it
// was sent (in an attribution header, say, on any status), and a record is printed and written to a file. With no key it changes nothing
// (replacing the empty string would insert the marker between every character).
func (c *chatClient) redact(rec Record) Record {
	if c.key == "" {
		return rec
	}
	for _, p := range []*string{&rec.Workload, &rec.Session, &rec.RequestID, &rec.Emitter, &rec.Provider, &rec.DeploymentID, &rec.Instance, &rec.Error} {
		*p = strings.ReplaceAll(*p, c.key, redacted)
	}
	return rec
}

// chat sends one request of a session's conversation and returns the assistant's reply text (the content deltas joined) and the record.
// A failed request has a non-empty Error in the record, and the reply is not to be used. Every record leaves through here, so the key is
// redacted in this one place.
func (c *chatClient) chat(ctx context.Context, w Workload, session string, turn int, msgs []chatMessage) (string, Record) {
	reply, rec := c.exchange(ctx, w, session, turn, msgs)
	return reply, c.redact(rec)
}

// exchange is the request and its answer, before the record is redacted.
func (c *chatClient) exchange(ctx context.Context, w Workload, session string, turn int, msgs []chatMessage) (string, Record) {
	rec := Record{Workload: w.Name, Session: session, Turn: turn}
	body := chatBody{Model: c.model, Messages: msgs, Stream: true, MaxTokens: w.OutputTokens}
	body.StreamOptions.IncludeUsage = true
	if !c.omitMinTokens {
		body.MinTokens = ptrInt(w.OutputTokens)
	}
	b, err := json.Marshal(body)
	if err != nil {
		rec.Start, rec.End, rec.Error = c.now(), c.now(), "request not encodable"
		return "", rec
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(b))
	if err != nil {
		rec.Start, rec.End, rec.Error = c.now(), c.now(), "request not built"
		return "", rec
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set(c.sessionHeader, session)
	for k, v := range w.Headers {
		req.Header.Set(k, v)
	}
	if c.key != "" {
		req.Header.Set("Authorization", "Bearer "+c.key)
	}

	rec.Start = c.now()
	resp, err := c.http.Do(req)
	if err != nil {
		rec.End, rec.Error = c.now(), "transport error"
		return "", rec
	}
	defer resp.Body.Close()
	rec.Status = resp.StatusCode
	rec.Attribution = attributionOf(resp.Header)
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10)) // let the connection be reused; the body is not read
		rec.End, rec.Error = c.now(), fmt.Sprintf("status %d", resp.StatusCode)
		return "", rec
	}

	var reply strings.Builder
	var first, last time.Time
	done := false
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for sc.Scan() {
		data, ok := strings.CutPrefix(sc.Text(), "data:")
		if !ok {
			continue
		}
		data = strings.TrimSpace(data)
		if data == "[DONE]" {
			done = true
			break
		}
		var ev event
		if err := json.Unmarshal([]byte(data), &ev); err != nil {
			rec.Error = "malformed event"
			break
		}
		if len(ev.Error) > 0 && string(ev.Error) != "null" {
			rec.Error = "error event"
			break
		}
		if text, any := ev.text(); any {
			now := c.now()
			if first.IsZero() {
				first = now
			}
			last = now
			reply.WriteString(text)
		}
		if ev.Usage != nil {
			rec.PromptTokens, rec.CompletionTokens = ptrInt(ev.Usage.PromptTokens), ptrInt(ev.Usage.CompletionTokens)
		}
	}
	rec.End = c.now()
	switch {
	case rec.Error != "":
	case sc.Err() != nil:
		rec.Error = "stream read error"
	case !done:
		rec.Error = "stream ended without [DONE]"
	}
	if !first.IsZero() {
		rec.TTFTMs = ptrFloat(ms(first.Sub(rec.Start)))
		if n := rec.CompletionTokens; n != nil && *n > 1 && last.After(first) {
			rec.DecodeTokensPerSec = ptrFloat(float64(*n-1) / last.Sub(first).Seconds())
		}
	}
	return reply.String(), rec
}
