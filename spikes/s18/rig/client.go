package rig

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

// Client sends the spike's requests to the front and records each attempt's outcome. Every request
// carries an ID in its prompt, s18id-<run>-<n>/c<attempt>, which the engine doubles' records hold
// (Tail), so the client's records and the doubles' are reconciled both ways.
type Client struct {
	URL, Key, Run string
	HTTP          *http.Client
	MaxTokens     int
	seq           atomic.Int64
}

// NewClient returns a client of the front at url.
func NewClient(url, key, run string) *Client {
	return &Client{URL: url, Key: key, Run: run, MaxTokens: 8,
		HTTP: &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 64, DisableCompression: true}}}
}

// Result is one attempt's outcome, as the client saw it.
type Result struct {
	ID       string `json:"id"`   // base/c<attempt>
	Base     string `json:"base"` // the request, across the client's attempts
	Attempt  int    `json:"attempt"`
	Session  string `json:"session"`
	Stream   bool   `json:"stream"`
	Marker   string `json:"marker,omitempty"`
	Status   int    `json:"status"`
	Code     string `json:"code,omitempty"`   // the front's error code, or the engine's error type
	Detail   string `json:"detail,omitempty"` // the front's error detail
	Err      string `json:"err,omitempty"`    // a transport error: no answer
	Instance string `json:"instance,omitempty"`
	Complete bool   `json:"complete"` // a whole reply: a 200 with choices, or a stream ending [DONE]
	Start    time.Time
	// Headers, FirstByte and End are since Start; FirstByte is the first byte of the body.
	Headers   time.Duration `json:"headers_ns"`
	FirstByte time.Duration `json:"first_byte_ns"`
	End       time.Duration `json:"end_ns"`
}

// NextBase is a new request's base ID.
func (c *Client) NextBase() string { return fmt.Sprintf("s18id-%s-%d", c.Run, c.seq.Add(1)) }

// Do sends one attempt of request base in session, streamed or not, its prompt carrying marker.
func (c *Client) Do(ctx context.Context, base string, attempt int, session string, stream bool, marker string) Result {
	r := Result{ID: fmt.Sprintf("%s/c%d", base, attempt), Base: base, Attempt: attempt, Session: session, Stream: stream, Marker: marker,
		Headers: -1, FirstByte: -1, End: -1}
	text := "s18 " + r.ID
	if marker != "" {
		text += " " + marker
	}
	body, _ := json.Marshal(map[string]any{"model": Model, "messages": []map[string]string{{"role": "user", "content": text}},
		"max_tokens": c.MaxTokens, "ignore_eos": true, "stream": stream})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.URL+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		r.Err = err.Error()
		return r
	}
	req.Header.Set("Authorization", "Bearer "+c.Key)
	req.Header.Set("Content-Type", "application/json")
	if session != "" {
		req.Header.Set("X-Session-Id", session)
	}
	r.Start = time.Now()
	resp, err := c.HTTP.Do(req)
	if err != nil {
		r.Err = err.Error()
		return r
	}
	defer resp.Body.Close()
	r.Headers, r.Status, r.Instance = time.Since(r.Start), resp.StatusCode, resp.Header.Get("Hpcburst-Instance-Uid")
	br := bufio.NewReader(resp.Body)
	if _, err := br.Peek(1); err == nil {
		r.FirstByte = time.Since(r.Start)
	}
	b, err := io.ReadAll(br)
	r.End = time.Since(r.Start)
	if err != nil {
		r.Err = err.Error()
	}
	if resp.StatusCode != http.StatusOK {
		var e struct {
			Error struct {
				Code   any    `json:"code"`
				Type   string `json:"type"`
				Detail string `json:"detail"`
			} `json:"error"`
		}
		_ = json.Unmarshal(b, &e)
		r.Code, r.Detail = fmt.Sprint(e.Error.Code), e.Error.Detail
		if e.Error.Type != "" && e.Error.Type != "hpcburst_error" {
			r.Code = e.Error.Type // the engine's own error, passed through
		}
		return r
	}
	if stream {
		r.Complete = strings.HasSuffix(string(b), "data: [DONE]\n\n") && !strings.Contains(string(b), "stream_cut")
	} else {
		var ok struct {
			Choices []json.RawMessage `json:"choices"`
		}
		r.Complete = json.Unmarshal(b, &ok) == nil && len(ok.Choices) > 0
	}
	return r
}

// SDKBackoff is the OpenAI SDK's default retry policy, as the spike models it: 2 retries, after 0.5 s
// and 1 s, with no jitter, on a connection error or a 408, 409, 429 or 5xx (Q6 of #528).
var SDKBackoff = []time.Duration{500 * time.Millisecond, time.Second}

// RetryableBySDK reports whether the SDK's default policy retries r.
func RetryableBySDK(r Result) bool {
	if r.Err != "" && r.Status == 0 {
		return true
	}
	return r.Status == 408 || r.Status == 409 || r.Status == 429 || r.Status >= 500
}

// DoRetrying sends request base, then retries it as the SDK does, at most len(backoff) times; each
// retry is a new attempt, a new request to the front with its own ID (base/c<n>).
func (c *Client) DoRetrying(ctx context.Context, base, session string, stream bool, marker string, backoff []time.Duration) []Result {
	out := []Result{c.Do(ctx, base, 0, session, stream, marker)}
	for i, d := range backoff {
		if !RetryableBySDK(out[len(out)-1]) {
			break
		}
		select {
		case <-time.After(d):
		case <-ctx.Done():
			return out
		}
		out = append(out, c.Do(ctx, base, i+1, session, stream, marker))
	}
	return out
}
