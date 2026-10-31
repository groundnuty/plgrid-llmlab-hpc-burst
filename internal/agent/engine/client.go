// Package engine is the allocation agent's client of one vLLM engine worker,
// over the engine's local Unix socket (HBEP-7, What it runs: Checks). It
// implements the agent's Engine: the health probe and metrics, verification's
// reads, the qualification checks and the warm-up rounds.
//
// It reads only what the engine reports, from vLLM v0.32.0's HTTP API
// (https://github.com/vllm-project/vllm/tree/v0.32.0, accessed 2026-10-31):
//
//   - GET /health: 200 healthy, 503 dead (vllm/entrypoints/serve/instrumentator/health.py);
//   - GET /version: {"version": ...} (serve/instrumentator/basic.py);
//   - GET /v1/models: the served names and max_model_len (openai/models/serving.py);
//   - POST /tokenize with messages: the rendered prompt's token count
//     (serve/tokenize/protocol.py), for the template's overhead and the
//     synthetic prompts' lengths;
//   - POST /v1/chat/completions: ignore_eos, min_tokens and return_token_ids
//     are vLLM's extensions (openai/chat_completion/protocol.py);
//     system_fingerprint is the engine fingerprint, recorded and never
//     compared (HBEP-3);
//   - GET /metrics: vllm:num_requests_running, vllm:num_requests_waiting,
//     vllm:generation_tokens_total, vllm:kv_cache_usage_perc,
//     vllm:cache_config_info (whose labels give the KV capacity and the cache
//     settings, as a real vLLM 0.32.0 capture in
//     spikes/s2b/results/gh200-1gpu/arms/A1-vr-hash/after-w0.prom shows), and
//     http_requests_total by grouped status (2xx, 4xx, 5xx; the default
//     prometheus-fastapi-instrumentator that serve/instrumentator/metrics.py
//     attaches).
//
// /health, /version, /metrics and /tokenize are outside vLLM's guarded
// prefixes; /v1 needs the Instance's key (serve/middleware/authenticate.py),
// which the client sends on every request.
//
// Synthetic prompts only (HBEP-7, Security: the controller sees no prompts).
// The package imports only the standard library and package agentapi.
package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"
)

// Client talks to one engine over its Unix socket.
type Client struct {
	socket string
	key    string
	hc     *http.Client

	mu           sync.Mutex
	model        string
	overhead     int32
	haveOverhead bool
	wordsFor     map[promptKey]int // calibrated filler lengths
}

// New returns a Client of the engine listening on socket, which it reaches
// with key, the Instance's key.
func New(socket, key string) *Client {
	tr := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", socket)
		},
		MaxIdleConns:        2048,
		MaxIdleConnsPerHost: 2048,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
	}
	return &Client{socket: socket, key: key, hc: &http.Client{Transport: tr}, wordsFor: map[promptKey]int{}}
}

// httpError is a non-2xx answer.
type httpError struct {
	path   string
	status int
	body   string
}

func (e *httpError) Error() string {
	return fmt.Sprintf("%s: HTTP %d: %.120s", e.path, e.status, e.body)
}

// do sends one request and returns the answer's status and body. A non-2xx
// status is an *httpError.
func (c *Client) do(ctx context.Context, method, path string, in any) ([]byte, error) {
	resp, err := c.send(ctx, method, path, in)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if resp.StatusCode/100 != 2 {
		return nil, &httpError{path: path, status: resp.StatusCode, body: string(b)}
	}
	return b, nil
}

// send sends one request and returns the response for the caller to read.
func (c *Client) send(ctx context.Context, method, path string, in any) (*http.Response, error) {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://engine"+path, body)
	if err != nil {
		return nil, err
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Authorization", "Bearer "+c.key)
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return resp, nil
}

func (c *Client) getJSON(ctx context.Context, path string, out any) error {
	b, err := c.do(ctx, http.MethodGet, path, nil)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b, out); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	return nil
}

func (c *Client) postJSON(ctx context.Context, path string, in, out any) error {
	b, err := c.do(ctx, http.MethodPost, path, in)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b, out); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	return nil
}

// Probe is the local health probe: GET /health answers 200.
func (c *Client) Probe(ctx context.Context) error {
	_, err := c.do(ctx, http.MethodGet, "/health", nil)
	return err
}

// modelCard is one entry of /v1/models.
type modelCard struct {
	ID          string `json:"id"`
	MaxModelLen int64  `json:"max_model_len"`
}

func (c *Client) models(ctx context.Context) ([]modelCard, error) {
	var out struct {
		Data []modelCard `json:"data"`
	}
	if err := c.getJSON(ctx, "/v1/models", &out); err != nil {
		return nil, err
	}
	return out.Data, nil
}

// servedModel returns the name the engine serves, which every request names.
func (c *Client) servedModel(ctx context.Context) (string, error) {
	c.mu.Lock()
	m := c.model
	c.mu.Unlock()
	if m != "" {
		return m, nil
	}
	cards, err := c.models(ctx)
	if err != nil {
		return "", err
	}
	if len(cards) == 0 || cards[0].ID == "" {
		return "", fmt.Errorf("/v1/models lists no model")
	}
	c.mu.Lock()
	c.model = cards[0].ID
	c.mu.Unlock()
	return cards[0].ID, nil
}
