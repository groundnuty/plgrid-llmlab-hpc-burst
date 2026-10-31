package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// Client is the agent's HTTPS client of the agent endpoint (agentapi's
// Transport). It trusts exactly the pinned certificate
// (agentapi.ClientTLSConfig) and sends the agent's identity on every call.
// It holds no session: every call carries the credential.
type Client struct {
	base            string // https://<host:port>, no trailing slash
	hc              *http.Client
	credential      string
	tag             string
	jobID           string
	agentInstanceID string
}

// ClientOptions configure a Client.
type ClientOptions struct {
	// Endpoint is host:port of the agent endpoint, as the agent reaches it: in
	// direct mode agent.json's Endpoint (the front's dial-home listener); in
	// ssh mode it is ignored, and Dial reaches the forward.
	Endpoint string
	// Dial, when set, replaces the TCP dial: in ssh mode it connects to the
	// ssh forward's local Unix socket.
	Dial func(ctx context.Context) (net.Conn, error)
	// Pin is agent.json's CertSPKISHA256.
	Pin string
	// Credential, Tag, JobID and AgentInstanceID identify the agent.
	Credential, Tag, JobID, AgentInstanceID string
}

// NewClient returns a Client. The HTTP client allows a long poll its full
// agentapi.LongPollTimeout plus ten seconds (agentapi.LongPollTimeout's
// documentation).
func NewClient(o ClientOptions) *Client {
	tr := &http.Transport{
		TLSClientConfig:     agentapi.ClientTLSConfig(o.Pin),
		TLSHandshakeTimeout: 10 * time.Second,
		MaxIdleConnsPerHost: 4,
		IdleConnTimeout:     90 * time.Second,
		ForceAttemptHTTP2:   true,
	}
	host := o.Endpoint
	if o.Dial != nil {
		dial := o.Dial
		tr.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) { return dial(ctx) }
		host = "agent-endpoint" // the pin is the trust; no host name is checked
	} else {
		d := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
		tr.DialContext = d.DialContext
	}
	return &Client{
		base:            "https://" + host,
		hc:              &http.Client{Transport: tr, Timeout: agentapi.LongPollTimeout + 10*time.Second},
		credential:      o.Credential,
		tag:             o.Tag,
		jobID:           o.JobID,
		agentInstanceID: o.AgentInstanceID,
	}
}

// Register implements API.
func (c *Client) Register(ctx context.Context, r agentapi.RegisterRequest) (agentapi.RegisterResponse, error) {
	var out agentapi.RegisterResponse
	err := c.do(ctx, http.MethodPost, agentapi.PathRegister, r, &out)
	return out, err
}

// Desired implements API.
func (c *Client) Desired(ctx context.Context, since string) (agentapi.DesiredState, error) {
	var out agentapi.DesiredState
	path := agentapi.PathDesired + "?" + url.Values{agentapi.QuerySince: {since}}.Encode()
	err := c.do(ctx, http.MethodGet, path, nil, &out)
	return out, err
}

// Report implements API. The report is encoded with agentapi.EncodeBody, so
// one above agentapi.MaxBodyBytes is refused here, before it is sent.
func (c *Client) Report(ctx context.Context, r agentapi.Report) error {
	return c.do(ctx, http.MethodPost, agentapi.PathReport, r, nil)
}

// CloseIdle closes idle connections, as after the path changed.
func (c *Client) CloseIdle() { c.hc.CloseIdleConnections() }

func (c *Client) do(ctx context.Context, method, path string, in, out any) error {
	var body io.Reader
	if in != nil {
		b, err := agentapi.EncodeBody(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, body)
	if err != nil {
		return fmt.Errorf("%s %s: %w", method, path, err)
	}
	if in != nil {
		req.Header.Set("Content-Type", agentapi.ContentType)
	}
	req.Header.Set("Authorization", "Bearer "+c.credential)
	req.Header.Set(agentapi.HeaderTag, c.tag)
	req.Header.Set(agentapi.HeaderJobID, c.jobID)
	req.Header.Set(agentapi.HeaderAgentInstanceID, c.agentInstanceID)
	resp, err := c.hc.Do(req)
	if err != nil {
		// A transport error carries the URL, never a header.
		return fmt.Errorf("%s %s: %w", method, path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return decodeError(resp)
	}
	if out == nil {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, agentapi.MaxBodyBytes))
		return nil
	}
	return agentapi.DecodeBody(resp.Body, out)
}

// decodeError turns an error response into an *agentapi.Error. A body that is
// not one keeps the status: 503 and 409 keep their protocol meaning, anything
// else is CodeInternal.
func decodeError(resp *http.Response) error {
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	var e agentapi.Error
	if json.Unmarshal(b, &e) == nil && e.Code != "" {
		return &e
	}
	code := agentapi.CodeInternal
	switch resp.StatusCode {
	case http.StatusServiceUnavailable, http.StatusBadGateway, http.StatusGatewayTimeout:
		code = agentapi.CodeUnavailable
	case http.StatusUnauthorized:
		code = agentapi.CodeUnauthenticated
	case http.StatusForbidden:
		code = agentapi.CodeJobMismatch
	case http.StatusConflict:
		code = agentapi.CodeNotRegistered
	}
	return agentapi.Errorf(code, "HTTP %d without a protocol error body", resp.StatusCode)
}

// codeOf returns the protocol code of err, or "" for a transport error.
func codeOf(err error) agentapi.Code {
	var e *agentapi.Error
	if errors.As(err, &e) {
		return e.Code
	}
	return ""
}
