package managed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// Limits of one catalog read.
const (
	// ConnectTimeout bounds connection set-up, HBEP-5's connectTimeout
	// (Time-outs and keep-alive).
	ConnectTimeout = 10 * time.Second
	// ReadTimeout bounds the whole read, the transport's command time-out
	// (provider.CommandTimeout). A listing is small, so it is far below
	// LLMLab's 180 s cut (L-6.1).
	ReadTimeout = provider.CommandTimeout
	// MaxCatalogBytes is the largest listing body read.
	MaxCatalogBytes = 4 << 20
)

var _ provider.Managed = (*Client)(nil)

// Client reads a managed gateway's catalog with a Provider's credentials:
// provider.Managed over HTTP (HBEP-4, Managed provider in alpha).
//
// It sends GET <baseURL>/models with "Authorization: Bearer <key>", follows
// no redirect (a 3xx is an unknown error, so the key never goes to another
// address), and reads at most MaxCatalogBytes within ReadTimeout. A 200 is
// decoded as OpenAI's list, {"data": [{"id": ...}, ...]}, and an entry's
// "revision" member, when it is a string, is its revision; OpenAI's model
// object has no revision, so that member name is an assumption shared with
// the double (llmlabtest). Unknown members are ignored. A 401 or 403 is an
// error that wraps provider.ErrKeyRejected; every other outcome is an unknown
// error. No error ever holds the key or any byte of the response body, which
// a gateway may fill with the request's own headers.
type Client struct {
	creds provider.ManagedCredentials
	http  *http.Client
}

// NewClient returns a Client for creds, which must be valid. transport nil
// takes a copy of http.DefaultTransport with ConnectTimeout.
func NewClient(creds provider.ManagedCredentials, transport http.RoundTripper) (*Client, error) {
	if err := creds.Validate(); err != nil {
		return nil, err
	}
	if transport == nil {
		transport = newTransport()
	}
	return &Client{creds: creds, http: &http.Client{
		Transport:     transport,
		Timeout:       ReadTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}, nil
}

// newTransport is a copy of http.DefaultTransport whose connection set-up
// takes at most ConnectTimeout.
func newTransport() *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.DialContext = (&net.Dialer{Timeout: ConnectTimeout, KeepAlive: 30 * time.Second}).DialContext
	t.TLSHandshakeTimeout = ConnectTimeout
	return t
}

// listing is the part of an OpenAI model list the Client reads.
type listing struct {
	Data *[]struct {
		ID       *string         `json:"id"`
		Revision json.RawMessage `json:"revision"`
	} `json:"data"`
}

// ListCatalog implements provider.Managed.
func (c *Client) ListCatalog(ctx context.Context) ([]provider.CatalogEntry, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.creds.CatalogURL(), nil)
	if err != nil {
		return nil, fmt.Errorf("catalog read: build the request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.creds.Key)
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("catalog read: %w", err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, MaxCatalogBytes))
		_ = resp.Body.Close()
	}()
	switch {
	case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
		return nil, fmt.Errorf("catalog read answered %d: %w", resp.StatusCode, provider.ErrKeyRejected)
	case resp.StatusCode != http.StatusOK:
		return nil, fmt.Errorf("catalog read answered %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, MaxCatalogBytes+1))
	if err != nil {
		return nil, fmt.Errorf("catalog read: read the body: %w", err)
	}
	if len(body) > MaxCatalogBytes {
		return nil, fmt.Errorf("catalog read: the listing is above %d bytes", MaxCatalogBytes)
	}
	return parseListing(body)
}

// parseListing decodes a listing body. Its errors describe the shape, never
// the content.
func parseListing(body []byte) ([]provider.CatalogEntry, error) {
	var l listing
	if err := json.Unmarshal(body, &l); err != nil {
		var syn *json.SyntaxError
		if errors.As(err, &syn) {
			return nil, fmt.Errorf("catalog read: the listing is not JSON (byte %d)", syn.Offset)
		}
		return nil, errors.New("catalog read: the listing is not an OpenAI model list")
	}
	if l.Data == nil {
		return nil, errors.New("catalog read: the listing has no data array")
	}
	out := make([]provider.CatalogEntry, 0, len(*l.Data))
	for i, d := range *l.Data {
		if d.ID == nil {
			return nil, fmt.Errorf("catalog read: listing entry %d has no string id", i)
		}
		e := provider.CatalogEntry{Name: *d.ID}
		if len(d.Revision) > 0 && string(d.Revision) != "null" {
			var r string
			if json.Unmarshal(d.Revision, &r) != nil {
				return nil, fmt.Errorf("catalog read: listing entry %d has a revision that is not a string", i)
			}
			e.Revision = &r
		}
		out = append(out, e)
	}
	return out, nil
}
