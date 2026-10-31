package managed

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// Astra B1: a reply net/http cannot parse never puts the key into the error.
// Go parses Location before CheckRedirect, and quotes a malformed header line
// or status in its error, so a gateway that echoes the request's key into
// either would reach the controller's log through a wrapped error. The
// control shows net/http's own error does quote the canary, so each case can
// fail.
func TestListCatalogTransportErrorsHoldNoKey(t *testing.T) {
	const canary = "sk-canary-b1-7f3a9c0d"
	for _, tc := range []struct{ name, reply string }{
		{"a Location Go cannot parse", "HTTP/1.1 302 Found\r\nLocation: /" + canary + "%zz\r\nContent-Length: 0\r\n\r\n"},
		{"a header line with no colon", "HTTP/1.1 200 OK\r\nBearer " + canary + "\r\nContent-Length: 0\r\n\r\n"},
		{"a malformed status code", "HTTP/1.1 2" + canary + " OK\r\nContent-Length: 0\r\n\r\n"},
		{"a malformed Content-Length", "HTTP/1.1 200 OK\r\nContent-Length: 5" + canary + "\r\n\r\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := startRawStub(t, tc.reply)
			// The control: net/http's own error holds the canary.
			plain := &http.Client{
				Transport:     newTransport(),
				CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
			}
			req, err := http.NewRequest(http.MethodGet, st.baseURL()+"/models", nil)
			if err != nil {
				t.Fatal(err)
			}
			if resp, err := plain.Do(req); err == nil {
				_ = resp.Body.Close()
				t.Fatalf("control: net/http accepted %q", tc.reply)
			} else if !strings.Contains(err.Error(), canary) {
				t.Fatalf("control: net/http's error %q does not quote the canary, so this case shows nothing", err)
			}

			c, err := NewClient(provider.ManagedCredentials{Key: canary, BaseURL: st.baseURL()}, nil)
			if err != nil {
				t.Fatal(err)
			}
			_, err = c.ListCatalog(context.Background())
			if err == nil || errors.Is(err, provider.ErrKeyRejected) {
				t.Fatalf("ListCatalog = %v, want an unknown error", err)
			}
			if strings.Contains(err.Error(), canary) || strings.Contains(err.Error(), "7f3a9c") {
				t.Fatalf("the error %q holds the key", err)
			}
		})
	}
}

// Astra B4: a GET that may have reached the gateway is never sent again by
// the transport. The stub answers the first request on a connection it keeps
// open, then drops every later request once it has read it. Go's default
// transport would send the second read on the kept connection and, when that
// drops, replay it on a new one, so the stub would count three; the control
// shows that.
func TestListCatalogNeverReplaysARead(t *testing.T) {
	dropAfterFirst := func(n int, _ bool) (string, bool) {
		if n == 1 {
			return rawOK(), true
		}
		return "", false
	}
	read := func(c *Client) error { _, err := c.ListCatalog(context.Background()); return err }

	t.Run("control: a keep-alive transport replays", func(t *testing.T) {
		st := startRawStubFunc(t, dropAfterFirst)
		keepAlive := http.DefaultTransport.(*http.Transport).Clone()
		c, err := NewClient(provider.ManagedCredentials{Key: testKey, BaseURL: st.baseURL()}, keepAlive)
		if err != nil {
			t.Fatal(err)
		}
		if err := read(c); err != nil {
			t.Fatal(err)
		}
		_ = read(c)
		if n := st.count(); n != 3 {
			t.Fatalf("control: the stub received %d requests, want 3 (one replayed), so this test shows nothing", n)
		}
	})

	st := startRawStubFunc(t, dropAfterFirst)
	c := newTestClient(t, st.baseURL())
	if err := read(c); err != nil {
		t.Fatal(err)
	}
	if err := read(c); err == nil {
		t.Fatal("the second read = nil, want the dropped connection's error")
	}
	if n := st.count(); n != 2 {
		t.Fatalf("the stub received %d requests, want 2: the dropped read was sent again", n)
	}
}
