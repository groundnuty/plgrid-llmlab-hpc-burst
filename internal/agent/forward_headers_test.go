package agent_test

// The forwarder passes the Instance's serving path to the engine as the front sent it (#440). The
// same POST goes straight to a recording upstream and through the agent's forwarder, from a client
// like the front's, with compression off (internal/front, transport.go). The upstream must see the same
// headers both ways and the body byte for byte. Go's http.Transport adds Accept-Encoding: gzip to a
// request that names no encoding, and would then gunzip a gzip reply and drop its Content-Encoding and
// Content-Length, unless DisableCompression is set. The positive control is a forwarder built as the
// agent's was before the fix, without DisableCompression: the check must see the header it adds.

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
)

// forwardBody is a request body a re-encoding would change: escapes, a duplicate member, odd
// whitespace and bytes beyond ASCII.
const forwardBody = "{ \"model\":\"m\", \"messages\":[{\"role\":\"user\",\"content\":\"caf\\u00e9 é \\/ \"}],\n\"x\":1,\"x\":2 }"

// recordedRequest is what the recording upstream saw of one request.
type recordedRequest struct {
	header http.Header
	body   []byte
}

// recordingUpstream answers every request with 200 on a Unix socket and records its headers and
// the exact bytes of its body.
type recordingUpstream struct {
	path string
	mu   sync.Mutex
	reqs []recordedRequest
}

func startRecordingUpstream(t *testing.T, dir string) *recordingUpstream {
	t.Helper()
	u := &recordingUpstream{path: filepath.Join(dir, "upstream.sock")}
	ln, err := net.Listen("unix", u.path)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		u.mu.Lock()
		u.reqs = append(u.reqs, recordedRequest{header: r.Header.Clone(), body: bytes.Clone(b)})
		u.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	})}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return u
}

func (u *recordingUpstream) requests() []recordedRequest {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]recordedRequest(nil), u.reqs...)
}

// frontLikePost sends forwardBody to the Unix socket at path as the front does: with the Instance
// key, a session header, and compression off, so nothing on the client's side adds an
// Accept-Encoding the forwarder could hide behind.
func frontLikePost(t *testing.T, path string) int {
	t.Helper()
	tr := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", path)
		},
		DisableCompression: true,
	}
	defer tr.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://instance/v1/chat/completions", strings.NewReader(forwardBody))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	req.Header.Set("X-Session-Id", "Conv-aB3xY9zQ==")
	resp, err := (&http.Client{Transport: tr}).Do(req)
	if err != nil {
		t.Fatalf("POST through %s: %v", filepath.Base(path), err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode
}

// headerDiff lists how via's headers differ from direct's; skip names a header whose value may
// differ (a router's own x-request-id), "" for none.
func headerDiff(direct, via http.Header, skip string) []string {
	names := map[string]bool{}
	for k := range direct {
		names[k] = true
	}
	for k := range via {
		names[k] = true
	}
	var sorted []string
	for k := range names {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)
	var out []string
	for _, k := range sorted {
		d, v := direct.Values(k), via.Values(k)
		switch {
		case k == skip && len(d) == 1 && len(v) == 1:
		case d == nil:
			out = append(out, "added "+k+": "+strings.Join(v, ", "))
		case v == nil:
			out = append(out, "removed "+k)
		case strings.Join(d, "\x00") != strings.Join(v, "\x00"):
			out = append(out, "changed "+k+": "+strings.Join(d, ", ")+" -> "+strings.Join(v, ", "))
		}
	}
	return out
}

// forwardDiff sends the request straight to the upstream and through the socket at via, and returns
// how the upstream's view of the second differs from the first, headers and body.
func forwardDiff(t *testing.T, u *recordingUpstream, via string) []string {
	t.Helper()
	if s := frontLikePost(t, u.path); s != http.StatusOK {
		t.Fatalf("straight to the upstream: status %d", s)
	}
	if s := frontLikePost(t, via); s != http.StatusOK {
		t.Fatalf("through %s: status %d", filepath.Base(via), s)
	}
	reqs := u.requests()
	if len(reqs) != 2 {
		t.Fatalf("the upstream saw %d requests, want 2", len(reqs))
	}
	problems := headerDiff(reqs[0].header, reqs[1].header, "")
	for i, r := range reqs {
		if !bytes.Equal(r.body, []byte(forwardBody)) {
			problems = append(problems, []string{"straight", "through the forwarder"}[i]+": the body arrived changed")
		}
	}
	return problems
}

// forwardDir is a directory whose paths fit a Unix socket's 107 bytes.
func forwardDir(t *testing.T) string {
	t.Helper()
	d, err := os.MkdirTemp("", "fw")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(d) })
	return d
}

func TestTheForwarderAddsNoHeaderAndKeepsTheBody(t *testing.T) {
	dir := forwardDir(t)
	u := startRecordingUpstream(t, dir)
	f, err := agent.NewForwarder(filepath.Join(dir, "serve.sock"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	f.SetTarget(u.path) // alpha: the forwarder's target is the engine's socket
	for _, p := range forwardDiff(t, u, f.Path()) {
		t.Error(p)
	}
}

// The control: a forwarder as the agent's was before #440, its Transport without DisableCompression,
// adds Accept-Encoding: gzip, and the check sees it.
func TestTheForwarderCheckSeesTheAddedAcceptEncoding(t *testing.T) {
	dir := forwardDir(t)
	u := startRecordingUpstream(t, dir)
	sock := filepath.Join(dir, "before.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	before := &httputil.ReverseProxy{
		Rewrite: func(r *httputil.ProxyRequest) {
			r.SetURL(&url.URL{Scheme: "http", Host: "engine"})
			r.Out.Host = r.In.Host
		},
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", u.path)
			},
			MaxIdleConnsPerHost: 64,
			IdleConnTimeout:     90 * time.Second,
		},
		FlushInterval: -1,
	}
	srv := &http.Server{Handler: before}
	go func() { _ = srv.Serve(ln) }()
	defer srv.Close()
	problems := forwardDiff(t, u, sock)
	if len(problems) != 1 || problems[0] != "added Accept-Encoding: gzip" {
		t.Errorf("the forwarder as it was before #440: %q, want exactly the added Accept-Encoding: gzip", problems)
	}
}
