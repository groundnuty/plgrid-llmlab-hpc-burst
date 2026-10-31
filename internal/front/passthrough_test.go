package front

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// HBEP-5, Test plan, Passthrough, and Listeners: in direct mode an agent
// registers, long-polls and reports through the dial-home listener, which
// presents the pinned certificate; the front passes every request under
// /agent/v1/ through unchanged to the lifecycle controller's loopback
// endpoint, streams the answer back, long polls included, and adds nothing.
// The client listener answers 404 under /agent/v1/.
//
// The controller's agent endpoint is a TLS stand-in on 127.0.0.1:29333, the
// port at port base 29330 (Host.AgentEndpointAddr): that port is this file's,
// and no other test of the package binds it. The calls are the three of HBEP-7:
// POST /agent/v1/register, GET /agent/v1/desired (a long poll) and
// POST /agent/v1/report.

const ptPortBase = 29330

// ptCaptured is a request as the stand-in received it.
type ptCaptured struct {
	Method, Path, Query, Host string
	Header                    http.Header
	Body                      []byte
}

// ptEndpoint is the stand-in for the lifecycle controller's agent endpoint.
type ptEndpoint struct {
	mu   sync.Mutex
	got  []ptCaptured
	desk func(http.ResponseWriter, *http.Request) // the /desired handler
}

func (p *ptEndpoint) requests() []ptCaptured {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]ptCaptured(nil), p.got...)
}

func (p *ptEndpoint) setDesk(h func(http.ResponseWriter, *http.Request)) {
	p.mu.Lock()
	p.desk = h
	p.mu.Unlock()
}

func (p *ptEndpoint) capture(r *http.Request) ptCaptured {
	b, _ := io.ReadAll(r.Body)
	c := ptCaptured{Method: r.Method, Path: r.URL.Path, Query: r.URL.RawQuery, Host: r.Host, Header: r.Header.Clone(), Body: b}
	p.mu.Lock()
	p.got = append(p.got, c)
	p.mu.Unlock()
	return c
}

// ptStandIn serves TLS on the agent endpoint's address with the certificate at
// the host's path.
func ptStandIn(t *testing.T, host agentapi.Host) *ptEndpoint {
	t.Helper()
	cert, err := tls.LoadX509KeyPair(host.AgentCertFile(), host.AgentKeyFile())
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", host.AgentEndpointAddr())
	if err != nil {
		t.Fatalf("port %s is reserved for this test's agent endpoint stand-in and is taken: %v", host.AgentEndpointAddr(), err)
	}
	p := &ptEndpoint{}
	mux := http.NewServeMux()
	mux.HandleFunc("/agent/v1/register", func(w http.ResponseWriter, r *http.Request) {
		c := p.capture(r)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Agent-Reply", "registered")
		w.Header().Set("Etag", `"g1"`)
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write(append([]byte(`{"registered":true,"echo":`), append(c.Body, '}')...))
	})
	mux.HandleFunc("/agent/v1/desired", func(w http.ResponseWriter, r *http.Request) {
		p.mu.Lock()
		h := p.desk
		p.mu.Unlock()
		h(w, r)
	})
	mux.HandleFunc("/agent/v1/report", func(w http.ResponseWriter, r *http.Request) {
		c := p.capture(r)
		sum := sha256.Sum256(c.Body)
		w.Header().Set("X-Body-Sha256", hex.EncodeToString(sum[:]))
		w.Header().Set("X-Body-Length", strconv.Itoa(len(c.Body)))
		w.WriteHeader(http.StatusNoContent)
	})
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		_ = srv.Serve(tls.NewListener(ln, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}))
	}()
	t.Cleanup(func() { _ = srv.Close() })
	return p
}

// ptServeDialHome serves the dial-home handler of a direct-mode front on a
// port of the test block, as dialHomeTLS makes it, and returns its address.
func ptServeDialHome(t *testing.T, host agentapi.Host) string {
	t.Helper()
	dh, err := NewDialHome(ModeDirect, host, nil)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := dialHomeTLS(ModeDirect, host)
	if err != nil || cfg == nil {
		t.Fatalf("dialHomeTLS: %v, %v", cfg, err)
	}
	ln, err := enginetest.Listen(testPortLo, testPortHi)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: dh, ReadHeaderTimeout: time.Minute}
	go func() { _ = srv.Serve(tls.NewListener(ln, cfg)) }()
	t.Cleanup(func() { _ = srv.Close() })
	return ln.Addr().String()
}

// ptHeaderDiff lists the differences between the headers a client sent and
// the ones the stand-in received. They must be the same set: the front adds
// nothing (and with the client's compression off, no Accept-Encoding).
func ptHeaderDiff(sent, got http.Header) []string {
	var diffs []string
	for k, want := range sent {
		if g := got.Values(k); strings.Join(g, "\x00") != strings.Join(want, "\x00") {
			diffs = append(diffs, k+": sent "+strings.Join(want, ",")+", received "+strings.Join(g, ","))
		}
	}
	for k, g := range got {
		if _, ok := sent[k]; !ok {
			diffs = append(diffs, k+": received "+strings.Join(g, ",")+" though the client sent none")
		}
	}
	return diffs
}

// ptAgent is the agent's HTTP client: it pins the certificate it was given,
// and has no compression, so an Accept-Encoding at the endpoint can only be
// the front's.
func ptAgent(pin string) *http.Client {
	return &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{
		TLSClientConfig: agentapi.ClientTLSConfig(pin), DisableCompression: true, DisableKeepAlives: true}}
}

func ptHeaders() http.Header {
	return http.Header{
		"Authorization":   {"Bearer allocation-credential-for-the-test-4d2f"},
		"Content-Type":    {"application/json"},
		"User-Agent":      {"hb-agent/test"},
		"X-Agent-Version": {"1.2.3"},
	}
}

// TestPassthroughDirectModeAgentRegistersLongPollsAndReports covers the three
// calls through the dial-home listener, which presents the pinned certificate.
func TestPassthroughDirectModeAgentRegistersLongPollsAndReports(t *testing.T) {
	host := agentapi.Host{StateDir: t.TempDir(), PortBase: ptPortBase}
	cert := writeAgentCert(t, host)
	end := ptStandIn(t, host)
	addr := ptServeDialHome(t, host)
	pin := agentapi.PinOf(cert)
	agent := ptAgent(pin)
	base := "https://" + addr

	t.Run("register", func(t *testing.T) {
		body := `{"allocationTag":"alloc-1","pid":4242,"capabilities":["engine","tunnel"]}`
		req, err := http.NewRequest(http.MethodPost, base+"/agent/v1/register?attempt=2", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header = ptHeaders()
		resp, err := agent.Do(req)
		if err != nil {
			t.Fatalf("the agent, pinning the certificate, cannot register through the dial-home listener: %v", err)
		}
		got := readAll(t, resp)
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("status %d: %s", resp.StatusCode, got)
		}
		if want := `{"registered":true,"echo":` + body + `}`; got != want {
			t.Errorf("response body %q, want %q", got, want)
		}
		if resp.Header.Get("X-Agent-Reply") != "registered" || resp.Header.Get("Etag") != `"g1"` || resp.Header.Get("Content-Type") != "application/json" {
			t.Errorf("response headers changed: %v", resp.Header)
		}
		reqs := end.requests()
		if len(reqs) != 1 {
			t.Fatalf("the endpoint saw %d requests, want 1", len(reqs))
		}
		r := reqs[0]
		if r.Method != http.MethodPost || r.Path != "/agent/v1/register" || r.Query != "attempt=2" || r.Host != addr {
			t.Errorf("the endpoint saw %s %s?%s for host %s", r.Method, r.Path, r.Query, r.Host)
		}
		if string(r.Body) != body {
			t.Errorf("request body %q, want %q", r.Body, body)
		}
	})

	t.Run("long poll is held and then streamed", func(t *testing.T) {
		const hold = time.Second
		chunk1, chunk2 := "{\"generation\":7}\n", "{\"generation\":8}\n"
		got1 := make(chan struct{})
		var confirmed bool
		var served sync.WaitGroup
		served.Add(1)
		end.setDesk(func(w http.ResponseWriter, r *http.Request) {
			defer served.Done()
			end.capture(r)
			select { // the hold of a long poll
			case <-time.After(hold):
			case <-r.Context().Done():
				return
			}
			w.Header().Set("Content-Type", "application/x-ndjson")
			w.Header().Set("Cache-Control", "no-store")
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, chunk1)
			_ = http.NewResponseController(w).Flush()
			select { // the second chunk waits for the agent to have the first
			case <-got1:
				confirmed = true
			case <-time.After(5 * time.Second):
			}
			_, _ = io.WriteString(w, chunk2)
		})
		before := len(end.requests())
		req, err := http.NewRequest(http.MethodGet, base+"/agent/v1/desired?generation=6", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header = ptHeaders()
		start := time.Now()
		resp, err := agent.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if took := time.Since(start); took < hold*9/10 {
			t.Errorf("the headers came after %v, want the endpoint's hold of %v", took, hold)
		}
		first := make([]byte, len(chunk1))
		if _, err := io.ReadFull(resp.Body, first); err != nil || string(first) != chunk1 {
			t.Fatalf("the first chunk: %q, %v; want %q", first, err, chunk1)
		}
		close(got1)
		rest, err := io.ReadAll(resp.Body)
		if err != nil || string(rest) != chunk2 {
			t.Fatalf("the rest: %q, %v; want %q", rest, err, chunk2)
		}
		served.Wait()
		if !confirmed {
			t.Errorf("the first chunk did not reach the agent before the second was written: the front buffered the long poll")
		}
		if resp.Header.Get("Cache-Control") != "no-store" || resp.Header.Get("Content-Type") != "application/x-ndjson" {
			t.Errorf("response headers changed: %v", resp.Header)
		}
		reqs := end.requests()
		if len(reqs) != before+1 {
			t.Fatalf("the endpoint saw %d requests, want %d", len(reqs), before+1)
		}
		r := reqs[len(reqs)-1]
		if r.Method != http.MethodGet || r.Path != "/agent/v1/desired" || r.Query != "generation=6" {
			t.Errorf("the endpoint saw %s %s?%s", r.Method, r.Path, r.Query)
		}
	})

	t.Run("report with a large body", func(t *testing.T) {
		body := []byte(`{"report":"` + strings.Repeat("0123456789abcdef", 16<<10) + `"}`) // 256 KiB
		sum := sha256.Sum256(body)
		before := len(end.requests())
		req, err := http.NewRequest(http.MethodPost, base+"/agent/v1/report", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header = ptHeaders()
		resp, err := agent.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = readAll(t, resp)
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("status %d, want 204", resp.StatusCode)
		}
		if resp.Header.Get("X-Body-Sha256") != hex.EncodeToString(sum[:]) || resp.Header.Get("X-Body-Length") != strconv.Itoa(len(body)) {
			t.Errorf("the endpoint received %s bytes with hash %s; the agent sent %d with %x",
				resp.Header.Get("X-Body-Length"), resp.Header.Get("X-Body-Sha256"), len(body), sum)
		}
		reqs := end.requests()
		if len(reqs) != before+1 || !bytes.Equal(reqs[len(reqs)-1].Body, body) {
			t.Fatalf("the endpoint's copy of the report differs from the agent's")
		}
	})

	t.Run("a client that pins another certificate refuses the listener", func(t *testing.T) {
		before := len(end.requests())
		_, err := ptAgent(strings.Repeat("0", 64)).Get(base + "/agent/v1/register")
		if err == nil {
			t.Fatal("a client pinning another key connected to the dial-home listener")
		}
		if n := len(end.requests()); n != before {
			t.Errorf("the endpoint saw %d requests from a client that refused the certificate", n-before)
		}
	})

	t.Run("only /agent/v1/ and the tunnel are served: another path is 404", func(t *testing.T) {
		before := len(end.requests())
		resp, err := agent.Post(base+"/v1/chat/completions", "application/json", strings.NewReader(`{"model":"m"}`))
		if err != nil {
			t.Fatal(err)
		}
		_ = readAll(t, resp)
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("POST /v1/chat/completions on the dial-home listener: status %d, want 404", resp.StatusCode)
		}
		if n := len(end.requests()); n != before {
			t.Errorf("the endpoint saw %d requests for a path outside /agent/v1/", n-before)
		}
	})
}

// TestPassthroughAddsNoRequestHeaders: "the front adds nothing" (Listeners): the
// headers the endpoint receives are the ones the agent sent, plus the
// Content-Length of its body, for each of the three calls. The agent's
// compression is off and it sends no Accept-Encoding, so one at the endpoint
// would be the front's.
func TestPassthroughAddsNoRequestHeaders(t *testing.T) {
	host := agentapi.Host{StateDir: t.TempDir(), PortBase: ptPortBase}
	cert := writeAgentCert(t, host)
	end := ptStandIn(t, host)
	end.setDesk(func(w http.ResponseWriter, r *http.Request) {
		end.capture(r)
		w.WriteHeader(http.StatusNoContent)
	})
	base := "https://" + ptServeDialHome(t, host)
	agent := ptAgent(agentapi.PinOf(cert))
	calls := []struct{ method, path, body string }{
		{http.MethodPost, "/agent/v1/register", `{"allocationTag":"alloc-1"}`},
		{http.MethodGet, "/agent/v1/desired?generation=1", ""},
		{http.MethodPost, "/agent/v1/report", `{"instances":[]}`},
	}
	for i, c := range calls {
		var rd io.Reader
		if c.body != "" {
			rd = strings.NewReader(c.body)
		}
		req, err := http.NewRequest(c.method, base+c.path, rd)
		if err != nil {
			t.Fatal(err)
		}
		req.Header = ptHeaders()
		resp, err := agent.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = readAll(t, resp)
		reqs := end.requests()
		if len(reqs) != i+1 {
			t.Fatalf("%s: the endpoint saw %d requests, want %d", c.path, len(reqs), i+1)
		}
		want := ptHeaders()
		if c.body != "" {
			want.Set("Content-Length", strconv.Itoa(len(c.body)))
		}
		if d := ptHeaderDiff(want, reqs[i].Header); len(d) > 0 {
			t.Errorf("%s %s: request headers not passed unchanged (HBEP-5, Listeners: the front adds nothing):\n  %s", c.method, c.path, strings.Join(d, "\n  "))
		}
	}
}

// TestPassthroughClientListenerAnswers404UnderAgentV1: a whole front in direct
// mode, with the agent endpoint's stand-in up. The same call that works on the
// dial-home listener is a 404 on the client listener, with or without the
// client key, and the endpoint sees nothing of it.
func TestPassthroughClientListenerAnswers404UnderAgentV1(t *testing.T) {
	host := agentapi.Host{StateDir: t.TempDir(), PortBase: ptPortBase}
	cert := writeAgentCert(t, host)
	end := ptStandIn(t, host)

	o := runOptions(t, ModeDirect, host, "")
	dial, err := listenFree()
	if err != nil {
		t.Fatal(err)
	}
	o.DialHomeListener = dial
	ready := make(chan *Front, 1)
	o.Ready = func(f *Front) { ready <- f }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, o) }()
	defer func() { cancel(); <-done }()
	var f *Front
	select {
	case f = <-ready:
	case err := <-done:
		t.Fatalf("Run returned early: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("the front did not start")
	}

	// The control: the dial-home listener of this front reaches the endpoint.
	agent := ptAgent(agentapi.PinOf(cert))
	resp, err := agent.Post("https://"+f.Dial.String()+"/agent/v1/register", "application/json", strings.NewReader(`{"allocationTag":"alloc-1"}`))
	if err != nil {
		t.Fatalf("the dial-home listener of Run: %v", err)
	}
	_ = readAll(t, resp)
	if resp.StatusCode != http.StatusCreated || len(end.requests()) != 1 {
		t.Fatalf("through the dial-home listener: status %d, %d requests at the endpoint; want 201 and 1", resp.StatusCode, len(end.requests()))
	}

	clientURL := "http://" + f.Client.String()
	for _, tc := range []struct{ method, path, key string }{
		{http.MethodPost, "/agent/v1/register", ""},
		{http.MethodPost, "/agent/v1/register", "client-key"},
		{http.MethodGet, "/agent/v1/desired?generation=1", "client-key"},
		{http.MethodPost, "/agent/v1/report", "client-key"},
	} {
		u, _ := url.Parse(clientURL + tc.path)
		req, err := http.NewRequest(tc.method, u.String(), strings.NewReader(`{"allocationTag":"alloc-1"}`))
		if err != nil {
			t.Fatal(err)
		}
		if tc.key != "" {
			req.Header.Set("Authorization", "Bearer "+tc.key)
		}
		resp, err := ctHTTP().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = readAll(t, resp)
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("%s %s (key %q) on the client listener: status %d, want 404", tc.method, tc.path, tc.key, resp.StatusCode)
		}
	}
	if n := len(end.requests()); n != 1 {
		t.Errorf("the endpoint saw %d requests, want only the dial-home one: the client listener passed one through", n)
	}
}
