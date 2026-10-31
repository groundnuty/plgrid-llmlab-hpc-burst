package agentserver

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

// The Allocation every rig starts with.
const (
	tagA       = "tag-a"
	uidA       = "uid-a"
	jobA       = "job-1"
	instanceA  = "11111111111111111111111111111111"
	namespaceA = "ns-example"
	nameA      = "alloc-example"
)

// The block of loopback ports issue #60 may listen on for HTTPS tests. Handler
// tests use no port at all.
const (
	portFirst = 29280
	portLast  = 29289
)

// testLookup is a map-based AllocationLookup.
type testLookup struct {
	mu     sync.Mutex
	allocs map[string]Allocation
	err    error // returned by every ByTag when set
}

func (l *testLookup) ByTag(_ context.Context, tag string) (Allocation, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.err != nil {
		return Allocation{}, l.err
	}
	a, ok := l.allocs[tag]
	if !ok {
		return Allocation{}, ErrNotFound
	}
	return a, nil
}

func (l *testLookup) set(tag string, a Allocation) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.allocs[tag] = a
}

// syncBuffer is a log sink the server's goroutines and the test share.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// rig is a Server over a fake Backend, a map lookup with one Allocation and its
// credential file in a temp directory.
type rig struct {
	t          *testing.T
	srv        *Server
	backend    *fake.Backend
	lookup     *testLookup
	host       agentapi.Host
	credential string
	logs       *syncBuffer
}

// newRig returns a rig whose Allocation (tagA, uidA) has its job ID recorded as
// jobA. mods adjust the Config before the Server is made.
func newRig(t *testing.T, mods ...func(*Config)) *rig {
	t.Helper()
	credential, err := agentapi.NewKey()
	if err != nil {
		t.Fatal(err)
	}
	r := &rig{
		t:          t,
		backend:    fake.NewBackend(),
		lookup:     &testLookup{allocs: map[string]Allocation{tagA: {Namespace: namespaceA, Name: nameA, UID: uidA, JobID: jobA}}},
		host:       agentapi.Host{StateDir: t.TempDir(), PortBase: portFirst - agentapi.AgentEndpointPortOffset},
		credential: credential,
		logs:       &syncBuffer{},
	}
	if err := agentapi.WriteKeyFile(agentapi.AllocationCredentialPath(r.host.CredentialsDir(), uidA), credential); err != nil {
		t.Fatal(err)
	}
	cfg := Config{
		Lookup:         r.lookup,
		CredentialsDir: r.host.CredentialsDir(),
		Backend:        r.backend,
		Host:           r.host,
		Logger:         slog.New(slog.NewTextHandler(r.logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}
	for _, m := range mods {
		m(&cfg)
	}
	r.srv, err = New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// ident is what an agent presents. An empty field is an absent header.
type ident struct{ credential, tag, jobID, instanceID string }

func (r *rig) good() ident { return ident{r.credential, tagA, jobA, instanceA} }

func (id ident) apply(h http.Header) {
	if id.credential != "" {
		h.Set("Authorization", "Bearer "+id.credential)
	}
	for name, v := range map[string]string{
		agentapi.HeaderTag:             id.tag,
		agentapi.HeaderJobID:           id.jobID,
		agentapi.HeaderAgentInstanceID: id.instanceID,
	} {
		if v != "" {
			h.Set(name, v)
		}
	}
}

// do sends one request straight to the Handler, with no network.
func (r *rig) do(method, target string, id ident, body []byte) *httptest.ResponseRecorder {
	return r.doCtx(context.Background(), method, target, id, body)
}

func (r *rig) doCtx(ctx context.Context, method, target string, id ident, body []byte) *httptest.ResponseRecorder {
	r.t.Helper()
	req := httptest.NewRequestWithContext(ctx, method, target, bytes.NewReader(body))
	id.apply(req.Header)
	rec := httptest.NewRecorder()
	r.srv.Handler().ServeHTTP(rec, req)
	return rec
}

// register registers the agent instance of id through the Handler and fails the
// test unless it is accepted.
func (r *rig) register(id ident) {
	r.t.Helper()
	rec := r.do(http.MethodPost, agentapi.PathRegister, id, registerBody(r.t))
	if rec.Code != http.StatusOK {
		r.t.Fatalf("register: status %d, body %s", rec.Code, rec.Body)
	}
}

func registerBody(t *testing.T) []byte {
	t.Helper()
	return mustJSON(t, agentapi.RegisterRequest{Node: "node-example", CPUArch: "x86_64"})
}

func reportBody(t *testing.T, rep agentapi.Report) []byte {
	t.Helper()
	return mustJSON(t, rep)
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// errorOf decodes the agentapi.Error body of an error answer and checks its
// framing.
func errorOf(t *testing.T, status int, header http.Header, body []byte) agentapi.Error {
	t.Helper()
	if ct := header.Get("Content-Type"); ct != agentapi.ContentType {
		t.Errorf("error answer %d has Content-Type %q, want %q", status, ct, agentapi.ContentType)
	}
	var e agentapi.Error
	if err := json.Unmarshal(body, &e); err != nil {
		t.Fatalf("error answer %d has body %q, not an agentapi.Error: %v", status, body, err)
	}
	return e
}

// wantError fails the test unless the recorded answer is an Error with the
// code, sent with the status that agentapi gives the code.
func wantError(t *testing.T, rec *httptest.ResponseRecorder, code agentapi.Code) {
	t.Helper()
	e := errorOf(t, rec.Code, rec.Header(), rec.Body.Bytes())
	if e.Code != code {
		t.Errorf("code = %q (%q), want %q", e.Code, e.Message, code)
	}
	if want := (&agentapi.Error{Code: code}).HTTPStatus(); rec.Code != want {
		t.Errorf("status = %d, want %d for %s", rec.Code, want, code)
	}
}

func (r *rig) wantBackendCalls(want int) {
	r.t.Helper()
	for _, m := range []string{fake.MethodRegister, fake.MethodDesired, fake.MethodReport} {
		if n := r.backend.Calls(m); n != want {
			r.t.Errorf("Backend %s calls = %d, want %d", m, n, want)
		}
	}
}

// ---- real HTTPS ----

// writeEndpointCert writes a new self-signed ECDSA P-256 certificate and its
// key where the Host says the launcher puts them, and returns its pin.
func writeEndpointCert(t *testing.T, host agentapi.Host) (pin string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "agent-endpoint-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(host.AgentCertFile()), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(host.AgentCertFile(), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(host.AgentKeyFile(), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return agentapi.PinOf(cert)
}

// listenInRange binds the first free port of issue #60's block and keeps the
// listener, so nothing can take the port between the check and the use.
func listenInRange(t *testing.T) net.Listener {
	t.Helper()
	for port := portFirst; port <= portLast; port++ {
		ln, err := net.Listen("tcp", "127.0.0.1:"+strconv.Itoa(port))
		if err == nil {
			return ln
		}
		if !errors.Is(err, syscall.EADDRINUSE) {
			t.Fatalf("listen on port %d: %v", port, err)
		}
	}
	t.Fatalf("no free port in %d-%d", portFirst, portLast)
	return nil
}

// endpoint is a running Serve over real TLS.
type endpoint struct {
	*rig
	base   string // https://127.0.0.1:<port>
	addr   string
	pin    string
	client *http.Client
	stop   func() error // ends Serve and returns what it returned
}

// serve starts Serve on a port of the block with a new certificate, and stops
// it when the test ends: Serve must return nil, its listener must be closed.
func (r *rig) serve() *endpoint {
	r.t.Helper()
	pin := writeEndpointCert(r.t, r.host)
	ln := listenInRange(r.t)
	addr := ln.Addr().String()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.srv.Serve(ctx, ln) }()
	e := &endpoint{rig: r, base: "https://" + addr, addr: addr, pin: pin}
	e.client = e.clientFor(pin)
	var once sync.Once
	var result error
	e.stop = func() error {
		once.Do(func() {
			cancel()
			select {
			case result = <-done:
			case <-time.After(15 * time.Second):
				result = errors.New("Serve did not return within 15 s of its context ending")
			}
		})
		return result
	}
	r.t.Cleanup(func() {
		if err := e.stop(); err != nil {
			r.t.Errorf("Serve returned %v, want nil", err)
		}
		if c, err := net.DialTimeout("tcp", addr, time.Second); err == nil {
			c.Close()
			r.t.Errorf("%s still accepts connections after Serve returned", addr)
		}
	})
	return e
}

// clientFor returns an HTTP client that trusts exactly the key with the pin, as
// the agent does, and closes its connections when the test ends.
func (e *endpoint) clientFor(pin string) *http.Client {
	tr := &http.Transport{TLSClientConfig: agentapi.ClientTLSConfig(pin)}
	e.t.Cleanup(tr.CloseIdleConnections)
	return &http.Client{Transport: tr, Timeout: 20 * time.Second}
}

// do sends one request over TLS and returns the status, the headers and the body.
func (e *endpoint) do(method, path string, id ident, body []byte) (*http.Response, []byte) {
	e.t.Helper()
	return e.doWith(context.Background(), e.client, method, path, id, bytes.NewReader(body))
}

func (e *endpoint) doWith(ctx context.Context, c *http.Client, method, path string, id ident, body io.Reader) (*http.Response, []byte) {
	e.t.Helper()
	req, err := http.NewRequestWithContext(ctx, method, e.base+path, body)
	if err != nil {
		e.t.Fatal(err)
	}
	id.apply(req.Header)
	resp, err := c.Do(req)
	if err != nil {
		e.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		e.t.Fatalf("%s %s: reading the answer: %v", method, path, err)
	}
	return resp, b
}
