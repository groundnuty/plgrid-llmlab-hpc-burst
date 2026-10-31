package agentserver

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

func TestOverHTTPSTheRightAgentRegistersAndPolls(t *testing.T) {
	x := newRig(t).serve()

	resp, body := x.do(http.MethodPost, agentapi.PathRegister, x.good(), registerBody(t))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("register: status %d (%s)", resp.StatusCode, body)
	}
	if resp.TLS == nil || resp.TLS.Version != tls.VersionTLS13 {
		t.Errorf("connection state = %+v, want TLS 1.3", resp.TLS)
	}
	resp, body = x.do(http.MethodGet, agentapi.PathDesired, x.good(), nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("desired: status %d (%s)", resp.StatusCode, body)
	}
	if n := len(x.backend.Registrations(uidA)); n != 1 {
		t.Errorf("registrations = %d, want 1", n)
	}
}

func TestOverHTTPSAWrongCredentialIs401AndAnotherJobIDIs403(t *testing.T) {
	x := newRig(t).serve()

	wrong := x.good()
	wrong.credential = strings.Repeat("cd", 32)
	resp, body := x.do(http.MethodPost, agentapi.PathRegister, wrong, registerBody(t))
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("wrong credential: status %d, want 401", resp.StatusCode)
	}
	if e := errorOf(t, resp.StatusCode, resp.Header, body); e.Code != agentapi.CodeUnauthenticated {
		t.Errorf("wrong credential: code %q, want %q", e.Code, agentapi.CodeUnauthenticated)
	}

	other := x.good()
	other.jobID = "job-2"
	resp, body = x.do(http.MethodPost, agentapi.PathRegister, other, registerBody(t))
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("another job ID: status %d, want 403", resp.StatusCode)
	}
	if e := errorOf(t, resp.StatusCode, resp.Header, body); e.Code != agentapi.CodeJobMismatch {
		t.Errorf("another job ID: code %q, want %q", e.Code, agentapi.CodeJobMismatch)
	}
	x.wantBackendCalls(0)
}

func TestAClientWithAnotherPinFailsTheHandshakeAndTheBackendSeesNothing(t *testing.T) {
	x := newRig(t).serve()
	otherPin := strings.Repeat("0", 64)
	c := x.clientFor(otherPin)

	req, err := http.NewRequest(http.MethodPost, x.base+agentapi.PathRegister, strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	x.good().apply(req.Header)
	resp, err := c.Do(req)
	if err == nil {
		resp.Body.Close()
		t.Fatal("a client with the wrong pin got an answer")
	}
	// The pin check ran: a refused connection or a timeout would say something else.
	if !strings.Contains(err.Error(), "is not the pinned "+otherPin) {
		t.Errorf("error = %v, want the pin mismatch of agentapi.VerifyPin", err)
	}
	x.wantBackendCalls(0)
}

func TestTLS12IsRefused(t *testing.T) {
	x := newRig(t).serve()
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}, "tcp", x.addr, &tls.Config{
		MaxVersion:         tls.VersionTLS12,
		InsecureSkipVerify: true, //nolint:gosec // only the version is under test
	})
	if err == nil {
		conn.Close()
		t.Fatal("a TLS 1.2 handshake succeeded")
	}
	if !strings.Contains(err.Error(), "protocol version") {
		t.Errorf("error = %v, want a protocol version refusal", err)
	}
}

func TestServeEndsOpenPollsAndReturnsNilWhenItsContextEnds(t *testing.T) {
	x := newRig(t, func(c *Config) { c.PollInterval, c.LongPollTimeout = 20*time.Millisecond, time.Minute }).serve()
	x.register(x.good())
	d := agentapi.DesiredState{Action: agentapi.ActionIdle}.WithVersion()
	x.backend.SetDesired(uidA, d)

	type answer struct {
		status int
		body   []byte
	}
	got := make(chan answer, 1)
	go func() {
		resp, body := x.doWith(context.Background(), x.client, http.MethodGet, since(d.Version), x.good(), nil)
		got <- answer{resp.StatusCode, body}
	}()
	deadline := time.Now().Add(5 * time.Second)
	for x.backend.Calls(fake.MethodDesired) < 3 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	start := time.Now()
	if err := x.stop(); err != nil {
		t.Fatalf("Serve returned %v, want nil", err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("Serve took %v to return with a poll open, want a prompt stop", elapsed)
	}
	select {
	case a := <-got:
		if a.status != http.StatusServiceUnavailable {
			t.Errorf("the open poll ended with %d (%s), want 503 so the agent retries", a.status, a.body)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the open poll never ended")
	}
}

func TestServeWithoutACertificateFailsAndClosesTheListener(t *testing.T) {
	x := newRig(t) // no certificate written
	ln := listenInRange(t)
	err := x.srv.Serve(context.Background(), ln)
	if err == nil || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Serve = %v, want an error that wraps a missing file", err)
	}
	if c, derr := net.DialTimeout("tcp", ln.Addr().String(), time.Second); derr == nil {
		c.Close()
		t.Error("the listener is still open after Serve failed")
	}
}

func TestTheHTTPServerTimeoutsLeaveRoomForALongPoll(t *testing.T) {
	// The values are the brief's: 10 s to read headers, 120 s idle, and a write
	// timeout 30 s above the poll, because it covers the handler too.
	x := newRig(t, func(c *Config) { c.LongPollTimeout = 45 * time.Second })
	srv := x.srv.httpServer(context.Background())
	if srv.ReadHeaderTimeout != 10*time.Second {
		t.Errorf("ReadHeaderTimeout = %v, want 10 s", srv.ReadHeaderTimeout)
	}
	if srv.IdleTimeout != 120*time.Second {
		t.Errorf("IdleTimeout = %v, want 120 s", srv.IdleTimeout)
	}
	if srv.WriteTimeout != 75*time.Second {
		t.Errorf("WriteTimeout = %v, want LongPollTimeout + 30 s = 75 s", srv.WriteTimeout)
	}
	if srv.ErrorLog == nil {
		t.Error("ErrorLog is not routed to the Logger")
	}
}
