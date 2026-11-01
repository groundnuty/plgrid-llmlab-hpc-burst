package front

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// Which deadline bounds an Instance's connection set-up (HBEP-5, Time-outs
// and keep-alive: connectTimeout applies to LLMLab only; HBEP-6:
// tunnelSetupTimeout bounds the dial to frps and the CONNECT exchange). The
// double stands in for frps's tcpmux port, so no frp binaries are needed.

// TestConnectTimeoutDoesNotBoundAnInstanceDial (PR #129 review, N3): with
// connectTimeout at 100 ms, far below tunnelSetupTimeout (a minute here),
// frps answers the CONNECT only after holding it a second, and the request
// through the Instance's transport still gets the engine's answer. Were
// connectTimeout to bound the dial too, the set-up would be cut at 100 ms.
func TestConnectTimeoutDoesNotBoundAnInstanceDial(t *testing.T) {
	m := startMuxDouble(t)
	uid := probeInstance("00000000-0000-4000-8000-000000000081", v1alpha1.InstanceReady, metav1.ConditionTrue).UID
	m.set(uid, http.StatusOK)
	cfg := Config{ConnectTimeout: Duration(100 * time.Millisecond), TunnelSetupTimeout: Duration(time.Minute)}.WithDefaults()
	tun := NewTunnel(m.ln.Addr().String(), cfg.TunnelSetupTimeout.D())
	tun.setRegistered(uid, true)
	tr, ok := newInstanceTransports(cfg, tun).get(uid)
	if !ok {
		t.Fatal("no transport for a registered Instance")
	}
	defer tr.CloseIdleConnections()
	held, release := m.holdNext(t, uid, "connect")
	go func() {
		<-held
		time.Sleep(time.Second) // a hold ten times connectTimeout; the pass does not depend on its length
		release()
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second) // a watchdog only
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://instance/health", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := tr.RoundTrip(req)
	if err != nil {
		t.Fatalf("a CONNECT that frps answers after 1 s, with connectTimeout 100 ms: %v; want the engine's answer", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status %d, want 200", resp.StatusCode)
	}
}

// TestRunBoundsTheTunnelSetUpByTunnelSetupTimeout (PR #129 review, N3): the
// Tunnel that Run builds cuts a CONNECT that frps never answers at
// tunnelSetupTimeout (200 ms), not at connectTimeout (3 s). The error names
// the deadline it was given, so no wall-clock window decides the verdict.
func TestRunBoundsTheTunnelSetUpByTunnelSetupTimeout(t *testing.T) {
	m := startMuxDouble(t)
	in := probeInstance("00000000-0000-4000-8000-000000000082", v1alpha1.InstanceReady, metav1.ConditionTrue)
	m.set(in.UID, http.StatusOK)
	f, _ := runProbeFront(t, m, func(o *Options) {
		o.Config.TunnelSetupTimeout = Duration(200 * time.Millisecond)
		o.Config.ConnectTimeout = Duration(3 * time.Second)
	})
	m.holdNext(t, in.UID, "connect")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second) // a watchdog only
	defer cancel()
	start := time.Now()
	c, err := f.Tunnel.Dial(ctx, in.UID)
	took := time.Since(start)
	if c != nil {
		c.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "no answer within tunnelSetupTimeout (200ms)") {
		t.Fatalf("a CONNECT frps never answers: %v; want the cut at tunnelSetupTimeout (200ms)", err)
	}
	if !errors.Is(err, os.ErrDeadlineExceeded) && !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("the cut %v is not the set-up deadline", err)
	}
	if took < 200*time.Millisecond {
		t.Errorf("the set-up was cut after %s, before tunnelSetupTimeout", took)
	}
}
