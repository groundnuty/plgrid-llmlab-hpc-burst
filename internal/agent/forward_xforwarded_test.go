package agent_test

// HBEP-5, Request headers (the science agent's ruling (b) on #429): toward an Instance the agent's
// forwarder drops a client's X-Forwarded-Proto, X-Forwarded-Host, X-Forwarded-For and Forwarded, so no
// client forwarding header reaches the engine or the router. Go's httputil.ReverseProxy does so when it
// rewrites a request; this pins it on alpha's path, with no router, so a later change to forward.go
// cannot start passing them silently. A header beside them passes unchanged: the positive control.

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
)

func TestTheForwarderDropsTheClientsForwardingHeaders(t *testing.T) {
	dir := forwardDir(t)
	u := startRecordingUpstream(t, dir)
	f, err := agent.NewForwarder(filepath.Join(dir, "serve.sock"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	f.SetTarget(u.path) // alpha: the engine's socket
	req := fmt.Sprintf("POST /v1/chat/completions HTTP/1.1\r\nHost: instance\r\nContent-Type: application/json\r\n"+
		"X-Forwarded-Proto: https\r\nX-Forwarded-Host: front.example\r\nX-Forwarded-For: 203.0.113.9\r\nForwarded: for=203.0.113.9;proto=https\r\n"+
		"X-Client-Trace: t-1\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s", len(forwardBody), forwardBody)
	if status, err := sendToSocket(f.Path(), req); err != nil || status != 200 {
		t.Fatalf("through the forwarder: %d, %v", status, err)
	}
	rs := u.requests()
	if len(rs) != 1 {
		t.Fatalf("the upstream saw %d requests, want 1", len(rs))
	}
	got := rs[0].header
	for _, h := range []string{"X-Forwarded-Proto", "X-Forwarded-Host", "X-Forwarded-For", "Forwarded"} {
		if v := got.Values(h); len(v) != 0 {
			t.Errorf("the client's %s reached the engine: %q", h, v)
		}
	}
	if v := got.Values("X-Client-Trace"); len(v) != 1 || v[0] != "t-1" {
		t.Errorf("X-Client-Trace reached the engine as %q, want exactly t-1: the control header must pass", v)
	}
}
