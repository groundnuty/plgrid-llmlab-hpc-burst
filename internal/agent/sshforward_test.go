package agent_test

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/enginedouble"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// sshConfig is agent.json in ssh mode, with a dial-home directory holding the
// key and the pinned host key.
func sshConfig(t *testing.T, root string) agentapi.AgentConfig {
	t.Helper()
	dh := filepath.Join(root, "dial-home")
	if err := os.MkdirAll(dh, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{agentapi.DialHomeKeyFile, agentapi.DialHomeKnownHostsFile} {
		if err := os.WriteFile(filepath.Join(dh, f), []byte("test\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return agentapi.AgentConfig{
		ProtocolVersion: agentapi.ProtocolVersion, Tag: hTag, Mode: agentapi.DialHomeSSH,
		Endpoint: "127.0.0.1:22", SSH: &agentapi.SSHForward{User: "hb", AgentPort: 29299},
		CertSPKISHA256: strings.Repeat("a", 64), StagingDir: filepath.Join(root, "staging"), DialHomeDir: dh,
	}
}

// When the forward stops, every ssh process it started is stopped and reaped:
// TERM, then KILL after the grace for one that ignores TERM. The processes are
// found by the PID and start time the forward recorded, never by name, and
// none is alive once Run has returned (HBEP-6, teardown).
func TestSSHForwardStopsAndReapsItsChildren(t *testing.T) {
	root, err := os.MkdirTemp("", "hbf")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	bin := filepath.Join(root, "bin")
	os.MkdirAll(bin, 0o700)
	if err := enginedouble.Install(bin); err != nil {
		t.Fatal(err)
	}
	// An ssh that ignores TERM, and a child of it in its process group.
	stubborn := filepath.Join(bin, "stubborn-ssh")
	if err := os.WriteFile(stubborn, []byte("#!/bin/sh\ntrap '' TERM\nsleep 300 &\nwhile :; do sleep 1; done\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	const grace = 400 * time.Millisecond
	for _, tc := range []struct {
		name    string
		ssh     string
		minTook time.Duration
	}{
		{"honours TERM", filepath.Join(bin, "ssh"), 0},
		{"ignores TERM", stubborn, grace},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			var started []agent.ProcessID
			fwd := agent.SSHForward{
				SSH: tc.ssh, Socket: filepath.Join(root, "agent.sock"), Config: sshConfig(t, root),
				Log: io.Discard, StopGrace: grace,
				OnStart: func(id agent.ProcessID) { mu.Lock(); started = append(started, id); mu.Unlock() },
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- fwd.Run(ctx) }()
			deadline := time.Now().Add(10 * time.Second)
			for {
				mu.Lock()
				n := len(started)
				mu.Unlock()
				if n > 0 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("the forward started no ssh")
				}
				time.Sleep(10 * time.Millisecond)
			}
			time.Sleep(200 * time.Millisecond) // the shell arms its trap
			mu.Lock()
			ids := append([]agent.ProcessID(nil), started...)
			mu.Unlock()
			for _, id := range ids {
				if id.StartTime == 0 || !id.Alive() {
					t.Fatalf("recorded %+v is not a live process", id)
				}
			}
			stopped := time.Now()
			cancel()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("Run did not return after its context ended")
			}
			if took := time.Since(stopped); took < tc.minTook {
				t.Fatalf("stopped after %s, before the %s grace", took, tc.minTook)
			}
			for _, id := range ids {
				if id.Alive() {
					t.Fatalf("ssh %+v still runs after the forward stopped", id)
				}
			}
		})
	}
}
