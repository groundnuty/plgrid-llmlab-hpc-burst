package main

// The agent in host-dialed mode (HBEP-6, Dial-home modes, host-dialed mode):
// it registers, is told to run, verifies and stops through its control
// listener, over a session that a separate "relay control" process (this
// binary, as main runs it) carries through its standard input and output to
// the control-plane end (hostdial.Serve) in the test process, which dials
// only the agent endpoint. frpc's serverPort is the serving listener's.

import (
	"bytes"
	"context"
	"errors"
	"io"

	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// relayPipes is a relay process's standard streams as one connection.
type relayPipes struct {
	io.Reader
	io.WriteCloser
}

func (p relayPipes) Close() error { return p.WriteCloser.Close() }

// keepRelay runs "relay <ch>" processes, one at a time, each serving its
// session to target, and starts another 100 ms after one ends (the first ones
// end at once, before the agent has made its sockets), until ctx ends.
func keepRelay(t *testing.T, ctx context.Context, ch agentapi.RelayChannel, stagingDir, relayRoot, target string, wg *sync.WaitGroup) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	// The relay finds its pointer from its own path, the staged agent's
	// (<stagingDir>/agent/<digest>/hb-agent): this binary, linked there.
	self := filepath.Join(stagingDir, agentapi.StagedAgentPath(strings.Repeat("e", 64)))
	if err := os.MkdirAll(filepath.Dir(self), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(exe, self); err != nil {
		t.Fatal(err)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			cmd := exec.Command(self, agentapi.RelaySubcommand, string(ch))
			cmd.Env = append(os.Environ(), "HB_TEST_ROLE="+roleAgent, "SLURM_JOB_ID="+eJobID, agentapi.EnvRelayRoot+"="+relayRoot)
			in, _ := cmd.StdinPipe()
			out, _ := cmd.StdoutPipe()
			cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
			if err := cmd.Start(); err != nil {
				return
			}
			sctx, cancel := context.WithCancel(ctx)
			served := make(chan struct{})
			go func() { _ = hostdial.Serve(sctx, relayPipes{out, in}, target, ch, nil, nil); close(served) }()
			waited := make(chan struct{})
			go func() { _ = cmd.Wait(); close(waited) }()
			select {
			case <-waited:
			case <-ctx.Done():
				_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
				<-waited
			}
			cancel()
			<-served
			select {
			case <-ctx.Done():
			case <-time.After(100 * time.Millisecond):
			}
		}
	}()
}

func TestAgentEndToEndHostDialed(t *testing.T) {
	root := shortRoot(t)
	ep := startEndpoint(t, root)
	relayRoot := filepath.Join(root, "x")
	if err := os.Mkdir(relayRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	j := newJob(t, root, agentapi.AgentConfig{
		ProtocolVersion: agentapi.ProtocolVersion, Tag: eTag, Mode: agentapi.DialHomeHostDialed,
		CertSPKISHA256: ep.pin, FrpcDigest: strings.Repeat("d", 64),
	})
	j.hostDialed = true
	j.extraEnv = append(j.extraEnv, agentapi.EnvRelayRoot+"="+relayRoot)
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	defer func() { cancel(); wg.Wait() }()
	keepRelay(t, ctx, agentapi.RelayControl, j.stagingDir, relayRoot, ep.addr, &wg)
	out := exercise(t, ep, j)
	if !strings.Contains(out, "relay session up") {
		t.Errorf("the agent logged no relay session:\n%s", out)
	}
	if left, _ := filepath.Glob(filepath.Join(relayRoot, "hb-relay-*")); len(left) != 0 {
		t.Errorf("relay directories %v are left after the agent stopped", left)
	}
	ptr, _ := agentapi.RelayPointerPath(j.stagingDir, eJobID)
	if _, err := os.Stat(ptr); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the relay pointer %s is left after the agent stopped: %v", ptr, err)
	}
}

func TestRelayUsage(t *testing.T) {
	env := func(kv ...string) func(string) string {
		m := map[string]string{}
		for i := 0; i+1 < len(kv); i += 2 {
			m[kv[i]] = kv[i+1]
		}
		return func(k string) string { return m[k] }
	}
	var out, errs bytes.Buffer
	for name, c := range map[string]struct {
		args []string
		env  func(string) string
		want int
	}{
		"no channel":      {nil, env(), 2},
		"another channel": {[]string{"plugin"}, env("SLURM_JOB_ID", "1"), 2},
		"two channels":    {[]string{"control", "serving"}, env("SLURM_JOB_ID", "1"), 2},
		"no job":          {[]string{"control"}, env(), 1},
		"no agent socket": {[]string{"control"}, env("SLURM_JOB_ID", "1", agentapi.EnvRelayRoot, t.TempDir()), 1},
	} {
		out.Reset()
		errs.Reset()
		if got := relay("/s/agent/"+strings.Repeat("e", 64)+"/hb-agent", c.args, c.env, strings.NewReader(""), &out, &errs); got != c.want {
			t.Errorf("%s: relay exited %d, want %d (stderr %q)", name, got, c.want, errs.String())
		}
		if out.Len() != 0 {
			t.Errorf("%s: relay wrote %q to stdout, which carries only the session", name, out.String())
		}
	}
}
