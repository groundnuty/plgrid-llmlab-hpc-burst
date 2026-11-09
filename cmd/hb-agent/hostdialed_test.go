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
	"regexp"
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

// keepRelay runs "relay <ch> ..." processes, one at a time, each serving its
// session to target, and starts another 100 ms after one ends (the first ones
// end at once, before the agent has published its socket), each with the next
// sequence of the epoch, until ctx ends.
func keepRelay(t *testing.T, ctx context.Context, ch agentapi.RelayChannel, allocDir, uid, target string, wg *sync.WaitGroup) {
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for seq := int64(1); ctx.Err() == nil; seq++ {
			a := agentapi.RelayArgs{Channel: ch, Dir: allocDir, Allocation: uid, Epoch: 1, Seq: seq}
			line, err := agentapi.RelayLine("1", self, a)
			if err != nil {
				t.Error(err)
				return
			}
			words := strings.Fields(line)
			cmd := exec.Command(self, words[6:]...) // the agent binary's own words: relay <ch> --dir ...
			cmd.Env = append(os.Environ(), "HB_TEST_ROLE="+roleAgent, "SLURM_JOB_ID="+eJobID)
			in, _ := cmd.StdinPipe()
			out, _ := cmd.StdoutPipe()
			cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
			if err := cmd.Start(); err != nil {
				return
			}
			sctx, cancel := context.WithCancel(ctx)
			served := make(chan struct{})
			go func() { _ = hostdial.Serve(sctx, relayPipes{out, in}, target, uid, nil, nil); close(served) }()
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

const eUIDHostDialed = "0a1b2c3d-0000-4000-8000-000000000099"

func TestAgentEndToEndHostDialed(t *testing.T) {
	root := shortRoot(t)
	ep := startEndpoint(t, root)
	j := newJob(t, root, agentapi.AgentConfig{
		ProtocolVersion: agentapi.ProtocolVersion, Tag: eTag, Mode: agentapi.DialHomeHostDialed,
		CertSPKISHA256: ep.pin, FrpcDigest: strings.Repeat("d", 64), Allocation: eUIDHostDialed,
	})
	j.hostDialed = true
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	defer func() { cancel(); wg.Wait() }()
	keepRelay(t, ctx, agentapi.RelayControl, j.allocDir, eUIDHostDialed, ep.addr, &wg)
	out := exercise(t, ep, j)
	if !strings.Contains(out, "relay session up") {
		t.Errorf("the agent logged no relay session:\n%s", out)
	}
	if _, err := os.Stat(agentapi.RelaySocketFile(j.allocDir)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the rendezvous file is left after the agent stopped: %v", err)
	}
	m := regexp.MustCompile(`host-dialed: listening for the relays.* socket=(\S+)`).FindStringSubmatch(out)
	if m == nil {
		t.Fatalf("the agent logged no relay socket:\n%s", out)
	}
	if dir := filepath.Dir(strings.Trim(m[1], `"`)); !strings.HasPrefix(filepath.Base(dir), "hb-relay-") {
		t.Errorf("the relay socket %s is not in a hb-relay-<random> directory", m[1])
	} else if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the relay socket's directory %s is left after the agent stopped: %v", dir, err)
	}
}

func TestRelayUsage(t *testing.T) {
	var out, errs bytes.Buffer
	dir := t.TempDir()
	good := []string{"control", "--dir", dir, "--allocation", "u-1", "--epoch", "1", "--seq", "1"}
	for name, c := range map[string]struct {
		args []string
		want int
		line string
	}{
		"no channel":      {nil, 2, ""},
		"another channel": {[]string{"plugin", "--dir", dir, "--allocation", "u-1", "--epoch", "1", "--seq", "1"}, 2, ""},
		"no fence":        {[]string{"control", "--dir", dir, "--allocation", "u-1"}, 2, ""},
		"no agent yet":    {good, hostdial.ExitNotReady, "hb-relay: agent not ready"},
	} {
		out.Reset()
		errs.Reset()
		if got := relay(c.args, strings.NewReader(""), &out, &errs); got != c.want {
			t.Errorf("%s: relay exited %d, want %d (stderr %q)", name, got, c.want, errs.String())
		}
		if c.line != "" && !strings.HasPrefix(errs.String(), c.line) {
			t.Errorf("%s: relay's first stderr line %q, want %q", name, errs.String(), c.line)
		}
		if out.Len() != 0 {
			t.Errorf("%s: relay wrote %q to stdout, which carries only the session", name, out.String())
		}
	}
}

// Host-dialed mode with no TMPDIR fails fast (science-agent ruling on #301):
// the relay socket has no place (hostdial.ErrNoSocketDir), and the agent
// cannot register without a relay, so it stops at once with the named error
// on its output and a non-zero exit, rather than holding the job's GPU until
// its registration deadline.
func TestAgentHostDialedWithNoTMPDIRFailsFast(t *testing.T) {
	root := shortRoot(t)
	ep := startEndpoint(t, root)
	j := newJob(t, root, agentapi.AgentConfig{
		ProtocolVersion: agentapi.ProtocolVersion, Tag: eTag, Mode: agentapi.DialHomeHostDialed,
		CertSPKISHA256: ep.pin, FrpcDigest: strings.Repeat("d", 64), Allocation: eUIDHostDialed,
	})
	j.extraEnv = append(j.extraEnv, "TMPDIR=")
	cmd, out := j.start(t)
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		if code := cmd.ProcessState.ExitCode(); err == nil || code == 0 {
			t.Errorf("the agent exited %d (%v), want a non-zero exit", code, err)
		}
	case <-time.After(30 * time.Second):
		t.Fatalf("the agent still runs 30 s after it started with no TMPDIR in host-dialed mode:\n%s", out.String())
	}
	if !strings.Contains(out.String(), "no directory for the relay socket") {
		t.Errorf("the agent's output does not name the missing socket directory:\n%s", out.String())
	}
}
