package main

// The relaykeeper subcommand's tests (issue #343, PR 3). The command line is
// tested through parseKeeper and the exit codes through relaykeeperMain. The
// process test runs the test binary again as the subcommand, in a process of
// its own: it starts, serves its socket, answers a ping with its epoch E,
// stops on SIGTERM with exit 0, and counts its second start.
//
// This is not a relay test: no relay runs, no ssh is started, and no Provider
// sends a set. It shows only the process's start, E, its socket and its stop.
// The relays' own behaviour is internal/relaykeeper's (the Runner) and
// internal/hostdial's.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// keeperChildEnv makes the test binary the subcommand's process: init below
// hands os.Args[1:] to runSubcommand and exits with its code. It is a role of
// its own, beside TestMain's controller child (childEnvRunner), because it
// runs in init, before TestMain, which this file does not touch.
const keeperChildEnv = "HPCBURST_TEST_RELAYKEEPER_CHILD"

func init() {
	if os.Getenv(keeperChildEnv) == "" {
		return
	}
	code, ok := runSubcommand(os.Args[1:], os.Stderr)
	if !ok {
		fmt.Fprintf(os.Stderr, "the test's keeper child: %q names no subcommand\n", os.Args[1:])
		code = 3
	}
	os.Exit(code)
}

// The keeper's command line is the launcher's contract: two required flags,
// and a refusal that names what was wrong.
func TestParseKeeper(t *testing.T) {
	cases := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{"both flags", []string{"--state-dir", "/state", "--port-base", "30000"}, ""},
		{"no state directory", []string{"--port-base", "30000"}, "--state-dir is required"},
		{"an empty state directory", []string{"--state-dir", "", "--port-base", "30000"}, "--state-dir is required"},
		{"no port base", []string{"--state-dir", "/state"}, "--port-base is required"},
		{"no flags", nil, "is required"},
		{"a port base that is not a number", []string{"--state-dir", "/state", "--port-base", "x"}, "invalid value"},
		{"a port base that is not an integer", []string{"--state-dir", "/state", "--port-base", "30000.5"}, "invalid value"},
		{"an empty port base", []string{"--state-dir", "/state", "--port-base", ""}, "invalid value"},
		{"an unknown flag", []string{"--state-dir", "/state", "--port-base", "30000", "--frobnicate"}, "not defined"},
		// A controller flag is not the keeper's.
		{"a controller flag", []string{"--state-dir", "/state", "--port-base", "30000", "--health-addr", "127.0.0.1:30012"}, "not defined"},
		{"a stray argument", []string{"--state-dir", "/state", "--port-base", "30000", "extra"}, "unexpected arguments"},
		// The controller's range: the block ends at its metrics, base+12.
		{"a port base below 1024", []string{"--state-dir", "/state", "--port-base", "80"}, "out of range"},
		{"a port base too high for the block", []string{"--state-dir", "/state", "--port-base", "65524"}, "out of range"},
		{"a port base at the top of the block", []string{"--state-dir", "/state", "--port-base", "65523"}, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f, err := parseKeeper(c.args, io.Discard)
			if c.wantErr == "" {
				if err != nil {
					t.Fatalf("parseKeeper: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("parseKeeper error %v, want one containing %q", err, c.wantErr)
			}
			if f != (keeperFlags{}) {
				t.Fatalf("parseKeeper returned flags %+v with its error", f)
			}
		})
	}

	t.Run("the flags fill the host", func(t *testing.T) {
		f, err := parseKeeper([]string{"--port-base", "30000", "--state-dir", "/state"}, io.Discard)
		if err != nil {
			t.Fatal(err)
		}
		if f.host.StateDir != "/state" || f.host.PortBase != 30000 {
			t.Fatalf("host %+v, want state /state and port base 30000", f.host)
		}
		// The ports the keeper's relays dial are the controller's relaySettings:
		// the same function, the same host.
		want := map[agentapi.RelayChannel]int{agentapi.RelayControl: 30003, agentapi.RelayServing: 30007}
		got := f.host.RelayChannelPorts()
		if len(got) != len(want) || got[agentapi.RelayControl] != want[agentapi.RelayControl] || got[agentapi.RelayServing] != want[agentapi.RelayServing] {
			t.Fatalf("relay channel ports %v, want %v (base+3 and base+7, HBEP-6)", got, want)
		}
	})
}

// A bad command line exits 2 with one line; -h prints the usage and exits 0;
// a failure to start exits 1 with one line. None of them starts a keeper that
// serves, so this runs in the test process.
func TestRelaykeeperExitCodes(t *testing.T) {
	notADir := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(notADir, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name     string
		args     []string
		wantCode int
		wantLine string
	}{
		{"no state directory", []string{"--port-base", "30000"}, 2, "--state-dir is required"},
		{"no port base", []string{"--state-dir", "/state"}, 2, "--port-base is required"},
		{"a port base that is not an integer", []string{"--state-dir", "/state", "--port-base", "x"}, 2, "invalid value"},
		{"an unknown flag", []string{"--state-dir", "/state", "--port-base", "30000", "--frobnicate"}, 2, "not defined"},
		// The state directory's parent is a file, so the keeper cannot make its
		// state file's directory: a failure of the run, before it serves.
		{"a state directory that cannot be used", []string{"--state-dir", filepath.Join(notADir, "state"), "--port-base", "30000"}, 1, "not a directory"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var stderr bytes.Buffer
			code, ok := runSubcommand(append([]string{"relaykeeper"}, c.args...), &stderr)
			if !ok {
				t.Fatal("runSubcommand did not take the relaykeeper subcommand")
			}
			if code != c.wantCode {
				t.Fatalf("exit code %d, want %d; stderr:\n%s", code, c.wantCode, &stderr)
			}
			out := strings.TrimSuffix(stderr.String(), "\n")
			if out == "" || strings.Contains(out, "\n") {
				t.Fatalf("stderr is not one line: %q", stderr.String())
			}
			if !strings.HasPrefix(out, "hpcburst-controller relaykeeper: ") || !strings.Contains(out, c.wantLine) {
				t.Fatalf("stderr %q, want a line from hpcburst-controller relaykeeper naming %q", out, c.wantLine)
			}
		})
	}

	t.Run("help", func(t *testing.T) {
		var stderr bytes.Buffer
		if code, _ := runSubcommand([]string{"relaykeeper", "-h"}, &stderr); code != 0 {
			t.Fatalf("exit code %d, want 0", code)
		}
		if !strings.Contains(stderr.String(), "--state-dir DIR --port-base PORT") {
			t.Fatalf("the usage is missing: %q", stderr.String())
		}
	})
}

// Only the word relaykeeper as the first argument makes the binary the keeper:
// the controller's own command line, and anything else, is left for parse.
func TestRunSubcommandLeavesTheControllersFlagsAlone(t *testing.T) {
	for _, args := range [][]string{
		nil,
		{"--state-dir", "/state", "--port-base", "29220", "--health-addr", "127.0.0.1:29226"},
		{"--state-dir", "relaykeeper"},
		{"relaykeeperx"},
		{"RELAYKEEPER"},
	} {
		var stderr bytes.Buffer
		if code, ok := runSubcommand(args, &stderr); ok || code != 0 || stderr.Len() != 0 {
			t.Errorf("runSubcommand(%q) = %d, %v with stderr %q; want it to leave the arguments for parse", args, code, ok, &stderr)
		}
	}
}

// keeperProc is one keeper process of the test, started as the test binary
// again. The test holds it by the pidfd its own clone returned (SysProcAttr's
// PidFD), and every signal it sends goes through that descriptor, so a signal
// can reach only this child, never a process that has its pid by then. Nothing
// here signals by pid: no kill(2), no signal or kill through os.Process, no
// lookup of a process by its pid, and the command is a plain exec.Command,
// since one with a context kills by pid when it ends.
type keeperProc struct {
	cmd    *exec.Cmd
	pidfd  int
	stderr *bytes.Buffer // read only after done is closed
	done   chan struct{}
}

func startKeeperProc(t *testing.T, stateDir string) *keeperProc {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	p := &keeperProc{pidfd: -1, stderr: &bytes.Buffer{}, done: make(chan struct{})}
	// Port base 30000 is #343's block (30000-30039); the keeper process binds
	// no port of it, since no relay runs.
	p.cmd = exec.Command(exe, "relaykeeper", "--state-dir", stateDir, "--port-base", "30000")
	p.cmd.Env = append(os.Environ(), keeperChildEnv+"=1")
	p.cmd.Stderr = p.stderr
	p.cmd.SysProcAttr = &syscall.SysProcAttr{PidFD: &p.pidfd}
	if err := p.cmd.Start(); err != nil {
		t.Fatalf("start the keeper process: %v", err)
	}
	if p.pidfd < 0 {
		t.Fatal("the test's own start got no pidfd from clone")
	}
	go func() {
		_ = p.cmd.Wait()
		close(p.done)
	}()
	t.Cleanup(func() {
		select {
		case <-p.done:
		default:
			_ = unix.PidfdSendSignal(p.pidfd, unix.SIGKILL, nil, 0)
			select {
			case <-p.done:
			case <-time.After(30 * time.Second):
				t.Errorf("the keeper process did not end after its KILL")
			}
		}
		_ = unix.Close(p.pidfd)
	})
	return p
}

// waitPong waits for the keeper's socket to serve and returns its answer to a
// ping. The socket appears at bind and answers only once it listens, so it
// asks again until it is answered, the process ends, or the deadline passes.
func (p *keeperProc) waitPong(t *testing.T, socket string) wire.Pong {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		var pong wire.Pong
		err := wire.Exchange(context.Background(), socket, wire.KindPing, struct{}{}, &pong)
		if err == nil {
			return pong
		}
		select {
		case <-p.done:
			t.Fatalf("the keeper process ended before it answered (last ping: %v); its stderr:\n%s", err, p.stderr)
		case <-time.After(20 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatalf("no answer from the keeper's socket in 30 s (last ping: %v)", err)
		}
	}
}

// terminate sends SIGTERM through the pidfd and waits for the process to end,
// and returns its exit code.
func (p *keeperProc) terminate(t *testing.T) int {
	t.Helper()
	if err := unix.PidfdSendSignal(p.pidfd, unix.SIGTERM, nil, 0); err != nil {
		t.Fatalf("SIGTERM through the pidfd: %v", err)
	}
	select {
	case <-p.done:
	case <-time.After(30 * time.Second):
		t.Fatalf("the keeper process did not stop within 30 s of SIGTERM (its stderr is not read while it runs)")
	}
	return p.cmd.ProcessState.ExitCode()
}

// shortStateDir makes a state directory (mode 700, as the launcher's) under a
// short path, since the keeper's socket is in it and may not pass 107 bytes.
func shortStateDir(t *testing.T) string {
	t.Helper()
	for _, base := range []string{os.TempDir(), "/tmp"} {
		dir, err := os.MkdirTemp(base, "rk")
		if err != nil {
			continue
		}
		if len(relaykeeper.SocketPath(dir)) > agentapi.MaxRelaySocketPath {
			os.Remove(dir)
			continue
		}
		t.Cleanup(func() { _ = os.RemoveAll(dir) })
		return dir
	}
	t.Fatalf("no temporary directory with a socket path of at most %d bytes", agentapi.MaxRelaySocketPath)
	return ""
}

// startsOnDisk reads the keeper's start count from its state file, which the
// test checks beside the pong's epoch: E is 2³¹ plus this count.
func startsOnDisk(t *testing.T, stateDir string) int64 {
	t.Helper()
	b, err := os.ReadFile(relaykeeper.StatePath(stateDir))
	if err != nil {
		t.Fatal(err)
	}
	var st struct {
		Starts int64 `json:"starts"`
	}
	if err := json.Unmarshal(b, &st); err != nil {
		t.Fatalf("the state file: %v", err)
	}
	return st.Starts
}

// The process starts, serves its socket, and answers a ping with E = 2³¹ plus
// its start count, written to the state file; SIGTERM stops it with exit 0
// and removes its socket; a second start on the same state directory counts
// again.
//
// This shows only the process's start, E, its socket and its stop. No relay
// runs here: it is not a relay test.
//
// E's expected values are HBEP-6's (2³¹ plus the start count: 2147483649 on
// the first start), not read from relaykeeper.EpochBase. The socket's absence
// after the stop is checked before the second start, because wire.Listen
// replaces a socket left behind by a keeper that is gone, so the second start
// would succeed over a stale file and prove nothing about the stop.
func TestRelaykeeperProcessStartsServesAndStops(t *testing.T) {
	const firstE = int64(2147483649) // 2³¹ + 1
	stateDir := shortStateDir(t)
	socket := relaykeeper.SocketPath(stateDir)

	for run, wantE := range []int64{firstE, firstE + 1} {
		p := startKeeperProc(t, stateDir)
		pong := p.waitPong(t, socket)
		if pong.KeeperEpoch != wantE {
			t.Fatalf("start %d: the pong's keeper epoch is %d, want %d", run+1, pong.KeeperEpoch, wantE)
		}
		// Running: the socket is a socket of this user's alone.
		fi, err := os.Lstat(socket)
		if err != nil || fi.Mode().Type() != fs.ModeSocket || fi.Mode().Perm() != 0o600 {
			t.Fatalf("start %d: the socket while it serves: %v, %v; want a socket of mode 600", run+1, fi, err)
		}
		if got := startsOnDisk(t, stateDir); got != int64(run+1) {
			t.Fatalf("start %d: the state file counts %d starts, want %d", run+1, got, run+1)
		}

		if code := p.terminate(t); code != 0 {
			t.Fatalf("start %d: exit code %d after SIGTERM, want 0; stderr:\n%s", run+1, code, p.stderr)
		}
		log := p.stderr.String()
		for _, want := range []string{"the relay keeper serves", "the relay keeper stops"} {
			if !strings.Contains(log, want) {
				t.Errorf("start %d: the keeper's log has no %q:\n%s", run+1, want, log)
			}
		}

		// Stopped: the state file stays, mode 600, and the socket is gone.
		if _, err := os.Lstat(socket); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("start %d: the socket after the stop: %v, want it removed", run+1, err)
		}
		fi, err = os.Lstat(relaykeeper.StatePath(stateDir))
		if err != nil || !fi.Mode().IsRegular() || fi.Mode().Perm() != 0o600 {
			t.Fatalf("start %d: the state file after the stop: %v, %v; want a regular file of mode 600", run+1, fi, err)
		}
	}
}
