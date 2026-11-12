// Package node is spike S17's node side (spike S14's, at 3f863bad, with a bulk
// handler beside each engine double), built as a test binary
// (go test -c -o s17node.test ./node) because internal/agent/enginedouble may
// be linked only by _test.go files (its import guard walks the whole module
// tree, spikes included).
//
// With S17_NODE=1 the binary runs the node instead of its tests:
//
//   - one engine double per tunnel (enginedouble.Handler, 50 ms before each
//     token: 20 tokens/s, as S1c's engine paced), each on its own Unix socket,
//     which that tunnel's frpc reaches through its unix_domain_socket plugin;
//   - in host-dialed mode, one job side per tunnel (hostdial.StartJob: the
//     agent's two loopback listeners and its relay socket, published in the
//     Allocation's directory), frpc's serverPort being the serving
//     listener's port;
//   - in host-dialed mode, a job side with no engine for a tunnel marked
//     relay_only: layout (d)'s stand-in for a third relay channel, "bulk";
//   - in host-dialed mode, one TCP acceptor standing in for "the login node's
//     sshd, then srun, then the relay step": for each connection it reads the
//     relay line the owner built and runs hostdial.Relay with the
//     connection as the step's standard input and output.
//
// It writes the ready file once everything listens, and the job sides'
// session counts to the state file every 2 s and at its end.
package node

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/enginedouble"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// TokenDelay paces the engine double at 20 tokens/s (issue #313).
const TokenDelay = 50 * time.Millisecond

func TestMain(m *testing.M) {
	if os.Getenv("S17_NODE") == "1" {
		if err := runNode(os.Args[1:]); err != nil {
			fmt.Fprintln(os.Stderr, "s17node:", err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

type spec struct {
	Mode    string `json:"mode"`
	Tunnels []struct {
		Index  int    `json:"index"`
		UID    string `json:"uid"`
		Dir    string `json:"dir"`
		Socket string `json:"socket"`
		// RelayOnly is layout (d)'s stand-in for a third relay channel: a job
		// side with no engine of its own, whose serving channel carries the bulk
		// frpc's connections over a relay of their own.
		RelayOnly bool `json:"relay_only,omitempty"`
	} `json:"tunnels"`
}

// ready is what the driver reads before it starts frpc.
type ready struct {
	ServingPorts []int `json:"serving_ports"` // per tunnel, host-dialed mode only
	RelayPort    int   `json:"relay_port"`    // the login-node stand-in's port (host-dialed mode only; the kernel chose it)
	PID          int   `json:"pid"`
}

// jobState is one job side's sessions, for the heartbeat-loss count: more
// than one session on a channel means its relay dropped and came back.
type jobState struct {
	Index           int    `json:"index"`
	ControlSessions uint64 `json:"control_sessions"`
	ServingSessions uint64 `json:"serving_sessions"`
	Refused         uint64 `json:"refused"`
	ServingUp       bool   `json:"serving_up"`
}

type nodeState struct {
	Jobs        []jobState `json:"jobs"`
	RelaySteps  int64      `json:"relay_steps"`
	RelayErrors int64      `json:"relay_errors"`
}

func runNode(args []string) error {
	fs := flag.NewFlagSet("node", flag.ContinueOnError)
	specPath := fs.String("spec", "", "the configuration's spec (JSON)")
	relayListen := fs.String("relay-listen", "127.0.0.1:0", "the login-node stand-in's address (host-dialed; port 0: the kernel chooses, published in the ready file)")
	tmp := fs.String("tmp", "", "the job sides' TMPDIR, for their relay sockets")
	readyPath := fs.String("ready", "", "the ready file")
	statePath := fs.String("state", "", "the state file")
	logPath := fs.String("log", "", "log file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	var sp spec
	b, err := os.ReadFile(*specPath)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b, &sp); err != nil {
		return err
	}
	if *readyPath == "" || *statePath == "" {
		return errors.New("-ready and -state are required")
	}
	lf, err := os.OpenFile(*logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer lf.Close()
	log := slog.New(slog.NewTextHandler(lf, nil))

	var servers []*http.Server
	defer func() {
		for _, s := range servers {
			_ = s.Close()
		}
	}()
	for _, t := range sp.Tunnels {
		if t.RelayOnly {
			continue
		}
		cfg := enginedouble.DefaultConfig()
		cfg.TokenDelay = TokenDelay
		_ = os.Remove(t.Socket)
		ln, err := net.Listen("unix", t.Socket)
		if err != nil {
			return fmt.Errorf("tunnel %d's engine socket: %w", t.Index, err)
		}
		srv := &http.Server{Handler: withBulk(enginedouble.Handler(cfg), log.With("tunnel", t.Index))}
		servers = append(servers, srv)
		go func() { _ = srv.Serve(ln) }()
	}

	rd := ready{PID: os.Getpid()}
	var jobs []*hostdial.Job
	defer func() {
		for _, j := range jobs {
			_ = j.Close()
		}
	}()
	var steps, stepErrs atomic.Int64
	var acceptor net.Listener
	if sp.Mode == "host-dialed" {
		for _, t := range sp.Tunnels {
			if err := os.MkdirAll(t.Dir, 0o700); err != nil {
				return err
			}
			j, err := hostdial.StartJob(hostdial.JobOptions{TmpDir: *tmp, AllocDir: t.Dir, Allocation: t.UID, Logger: log.With("tunnel", t.Index)})
			if err != nil {
				return fmt.Errorf("tunnel %d's job side: %w", t.Index, err)
			}
			jobs = append(jobs, j)
			rd.ServingPorts = append(rd.ServingPorts, j.Port(agentapi.RelayServing))
		}
		if acceptor, err = net.Listen("tcp4", *relayListen); err != nil {
			return err
		}
		defer acceptor.Close()
		rd.RelayPort = acceptor.Addr().(*net.TCPAddr).Port
		go acceptSteps(acceptor, log, &steps, &stepErrs)
	}
	if err := writeJSON(*readyPath, rd); err != nil {
		return err
	}
	state := func() nodeState {
		st := nodeState{RelaySteps: steps.Load(), RelayErrors: stepErrs.Load()}
		for i, j := range jobs {
			st.Jobs = append(st.Jobs, jobState{Index: i, ControlSessions: j.Sessions(agentapi.RelayControl),
				ServingSessions: j.Sessions(agentapi.RelayServing), Refused: j.Refused(agentapi.RelayServing),
				ServingUp: j.Connected(agentapi.RelayServing)})
		}
		return st
	}
	term := make(chan os.Signal, 1)
	signal.Notify(term, syscall.SIGTERM, syscall.SIGINT)
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			if err := writeJSON(*statePath, state()); err != nil {
				log.Warn("the state file", "err", err)
			}
		case <-term:
			return writeJSON(*statePath, state())
		}
	}
}

// acceptSteps runs one relay step per connection: the relay line first, then
// hostdial.Relay over the connection, as srun runs "relay <channel> ..." with
// the step's standard streams carried back to the owner's ssh.
func acceptSteps(ln net.Listener, log *slog.Logger, steps, stepErrs *atomic.Int64) {
	var wg sync.WaitGroup
	defer wg.Wait()
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		_ = c.(*net.TCPConn).SetNoDelay(true)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer c.Close()
			a, err := readRelayLine(c)
			if err != nil {
				stepErrs.Add(1)
				log.Warn("a relay step's line", "err", err)
				return
			}
			steps.Add(1)
			if err := hostdial.Relay(a, c, c); err != nil {
				stepErrs.Add(1)
				// The step's standard error, as a real relay writes it.
				log.Info("a relay step ended", "channel", a.Channel, "line", hostdial.RelayMessage(a, err))
			}
		}()
	}
}

// readRelayLine reads the line the owner built (agentapi.RelayLine), one byte
// at a time so nothing of the session is consumed, and parses the relay's
// arguments after "relay".
func readRelayLine(r io.Reader) (agentapi.RelayArgs, error) {
	var line []byte
	one := make([]byte, 1)
	for {
		if _, err := io.ReadFull(r, one); err != nil {
			return agentapi.RelayArgs{}, err
		}
		if one[0] == '\n' {
			break
		}
		if line = append(line, one[0]); len(line) > 1024 {
			return agentapi.RelayArgs{}, errors.New("a relay line over 1 KiB")
		}
	}
	return parseRelayLine(string(line))
}

func parseRelayLine(line string) (agentapi.RelayArgs, error) {
	f := strings.Fields(line)
	for i, w := range f {
		if w == agentapi.RelaySubcommand && i > 0 && strings.HasPrefix(f[0], "srun") {
			return agentapi.ParseRelayArgs(f[i+1:])
		}
	}
	return agentapi.RelayArgs{}, fmt.Errorf("not a relay line: %d words", len(f))
}

func writeJSON(path string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	tmp := filepath.Join(filepath.Dir(path), "."+filepath.Base(path)+".tmp")
	if err := os.WriteFile(tmp, append(b, '\n'), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// The node's own unit tests. They run only the node's parsing; the whole
// stack runs under the driver (s14.py), never under go test.

func TestParseRelayLineTakesTheArgumentsAfterRelay(t *testing.T) {
	want := agentapi.RelayArgs{Channel: agentapi.RelayServing, Dir: "/alloc/example-0", Allocation: "example-uid-0", Epoch: 1, Seq: 7}
	line, err := agentapi.RelayLine("1000", "/staging/example/agent/hb-agent", want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := parseRelayLine(line)
	if err != nil || got != want {
		t.Fatalf("parseRelayLine(%q) = %+v, %v; want %+v", line, got, err, want)
	}
}

func TestParseRelayLineRefusesAnotherCommand(t *testing.T) {
	for _, line := range []string{"", "relay serving --dir /a", "echo relay serving --dir /a --allocation u --epoch 1 --seq 1"} {
		if _, err := parseRelayLine(line); err == nil {
			t.Errorf("parseRelayLine(%q) accepted it", line)
		}
	}
}

func TestReadRelayLineLeavesTheSessionUnread(t *testing.T) {
	a := agentapi.RelayArgs{Channel: agentapi.RelayControl, Dir: "/alloc/example-1", Allocation: "example-uid-1", Epoch: 2, Seq: 3}
	line, err := agentapi.RelayLine("1001", "/staging/example/agent/hb-agent", a)
	if err != nil {
		t.Fatal(err)
	}
	r := bufio.NewReader(strings.NewReader(line + "\nSESSION-BYTES"))
	got, err := readRelayLine(r)
	if err != nil || got != a {
		t.Fatalf("readRelayLine = %+v, %v; want %+v", got, err, a)
	}
	rest, _ := io.ReadAll(r)
	if string(rest) != "SESSION-BYTES" {
		t.Fatalf("the bytes after the line: %q, want %q", rest, "SESSION-BYTES")
	}
}
